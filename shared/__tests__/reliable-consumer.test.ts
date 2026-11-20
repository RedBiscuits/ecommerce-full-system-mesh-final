import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { z } from 'zod';
import {
  DeliveryContext,
  DeliverySettled,
  NonRetryableError,
  PLATFORM_EXCHANGES,
  ReliableConsumer,
  ReliableConsumerOptions,
  jitteredRetryDelay,
  retryCeiling,
} from '../messaging';
import { TestBroker, createTestBroker, nextSettled, waitUntil } from './helpers/broker';

const EXCHANGE = 'test.events';
const Body = z.object({ id: z.string(), n: z.number().int() });
type BodyT = z.infer<typeof Body>;

const FAST_RETRY = { maxAttempts: 4, baseDelayMs: 20, maxDelayMs: 80, minDelayMs: 5 };

describe('ReliableConsumer (per-consumer queues, jittered retries, DLQ)', () => {
  let broker: TestBroker;
  const consumers: Array<ReliableConsumer<BodyT>> = [];

  function makeConsumer(
    name: string,
    handler: (m: BodyT, ctx: DeliveryContext) => Promise<string | void>,
    overrides: Partial<ReliableConsumerOptions<BodyT>> = {}
  ): ReliableConsumer<BodyT> {
    const consumer = new ReliableConsumer<BodyT>({
      name,
      bindings: [{ exchange: EXCHANGE, exchangeType: 'topic', routingKey: 'thing.#' }],
      decode: (raw) => Body.parse(raw),
      handler,
      url: broker.url,
      connect: broker.connect,
      prefetch: 4,
      retry: FAST_RETRY,
      ttlPumpIntervalMs: 5,
      reconnect: { initialDelayMs: 1, maxDelayMs: 5, jitter: 0 },
      ...overrides,
    });
    consumers.push(consumer);
    return consumer;
  }

  function collect(consumer: ReliableConsumer<BodyT>): DeliverySettled[] {
    const events: DeliverySettled[] = [];
    consumer.on('settled', (e: DeliverySettled) => events.push(e));
    return events;
  }

  beforeEach(async () => {
    broker = await createTestBroker();
  });

  afterEach(async () => {
    for (const c of consumers.splice(0)) await c.close();
    await broker.close();
  });

  it('declares its own work, retry-tier and DLQ queues bound to the producer exchange', async () => {
    const c = makeConsumer('svc.things', async () => undefined);
    await c.start();
    expect(c.queues).toEqual({
      workQueue: 'svc.things',
      retryQueues: ['svc.things.retry.1', 'svc.things.retry.2', 'svc.things.retry.3'],
      deadLetterQueue: 'svc.things.dlq',
    });
    for (const q of [c.queues.workQueue, ...c.queues.retryQueues, c.queues.deadLetterQueue]) {
      await expect(broker.inspector.checkQueue(q)).resolves.toMatchObject({ messageCount: 0 });
    }
  });

  it('acks a valid message exactly once and reports the handler outcome', async () => {
    const seen: BodyT[] = [];
    const c = makeConsumer('svc.ok', async (m) => {
      seen.push(m);
      return 'PROCESSED';
    });
    await c.start();
    const settled = nextSettled(c);
    broker.publishJson(EXCHANGE, 'thing.created', { id: 'a', n: 1 }, { messageId: 'evt-1', type: 'thing.created' });
    expect(await settled).toMatchObject({ outcome: 'acked', attempt: 1, messageId: 'evt-1', result: 'PROCESSED' });
    expect(seen).toEqual([{ id: 'a', n: 1 }]);
    expect(await broker.depth('svc.ok')).toBe(0);
  });

  it('dead-letters a malformed (non-JSON) message immediately, without calling the handler', async () => {
    let calls = 0;
    const c = makeConsumer('svc.poison', async () => {
      calls++;
    });
    await c.start();
    const settled = nextSettled(c);
    broker.publishRaw(EXCHANGE, 'thing.created', Buffer.from('{not json'), { messageId: 'evt-bad' });
    expect(await settled).toMatchObject({ outcome: 'dead-lettered', attempt: 1 });
    expect(calls).toBe(0);
    const [parked] = await c.inspectDeadLetters();
    expect(parked).toMatchObject({ messageId: 'evt-bad', attempt: 1 });
    expect(parked.reason).toMatch(/^MALFORMED_MESSAGE/);
    expect(await broker.depth('svc.poison.dlq')).toBe(1); // inspect puts it back
  });

  it('dead-letters a schema-invalid message immediately with the validation details', async () => {
    const c = makeConsumer('svc.schema', async () => undefined);
    await c.start();
    const settled = nextSettled(c);
    broker.publishJson(EXCHANGE, 'thing.created', { id: 42 }, { messageId: 'evt-schema' });
    const e = await settled;
    expect(e.outcome).toBe('dead-lettered');
    expect(e.reason).toMatch(/^SCHEMA_INVALID: id: Expected string/);
  });

  it('retries a transient failure with jittered backoff on per-attempt tiers, then succeeds', async () => {
    let calls = 0;
    const attempts: number[] = [];
    const c = makeConsumer('svc.flaky', async (_m, ctx) => {
      calls++;
      attempts.push(ctx.attempt);
      if (calls < 3) throw new Error(`dependency blip #${calls}`);
      return 'PROCESSED';
    });
    const events = collect(c);
    await c.start();
    const done = nextSettled(c, (e) => e.outcome === 'acked');
    broker.publishJson(EXCHANGE, 'thing.updated', { id: 'b', n: 2 }, { messageId: 'evt-flaky' });
    await done;

    expect(attempts).toEqual([1, 2, 3]);
    const retries = events.filter((e) => e.outcome === 'retry-scheduled');
    expect(retries.map((e) => e.retryQueue)).toEqual(['svc.flaky.retry.1', 'svc.flaky.retry.2']);
    expect(retries.map((e) => e.reason)).toEqual(['dependency blip #1', 'dependency blip #2']);
    // Every delay lies inside its full-jitter window.
    retries.forEach((e, i) => {
      expect(e.delayMs).toBeGreaterThanOrEqual(FAST_RETRY.minDelayMs);
      expect(e.delayMs).toBeLessThanOrEqual(retryCeiling(i + 1, FAST_RETRY));
    });
    expect(await broker.depth('svc.flaky.dlq')).toBe(0);
  });

  it('dead-letters after maxAttempts with RETRIES_EXHAUSTED and the last error', async () => {
    let calls = 0;
    const c = makeConsumer('svc.down', async () => {
      calls++;
      throw new Error('still down');
    });
    await c.start();
    const dead = nextSettled(c, (e) => e.outcome === 'dead-lettered');
    broker.publishJson(EXCHANGE, 'thing.updated', { id: 'c', n: 3 }, { messageId: 'evt-down' });
    const e = await dead;
    expect(calls).toBe(FAST_RETRY.maxAttempts);
    expect(e.attempt).toBe(FAST_RETRY.maxAttempts);
    expect(e.reason).toBe(`RETRIES_EXHAUSTED after ${FAST_RETRY.maxAttempts} attempt(s): still down`);
    const [parked] = await c.inspectDeadLetters();
    expect(parked).toMatchObject({ messageId: 'evt-down', attempt: FAST_RETRY.maxAttempts, body: { id: 'c', n: 3 } });
  });

  it('dead-letters a NonRetryableError on the first attempt (no retry budget burnt)', async () => {
    let calls = 0;
    const c = makeConsumer('svc.rule', async () => {
      calls++;
      throw new NonRetryableError('AMOUNT_MISMATCH', 'paid 10 but order totals 12');
    });
    await c.start();
    const dead = nextSettled(c, (e) => e.outcome === 'dead-lettered');
    broker.publishJson(EXCHANGE, 'thing.updated', { id: 'd', n: 4 });
    expect((await dead).reason).toBe('NON_RETRYABLE: AMOUNT_MISMATCH: paid 10 but order totals 12');
    expect(calls).toBe(1);
  });

  it('isolates consumers: a retry or DLQ replay only reaches the consumer that failed', async () => {
    const okCalls: string[] = [];
    let failing = true;
    const failCalls: string[] = [];
    const ok = makeConsumer('svc.a', async (m) => {
      okCalls.push(m.id);
    });
    const flaky = makeConsumer(
      'svc.b',
      async (m) => {
        failCalls.push(m.id);
        if (failing) throw new Error('b is broken');
      },
      { retry: { ...FAST_RETRY, maxAttempts: 2 } }
    );
    await ok.start();
    await flaky.start();

    const okDone = nextSettled(ok, (e) => e.outcome === 'acked');
    const flakyDead = nextSettled(flaky, (e) => e.outcome === 'dead-lettered');
    broker.publishJson(EXCHANGE, 'thing.created', { id: 'x', n: 1 }, { messageId: 'evt-x' });
    await okDone;
    await flakyDead;
    expect(okCalls).toEqual(['x']);
    expect(failCalls).toEqual(['x', 'x']); // 2 attempts, then parked

    // Fix the consumer and replay its DLQ: only svc.b sees the message again.
    failing = false;
    const replayedAck = nextSettled(flaky, (e) => e.outcome === 'acked');
    const summary = await flaky.replayDeadLetters();
    expect(summary).toMatchObject({ requested: 1, replayed: 1, remaining: 0 });
    const e = await replayedAck;
    expect(e.attempt).toBe(1); // fresh attempt budget
    expect(failCalls).toEqual(['x', 'x', 'x']);
    await new Promise((r) => setTimeout(r, 30));
    expect(okCalls).toEqual(['x']); // svc.a was never involved
  });

  it('passes the replay count to the handler and keeps the body untouched', async () => {
    let fail = true;
    const contexts: DeliveryContext[] = [];
    const c = makeConsumer(
      'svc.replay',
      async (_m, ctx) => {
        contexts.push(ctx);
        if (fail) throw new NonRetryableError('BROKEN');
      },
      {}
    );
    await c.start();
    const dead = nextSettled(c, (e) => e.outcome === 'dead-lettered');
    broker.publishJson(EXCHANGE, 'thing.created', { id: 'r', n: 9 }, { messageId: 'evt-r' });
    await dead;
    fail = false;
    const acked = nextSettled(c, (e) => e.outcome === 'acked');
    await c.replayDeadLetters({ limit: 10 });
    await acked;
    expect(contexts.map((x) => [x.attempt, x.replayCount, x.messageId])).toEqual([
      [1, 0, 'evt-r'],
      [1, 1, 'evt-r'],
    ]);
  });

  it('survives a broker drop: the session is rebuilt and unacked work is redelivered', async () => {
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    const handled: string[] = [];
    let first = true;
    const c = makeConsumer('svc.drop', async (m, ctx) => {
      if (first) {
        first = false;
        await gate; // in flight while the connection drops
        return 'LATE';
      }
      handled.push(`${m.id}:${ctx.redelivered}`);
      return 'PROCESSED';
    });
    await c.start();
    const sessions: number[] = [];
    c.on('session', (epoch: number) => sessions.push(epoch));

    broker.publishJson(EXCHANGE, 'thing.created', { id: 'z', n: 1 }, { messageId: 'evt-z' });
    await waitUntil(() => !first, { label: 'first delivery in flight' });
    await broker.dropLatest();
    await waitUntil(() => sessions.length === 1, { label: 'session rebuilt' });
    release();

    await waitUntil(() => handled.length === 1, { label: 'redelivery' });
    expect(handled[0]).toMatch(/^z:/);
    expect(broker.connections.length).toBeGreaterThanOrEqual(2);
    // The message is finally settled on the live session and not left behind.
    await waitUntil(async () => (await broker.depth('svc.drop')) === 0, { label: 'queue drained' });
  });

  it('reports queue depths per tier', async () => {
    const c = makeConsumer('svc.depth', async () => {
      throw new NonRetryableError('NOPE');
    });
    await c.start();
    const dead = nextSettled(c, (e) => e.outcome === 'dead-lettered');
    broker.publishJson(EXCHANGE, 'thing.created', { id: 'q', n: 1 });
    await dead;
    expect(await c.queueDepths()).toEqual({
      work: 0,
      retry: { 'svc.depth.retry.1': 0, 'svc.depth.retry.2': 0, 'svc.depth.retry.3': 0 },
      deadLetter: 1,
    });
  });

  it('routes the requeue and dead-letter traffic through the shared platform exchanges', async () => {
    const c = makeConsumer('svc.route', async () => undefined);
    await c.start();
    await expect(broker.inspector.checkExchange(PLATFORM_EXCHANGES.requeue)).resolves.toBeTruthy();
    await expect(broker.inspector.checkExchange(PLATFORM_EXCHANGES.deadLetter)).resolves.toBeTruthy();
    // A message requeued for svc.route only lands in svc.route.
    const acked = nextSettled(c, (e) => e.outcome === 'acked');
    broker.publishJson(PLATFORM_EXCHANGES.requeue, 'svc.route', { id: 'rq', n: 1 });
    await acked;
  });

  it('validates its configuration', () => {
    expect(() => makeConsumer('bad', async () => undefined, { prefetch: 0 })).toThrow(/prefetch/);
    expect(() => makeConsumer('bad2', async () => undefined, { retry: { maxAttempts: 0 } })).toThrow(/maxAttempts/);
    consumers.splice(0);
  });
});

describe('full-jitter retry delays', () => {
  const policy = { maxAttempts: 6, baseDelayMs: 100, maxDelayMs: 1000, minDelayMs: 10 };

  it('doubles the window per attempt and caps it', () => {
    expect([1, 2, 3, 4, 5, 6].map((a) => retryCeiling(a, policy))).toEqual([100, 200, 400, 800, 1000, 1000]);
  });

  it('spreads delays over [min, ceiling]', () => {
    expect(jitteredRetryDelay(3, { ...policy, random: () => 0 })).toBe(10);
    expect(jitteredRetryDelay(3, { ...policy, random: () => 0.5 })).toBe(205);
    expect(jitteredRetryDelay(3, { ...policy, random: () => 0.999999 })).toBe(400);
  });

  it('never exceeds the cap and never goes below the floor', () => {
    for (let i = 0; i < 2000; i++) {
      const attempt = 1 + (i % 10);
      const d = jitteredRetryDelay(attempt, policy);
      expect(d).toBeGreaterThanOrEqual(10);
      expect(d).toBeLessThanOrEqual(1000);
    }
  });
});
