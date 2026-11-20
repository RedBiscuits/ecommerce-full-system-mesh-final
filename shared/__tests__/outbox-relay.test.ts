import { PGlite } from '@electric-sql/pglite';
import type { ChannelModel, ConfirmChannel } from 'amqplib';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { buildIntegrationEvent, deterministicEventId } from '../contracts';
import {
  AmqpConnect,
  INTEGRATION_TABLES_SQL,
  OutboxRelay,
  OutboxRelayOptions,
  SqlOutboxStore,
  claimCheckpoint,
  completeCheckpoint,
  enqueueIntegrationEvent,
  listCheckpoints,
  readCheckpoint,
} from '../messaging';
import { TestBroker, createTestBroker, waitUntil } from './helpers/broker';

const EXCHANGE = 'orders.events';

function event(n: number, overrides: Partial<{ eventId: string; correlationId: string }> = {}) {
  return buildIntegrationEvent({
    eventId: overrides.eventId ?? `evt-${n}`,
    eventType: 'order.placed',
    producer: 'test',
    correlationId: overrides.correlationId ?? `order-${n}`,
    causationId: null,
    aggregateId: `order-${n}`,
    payload: { n },
  });
}

describe('OutboxRelay + SqlOutboxStore (transactional outbox on PostgreSQL)', () => {
  let db: PGlite;
  let broker: TestBroker;
  const relays: OutboxRelay[] = [];

  async function bindInspectorQueue(name = 'inspect.orders'): Promise<string> {
    await broker.inspector.assertExchange(EXCHANGE, 'topic', { durable: true });
    await broker.inspector.assertQueue(name, { durable: true });
    await broker.inspector.bindQueue(name, EXCHANGE, '#');
    return name;
  }

  async function drainQueue(queue: string) {
    const out: Array<{ body: any; messageId?: string; type?: string; correlationId?: string }> = [];
    for (;;) {
      const msg = await broker.inspector.get(queue, { noAck: true });
      if (!msg) return out;
      out.push({
        body: JSON.parse(msg.content.toString()),
        messageId: msg.properties.messageId,
        type: msg.properties.type,
        correlationId: msg.properties.correlationId,
      });
    }
  }

  function makeRelay(overrides: Partial<OutboxRelayOptions> = {}): OutboxRelay {
    const relay = new OutboxRelay({
      name: 'test',
      store: new SqlOutboxStore(db),
      url: broker.url,
      connect: broker.connect,
      exchanges: [{ name: EXCHANGE, type: 'topic' }],
      pollIntervalMs: 0,
      batchSize: 3,
      retry: { maxAttempts: 3, baseDelayMs: 1, maxDelayMs: 2, minDelayMs: 0 },
      publishTimeoutMs: 500,
      reconnect: { initialDelayMs: 1, maxDelayMs: 5, jitter: 0 },
      ...overrides,
    });
    relays.push(relay);
    return relay;
  }

  async function statuses() {
    const res = await db.query<{ event_id: string; status: string; attempts: number }>(
      `SELECT event_id, status, attempts FROM integration_outbox ORDER BY seq`
    );
    return res.rows.map((r) => [r.event_id, r.status, Number(r.attempts)]);
  }

  beforeAll(async () => {
    db = new PGlite();
    await db.exec(INTEGRATION_TABLES_SQL);
  });

  afterAll(async () => {
    await db.close();
  });

  beforeEach(async () => {
    await db.exec('TRUNCATE integration_outbox, integration_checkpoints RESTART IDENTITY');
    broker = await createTestBroker();
  });

  afterEach(async () => {
    for (const r of relays.splice(0)) await r.close();
    await broker.close();
  });

  it('publishes committed rows in commit order with the event id as messageId', async () => {
    const queue = await bindInspectorQueue();
    await db.transaction(async (tx) => {
      for (let i = 1; i <= 7; i++) {
        await enqueueIntegrationEvent(tx, { exchange: EXCHANGE, routingKey: 'order.placed', event: event(i) });
      }
    });
    const relay = makeRelay();
    await relay.start();
    await relay.drain();

    const received = await drainQueue(queue);
    expect(received.map((m) => m.messageId)).toEqual(['evt-1', 'evt-2', 'evt-3', 'evt-4', 'evt-5', 'evt-6', 'evt-7']);
    expect(received[0]).toMatchObject({ type: 'order.placed', correlationId: 'order-1', body: { payload: { n: 1 } } });
    expect((await statuses()).every(([, s]) => s === 'PUBLISHED')).toBe(true);
    expect(await relay.stats()).toMatchObject({ pending: 0, published: 7, failed: 0 });
  });

  it('a rolled-back transaction leaves no outbox row (no event for a change that never happened)', async () => {
    await expect(
      db.transaction(async (tx) => {
        await enqueueIntegrationEvent(tx, { exchange: EXCHANGE, routingKey: 'order.placed', event: event(1) });
        throw new Error('business rule failed');
      })
    ).rejects.toThrow('business rule failed');
    expect(await statuses()).toEqual([]);
  });

  it('never records the same (deterministic) event twice', async () => {
    const id = deterministicEventId('order.placed', 'order-9');
    expect(await enqueueIntegrationEvent(db, { exchange: EXCHANGE, routingKey: 'k', event: event(9, { eventId: id }) })).toBe(true);
    expect(await enqueueIntegrationEvent(db, { exchange: EXCHANGE, routingKey: 'k', event: event(9, { eventId: id }) })).toBe(false);
    expect(await statuses()).toHaveLength(1);
    expect(id).toBe(deterministicEventId('order.placed', 'order-9'));
    expect(id).not.toBe(deterministicEventId('order.placed', 'order-10'));
  });

  it('keeps rows in the database during a broker outage and drains them on reconnect', async () => {
    const queue = await bindInspectorQueue();
    const relay = makeRelay();
    await relay.start();

    broker.state.up = false;
    await broker.dropLatest();
    await enqueueIntegrationEvent(db, { exchange: EXCHANGE, routingKey: 'order.placed', event: event(1) });
    await enqueueIntegrationEvent(db, { exchange: EXCHANGE, routingKey: 'order.placed', event: event(2) });
    const idle = await relay.runOnce();
    expect(idle.skipped).toBe('no-connection');
    expect((await statuses()).map(([, s]) => s)).toEqual(['PENDING', 'PENDING']);

    broker.state.up = true;
    await waitUntil(async () => (await relay.stats()).published === 2, { label: 'backlog drained' });
    expect((await drainQueue(queue)).map((m) => m.messageId)).toEqual(['evt-1', 'evt-2']);
  });

  it('backs off on broker nacks, parks the row after maxAttempts, and an operator retry publishes it', async () => {
    const queue = await bindInspectorQueue();
    let nackAll = true;
    const connect: AmqpConnect = async (url) => {
      const conn = (await broker.connect(url)) as ChannelModel;
      const create = conn.createConfirmChannel.bind(conn);
      conn.createConfirmChannel = async () => {
        const ch = (await create()) as ConfirmChannel;
        const publish = ch.publish.bind(ch);
        ch.publish = ((ex: string, rk: string, content: Buffer, opts: any, cb?: (err: unknown) => void) => {
          if (nackAll && opts?.messageId === 'evt-poison') {
            setImmediate(() => cb?.(new Error('message nacked')));
            return true;
          }
          return publish(ex, rk, content, opts, cb);
        }) as typeof ch.publish;
        return ch;
      };
      return conn;
    };
    await enqueueIntegrationEvent(db, { exchange: EXCHANGE, routingKey: 'k', event: event(1, { eventId: 'evt-poison' }) });
    await enqueueIntegrationEvent(db, { exchange: EXCHANGE, routingKey: 'k', event: event(2, { eventId: 'evt-fine' }) });

    const relay = makeRelay({ connect, pollIntervalMs: 5 });
    await relay.start();
    await waitUntil(
      async () => (await statuses()).some(([id, s]) => id === 'evt-poison' && s === 'FAILED'),
      { label: 'poison row parked' }
    );
    // The poison row does not block the queue behind it.
    expect(await statuses()).toEqual([
      ['evt-poison', 'FAILED', 3],
      ['evt-fine', 'PUBLISHED', 1],
    ]);

    nackAll = false;
    expect(await new SqlOutboxStore(db).retryFailed()).toBe(1);
    await relay.drain();
    expect((await statuses())[0]).toEqual(['evt-poison', 'PUBLISHED', 1]);
    expect((await drainQueue(queue)).map((m) => m.messageId).sort()).toEqual(['evt-fine', 'evt-poison']);
  });

  it('two relays never publish the same row (claims are leases)', async () => {
    const queue = await bindInspectorQueue();
    for (let i = 1; i <= 12; i++) {
      await enqueueIntegrationEvent(db, { exchange: EXCHANGE, routingKey: 'k', event: event(i) });
    }
    const a = makeRelay({ relayId: 'relay-a' });
    const b = makeRelay({ relayId: 'relay-b' });
    await Promise.all([a.start(), b.start()]);
    await Promise.all([a.drain(), b.drain()]);
    await waitUntil(async () => (await a.stats()).published === 12, { label: 'all published' });
    const ids = (await drainQueue(queue)).map((m) => m.messageId);
    expect(ids).toHaveLength(12);
    expect(new Set(ids).size).toBe(12);
  });

  it('an expired lease makes a row claimable again (the claiming relay died mid-publish)', async () => {
    await enqueueIntegrationEvent(db, { exchange: EXCHANGE, routingKey: 'k', event: event(1) });
    const store = new SqlOutboxStore(db);
    const first = await store.claim({ relayId: 'dead-relay', limit: 10, leaseMs: 50 });
    expect(first.map((r) => r.eventId)).toEqual(['evt-1']);
    expect(await store.claim({ relayId: 'other', limit: 10, leaseMs: 60_000 })).toEqual([]);
    await new Promise((r) => setTimeout(r, 80));
    const again = await store.claim({ relayId: 'other', limit: 10, leaseMs: 60_000 });
    expect(again.map((r) => [r.eventId, r.attempts])).toEqual([['evt-1', 2]]);
    // The dead relay can no longer mark it.
    await store.markFailed('evt-1', 'dead-relay', 'late', null);
    expect((await statuses())[0][1]).toBe('PENDING');
  });
});

describe('integration checkpoints (per consumer, per event, per step)', () => {
  let db: PGlite;

  beforeAll(async () => {
    db = new PGlite();
    await db.exec(INTEGRATION_TABLES_SQL);
  });

  afterAll(async () => {
    await db.close();
  });

  it('a step can be claimed once per consumer; other consumers keep their own checkpoint', async () => {
    const key = { consumer: 'billing.payment-results', eventId: 'evt-1', step: 'done', correlationId: 'order-1' };
    expect(await claimCheckpoint(db, key)).toBe(true);
    expect(await claimCheckpoint(db, key)).toBe(false);
    expect(await claimCheckpoint(db, { ...key, consumer: 'ledger.order-settlements' })).toBe(true);
    await completeCheckpoint(db, { ...key, outcome: 'CAPTURED', detail: { credits: 20 } });
    expect(await readCheckpoint(db, key)).toMatchObject({ outcome: 'CAPTURED', detail: { credits: 20 }, correlationId: 'order-1' });
    const trail = await listCheckpoints(db, { correlationId: 'order-1' });
    expect(trail.map((c) => c.consumer).sort()).toEqual(['billing.payment-results', 'ledger.order-settlements']);
  });

  it('a claim inside a rolled-back transaction does not stick (the step is retried)', async () => {
    const key = { consumer: 'zatca.ledger-settlements', eventId: 'evt-2', step: 'issued' };
    await expect(
      db.transaction(async (tx) => {
        expect(await claimCheckpoint(tx, key)).toBe(true);
        throw new Error('boom');
      })
    ).rejects.toThrow('boom');
    expect(await readCheckpoint(db, key)).toBeNull();
    expect(await claimCheckpoint(db, key)).toBe(true);
  });
});
