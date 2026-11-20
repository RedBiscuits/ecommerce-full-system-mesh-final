/**
 * Observability and resilience of the tiered store:
 *
 * - Prometheus metrics (prom-client): every counter, the flush-latency
 *   histogram and the level gauges move exactly as traffic says they should.
 * - The L2 circuit breaker: 5 consecutive disk failures within 10 s open it,
 *   the store falls back to L1-only mode, a `tier.circuit` event announces
 *   the jittered retry, a half-open probe reconnects, and buffered writes
 *   are replayed instead of lost.
 *
 * Every test uses its own prom-client `Registry`, so no global state leaks
 * between tests and no prom-client mocking is needed. Disk failures come
 * from the scripted `FaultyStore` (unit level) or from a `vi.spyOn` on the
 * real `FsCacheStore` (end to end).
 */
import { Gauge, Registry } from 'prom-client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { CacheIntegrityError } from '../../../src/cache/errors';
import { SlruMemoryStore } from '../../../src/cache/slru-memory-store';
import type { TierCircuitOptions } from '../../../src/cache/tiered-cache-store';
import { TieredCacheStore } from '../../../src/cache/tiered-cache-store';
import { FrequencySketch } from '../../../src/cache/tiny-lfu';
import { createFileCache, type FileCache } from '../../../src/main';
import { CircuitOpenError } from '../../../src/resilience/errors';
import type {
  TelemetryEvent,
  TierCircuitEvent,
  TierFlushEvent,
} from '../../../src/telemetry/events';
import { PrometheusTelemetry } from '../../../src/telemetry/prometheus';
import { CompositeSink } from '../../../src/telemetry/sink';
import { FakeClock } from '../../helpers/fake-clock';
import { errno, FaultyStore } from '../../helpers/faulty-store';
import { makeTempDir, type TempDir } from '../../helpers/temp-dir';

const LIFETIME = { createdAt: 1_000, expiresAt: 2_000, staleUntil: 3_000 };
const body = (fill: number): Buffer => Buffer.alloc(100, fill);

// --- harness ------------------------------------------------------------------

interface Harness {
  readonly tiered: TieredCacheStore;
  readonly l1: SlruMemoryStore;
  readonly l2: FaultyStore;
  readonly clock: FakeClock;
  readonly registry: Registry;
  readonly prometheus: PrometheusTelemetry;
  readonly events: TelemetryEvent[];
}

const opened: TieredCacheStore[] = [];
afterEach(async () => {
  await Promise.allSettled(opened.splice(0).map((store) => store.close()));
});

function harness(
  options: {
    writeBehind?: boolean;
    maxPendingWrites?: number;
    circuit?: TierCircuitOptions | false;
    random?: () => number;
    clock?: FakeClock;
  } = {},
): Harness {
  const clock = options.clock ?? new FakeClock();
  const l1 = new SlruMemoryStore({
    maxMemoryBytes: 100_000,
    sketch: new FrequencySketch({ capacity: 256, seed: 1, sampleSize: 1e9 }),
  });
  const l2 = new FaultyStore();
  const registry = new Registry();
  const prometheus = new PrometheusTelemetry({ registry });
  const events: TelemetryEvent[] = [];
  const tiered = new TieredCacheStore({
    l1,
    l2,
    clock,
    random: options.random ?? ((): number => 0.5),
    telemetry: new CompositeSink([
      prometheus,
      { emit: (event): number => events.push(event) },
    ]),
    writeBehind:
      options.writeBehind === false
        ? false
        : {
            flushDelayMs: 60_000, // only explicit flushes
            maxPendingWrites: options.maxPendingWrites,
          },
    onFlushError: (): void => undefined,
    circuit: options.circuit,
  });
  prometheus.observe(tiered);
  opened.push(tiered);
  return { tiered, l1, l2, clock, registry, prometheus, events };
}

/**
 * The value of one series. `name` may carry a histogram suffix
 * (`_count`, `_sum`, `_bucket`). Labels match as a subset.
 */
async function sample(
  registry: Registry,
  name: string,
  labels: Record<string, string | number> = {},
): Promise<number> {
  const base = registry.getSingleMetric(name)
    ? name
    : name.replace(/_(count|sum|bucket)$/, '');
  const metric = registry.getSingleMetric(base);
  if (metric === undefined) {
    throw new Error(`no metric ${name}`);
  }
  const { values } = await metric.get();
  // Histogram samples carry their series name (`_bucket`, `_sum`, ...);
  // counter and gauge samples do not.
  const found = (
    values as Array<(typeof values)[number] & { metricName?: string }>
  ).find(
    (v) =>
      (v.metricName ?? base) === name &&
      Object.entries(labels).every(([k, want]) => v.labels[k] === want),
  );
  return found?.value ?? 0;
}

const circuitEvents = (events: TelemetryEvent[]): TierCircuitEvent[] =>
  events.filter((e): e is TierCircuitEvent => e.type === 'tier.circuit');

const flushEvents = (events: TelemetryEvent[]): TierFlushEvent[] =>
  events.filter((e): e is TierFlushEvent => e.type === 'tier.flush');

/**
 * Trips the breaker with 5 EIO write-through failures, 1 s apart. The clock
 * is left at the moment it opened.
 */
async function tripWithWrites(h: Harness): Promise<void> {
  h.l2.failNext('write', errno('EIO'), 5);
  for (let i = 0; i < 5; i++) {
    if (i > 0) {
      h.clock.advance(1_000);
    }
    await expect(
      h.tiered.write(`fail-${i}`, body(i), LIFETIME),
    ).rejects.toThrow('EIO');
  }
  expect(h.tiered.stats().circuit?.state).toBe('open');
}

// --- Prometheus metrics ---------------------------------------------------------

describe('Prometheus metrics', () => {
  it('counts L1 hits, write-buffer hits, L2 hits and misses separately', async () => {
    const { tiered, l1, l2, registry } = harness();
    await l2.write('on-disk', body(1), LIFETIME);
    await tiered.write('in-memory', body(2), LIFETIME);
    await tiered.write('buffered', body(3), LIFETIME);
    l1.discard('buffered'); // only the write buffer has it now

    await tiered.read('in-memory'); // L1
    await tiered.read('in-memory'); // L1
    await tiered.read('buffered'); // write buffer
    await tiered.read('on-disk'); // L2 (then promoted)
    await tiered.read('nowhere'); // miss

    const hits = (tier: string): Promise<number> =>
      sample(registry, 'file_cache_hits_total', { cache: 'default', tier });
    expect(await hits('l1')).toBe(2);
    expect(await hits('write_buffer')).toBe(1);
    expect(await hits('l2')).toBe(1);
    expect(
      await sample(registry, 'file_cache_misses_total', {
        reason: 'not_found',
      }),
    ).toBe(1);
    expect(
      await sample(registry, 'file_cache_promotions_total', {
        result: 'admitted',
      }),
    ).toBe(1);
  });

  it('counts readers that coalesced onto one L2 read', async () => {
    const { tiered, l2, registry } = harness();
    await l2.write('k', body(1), LIFETIME);
    await Promise.all([tiered.read('k'), tiered.read('k'), tiered.read('k')]);
    expect(l2.calls.read).toBe(1);
    expect(await sample(registry, 'file_cache_coalesced_reads_total')).toBe(2);
    expect(
      await sample(registry, 'file_cache_hits_total', { tier: 'l2' }),
    ).toBe(3);
  });

  it('counts writes by how they were accepted', async () => {
    const wb = harness({ maxPendingWrites: 1 });
    await wb.tiered.write('a', body(1), LIFETIME); // buffered
    await wb.tiered.write('a', body(2), LIFETIME); // coalesced
    await wb.tiered.write('b', body(3), LIFETIME); // ring full: fallback
    const mode = (m: string): Promise<number> =>
      sample(wb.registry, 'file_cache_writes_total', { mode: m });
    expect(await mode('buffered')).toBe(1);
    expect(await mode('coalesced')).toBe(1);
    expect(await mode('fallback')).toBe(1);

    const wt = harness({ writeBehind: false });
    await wt.tiered.write('c', body(4), LIFETIME);
    expect(
      await sample(wt.registry, 'file_cache_writes_total', {
        mode: 'write_through',
      }),
    ).toBe(1);
  });

  it('tracks the write-behind queue length, bytes and capacity at scrape time', async () => {
    const { tiered, registry } = harness({ maxPendingWrites: 8 });
    for (const k of ['a', 'b', 'c']) {
      await tiered.write(k, body(1), LIFETIME);
    }
    expect(await sample(registry, 'file_cache_write_behind_queue_length')).toBe(
      3,
    );
    expect(await sample(registry, 'file_cache_write_behind_queue_bytes')).toBe(
      300,
    );
    expect(
      await sample(registry, 'file_cache_write_behind_queue_capacity'),
    ).toBe(8);

    await tiered.flush();
    expect(await sample(registry, 'file_cache_write_behind_queue_length')).toBe(
      0,
    );
  });

  it('records every flush in the latency histogram, by outcome', async () => {
    const { tiered, l2, registry } = harness();
    for (const k of ['a', 'b', 'c']) {
      await tiered.write(k, body(1), LIFETIME);
    }
    await tiered.flush();
    expect(
      await sample(registry, 'file_cache_flush_duration_seconds_count', {
        outcome: 'ok',
      }),
    ).toBe(3);
    expect(
      await sample(registry, 'file_cache_flush_duration_seconds_sum', {
        outcome: 'ok',
      }),
    ).toBeGreaterThan(0);
    expect(
      await sample(registry, 'file_cache_flush_duration_seconds_bucket', {
        outcome: 'ok',
        le: '+Inf',
      }),
    ).toBe(3);

    // A non-disk error is final: `failed`. A disk error is retried: `requeued`.
    await tiered.write('bad', body(1), LIFETIME);
    l2.failNext('write', new Error('not a disk error'));
    await expect(tiered.flush()).rejects.toThrow(AggregateError);
    await tiered.write('flaky', body(1), LIFETIME);
    l2.failNext('write', errno('EIO'));
    await tiered.flush(); // EIO once, then the retry lands
    const flushes = (outcome: string): Promise<number> =>
      sample(registry, 'file_cache_flushes_total', { outcome });
    expect(await flushes('ok')).toBe(4);
    expect(await flushes('failed')).toBe(1);
    expect(await flushes('requeued')).toBe(1);
  });

  it('reports L1 size per segment', async () => {
    const { tiered, l1, registry } = harness();
    await tiered.write('a', body(1), LIFETIME);
    await tiered.write('b', body(1), LIFETIME);
    await tiered.read('b'); // second access: promoted to protected
    const stats = l1.stats();
    expect(
      await sample(registry, 'file_cache_l1_bytes', { segment: 'probation' }),
    ).toBe(stats.probationBytes);
    expect(
      await sample(registry, 'file_cache_l1_bytes', { segment: 'protected' }),
    ).toBe(stats.protectedBytes);
    expect(await sample(registry, 'file_cache_l1_entries')).toBe(2);
  });

  it('renders valid exposition text for a /metrics endpoint', async () => {
    const { tiered, prometheus } = harness();
    await tiered.write('a', body(1), LIFETIME);
    await tiered.read('a');
    const text = await prometheus.metrics();
    expect(prometheus.contentType).toMatch(/^text\/plain/);
    expect(text).toContain('# TYPE file_cache_hits_total counter');
    expect(text).toContain(
      'file_cache_hits_total{cache="default",tier="l1"} 1',
    );
    expect(text).toContain(
      '# TYPE file_cache_flush_duration_seconds histogram',
    );
    expect(text).toContain(
      'file_cache_circuit_state{cache="default",state="closed"} 1',
    );
  });

  it('lets several caches share one registry, told apart by the cache label', async () => {
    const registry = new Registry();
    const stores = ['a', 'b'].map((cacheName) => {
      const prometheus = new PrometheusTelemetry({ registry, cacheName });
      const tiered = new TieredCacheStore({
        l1: new SlruMemoryStore({ maxMemoryBytes: 100_000 }),
        l2: new FaultyStore(),
        telemetry: prometheus,
        writeBehind: { flushDelayMs: 60_000 },
      });
      prometheus.observe(tiered);
      opened.push(tiered);
      return tiered;
    });
    await stores[0]?.write('k', body(1), LIFETIME);
    await stores[0]?.read('k');
    expect(
      await sample(registry, 'file_cache_hits_total', { cache: 'a' }),
    ).toBe(1);
    expect(
      await sample(registry, 'file_cache_hits_total', { cache: 'b' }),
    ).toBe(0);
    expect(
      await sample(registry, 'file_cache_write_behind_queue_length', {
        cache: 'a',
      }),
    ).toBe(1);
    expect(
      await sample(registry, 'file_cache_write_behind_queue_length', {
        cache: 'b',
      }),
    ).toBe(0);
  });

  it('refuses a name already taken by a different metric type', () => {
    const registry = new Registry();
    new Gauge({
      name: 'file_cache_hits_total',
      help: 'not ours',
      registers: [registry],
    });
    expect(() => new PrometheusTelemetry({ registry })).toThrow(TypeError);
  });

  it('stops reading the store after dispose()', async () => {
    const { tiered, registry, prometheus } = harness();
    await tiered.write('a', body(1), LIFETIME);
    expect(await sample(registry, 'file_cache_write_behind_queue_length')).toBe(
      1,
    );
    prometheus.dispose();
    await tiered.flush();
    // No longer refreshed from the store: the last value stays.
    expect(await sample(registry, 'file_cache_write_behind_queue_length')).toBe(
      1,
    );
  });
});

// --- circuit breaker -----------------------------------------------------------

describe('L2 circuit breaker', () => {
  it('opens after 5 consecutive disk failures within 10 s and announces the jittered retry', async () => {
    const h = harness({ writeBehind: false, random: () => 0.5 });
    h.l2.failNext('write', errno('EIO'), 5);
    for (let i = 0; i < 4; i++) {
      await expect(h.tiered.write(`k${i}`, body(i), LIFETIME)).rejects.toThrow(
        'EIO',
      );
      h.clock.advance(1_000);
    }
    expect(h.tiered.stats().circuit?.state).toBe('closed');
    await expect(h.tiered.write('k4', body(4), LIFETIME)).rejects.toThrow(
      'EIO',
    );

    expect(h.tiered.stats().circuit?.state).toBe('open');
    const [opened] = circuitEvents(h.events);
    expect(opened).toMatchObject({
      state: 'open',
      consecutiveFailures: 5,
      error: expect.stringContaining('EIO'),
    });
    // 30 s cooldown + 0.5 × 20 % jitter = 33 s, and never before 30 s.
    expect(opened?.retryAt).toBe(h.clock.now() + 33_000);

    expect(
      await sample(h.registry, 'file_cache_circuit_state', { state: 'open' }),
    ).toBe(1);
    expect(
      await sample(h.registry, 'file_cache_circuit_state', { state: 'closed' }),
    ).toBe(0);
    expect(
      await sample(h.registry, 'file_cache_circuit_transitions_total', {
        state: 'open',
      }),
    ).toBe(1);
  });

  it('does not open when the failures are spread beyond the 10 s window', async () => {
    const h = harness({ writeBehind: false });
    h.l2.failNext('write', errno('EIO'), 6);
    for (let i = 0; i < 6; i++) {
      await expect(h.tiered.write(`k${i}`, body(i), LIFETIME)).rejects.toThrow(
        'EIO',
      );
      h.clock.advance(3_000); // 5 failures always span more than 10 s
    }
    expect(h.tiered.stats().circuit?.state).toBe('closed');
  });

  it('opens on ENOSPC even while reads keep succeeding', async () => {
    const h = harness({ writeBehind: false });
    await h.l2.write('readable', body(1), LIFETIME);
    h.l2.failNext('write', errno('ENOSPC'), 5);
    for (let i = 0; i < 5; i++) {
      h.l1.discard('readable');
      await expect(h.tiered.read('readable')).resolves.toBeDefined(); // disk read OK
      await expect(h.tiered.write(`k${i}`, body(i), LIFETIME)).rejects.toThrow(
        'ENOSPC',
      );
    }
    expect(h.tiered.stats().l2Hits).toBe(5); // reads succeeded in between
    expect(h.tiered.stats().circuit?.state).toBe('open');
  });

  it('is not tripped by errors that are not disk failures', async () => {
    const h = harness({ writeBehind: false });
    await h.l2.write('k', body(1), LIFETIME);
    h.l2.failNext('write', new Error('bug in a decorator'), 10);
    h.l2.failNext('read', new CacheIntegrityError('/x', 'bad magic'), 10);
    for (let i = 0; i < 10; i++) {
      await expect(h.tiered.write(`w${i}`, body(i), LIFETIME)).rejects.toThrow(
        'bug in a decorator',
      );
      await expect(h.tiered.read('k')).rejects.toThrow(CacheIntegrityError);
    }
    expect(h.tiered.stats().circuit?.state).toBe('closed');
  });

  it('L1-only mode: serves memory, never touches L2, buffers writes, refuses deletes', async () => {
    const h = harness({ writeBehind: false });
    await h.tiered.write('cached', body(7), LIFETIME); // in L1 and on disk
    await h.l2.write('disk-only', body(8), LIFETIME);
    await tripWithWrites(h);
    const calls = { ...h.l2.calls };

    // Reads: memory still answers; a disk-only key is a miss, L2 untouched.
    expect((await h.tiered.read('cached'))?.value[0]).toBe(7);
    await expect(h.tiered.read('disk-only')).resolves.toBeUndefined();
    await expect(h.tiered.readMetadata('disk-only')).resolves.toBeUndefined();
    expect(h.l2.calls.read).toBe(calls.read);
    expect(h.l2.calls.readMetadata).toBe(calls.readMetadata);
    expect(h.tiered.stats().l2Reads).toBe(0); // not even attempted
    expect(
      await sample(h.registry, 'file_cache_misses_total', {
        reason: 'short_circuited',
      }),
    ).toBe(1);

    // Writes: accepted and readable, buffered, not sent to L2.
    await h.tiered.write('new', body(9), LIFETIME);
    expect((await h.tiered.read('new'))?.value[0]).toBe(9);
    expect(h.l2.calls.write).toBe(calls.write);
    expect(h.tiered.stats().pendingWrites).toBe(1);

    // Deletes can't be made durable: refused, nothing changed.
    await expect(h.tiered.delete('cached')).rejects.toBeInstanceOf(
      CircuitOpenError,
    );
    await expect(
      h.tiered.deleteIf('cached', () => true),
    ).rejects.toBeInstanceOf(CircuitOpenError);
    await expect(h.tiered.clear()).rejects.toBeInstanceOf(CircuitOpenError);
    expect((await h.tiered.read('cached'))?.value[0]).toBe(7);
    expect(h.l2.calls.delete + h.l2.calls.deleteIf + h.l2.calls.clear).toBe(0);
  });

  it('half-opens after the cooldown; a buffered write is the probe, then the buffer drains', async () => {
    const h = harness({ writeBehind: false, random: () => 0.5 });
    await tripWithWrites(h);
    for (const k of ['x', 'y', 'z']) {
      await h.tiered.write(k, body(1), LIFETIME); // buffered while open
    }

    h.clock.advance(32_999);
    expect(h.tiered.stats().circuit?.state).toBe('open');
    h.clock.advance(1); // 33 s: cooldown + jitter
    expect(h.tiered.stats().circuit?.state).toBe('half-open');
    await vi.waitFor(() =>
      expect(h.tiered.stats()).toMatchObject({
        circuit: { state: 'closed' },
        pendingWrites: 0,
      }),
    );

    expect(circuitEvents(h.events).map((e) => e.state)).toEqual([
      'open',
      'half-open',
      'closed',
    ]);
    const flushes = flushEvents(h.events);
    expect(flushes[0]).toMatchObject({ outcome: 'ok', probe: true });
    expect(flushes.filter((f) => f.outcome === 'ok')).toHaveLength(3);
    for (const k of ['x', 'y', 'z']) {
      expect(h.l2.entries.has(k)).toBe(true); // replayed, not lost
    }
    // Back to normal: write-through reaches L2 before resolving again.
    await h.tiered.write('after', body(1), LIFETIME);
    expect(h.l2.entries.has('after')).toBe(true);
  });

  it('a failed probe re-opens with a fresh jittered cooldown and keeps the write', async () => {
    const rolls = [0, 1];
    const h = harness({
      writeBehind: false,
      random: () => rolls.shift() ?? 0,
    });
    await tripWithWrites(h); // jitter roll 0: retry in exactly 30 s
    await h.tiered.write('buffered', body(5), LIFETIME);

    h.l2.failNext('write', errno('EIO'));
    h.clock.advance(30_000);
    expect(h.tiered.stats().circuit?.state).toBe('half-open');
    await vi.waitFor(() =>
      expect(h.tiered.stats().circuit?.state).toBe('open'),
    );
    // Roll 1: 30 s + 20 % = 36 s from now.
    expect(h.tiered.stats().circuit?.retryAt).toBe(h.clock.now() + 36_000);
    expect(circuitEvents(h.events).map((e) => e.state)).toEqual([
      'open',
      'half-open',
      'open',
    ]);
    expect(flushEvents(h.events).at(-1)).toMatchObject({
      outcome: 'requeued',
      probe: true,
    });
    expect((await h.tiered.read('buffered'))?.value[0]).toBe(5); // still here
    expect(h.l2.entries.has('buffered')).toBe(false);

    h.clock.advance(36_000);
    h.tiered.stats(); // notices the cooldown ended
    await vi.waitFor(() => expect(h.l2.entries.has('buffered')).toBe(true));
  });

  it('with nothing buffered, the probe is a cheap metadata read', async () => {
    const h = harness({ writeBehind: false, random: () => 0 });
    await tripWithWrites(h);
    const before = h.l2.calls.readMetadata;
    h.clock.advance(30_000);
    h.tiered.stats();
    await vi.waitFor(() =>
      expect(h.tiered.stats().circuit?.state).toBe('closed'),
    );
    expect(h.l2.calls.readMetadata).toBe(before + 1);
  });

  it('announces half-open on its own timer and reconnects with no traffic', async () => {
    const events: TelemetryEvent[] = [];
    const l2 = new FaultyStore();
    const tiered = new TieredCacheStore({
      l1: new SlruMemoryStore({ maxMemoryBytes: 100_000 }),
      l2,
      writeBehind: false,
      circuit: { failureThreshold: 2, cooldownMs: 40, jitterRatio: 0 },
      telemetry: { emit: (e): number => events.push(e) },
    });
    opened.push(tiered);
    l2.failNext('write', errno('EIO'), 2);
    await expect(tiered.write('a', body(1), LIFETIME)).rejects.toThrow('EIO');
    await expect(tiered.write('b', body(1), LIFETIME)).rejects.toThrow('EIO');
    await tiered.write('c', body(1), LIFETIME); // buffered while open

    // Nobody touches the store: the timer alone drives recovery.
    await vi.waitFor(
      () =>
        expect(circuitEvents(events).map((e) => e.state)).toEqual([
          'open',
          'half-open',
          'closed',
        ]),
      { timeout: 2_000 },
    );
    await vi.waitFor(() => expect(l2.entries.has('c')).toBe(true));
  });

  it('write-behind: buffered writes that hit disk errors are requeued, not dropped', async () => {
    const h = harness({ random: () => 0 });
    await h.tiered.write('k', body(1), LIFETIME);
    h.l2.failNext('write', errno('EIO'), 5);
    // Five failed attempts trip the circuit. flush() reports it.
    await expect(h.tiered.flush()).rejects.toBeInstanceOf(CircuitOpenError);
    expect(h.tiered.stats()).toMatchObject({
      circuit: { state: 'open' },
      pendingWrites: 1,
      requeued: 5,
      flushFailures: 0,
    });
    expect((await h.tiered.read('k'))?.value[0]).toBe(1);

    h.clock.advance(30_000);
    h.tiered.stats();
    await vi.waitFor(() => expect(h.l2.entries.has('k')).toBe(true));
    expect(
      await sample(h.registry, 'file_cache_flushes_total', {
        outcome: 'requeued',
      }),
    ).toBe(5);
  });

  it('keeps writes in memory only when the buffer is full while open', async () => {
    const h = harness({ maxPendingWrites: 1 });
    await h.tiered.write('warmup', body(0), LIFETIME);
    await h.tiered.flush();
    h.l2.failNext('write', errno('EIO'), 5);
    await h.tiered.write('q', body(1), LIFETIME);
    await expect(h.tiered.flush()).rejects.toBeInstanceOf(CircuitOpenError);

    await h.tiered.write('overflow', body(2), LIFETIME); // ring is full
    expect((await h.tiered.read('overflow'))?.value[0]).toBe(2);
    expect(h.tiered.stats().l1OnlyWrites).toBe(1);
    expect(
      await sample(h.registry, 'file_cache_writes_total', { mode: 'l1_only' }),
    ).toBe(1);
  });

  it('close() makes a final attempt past an open circuit, and rejects if the disk still fails', async () => {
    const ok = harness({ writeBehind: false });
    await tripWithWrites(ok);
    await ok.tiered.write('p', body(1), LIFETIME);
    await ok.tiered.close(); // the disk works again: drained despite the circuit
    expect(ok.l2.entries.has('p')).toBe(true);

    const bad = harness({ writeBehind: false });
    await tripWithWrites(bad);
    await bad.tiered.write('lost', body(1), LIFETIME);
    bad.l2.failNext('write', errno('EIO'));
    await expect(bad.tiered.close()).rejects.toThrow(/"lost"/);
    expect(
      await sample(bad.registry, 'file_cache_flushes_total', {
        outcome: 'failed',
      }),
    ).toBe(1);
  });

  it('can be disabled', async () => {
    const h = harness({ writeBehind: false, circuit: false });
    h.l2.failNext('write', errno('EIO'), 20);
    for (let i = 0; i < 20; i++) {
      await expect(h.tiered.write(`k${i}`, body(1), LIFETIME)).rejects.toThrow(
        'EIO',
      );
    }
    expect(h.tiered.stats().circuit).toBeUndefined();
  });
});

// --- end to end through createFileCache ------------------------------------------

describe('createFileCache: Prometheus + tier circuit, end to end', () => {
  let dir: TempDir;
  let fileCache: FileCache | undefined;

  beforeEach(async () => {
    dir = await makeTempDir();
  });

  afterEach(async () => {
    vi.restoreAllMocks();
    await fileCache?.close().catch(() => undefined);
    fileCache = undefined;
    await dir.cleanup();
  });

  it('a failing disk opens the circuit; the cache degrades to memory but stays ready', async () => {
    const registry = new Registry();
    fileCache = await createFileCache({
      directory: dir.path,
      store: { fsync: false },
      sweeper: false,
      ttlJitterRatio: 0,
      resilience: { retry: { maxAttempts: 1 } },
      onStoreError: (): void => undefined,
      prometheus: { registry, cacheName: 'api' },
      tiering: {
        memoryBytes: 1_000_000,
        circuit: { failureThreshold: 3, cooldownMs: 60_000 },
      },
    });
    const { cache } = fileCache;
    await cache.put('before', 'cached');

    // The disk starts failing with EIO.
    const write = vi
      .spyOn(fileCache.fsStore, 'write')
      .mockRejectedValue(errno('EIO'));
    for (let i = 0; i < 3; i++) {
      await expect(cache.put(`k${i}`, 'v')).rejects.toThrow(/failed/);
    }

    // One breaker only: the resilient decorator's own is off with tiering.
    expect(fileCache.health().store?.circuit).toBeUndefined();
    expect(fileCache.health().tiering?.circuit?.state).toBe('open');

    // L1-only mode: memory still serves and accepts writes.
    expect((await cache.get('before'))?.value.toString()).toBe('cached');
    await cache.put('during', 'accepted');
    expect((await cache.get('during'))?.value.toString()).toBe('accepted');
    const writesWhileOpen = write.mock.calls.length;
    expect(writesWhileOpen).toBe(3); // nothing new reached the disk

    // Health: degraded, not down, so it stays in rotation.
    const report = await fileCache.diagnostics();
    expect(report.ready).toBe(true);
    expect(report.status).toBe('degraded');
    expect(report.checks.find((c) => c.name === 'tiering')).toMatchObject({
      status: 'warn',
      details: { circuit: 'open', pendingWrites: 1 },
    });
    // In-process alerts: a warning (degraded), not the critical store alert.
    expect(fileCache.metrics?.snapshot().store.tierCircuitState).toBe('open');
    expect(report.alerts).toContainEqual(
      expect.objectContaining({
        code: 'tier.circuit-open',
        severity: 'warning',
      }),
    );
    expect(report.alerts.map((a) => a.code)).not.toContain(
      'store.circuit-open',
    );

    // Prometheus sees it all, under the configured cache label.
    expect(fileCache.prometheus?.registry).toBe(registry);
    const text = await registry.metrics();
    expect(text).toContain(
      'file_cache_circuit_state{cache="api",state="open"} 1',
    );
    expect(text).toContain(
      'file_cache_write_behind_queue_length{cache="api"} 1',
    );
    expect(text).toContain('file_cache_hits_total{cache="api",tier="l1"} 2');

    // The disk recovers; shutdown drains what was buffered while open.
    write.mockRestore();
    await fileCache.close();
    const { FsCacheStore } =
      await import('../../../src/cache/fs-cache-store');
    const disk = await FsCacheStore.open({ directory: dir.path });
    expect((await disk.read('during'))?.value.toString()).toBe('accepted');
  });

  it('no exporter unless asked for', async () => {
    fileCache = await createFileCache({ directory: dir.path, sweeper: false });
    expect(fileCache.prometheus).toBeUndefined();
  });
});
