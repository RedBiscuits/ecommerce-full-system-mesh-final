import { afterEach, describe, expect, it } from 'vitest';

import type { TelemetryEvent } from '../../../src/telemetry/events';
import {
  LATENCY_BUCKETS_MS,
  LatencyHistogram,
  MetricsCollector,
} from '../../../src/telemetry/metrics';
import { FakeClock } from '../../helpers/fake-clock';

const at = 1_700_000_000_000;
const lookup = (
  outcome: 'hit' | 'stale' | 'miss',
  durationMs = 1,
): TelemetryEvent => ({
  type: 'cache.lookup',
  at,
  operation: 'get',
  key: 'k',
  outcome,
  freshness:
    outcome === 'hit' ? 'fresh' : outcome === 'stale' ? 'stale' : undefined,
  durationMs,
});
const request = (
  route: string,
  status: number,
  durationMs = 5,
  extra: Partial<Extract<TelemetryEvent, { type: 'http.request' }>> = {},
): TelemetryEvent => ({
  type: 'http.request',
  at,
  method: 'GET',
  route,
  status,
  durationMs,
  cacheStatus: undefined,
  bytes: 10,
  aborted: false,
  clientKey: undefined,
  ...extra,
});

describe('LatencyHistogram', () => {
  it('tracks count/sum/min/max/mean and bucket-estimated percentiles', () => {
    const h = new LatencyHistogram();
    for (const v of [0.5, 1, 3, 7, 40, 900]) {
      h.record(v);
    }
    const s = h.snapshot();
    expect(s.count).toBe(6);
    expect(s.minMs).toBe(0.5);
    expect(s.maxMs).toBe(900);
    expect(s.meanMs).toBeCloseTo(158.58, 1);
    expect(s.p50Ms).toBe(5); // 3rd value (3) sits in the ≤5 bucket
    expect(s.p99Ms).toBe(1_000);
    expect(s.buckets).toHaveLength(LATENCY_BUCKETS_MS.length + 1);
    expect(s.buckets.at(-1)).toBe(6);
  });

  it('is empty-safe and clamps negatives', () => {
    const h = new LatencyHistogram();
    expect(h.snapshot()).toMatchObject({
      count: 0,
      minMs: 0,
      maxMs: 0,
      meanMs: 0,
      p50Ms: 0,
      p99Ms: 0,
    });
    h.record(-5);
    expect(h.snapshot().minMs).toBe(0);
  });

  it('values beyond the last bound land in the +∞ bucket and report max as p99', () => {
    const h = new LatencyHistogram();
    h.record(50_000);
    expect(h.snapshot().p99Ms).toBe(50_000);
  });
});

describe('MetricsCollector', () => {
  let collector: MetricsCollector | undefined;

  afterEach(() => {
    collector?.close();
  });

  const make = (options = {}): MetricsCollector => {
    collector = new MetricsCollector({
      clock: new FakeClock(at),
      eventLoop: false,
      ...options,
    });
    return collector;
  };

  it('aggregates cache hits, misses, stale and the hit ratio', () => {
    const m = make();
    for (const outcome of ['hit', 'hit', 'stale', 'miss'] as const) {
      m.emit(lookup(outcome));
    }
    const c = m.snapshot().cache;
    expect(c).toMatchObject({
      lookups: 4,
      hits: 2,
      stale: 1,
      misses: 1,
      hitRatio: 0.75,
    });
    expect(c.latency['cache.get']?.count).toBe(4);
    expect(make().snapshot().cache.hitRatio).toBeNull();
  });

  it('aggregates loads, writes, store errors, corruption and sweeps into counters and recent errors', () => {
    const m = make();
    m.emit({
      type: 'cache.load',
      at,
      key: 'k',
      background: false,
      durationMs: 10,
      outcome: 'stored',
      source: 'loader',
      bytes: 100,
      error: undefined,
    });
    m.emit({
      type: 'cache.load',
      at,
      key: 'k',
      background: true,
      durationMs: 10,
      outcome: 'unstored',
      source: 'loader',
      bytes: 50,
      error: undefined,
    });
    m.emit({
      type: 'cache.load',
      at,
      key: 'k',
      background: false,
      durationMs: 10,
      outcome: 'error',
      source: undefined,
      bytes: undefined,
      error: 'Error: origin down',
    });
    m.emit({
      type: 'cache.write',
      at,
      operation: 'put',
      key: 'k',
      durationMs: 2,
      bytes: 5,
      error: undefined,
    });
    m.emit({
      type: 'cache.write',
      at,
      operation: 'delete',
      key: 'k',
      durationMs: 2,
      bytes: undefined,
      error: 'Error: EIO',
    });
    m.emit({
      type: 'cache.store-error',
      at,
      operation: 'read',
      key: 'k',
      error: 'Error: EIO',
    });
    m.emit({
      type: 'cache.corrupt-entry',
      at,
      path: '/p',
      reason: 'checksum mismatch',
    });
    m.emit({
      type: 'cache.sweep',
      at,
      report: {
        scanned: 10,
        expired: 3,
        evicted: 1,
        failed: 0,
        liveBytes: 1,
        durationMs: 5,
        skipped: false,
      },
    });

    const s = m.snapshot();
    expect(s.cache).toMatchObject({
      loads: 3,
      loadErrors: 1,
      loadsUnstored: 1,
      backgroundLoads: 1,
      bytesLoaded: 150,
      writes: 2,
      writeErrors: 1,
      storeErrors: 1,
      corruptEntries: 1,
      lastSweep: { at, expired: 3, evicted: 1, failed: 0, skipped: false },
    });
    expect(s.recentErrors.map((e) => e.source)).toEqual([
      'cache.load',
      'cache.delete',
      'store.read',
      'corrupt-entry',
    ]);
  });

  it('aggregates resilience events and the circuit state', () => {
    const m = make();
    m.emit({
      type: 'store.retry',
      at,
      operation: 'read',
      attempt: 1,
      delayMs: 20,
      error: 'EMFILE',
    });
    m.emit({
      type: 'store.failure',
      at,
      operation: 'write',
      kind: 'permission',
      attempts: 1,
      error: 'EROFS',
    });
    m.emit({ type: 'store.capacity-exhausted', at, reclaimed: false });
    m.emit({ type: 'store.circuit', at, state: 'open', error: 'EIO' });
    m.emit({ type: 'store.circuit', at, state: 'closed', error: undefined });
    expect(m.snapshot().store).toEqual({
      retries: 1,
      failures: 1,
      capacityEvents: 1,
      circuitOpens: 1,
      circuitState: 'closed',
      tierCircuitOpens: 0,
      tierCircuitState: undefined,
    });
  });

  it('tracks the tier circuit apart from the store circuit', () => {
    const m = make();
    const tier = (state: 'open' | 'half-open' | 'closed'): TelemetryEvent => ({
      type: 'tier.circuit',
      at,
      state,
      retryAt: undefined,
      consecutiveFailures: 5,
      error: state === 'open' ? 'Error: EIO [EIO]' : undefined,
    });
    m.emit(tier('open'));
    expect(m.snapshot().store).toMatchObject({
      tierCircuitState: 'open',
      tierCircuitOpens: 1,
      circuitState: undefined, // the store circuit is untouched
      circuitOpens: 0,
    });
    m.emit(tier('half-open'));
    m.emit(tier('closed'));
    expect(m.snapshot().store.tierCircuitState).toBe('closed');
    expect(m.snapshot().recentErrors.at(-1)).toMatchObject({
      source: 'tier.circuit',
    });
  });

  it('ranks the most throttled clients with a bounded table', () => {
    const m = make({ maxLimitedKeys: 3 });
    const decision = (key: string, admitted: boolean): TelemetryEvent => ({
      type: 'rate-limit',
      at,
      key,
      admitted,
      remaining: 0,
      retryAfterMs: 100,
      route: undefined,
    });
    for (let i = 0; i < 5; i += 1) m.emit(decision('abuser', false));
    for (let i = 0; i < 2; i += 1) m.emit(decision('bursty', false));
    m.emit(decision('once', false));
    m.emit(decision('new', false)); // evicts the least-throttled ('once')
    m.emit(decision('good', true));

    const r = m.snapshot().rateLimit;
    expect(r).toMatchObject({
      decisions: 10,
      admitted: 1,
      limited: 9,
      limitedRatio: 0.9,
    });
    expect(r.topLimited.map((t) => t.key)).toEqual(['abuser', 'bursty', 'new']);
  });

  it('aggregates HTTP requests per route with status classes, cache outcomes and latency', () => {
    const m = make();
    m.emit(request('GET /users/:id', 200, 3, { cacheStatus: 'HIT' }));
    m.emit(request('GET /users/:id', 200, 30, { cacheStatus: 'MISS' }));
    m.emit(request('GET /users/:id', 404, 1));
    m.emit(request('GET /users/:id', 500, 1_500));
    m.emit(request('POST /orders', 201, 8, { aborted: true }));

    const h = m.snapshot().http;
    expect(h).toMatchObject({
      requests: 5,
      errors: 1,
      aborted: 1,
      errorRatio: 0.2,
    });
    expect(h.routes['GET /users/:id']).toMatchObject({
      requests: 4,
      errors: 1,
      statuses: { '2xx': 2, '4xx': 1, '5xx': 1 },
      cache: { HIT: 1, MISS: 1 },
    });
    expect(h.routes['GET /users/:id']?.latency.p99Ms).toBe(2_500);
    expect(h.routes['POST /orders']).toMatchObject({ requests: 1, aborted: 1 });
  });

  it('folds routes beyond maxRoutes into "other" so cardinality stays bounded', () => {
    const m = make({ maxRoutes: 2 });
    m.emit(request('GET /a', 200));
    m.emit(request('GET /b', 200));
    m.emit(request('GET /c', 200));
    m.emit(request('GET /d', 200));
    const routes = m.snapshot().http.routes;
    expect(Object.keys(routes).sort()).toEqual(['GET /a', 'GET /b', 'other']);
    expect(routes['other']?.requests).toBe(2);
  });

  it('caps the recent-error ring', () => {
    const m = make({ recentErrors: 2 });
    for (let i = 0; i < 5; i += 1) {
      m.emit({
        type: 'cache.store-error',
        at: at + i,
        operation: 'read',
        key: 'k',
        error: `e${i}`,
      });
    }
    expect(m.snapshot().recentErrors.map((e) => e.message)).toEqual([
      'e3',
      'e4',
    ]);
  });

  it('reports process metrics and the event-loop histogram when enabled', async () => {
    const m = make({ eventLoop: true });
    await new Promise((r) => setTimeout(r, 60));
    const p = m.snapshot().process;
    expect(p.rssBytes).toBeGreaterThan(0);
    expect(p.heapUsedBytes).toBeGreaterThan(0);
    expect(p.eventLoop).toBeDefined();
    expect(p.eventLoop?.p99Ms).toBeGreaterThanOrEqual(0);
    expect(make().snapshot().process.eventLoop).toBeUndefined();
  });

  it('reset() zeroes everything; close() is idempotent', () => {
    const m = make({ eventLoop: true });
    m.emit(lookup('hit'));
    m.emit(request('GET /a', 500));
    m.reset();
    const s = m.snapshot();
    expect(s.cache.lookups).toBe(0);
    expect(s.http.requests).toBe(0);
    expect(s.recentErrors).toEqual([]);
    m.close();
    m.close();
    expect(m.snapshot().process.eventLoop).toBeUndefined();
  });
});
