import { describe, expect, it } from 'vitest';

import { evaluateAlerts } from '../../../src/telemetry/alerts';
import type { TelemetryEvent } from '../../../src/telemetry/events';
import { MetricsCollector } from '../../../src/telemetry/metrics';
import { FakeClock } from '../../helpers/fake-clock';

const at = 1_700_000_000_000;

function collect(events: TelemetryEvent[]): MetricsCollector {
  const m = new MetricsCollector({
    clock: new FakeClock(at),
    eventLoop: false,
  });
  for (const e of events) {
    m.emit(e);
  }
  return m;
}

const request = (status: number, durationMs = 1): TelemetryEvent => ({
  type: 'http.request',
  at,
  method: 'GET',
  route: 'GET /r',
  status,
  durationMs,
  cacheStatus: undefined,
  bytes: 1,
  aborted: false,
  clientKey: undefined,
});

describe('evaluateAlerts', () => {
  it('an open tier circuit is a warning (L1-only mode), never the critical store alert', () => {
    const m = collect([
      {
        type: 'tier.circuit',
        at,
        state: 'open',
        retryAt: at + 30_000,
        consecutiveFailures: 5,
        error: 'Error: EIO [EIO]',
      },
    ]);
    const alerts = evaluateAlerts(m.snapshot());
    expect(alerts).toEqual([
      expect.objectContaining({
        code: 'tier.circuit-open',
        severity: 'warning',
      }),
    ]);
  });

  it('is quiet on a healthy, well-sampled system', () => {
    const m = collect(Array.from({ length: 100 }, () => request(200)));
    expect(evaluateAlerts(m.snapshot())).toEqual([]);
  });

  it('stays quiet below the sample floor, then fires on a high 5xx ratio', () => {
    const few = collect([request(500), request(500)]);
    expect(evaluateAlerts(few.snapshot())).toEqual([]);
    const many = collect([
      ...Array.from({ length: 90 }, () => request(200)),
      ...Array.from({ length: 10 }, () => request(503)),
    ]);
    const alerts = evaluateAlerts(many.snapshot());
    expect(alerts).toContainEqual(
      expect.objectContaining({
        code: 'http.error-ratio',
        severity: 'critical',
        value: 0.1,
      }),
    );
  });

  it('flags slow routes, slow cache ops and event-loop lag', () => {
    const m = collect([
      ...Array.from({ length: 50 }, () => request(200, 3_000)),
      ...Array.from({ length: 50 }, (): TelemetryEvent => ({
        type: 'cache.lookup',
        at,
        operation: 'get',
        key: 'k',
        outcome: 'hit',
        freshness: 'fresh',
        durationMs: 400,
      })),
    ]);
    const snapshot = {
      ...m.snapshot(),
      process: {
        ...m.snapshot().process,
        eventLoop: { p50Ms: 10, p99Ms: 500, maxMs: 600 },
      },
    };
    const codes = evaluateAlerts(snapshot).map((a) => a.code);
    expect(codes).toEqual(
      expect.arrayContaining([
        'http.route-latency',
        'cache.latency',
        'process.event-loop',
      ]),
    );
  });

  it('flags cache hit ratio, loader failures, corruption, store failures and capacity', () => {
    const m = collect([
      ...Array.from({ length: 200 }, (): TelemetryEvent => ({
        type: 'cache.lookup',
        at,
        operation: 'get',
        key: 'k',
        outcome: 'miss',
        freshness: undefined,
        durationMs: 1,
      })),
      ...Array.from({ length: 50 }, (_, i): TelemetryEvent => ({
        type: 'cache.load',
        at,
        key: 'k',
        background: false,
        durationMs: 1,
        outcome: i < 10 ? 'error' : 'stored',
        source: 'loader',
        bytes: 1,
        error: i < 10 ? 'boom' : undefined,
      })),
      { type: 'cache.corrupt-entry', at, path: '/p', reason: 'bad' },
      {
        type: 'store.failure',
        at,
        operation: 'write',
        kind: 'permission',
        attempts: 1,
        error: 'EROFS',
      },
      { type: 'store.capacity-exhausted', at, reclaimed: false },
      { type: 'store.circuit', at, state: 'open', error: 'EIO' },
    ]);
    const alerts = evaluateAlerts(m.snapshot());
    const codes = alerts.map((a) => a.code);
    expect(codes).toEqual(
      expect.arrayContaining([
        'cache.hit-ratio',
        'cache.load-errors',
        'cache.corruption',
        'store.failures',
        'store.capacity',
        'store.circuit-open',
      ]),
    );
    expect(alerts.find((a) => a.code === 'cache.load-errors')).toMatchObject({
      severity: 'critical',
      value: 0.2,
    });
  });

  it('reports rate-limit pressure with the top offender', () => {
    const m = collect(
      Array.from({ length: 60 }, (_, i): TelemetryEvent => ({
        type: 'rate-limit',
        at,
        key: i % 3 === 0 ? 'ok' : 'net:10.0.0.1',
        admitted: i % 3 === 0,
        remaining: 0,
        retryAfterMs: 1,
        route: undefined,
      })),
    );
    const alert = evaluateAlerts(m.snapshot()).find(
      (a) => a.code === 'rate-limit.pressure',
    );
    expect(alert?.message).toContain('net:10.0.0.1');
  });

  it('honours custom thresholds', () => {
    const m = collect(Array.from({ length: 10 }, () => request(500)));
    expect(
      evaluateAlerts(m.snapshot(), { minSamples: 5, maxHttpErrorRatio: 0.5 }),
    ).toHaveLength(1);
    expect(
      evaluateAlerts(m.snapshot(), { minSamples: 5, maxHttpErrorRatio: 1 }),
    ).toHaveLength(0);
  });
});
