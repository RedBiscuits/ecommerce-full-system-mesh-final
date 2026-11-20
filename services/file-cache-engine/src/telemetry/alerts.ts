import type { MetricsSnapshot } from './metrics';

export type AlertSeverity = 'warning' | 'critical';

export interface Alert {
  readonly code: string;
  readonly severity: AlertSeverity;
  readonly message: string;
  readonly value: number | string;
  readonly threshold: number | string;
}

export interface AlertThresholds {
  /** 5xx share of HTTP requests. Default: `0.05`. */
  readonly maxHttpErrorRatio?: number;
  /** Minimum cache hit ratio, evaluated after `minLookupsForHitRatio`. Default: `0.2`. */
  readonly minHitRatio?: number;
  readonly minLookupsForHitRatio?: number;
  /** Loader failure share. Default: `0.1`. */
  readonly maxLoadErrorRatio?: number;
  /** Share of rate-limit decisions that were rejections. Default: `0.5`. */
  readonly maxLimitedRatio?: number;
  /** Per-route p99 latency (ms). Default: `2000`. */
  readonly maxRouteP99Ms?: number;
  /** Cache lookup p99 latency (ms). Default: `250`. */
  readonly maxCacheP99Ms?: number;
  /** Event-loop delay p99 (ms). Default: `200`. */
  readonly maxEventLoopP99Ms?: number;
  /** Any corrupt entry is worth a look. Default: `0`. */
  readonly maxCorruptEntries?: number;
  /** Store failures after retries. Default: `0`. */
  readonly maxStoreFailures?: number;
  /** Minimum requests/lookups before ratio alerts fire. Default: `50`. */
  readonly minSamples?: number;
}

const DEFAULTS: Required<AlertThresholds> = {
  maxHttpErrorRatio: 0.05,
  minHitRatio: 0.2,
  minLookupsForHitRatio: 200,
  maxLoadErrorRatio: 0.1,
  maxLimitedRatio: 0.5,
  maxRouteP99Ms: 2_000,
  maxCacheP99Ms: 250,
  maxEventLoopP99Ms: 200,
  maxCorruptEntries: 0,
  maxStoreFailures: 0,
  minSamples: 50,
};

/**
 * Turns a metrics snapshot into actionable alerts. Pure and cheap: call it
 * from a diagnostics endpoint, a scheduled check, or a test.
 */
export function evaluateAlerts(
  snapshot: MetricsSnapshot,
  thresholds: AlertThresholds = {},
): Alert[] {
  const t = { ...DEFAULTS, ...thresholds };
  const alerts: Alert[] = [];
  const push = (
    code: string,
    severity: AlertSeverity,
    message: string,
    value: number | string,
    threshold: number | string,
  ): void => {
    alerts.push({ code, severity, message, value, threshold });
  };

  if (snapshot.store.circuitState === 'open') {
    push(
      'store.circuit-open',
      'critical',
      'Cache store circuit is open; requests bypass the cache',
      'open',
      'closed',
    );
  }
  if (snapshot.store.tierCircuitState === 'open') {
    push(
      'tier.circuit-open',
      'warning',
      'Disk circuit is open; serving from memory only (L1-only mode)',
      'open',
      'closed',
    );
  }
  if (snapshot.store.failures > t.maxStoreFailures) {
    push(
      'store.failures',
      'warning',
      'Store operations failed after retries',
      snapshot.store.failures,
      t.maxStoreFailures,
    );
  }
  if (snapshot.store.capacityEvents > 0) {
    push(
      'store.capacity',
      'critical',
      'Disk full / quota exhausted was observed',
      snapshot.store.capacityEvents,
      0,
    );
  }
  if (snapshot.cache.corruptEntries > t.maxCorruptEntries) {
    push(
      'cache.corruption',
      'warning',
      'Corrupt cache entries were quarantined',
      snapshot.cache.corruptEntries,
      t.maxCorruptEntries,
    );
  }

  const { http } = snapshot;
  if (
    http.requests >= t.minSamples &&
    http.errorRatio !== null &&
    http.errorRatio > t.maxHttpErrorRatio
  ) {
    push(
      'http.error-ratio',
      'critical',
      'HTTP 5xx ratio is above threshold',
      http.errorRatio,
      t.maxHttpErrorRatio,
    );
  }
  for (const [route, metrics] of Object.entries(http.routes)) {
    if (
      metrics.requests >= t.minSamples &&
      metrics.latency.p99Ms > t.maxRouteP99Ms
    ) {
      push(
        'http.route-latency',
        'warning',
        `p99 latency of ${route} is above threshold`,
        metrics.latency.p99Ms,
        t.maxRouteP99Ms,
      );
    }
  }

  const { cache } = snapshot;
  if (
    cache.lookups >= t.minLookupsForHitRatio &&
    cache.hitRatio !== null &&
    cache.hitRatio < t.minHitRatio
  ) {
    push(
      'cache.hit-ratio',
      'warning',
      'Cache hit ratio is below threshold',
      cache.hitRatio,
      t.minHitRatio,
    );
  }
  if (cache.loads >= t.minSamples) {
    const loadErrorRatio = cache.loadErrors / cache.loads;
    if (loadErrorRatio > t.maxLoadErrorRatio) {
      push(
        'cache.load-errors',
        'critical',
        'Loader (origin) failure ratio is above threshold',
        round4(loadErrorRatio),
        t.maxLoadErrorRatio,
      );
    }
  }
  for (const [operation, latency] of Object.entries(cache.latency)) {
    if (latency.count >= t.minSamples && latency.p99Ms > t.maxCacheP99Ms) {
      push(
        'cache.latency',
        'warning',
        `p99 latency of ${operation} is above threshold`,
        latency.p99Ms,
        t.maxCacheP99Ms,
      );
    }
  }

  const { rateLimit } = snapshot;
  if (
    rateLimit.decisions >= t.minSamples &&
    rateLimit.limitedRatio !== null &&
    rateLimit.limitedRatio > t.maxLimitedRatio
  ) {
    const top = rateLimit.topLimited[0];
    push(
      'rate-limit.pressure',
      'warning',
      `Most requests are being throttled${top === undefined ? '' : ` (top client: ${top.key})`}`,
      rateLimit.limitedRatio,
      t.maxLimitedRatio,
    );
  }

  const loop = snapshot.process.eventLoop;
  if (loop !== undefined && loop.p99Ms > t.maxEventLoopP99Ms) {
    push(
      'process.event-loop',
      'warning',
      'Event-loop delay p99 is above threshold',
      loop.p99Ms,
      t.maxEventLoopP99Ms,
    );
  }

  return alerts;
}

function round4(value: number): number {
  return Math.round(value * 10_000) / 10_000;
}
