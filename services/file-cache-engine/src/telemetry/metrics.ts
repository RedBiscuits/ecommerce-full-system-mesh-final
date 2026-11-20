import { monitorEventLoopDelay } from 'node:perf_hooks';

import type { Clock } from '../shared/clock';
import { systemClock } from '../shared/clock';
import type { TelemetryEvent } from './events';
import type { TelemetrySink } from './sink';

/** Upper bounds (ms) of the latency buckets; the last bucket is +∞. */
export const LATENCY_BUCKETS_MS: readonly number[] = [
  1, 2, 5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000,
];

export interface HistogramSnapshot {
  readonly count: number;
  readonly sumMs: number;
  readonly minMs: number;
  readonly maxMs: number;
  readonly meanMs: number;
  /** Bucket-estimated percentiles (upper bound of the containing bucket). */
  readonly p50Ms: number;
  readonly p90Ms: number;
  readonly p99Ms: number;
  /** Cumulative counts aligned with {@link LATENCY_BUCKETS_MS} plus +∞. */
  readonly buckets: readonly number[];
}

/**
 * Fixed-bucket latency histogram: O(1) record, O(buckets) snapshot, and no
 * unbounded memory. Percentiles are conservative estimates (rounded up to
 * the bucket boundary), which is what alerting wants.
 */
export class LatencyHistogram {
  readonly #counts: number[] = Array.from(
    { length: LATENCY_BUCKETS_MS.length + 1 },
    () => 0,
  );
  #count = 0;
  #sum = 0;
  #min = Number.POSITIVE_INFINITY;
  #max = 0;

  record(durationMs: number): void {
    const value = Math.max(0, durationMs);
    let index = LATENCY_BUCKETS_MS.findIndex((bound) => value <= bound);
    if (index === -1) {
      index = LATENCY_BUCKETS_MS.length;
    }
    this.#counts[index] = (this.#counts[index] ?? 0) + 1;
    this.#count += 1;
    this.#sum += value;
    this.#min = Math.min(this.#min, value);
    this.#max = Math.max(this.#max, value);
  }

  snapshot(): HistogramSnapshot {
    const cumulative: number[] = [];
    let running = 0;
    for (const count of this.#counts) {
      running += count;
      cumulative.push(running);
    }
    return {
      count: this.#count,
      sumMs: round(this.#sum),
      minMs: this.#count === 0 ? 0 : round(this.#min),
      maxMs: round(this.#max),
      meanMs: this.#count === 0 ? 0 : round(this.#sum / this.#count),
      p50Ms: this.#percentile(cumulative, 0.5),
      p90Ms: this.#percentile(cumulative, 0.9),
      p99Ms: this.#percentile(cumulative, 0.99),
      buckets: cumulative,
    };
  }

  #percentile(cumulative: readonly number[], quantile: number): number {
    if (this.#count === 0) {
      return 0;
    }
    const rank = Math.ceil(this.#count * quantile);
    const index = cumulative.findIndex((c) => c >= rank);
    if (index === -1 || index >= LATENCY_BUCKETS_MS.length) {
      return round(this.#max);
    }
    return LATENCY_BUCKETS_MS[index] ?? round(this.#max);
  }
}

export interface RecentError {
  readonly at: number;
  readonly source: string;
  readonly message: string;
}

export interface RouteMetrics {
  readonly requests: number;
  /** Requests by status class: `2xx`, `3xx`, `4xx`, `5xx`. */
  readonly statuses: Readonly<Record<string, number>>;
  readonly errors: number;
  readonly aborted: number;
  readonly latency: HistogramSnapshot;
  readonly cache: Readonly<Record<string, number>>;
}

export interface MetricsSnapshot {
  readonly since: number;
  readonly at: number;
  readonly cache: {
    readonly lookups: number;
    readonly hits: number;
    readonly stale: number;
    readonly misses: number;
    /** `(hits + stale) / lookups`, or `null` before any lookup. */
    readonly hitRatio: number | null;
    readonly loads: number;
    readonly loadErrors: number;
    readonly loadsUnstored: number;
    readonly backgroundLoads: number;
    readonly writes: number;
    readonly writeErrors: number;
    readonly storeErrors: number;
    readonly corruptEntries: number;
    readonly bytesLoaded: number;
    readonly latency: Readonly<Record<string, HistogramSnapshot>>;
    readonly lastSweep:
      | {
          readonly at: number;
          readonly expired: number;
          readonly evicted: number;
          readonly failed: number;
          readonly skipped: boolean;
        }
      | undefined;
  };
  readonly store: {
    readonly retries: number;
    readonly failures: number;
    readonly capacityEvents: number;
    readonly circuitOpens: number;
    readonly circuitState: string | undefined;
    /**
     * The tiered store's disk circuit. Open means L1-only mode: degraded,
     * but still serving, unlike the store circuit above.
     */
    readonly tierCircuitOpens: number;
    readonly tierCircuitState: string | undefined;
  };
  readonly rateLimit: {
    readonly decisions: number;
    readonly admitted: number;
    readonly limited: number;
    /** `limited / decisions`, or `null` before any decision. */
    readonly limitedRatio: number | null;
    /** Most-throttled client keys, descending. */
    readonly topLimited: readonly {
      readonly key: string;
      readonly count: number;
    }[];
  };
  readonly http: {
    readonly requests: number;
    readonly errors: number;
    readonly aborted: number;
    /** `5xx / requests`, or `null` before any request. */
    readonly errorRatio: number | null;
    readonly latency: HistogramSnapshot;
    readonly routes: Readonly<Record<string, RouteMetrics>>;
  };
  readonly process: {
    readonly uptimeMs: number;
    readonly rssBytes: number;
    readonly heapUsedBytes: number;
    /** Event-loop delay percentiles (ms), when monitoring is enabled. */
    readonly eventLoop:
      | {
          readonly p50Ms: number;
          readonly p99Ms: number;
          readonly maxMs: number;
        }
      | undefined;
  };
  readonly recentErrors: readonly RecentError[];
}

export interface MetricsCollectorOptions {
  /** Sample event-loop delay (cheap native histogram). Default: `true`. */
  readonly eventLoop?: boolean;
  /** Distinct routes tracked; further routes fold into `other`. Default: 200. */
  readonly maxRoutes?: number;
  /** Throttled client keys tracked for `topLimited`. Default: 500. */
  readonly maxLimitedKeys?: number;
  /** Size of the recent-error ring. Default: 50. */
  readonly recentErrors?: number;
  readonly clock?: Clock;
}

interface RouteState {
  requests: number;
  statuses: Record<string, number>;
  errors: number;
  aborted: number;
  latency: LatencyHistogram;
  cache: Record<string, number>;
}

const OTHER_ROUTE = 'other';

/**
 * In-memory aggregation of telemetry into counters, ratios and latency
 * histograms. Bounded: routes, throttled keys and errors are capped, so it
 * cannot grow with traffic. Plug it in as a sink, read `snapshot()` from a
 * diagnostics endpoint or a metrics scraper.
 */
export class MetricsCollector implements TelemetrySink {
  readonly #clock: Clock;
  readonly #maxRoutes: number;
  readonly #maxLimitedKeys: number;
  readonly #maxRecentErrors: number;
  readonly #since: number;
  #eventLoop: ReturnType<typeof monitorEventLoopDelay> | undefined;

  // cache
  #lookups = 0;
  #hits = 0;
  #stale = 0;
  #misses = 0;
  #loads = 0;
  #loadErrors = 0;
  #loadsUnstored = 0;
  #backgroundLoads = 0;
  #writes = 0;
  #writeErrors = 0;
  #storeErrors = 0;
  #corruptEntries = 0;
  #bytesLoaded = 0;
  readonly #cacheLatency = new Map<string, LatencyHistogram>();
  #lastSweep: MetricsSnapshot['cache']['lastSweep'];

  // store
  #retries = 0;
  #failures = 0;
  #capacityEvents = 0;
  #circuitOpens = 0;
  #circuitState: string | undefined;
  #tierCircuitOpens = 0;
  #tierCircuitState: string | undefined;

  // rate limit
  #decisions = 0;
  #admitted = 0;
  #limited = 0;
  readonly #limitedByKey = new Map<string, number>();

  // http
  #requests = 0;
  #httpErrors = 0;
  #httpAborted = 0;
  readonly #httpLatency = new LatencyHistogram();
  readonly #routes = new Map<string, RouteState>();

  readonly #recentErrors: RecentError[] = [];

  constructor(options: MetricsCollectorOptions = {}) {
    this.#clock = options.clock ?? systemClock;
    this.#maxRoutes = Math.max(1, options.maxRoutes ?? 200);
    this.#maxLimitedKeys = Math.max(1, options.maxLimitedKeys ?? 500);
    this.#maxRecentErrors = Math.max(0, options.recentErrors ?? 50);
    this.#since = this.#clock.now();
    if (options.eventLoop ?? true) {
      this.#eventLoop = monitorEventLoopDelay({ resolution: 20 });
      this.#eventLoop.enable();
    }
  }

  emit(event: TelemetryEvent): void {
    switch (event.type) {
      case 'cache.lookup':
        this.#lookups += 1;
        if (event.outcome === 'hit') {
          this.#hits += 1;
        } else if (event.outcome === 'stale') {
          this.#stale += 1;
        } else {
          this.#misses += 1;
        }
        this.#histogram(`cache.${event.operation}`).record(event.durationMs);
        break;
      case 'cache.load':
        this.#loads += 1;
        if (event.background) {
          this.#backgroundLoads += 1;
        }
        if (event.outcome === 'error') {
          this.#loadErrors += 1;
          this.#error('cache.load', event.error ?? 'unknown', event.at);
        } else if (event.outcome === 'unstored') {
          this.#loadsUnstored += 1;
        }
        this.#bytesLoaded += event.bytes ?? 0;
        this.#histogram('cache.load').record(event.durationMs);
        break;
      case 'cache.write':
        this.#writes += 1;
        if (event.error !== undefined) {
          this.#writeErrors += 1;
          this.#error(`cache.${event.operation}`, event.error, event.at);
        }
        this.#histogram(`cache.${event.operation}`).record(event.durationMs);
        break;
      case 'cache.store-error':
        this.#storeErrors += 1;
        this.#error(`store.${event.operation}`, event.error, event.at);
        break;
      case 'cache.corrupt-entry':
        this.#corruptEntries += 1;
        this.#error(
          'corrupt-entry',
          `${event.reason} (${event.path})`,
          event.at,
        );
        break;
      case 'cache.sweep':
        this.#lastSweep = {
          at: event.at,
          expired: event.report.expired,
          evicted: event.report.evicted,
          failed: event.report.failed,
          skipped: event.report.skipped,
        };
        break;
      case 'store.retry':
        this.#retries += 1;
        break;
      case 'store.failure':
        this.#failures += 1;
        this.#error(`store.${event.operation}`, event.error, event.at);
        break;
      case 'store.capacity-exhausted':
        this.#capacityEvents += 1;
        this.#error(
          'store.capacity',
          event.reclaimed ? 'disk full (reclaimed)' : 'disk full',
          event.at,
        );
        break;
      case 'store.circuit':
        this.#circuitState = event.state;
        if (event.state === 'open') {
          this.#circuitOpens += 1;
          this.#error(
            'store.circuit',
            event.error ?? 'circuit opened',
            event.at,
          );
        }
        break;
      case 'tier.circuit':
        // Kept apart from the store circuit: an open tier circuit means
        // L1-only mode, a degradation that must not alert like an outage.
        this.#tierCircuitState = event.state;
        if (event.state === 'open') {
          this.#tierCircuitOpens += 1;
          this.#error(
            'tier.circuit',
            event.error ?? 'circuit opened',
            event.at,
          );
        }
        break;
      case 'tier.flush':
        if (event.outcome === 'failed') {
          this.#error('tier.flush', event.error ?? 'flush failed', event.at);
        }
        break;
      case 'tier.read':
      case 'tier.promotion':
      case 'tier.write':
        // Per-request tier counters belong to the Prometheus exporter; the
        // in-memory collector only tracks what its alerts need.
        break;
      case 'rate-limit':
        this.#decisions += 1;
        if (event.admitted) {
          this.#admitted += 1;
        } else {
          this.#limited += 1;
          this.#countLimited(event.key);
        }
        break;
      case 'http.request':
        this.#request(event);
        break;
      default:
        assertNever(event);
    }
  }

  snapshot(): MetricsSnapshot {
    const memory = process.memoryUsage();
    const routes: Record<string, RouteMetrics> = {};
    for (const [route, state] of this.#routes) {
      routes[route] = {
        requests: state.requests,
        statuses: { ...state.statuses },
        errors: state.errors,
        aborted: state.aborted,
        latency: state.latency.snapshot(),
        cache: { ...state.cache },
      };
    }
    const latency: Record<string, HistogramSnapshot> = {};
    for (const [name, histogram] of this.#cacheLatency) {
      latency[name] = histogram.snapshot();
    }
    const eventLoop = this.#eventLoop;
    return {
      since: this.#since,
      at: this.#clock.now(),
      cache: {
        lookups: this.#lookups,
        hits: this.#hits,
        stale: this.#stale,
        misses: this.#misses,
        hitRatio: ratio(this.#hits + this.#stale, this.#lookups),
        loads: this.#loads,
        loadErrors: this.#loadErrors,
        loadsUnstored: this.#loadsUnstored,
        backgroundLoads: this.#backgroundLoads,
        writes: this.#writes,
        writeErrors: this.#writeErrors,
        storeErrors: this.#storeErrors,
        corruptEntries: this.#corruptEntries,
        bytesLoaded: this.#bytesLoaded,
        latency,
        lastSweep: this.#lastSweep,
      },
      store: {
        retries: this.#retries,
        failures: this.#failures,
        capacityEvents: this.#capacityEvents,
        circuitOpens: this.#circuitOpens,
        circuitState: this.#circuitState,
        tierCircuitOpens: this.#tierCircuitOpens,
        tierCircuitState: this.#tierCircuitState,
      },
      rateLimit: {
        decisions: this.#decisions,
        admitted: this.#admitted,
        limited: this.#limited,
        limitedRatio: ratio(this.#limited, this.#decisions),
        topLimited: [...this.#limitedByKey]
          .sort((a, b) => b[1] - a[1])
          .slice(0, 10)
          .map(([key, count]) => ({ key, count })),
      },
      http: {
        requests: this.#requests,
        errors: this.#httpErrors,
        aborted: this.#httpAborted,
        errorRatio: ratio(this.#httpErrors, this.#requests),
        latency: this.#httpLatency.snapshot(),
        routes,
      },
      process: {
        uptimeMs: Math.round(process.uptime() * 1000),
        rssBytes: memory.rss,
        heapUsedBytes: memory.heapUsed,
        eventLoop:
          eventLoop === undefined
            ? undefined
            : {
                p50Ms: round(eventLoop.percentile(50) / 1e6),
                p99Ms: round(eventLoop.percentile(99) / 1e6),
                maxMs: round(eventLoop.max / 1e6),
              },
      },
      recentErrors: [...this.#recentErrors],
    };
  }

  /** Zeroes every counter and histogram (event-loop monitor included). */
  reset(): void {
    this.#lookups = this.#hits = this.#stale = this.#misses = 0;
    this.#loads =
      this.#loadErrors =
      this.#loadsUnstored =
      this.#backgroundLoads =
        0;
    this.#writes =
      this.#writeErrors =
      this.#storeErrors =
      this.#corruptEntries =
        0;
    this.#bytesLoaded = 0;
    this.#cacheLatency.clear();
    this.#lastSweep = undefined;
    this.#retries =
      this.#failures =
      this.#capacityEvents =
      this.#circuitOpens =
      this.#tierCircuitOpens =
        0;
    this.#decisions = this.#admitted = this.#limited = 0;
    this.#limitedByKey.clear();
    this.#requests = this.#httpErrors = this.#httpAborted = 0;
    this.#routes.clear();
    this.#recentErrors.length = 0;
    this.#eventLoop?.reset();
  }

  /** Disables the event-loop monitor. Idempotent. */
  close(): void {
    this.#eventLoop?.disable();
    this.#eventLoop = undefined;
  }

  // --- internals ----------------------------------------------------------

  #histogram(name: string): LatencyHistogram {
    let histogram = this.#cacheLatency.get(name);
    if (histogram === undefined) {
      histogram = new LatencyHistogram();
      this.#cacheLatency.set(name, histogram);
    }
    return histogram;
  }

  #request(event: Extract<TelemetryEvent, { type: 'http.request' }>): void {
    this.#requests += 1;
    this.#httpLatency.record(event.durationMs);
    const isError = event.status >= 500;
    if (isError) {
      this.#httpErrors += 1;
    }
    if (event.aborted) {
      this.#httpAborted += 1;
    }

    let route = this.#routes.get(event.route);
    if (route === undefined) {
      const name =
        this.#routes.size >= this.#maxRoutes && !this.#routes.has(event.route)
          ? OTHER_ROUTE
          : event.route;
      route = this.#routes.get(name);
      if (route === undefined) {
        route = {
          requests: 0,
          statuses: {},
          errors: 0,
          aborted: 0,
          latency: new LatencyHistogram(),
          cache: {},
        };
        this.#routes.set(name, route);
      }
    }
    route.requests += 1;
    const statusClass = `${Math.floor(event.status / 100)}xx`;
    route.statuses[statusClass] = (route.statuses[statusClass] ?? 0) + 1;
    if (isError) {
      route.errors += 1;
    }
    if (event.aborted) {
      route.aborted += 1;
    }
    route.latency.record(event.durationMs);
    if (event.cacheStatus !== undefined) {
      route.cache[event.cacheStatus] =
        (route.cache[event.cacheStatus] ?? 0) + 1;
    }
  }

  #countLimited(key: string): void {
    const current = this.#limitedByKey.get(key);
    if (
      current === undefined &&
      this.#limitedByKey.size >= this.#maxLimitedKeys
    ) {
      // Drop the least-throttled key to make room; keeps memory bounded.
      let victim: string | undefined;
      let smallest = Number.POSITIVE_INFINITY;
      for (const [k, count] of this.#limitedByKey) {
        if (count < smallest) {
          smallest = count;
          victim = k;
        }
      }
      if (victim !== undefined) {
        this.#limitedByKey.delete(victim);
      }
    }
    this.#limitedByKey.set(key, (current ?? 0) + 1);
  }

  #error(source: string, message: string, at: number): void {
    if (this.#maxRecentErrors === 0) {
      return;
    }
    this.#recentErrors.push({ at, source, message });
    if (this.#recentErrors.length > this.#maxRecentErrors) {
      this.#recentErrors.shift();
    }
  }
}

function ratio(numerator: number, denominator: number): number | null {
  return denominator === 0 ? null : round(numerator / denominator, 4);
}

function round(value: number, digits = 2): number {
  const factor = 10 ** digits;
  return Math.round(value * factor) / factor;
}

function assertNever(value: never): never {
  throw new Error(`Unhandled telemetry event: ${JSON.stringify(value)}`);
}
