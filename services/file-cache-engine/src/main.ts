/**
 * Public entry point.
 *
 * Two independent modules are exposed, plus a convenience factory that wires
 * them together with sensible defaults:
 *
 * - `cache/`       – file-backed cache (`FsCacheStore`), policy layer
 *                    (`CacheManager`) and cold-entry GC (`ExpirySweeper`).
 * - `rate-limit/`  – token-bucket limiters for bounding request rates.
 * - `http/`        – Express/Node-compatible middleware: client identity,
 *                    429 rate limiting, and GET response caching.
 * - `resilience/`  – retry, circuit breaker, cross-process lock and the
 *                    fault-tolerant store decorator.
 * - `telemetry/`   – typed events, sinks, in-memory metrics and alerts.
 * - `diagnostics/` – health checks, readiness endpoints and an offline CLI.
 */
import { join } from 'node:path';

import type { CacheManagerOptions } from './cache/cache-manager';
import { CacheManager } from './cache/cache-manager';
import type { ExpirySweeperOptions } from './cache/expiry-sweeper';
import { ExpirySweeper } from './cache/expiry-sweeper';
import type { FsCacheStoreOptions } from './cache/fs-cache-store';
import { FsCacheStore } from './cache/fs-cache-store';
import { SlruMemoryStore } from './cache/slru-memory-store';
import type {
  TierCircuitOptions,
  TieredCacheStoreStats,
} from './cache/tiered-cache-store';
import { TieredCacheStore } from './cache/tiered-cache-store';
import type { CacheStore } from './cache/types';
import type { TokenBucketOptions } from './rate-limit/token-bucket';
import { TokenBucketRateLimiter } from './rate-limit/token-bucket';
import type { RateLimiter } from './rate-limit/types';
import { systemClock } from './shared/clock';
import type {
  ResilienceEvent,
  ResilientStoreOptions,
  StoreHealth,
} from './resilience/resilient-store';
import { ResilientCacheStore } from './resilience/resilient-store';
import type {
  DiagnosticsOptions,
  DiagnosticsReport,
} from './diagnostics/diagnostics';
import { runDiagnostics } from './diagnostics/diagnostics';
import type { AlertThresholds } from './telemetry/alerts';
import { describeError } from './telemetry/events';
import type {
  MetricsCollectorOptions,
  MetricsSnapshot,
} from './telemetry/metrics';
import { MetricsCollector } from './telemetry/metrics';
import type { PrometheusTelemetryOptions } from './telemetry/prometheus';
import { PrometheusTelemetry } from './telemetry/prometheus';
import type { TelemetrySink } from './telemetry/sink';
import { CompositeSink } from './telemetry/sink';

export * from './cache/index';
export * from './rate-limit/index';
export * from './http/index';
export * from './resilience/index';
export * from './telemetry/index';
export * from './diagnostics/index';
export type { Clock } from './shared/clock';
export { systemClock } from './shared/clock';
export { KeyedMutex } from './shared/keyed-mutex';
export { SingleFlight } from './shared/single-flight';

/** Adds an in-memory tier (L1) in front of the disk. */
export interface TieringOptions {
  /**
   * L1 memory budget in bytes: a segmented LRU (20 % probationary, 80 %
   * protected) with TinyLFU admission, so scans and one-hit wonders cannot
   * push out entries that are re-read.
   */
  readonly memoryBytes: number;
  /**
   * Acknowledge writes once they are in memory and flush them to disk in the
   * background (batched, with rapid rewrites coalesced). `close()` drains
   * them. The trade-off: a crash loses writes not yet flushed (about 10 ms
   * worth). Default: `false`, each write resolves once it is on disk.
   */
  readonly writeBehind?: boolean;
  /**
   * Circuit breaker for the disk tier. Default: opens after 5 consecutive
   * disk failures (EIO, ENOSPC, ...) within 10 s. It then serves from
   * memory only (L1-only mode, writes buffered) and probes the disk again
   * after 30 s plus up to 20 % jitter. `false` disables it. The resilient
   * decorator's own breaker is turned off when tiering is on (unless
   * `resilience.circuit` is set explicitly), so this is the only one.
   */
  readonly circuit?: TierCircuitOptions | false;
}

export interface FileCacheOptions extends Omit<
  CacheManagerOptions,
  'rateLimiter'
> {
  /** Root directory for cached files. */
  readonly directory: string;
  /** Storage tuning (fsync, checksum verification, file mode). */
  readonly store?: Omit<FsCacheStoreOptions, 'directory'>;
  /**
   * Background GC configuration. `false` disables the sweeper entirely
   * (you can still call `sweeper.sweepOnce()` yourself). Default: enabled,
   * guarded by a lock file in `directory` so only one process sweeps.
   */
  readonly sweeper?: ExpirySweeperOptions | false;
  /**
   * Fault tolerance for the store (retries, circuit breaker, disk-full
   * reclaim). `false` uses the raw store. Default: enabled; a full disk
   * triggers an emergency sweep before the write is retried.
   */
  readonly resilience?:
    | Omit<ResilientStoreOptions, 'onCapacityExhausted'>
    | false;
  /**
   * Throttle loader invocations (cache misses). Pass an existing
   * {@link RateLimiter} to share one budget across components, or bucket
   * options to create a dedicated limiter. Default: no limiting.
   */
  readonly rateLimiter?: RateLimiter | TokenBucketOptions;
  /**
   * Your own telemetry destination (logger, StatsD/OTel bridge, …). It
   * receives every event in addition to the built-in metrics collector.
   */
  readonly telemetry?: TelemetrySink;
  /** Tuning for the built-in metrics collector, or `false` to disable it. */
  readonly metrics?: MetricsCollectorOptions | false;
  /** Thresholds used by `diagnostics()` to raise alerts. */
  readonly alertThresholds?: AlertThresholds;
  /**
   * Put a memory tier in front of the disk. Default: disk only.
   * The sweeper then runs through the tiered store, so whatever it removes
   * from disk leaves memory too.
   */
  readonly tiering?: TieringOptions;
  /**
   * Export Prometheus metrics via prom-client (`true`, or options). Read
   * them from `fileCache.prometheus`: `.metrics()` and `.contentType` for a
   * `/metrics` endpoint, or pass your own `registry`.
   */
  readonly prometheus?: PrometheusTelemetryOptions | boolean;
}

export interface FileCacheHealth {
  readonly store: StoreHealth | undefined;
  readonly sweeperRunning: boolean;
  /** Tier counters (L1/L2 hits, pending writes, …) when tiering is on. */
  readonly tiering: TieredCacheStoreStats | undefined;
}

export interface FileCache {
  readonly cache: CacheManager;
  /** Built-in aggregation; `undefined` when disabled via `metrics: false`. */
  readonly metrics: MetricsCollector | undefined;
  /** The combined sink every component reports to (metrics + yours). */
  readonly telemetry: TelemetrySink;
  /**
   * The store the manager talks to: the tiered store when tiering is on,
   * otherwise the resilient decorator (unless that is disabled too).
   */
  readonly store: CacheStore;
  /** The memory + disk coordinator, when `tiering` is set. */
  readonly tiered: TieredCacheStore | undefined;
  /** The Prometheus exporter, when `prometheus` is set. */
  readonly prometheus: PrometheusTelemetry | undefined;
  /** The underlying file-system store. */
  readonly fsStore: FsCacheStore;
  readonly sweeper: ExpirySweeper;
  readonly rateLimiter: RateLimiter | undefined;
  /** Snapshot for readiness probes and dashboards. */
  health(): FileCacheHealth;
  /**
   * Runs every health check (directory, disk, temp files, lock, integrity
   * sample, circuit, sweeper, metric alerts) and returns the full report.
   */
  diagnostics(
    overrides?: Partial<DiagnosticsOptions>,
  ): Promise<DiagnosticsReport>;
  /**
   * Full teardown, in order: stop the sweeper (waits for a sweep in
   * progress), abort and drain in-flight loaders, flush every write still
   * pending in memory to disk (write-behind), cut pending store retries
   * short, and close the rate limiter *if this factory created it* (a shared
   * limiter passed in by the caller is left alone). Afterwards no timer or
   * listener owned by the cache remains, and further cache operations reject
   * with `CacheClosedError`. Teardown always completes. If some pending
   * write could not be flushed, it then rejects, so a lost write is never
   * silent. Idempotent.
   */
  close(): Promise<void>;
}

/**
 * Opens (or creates) a file cache at `directory`, wires the store, policy
 * layer and sweeper together, and starts the sweeper unless disabled.
 */
export async function createFileCache(
  options: FileCacheOptions,
): Promise<FileCache> {
  const {
    directory,
    store: storeOptions,
    sweeper: sweeperOptions,
    resilience: resilienceOptions,
    rateLimiter: limiterOptions,
    telemetry: userSink,
    metrics: metricsOptions,
    alertThresholds,
    tiering,
    prometheus: prometheusOptions,
    ...managerOptions
  } = options;

  const clock = managerOptions.clock ?? systemClock;
  const metrics =
    metricsOptions === false
      ? undefined
      : new MetricsCollector({ clock, ...metricsOptions });
  const prometheus =
    prometheusOptions === undefined || prometheusOptions === false
      ? undefined
      : new PrometheusTelemetry(
          prometheusOptions === true ? {} : prometheusOptions,
        );
  const telemetry = new CompositeSink([metrics, prometheus, userSink]);

  const fsStore = await FsCacheStore.open({
    directory,
    ...storeOptions,
    onCorruptEntry: (error): void => {
      telemetry.emit({
        type: 'cache.corrupt-entry',
        at: clock.now(),
        path: error.path,
        reason: error.message,
      });
      storeOptions?.onCorruptEntry?.(error);
    },
  });

  const sweepIntervalMs =
    sweeperOptions === false
      ? undefined
      : (sweeperOptions?.intervalMs ?? 60_000);

  const diskStore: CacheStore =
    resilienceOptions === false
      ? fsStore
      : new ResilientCacheStore(fsStore, {
          clock: managerOptions.clock,
          random: managerOptions.random,
          ...resilienceOptions,
          // With tiering, the tier's breaker (which can fall back to memory)
          // is the one circuit. Two in series would fight over the same
          // failures. (After the spread: an explicit `circuit: undefined`
          // must not re-enable it.)
          ...(tiering !== undefined && resilienceOptions?.circuit === undefined
            ? { circuit: false as const }
            : {}),
          onCapacityExhausted: async (): Promise<void> => {
            // Called only during writes, after `sweeper` is initialised.
            await sweeper.sweepOnce();
          },
          onEvent: (event): void => {
            forwardResilienceEvent(telemetry, clock.now(), event);
            resilienceOptions?.onEvent?.(event);
          },
        });

  const tiered =
    tiering === undefined
      ? undefined
      : new TieredCacheStore({
          l1: new SlruMemoryStore({
            maxMemoryBytes: tiering.memoryBytes,
            // Deterministic when the caller injects `random` (tests).
            seed:
              managerOptions.random === undefined
                ? undefined
                : Math.floor(managerOptions.random() * 2 ** 32),
          }),
          l2: diskStore,
          circuit: tiering.circuit,
          telemetry,
          clock: managerOptions.clock,
          random: managerOptions.random,
          writeBehind: tiering.writeBehind === true ? {} : false,
          onFlushError: (error, key): void => {
            telemetry.emit({
              type: 'cache.store-error',
              at: clock.now(),
              operation: 'write',
              key,
              error: describeError(error),
            });
            (managerOptions.onStoreError ?? logFlushError)(error, {
              key,
              operation: 'write',
            });
          },
        });
  const store: CacheStore = tiered ?? diskStore;
  if (tiered !== undefined) {
    prometheus?.observe(tiered);
  }

  // The sweeper works on the raw disk store on purpose: when the disk is
  // full or the circuit is open, reclaiming space must still be possible.
  // With tiering it goes through the tiered store's sweeper view, which
  // deletes on that same raw store but also drops the entries from memory.
  const sweeper = new ExpirySweeper(tiered?.sweeperView(fsStore) ?? fsStore, {
    clock: managerOptions.clock,
    random: managerOptions.random,
    lock: { path: join(directory, SWEEP_LOCK_FILE) },
    ...(sweeperOptions === false ? {} : sweeperOptions),
    onSweep: (report): void => {
      telemetry.emit({ type: 'cache.sweep', at: clock.now(), report });
      if (sweeperOptions !== false) {
        sweeperOptions?.onSweep?.(report);
      }
    },
  });

  const rateLimiter = resolveRateLimiter(limiterOptions, managerOptions.clock);
  const ownsRateLimiter =
    rateLimiter !== undefined && rateLimiter !== limiterOptions;
  const cache = new CacheManager(store, {
    ...managerOptions,
    rateLimiter,
    telemetry,
  });

  if (sweeperOptions !== false) {
    sweeper.start();
  }

  let closing: Promise<void> | undefined;
  const close = async (): Promise<void> => {
    await sweeper.stop();
    await cache.close();
    try {
      // Drain write-behind *before* closing the disk store: once that is
      // closed, a late flush would fail ("Cache store is closing").
      await tiered?.close();
    } finally {
      if (diskStore instanceof ResilientCacheStore) {
        diskStore.close();
      }
      if (ownsRateLimiter && rateLimiter instanceof TokenBucketRateLimiter) {
        rateLimiter.close();
      }
      metrics?.close();
      prometheus?.dispose();
    }
  };

  const storeHealth = (): StoreHealth | undefined =>
    diskStore instanceof ResilientCacheStore ? diskStore.health() : undefined;

  return {
    cache,
    metrics,
    telemetry,
    store,
    tiered,
    prometheus,
    fsStore,
    sweeper,
    rateLimiter,
    health: (): FileCacheHealth => ({
      store: storeHealth(),
      sweeperRunning: sweeper.isRunning,
      tiering: tiered?.stats(),
    }),
    diagnostics: (overrides = {}): Promise<DiagnosticsReport> =>
      runDiagnostics({
        directory,
        store: fsStore,
        sweeper,
        sweepIntervalMs,
        storeHealth,
        metrics:
          metrics === undefined
            ? undefined
            : (): MetricsSnapshot => metrics.snapshot(),
        thresholds: alertThresholds,
        tiering:
          tiered === undefined
            ? undefined
            : (): TieredCacheStoreStats => tiered.stats(),
        clock,
        ...overrides,
      }),
    close: (): Promise<void> => {
      closing ??= close();
      return closing;
    },
  };
}

const SWEEP_LOCK_FILE = 'sweep.lock';

function logFlushError(error: unknown, context: { key: string }): void {
  console.error(
    `[cache] write-behind flush failed for "${context.key}":`,
    error,
  );
}

function forwardResilienceEvent(
  sink: TelemetrySink,
  at: number,
  event: ResilienceEvent,
): void {
  switch (event.type) {
    case 'retry':
      sink.emit({
        type: 'store.retry',
        at,
        operation: event.operation,
        attempt: event.attempt,
        delayMs: event.delayMs,
        error: describeError(event.error),
      });
      break;
    case 'circuit':
      sink.emit({
        type: 'store.circuit',
        at,
        state: event.state,
        error:
          event.error === undefined ? undefined : describeError(event.error),
      });
      break;
    case 'capacity-exhausted':
      sink.emit({
        type: 'store.capacity-exhausted',
        at,
        reclaimed: event.reclaimed,
      });
      break;
    case 'store-failure':
      sink.emit({
        type: 'store.failure',
        at,
        operation: event.operation,
        kind: event.kind,
        attempts: event.attempts,
        error: describeError(event.error),
      });
      break;
    default:
      break;
  }
}

function resolveRateLimiter(
  option: RateLimiter | TokenBucketOptions | undefined,
  clock: CacheManagerOptions['clock'],
): RateLimiter | undefined {
  if (option === undefined) {
    return undefined;
  }
  if (isRateLimiter(option)) {
    return option;
  }
  return new TokenBucketRateLimiter({ clock, ...option });
}

function isRateLimiter(
  value: RateLimiter | TokenBucketOptions,
): value is RateLimiter {
  return 'acquire' in value && typeof value.acquire === 'function';
}
