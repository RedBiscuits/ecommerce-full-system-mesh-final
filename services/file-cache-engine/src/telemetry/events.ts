import type { Freshness, ResultSource } from '../cache/index';
import type { SweepReport } from '../cache/expiry-sweeper';
import type { CircuitState } from '../resilience/circuit-breaker';
import type { FailureKind } from '../resilience/errors';

/**
 * Every telemetry event carries `type` and `at` (epoch ms). Events are plain
 * data: safe to serialise, cheap to construct, and never hold references to
 * request objects or payloads.
 */
interface Base {
  readonly at: number;
}

/** A lookup (`get`/`head`/`getOrLoad`) concluded. */
export interface CacheLookupEvent extends Base {
  readonly type: 'cache.lookup';
  readonly operation: 'get' | 'head' | 'getOrLoad';
  readonly key: string;
  readonly outcome: 'hit' | 'stale' | 'miss';
  readonly freshness: Freshness | undefined;
  readonly durationMs: number;
}

/** A loader ran (miss, forced load, or background revalidation). */
export interface CacheLoadEvent extends Base {
  readonly type: 'cache.load';
  readonly key: string;
  readonly background: boolean;
  readonly durationMs: number;
  readonly outcome: 'stored' | 'unstored' | 'error';
  readonly source: ResultSource | undefined;
  readonly bytes: number | undefined;
  readonly error: string | undefined;
}

/** An explicit mutation (`put`/`delete`/`clear`). */
export interface CacheWriteEvent extends Base {
  readonly type: 'cache.write';
  readonly operation: 'put' | 'delete' | 'clear';
  readonly key: string | undefined;
  readonly durationMs: number;
  readonly bytes: number | undefined;
  readonly error: string | undefined;
}

/** The store failed on a read-through path and the call degraded. */
export interface CacheStoreErrorEvent extends Base {
  readonly type: 'cache.store-error';
  readonly operation: 'read' | 'write';
  readonly key: string;
  readonly error: string;
}

/** A stored file failed validation and was quarantined. */
export interface CacheCorruptEntryEvent extends Base {
  readonly type: 'cache.corrupt-entry';
  readonly path: string;
  readonly reason: string;
}

export interface CacheSweepEvent extends Base {
  readonly type: 'cache.sweep';
  readonly report: SweepReport;
}

/** Resilience layer: a transient failure is being retried. */
export interface StoreRetryEvent extends Base {
  readonly type: 'store.retry';
  readonly operation: string;
  readonly attempt: number;
  readonly delayMs: number;
  readonly error: string;
}

export interface StoreCircuitEvent extends Base {
  readonly type: 'store.circuit';
  readonly state: CircuitState;
  readonly error: string | undefined;
}

export interface StoreCapacityEvent extends Base {
  readonly type: 'store.capacity-exhausted';
  readonly reclaimed: boolean;
}

export interface StoreFailureEvent extends Base {
  readonly type: 'store.failure';
  readonly operation: string;
  readonly kind: FailureKind;
  readonly attempts: number;
  readonly error: string;
}

/**
 * Which tier answered a tiered-store read. `write-buffer` is a write not yet
 * flushed to disk. `shortCircuited` means the disk was skipped because its
 * circuit is not closed (L1-only mode).
 */
export interface TierReadEvent extends Base {
  readonly type: 'tier.read';
  readonly key: string;
  readonly source: 'l1' | 'write-buffer' | 'l2' | 'miss';
  /** Joined a disk read already in flight for the key. */
  readonly coalesced: boolean;
  readonly shortCircuited: boolean;
}

/** A disk hit was offered to L1. */
export interface TierPromotionEvent extends Base {
  readonly type: 'tier.promotion';
  readonly key: string;
  readonly admitted: boolean;
}

/**
 * How a write was accepted:
 * - `buffered`: queued for a background flush (write-behind, or the circuit
 *   is not closed).
 * - `coalesced`: merged into a queued write of the same key.
 * - `write-through`: written to disk before resolving.
 * - `fallback`: written to disk before resolving because the buffer was full.
 * - `l1-only`: buffer full while the circuit is open. Kept in memory only.
 */
export interface TierWriteEvent extends Base {
  readonly type: 'tier.write';
  readonly key: string;
  readonly mode:
    | 'buffered'
    | 'coalesced'
    | 'write-through'
    | 'fallback'
    | 'l1-only';
  readonly bytes: number;
}

/**
 * A buffered write's attempt to reach disk. `requeued`: the disk failed, or
 * the circuit refused it. It stays buffered and is retried. `failed`: given
 * up (dropped, or the final attempt during `close()`).
 */
export interface TierFlushEvent extends Base {
  readonly type: 'tier.flush';
  readonly key: string;
  readonly outcome: 'ok' | 'requeued' | 'failed';
  readonly durationMs: number;
  /** This write was the half-open recovery probe. */
  readonly probe: boolean;
  readonly error: string | undefined;
}

/**
 * The tier's disk circuit changed state. `open` carries `retryAt`: when a
 * half-open probe will try the disk again (cooldown plus jitter).
 */
export interface TierCircuitEvent extends Base {
  readonly type: 'tier.circuit';
  readonly state: CircuitState;
  readonly retryAt: number | undefined;
  readonly consecutiveFailures: number;
  readonly error: string | undefined;
}

/** A rate-limit decision. */
export interface RateLimitEvent extends Base {
  readonly type: 'rate-limit';
  readonly key: string;
  readonly admitted: boolean;
  readonly remaining: number;
  readonly retryAfterMs: number;
  readonly route: string | undefined;
}

/** An HTTP request finished (or the socket closed). */
export interface HttpRequestEvent extends Base {
  readonly type: 'http.request';
  readonly method: string;
  /** Normalised route template ("API"), e.g. `GET /users/:id`. */
  readonly route: string;
  readonly status: number;
  readonly durationMs: number;
  readonly cacheStatus: string | undefined;
  readonly bytes: number | undefined;
  readonly aborted: boolean;
  readonly clientKey: string | undefined;
}

export type TelemetryEvent =
  | CacheLookupEvent
  | CacheLoadEvent
  | CacheWriteEvent
  | CacheStoreErrorEvent
  | CacheCorruptEntryEvent
  | CacheSweepEvent
  | StoreRetryEvent
  | StoreCircuitEvent
  | StoreCapacityEvent
  | StoreFailureEvent
  | RateLimitEvent
  | HttpRequestEvent
  | TierReadEvent
  | TierPromotionEvent
  | TierWriteEvent
  | TierFlushEvent
  | TierCircuitEvent;

export type TelemetryEventType = TelemetryEvent['type'];

/** Renders any thrown value as a short, serialisable message. */
export function describeError(error: unknown): string {
  if (error instanceof Error) {
    const code =
      'code' in error && typeof error.code === 'string'
        ? ` [${error.code}]`
        : '';
    return `${error.name}: ${error.message}${code}`;
  }
  return String(error);
}
