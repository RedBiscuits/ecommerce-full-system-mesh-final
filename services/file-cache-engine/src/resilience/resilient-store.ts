import type {
  CacheEntry,
  CacheKey,
  CacheStore,
  EntryAttributes,
  EntryLifetime,
  EntryMetadata,
} from '../cache/types';
import type { Clock } from '../shared/clock';
import { systemClock } from '../shared/clock';
import type {
  CircuitBreakerOptions,
  CircuitSnapshot,
} from './circuit-breaker';
import { CircuitBreaker } from './circuit-breaker';
import type { FailureKind } from './errors';
import {
  CircuitOpenError,
  classifyFailure,
  StoreUnavailableError,
} from './errors';
import type { RetryPolicy } from './retry';
import { withRetry } from './retry';

export type StoreOperation =
  | 'read'
  | 'readMetadata'
  | 'write'
  | 'delete'
  | 'deleteIf'
  | 'clear'
  | 'scan';

/** Typed observability events emitted by {@link ResilientCacheStore}. */
export type ResilienceEvent =
  | {
      readonly type: 'retry';
      readonly operation: StoreOperation;
      readonly key: CacheKey | undefined;
      readonly attempt: number;
      readonly delayMs: number;
      readonly error: unknown;
    }
  | {
      readonly type: 'circuit';
      readonly state: CircuitSnapshot['state'];
      readonly error: unknown;
    }
  | {
      readonly type: 'capacity-exhausted';
      readonly key: CacheKey;
      readonly reclaimed: boolean;
      readonly error: unknown;
    }
  | {
      readonly type: 'store-failure';
      readonly operation: StoreOperation;
      readonly key: CacheKey | undefined;
      readonly kind: FailureKind;
      readonly attempts: number;
      readonly error: unknown;
    };

export interface ResilientStoreOptions {
  /** Backoff policy for transient failures. */
  readonly retry?: Partial<RetryPolicy>;
  /** Circuit breaker tuning, or `false` to disable fast-failing. */
  readonly circuit?:
    | Omit<CircuitBreakerOptions, 'clock' | 'onStateChange'>
    | false;
  /**
   * Invoked when a write hits `ENOSPC`/`EDQUOT`. Typically an emergency
   * sweep. The write is retried once if it resolves.
   */
  readonly onCapacityExhausted?: () => Promise<void>;
  /** Minimum spacing between reclaim attempts. Default: 10 s. */
  readonly reclaimCooldownMs?: number;
  readonly onEvent?: (event: ResilienceEvent) => void;
  readonly clock?: Clock;
  readonly random?: () => number;
}

export interface StoreHealth {
  readonly circuit: CircuitSnapshot | undefined;
  readonly lastFailure:
    | {
        readonly at: number;
        readonly kind: FailureKind;
        readonly error: unknown;
      }
    | undefined;
  readonly lastReclaimAt: number | undefined;
}

/**
 * Decorator that makes any {@link CacheStore} fault tolerant without changing
 * its contract (open/closed principle):
 *
 * - transient errors are retried with jittered exponential backoff;
 * - repeated failures open a circuit so callers fail fast for a cooldown
 *   instead of queueing behind a sick disk;
 * - a full disk triggers a (rate-limited) reclaim and one more attempt.
 *
 * Every final failure is normalised to {@link StoreUnavailableError} with the
 * original error as `cause`, so policy layers can decide how to degrade.
 */
export class ResilientCacheStore implements CacheStore {
  readonly #inner: CacheStore;
  readonly #retry: Partial<RetryPolicy>;
  readonly #circuit: CircuitBreaker | undefined;
  readonly #onCapacityExhausted: (() => Promise<void>) | undefined;
  readonly #reclaimCooldownMs: number;
  readonly #onEvent: ((event: ResilienceEvent) => void) | undefined;
  readonly #clock: Clock;
  readonly #random: (() => number) | undefined;

  #lastFailure: StoreHealth['lastFailure'];
  #lastReclaimAt: number | undefined;
  #reclaimInFlight: Promise<boolean> | undefined;
  /** Aborted by `close()`: cuts retry backoffs short. */
  readonly #shutdown = new AbortController();

  constructor(inner: CacheStore, options: ResilientStoreOptions = {}) {
    this.#inner = inner;
    this.#retry = options.retry ?? {};
    this.#clock = options.clock ?? systemClock;
    this.#random = options.random;
    this.#onEvent = options.onEvent;
    this.#onCapacityExhausted = options.onCapacityExhausted;
    this.#reclaimCooldownMs = options.reclaimCooldownMs ?? 10_000;
    this.#circuit =
      options.circuit === false
        ? undefined
        : new CircuitBreaker({
            name: 'cache-store',
            ...options.circuit,
            clock: this.#clock,
            onStateChange: (state, error): void => {
              this.#onEvent?.({ type: 'circuit', state, error });
            },
          });
  }

  /** The wrapped store, for diagnostics and maintenance paths. */
  get inner(): CacheStore {
    return this.#inner;
  }

  health(): StoreHealth {
    return {
      circuit: this.#circuit?.snapshot(),
      lastFailure: this.#lastFailure,
      lastReclaimAt: this.#lastReclaimAt,
    };
  }

  /** Forces the circuit closed. */
  reset(): void {
    this.#circuit?.reset();
  }

  /**
   * Aborts pending retry backoffs so callers fail promptly during shutdown.
   * Operations already past their backoff complete normally. Idempotent.
   */
  close(): void {
    this.#shutdown.abort(new Error('Cache store is closing'));
  }

  read(key: CacheKey): Promise<CacheEntry | undefined> {
    return this.#run('read', key, () => this.#inner.read(key));
  }

  readMetadata(key: CacheKey): Promise<EntryMetadata | undefined> {
    return this.#run('readMetadata', key, () => this.#inner.readMetadata(key));
  }

  async write(
    key: CacheKey,
    value: Uint8Array,
    lifetime: EntryLifetime,
    attributes?: EntryAttributes,
  ): Promise<EntryMetadata> {
    const attempt = (): Promise<EntryMetadata> =>
      this.#inner.write(key, value, lifetime, attributes);
    try {
      return await this.#run('write', key, attempt);
    } catch (error) {
      if (
        !(error instanceof StoreUnavailableError) ||
        error.kind !== 'capacity'
      ) {
        throw error;
      }
      const reclaimed = await this.#reclaim();
      this.#onEvent?.({
        type: 'capacity-exhausted',
        key,
        reclaimed,
        error: error.cause,
      });
      if (!reclaimed) {
        throw error;
      }
      return this.#run('write', key, attempt);
    }
  }

  delete(key: CacheKey): Promise<boolean> {
    return this.#run('delete', key, () => this.#inner.delete(key));
  }

  deleteIf(
    key: CacheKey,
    predicate: (metadata: EntryMetadata) => boolean,
  ): Promise<boolean> {
    return this.#run('deleteIf', key, () =>
      this.#inner.deleteIf(key, predicate),
    );
  }

  clear(): Promise<void> {
    return this.#run('clear', undefined, () => this.#inner.clear());
  }

  scan(): AsyncIterable<EntryMetadata> {
    // Iteration is long-lived and already tolerant inside the store; it is
    // deliberately not retried or circuit-gated as a whole.
    return this.#inner.scan();
  }

  // --- internals ----------------------------------------------------------

  async #run<T>(
    operation: StoreOperation,
    key: CacheKey | undefined,
    task: () => Promise<T>,
  ): Promise<T> {
    let attempts = 0;
    const attemptOnce = async (attempt: number): Promise<T> => {
      attempts = attempt;
      return this.#circuit === undefined ? task() : this.#circuit.execute(task);
    };
    try {
      return await withRetry(attemptOnce, this.#retry, {
        signal: this.#shutdown.signal,
        random: this.#random,
        onRetry: (error, attempt, delayMs): void => {
          this.#onEvent?.({
            type: 'retry',
            operation,
            key,
            attempt,
            delayMs,
            error,
          });
        },
      });
    } catch (error) {
      if (error instanceof CircuitOpenError) {
        throw new StoreUnavailableError(operation, 'transient', attempts, {
          cause: error,
        });
      }
      const kind = classifyFailure(error);
      this.#lastFailure = { at: this.#clock.now(), kind, error };
      this.#onEvent?.({
        type: 'store-failure',
        operation,
        key,
        kind,
        attempts,
        error,
      });
      throw new StoreUnavailableError(operation, kind, attempts, {
        cause: error,
      });
    }
  }

  /** At most one reclaim at a time, and at most one per cooldown window. */
  #reclaim(): Promise<boolean> {
    if (this.#onCapacityExhausted === undefined) {
      return Promise.resolve(false);
    }
    if (this.#reclaimInFlight !== undefined) {
      return this.#reclaimInFlight;
    }
    const now = this.#clock.now();
    if (
      this.#lastReclaimAt !== undefined &&
      now - this.#lastReclaimAt < this.#reclaimCooldownMs
    ) {
      return Promise.resolve(false);
    }
    this.#lastReclaimAt = now;
    const reclaim = this.#onCapacityExhausted;
    this.#reclaimInFlight = reclaim()
      .then(
        () => true,
        () => false,
      )
      .finally(() => {
        this.#reclaimInFlight = undefined;
      });
    return this.#reclaimInFlight;
  }
}
