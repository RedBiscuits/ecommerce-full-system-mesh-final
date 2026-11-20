import type { Clock } from '../shared/clock';
import { systemClock } from '../shared/clock';
import type { TokenBucketOptions } from './token-bucket';
import { TokenBucketRateLimiter } from './token-bucket';
import type { AcquireOptions } from './types';

export interface KeyedRateLimiterOptions extends Omit<
  TokenBucketOptions,
  'clock'
> {
  /**
   * Buckets untouched for this long are dropped (a dropped bucket is
   * equivalent to a full one). Default: 10 minutes.
   */
  readonly idleTtlMs?: number;
  /** How often idle buckets are pruned. Default: `idleTtlMs`. */
  readonly pruneIntervalMs?: number;
  /**
   * Hard cap on tracked keys. When reached, the least recently used idle
   * bucket is evicted to make room — bounding memory under a flood of
   * never-repeating keys (spoofed addresses, scanners). Default: unlimited.
   */
  readonly maxKeys?: number;
  readonly clock?: Clock;
}

interface Slot {
  readonly limiter: TokenBucketRateLimiter;
  lastUsedAt: number;
}

const DEFAULT_IDLE_TTL_MS = 10 * 60_000;

/**
 * One token bucket per key (client id, IP, tenant, upstream host, ...).
 *
 * Buckets are created on first use and pruned after `idleTtlMs` of
 * inactivity, so memory is bounded by the number of *active* keys.
 */
export class KeyedRateLimiter {
  readonly #bucketOptions: TokenBucketOptions;
  readonly #idleTtlMs: number;
  readonly #pruneIntervalMs: number;
  readonly #maxKeys: number;
  readonly #clock: Clock;
  /** Insertion order doubles as LRU order: touched slots are re-inserted. */
  readonly #slots = new Map<string, Slot>();
  #lastPruneAt: number;

  constructor(options: KeyedRateLimiterOptions) {
    this.#bucketOptions = {
      capacity: options.capacity,
      refillPerSecond: options.refillPerSecond,
      maxQueueSize: options.maxQueueSize,
      initialTokens: options.initialTokens,
      clock: options.clock ?? systemClock,
    };
    this.#idleTtlMs = options.idleTtlMs ?? DEFAULT_IDLE_TTL_MS;
    this.#pruneIntervalMs = options.pruneIntervalMs ?? this.#idleTtlMs;
    this.#maxKeys = Math.max(1, options.maxKeys ?? Number.POSITIVE_INFINITY);
    this.#clock = options.clock ?? systemClock;
    this.#lastPruneAt = this.#clock.now();
  }

  /** Number of keys currently holding a bucket. */
  get size(): number {
    return this.#slots.size;
  }

  /** Returns the bucket dedicated to `key` (created on first use). */
  limiter(key: string): TokenBucketRateLimiter {
    const now = this.#clock.now();
    this.#maybePrune(now);

    let slot = this.#slots.get(key);
    if (slot === undefined) {
      this.#makeRoom();
      slot = {
        limiter: new TokenBucketRateLimiter(this.#bucketOptions),
        lastUsedAt: now,
      };
    } else {
      slot.lastUsedAt = now;
      this.#slots.delete(key);
    }
    this.#slots.set(key, slot);
    return slot.limiter;
  }

  tryAcquire(key: string, cost = 1): boolean {
    return this.limiter(key).tryAcquire(cost);
  }

  acquire(key: string, cost = 1, options?: AcquireOptions): Promise<void> {
    return this.limiter(key).acquire(cost, options);
  }

  /**
   * Closes every bucket (rejecting their waiters) and drops all state. The
   * limiter stays usable afterwards — buckets are created on demand — so a
   * shutdown race with late requests cannot throw. Idempotent.
   */
  close(): void {
    for (const slot of this.#slots.values()) {
      slot.limiter.close();
    }
    this.#slots.clear();
  }

  /** Drops every idle bucket now, regardless of the prune interval. */
  prune(): number {
    const now = this.#clock.now();
    this.#lastPruneAt = now;
    let dropped = 0;
    for (const [key, slot] of this.#slots) {
      if (
        now - slot.lastUsedAt >= this.#idleTtlMs &&
        slot.limiter.queueLength === 0
      ) {
        this.#slots.delete(key);
        dropped += 1;
      }
    }
    return dropped;
  }

  /** Evicts least-recently-used idle buckets until one slot is free. */
  #makeRoom(): void {
    if (this.#slots.size < this.#maxKeys) {
      return;
    }
    for (const [key, slot] of this.#slots) {
      if (slot.limiter.queueLength === 0) {
        this.#slots.delete(key);
        if (this.#slots.size < this.#maxKeys) {
          return;
        }
      }
    }
  }

  /** Amortised pruning on the access path: no background timer required. */
  #maybePrune(now: number): void {
    if (now - this.#lastPruneAt >= this.#pruneIntervalMs) {
      this.prune();
    }
  }
}
