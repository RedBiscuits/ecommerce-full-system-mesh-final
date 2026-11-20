import type { Clock } from '../shared/clock';
import { systemClock } from '../shared/clock';
import { RateLimitExceededError } from './errors';
import type { AcquireOptions, RateLimiter } from './types';

export interface TokenBucketOptions {
  /** Maximum burst size (tokens the bucket can hold). */
  readonly capacity: number;
  /** Sustained rate at which tokens are replenished. */
  readonly refillPerSecond: number;
  /**
   * Maximum number of callers allowed to wait in `acquire`. Additional
   * callers are rejected immediately (`queue-full`) — back-pressure instead of
   * unbounded memory growth. Default: `Infinity`.
   */
  readonly maxQueueSize?: number;
  /** Initial fill level. Default: `capacity`. */
  readonly initialTokens?: number;
  readonly clock?: Clock;
}

const CLOSED = new Error('Rate limiter is closed');

interface Waiter {
  readonly cost: number;
  readonly resolve: () => void;
  readonly reject: (error: Error) => void;
  cleanup: () => void;
}

/**
 * Token-bucket rate limiter.
 *
 * Tokens accrue continuously at `refillPerSecond` up to `capacity`. Waiting
 * callers form a FIFO queue; a single timer is armed for the head of the queue
 * so the limiter costs O(1) timers regardless of how many callers wait. That
 * timer keeps the event loop alive while anyone is queued — a caller awaiting
 * `acquire()` must not see the process exit underneath it.
 */
export class TokenBucketRateLimiter implements RateLimiter {
  readonly #capacity: number;
  readonly #refillPerMs: number;
  readonly #maxQueueSize: number;
  readonly #clock: Clock;
  readonly #queue: Waiter[] = [];

  #tokens: number;
  #lastRefillAt: number;
  #timer: NodeJS.Timeout | undefined;
  #closed = false;

  constructor(options: TokenBucketOptions) {
    if (!(options.capacity > 0) || !Number.isFinite(options.capacity)) {
      throw new RangeError('capacity must be a positive finite number');
    }
    if (
      !(options.refillPerSecond > 0) ||
      !Number.isFinite(options.refillPerSecond)
    ) {
      throw new RangeError('refillPerSecond must be a positive finite number');
    }
    this.#capacity = options.capacity;
    this.#refillPerMs = options.refillPerSecond / 1000;
    this.#maxQueueSize = options.maxQueueSize ?? Number.POSITIVE_INFINITY;
    this.#clock = options.clock ?? systemClock;
    this.#tokens = Math.min(
      this.#capacity,
      options.initialTokens ?? this.#capacity,
    );
    this.#lastRefillAt = this.#clock.now();
  }

  /** Maximum burst size. */
  get capacity(): number {
    return this.#capacity;
  }

  /**
   * Milliseconds until `cost` tokens could be available for a newcomer,
   * accounting for callers already queued ahead. `0` means "right now".
   */
  estimateWaitMs(cost = 1): number {
    this.#assertCost(cost);
    const queued = this.#queue.reduce((sum, w) => sum + w.cost, 0);
    return this.#waitFor(cost + queued);
  }

  /** Tokens currently available (after accounting for refill). */
  get availableTokens(): number {
    this.#refill();
    return this.#tokens;
  }

  /** Callers currently waiting in `acquire`. */
  get queueLength(): number {
    return this.#queue.length;
  }

  get isClosed(): boolean {
    return this.#closed;
  }

  /**
   * Rejects every waiting caller with {@link RateLimitExceededError}
   * (`aborted`), releases the timer, and refuses further admissions. Lets a
   * shutting-down service fail pending requests fast instead of holding
   * sockets open until tokens refill. Idempotent.
   */
  close(): void {
    if (this.#closed) {
      return;
    }
    this.#closed = true;
    clearTimeout(this.#timer);
    this.#timer = undefined;
    const waiters = this.#queue.splice(0);
    for (const waiter of waiters) {
      waiter.cleanup();
      waiter.reject(
        new RateLimitExceededError('aborted', 0, { cause: CLOSED }),
      );
    }
  }

  tryAcquire(cost = 1): boolean {
    this.#assertCost(cost);
    if (this.#closed) {
      return false;
    }
    // Queued callers have priority; never let a newcomer jump the line.
    if (this.#queue.length > 0) {
      return false;
    }
    this.#refill();
    if (this.#tokens < cost) {
      return false;
    }
    this.#tokens -= cost;
    return true;
  }

  acquire(cost = 1, options: AcquireOptions = {}): Promise<void> {
    this.#assertCost(cost);
    if (this.tryAcquire(cost)) {
      return Promise.resolve();
    }
    if (this.#closed) {
      return Promise.reject(
        new RateLimitExceededError('aborted', 0, { cause: CLOSED }),
      );
    }
    if (options.signal?.aborted) {
      return Promise.reject(
        new RateLimitExceededError('aborted', 0, {
          cause: options.signal.reason,
        }),
      );
    }
    if (this.#queue.length >= this.#maxQueueSize) {
      return Promise.reject(
        new RateLimitExceededError('queue-full', this.#waitFor(cost)),
      );
    }

    return new Promise<void>((resolve, reject) => {
      const waiter: Waiter = {
        cost,
        resolve,
        reject,
        cleanup: () => undefined,
      };
      const disposers: Array<() => void> = [];

      if (options.timeoutMs !== undefined) {
        const timer = setTimeout(() => {
          this.#dequeue(waiter);
          reject(new RateLimitExceededError('timeout', this.#waitFor(cost)));
        }, options.timeoutMs);
        timer.unref();
        disposers.push(() => clearTimeout(timer));
      }

      if (options.signal !== undefined) {
        const signal = options.signal;
        const onAbort = (): void => {
          this.#dequeue(waiter);
          reject(
            new RateLimitExceededError('aborted', this.#waitFor(cost), {
              cause: signal.reason,
            }),
          );
        };
        signal.addEventListener('abort', onAbort, { once: true });
        disposers.push(() => signal.removeEventListener('abort', onAbort));
      }

      waiter.cleanup = (): void => {
        for (const dispose of disposers) {
          dispose();
        }
      };

      this.#queue.push(waiter);
      this.#armTimer();
    });
  }

  /** Milliseconds until `cost` tokens could be available, ignoring the queue. */
  #waitFor(cost: number): number {
    this.#refill();
    const deficit = cost - this.#tokens;
    return deficit <= 0 ? 0 : deficit / this.#refillPerMs;
  }

  #refill(): void {
    const now = this.#clock.now();
    const elapsed = now - this.#lastRefillAt;
    if (elapsed <= 0) {
      return;
    }
    this.#tokens = Math.min(
      this.#capacity,
      this.#tokens + elapsed * this.#refillPerMs,
    );
    this.#lastRefillAt = now;
  }

  #dequeue(waiter: Waiter): void {
    const index = this.#queue.indexOf(waiter);
    if (index === -1) {
      return;
    }
    this.#queue.splice(index, 1);
    waiter.cleanup();
    if (index === 0) {
      this.#armTimer();
    }
  }

  /** (Re)arms the single timer for whoever is at the head of the queue. */
  #armTimer(): void {
    clearTimeout(this.#timer);
    this.#timer = undefined;

    const head = this.#queue[0];
    if (head === undefined) {
      return;
    }
    const delay = Math.ceil(this.#waitFor(head.cost));
    this.#timer = setTimeout(() => {
      this.#timer = undefined;
      this.#drain();
    }, delay);
  }

  #drain(): void {
    this.#refill();
    while (this.#queue.length > 0) {
      const head = this.#queue[0];
      if (head === undefined || this.#tokens < head.cost) {
        break;
      }
      this.#queue.shift();
      this.#tokens -= head.cost;
      head.cleanup();
      head.resolve();
    }
    this.#armTimer();
  }

  #assertCost(cost: number): void {
    if (!(cost > 0) || !Number.isFinite(cost)) {
      throw new RangeError('cost must be a positive finite number');
    }
    if (cost > this.#capacity) {
      throw new RangeError(
        `cost ${cost} exceeds bucket capacity ${this.#capacity}`,
      );
    }
  }
}
