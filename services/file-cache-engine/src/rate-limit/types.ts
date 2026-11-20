export interface AcquireOptions {
  /** Abort waiting; rejects with {@link RateLimitExceededError} (`aborted`). */
  readonly signal?: AbortSignal;
  /**
   * Maximum time to wait for capacity. Rejects with
   * {@link RateLimitExceededError} (`timeout`) once exceeded.
   */
  readonly timeoutMs?: number;
}

/**
 * Port for request throttling. Implementations must be safe to share across
 * many concurrent callers.
 */
export interface RateLimiter {
  /**
   * Consumes `cost` units if immediately available. Never waits.
   * @returns `true` when the units were consumed.
   */
  tryAcquire(cost?: number): boolean;

  /**
   * Waits until `cost` units are available and consumes them. Callers are
   * served in FIFO order.
   */
  acquire(cost?: number, options?: AcquireOptions): Promise<void>;
}
