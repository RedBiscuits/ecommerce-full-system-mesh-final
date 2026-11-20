export type RateLimitRejection = 'queue-full' | 'timeout' | 'aborted';

/** Raised when a request could not be admitted by a rate limiter. */
export class RateLimitExceededError extends Error {
  constructor(
    readonly reason: RateLimitRejection,
    /** Suggested wait before retrying, in milliseconds (best effort). */
    readonly retryAfterMs: number,
    options?: ErrorOptions,
  ) {
    super(
      `Rate limit exceeded (${reason}); retry after ~${Math.ceil(retryAfterMs)} ms`,
      options,
    );
    this.name = new.target.name;
  }
}
