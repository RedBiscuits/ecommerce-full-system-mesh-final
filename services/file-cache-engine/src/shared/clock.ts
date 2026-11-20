/**
 * Minimal time source abstraction.
 *
 * Every time-dependent component (TTL checks, token refill, sweeps) depends on
 * this interface instead of `Date.now()` directly so behaviour can be tested
 * deterministically and, if needed, driven by a monotonic source.
 */
export interface Clock {
  /** Current time as milliseconds since the Unix epoch. */
  now(): number;
}

/** Default wall-clock implementation backed by `Date.now()`. */
export const systemClock: Clock = {
  now(): number {
    return Date.now();
  },
};
