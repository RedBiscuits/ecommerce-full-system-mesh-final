/**
 * Retry delays with "full jitter" (AWS Architecture Blog):
 *
 *   delay(attempt) = random(min, min(max, base × 2^(attempt − 1)))
 *
 * Retrying after a fixed delay makes every consumer that failed at the same
 * moment (a dependency blip) come back at the same moment and fail again;
 * spreading retries over the whole window breaks that herd. `attempt` is the
 * 1-based number of the attempt that just failed.
 */
export interface RetryPolicy {
  /** Total delivery attempts (first try included) before a message is dead-lettered. */
  maxAttempts: number;
  /** Ceiling of the jitter window for the first retry. */
  baseDelayMs: number;
  /** Upper bound of any jitter window. */
  maxDelayMs: number;
  /** Lower bound of any delay, so a retry is never scheduled immediately. */
  minDelayMs?: number;
  /** Injectable [0, 1) source, for deterministic tests. */
  random?: () => number;
}

export const DEFAULT_RETRY_POLICY: RetryPolicy = {
  maxAttempts: 5,
  baseDelayMs: 1_000,
  maxDelayMs: 60_000,
  minDelayMs: 100,
};

export function assertValidRetryPolicy(policy: RetryPolicy): void {
  const { maxAttempts, baseDelayMs, maxDelayMs, minDelayMs = 0 } = policy;
  if (!Number.isInteger(maxAttempts) || maxAttempts < 1) throw new Error('retry.maxAttempts must be a positive integer');
  for (const [name, value] of Object.entries({ baseDelayMs, maxDelayMs, minDelayMs })) {
    if (!Number.isInteger(value) || value < 0) throw new Error(`retry.${name} must be a non-negative integer`);
  }
  if (minDelayMs > maxDelayMs) throw new Error('retry.minDelayMs must not exceed retry.maxDelayMs');
}

/** Exclusive upper bound of the jitter window after the given failed attempt. */
export function retryCeiling(attempt: number, policy: RetryPolicy): number {
  const exponent = Math.max(0, attempt - 1);
  return Math.min(policy.maxDelayMs, policy.baseDelayMs * 2 ** exponent);
}

/** Full-jitter delay before the retry that follows failed attempt `attempt`. */
export function jitteredRetryDelay(attempt: number, policy: RetryPolicy): number {
  const random = policy.random ?? Math.random;
  const min = policy.minDelayMs ?? 0;
  const ceiling = Math.max(min, retryCeiling(attempt, policy));
  return Math.round(min + random() * (ceiling - min));
}
