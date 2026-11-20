import { sleep } from '../shared/sleep';
import { classifyFailure } from './errors';

export interface RetryPolicy {
  /** Total attempts including the first. Default: `3`. */
  readonly maxAttempts: number;
  /** Delay before the second attempt; doubles each time. Default: 20 ms. */
  readonly baseDelayMs: number;
  /** Upper bound for a single delay. Default: 500 ms. */
  readonly maxDelayMs: number;
  /** ±ratio randomisation so retries from many callers do not align. Default: `0.5`. */
  readonly jitterRatio: number;
  /** Decides whether an error is worth another attempt. Default: transient errors only. */
  readonly shouldRetry: (error: unknown, attempt: number) => boolean;
}

export const DEFAULT_RETRY_POLICY: RetryPolicy = {
  maxAttempts: 3,
  baseDelayMs: 20,
  maxDelayMs: 500,
  jitterRatio: 0.5,
  shouldRetry: (error): boolean => classifyFailure(error) === 'transient',
};

export interface RetryOptions {
  readonly signal?: AbortSignal;
  /** Observability hook invoked before each backoff sleep. */
  readonly onRetry?: (error: unknown, attempt: number, delayMs: number) => void;
  readonly random?: () => number;
}

/** Delay for the attempt that just failed (1-based), with jitter applied. */
export function backoffDelay(
  policy: RetryPolicy,
  attempt: number,
  random: () => number = Math.random,
): number {
  const exponential = Math.min(
    policy.maxDelayMs,
    policy.baseDelayMs * 2 ** (attempt - 1),
  );
  const jitter = 1 + (random() * 2 - 1) * policy.jitterRatio;
  return Math.max(0, Math.round(exponential * jitter));
}

/**
 * Runs `task` until it succeeds, the policy gives up, or `signal` aborts.
 * The last error is rethrown unchanged so callers can classify it.
 */
export async function withRetry<T>(
  task: (attempt: number) => Promise<T>,
  policy: Partial<RetryPolicy> = {},
  options: RetryOptions = {},
): Promise<T> {
  const effective: RetryPolicy = { ...DEFAULT_RETRY_POLICY, ...policy };
  const maxAttempts = Math.max(1, effective.maxAttempts);

  for (let attempt = 1; ; attempt += 1) {
    options.signal?.throwIfAborted();
    try {
      return await task(attempt);
    } catch (error) {
      if (attempt >= maxAttempts || !effective.shouldRetry(error, attempt)) {
        throw error;
      }
      const delay = backoffDelay(effective, attempt, options.random);
      options.onRetry?.(error, attempt, delay);
      await sleep(delay, { signal: options.signal });
    }
  }
}
