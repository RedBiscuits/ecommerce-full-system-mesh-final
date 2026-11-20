/**
 * src/utils/backoff.ts
 *
 * Retry delays with jitter. Retrying after a *fixed* backoff makes every
 * worker that backed off at the same moment come back at the same moment —
 * the thundering herd that turns one lock collision into a dozen. "Full
 * jitter" (the AWS Architecture Blog recipe) spreads them out:
 *
 *   delay(attempt) = random(min, min(max, base × 2^attempt))
 *
 * `attempt` is 0-based: the first retry waits up to `base`, the second up to
 * `2 × base`, … capped at `max`. `min` keeps a retry from being scheduled
 * immediately.
 */

export interface JitterOptions {
  baseMs: number;
  maxMs: number;
  /** Lower bound for any delay. Default 0. */
  minMs?: number;
  /** Injectable source of [0, 1) randomness. */
  random?: () => number;
}

/** The (exclusive) upper bound of the window for `attempt`, before jitter. */
export function backoffCeiling(attempt: number, { baseMs, maxMs }: Pick<JitterOptions, 'baseMs' | 'maxMs'>): number {
  return Math.min(maxMs, baseMs * 2 ** Math.max(0, attempt));
}

/** A full-jitter delay for the given 0-based attempt. */
export function jitteredDelay(attempt: number, options: JitterOptions): number {
  const random = options.random ?? Math.random;
  const minMs = options.minMs ?? 0;
  const ceiling = Math.max(minMs, backoffCeiling(attempt, options));
  return Math.round(minMs + random() * (ceiling - minMs));
}
