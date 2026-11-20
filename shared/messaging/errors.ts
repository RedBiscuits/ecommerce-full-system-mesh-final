/**
 * Error classification for integration consumers.
 *
 * A consumer handler signals "retrying cannot help" (bad data, a business
 * rule the message violates, a conflict a human must resolve) by throwing a
 * NonRetryableError. The message is then dead-lettered at once instead of
 * burning its retry budget. Anything else is treated as transient and retried
 * with jittered exponential backoff until the attempt budget is exhausted.
 *
 * Services keep their own error types; any error carrying `retryable: false`
 * is classified the same way, so a service does not have to depend on this
 * class to opt out of retries.
 */
export class NonRetryableError extends Error {
  public readonly retryable = false;

  constructor(
    /** Stable machine-readable reason, recorded on the dead-lettered copy. */
    public readonly reason: string,
    message?: string
  ) {
    super(message ?? reason);
    this.name = 'NonRetryableError';
  }
}

export function isNonRetryable(err: unknown): boolean {
  if (err instanceof NonRetryableError) return true;
  return typeof err === 'object' && err !== null && (err as { retryable?: unknown }).retryable === false;
}

/** Reason string recorded in `x-failure-reason` for a failed delivery. */
export function failureReasonOf(err: unknown): string {
  if (err instanceof NonRetryableError) return `${err.reason}: ${err.message}`;
  const reason = typeof err === 'object' && err !== null ? (err as { reason?: unknown }).reason : undefined;
  const message = err instanceof Error ? err.message : String(err);
  return typeof reason === 'string' && reason !== message ? `${reason}: ${message}` : message;
}

export function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

/** Cap header size: error text must not bloat every retried message. */
export function truncate(value: string, max = 500): string {
  return value.length <= max ? value : `${value.slice(0, max)}…`;
}
