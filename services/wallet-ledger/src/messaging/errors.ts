/**
 * Thrown by a consumer when retrying cannot help (e.g. the ledger itself is not
 * trustworthy). The message is dead-lettered immediately for manual handling.
 */
export class NonRetryableError extends Error {
  constructor(
    public readonly reason: string,
    message?: string
  ) {
    super(message ?? reason);
    this.name = 'NonRetryableError';
  }
}

/** The broker refused a publish (e.g. queue at x-max-length with reject-publish). */
export class PublishRejectedError extends Error {
  constructor(destination: string, cause?: unknown) {
    super(`Broker rejected publish to ${destination}${cause instanceof Error ? `: ${cause.message}` : ''}`);
    this.name = 'PublishRejectedError';
  }
}
