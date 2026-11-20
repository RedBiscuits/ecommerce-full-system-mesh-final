/**
 * src/utils/httpError.ts
 *
 * Error carrying an HTTP status. The global errorHandler middleware reads
 * `status` and renders `{ error: { status, message } }`.
 */

export class HttpError extends Error {
  public readonly status: number;

  constructor(status: number, message: string) {
    super(message);
    this.name = 'HttpError';
    this.status = status;
  }
}
