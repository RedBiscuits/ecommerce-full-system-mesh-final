import type { IncomingMessage, ServerResponse } from 'node:http';

/**
 * Framework-agnostic middleware contract.
 *
 * Typed against Node's own request/response so the same function works with
 * `http.createServer`, Connect, and Express (whose `Request`/`Response`
 * extend `IncomingMessage`/`ServerResponse`). Errors are always forwarded via
 * `next(error)` — never thrown, never left as an unhandled rejection — so the
 * middlewares are safe under both Express 4 and 5.
 */
export type NextFunction = (error?: unknown) => void;

export type Middleware = (
  req: IncomingMessage,
  res: ServerResponse,
  next: NextFunction,
) => void;

export type { IncomingMessage, ServerResponse };
