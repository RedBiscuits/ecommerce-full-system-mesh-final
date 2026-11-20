import type {
  IncomingHttpHeaders,
  IncomingMessage,
  Server,
  ServerResponse,
} from 'node:http';
import { createServer, request } from 'node:http';
import type { AddressInfo } from 'node:net';

import type { Middleware } from '../../src/http/types';

export type Handler = (req: IncomingMessage, res: ServerResponse) => void;

export interface TestServer {
  readonly url: string;
  readonly server: Server;
  close(): Promise<void>;
}

/**
 * Minimal Connect-style composer: runs `middlewares` in order, then
 * `handler`. Errors passed to `next` become a 500 — the same contract
 * Express' default error handler provides.
 */
export function compose(middlewares: readonly Middleware[], handler: Handler) {
  return (req: IncomingMessage, res: ServerResponse): void => {
    let index = 0;
    const next = (error?: unknown): void => {
      if (error !== undefined) {
        res.statusCode = 500;
        res.end(error instanceof Error ? error.message : String(error));
        return;
      }
      const middleware = middlewares[index++];
      if (middleware === undefined) {
        handler(req, res);
        return;
      }
      middleware(req, res, next);
    };
    next();
  };
}

export async function startServer(
  middlewares: readonly Middleware[],
  handler: Handler,
): Promise<TestServer> {
  const server = createServer(compose(middlewares, handler));
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address() as AddressInfo;
  return {
    url: `http://127.0.0.1:${port}`,
    server,
    close: () =>
      new Promise<void>((resolve, reject) => {
        server.closeAllConnections();
        server.close((error) => (error ? reject(error) : resolve()));
      }),
  };
}

export interface RawResponse {
  readonly status: number;
  readonly headers: IncomingHttpHeaders;
  readonly body: string;
}

/**
 * Bare `http.request` GET. Unlike `fetch`, it sends *only* the headers given
 * (undici adds `Cache-Control: no-cache` to conditional requests).
 */
export function rawGet(
  url: string,
  headers: Record<string, string> = {},
): Promise<RawResponse> {
  return new Promise((resolve, reject) => {
    const req = request(url, { method: 'GET', headers }, (res) => {
      const chunks: Buffer[] = [];
      res.on('data', (chunk: Buffer) => chunks.push(chunk));
      res.on('end', () =>
        resolve({
          status: res.statusCode ?? 0,
          headers: res.headers,
          body: Buffer.concat(chunks).toString(),
        }),
      );
      res.on('error', reject);
    });
    req.on('error', reject);
    req.end();
  });
}
