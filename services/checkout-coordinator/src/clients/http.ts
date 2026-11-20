import type { FastifyInstance } from 'fastify';
import { jitteredRetryDelay, type RetryPolicy } from '../../../../shared/messaging';

/**
 * A downstream call failed.
 *   REJECTED     the service refused (4xx business answer: insufficient funds / credits, a
 *                released reservation, …). Retrying cannot help; the checkout is compensated.
 *   UNAVAILABLE  no answer (network, timeout) or a 5xx / 408 / 429: the outcome is unknown or
 *                transient. Retried with jittered backoff; the same idempotency key makes a retry
 *                safe even if the first attempt did go through.
 */
export class DownstreamError extends Error {
  constructor(
    public readonly service: 'billing-credits' | 'wallet-ledger',
    public readonly kind: 'REJECTED' | 'UNAVAILABLE',
    public readonly status: number | null,
    public readonly code: string,
    message: string
  ) {
    super(message);
    this.name = 'DownstreamError';
  }
}

export interface HttpRequest {
  method: 'GET' | 'POST';
  path: string;
  body?: unknown;
  headers?: Record<string, string>;
}

export interface HttpResponse {
  status: number;
  body: unknown;
}

export type HttpTransport = (request: HttpRequest) => Promise<HttpResponse>;

/** Real HTTP (fetch) against a base URL, with a per-request timeout. */
export function fetchTransport(baseUrl: string, timeoutMs: number): HttpTransport {
  return async (request) => {
    const res = await fetch(new URL(request.path, baseUrl), {
      method: request.method,
      headers: { 'content-type': 'application/json', ...request.headers },
      body: request.body === undefined ? undefined : JSON.stringify(request.body),
      signal: AbortSignal.timeout(timeoutMs),
    });
    const text = await res.text();
    let body: unknown = text;
    try {
      body = text ? JSON.parse(text) : null;
    } catch {
      // not JSON: keep the text
    }
    return { status: res.status, body };
  };
}

/** In-process HTTP through Fastify's injector (same routing, validation and auth as the wire). */
export function injectTransport(app: FastifyInstance): HttpTransport {
  return async (request) => {
    const res = await app.inject({
      method: request.method,
      url: request.path,
      headers: { 'content-type': 'application/json', ...request.headers },
      ...(request.body === undefined ? {} : { payload: JSON.stringify(request.body) }),
    });
    let body: unknown = res.body;
    try {
      body = res.body ? JSON.parse(res.body) : null;
    } catch {
      // not JSON
    }
    return { status: res.statusCode, body };
  };
}

/** Send, and turn anything but a 2xx into a classified DownstreamError. */
export async function call<T>(service: DownstreamError['service'], transport: HttpTransport, request: HttpRequest): Promise<T> {
  let res: HttpResponse;
  try {
    res = await transport(request);
  } catch (err) {
    throw new DownstreamError(service, 'UNAVAILABLE', null, 'UNREACHABLE', `${service} unreachable: ${(err as Error).message}`);
  }
  if (res.status >= 200 && res.status < 300) return res.body as T;
  const body = (res.body ?? {}) as { code?: unknown; error?: unknown };
  const code = typeof body.code === 'string' ? body.code : `HTTP_${res.status}`;
  const message = typeof body.error === 'string' ? body.error : `${service} answered ${res.status}`;
  const transient = res.status >= 500 || res.status === 408 || res.status === 429;
  throw new DownstreamError(service, transient ? 'UNAVAILABLE' : 'REJECTED', res.status, code, message);
}

/** Retry UNAVAILABLE failures with full-jitter backoff; REJECTED ones surface at once. */
export async function withRetries<T>(
  policy: RetryPolicy,
  operation: () => Promise<T>,
  sleep: (ms: number) => Promise<void> = (ms) => new Promise((r) => setTimeout(r, ms))
): Promise<T> {
  for (let attempt = 1; ; attempt++) {
    try {
      return await operation();
    } catch (err) {
      if (!(err instanceof DownstreamError) || err.kind !== 'UNAVAILABLE' || attempt >= policy.maxAttempts) throw err;
      await sleep(jitteredRetryDelay(attempt, policy));
    }
  }
}
