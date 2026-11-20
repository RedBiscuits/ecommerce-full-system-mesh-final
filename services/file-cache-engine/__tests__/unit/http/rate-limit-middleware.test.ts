import { afterEach, describe, expect, it, vi } from 'vitest';

import { ClientIdentityResolver } from '../../../src/http/client-identity';
import type { RateLimitMiddlewareOptions } from '../../../src/http/rate-limit-middleware';
import { createRateLimitMiddleware } from '../../../src/http/rate-limit-middleware';
import type { Middleware } from '../../../src/http/types';
import { KeyedRateLimiter } from '../../../src/rate-limit/keyed-rate-limiter';
import { startServer, type TestServer } from '../../helpers/http-server';

describe('createRateLimitMiddleware', () => {
  let server: TestServer | undefined;

  afterEach(async () => {
    await server?.close();
    server = undefined;
  });

  async function serve(middleware: Middleware): Promise<TestServer> {
    server = await startServer([middleware], (_req, res) => {
      res.end('ok');
    });
    return server;
  }

  it('admits up to capacity then answers 429 with Retry-After and RateLimit-* headers', async () => {
    const { url } = await serve(
      createRateLimitMiddleware({
        limiter: { capacity: 2, refillPerSecond: 1 },
      }),
    );

    const first = await fetch(url);
    expect(first.status).toBe(200);
    expect(first.headers.get('ratelimit-limit')).toBe('2');
    expect(first.headers.get('ratelimit-remaining')).toBe('1');
    expect(first.headers.get('ratelimit-reset')).toBe('0');
    expect(first.headers.get('retry-after')).toBeNull();

    await fetch(url);
    const limited = await fetch(url);
    expect(limited.status).toBe(429);
    expect(limited.headers.get('retry-after')).toBe('1');
    expect(limited.headers.get('ratelimit-remaining')).toBe('0');
    expect(limited.headers.get('cache-control')).toBe('no-store');
    expect(limited.headers.get('content-type')).toContain('application/json');
    await expect(limited.json()).resolves.toMatchObject({
      error: 'Too Many Requests',
      retryAfterMs: expect.any(Number),
    });
  });

  it('keys buckets per client identity', async () => {
    const { url } = await serve(
      createRateLimitMiddleware({
        limiter: { capacity: 1, refillPerSecond: 0.001 },
      }),
    );
    expect((await fetch(url)).status).toBe(200);
    expect((await fetch(url)).status).toBe(429);
    // A different API key is a different client.
    expect((await fetch(url, { headers: { 'x-api-key': 'k' } })).status).toBe(
      200,
    );
  });

  it('supports custom key resolvers, exemptions and skips', async () => {
    const limiter = new KeyedRateLimiter({
      capacity: 1,
      refillPerSecond: 0.001,
    });
    const { url } = await serve(
      createRateLimitMiddleware({
        limiter,
        identity: new ClientIdentityResolver(),
        keyResolver: (identity, req) =>
          req.url === '/exempt' ? undefined : (identity.country ?? 'unknown'),
        skip: (req) => req.url === '/health',
      }),
    );
    expect(
      (await fetch(`${url}/`, { headers: { 'cf-ipcountry': 'DE' } })).status,
    ).toBe(200);
    expect(
      (await fetch(`${url}/`, { headers: { 'cf-ipcountry': 'DE' } })).status,
    ).toBe(429);
    expect(
      (await fetch(`${url}/`, { headers: { 'cf-ipcountry': 'FR' } })).status,
    ).toBe(200);
    expect(
      (await fetch(`${url}/exempt`, { headers: { 'cf-ipcountry': 'DE' } }))
        .status,
    ).toBe(200);
    expect(
      (await fetch(`${url}/health`, { headers: { 'cf-ipcountry': 'DE' } }))
        .status,
    ).toBe(200);
    expect(limiter.size).toBe(2);
  });

  it('charges per-request costs', async () => {
    const { url } = await serve(
      createRateLimitMiddleware({
        limiter: { capacity: 3, refillPerSecond: 0.001 },
        cost: (req) => (req.url === '/expensive' ? 3 : 1),
      }),
    );
    expect((await fetch(`${url}/expensive`)).status).toBe(200);
    expect((await fetch(`${url}/cheap`)).status).toBe(429);
  });

  it('can wait briefly for capacity instead of failing fast', async () => {
    const { url } = await serve(
      createRateLimitMiddleware({
        limiter: { capacity: 1, refillPerSecond: 20 }, // token every 50 ms
        waitTimeoutMs: 500,
      }),
    );
    await fetch(url);
    const started = Date.now();
    const second = await fetch(url);
    expect(second.status).toBe(200);
    expect(Date.now() - started).toBeGreaterThanOrEqual(30);
  });

  it('still rejects when the wait times out', async () => {
    const { url } = await serve(
      createRateLimitMiddleware({
        limiter: { capacity: 1, refillPerSecond: 0.1 }, // token every 10 s
        waitTimeoutMs: 20,
      }),
    );
    await fetch(url);
    expect((await fetch(url)).status).toBe(429);
  });

  it('emits legacy / both / no header styles', async () => {
    for (const [style, expected] of [
      ['legacy', { std: false, legacy: true }],
      ['both', { std: true, legacy: true }],
      [false, { std: false, legacy: false }],
    ] as const) {
      const { url } = await serve(
        createRateLimitMiddleware({
          limiter: { capacity: 5, refillPerSecond: 1 },
          headers: style,
        }),
      );
      const response = await fetch(url);
      expect(response.headers.has('ratelimit-limit')).toBe(expected.std);
      expect(response.headers.has('x-ratelimit-limit')).toBe(expected.legacy);
      await server?.close();
      server = undefined;
    }
  });

  it('lets callers customise the 429 response', async () => {
    const onLimited = vi.fn<
      NonNullable<RateLimitMiddlewareOptions['onLimited']>
    >((_req, res, info) => {
      res.setHeader('content-type', 'text/plain');
      res.end(`slow down ${info.key}`);
    });
    const { url } = await serve(
      createRateLimitMiddleware({
        limiter: { capacity: 1, refillPerSecond: 0.001 },
        onLimited,
      }),
    );
    await fetch(url);
    const limited = await fetch(url);
    expect(limited.status).toBe(429);
    await expect(limited.text()).resolves.toBe('slow down net:127.0.0.1');
    expect(onLimited).toHaveBeenCalledOnce();
  });

  it('forwards unexpected failures to next(error)', async () => {
    const { url } = await serve(
      createRateLimitMiddleware({
        limiter: { capacity: 1, refillPerSecond: 1 },
        keyResolver: () => {
          throw new Error('resolver exploded');
        },
      }),
    );
    const response = await fetch(url);
    expect(response.status).toBe(500);
    await expect(response.text()).resolves.toBe('resolver exploded');
  });
});
