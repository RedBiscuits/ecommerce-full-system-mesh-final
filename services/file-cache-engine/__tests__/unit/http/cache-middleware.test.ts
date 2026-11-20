import type { IncomingMessage, ServerResponse } from 'node:http';

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { CacheManager } from '../../../src/cache/cache-manager';
import { FsCacheStore } from '../../../src/cache/fs-cache-store';
import type { CacheMiddlewareOptions } from '../../../src/http/cache-middleware';
import { createCacheMiddleware } from '../../../src/http/cache-middleware';
import { FakeClock } from '../../helpers/fake-clock';
import {
  rawGet,
  startServer,
  type Handler,
  type TestServer,
} from '../../helpers/http-server';
import { makeTempDir, type TempDir } from '../../helpers/temp-dir';

const TTL = 10_000;
const SWR = 5_000;

describe('createCacheMiddleware', () => {
  let dir: TempDir;
  let clock: FakeClock;
  let cache: CacheManager;
  let server: TestServer | undefined;
  let hits: number;

  beforeEach(async () => {
    dir = await makeTempDir();
    clock = new FakeClock();
    const store = await FsCacheStore.open({
      directory: dir.path,
      fsync: false,
    });
    cache = new CacheManager(store, {
      clock,
      ttlMs: TTL,
      staleWhileRevalidateMs: SWR,
      ttlJitterRatio: 0,
    });
    hits = 0;
  });

  afterEach(async () => {
    await server?.close();
    server = undefined;
    await dir.cleanup();
  });

  /** Origin that counts invocations and echoes a body (optionally delayed). */
  function origin(
    body: (req: IncomingMessage) => string = () => 'origin-body',
    decorate: (res: ServerResponse) => void = () => undefined,
    delayMs = 0,
  ): Handler {
    return (req, res) => {
      hits += 1;
      const send = (): void => {
        res.setHeader('content-type', 'text/plain');
        decorate(res);
        res.end(body(req));
      };
      if (delayMs > 0) {
        setTimeout(send, delayMs);
      } else {
        send();
      }
    };
  }

  async function serve(
    handler: Handler,
    options: Partial<CacheMiddlewareOptions> = {},
  ): Promise<string> {
    server = await startServer(
      [createCacheMiddleware({ cache, clock, vary: [], ...options })],
      handler,
    );
    return server.url;
  }

  /** Default cache key for `url` (with `vary: []`). */
  const keyFor = (url: string): string => {
    const { host, pathname, search } = new URL(url);
    return `GET ${host}${pathname}${search}`;
  };

  /**
   * The entry is persisted *after* the response is flushed (write-behind),
   * so wait for the in-flight load to settle before asserting on the cache.
   */
  const settled = (url: string): Promise<void> =>
    vi.waitFor(() => expect(cache.isLoading(keyFor(url))).toBe(false));

  /** `fetch` that also waits for any resulting cache write to land. */
  async function get(url: string, init?: RequestInit): Promise<Response> {
    const response = await fetch(url, init);
    await settled(url);
    return response;
  }

  describe('hits and misses', () => {
    it('serves the second GET from cache with HIT, Age and Content-Length', async () => {
      const url = await serve(origin());

      const miss = await get(url);
      expect(miss.headers.get('x-cache')).toBe('MISS');
      await expect(miss.text()).resolves.toBe('origin-body');

      clock.advance(2_500);
      const hit = await get(url);
      expect(hit.status).toBe(200);
      expect(hit.headers.get('x-cache')).toBe('HIT');
      expect(hit.headers.get('age')).toBe('2');
      expect(hit.headers.get('content-type')).toBe('text/plain');
      expect(hit.headers.get('content-length')).toBe('11');
      await expect(hit.text()).resolves.toBe('origin-body');
      expect(hits).toBe(1);
    });

    it('keys by host, path, query and the vary headers', async () => {
      const url = await serve(
        origin((req) => `path:${req.url}`),
        {
          vary: ['accept-encoding'],
        },
      );
      await expect((await get(`${url}/a`)).text()).resolves.toBe('path:/a');
      await expect((await get(`${url}/a?x=1`)).text()).resolves.toBe(
        'path:/a?x=1',
      );
      await expect((await get(`${url}/a`)).text()).resolves.toBe('path:/a');
      expect(hits).toBe(2);

      await fetch(`${url}/a`, { headers: { 'accept-encoding': 'br' } });
      await vi.waitFor(() => expect(hits).toBe(3));
    });

    it('only caches GET; other methods pass straight through', async () => {
      const url = await serve(origin());
      await get(url, { method: 'POST' });
      const post = await get(url, { method: 'POST' });
      expect(post.headers.get('x-cache')).toBeNull();
      expect(hits).toBe(2);
    });

    it('answers HEAD from a cached GET and passes HEAD misses through', async () => {
      const url = await serve(origin());
      const headMiss = await get(url, { method: 'HEAD' });
      expect(headMiss.headers.get('x-cache')).toBe('MISS');
      expect(hits).toBe(1);

      await get(url);
      const headHit = await get(url, { method: 'HEAD' });
      expect(headHit.status).toBe(200);
      expect(headHit.headers.get('x-cache')).toBe('HIT');
      expect(headHit.headers.get('content-length')).toBe('11');
      await expect(headHit.text()).resolves.toBe('');
      expect(hits).toBe(2);
    });

    it('bypasses the cache for private requests (Authorization / Cookie)', async () => {
      const url = await serve(origin());
      const headerList: Array<Record<string, string>> = [
        { authorization: 'Bearer t' },
        { cookie: 'sid=1' },
        { authorization: 'Bearer t' },
      ];
      for (const headers of headerList) {
        const response = await get(url, { headers });
        expect(response.headers.get('x-cache')).toBe('BYPASS');
      }
      expect(hits).toBe(3);
    });

    it('honours request Cache-Control: no-store (bypass) and no-cache (revalidate)', async () => {
      const url = await serve(origin());
      await get(url);
      expect(
        (
          await get(url, { headers: { 'cache-control': 'no-store' } })
        ).headers.get('x-cache'),
      ).toBe('BYPASS');
      expect(hits).toBe(2);

      expect(
        (
          await get(url, { headers: { 'cache-control': 'no-cache' } })
        ).headers.get('x-cache'),
      ).toBe('MISS');
      expect(hits).toBe(3);
      expect((await get(url)).headers.get('x-cache')).toBe('HIT');
    });

    it('supports a custom key resolver that can opt out', async () => {
      const url = await serve(origin(), {
        keyResolver: (req) => (req.url === '/never' ? undefined : 'fixed'),
      });
      await get(`${url}/a`);
      expect((await get(`${url}/b`)).headers.get('x-cache')).toBe('HIT');
      expect((await get(`${url}/never`)).headers.get('x-cache')).toBe('BYPASS');
      expect(hits).toBe(2);
    });

    it('can rename or omit the status header', async () => {
      const url = await serve(origin(), { statusHeader: false });
      const response = await get(url);
      expect(response.headers.get('x-cache')).toBeNull();
    });
  });

  describe('response cacheability', () => {
    it.each([
      [
        'Cache-Control: no-store',
        (res: ServerResponse): void => {
          res.setHeader('cache-control', 'no-store');
        },
      ],
      [
        'Cache-Control: private',
        (res: ServerResponse): void => {
          res.setHeader('cache-control', 'private, max-age=60');
        },
      ],
      [
        'Cache-Control: no-cache',
        (res: ServerResponse): void => {
          res.setHeader('cache-control', 'no-cache');
        },
      ],
      [
        'max-age=0',
        (res: ServerResponse): void => {
          res.setHeader('cache-control', 'max-age=0');
        },
      ],
      [
        'Set-Cookie',
        (res: ServerResponse): void => {
          res.setHeader('set-cookie', 'sid=1');
        },
      ],
      [
        'Vary: *',
        (res: ServerResponse): void => {
          res.setHeader('vary', '*');
        },
      ],
      [
        '500 status',
        (res: ServerResponse): void => {
          res.statusCode = 500;
        },
      ],
    ])('never stores a response with %s', async (_label, decorate) => {
      const url = await serve(origin(() => 'x', decorate));
      await get(url);
      await get(url);
      expect(hits).toBe(2);
    });

    it('takes freshness from s-maxage / max-age / stale-while-revalidate', async () => {
      const url = await serve(
        origin(
          () => 'x',
          (res) =>
            res.setHeader(
              'cache-control',
              'public, max-age=10, s-maxage=60, stale-while-revalidate=30',
            ),
        ),
      );
      await get(url);
      const metadata = await cache.head(keyFor(url));
      expect(metadata).toBeDefined();
      expect(metadata!.expiresAt - metadata!.createdAt).toBe(60_000);
      expect(metadata!.staleUntil - metadata!.expiresAt).toBe(30_000);
    });

    it('falls back to the configured ttl when the response is silent', async () => {
      const url = await serve(origin(), {
        ttlMs: 1_234,
        staleWhileRevalidateMs: 0,
      });
      await get(url);
      const metadata = await cache.head(keyFor(url));
      expect(metadata!.expiresAt - metadata!.createdAt).toBe(1_234);
      expect(metadata!.staleUntil).toBe(metadata!.expiresAt);
    });

    it('can ignore response Cache-Control entirely', async () => {
      const url = await serve(
        origin(
          () => 'x',
          (res) => res.setHeader('cache-control', 'no-store'),
        ),
        { respectCacheControl: false },
      );
      await get(url);
      expect((await get(url)).headers.get('x-cache')).toBe('HIT');
    });

    it('streams oversized responses through without storing them', async () => {
      const url = await serve(
        origin(() => 'x'.repeat(100)),
        { maxEntryBytes: 50 },
      );
      await expect((await get(url)).text()).resolves.toHaveLength(100);
      expect((await get(url)).headers.get('x-cache')).toBe('MISS');
      expect(hits).toBe(2);
    });

    it('caches statuses from the configurable list (404 by default, 418 opt-in)', async () => {
      const url = await serve(
        origin(
          (req) => req.url ?? '',
          (res) => {
            res.statusCode = 404;
          },
        ),
      );
      await get(url);
      expect((await get(url)).status).toBe(404);
      expect(hits).toBe(1);

      await server?.close();
      hits = 0;
      const teapot = await serve(
        origin(
          () => 'tea',
          (res) => {
            res.statusCode = 418;
          },
        ),
        { cacheableStatuses: [418] },
      );
      await get(teapot);
      expect((await get(teapot)).headers.get('x-cache')).toBe('HIT');
      expect(hits).toBe(1);
    });

    it('captures responses written via writeHead and chunked writes', async () => {
      const url = await serve((_req, res) => {
        hits += 1;
        res.writeHead(200, {
          'content-type': 'application/json',
          etag: '"v1"',
        });
        res.write('{"a":');
        res.end('1}');
      });
      await get(url);
      const hit = await get(url);
      expect(hit.headers.get('x-cache')).toBe('HIT');
      expect(hit.headers.get('content-type')).toBe('application/json');
      expect(hit.headers.get('etag')).toBe('"v1"');
      await expect(hit.json()).resolves.toEqual({ a: 1 });
      expect(hits).toBe(1);
    });

    it('does not replay Set-Cookie or hop-by-hop headers and recomputes length', async () => {
      const url = await serve(
        origin(
          () => 'abc',
          (res) => {
            res.setHeader('connection', 'close');
            res.setHeader('x-custom', ['one', 'two']);
          },
        ),
      );
      await get(url);
      const hit = await get(url);
      expect(hit.headers.get('x-cache')).toBe('HIT');
      expect(hit.headers.get('x-custom')).toBe('one, two');
      expect(hit.headers.get('content-length')).toBe('3');
    });
  });

  describe('conditional requests', () => {
    // `fetch` adds `Cache-Control: no-cache` to conditional requests (per the
    // Fetch spec), which rightly forces revalidation — so use a raw client.
    it('answers 304 from cache when If-None-Match matches the stored ETag', async () => {
      const url = await serve(
        origin(
          () => 'abc',
          (res) => res.setHeader('etag', '"v1"'),
        ),
      );
      await get(url);
      const conditional = await rawGet(url, { 'if-none-match': '"v1"' });
      expect(conditional.status).toBe(304);
      expect(conditional.headers['etag']).toBe('"v1"');
      expect(conditional.headers['x-cache']).toBe('HIT');
      expect(conditional.headers['content-type']).toBeUndefined();
      expect(conditional.body).toBe('');

      const mismatch = await rawGet(url, { 'if-none-match': '"v0"' });
      expect(mismatch.status).toBe(200);
      expect(mismatch.body).toBe('abc');
      expect(hits).toBe(1);
    });

    it('honours If-Modified-Since against a stored Last-Modified', async () => {
      const url = await serve(
        origin(
          () => 'abc',
          (res) =>
            res.setHeader('last-modified', 'Wed, 21 Oct 2015 07:28:00 GMT'),
        ),
      );
      await get(url);
      const notModified = await rawGet(url, {
        'if-modified-since': 'Thu, 22 Oct 2015 07:28:00 GMT',
      });
      expect(notModified.status).toBe(304);
      const modified = await rawGet(url, {
        'if-modified-since': 'Tue, 20 Oct 2015 07:28:00 GMT',
      });
      expect(modified.status).toBe(200);
      expect(hits).toBe(1);
    });

    it('does not leak its own diagnostic header into stored responses', async () => {
      const url = await serve(origin());
      await get(url);
      const metadata = await cache.head(keyFor(url));
      expect(metadata?.attributes['headers']).not.toContain('x-cache');
    });
  });

  describe('stampede protection', () => {
    it('coalesces concurrent misses: one origin call, followers replayed', async () => {
      const url = await serve(origin(() => 'slow', undefined, 50));
      const responses = await Promise.all(
        Array.from({ length: 20 }, () => fetch(url)),
      );
      const bodies = await Promise.all(responses.map((r) => r.text()));
      expect(bodies.every((b) => b === 'slow')).toBe(true);
      const statuses = responses.map((r) => r.headers.get('x-cache'));
      expect(statuses.filter((s) => s === 'MISS')).toHaveLength(1);
      expect(statuses.filter((s) => s === 'HIT')).toHaveLength(19);
      expect(hits).toBe(1);
    });

    it('never replays an uncacheable (private) leader response to followers', async () => {
      const url = await serve(
        origin(
          (req) => `secret-for-${req.headers['x-user'] ?? 'anon'}`,
          (res) => res.setHeader('cache-control', 'private'),
          50,
        ),
      );
      const responses = await Promise.all(
        ['a', 'b', 'c'].map((user) =>
          fetch(url, { headers: { 'x-user': user } }),
        ),
      );
      const bodies = await Promise.all(responses.map((r) => r.text()));
      expect(bodies.sort()).toEqual([
        'secret-for-a',
        'secret-for-b',
        'secret-for-c',
      ]);
      expect(hits).toBe(3);
    });

    it('serves stale while one request revalidates', async () => {
      const url = await serve(origin(() => `v${hits}`, undefined, 300));
      await get(url); // v1 stored
      clock.advance(TTL + 1); // now stale

      const revalidator = fetch(url);
      await vi.waitFor(() => expect(cache.isLoading(keyFor(url))).toBe(true), {
        interval: 5,
      });
      const staleHit = await fetch(url);
      expect(staleHit.headers.get('x-cache')).toBe('STALE');
      await expect(staleHit.text()).resolves.toBe('v1');

      const fresh = await revalidator;
      expect(fresh.headers.get('x-cache')).toBe('MISS');
      await expect(fresh.text()).resolves.toBe('v2');
      expect(hits).toBe(2);

      await settled(url); // write-behind of the revalidated entry
      expect((await get(url)).headers.get('x-cache')).toBe('HIT');
    });

    it('treats fully expired entries as misses', async () => {
      const url = await serve(origin());
      await get(url);
      clock.advance(TTL + SWR + 1);
      expect((await get(url)).headers.get('x-cache')).toBe('MISS');
      expect(hits).toBe(2);
    });
  });

  describe('resilience', () => {
    it('degrades to origin when the cache read fails', async () => {
      const onError = vi.fn<NonNullable<CacheMiddlewareOptions['onError']>>();
      vi.spyOn(cache, 'get').mockRejectedValueOnce(new Error('disk gone'));
      const url = await serve(origin(), { onError });
      const response = await get(url);
      expect(response.status).toBe(200);
      expect(response.headers.get('x-cache')).toBe('BYPASS');
      expect(onError).toHaveBeenCalledOnce();
    });

    it('still answers the client when the cache write fails', async () => {
      const onError = vi.fn<NonNullable<CacheMiddlewareOptions['onError']>>();
      vi.spyOn(cache, 'put');
      vi.spyOn(cache, 'getOrLoad').mockRejectedValueOnce(
        new Error('write failed'),
      );
      const url = await serve(origin(), { onError });
      const response = await get(url);
      expect(response.status).toBe(200);
      await expect(response.text()).resolves.toBe('origin-body');
      expect(onError).toHaveBeenCalledOnce();
    });

    it('does not cache when the client disconnects mid-response', async () => {
      const controller = new AbortController();
      const url = await serve((_req, res) => {
        hits += 1;
        res.write('partial');
        setTimeout(() => controller.abort(), 20);
        res.on('close', () => {
          if (!res.writableEnded) {
            res.destroy();
          }
        });
      });
      await fetch(url, { signal: controller.signal }).catch(() => undefined);
      await settled(url);
      await expect(cache.head(keyFor(url))).resolves.toBeUndefined();
    });
  });
});
