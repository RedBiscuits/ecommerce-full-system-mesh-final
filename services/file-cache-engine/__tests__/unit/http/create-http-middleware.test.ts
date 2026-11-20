import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import { CacheManager } from '../../../src/cache/cache-manager';
import { FsCacheStore } from '../../../src/cache/fs-cache-store';
import { getClientIdentity } from '../../../src/http/client-identity';
import { createHttpMiddleware } from '../../../src/http/create-http-middleware';
import { startServer, type TestServer } from '../../helpers/http-server';
import { makeTempDir, type TempDir } from '../../helpers/temp-dir';

describe('createHttpMiddleware', () => {
  let dir: TempDir;
  let cache: CacheManager;
  let server: TestServer | undefined;

  beforeEach(async () => {
    dir = await makeTempDir();
    const store = await FsCacheStore.open({
      directory: dir.path,
      fsync: false,
    });
    cache = new CacheManager(store, { ttlJitterRatio: 0 });
  });

  afterEach(async () => {
    await server?.close();
    server = undefined;
    await dir.cleanup();
  });

  it('builds an identify → rateLimit → cache stack sharing one resolver', () => {
    const stack = createHttpMiddleware({
      identity: { trustProxy: true },
      rateLimit: { limiter: { capacity: 10, refillPerSecond: 1 } },
      cache,
    });
    expect(stack.stack).toEqual([stack.identify, stack.rateLimit, stack.cache]);
  });

  it('omits disabled stages', () => {
    const stack = createHttpMiddleware();
    expect(stack.rateLimit).toBeUndefined();
    expect(stack.cache).toBeUndefined();
    expect(stack.stack).toEqual([stack.identify]);
  });

  it('accepts full cache middleware options', async () => {
    const stack = createHttpMiddleware({
      cache: { cache, statusHeader: 'X-Served-From', vary: [] },
    });
    server = await startServer(stack.stack, (_req, res) => res.end('ok'));
    const response = await fetch(server.url);
    expect(response.headers.get('x-served-from')).toBe('MISS');
  });

  it('rate limits before the cache so hits still count against the quota', async () => {
    let hits = 0;
    const stack = createHttpMiddleware({
      rateLimit: { limiter: { capacity: 2, refillPerSecond: 0.001 } },
      cache: { cache, vary: [] },
    });
    server = await startServer(stack.stack, (req, res) => {
      hits += 1;
      res.end(`identity:${getClientIdentity(req)?.ip ?? 'none'}`);
    });

    const first = await fetch(server.url);
    await expect(first.text()).resolves.toBe('identity:127.0.0.1');
    const second = await fetch(server.url);
    expect(second.headers.get('ratelimit-remaining')).toBe('0');
    const third = await fetch(server.url);
    expect(third.status).toBe(429);
    expect(third.headers.get('x-cache')).toBeNull();
    expect(hits).toBe(1);
  });
});
