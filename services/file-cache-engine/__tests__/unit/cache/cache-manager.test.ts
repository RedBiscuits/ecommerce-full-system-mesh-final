import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import type {
  CacheManagerOptions,
  Loader,
} from '../../../src/cache/cache-manager';
import { CacheManager } from '../../../src/cache/cache-manager';
import {
  EntryTooLargeError,
  InvalidCacheKeyError,
} from '../../../src/cache/errors';
import { FsCacheStore } from '../../../src/cache/fs-cache-store';
import type { RateLimiter } from '../../../src/rate-limit/types';
import { FakeClock } from '../../helpers/fake-clock';
import { makeTempDir, type TempDir } from '../../helpers/temp-dir';

const TTL = 10_000;
const SWR = 5_000;

describe('CacheManager', () => {
  let dir: TempDir;
  let store: FsCacheStore;
  let clock: FakeClock;
  let cache: CacheManager;

  beforeEach(async () => {
    dir = await makeTempDir();
    store = await FsCacheStore.open({ directory: dir.path, fsync: false });
    clock = new FakeClock();
    cache = new CacheManager(store, {
      clock,
      ttlMs: TTL,
      staleWhileRevalidateMs: SWR,
      ttlJitterRatio: 0,
    });
  });

  afterEach(async () => {
    await dir.cleanup();
  });

  describe('put / get / head / delete', () => {
    it('stores strings and bytes and reads them back fresh', async () => {
      await cache.put('s', 'hello');
      await cache.put('b', new Uint8Array([1, 2, 3]));

      const s = await cache.get('s');
      expect(s?.value.toString()).toBe('hello');
      expect(s?.freshness).toBe('fresh');
      expect(Buffer.from((await cache.get('b'))?.value ?? [])).toEqual(
        Buffer.from([1, 2, 3]),
      );
    });

    it('computes lifetime from the clock and options', async () => {
      const metadata = await cache.put('k', 'v', {
        ttlMs: 1_000,
        staleWhileRevalidateMs: 500,
        attributes: { etag: '"abc"' },
      });
      expect(metadata).toMatchObject({
        createdAt: clock.now(),
        expiresAt: clock.now() + 1_000,
        staleUntil: clock.now() + 1_500,
        attributes: { etag: '"abc"' },
      });
    });

    it('treats stale entries as a miss unless allowStale is set', async () => {
      await cache.put('k', 'v');
      clock.advance(TTL + 1);

      await expect(cache.get('k')).resolves.toBeUndefined();
      const stale = await cache.get('k', { allowStale: true });
      expect(stale?.freshness).toBe('stale');
      expect((await cache.head('k'))?.freshness).toBe('stale');
    });

    it('treats expired entries as a miss without deleting them (read path is write-free)', async () => {
      await cache.put('k', 'v');
      clock.advance(TTL + SWR + 1);

      await expect(
        cache.get('k', { allowStale: true }),
      ).resolves.toBeUndefined();
      expect((await cache.head('k'))?.freshness).toBe('expired');
      await expect(store.readMetadata('k')).resolves.toBeDefined();
    });

    it('delete is idempotent and head reports a miss afterwards', async () => {
      await cache.put('k', 'v');
      await expect(cache.delete('k')).resolves.toBe(true);
      await expect(cache.delete('k')).resolves.toBe(false);
      await expect(cache.head('k')).resolves.toBeUndefined();
    });

    it('validates keys and sizes up front', async () => {
      await expect(cache.get('')).rejects.toThrow(InvalidCacheKeyError);
      const bounded = new CacheManager(store, { maxEntryBytes: 4, clock });
      await expect(bounded.put('k', 'toolong')).rejects.toThrow(
        EntryTooLargeError,
      );
    });

    it('rejects nonsensical configuration', () => {
      expect(() => new CacheManager(store, { ttlMs: 0 })).toThrow(RangeError);
      expect(() => new CacheManager(store, { ttlJitterRatio: 1 })).toThrow(
        RangeError,
      );
    });
  });

  describe('TTL jitter', () => {
    it('spreads expiry within ±ratio so entries do not expire in lock-step', async () => {
      const randoms = [0, 0.5, 1 - Number.EPSILON];
      let i = 0;
      const jittered = new CacheManager(store, {
        clock,
        ttlMs: 1_000,
        staleWhileRevalidateMs: 0,
        ttlJitterRatio: 0.2,
        random: (): number => randoms[i++ % randoms.length] ?? 0,
      });
      const lo = await jittered.put('lo', 'v');
      const mid = await jittered.put('mid', 'v');
      const hi = await jittered.put('hi', 'v');

      expect(lo.expiresAt - lo.createdAt).toBe(800);
      expect(mid.expiresAt - mid.createdAt).toBe(1_000);
      expect(hi.expiresAt - hi.createdAt).toBe(1_200);
    });
  });

  describe('getOrLoad', () => {
    it('calls the loader on a miss, stores the value, and serves from cache afterwards', async () => {
      const loader = vi.fn<Loader>(async () => 'origin');

      const first = await cache.getOrLoad('k', loader);
      expect(first.source).toBe('loader');
      expect(first.stored).toBe(true);
      expect(first.value.toString()).toBe('origin');

      const second = await cache.getOrLoad('k', loader);
      expect(second.source).toBe('cache');
      expect(loader).toHaveBeenCalledTimes(1);
    });

    it('coalesces concurrent misses into a single loader call and a single write', async () => {
      const gate = Promise.withResolvers<void>();
      const loader = vi.fn<Loader>(async () => {
        await gate.promise;
        return 'origin';
      });
      const writeSpy = vi.spyOn(store, 'write');

      const results = Promise.all(
        Array.from({ length: 50 }, () => cache.getOrLoad('hot', loader)),
      );
      await vi.waitFor(() => expect(loader).toHaveBeenCalledTimes(1));
      gate.resolve();

      const settled = await results;
      expect(settled.every((r) => r.value.toString() === 'origin')).toBe(true);
      expect(loader).toHaveBeenCalledTimes(1);
      expect(writeSpy).toHaveBeenCalledTimes(1);
    });

    it('serves stale immediately and revalidates once in the background', async () => {
      await cache.put('k', 'v1');
      clock.advance(TTL + 1);

      const gate = Promise.withResolvers<void>();
      const loader = vi.fn<Loader>(async ({ stale }) => {
        expect(stale?.value.toString()).toBe('v1');
        await gate.promise;
        return 'v2';
      });

      const hits = await Promise.all(
        Array.from({ length: 10 }, () => cache.getOrLoad('k', loader)),
      );
      expect(hits.every((h) => h.source === 'stale')).toBe(true);
      expect(hits.every((h) => h.value.toString() === 'v1')).toBe(true);
      await vi.waitFor(() => expect(loader).toHaveBeenCalledTimes(1));

      gate.resolve();
      await vi.waitFor(async () => {
        expect((await cache.get('k'))?.value.toString()).toBe('v2');
      });
      expect((await cache.getOrLoad('k', loader)).source).toBe('cache');
      expect(loader).toHaveBeenCalledTimes(1);
    });

    it('blocks on the loader for stale entries when staleWhileRevalidate is off', async () => {
      await cache.put('k', 'v1');
      clock.advance(TTL + 1);
      const loader = vi.fn<Loader>(async () => 'v2');

      const hit = await cache.getOrLoad('k', loader, {
        staleWhileRevalidate: false,
      });
      expect(hit.source).toBe('loader');
      expect(hit.value.toString()).toBe('v2');
    });

    it('reloads once an entry is fully expired', async () => {
      await cache.put('k', 'v1');
      clock.advance(TTL + SWR + 1);
      const loader = vi.fn<Loader>(async () => 'v2');

      const hit = await cache.getOrLoad('k', loader);
      expect(hit.source).toBe('loader');
      expect(hit.value.toString()).toBe('v2');
    });

    it('lets the loader override TTL and attributes per entry', async () => {
      const hit = await cache.getOrLoad('k', async () => ({
        value: 'v',
        ttlMs: 42,
        staleWhileRevalidateMs: 0,
        attributes: { 'content-type': 'text/plain' },
      }));
      expect(hit.metadata.expiresAt - hit.metadata.createdAt).toBe(42);
      expect(hit.metadata.staleUntil).toBe(hit.metadata.expiresAt);
      expect(hit.metadata.attributes).toEqual({ 'content-type': 'text/plain' });
    });

    it('passes oversized loader results through without storing them', async () => {
      const bounded = new CacheManager(store, { maxEntryBytes: 2, clock });
      const hit = await bounded.getOrLoad('k', async () => 'way too big');
      expect(hit.stored).toBe(false);
      expect(hit.value.toString()).toBe('way too big');
      await expect(store.read('k')).resolves.toBeUndefined();
    });

    it('propagates loader failures to every coalesced caller and leaves no entry', async () => {
      const loader = vi.fn<Loader>(async () => {
        throw new Error('origin down');
      });
      const results = await Promise.allSettled([
        cache.getOrLoad('k', loader),
        cache.getOrLoad('k', loader),
      ]);
      expect(results.every((r) => r.status === 'rejected')).toBe(true);
      expect(loader).toHaveBeenCalledTimes(1);
      await expect(store.read('k')).resolves.toBeUndefined();
    });

    it('reports background revalidation failures through the hook', async () => {
      const onBackgroundError =
        vi.fn<NonNullable<CacheManagerOptions['onBackgroundError']>>();
      const reporting = new CacheManager(store, {
        clock,
        ttlMs: TTL,
        staleWhileRevalidateMs: SWR,
        ttlJitterRatio: 0,
        onBackgroundError,
      });
      await reporting.put('k', 'v1');
      clock.advance(TTL + 1);

      const hit = await reporting.getOrLoad('k', async () => {
        throw new Error('origin down');
      });
      expect(hit.source).toBe('stale');
      await vi.waitFor(() => expect(onBackgroundError).toHaveBeenCalledOnce());
      expect(onBackgroundError.mock.calls[0]?.[1]).toEqual({
        key: 'k',
        operation: 'revalidate',
      });
    });

    it('does not leak the caller abort signal into the background refresh', async () => {
      await cache.put('k', 'v1');
      clock.advance(TTL + 1);
      const controller = new AbortController();
      let receivedSignal: AbortSignal | undefined;

      await cache.getOrLoad(
        'k',
        async ({ signal }) => {
          receivedSignal = signal;
          return 'v2';
        },
        { signal: controller.signal },
      );
      await vi.waitFor(async () => {
        expect((await cache.get('k'))?.value.toString()).toBe('v2');
      });
      controller.abort(new Error('caller gone'));
      expect(receivedSignal?.aborted).toBe(false);
    });

    it('coalesces callers whose lookups overlap, even when the loader fails instantly', async () => {
      const loader = vi.fn<Loader>(async () => {
        throw new Error('origin down');
      });
      const results = await Promise.allSettled(
        Array.from({ length: 8 }, () => cache.getOrLoad('k', loader)),
      );
      expect(results.every((r) => r.status === 'rejected')).toBe(true);
      expect(loader).toHaveBeenCalledTimes(1);
    });

    it('shares one store read between concurrent lookups of a hot key', async () => {
      await cache.put('hot', 'v');
      const readSpy = vi.spyOn(store, 'read');
      const hits = await Promise.all(
        Array.from({ length: 20 }, () => cache.get('hot')),
      );
      expect(hits.every((h) => h?.value.toString() === 'v')).toBe(true);
      expect(readSpy).toHaveBeenCalledTimes(1);
    });
  });

  describe('rate limiting of loaders', () => {
    it('acquires one unit from the limiter per loader invocation, not per caller', async () => {
      const limiter: RateLimiter = {
        tryAcquire: vi.fn<RateLimiter['tryAcquire']>(() => true),
        acquire: vi.fn<RateLimiter['acquire']>(async () => undefined),
      };
      const limited = new CacheManager(store, { clock, rateLimiter: limiter });
      const loader = vi.fn<Loader>(async () => 'v');

      await Promise.all(
        Array.from({ length: 5 }, () => limited.getOrLoad('k', loader)),
      );
      await limited.getOrLoad('k', loader);

      expect(loader).toHaveBeenCalledTimes(1);
      expect(limiter.acquire).toHaveBeenCalledTimes(1);
      expect(limiter.tryAcquire).not.toHaveBeenCalled();
    });

    it('surfaces limiter rejections instead of calling the loader', async () => {
      const limiter: RateLimiter = {
        tryAcquire: () => false,
        acquire: async () => {
          throw new Error('throttled');
        },
      };
      const limited = new CacheManager(store, { clock, rateLimiter: limiter });
      const loader = vi.fn<Loader>(async () => 'v');

      await expect(limited.getOrLoad('k', loader)).rejects.toThrow('throttled');
      expect(loader).not.toHaveBeenCalled();
    });
  });
});
