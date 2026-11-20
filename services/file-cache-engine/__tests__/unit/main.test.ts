import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import {
  CacheManager,
  ExpirySweeper,
  FsCacheStore,
  ResilientCacheStore,
  TokenBucketRateLimiter,
  createFileCache,
  TieredCacheStore,
  type FileCache,
  type Loader,
} from '../../src/main';
import { FakeClock } from '../helpers/fake-clock';
import { makeTempDir, type TempDir } from '../helpers/temp-dir';

describe('createFileCache', () => {
  let dir: TempDir;
  let fileCache: FileCache | undefined;

  beforeEach(async () => {
    dir = await makeTempDir();
  });

  afterEach(async () => {
    await fileCache?.close();
    fileCache = undefined;
    await dir.cleanup();
  });

  it('wires store, manager and a running sweeper together', async () => {
    fileCache = await createFileCache({
      directory: dir.path,
      store: { fsync: false },
    });

    expect(fileCache.cache).toBeInstanceOf(CacheManager);
    expect(fileCache.store).toBeInstanceOf(ResilientCacheStore);
    expect(fileCache.fsStore).toBeInstanceOf(FsCacheStore);
    expect(fileCache.health()).toMatchObject({
      sweeperRunning: true,
      store: { circuit: { state: 'closed' } },
    });
    expect(fileCache.sweeper).toBeInstanceOf(ExpirySweeper);
    expect(fileCache.sweeper.isRunning).toBe(true);
    expect(fileCache.rateLimiter).toBeUndefined();

    await fileCache.close();
    expect(fileCache.sweeper.isRunning).toBe(false);
  });

  it('can be created with the sweeper disabled', async () => {
    fileCache = await createFileCache({ directory: dir.path, sweeper: false });
    expect(fileCache.sweeper.isRunning).toBe(false);
  });

  it('can use the raw store when resilience is disabled', async () => {
    fileCache = await createFileCache({
      directory: dir.path,
      sweeper: false,
      resilience: false,
    });
    expect(fileCache.store).toBe(fileCache.fsStore);
    expect(fileCache.health().store).toBeUndefined();
  });

  it('builds a token bucket from options or accepts an existing limiter', async () => {
    fileCache = await createFileCache({
      directory: dir.path,
      sweeper: false,
      rateLimiter: { capacity: 1, refillPerSecond: 1 },
    });
    expect(fileCache.rateLimiter).toBeInstanceOf(TokenBucketRateLimiter);
    await fileCache.close();

    const shared = new TokenBucketRateLimiter({
      capacity: 1,
      refillPerSecond: 1,
    });
    fileCache = await createFileCache({
      directory: dir.path,
      sweeper: false,
      rateLimiter: shared,
    });
    expect(fileCache.rateLimiter).toBe(shared);
  });

  it('end to end: load, hit, expire, sweep', async () => {
    const clock = new FakeClock();
    fileCache = await createFileCache({
      directory: dir.path,
      store: { fsync: false },
      sweeper: { batchPauseMs: 0 },
      clock,
      ttlMs: 1_000,
      staleWhileRevalidateMs: 0,
      ttlJitterRatio: 0,
    });
    const { cache, sweeper } = fileCache;
    const loader = vi.fn<Loader>(async () => Buffer.from('payload'));

    expect((await cache.getOrLoad('k', loader)).source).toBe('loader');
    expect((await cache.getOrLoad('k', loader)).source).toBe('cache');
    expect(loader).toHaveBeenCalledTimes(1);

    clock.advance(1_001);
    await expect(cache.get('k')).resolves.toBeUndefined();
    expect(await sweeper.sweepOnce()).toMatchObject({ expired: 1 });
    await expect(cache.head('k')).resolves.toBeUndefined();
  });

  describe('tiering', () => {
    it('puts a tiered store in front of the resilient disk store', async () => {
      fileCache = await createFileCache({
        directory: dir.path,
        store: { fsync: false },
        sweeper: false,
        tiering: { memoryBytes: 1_000_000 },
      });
      expect(fileCache.store).toBeInstanceOf(TieredCacheStore);
      expect(fileCache.tiered).toBe(fileCache.store);
      expect(fileCache.tiered?.l2).toBeInstanceOf(ResilientCacheStore);
      expect(fileCache.tiered?.l1.maxMemoryBytes).toBe(1_000_000);
      expect(fileCache.health().tiering).toMatchObject({ l1Hits: 0 });

      // writeBehind defaults to false: a resolved put is already on disk.
      await fileCache.cache.put('k', 'v');
      await expect(fileCache.fsStore.read('k')).resolves.toBeDefined();
    });

    it('is off by default', async () => {
      fileCache = await createFileCache({
        directory: dir.path,
        sweeper: false,
      });
      expect(fileCache.tiered).toBeUndefined();
      expect(fileCache.health().tiering).toBeUndefined();
    });

    it('works without the resilience decorator', async () => {
      fileCache = await createFileCache({
        directory: dir.path,
        store: { fsync: false },
        sweeper: false,
        resilience: false,
        tiering: { memoryBytes: 1_000_000 },
      });
      expect(fileCache.tiered?.l2).toBe(fileCache.fsStore);
    });

    it('rejects an invalid memory budget', async () => {
      await expect(
        createFileCache({
          directory: dir.path,
          sweeper: false,
          tiering: { memoryBytes: 0 },
        }),
      ).rejects.toThrow(RangeError);
    });

    it('the sweeper purges expired entries from memory, not just from disk', async () => {
      const clock = new FakeClock();
      fileCache = await createFileCache({
        directory: dir.path,
        store: { fsync: false },
        sweeper: { batchPauseMs: 0 },
        clock,
        ttlMs: 1_000,
        staleWhileRevalidateMs: 0,
        ttlJitterRatio: 0,
        tiering: { memoryBytes: 1_000_000 },
      });
      const { cache, sweeper, tiered } = fileCache;
      await cache.getOrLoad('k', async () => 'payload');
      await cache.getOrLoad('k', async () => 'payload'); // hit → protected
      expect(tiered?.l1.segmentOf('k')).toBe('protected');

      clock.advance(1_001);
      expect(await sweeper.sweepOnce()).toMatchObject({ expired: 1 });
      expect(tiered?.l1.segmentOf('k')).toBeUndefined();
      await expect(fileCache.fsStore.read('k')).resolves.toBeUndefined();
    });

    it('write-behind: close() drains pending writes before closing the disk store', async () => {
      const failures: unknown[] = [];
      fileCache = await createFileCache({
        directory: dir.path,
        store: { fsync: false },
        sweeper: false,
        onStoreError: (error): void => {
          failures.push(error);
        },
        tiering: { memoryBytes: 1_000_000, writeBehind: true },
      });
      for (let i = 0; i < 25; i++) {
        await fileCache.cache.put(`k${i}`, `v${i}`);
      }
      await fileCache.close();
      expect(failures).toEqual([]); // no "Cache store is closing"
      expect(fileCache.health().tiering).toMatchObject({ pendingWrites: 0 });

      const reopened = await FsCacheStore.open({ directory: dir.path });
      for (let i = 0; i < 25; i++) {
        expect((await reopened.read(`k${i}`))?.value.toString()).toBe(`v${i}`);
      }
    });
  });
});
