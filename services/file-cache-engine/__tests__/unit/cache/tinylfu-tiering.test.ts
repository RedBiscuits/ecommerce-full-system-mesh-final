/**
 * End-to-end policy checks for the memory tier, through `createFileCache`
 * and `CacheManager`, the way production traffic reaches it:
 *
 * - TinyLFU admission (scan resistance): one-hit wonders never displace
 *   entries that are re-read.
 * - SLRU: an entry is promoted from probationary to protected on its second
 *   access.
 * - Write-behind: rapid rewrites coalesce into one disk write, and every
 *   dirty entry is flushed by `close()`.
 *
 * Sizes: payloads are 16 000 bytes, so a `hot-NNN` entry is charged
 * 512 + 16 000 + 2 × 7 = 16 526 bytes. With a 4 MiB L1, probation holds 50
 * of them and protected about 200.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { FsCacheStore } from '../../../src/cache/fs-cache-store';
import {
  createFileCache,
  type FileCache,
  type FileCacheOptions,
} from '../../../src/main';
import { makeTempDir, type TempDir } from '../../helpers/temp-dir';

const MEMORY_BYTES = 4 * 1024 * 1024;
const PROBATION_SLOTS = 50;
const PAYLOAD = 'x'.repeat(16_000);

const hotKey = (i: number): string => `hot-${String(i).padStart(3, '0')}`;
const scanKey = (i: number): string => `scan-${String(i).padStart(4, '0')}`;

/** Mulberry32: a seeded `random`, so the sketch's hash seed is fixed. */
function seeded(seed: number): () => number {
  let a = seed;
  return () => {
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4_294_967_296;
  };
}

// Real disk I/O (hundreds of 16 KB files): give loaded CI machines room.
describe(
  'memory tier policy (TinyLFU + SLRU + write-behind)',
  { timeout: 20_000 },
  () => {
    let dir: TempDir;
    let fileCache: FileCache | undefined;
    /** Loader invocations per key: a reload means the cache missed. */
    let loads: Map<string, number>;

    beforeEach(async () => {
      dir = await makeTempDir();
      loads = new Map();
    });

    afterEach(async () => {
      vi.useRealTimers();
      await fileCache?.close();
      fileCache = undefined;
      await dir.cleanup();
    });

    async function open(
      overrides: Partial<FileCacheOptions> = {},
      writeBehind = false,
    ): Promise<FileCache> {
      fileCache = await createFileCache({
        directory: dir.path,
        store: { fsync: false },
        sweeper: false,
        ttlJitterRatio: 0,
        random: seeded(42),
        tiering: { memoryBytes: MEMORY_BYTES, writeBehind },
        ...overrides,
      });
      return fileCache;
    }

    /** One request, as the HTTP middleware would issue it. */
    async function request(cache: FileCache, key: string): Promise<string> {
      const result = await cache.cache.getOrLoad(key, async () => {
        loads.set(key, (loads.get(key) ?? 0) + 1);
        return PAYLOAD;
      });
      return result.source;
    }

    const segmentOf = (cache: FileCache, key: string): string | undefined =>
      cache.tiered?.l1.segmentOf(key);

    describe.each([
      { mode: 'write-through', writeBehind: false },
      { mode: 'write-behind', writeBehind: true },
    ])('scan resistance ($mode)', ({ writeBehind }) => {
      it('one-hit wonders do not evict a frequently read working set', async () => {
        const cache = await open({}, writeBehind);
        const hot = Array.from({ length: 100 }, (_, i) => hotKey(i));

        // Warm up: request the hot set until every key has been promoted
        // to the protected segment (see the SLRU tests below).
        for (let round = 0; round < 6; round++) {
          for (const key of hot) {
            await request(cache, key);
          }
          await cache.tiered?.flush(); // deterministic: no write still pending
          if (hot.every((key) => segmentOf(cache, key) === 'protected')) {
            break;
          }
        }
        expect(hot.filter((k) => segmentOf(cache, k) !== 'protected')).toEqual(
          [],
        );
        const hotLoads = hot.map((key) => loads.get(key));

        // A scan: 600 distinct keys, each requested exactly once. That is
        // 12× what probation can hold, and 3× the whole L1.
        const rejectedBefore = cache.tiered?.l1.stats().rejected ?? 0;
        for (let i = 0; i < 600; i++) {
          await request(cache, scanKey(i));
        }
        const rejected =
          (cache.tiered?.l1.stats().rejected ?? 0) - rejectedBefore;
        // Only the free probationary slots took scan keys; every later one
        // lost admission (frequency 1 is not higher than the victim's 1).
        expect(rejected).toBeGreaterThanOrEqual(600 - PROBATION_SLOTS);

        // The working set survived, and is still served from memory.
        const l1HitsBefore = cache.tiered?.stats().l1Hits ?? 0;
        for (const key of hot) {
          expect(segmentOf(cache, key)).toBe('protected');
          expect(await request(cache, key)).toBe('cache');
        }
        expect((cache.tiered?.stats().l1Hits ?? 0) - l1HitsBefore).toBe(100);
        expect(hot.map((key) => loads.get(key))).toEqual(hotLoads); // no reloads
      });
    });

    describe('TinyLFU admission into probation', () => {
      it('keeps a frequently requested probationary entry against one-hit wonders', async () => {
        const cache = await open();
        // `popular` is asked for 5 times before it is cached: frequency 5.
        for (let i = 0; i < 5; i++) {
          await cache.cache.get('popular');
        }
        await cache.cache.put('popular', PAYLOAD);
        expect(segmentOf(cache, 'popular')).toBe('probation');

        // Fill probation behind it, leaving `popular` as the LRU victim.
        for (let i = 0; i < PROBATION_SLOTS - 1; i++) {
          await request(cache, scanKey(i));
        }
        const rejectedBefore = cache.tiered?.l1.stats().rejected ?? 0;
        for (let i = PROBATION_SLOTS; i < 500; i++) {
          await request(cache, scanKey(i)); // frequency 1 each
        }
        expect(segmentOf(cache, 'popular')).toBe('probation'); // still there
        expect((cache.tiered?.l1.stats().rejected ?? 0) - rejectedBefore).toBe(
          500 - PROBATION_SLOTS,
        );
      });

      it('admits a newcomer hotter than the victim, and evicts the victim', async () => {
        const cache = await open();
        for (let i = 0; i < PROBATION_SLOTS; i++) {
          await request(cache, scanKey(i)); // probation full, all frequency 1
        }
        const victim = scanKey(0); // least recently used
        expect(segmentOf(cache, victim)).toBe('probation');

        await request(cache, 'newcomer'); // frequency 1: a tie, rejected
        expect(segmentOf(cache, 'newcomer')).toBeUndefined();
        await request(cache, 'newcomer'); // frequency 2: beats the victim
        expect(segmentOf(cache, 'newcomer')).toBe('probation');
        expect(segmentOf(cache, victim)).toBeUndefined();
      });
    });

    describe('SLRU promotion', () => {
      it('promotes from probationary to protected on the second access', async () => {
        const cache = await open();
        expect(await request(cache, 'k')).toBe('loader'); // 1st: miss + write
        expect(segmentOf(cache, 'k')).toBe('probation');

        expect(await request(cache, 'k')).toBe('cache'); // 2nd: L1 hit
        expect(segmentOf(cache, 'k')).toBe('protected');

        expect(await request(cache, 'k')).toBe('cache'); // stays protected
        expect(segmentOf(cache, 'k')).toBe('protected');
        expect(loads.get('k')).toBe(1);
      });

      it('never promotes an entry accessed only once', async () => {
        const cache = await open();
        await request(cache, 'once');
        await request(cache, 'other');
        await request(cache, 'other');
        expect(segmentOf(cache, 'once')).toBe('probation');
        expect(cache.tiered?.l1.stats().protectedBytes).toBeGreaterThan(0);
      });

      it('re-warms from disk the same way after a restart: probation, then protected', async () => {
        let cache = await open();
        await request(cache, 'k');
        await cache.close();

        cache = await open(); // empty memory, same disk
        expect(segmentOf(cache, 'k')).toBeUndefined();
        expect(await request(cache, 'k')).toBe('cache'); // L2 hit → promoted
        expect(segmentOf(cache, 'k')).toBe('probation');
        expect(await request(cache, 'k')).toBe('cache'); // L1 hit
        expect(segmentOf(cache, 'k')).toBe('protected');
        expect(loads.get('k')).toBe(1); // never reloaded from origin
        expect(cache.tiered?.stats()).toMatchObject({
          l2Hits: 1,
          l1Hits: 1,
          promoted: 1,
        });
      });
    });

    describe('write-behind', () => {
      it('coalesces rapid rewrites and flushes every dirty entry on close()', async () => {
        // Freeze the flush timer: nothing reaches disk until close().
        vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] });
        const cache = await open({}, true);

        for (let i = 0; i < 100; i++) {
          await cache.cache.put('config', `v${i}`); // one hot key, 100 times
        }
        for (let i = 0; i < 20; i++) {
          await cache.cache.put(hotKey(i), `dirty-${i}`);
        }
        expect(cache.tiered?.stats()).toMatchObject({
          coalescedWrites: 99,
          pendingWrites: 21,
          flushed: 0,
        });
        // Dirty: acknowledged and readable, but not on disk yet.
        await expect(cache.fsStore.read('config')).resolves.toBeUndefined();
        expect((await cache.cache.get('config'))?.value.toString()).toBe('v99');

        await cache.close();
        expect(cache.tiered?.stats()).toMatchObject({
          flushed: 21, // not 120: coalescing saved 99 disk writes
          pendingWrites: 0,
          pendingBytes: 0,
        });

        const disk = await FsCacheStore.open({ directory: dir.path });
        expect((await disk.read('config'))?.value.toString()).toBe('v99');
        for (let i = 0; i < 20; i++) {
          expect((await disk.read(hotKey(i)))?.value.toString()).toBe(
            `dirty-${i}`,
          );
        }
      });
    });
  },
);
