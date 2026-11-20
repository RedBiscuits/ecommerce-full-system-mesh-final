import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { CacheManager } from '../../../src/cache/cache-manager';
import { ExpirySweeper } from '../../../src/cache/expiry-sweeper';
import { FsCacheStore } from '../../../src/cache/fs-cache-store';
import {
  ENTRY_OVERHEAD_BYTES,
  SlruMemoryStore,
} from '../../../src/cache/slru-memory-store';
import type { WriteBehindOptions } from '../../../src/cache/tiered-cache-store';
import { TieredCacheStore } from '../../../src/cache/tiered-cache-store';
import { FrequencySketch } from '../../../src/cache/tiny-lfu';
import type {
  CacheEntry,
  CacheKey,
  CacheStore,
  EntryAttributes,
  EntryLifetime,
  EntryMetadata,
} from '../../../src/cache/types';
import { ResilientCacheStore } from '../../../src/resilience/resilient-store';
import { FakeClock } from '../../helpers/fake-clock';
import { errno, FaultyStore } from '../../helpers/faulty-store';
import { makeTempDir, type TempDir } from '../../helpers/temp-dir';

const LIFETIME = { createdAt: 1_000, expiresAt: 2_000, staleUntil: 3_000 };
/** Charged size of each `k-NNN` entry in L1: probation holds exactly 2. */
const ENTRY = 1_000;
const PAYLOAD = ENTRY - ENTRY_OVERHEAD_BYTES - 2 * 5;

const body = (fill: number): Buffer => Buffer.alloc(PAYLOAD, fill);

/**
 * L2 double whose reads and writes can be held *after* they have done their
 * work. That is the dangerous interleaving: an L2 read that already holds
 * the old value, resuming only after a mutation has completed.
 */
class GatedStore implements CacheStore {
  readonly inner = new FaultyStore();
  readonly #readHolds: Array<Promise<void>> = [];
  readonly #writeHolds: Array<{
    when: 'before' | 'after';
    promise: Promise<void>;
  }> = [];
  readonly #clearHolds: Array<Promise<void>> = [];

  get calls(): FaultyStore['calls'] {
    return this.inner.calls;
  }

  /** The next `read` does its lookup, then waits for the returned release. */
  holdNextRead(): () => void {
    const { promise, resolve } = Promise.withResolvers<void>();
    this.#readHolds.push(promise);
    return resolve;
  }

  /**
   * The next `write` waits for the returned release: `after` it lands
   * (default), or `before` it touches the inner store.
   */
  holdNextWrite(when: 'before' | 'after' = 'after'): () => void {
    const { promise, resolve } = Promise.withResolvers<void>();
    this.#writeHolds.push({ when, promise });
    return resolve;
  }

  /** The next `clear` waits for the returned release *before* clearing. */
  holdNextClear(): () => void {
    const { promise, resolve } = Promise.withResolvers<void>();
    this.#clearHolds.push(promise);
    return resolve;
  }

  async read(key: CacheKey): Promise<CacheEntry | undefined> {
    const hold = this.#readHolds.shift();
    const entry = await this.inner.read(key);
    await hold;
    return entry;
  }

  readMetadata(key: CacheKey): Promise<EntryMetadata | undefined> {
    return this.inner.readMetadata(key);
  }

  async write(
    key: CacheKey,
    value: Uint8Array,
    lifetime: EntryLifetime,
    attributes?: EntryAttributes,
  ): Promise<EntryMetadata> {
    const hold = this.#writeHolds.shift();
    if (hold?.when === 'before') {
      await hold.promise;
    }
    const metadata = await this.inner.write(key, value, lifetime, attributes);
    if (hold?.when === 'after') {
      await hold.promise;
    }
    return metadata;
  }

  delete(key: CacheKey): Promise<boolean> {
    return this.inner.delete(key);
  }

  deleteIf(
    key: CacheKey,
    predicate: (metadata: EntryMetadata) => boolean,
  ): Promise<boolean> {
    return this.inner.deleteIf(key, predicate);
  }

  async clear(): Promise<void> {
    await this.#clearHolds.shift();
    return this.inner.clear();
  }

  scan(): AsyncIterable<EntryMetadata> {
    return this.inner.scan();
  }
}

function makeL1(): SlruMemoryStore {
  return new SlruMemoryStore({
    maxMemoryBytes: 10_000, // probation 2 entries, protected 8
    sketch: new FrequencySketch({ capacity: 1_024, seed: 1, sampleSize: 1e12 }),
  });
}

/** Every store a test creates is closed (drained) after the test. */
const opened: TieredCacheStore[] = [];
function track(store: TieredCacheStore): TieredCacheStore {
  opened.push(store);
  return store;
}
afterEach(async () => {
  await Promise.allSettled(opened.splice(0).map((store) => store.close()));
});

/** A flush delay long enough that only explicit triggers flush. */
const MANUAL = { flushDelayMs: 60_000 };

function setup(
  writeBehind: WriteBehindOptions | false = MANUAL,
  extra: { onFlushError?: (error: unknown, key: string) => void } = {},
): {
  l1: SlruMemoryStore;
  l2: GatedStore;
  tiered: TieredCacheStore;
} {
  const l1 = makeL1();
  const l2 = new GatedStore();
  return {
    l1,
    l2,
    tiered: track(new TieredCacheStore({ l1, l2, writeBehind, ...extra })),
  };
}

/** Lets already-resolved continuations run (no timers involved). */
const flush = (): Promise<void> => new Promise((r) => setImmediate(r));

async function keysOf(store: CacheStore): Promise<string[]> {
  const keys: string[] = [];
  for await (const metadata of store.scan()) {
    keys.push(metadata.key);
  }
  return keys.sort();
}

describe('TieredCacheStore – reads', () => {
  it('serves an L1 hit without touching L2', async () => {
    const { l2, tiered } = setup();
    await tiered.write('k-001', body(1), LIFETIME);
    const entry = await tiered.read('k-001');
    expect(entry?.value[0]).toBe(1);
    expect(l2.calls.read).toBe(0);
    expect(tiered.stats()).toMatchObject({ l1Hits: 1, l2Reads: 0 });
  });

  it('falls back to L2 on an L1 miss and promotes the hit into L1', async () => {
    const { l1, l2, tiered } = setup();
    await l2.write('k-001', body(7), LIFETIME); // only on disk
    const first = await tiered.read('k-001');
    expect(first?.value[0]).toBe(7);
    expect(first?.metadata.key).toBe('k-001');
    expect(l1.segmentOf('k-001')).toBe('probation');

    await tiered.read('k-001'); // now an L1 hit
    expect(l2.calls.read).toBe(1);
    expect(tiered.stats()).toMatchObject({
      l1Hits: 1,
      l2Hits: 1,
      promoted: 1,
    });
  });

  it('resolves undefined when neither tier has the key', async () => {
    const { tiered } = setup();
    await expect(tiered.read('k-404')).resolves.toBeUndefined();
    expect(tiered.stats()).toMatchObject({ misses: 1, l2Reads: 1 });
  });

  it('lets TinyLFU admission decline a cold L2 hit, then promote it once it is hot', async () => {
    const { l1, l2, tiered } = setup();
    // Fill L1 probation with two entries, each read twice (frequency 2).
    for (const k of ['k-001', 'k-002']) {
      await l2.write(k, body(1), LIFETIME);
      await l1.read(k);
      await tiered.read(k); // L1 miss → L2 hit → promoted (room available)
    }
    expect(l1.segmentOf('k-001')).toBe('probation');
    expect(l1.segmentOf('k-002')).toBe('probation');

    await l2.write('k-003', body(3), LIFETIME);
    // Frequency 1 vs victim 2: served from L2 but not promoted.
    expect((await tiered.read('k-003'))?.value[0]).toBe(3);
    expect(l1.segmentOf('k-003')).toBeUndefined();
    expect(tiered.stats().notPromoted).toBe(1);

    await tiered.read('k-003'); // frequency 2: a tie still loses
    expect(l1.segmentOf('k-003')).toBeUndefined();
    await tiered.read('k-003'); // frequency 3: beats the LRU victim
    expect(l1.segmentOf('k-003')).toBe('probation');
    expect(l1.segmentOf('k-001')).toBeUndefined(); // the victim
    expect(tiered.stats().promoted).toBe(3);
  });

  it('keeps serving L1 hits while L2 is failing', async () => {
    const l1 = makeL1();
    const l2 = new FaultyStore();
    const tiered = track(new TieredCacheStore({ l1, l2 }));
    await tiered.write('k-001', body(1), LIFETIME);
    l2.failNext('read', new Error('disk on fire'), 10);
    await expect(tiered.read('k-001')).resolves.toBeDefined();
    await expect(tiered.read('k-002')).rejects.toThrow('disk on fire');
  });

  it('does not let readMetadata promote or count as an access', async () => {
    const { l1, l2, tiered } = setup();
    await l2.write('k-001', body(1), LIFETIME);
    expect((await tiered.readMetadata('k-001'))?.key).toBe('k-001');
    expect(l1.segmentOf('k-001')).toBeUndefined();
    expect(l1.stats()).toMatchObject({ hits: 0, misses: 0 });
  });
});

describe('TieredCacheStore – request coalescing', () => {
  it('shares one L2 read among concurrent L1 misses', async () => {
    const { l2, tiered } = setup();
    await l2.write('k-001', body(5), LIFETIME);
    const release = l2.holdNextRead();
    const readers = Array.from({ length: 20 }, () => tiered.read('k-001'));
    await flush();
    expect(tiered.stats()).toMatchObject({ inFlight: 1, l2Reads: 1 });
    release();

    const entries = await Promise.all(readers);
    expect(l2.calls.read).toBe(1);
    expect(entries.every((e) => e?.value[0] === 5)).toBe(true);
    expect(tiered.stats()).toMatchObject({
      coalesced: 19,
      l2Hits: 20,
      promoted: 1,
      inFlight: 0,
    });
  });

  it('gives every coalesced reader its own copy of the bytes', async () => {
    const { l2, tiered } = setup();
    await l2.write('k-001', body(5), LIFETIME);
    const [a, b] = await Promise.all([
      tiered.read('k-001'),
      tiered.read('k-001'),
    ]);
    a?.value.fill(0);
    expect(b?.value[0]).toBe(5);
    expect((await tiered.read('k-001'))?.value[0]).toBe(5); // L1 untouched
  });

  it('coalesces misses too', async () => {
    const { l2, tiered } = setup();
    const results = await Promise.all(
      Array.from({ length: 5 }, () => tiered.read('k-404')),
    );
    expect(results).toEqual([
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
    ]);
    expect(l2.calls.read).toBe(1);
    expect(tiered.stats().misses).toBe(5);
  });

  it('counts every coalesced reader as an access in the sketch', async () => {
    const { l1, l2, tiered } = setup();
    await l2.write('k-001', body(1), LIFETIME);
    await Promise.all(Array.from({ length: 4 }, () => tiered.read('k-001')));
    expect(l1.stats().misses).toBe(4); // four L1 lookups → four increments
  });

  it('propagates an L2 failure to every joined reader, then retries fresh', async () => {
    const l1 = makeL1();
    const l2 = new FaultyStore();
    const tiered = track(new TieredCacheStore({ l1, l2 }));
    await l2.write('k-001', body(1), LIFETIME);
    l2.failNext('read', new Error('EIO'));
    const results = await Promise.allSettled([
      tiered.read('k-001'),
      tiered.read('k-001'),
    ]);
    expect(results.map((r) => r.status)).toEqual(['rejected', 'rejected']);
    expect(tiered.stats().inFlight).toBe(0);

    await expect(tiered.read('k-001')).resolves.toBeDefined();
    expect(l2.calls.read).toBe(2); // failures are not cached
  });

  it('cleans up even if L2 throws synchronously', async () => {
    const l1 = makeL1();
    const l2 = new FaultyStore();
    l2.read = (): Promise<CacheEntry | undefined> => {
      throw new Error('sync boom');
    };
    const tiered = track(new TieredCacheStore({ l1, l2 }));
    await expect(tiered.read('k-001')).rejects.toThrow('sync boom');
    expect(tiered.stats().inFlight).toBe(0);
  });
});

describe('TieredCacheStore – mutations never resurrect stale data', () => {
  it('delete during an in-flight L2 read: the old value is not promoted, later readers miss', async () => {
    const { l1, l2, tiered } = setup();
    await l2.write('k-001', body(1), LIFETIME);
    const release = l2.holdNextRead();
    const early = tiered.read('k-001'); // has read the old value, held
    await flush();

    await expect(tiered.delete('k-001')).resolves.toBe(true);
    // A reader arriving after the delete must not join the old flight.
    await expect(tiered.read('k-001')).resolves.toBeUndefined();

    release();
    expect((await early)?.value[0]).toBe(1); // it started before the delete
    expect(l1.segmentOf('k-001')).toBeUndefined();
    await expect(tiered.read('k-001')).resolves.toBeUndefined();
  });

  it('write during an in-flight L2 read: L1 ends with the new value', async () => {
    const { l2, tiered } = setup();
    await l2.write('k-001', body(1), LIFETIME);
    const release = l2.holdNextRead();
    const early = tiered.read('k-001');
    await flush();

    await tiered.write('k-001', body(2), LIFETIME);
    release();
    await early;
    expect((await tiered.read('k-001'))?.value[0]).toBe(2);
    expect(l2.calls.read).toBe(1); // and that was an L1 hit
  });

  it('clear during an in-flight L2 read: nothing survives in either tier', async () => {
    const { l1, l2, tiered } = setup();
    await l2.write('k-001', body(1), LIFETIME);
    const release = l2.holdNextRead();
    const early = tiered.read('k-001');
    await flush();

    await tiered.clear();
    release();
    await early;
    expect(l1.stats().entries).toBe(0);
    await expect(tiered.read('k-001')).resolves.toBeUndefined();
  });

  it('a read that starts during the L2 clear cannot leave pre-clear data in L1', async () => {
    const { l1, l2, tiered } = setup();
    await l2.write('k-001', body(1), LIFETIME);
    const release = l2.holdNextClear();
    const clearing = tiered.clear();
    await flush();
    // L1 is already empty and L2 not yet cleared: this read promotes the
    // old value into L1 while clear() is still running.
    expect((await tiered.read('k-001'))?.value[0]).toBe(1);
    release();
    await clearing;
    expect(l1.stats().entries).toBe(0);
    await expect(tiered.read('k-001')).resolves.toBeUndefined();
  });

  it('write-through: a write that straddles clear() does not leave a value only in L1', async () => {
    const { l1, l2, tiered } = setup(false);
    const release = l2.holdNextWrite();
    const writing = tiered.write('k-001', body(1), LIFETIME); // landed in L2
    await flush();
    await tiered.clear(); // wipes L2 underneath the pending write
    release();
    await writing;
    expect(l1.segmentOf('k-001')).toBeUndefined();
    await expect(tiered.read('k-001')).resolves.toBeUndefined();
  });

  it('write-through: a failed L2 write evicts the L1 copy instead of leaving it stale', async () => {
    const l1 = makeL1();
    const l2 = new FaultyStore();
    const tiered = track(new TieredCacheStore({ l1, l2, writeBehind: false }));
    await tiered.write('k-001', body(1), LIFETIME);
    expect(l1.segmentOf('k-001')).toBe('probation');

    l2.failNext('write', new Error('ENOSPC'));
    await expect(tiered.write('k-001', body(2), LIFETIME)).rejects.toThrow(
      'ENOSPC',
    );
    expect(l1.segmentOf('k-001')).toBeUndefined();
    expect((await tiered.read('k-001'))?.value[0]).toBe(1); // L2's truth
  });

  it('write-through: serialises writes per key so both tiers end on the same value', async () => {
    const { l2, tiered } = setup(false);
    const release = l2.holdNextWrite();
    const first = tiered.write('k-001', body(1), LIFETIME);
    const second = tiered.write('k-001', body(2), LIFETIME);
    await flush();
    release();
    await Promise.all([first, second]);
    expect(l2.inner.entries.get('k-001')?.value[0]).toBe(2);
    expect((await tiered.read('k-001'))?.value[0]).toBe(2);
  });
});

describe('TieredCacheStore – delete, deleteIf, clear, scan', () => {
  it('delete removes from both tiers and reports whether either had it', async () => {
    const { l1, l2, tiered } = setup();
    await tiered.write('k-001', body(1), LIFETIME);
    await expect(tiered.delete('k-001')).resolves.toBe(true);
    expect(l1.segmentOf('k-001')).toBeUndefined();
    expect(l2.inner.entries.has('k-001')).toBe(false);
    await expect(tiered.delete('k-001')).resolves.toBe(false);

    await l1.write('k-002', body(2), LIFETIME); // L1-only (L2 lost it)
    await expect(tiered.delete('k-002')).resolves.toBe(true);
    expect(l1.segmentOf('k-002')).toBeUndefined();
  });

  it('deleteIf follows L2’s verdict and drops L1 with it', async () => {
    const { l1, l2, tiered } = setup();
    await tiered.write('k-001', body(1), LIFETIME);
    await expect(tiered.deleteIf('k-001', () => false)).resolves.toBe(false);
    expect(l1.segmentOf('k-001')).toBe('probation');

    await expect(
      tiered.deleteIf('k-001', (m) => m.expiresAt === 2_000),
    ).resolves.toBe(true);
    expect(l1.segmentOf('k-001')).toBeUndefined();
    expect(l2.inner.entries.has('k-001')).toBe(false);
  });

  it('deleteIf also removes an L1 copy that L2 no longer has', async () => {
    const { l1, tiered } = setup();
    await l1.write('k-001', body(1), LIFETIME);
    await expect(tiered.deleteIf('k-001', () => true)).resolves.toBe(true);
    expect(l1.segmentOf('k-001')).toBeUndefined();
  });

  it('clear empties both tiers', async () => {
    const { l1, l2, tiered } = setup();
    await tiered.write('k-001', body(1), LIFETIME);
    await l2.write('k-002', body(2), LIFETIME);
    await tiered.clear();
    expect(l1.stats().entries).toBe(0);
    expect(l2.inner.entries.size).toBe(0);
    expect(await keysOf(tiered)).toEqual([]);
  });

  it('scan yields the union of both tiers, each key once', async () => {
    const { l1, l2, tiered } = setup();
    await tiered.write('k-001', body(1), LIFETIME); // both
    await l2.write('k-002', body(2), LIFETIME); // L2 only
    await l1.write('k-003', body(3), LIFETIME); // L1 only
    expect(await keysOf(tiered)).toEqual(['k-001', 'k-002', 'k-003']);
  });

  it('rejects using one store as both tiers', () => {
    const l1 = makeL1();
    expect(() => new TieredCacheStore({ l1, l2: l1 })).toThrow(TypeError);
  });
});

describe('TieredCacheStore – over a real ResilientCacheStore(FsCacheStore)', () => {
  let dir: TempDir;
  let l2: ResilientCacheStore;

  beforeEach(async () => {
    dir = await makeTempDir();
    const fs = await FsCacheStore.open({ directory: dir.path, fsync: false });
    l2 = new ResilientCacheStore(fs);
  });

  afterEach(async () => {
    // Shutdown order matters: drain the write-behind buffer into L2 first,
    // then close L2. The other way round, the drain hits a closed store.
    await Promise.all(opened.splice(0).map((store) => store.close()));
    l2.close();
    await dir.cleanup();
  });

  it('persists through L2, serves from L1, and re-warms a cold L1 from disk', async () => {
    const clock = new FakeClock();
    const tiered = track(new TieredCacheStore({ l1: makeL1(), l2 }));
    const cache = new CacheManager(tiered, { clock, ttlJitterRatio: 0 });
    let loads = 0;
    const loader = async (): Promise<string> => {
      loads += 1;
      return 'origin';
    };
    await cache.getOrLoad('k-001', loader);
    expect((await cache.getOrLoad('k-001', loader)).source).toBe('cache');
    expect(tiered.stats()).toMatchObject({ l1Hits: 1, l2Reads: 1 });
    await tiered.close(); // shutdown drains the write to disk

    // A restarted process: empty memory, same disk.
    const restarted = track(new TieredCacheStore({ l1: makeL1(), l2 }));
    const entry = await restarted.read('k-001');
    expect(entry?.value.toString()).toBe('origin');
    expect(restarted.stats()).toMatchObject({ l2Hits: 1, promoted: 1 });
    expect(loads).toBe(1);
  });

  it('an ExpirySweeper run through it purges expired entries from both tiers', async () => {
    const clock = new FakeClock(10_000);
    const l1 = makeL1();
    const tiered = track(new TieredCacheStore({ l1, l2 }));
    await tiered.write('k-001', body(1), {
      createdAt: 0,
      expiresAt: 1,
      staleUntil: 2,
    });
    await tiered.write('k-002', body(2), {
      createdAt: 10_000,
      expiresAt: 20_000,
      staleUntil: 30_000,
    });
    await new ExpirySweeper(tiered, { clock }).sweepOnce();
    expect(await keysOf(tiered)).toEqual(['k-002']);
    expect(l1.segmentOf('k-001')).toBeUndefined();
  });

  it('delete and clear reach the disk', async () => {
    const tiered = track(new TieredCacheStore({ l1: makeL1(), l2 }));
    await tiered.write('k-001', body(1), LIFETIME);
    await tiered.write('k-002', body(2), LIFETIME);
    await tiered.delete('k-001');
    await expect(l2.read('k-001')).resolves.toBeUndefined();
    await tiered.clear();
    await expect(l2.read('k-002')).resolves.toBeUndefined();
    expect(await keysOf(tiered)).toEqual([]);
  });
});

describe('TieredCacheStore – write-behind', () => {
  it('answers the write from memory at once and reaches L2 only on flush', async () => {
    const { l1, l2, tiered } = setup();
    await tiered.write('k-001', body(1), LIFETIME);
    expect(l2.calls.write).toBe(0);
    expect(l1.segmentOf('k-001')).toBe('probation');
    expect((await tiered.read('k-001'))?.value[0]).toBe(1);
    expect(tiered.stats()).toMatchObject({ pendingWrites: 1 });

    await tiered.flush();
    expect(l2.inner.entries.get('k-001')?.value[0]).toBe(1);
    expect(tiered.stats()).toMatchObject({
      pendingWrites: 0,
      pendingBytes: 0,
      flushed: 1,
    });
  });

  it('flushes on its own after flushDelayMs', async () => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] });
    try {
      const { l2, tiered } = setup({ flushDelayMs: 50 });
      await tiered.write('k-001', body(1), LIFETIME);
      await vi.advanceTimersByTimeAsync(49);
      expect(l2.calls.write).toBe(0);
      await vi.advanceTimersByTimeAsync(1);
      await tiered.flush();
      expect(l2.calls.write).toBe(1);
      expect(vi.getTimerCount()).toBe(0); // nothing left armed
    } finally {
      vi.useRealTimers();
    }
  });

  it('flushes a full batch right away, and in batches of batchSize', async () => {
    const { l2, tiered } = setup({ flushDelayMs: 60_000, batchSize: 4 });
    for (let i = 0; i < 4; i++) {
      await tiered.write(`k-00${i}`, body(i), LIFETIME);
    }
    await flush(); // no timer elapsed: the full batch triggered the flush
    expect(l2.calls.write).toBe(4);

    for (let i = 4; i < 10; i++) {
      await tiered.write(`k-00${i}`, body(i), LIFETIME);
    }
    await tiered.flush();
    expect(l2.calls.write).toBe(10);
    expect(tiered.stats().batches).toBe(3); // 4 + 4 + 2
  });

  it('collapses rapid rewrites of a key into one L2 write of the last value', async () => {
    const { l2, tiered } = setup();
    for (let i = 0; i < 50; i++) {
      await tiered.write('k-001', body(i), LIFETIME);
    }
    expect(tiered.stats()).toMatchObject({
      coalescedWrites: 49,
      pendingWrites: 1,
      pendingBytes: PAYLOAD,
    });
    await tiered.flush();
    expect(l2.calls.write).toBe(1);
    expect(l2.inner.entries.get('k-001')?.value[0]).toBe(49);
  });

  it('never coalesces into a write that is already flushing', async () => {
    const { l2, tiered } = setup();
    await tiered.write('k-001', body(1), LIFETIME);
    const release = l2.holdNextWrite('before');
    const flushing = tiered.flush(); // v1 is now being written
    await flush();
    await tiered.write('k-001', body(2), LIFETIME); // must be a new write
    expect((await tiered.read('k-001'))?.value[0]).toBe(2);
    release();
    await flushing;
    await tiered.flush();
    expect(l2.calls.write).toBe(2);
    expect(l2.inner.entries.get('k-001')?.value[0]).toBe(2);
  });

  it('keeps a write readable while L1 declines it and L2 does not have it yet', async () => {
    const { l1, l2, tiered } = setup();
    await l2.write('k-003', body(0), LIFETIME); // an older value on disk
    // Make L1's probation hot so admission declines the new write.
    for (const k of ['k-001', 'k-002']) {
      await l1.read(k);
      await l1.read(k);
      await l1.write(k, body(9), LIFETIME);
    }
    await tiered.write('k-003', body(3), LIFETIME);
    expect(l1.segmentOf('k-003')).toBeUndefined();

    expect((await tiered.read('k-003'))?.value[0]).toBe(3); // not disk's 0
    expect(tiered.stats()).toMatchObject({ pendingHits: 1, l2Reads: 0 });
  });

  it('keeps a write readable while it is being flushed', async () => {
    const { l1, l2, tiered } = setup();
    await l2.write('k-001', body(0), LIFETIME);
    await tiered.write('k-001', body(1), LIFETIME);
    l1.discard('k-001'); // as if evicted
    const release = l2.holdNextWrite('before');
    const flushing = tiered.flush();
    await flush();
    expect((await tiered.read('k-001'))?.value[0]).toBe(1); // not disk's 0
    release();
    await flushing;
    expect((await tiered.read('k-001'))?.value[0]).toBe(1);
  });

  it('isolates the pending value from the caller’s buffer', async () => {
    const { l2, tiered } = setup();
    const buffer = body(1);
    await tiered.write('k-001', buffer, LIFETIME);
    buffer.fill(7);
    await tiered.flush();
    expect(l2.inner.entries.get('k-001')?.value[0]).toBe(1);
  });

  it('reports pending metadata from readMetadata and scan', async () => {
    const { l1, l2, tiered } = setup();
    await l2.write('k-001', body(0), { ...LIFETIME, expiresAt: 1_500 });
    await tiered.write('k-001', body(1), LIFETIME);
    l1.discard('k-001');
    expect((await tiered.readMetadata('k-001'))?.expiresAt).toBe(2_000);

    await tiered.write('k-002', body(2), LIFETIME);
    l1.discard('k-002'); // only pending now
    const scanned = new Map<string, number>();
    for await (const m of tiered.scan()) {
      scanned.set(m.key, m.expiresAt);
    }
    expect([...scanned]).toEqual([
      ['k-001', 2_000],
      ['k-002', 2_000],
    ]);
  });
});

describe('TieredCacheStore – write-behind backpressure', () => {
  it('falls back to a synchronous L2 write when the ring is full', async () => {
    const { l2, tiered } = setup({ ...MANUAL, maxPendingWrites: 2 });
    await tiered.write('k-001', body(1), LIFETIME);
    await tiered.write('k-002', body(2), LIFETIME);
    await tiered.write('k-003', body(3), LIFETIME); // ring full
    expect(l2.inner.entries.has('k-003')).toBe(true); // already on disk
    expect(l2.inner.entries.has('k-001')).toBe(false); // still queued
    expect(tiered.stats()).toMatchObject({
      syncFallbacks: 1,
      pendingWrites: 2,
    });
  });

  it('still coalesces into a queued key when the ring is full', async () => {
    const { tiered } = setup({ ...MANUAL, maxPendingWrites: 1 });
    await tiered.write('k-001', body(1), LIFETIME);
    await tiered.write('k-001', body(2), LIFETIME);
    expect(tiered.stats()).toMatchObject({
      syncFallbacks: 0,
      coalescedWrites: 1,
    });
  });

  it('falls back when the pending byte budget would be exceeded', async () => {
    const { l2, tiered } = setup({ ...MANUAL, maxPendingBytes: PAYLOAD + 10 });
    await tiered.write('k-001', body(1), LIFETIME);
    await tiered.write('k-002', body(2), LIFETIME); // would exceed bytes
    expect(l2.inner.entries.has('k-002')).toBe(true);
    expect(tiered.stats()).toMatchObject({
      syncFallbacks: 1,
      pendingBytes: PAYLOAD,
    });
  });

  it('a fallback write supersedes the queued older value of its key', async () => {
    const { l2, tiered } = setup({ ...MANUAL, maxPendingBytes: PAYLOAD + 10 });
    await tiered.write('k-001', body(1), LIFETIME); // queued
    const bigger = Buffer.alloc(PAYLOAD + 100, 2);
    await tiered.write('k-001', bigger, LIFETIME); // can't coalesce: sync
    await tiered.flush(); // the queued v1 must not land after v2
    expect(l2.calls.write).toBe(1);
    expect(l2.inner.entries.get('k-001')?.value[0]).toBe(2);
    expect(tiered.stats().pendingBytes).toBe(0);
  });

  it('rejects a failed fallback write to its caller and rolls L1 back', async () => {
    const l1 = makeL1();
    const l2 = new FaultyStore();
    const tiered = track(
      new TieredCacheStore({
        l1,
        l2,
        writeBehind: { ...MANUAL, maxPendingWrites: 1 },
      }),
    );
    await tiered.write('k-001', body(1), LIFETIME);
    l2.failNext('write', new Error('EIO'));
    await expect(tiered.write('k-002', body(2), LIFETIME)).rejects.toThrow(
      'EIO',
    );
    expect(l1.segmentOf('k-002')).toBeUndefined();
    await expect(tiered.read('k-002')).resolves.toBeUndefined();
  });
});

describe('TieredCacheStore – write-behind failures', () => {
  it('reports a failed flush, rolls back to L2’s value, and makes flush() reject', async () => {
    const l1 = makeL1();
    const l2 = new FaultyStore();
    const errors: string[] = [];
    const tiered = track(
      new TieredCacheStore({
        l1,
        l2,
        writeBehind: MANUAL,
        onFlushError: (_error, k): void => {
          errors.push(k);
        },
      }),
    );
    await l2.write('k-001', body(0), LIFETIME);
    await tiered.write('k-001', body(1), LIFETIME);
    l2.failNext('write', new Error('EIO'));

    const flushed = tiered.flush();
    await expect(flushed).rejects.toThrow(AggregateError);
    await expect(flushed).rejects.toThrow(/"k-001"/);
    expect(errors).toEqual(['k-001']);
    expect(l1.segmentOf('k-001')).toBeUndefined();
    expect((await tiered.read('k-001'))?.value[0]).toBe(0); // durable truth
    expect(tiered.stats()).toMatchObject({ flushFailures: 1, pendingBytes: 0 });
  });

  it('a failed older write does not roll back a newer pending one', async () => {
    const { l1, l2, tiered } = setup(MANUAL, {
      onFlushError: (): void => undefined,
    });
    await tiered.write('k-001', body(1), LIFETIME);
    const release = l2.holdNextWrite('before');
    const flushing = tiered.flush().catch(() => undefined);
    await flush();
    await tiered.write('k-001', body(2), LIFETIME); // newer, queued
    l2.inner.failNext('write', new Error('EIO')); // v1 will fail
    release();
    await flushing;
    expect(l1.segmentOf('k-001')).toBeDefined(); // v2 kept its L1 copy
    expect((await tiered.read('k-001'))?.value[0]).toBe(2);
    await tiered.flush();
    expect(l2.inner.entries.get('k-001')?.value[0]).toBe(2);
  });
});

describe('TieredCacheStore – write-behind vs delete and clear', () => {
  it('delete cancels a queued write: it never reaches L2', async () => {
    const { l2, tiered } = setup();
    await tiered.write('k-001', body(1), LIFETIME);
    await expect(tiered.delete('k-001')).resolves.toBe(true);
    await tiered.flush();
    expect(l2.calls.write).toBe(0);
    await expect(tiered.read('k-001')).resolves.toBeUndefined();
    expect(tiered.stats()).toMatchObject({ pendingWrites: 0, pendingBytes: 0 });
  });

  it('a delete issued while a flush is writing is applied after it', async () => {
    const { l2, tiered } = setup();
    await tiered.write('k-001', body(1), LIFETIME);
    const release = l2.holdNextWrite('before');
    const flushing = tiered.flush();
    await flush();
    const deleting = tiered.delete('k-001');
    release();
    await Promise.all([flushing, deleting]);
    expect(l2.inner.entries.has('k-001')).toBe(false);
    await expect(tiered.read('k-001')).resolves.toBeUndefined();
  });

  it('a write taken for flushing while deleteIf holds the key lock is dropped', async () => {
    const { l2, tiered } = setup();
    await tiered.write('k-001', body(1), LIFETIME);
    const deleting = tiered.deleteIf('k-001', () => true); // holds the lock
    const flushing = tiered.flush(); // takes k-001, queues behind the lock
    await expect(deleting).resolves.toBe(true);
    await flushing;
    expect(l2.calls.write).toBe(0);
    expect(l2.inner.entries.has('k-001')).toBe(false);
    await expect(tiered.read('k-001')).resolves.toBeUndefined();
  });

  it('deleteIf judges the pending write, not L2’s older copy', async () => {
    const { l2, tiered } = setup();
    await l2.write('k-001', body(0), { ...LIFETIME, expiresAt: 1 });
    await tiered.write('k-001', body(1), LIFETIME); // expiresAt 2_000
    const expired = (m: EntryMetadata): boolean => m.expiresAt < 1_000;
    await expect(tiered.deleteIf('k-001', expired)).resolves.toBe(false);
    await expect(tiered.deleteIf('k-001', () => true)).resolves.toBe(true);
    await tiered.flush();
    expect(l2.inner.entries.has('k-001')).toBe(false);
    await expect(tiered.read('k-001')).resolves.toBeUndefined();
  });

  it('clear cancels queued writes and waits for the one being written', async () => {
    const { l2, tiered } = setup({ ...MANUAL, batchSize: 1 });
    await tiered.write('k-001', body(1), LIFETIME);
    await tiered.write('k-002', body(2), LIFETIME);
    const release = l2.holdNextWrite('before');
    const flushing = tiered.flush(); // k-001 is being written
    await flush();
    const clearing = tiered.clear();
    await flush();
    release();
    await Promise.all([clearing, flushing]);
    expect(l2.inner.entries.size).toBe(0);
    expect(l2.calls.write).toBe(1); // k-002 was cancelled
    expect(tiered.stats()).toMatchObject({ pendingWrites: 0, pendingBytes: 0 });
  });

  it('keeps writes issued while clear() is running', async () => {
    const { l2, tiered } = setup();
    await tiered.write('k-001', body(1), LIFETIME);
    const release = l2.holdNextClear();
    const clearing = tiered.clear();
    await flush();
    await tiered.write('k-002', body(2), LIFETIME); // during the clear
    release();
    await clearing;
    expect((await tiered.read('k-002'))?.value[0]).toBe(2);
    await expect(tiered.read('k-001')).resolves.toBeUndefined();
    await tiered.flush();
    expect([...l2.inner.entries.keys()]).toEqual(['k-002']);
  });
});

describe('TieredCacheStore – close() drains strictly', () => {
  it('flushes every pending write before resolving', async () => {
    const { l2, tiered } = setup({ ...MANUAL, batchSize: 3 });
    for (let i = 0; i < 20; i++) {
      await tiered.write(`k-0${String(i).padStart(2, '0')}`, body(i), LIFETIME);
    }
    await tiered.close();
    expect(l2.inner.entries.size).toBe(20);
    expect(tiered.stats()).toMatchObject({ pendingWrites: 0, pendingBytes: 0 });
    expect(tiered.isClosed).toBe(true);
  });

  it('writes synchronously after close, and is idempotent', async () => {
    const { l2, tiered } = setup();
    const first = tiered.close();
    expect(tiered.close()).toBe(first);
    await first;
    await tiered.write('k-001', body(1), LIFETIME);
    expect(l2.inner.entries.has('k-001')).toBe(true); // no buffering now
    expect(tiered.stats().syncFallbacks).toBe(0); // closed, not backpressure
  });

  it('waits for a fallback write that is still in flight', async () => {
    const { l2, tiered } = setup({ ...MANUAL, maxPendingWrites: 1 });
    await tiered.write('k-001', body(1), LIFETIME);
    const release = l2.holdNextWrite('before');
    const fallback = tiered.write('k-002', body(2), LIFETIME);
    await flush();
    let closed = false;
    const closing = tiered.close().then(() => {
      closed = true;
    });
    await flush();
    expect(closed).toBe(false);
    release();
    await Promise.all([closing, fallback]);
    expect(l2.inner.entries.size).toBe(2);
  });

  it('rejects if a pending write could not be persisted (never silent)', async () => {
    const l2 = new FaultyStore();
    const tiered = track(
      new TieredCacheStore({
        l1: makeL1(),
        l2,
        writeBehind: MANUAL,
        onFlushError: (): void => undefined,
      }),
    );
    await tiered.write('k-001', body(1), LIFETIME);
    await tiered.write('k-002', body(2), LIFETIME);
    l2.failNext('write', new Error('EIO'));
    await expect(tiered.close()).rejects.toThrow(/1 write\(s\) failed/);
    expect(l2.entries.size).toBe(1);
  });

  it('leaves no timer behind', async () => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] });
    try {
      const { tiered } = setup({ flushDelayMs: 1_000 });
      await tiered.write('k-001', body(1), LIFETIME);
      expect(vi.getTimerCount()).toBe(1);
      await tiered.close();
      expect(vi.getTimerCount()).toBe(0);
    } finally {
      vi.useRealTimers();
    }
  });
});

describe('TieredCacheStore – write-behind randomized model check', () => {
  /** Mulberry32: small seeded PRNG so failures are reproducible. */
  function prng(seed: number): () => number {
    let a = seed;
    return () => {
      a = (a + 0x6d2b79f5) | 0;
      let t = Math.imul(a ^ (a >>> 15), 1 | a);
      t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
      return ((t ^ (t >>> 14)) >>> 0) / 4_294_967_296;
    };
  }

  /** L2 whose every operation takes a random number of event-loop turns. */
  class SlowStore extends FaultyStore {
    constructor(readonly random: () => number) {
      super();
    }
    async #lag(): Promise<void> {
      for (let i = Math.floor(this.random() * 3); i > 0; i--) {
        await new Promise((r) => setImmediate(r));
      }
    }
    override async read(k: CacheKey): Promise<CacheEntry | undefined> {
      await this.#lag();
      const entry = await super.read(k);
      await this.#lag();
      return entry;
    }
    override async write(
      k: CacheKey,
      v: Uint8Array,
      l: EntryLifetime,
      a?: EntryAttributes,
    ): Promise<EntryMetadata> {
      await this.#lag();
      const m = await super.write(k, v, l, a);
      await this.#lag();
      return m;
    }
    override async delete(k: CacheKey): Promise<boolean> {
      await this.#lag();
      return super.delete(k);
    }
  }

  it.each([1, 2, 3, 4, 5, 6])(
    'reads always see the latest write, and close() persists exactly the model (seed %i)',
    async (seed) => {
      const random = prng(seed);
      const l2 = new SlowStore(random);
      const tiered = track(
        new TieredCacheStore({
          l1: makeL1(), // tiny: admission declines and evictions are common
          l2,
          writeBehind: {
            flushDelayMs: 0,
            batchSize: 3,
            maxPendingWrites: 6,
            maxPendingBytes: 8 * PAYLOAD,
          },
        }),
      );
      const model = new Map<string, number>();
      const violations: string[] = [];
      let version = 0;

      for (let step = 0; step < 1_500; step++) {
        const k = `k-${String(Math.floor(random() * 12)).padStart(3, '0')}`;
        const op = random();
        if (op < 0.45) {
          const entry = await tiered.read(k);
          const got = entry?.value.readUInt32BE(0);
          if (got !== model.get(k)) {
            violations.push(
              `step ${step}: ${k} read ${got}, want ${model.get(k)}`,
            );
          }
        } else if (op < 0.85) {
          version += 1;
          const value = Buffer.alloc(PAYLOAD);
          value.writeUInt32BE(version, 0);
          await tiered.write(k, value, LIFETIME);
          model.set(k, version);
        } else if (op < 0.95) {
          await tiered.delete(k);
          model.delete(k);
        } else if (op < 0.99) {
          await new Promise((r) => setImmediate(r)); // let flushes progress
        } else {
          await tiered.clear();
          model.clear();
        }
      }

      await tiered.close();
      expect(violations).toEqual([]);
      const persisted = new Map(
        [...l2.entries].map(([k, e]) => [k, e.value.readUInt32BE(0)]),
      );
      expect(persisted).toEqual(model); // nothing lost, nothing resurrected
      expect(tiered.stats()).toMatchObject({
        pendingWrites: 0,
        pendingBytes: 0,
      });
      expect(tiered.stats().coalescedWrites).toBeGreaterThan(0);
      expect(tiered.stats().syncFallbacks).toBeGreaterThan(0);
    },
  );
});

describe('TieredCacheStore – sweeperView', () => {
  const EXPIRED = { createdAt: 0, expiresAt: 1, staleUntil: 2 };
  const FRESH = { createdAt: 10_000, expiresAt: 20_000, staleUntil: 30_000 };

  /** Rejects if `promise` has not settled within `ms` (deadlock guard). */
  function within<T>(promise: Promise<T>, ms: number): Promise<T> {
    return Promise.race([
      promise,
      new Promise<never>((_, reject) =>
        setTimeout(() => reject(new Error(`still pending after ${ms} ms`)), ms),
      ),
    ]);
  }

  it('deletes through the given disk store and drops the L1 copy', async () => {
    const l1 = makeL1();
    const l2 = new FaultyStore(); // stands in for the resilient decorator
    const raw = new FaultyStore(); // the disk the sweeper should use
    const tiered = track(new TieredCacheStore({ l1, l2, writeBehind: false }));
    await raw.write('k-001', body(1), EXPIRED);
    await l1.write('k-001', body(1), EXPIRED);

    const sweeper = new ExpirySweeper(tiered.sweeperView(raw), {
      clock: new FakeClock(10_000),
    });
    expect((await sweeper.sweepOnce()).expired).toBe(1);
    expect(raw.entries.has('k-001')).toBe(false);
    expect(l1.segmentOf('k-001')).toBeUndefined();
    expect(l2.calls.deleteIf).toBe(0); // never went through the decorator
  });

  it('skips a key that is being written instead of waiting for it', async () => {
    const { l2, tiered } = setup(false);
    await l2.inner.write('k-001', body(0), EXPIRED);
    const release = l2.holdNextWrite('before');
    const writing = tiered.write('k-001', body(1), FRESH); // holds the lock
    await flush();
    const view = tiered.sweeperView(l2);
    await expect(view.deleteIf('k-001', () => true)).resolves.toBe(false);
    release();
    await writing;
    expect(l2.inner.entries.get('k-001')?.value[0]).toBe(1);
  });

  it('an emergency sweep inside a write of an expired key does not deadlock', async () => {
    const raw = new FaultyStore();
    await raw.write('k-001', body(0), EXPIRED); // the key being refreshed
    await raw.write('k-002', body(0), EXPIRED); // reclaimable garbage
    const sweeps: Array<Promise<unknown>> = [];
    let sweeper: ExpirySweeper | undefined;
    const resilient = new ResilientCacheStore(raw, {
      retry: { maxAttempts: 1 },
      circuit: false,
      onCapacityExhausted: async (): Promise<void> => {
        const sweep = sweeper?.sweepOnce();
        sweeps.push(sweep ?? Promise.resolve());
        await sweep;
      },
    });
    const tiered = track(
      new TieredCacheStore({ l1: makeL1(), l2: resilient, writeBehind: false }),
    );
    sweeper = new ExpirySweeper(tiered.sweeperView(raw), {
      clock: new FakeClock(10_000),
      batchPauseMs: 0,
    });

    raw.failNext('write', errno('ENOSPC'));
    await within(tiered.write('k-001', body(1), FRESH), 2_000);
    expect(sweeps).toHaveLength(1);
    expect(raw.entries.has('k-002')).toBe(false); // space was reclaimed
    expect(raw.entries.get('k-001')?.value[0]).toBe(1); // and the write landed
    resilient.close();
  });
});
