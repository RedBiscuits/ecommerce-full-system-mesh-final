import { describe, expect, it } from 'vitest';

import { CacheManager } from '../../../src/cache/cache-manager';
import { InvalidCacheKeyError } from '../../../src/cache/errors';
import { ExpirySweeper } from '../../../src/cache/expiry-sweeper';
import {
  ENTRY_OVERHEAD_BYTES,
  SlruMemoryStore,
} from '../../../src/cache/slru-memory-store';
import { FrequencySketch } from '../../../src/cache/tiny-lfu';
import type { EntryMetadata } from '../../../src/cache/types';
import { FakeClock } from '../../helpers/fake-clock';

const LIFETIME = { createdAt: 1_000, expiresAt: 2_000, staleUntil: 3_000 };
/** Every test key has this many characters, so charges are predictable. */
const KEY_LENGTH = 5;
/** Charged size of each test entry: 10 of them fill 10_000 bytes. */
const ENTRY = 1_000;
const PAYLOAD = ENTRY - ENTRY_OVERHEAD_BYTES - 2 * KEY_LENGTH;

/** `k-007` style keys: fixed length keeps the byte math exact. */
const key = (i: number): string => `k-${String(i).padStart(3, '0')}`;

const body = (fill: number, size = PAYLOAD): Uint8Array =>
  new Uint8Array(size).fill(fill);

/**
 * 10_000 bytes → probation 2_000 (2 entries), protected 8_000 (8 entries).
 * The sketch is seeded and never ages, so frequencies are exact.
 */
function makeStore(
  overrides: { maxMemoryBytes?: number; probationRatio?: number } = {},
): SlruMemoryStore {
  return new SlruMemoryStore({
    maxMemoryBytes: 10_000,
    sketch: new FrequencySketch({
      capacity: 1_024,
      seed: 1,
      sampleSize: 1e12,
    }),
    ...overrides,
  });
}

/** What `CacheManager.getOrLoad` does on a miss: read, then write. */
async function load(
  store: SlruMemoryStore,
  k: string,
  fill = 1,
): Promise<boolean> {
  if ((await store.read(k)) !== undefined) {
    return true;
  }
  await store.write(k, body(fill), LIFETIME);
  return (await store.readMetadata(k)) !== undefined;
}

async function keys(store: SlruMemoryStore): Promise<string[]> {
  const found: string[] = [];
  for await (const metadata of store.scan()) {
    found.push(metadata.key);
  }
  return found.sort();
}

describe('SlruMemoryStore – CacheStore contract', () => {
  it('round-trips payload and metadata', async () => {
    const store = makeStore();
    const written = await store.write('k-001', Buffer.from('hello'), LIFETIME, {
      'content-type': 'text/plain',
    });
    expect(written).toMatchObject({
      key: 'k-001',
      ...LIFETIME,
      size: 5,
      attributes: { 'content-type': 'text/plain' },
    });
    expect(written.checksum).toMatch(/^sha256:[0-9a-f]{64}$/);
    expect(Object.isFrozen(written)).toBe(true);
    expect(Object.isFrozen(written.attributes)).toBe(true);

    const entry = await store.read('k-001');
    expect(entry?.value.toString()).toBe('hello');
    expect(entry?.metadata).toEqual(written);
    await expect(store.readMetadata('k-001')).resolves.toEqual(written);
  });

  it('resolves undefined on a miss', async () => {
    const store = makeStore();
    await expect(store.read('nope!')).resolves.toBeUndefined();
    await expect(store.readMetadata('nope!')).resolves.toBeUndefined();
  });

  it('isolates cached bytes from the caller’s buffers', async () => {
    const store = makeStore();
    const input = Buffer.from('abc');
    await store.write('k-001', input, LIFETIME);
    input.fill(0); // caller reuses its buffer
    const update = Buffer.from('xyz');
    await store.write('k-002', Buffer.from('old'), LIFETIME);
    await store.write('k-002', update, LIFETIME); // the update path copies too
    update.fill(0);
    expect((await store.read('k-002'))?.value.toString()).toBe('xyz');

    const first = await store.read('k-001');
    first?.value.fill(0); // caller mutates what it was given
    const second = await store.read('k-001');
    expect(second?.value.toString()).toBe('abc');
  });

  it('deletes idempotently', async () => {
    const store = makeStore();
    await store.write('k-001', body(1), LIFETIME);
    await expect(store.delete('k-001')).resolves.toBe(true);
    await expect(store.delete('k-001')).resolves.toBe(false);
    await expect(store.read('k-001')).resolves.toBeUndefined();
    expect(store.stats().bytes).toBe(0);
  });

  it('deleteIf acts on current metadata only', async () => {
    const store = makeStore();
    await store.write('k-001', body(1), LIFETIME);
    const seen: EntryMetadata[] = [];
    await expect(
      store.deleteIf('k-001', (m) => {
        seen.push(m);
        return false;
      }),
    ).resolves.toBe(false);
    expect(seen).toHaveLength(1);
    await expect(store.deleteIf('k-001', () => true)).resolves.toBe(true);
    await expect(store.deleteIf('k-001', () => true)).resolves.toBe(false);
  });

  it('clear() empties both segments', async () => {
    const store = makeStore();
    await store.write('k-001', body(1), LIFETIME);
    await store.read('k-001'); // promote to protected
    await store.write('k-002', body(2), LIFETIME);
    await store.clear();
    expect(store.stats()).toMatchObject({
      entries: 0,
      bytes: 0,
      probationBytes: 0,
      protectedBytes: 0,
    });
    expect(await keys(store)).toEqual([]);
  });

  it('scan() yields every entry in either segment exactly once', async () => {
    const store = makeStore();
    for (let i = 0; i < 4; i++) {
      await store.write(key(i), body(i), LIFETIME);
      await store.read(key(i)); // each moves to protected, freeing probation
    }
    await store.write(key(9), body(9), LIFETIME); // stays in probation
    expect(await keys(store)).toEqual([
      'k-000',
      'k-001',
      'k-002',
      'k-003',
      'k-009',
    ]);
  });

  it('rejects invalid keys like the file store does', async () => {
    const store = makeStore();
    await expect(store.read('')).rejects.toThrow(InvalidCacheKeyError);
    await expect(store.write('', body(1), LIFETIME)).rejects.toThrow(
      InvalidCacheKeyError,
    );
  });

  it('validates options', () => {
    for (const maxMemoryBytes of [0, -1, 1.5, Number.NaN, Infinity]) {
      expect(() => new SlruMemoryStore({ maxMemoryBytes })).toThrow(RangeError);
    }
    for (const probationRatio of [0, 1, -0.1, 1.5, Number.NaN]) {
      expect(
        () => new SlruMemoryStore({ maxMemoryBytes: 1_000, probationRatio }),
      ).toThrow(RangeError);
    }
  });
});

describe('SlruMemoryStore – segments and byte budget', () => {
  it('reserves 20 % of maxMemoryBytes for probation by default', () => {
    const store = makeStore();
    expect(store.probationCapacity).toBe(2_000);
    expect(store.protectedCapacity).toBe(8_000);
  });

  it('charges payload + key + attributes + fixed overhead', async () => {
    const store = makeStore();
    await store.write('k-001', new Uint8Array(100), LIFETIME, { ab: 'cde' });
    expect(store.stats().bytes).toBe(
      ENTRY_OVERHEAD_BYTES + 100 + 2 * 5 + 2 * (2 + 3),
    );
  });

  it('charges empty payloads too, so they cannot grow without bound', async () => {
    const store = makeStore();
    for (let i = 0; i < 100; i++) {
      await store.write(key(i), new Uint8Array(0), LIFETIME);
    }
    expect(store.stats().entries).toBeLessThan(100);
    expect(store.stats().probationBytes).toBeLessThanOrEqual(2_000);
  });

  it('admits new keys into probation and promotes them on a read hit', async () => {
    const store = makeStore();
    await store.write('k-001', body(1), LIFETIME);
    expect(store.stats()).toMatchObject({
      probationBytes: ENTRY,
      protectedBytes: 0,
    });
    await store.read('k-001');
    expect(store.stats()).toMatchObject({
      probationBytes: 0,
      protectedBytes: ENTRY,
    });
  });

  it('readMetadata does not count as use (no promotion)', async () => {
    const store = makeStore();
    await store.write('k-001', body(1), LIFETIME);
    await store.readMetadata('k-001');
    expect(store.stats().protectedBytes).toBe(0);
  });

  it('a protected hit refreshes recency, so the least recently read is demoted', async () => {
    const store = makeStore();
    for (let i = 0; i < 8; i++) {
      await store.write(key(i), body(i), LIFETIME);
      await store.read(key(i));
    }
    await store.read(key(0)); // k-000 was protected's LRU; now it is MRU
    await store.write(key(8), body(8), LIFETIME);
    await store.read(key(8)); // overflow: demote the LRU, now k-001
    expect(store.segmentOf(key(0))).toBe('protected');
    expect(store.segmentOf(key(1))).toBe('probation');
    expect(store.segmentOf('k-999')).toBeUndefined();
  });

  it('demotes protected overflow to probation instead of evicting it', async () => {
    const store = makeStore();
    // Fill protected with 8 entries: k-000 is its LRU.
    for (let i = 0; i < 8; i++) {
      await store.write(key(i), body(i), LIFETIME);
      await store.read(key(i));
    }
    expect(store.stats().protectedBytes).toBe(8_000);

    await store.write(key(8), body(8), LIFETIME);
    await store.read(key(8)); // promote a 9th: k-000 is demoted
    expect(store.stats()).toMatchObject({
      entries: 9,
      protectedBytes: 8_000,
      probationBytes: ENTRY,
      evicted: 0,
    });
    // Still cached, and a read earns it its protected slot back.
    expect((await store.read(key(0)))?.value[0]).toBe(0);
    expect(store.stats().protectedBytes).toBe(8_000);
  });
});

describe('SlruMemoryStore – TinyLFU admission', () => {
  it('rejects one-hit wonders once probation is full', async () => {
    const store = makeStore();
    expect(await load(store, 'k-001')).toBe(true);
    expect(await load(store, 'k-002')).toBe(true); // probation now full

    // Seen once, like each victim: a tie is not "higher", so it's rejected.
    expect(await load(store, 'k-003')).toBe(false);
    expect(await keys(store)).toEqual(['k-001', 'k-002']);
    expect(store.stats()).toMatchObject({ rejected: 1, evicted: 0 });
  });

  it('admits a newcomer more frequent than the victim, evicting the victim', async () => {
    const store = makeStore();
    await load(store, 'k-001'); // LRU of probation → the victim
    await load(store, 'k-002');
    expect(await load(store, 'k-003')).toBe(false); // frequency 1: rejected
    expect(await load(store, 'k-003')).toBe(true); // frequency 2: admitted

    expect(await keys(store)).toEqual(['k-002', 'k-003']);
    expect(store.stats()).toMatchObject({ evicted: 1, rejected: 1 });
  });

  it('is all-or-nothing when the newcomer needs several victims', async () => {
    const store = makeStore();
    await load(store, 'k-001'); // LRU victim: frequency 1
    for (let i = 0; i < 3; i++) {
      await store.read('k-002');
    }
    await store.write('k-002', body(2), LIFETIME); // MRU victim: frequency 3

    // A double-size newcomer needs both slots. At frequency 2 it beats
    // k-001 but not k-002, so neither may be evicted.
    const big = body(7, PAYLOAD + ENTRY);
    await store.read('big!!');
    await store.read('big!!');
    await store.write('big!!', big, LIFETIME);
    expect(await keys(store)).toEqual(['k-001', 'k-002']);
    expect(store.stats()).toMatchObject({ evicted: 0, rejected: 1 });

    // At frequency 4 it beats both, and replaces both.
    await store.read('big!!');
    await store.read('big!!');
    await store.write('big!!', big, LIFETIME);
    expect(await keys(store)).toEqual(['big!!']);
    expect(store.stats()).toMatchObject({ evicted: 2, probationBytes: 2_000 });
  });

  it('never admits an entry larger than probation, and evicts nothing for it', async () => {
    const store = makeStore();
    await load(store, 'k-001');
    for (let i = 0; i < 10; i++) {
      await store.read('huge!');
    }
    await store.write('huge!', body(1, 2_000), LIFETIME); // charge > 2_000
    expect(await keys(store)).toEqual(['k-001']);
    expect(store.stats()).toMatchObject({ rejected: 1, evicted: 0 });
  });

  it('keeps a hot working set through a one-pass scan (scan resistance)', async () => {
    const store = makeStore({ maxMemoryBytes: 50_000 }); // 10 + 40 slots
    const hot = Array.from({ length: 30 }, (_, i) => key(i));
    for (let round = 0; round < 3; round++) {
      for (const k of hot) {
        await load(store, k);
      }
    }
    expect(await keys(store)).toEqual(expect.arrayContaining(hot));

    for (let i = 100; i < 600; i++) {
      await load(store, key(i)); // 500 keys, each requested exactly once
    }
    const hits = store.stats().hits;
    for (const k of hot) {
      expect(await store.read(k)).toBeDefined();
    }
    expect(store.stats().hits - hits).toBe(hot.length);
  });
});

describe('SlruMemoryStore – updates', () => {
  it('replaces a resident entry without an admission check', async () => {
    const store = makeStore();
    await load(store, 'k-001');
    await load(store, 'k-002'); // probation full
    await store.write('k-001', body(9), LIFETIME); // update, not a newcomer
    expect((await store.read('k-001'))?.value[0]).toBe(9);
    expect(store.stats().rejected).toBe(0);
  });

  it('drops (never serves the old value of) an entry that outgrows its segment', async () => {
    const store = makeStore();
    await store.write('k-001', body(1), LIFETIME);
    await store.write('k-002', body(2), LIFETIME);
    await store.write('k-001', body(3, 5_000), LIFETIME); // > probation
    await expect(store.read('k-001')).resolves.toBeUndefined();
    // …and its neighbour was not flushed to make room for it.
    expect(await keys(store)).toEqual(['k-002']);
    expect(store.stats().bytes).toBe(ENTRY);
  });

  it('evicts from probation’s tail when an update grows past capacity', async () => {
    const store = makeStore();
    await store.write('k-001', body(1), LIFETIME); // LRU
    await store.write('k-002', body(2), LIFETIME);
    await store.write('k-002', body(2, PAYLOAD + 500), LIFETIME);
    expect(await keys(store)).toEqual(['k-002']);
    expect(store.stats()).toMatchObject({ evicted: 1, probationBytes: 1_500 });
  });
});

describe('SlruMemoryStore – offer (promotion from another tier)', () => {
  const metadataFor = (k: string, size: number): EntryMetadata => ({
    key: k,
    ...LIFETIME,
    size,
    checksum: 'sha256:from-l2',
    attributes: { etag: '"v1"' },
  });

  it('stores the given metadata as-is, without recomputing the checksum', async () => {
    const store = makeStore();
    expect(store.offer(metadataFor('k-001', PAYLOAD), body(4))).toBe(true);
    const metadata = await store.readMetadata('k-001');
    expect(metadata?.checksum).toBe('sha256:from-l2');
    expect(Object.isFrozen(metadata)).toBe(true);
    expect(Object.isFrozen(metadata?.attributes)).toBe(true);
  });

  it('goes through admission for new keys and does not count as an access', async () => {
    const store = makeStore();
    await load(store, 'k-001');
    await load(store, 'k-002'); // probation full, victims at frequency 1
    expect(store.offer(metadataFor('k-003', PAYLOAD), body(3))).toBe(false);
    expect(store.offer(metadataFor('k-003', PAYLOAD), body(3))).toBe(false);
    expect(store.stats()).toMatchObject({ rejected: 2, hits: 0, misses: 2 });
  });

  it('replaces a resident key', async () => {
    const store = makeStore();
    await store.write('k-001', body(1), LIFETIME);
    expect(store.offer(metadataFor('k-001', PAYLOAD), body(8))).toBe(true);
    expect((await store.read('k-001'))?.value[0]).toBe(8);
  });

  it('rejects metadata whose size does not match the payload', () => {
    const store = makeStore();
    expect(() => store.offer(metadataFor('k-001', 3), body(1))).toThrow(
      RangeError,
    );
  });
});

describe('SlruMemoryStore – randomized model check', () => {
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

  it.each([1, 2, 3, 4, 5])(
    'never exceeds its budgets or serves a stale value (seed %i)',
    async (seed) => {
      const random = prng(seed);
      const store = new SlruMemoryStore({
        maxMemoryBytes: 20_000,
        sketch: new FrequencySketch({
          capacity: 64,
          seed,
          sampleSize: 200, // ages often
          scheduleReset: (reset): void => reset(),
        }),
      });
      /** Last value written per key: a hit must return exactly this. */
      const model = new Map<string, number>();
      let version = 0;
      const violations: string[] = [];

      for (let step = 0; step < 4_000; step++) {
        // Skewed key choice: a few hot keys plus a long cold tail.
        const k = key(
          random() < 0.6
            ? Math.floor(random() * 8)
            : Math.floor(random() * 400),
        );
        const op = random();
        if (op < 0.55) {
          const entry = await store.read(k);
          if (
            entry !== undefined &&
            entry.value.readUInt32BE(0) !== model.get(k)
          ) {
            violations.push(`step ${step}: stale or foreign value for ${k}`);
          }
        } else if (op < 0.9) {
          version += 1;
          // 5 % are bigger than probation (4_000): exercises the oversize
          // paths (rejected newcomers, dropped updates).
          const size =
            random() < 0.05
              ? 4_000 + Math.floor(random() * 4_000)
              : 4 + Math.floor(random() * 3_000);
          const value = Buffer.alloc(size);
          value.writeUInt32BE(version, 0);
          await store.write(k, value, LIFETIME);
          model.set(k, version);
        } else if (op < 0.97) {
          await store.delete(k);
          model.delete(k);
        } else {
          await store.deleteIf(k, () => random() < 0.5);
          if ((await store.readMetadata(k)) === undefined) {
            model.delete(k);
          }
        }

        const { probationBytes, protectedBytes } = store.stats();
        if (
          probationBytes < 0 ||
          probationBytes > store.probationCapacity ||
          protectedBytes < 0 ||
          protectedBytes > store.protectedCapacity
        ) {
          violations.push(
            `step ${step}: probation ${probationBytes}, protected ${protectedBytes}`,
          );
        }
      }

      expect(violations).toEqual([]);

      // Byte totals match the entries actually present.
      let charged = 0;
      let entries = 0;
      for await (const metadata of store.scan()) {
        entries += 1;
        charged += ENTRY_OVERHEAD_BYTES + metadata.size + 2 * KEY_LENGTH;
      }
      expect(store.stats()).toMatchObject({ entries, bytes: charged });
    },
  );
});

describe('SlruMemoryStore – as a CacheManager / ExpirySweeper backend', () => {
  it('serves getOrLoad hits from memory', async () => {
    const clock = new FakeClock();
    const store = makeStore();
    const cache = new CacheManager(store, { clock, ttlJitterRatio: 0 });
    let calls = 0;
    const loader = async (): Promise<string> => {
      calls += 1;
      return 'payload';
    };
    const first = await cache.getOrLoad('k-001', loader);
    const second = await cache.getOrLoad('k-001', loader);
    expect(first.source).toBe('loader');
    expect(second.source).toBe('cache');
    expect(second.value.toString()).toBe('payload');
    expect(calls).toBe(1);
  });

  it('can be swept of expired entries', async () => {
    const clock = new FakeClock(10_000);
    const store = makeStore();
    await store.write('k-001', body(1), {
      createdAt: 0,
      expiresAt: 1,
      staleUntil: 2,
    });
    await store.write('k-002', body(2), {
      createdAt: 10_000,
      expiresAt: 20_000,
      staleUntil: 30_000,
    });
    const sweeper = new ExpirySweeper(store, { clock });
    await sweeper.sweepOnce();
    expect(await keys(store)).toEqual(['k-002']);
  });
});
