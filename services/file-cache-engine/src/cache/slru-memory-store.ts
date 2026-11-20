import { assertValidKey, checksumOf } from './cache-key';
import { FrequencySketch, MAX_SKETCH_CAPACITY } from './tiny-lfu';
import type {
  CacheEntry,
  CacheKey,
  CacheStore,
  EntryAttributes,
  EntryLifetime,
  EntryMetadata,
} from './types';

export interface SlruMemoryStoreOptions {
  /**
   * Memory budget across both segments, in bytes. Each entry is charged its
   * payload plus an estimate of its key, attributes and bookkeeping (see
   * {@link ENTRY_OVERHEAD_BYTES}), so even empty payloads are bounded.
   */
  readonly maxMemoryBytes: number;
  /**
   * Share of `maxMemoryBytes` reserved for the probationary segment. The rest
   * is the protected segment. Default: `0.2`.
   */
  readonly probationRatio?: number;
  /**
   * Frequency sketch used for admission decisions. Default: a sketch sized
   * for `maxMemoryBytes / 4 KiB` entries. Pass your own to tune it, or to
   * share access history with another tier.
   */
  readonly sketch?: FrequencySketch;
  /** Hash seed for the default sketch (ignored when `sketch` is given). */
  readonly seed?: number;
}

export interface SlruMemoryStoreStats {
  readonly entries: number;
  /** Charged bytes across both segments (never above `maxMemoryBytes`). */
  readonly bytes: number;
  readonly probationBytes: number;
  readonly protectedBytes: number;
  readonly hits: number;
  readonly misses: number;
  /** New keys that were stored. */
  readonly admitted: number;
  /** New keys turned away: too big, or not more frequent than the victims. */
  readonly rejected: number;
  /** Entries removed to make room (`delete`/`clear` not included). */
  readonly evicted: number;
}

/**
 * Fixed per-entry charge on top of the payload and key. It covers the entry
 * node, the frozen metadata and checksum string, the `Buffer` wrapper, and
 * the index slot. Measured at ~510–540 bytes per entry on Node 22 (V8 12).
 */
export const ENTRY_OVERHEAD_BYTES = 512;
const DEFAULT_PROBATION_RATIO = 0.2;
/** Average entry size assumed when sizing the default sketch. */
const ESTIMATED_ENTRY_BYTES = 4 * 1024;

/**
 * In-memory {@link CacheStore} with segmented LRU (SLRU) eviction and
 * TinyLFU admission, budgeted in bytes.
 *
 * - **Probationary segment** (`probationRatio`, default 20 %): new keys land
 *   here. When a newcomer does not fit, the least recently used entries
 *   (from the tail) become victims. The newcomer is admitted only if its
 *   estimated access frequency is *strictly higher* than every victim's.
 *   Then the victims are evicted. Otherwise the newcomer is dropped and
 *   nothing is evicted. Scans and one-hit wonders therefore cannot push out
 *   entries that are re-read.
 * - **Protected segment** (the rest): a read hit in probation promotes the
 *   entry here. When protected overflows, its LRU entries are demoted back
 *   to the head of probation (they get a second chance), and probation
 *   evicts from its tail.
 *
 * Both segments have fixed capacities (classic SLRU). Until entries are
 * re-read, the cache holds at most the probationary share. Demand is
 * recorded only by `read`: `readMetadata`, `scan` and `deleteIf` do not
 * change recency or frequency, so sweeps and health probes leave the
 * policy alone.
 *
 * Lifetimes are stored but not interpreted, as with `FsCacheStore`. Expired
 * entries stay until they are evicted, deleted, or removed by a sweeper.
 * Payloads are copied on write and on read, so callers can never mutate
 * cached bytes. Every operation runs synchronously inside its promise,
 * which makes each one atomic without locks.
 *
 * `write` always resolves with the entry's metadata, even when admission
 * rejects a new key. A cache in front of a slower tier is allowed to
 * decline. Use {@link stats} to observe admissions and rejections.
 */
export class SlruMemoryStore implements CacheStore {
  readonly #index = new Map<CacheKey, EntryNode>();
  readonly #probation = new LruList();
  readonly #protected = new LruList();
  readonly #maxBytes: number;
  readonly #probationCapacity: number;
  readonly #protectedCapacity: number;
  readonly #sketch: FrequencySketch;
  #hits = 0;
  #misses = 0;
  #admitted = 0;
  #rejected = 0;
  #evicted = 0;

  constructor(options: SlruMemoryStoreOptions) {
    const { maxMemoryBytes } = options;
    if (!Number.isSafeInteger(maxMemoryBytes) || maxMemoryBytes < 1) {
      throw new RangeError('maxMemoryBytes must be a positive safe integer');
    }
    const ratio = options.probationRatio ?? DEFAULT_PROBATION_RATIO;
    if (!(ratio > 0 && ratio < 1)) {
      throw new RangeError('probationRatio must be in (0, 1)');
    }
    this.#maxBytes = maxMemoryBytes;
    this.#probationCapacity = Math.floor(maxMemoryBytes * ratio);
    this.#protectedCapacity = maxMemoryBytes - this.#probationCapacity;
    this.#sketch =
      options.sketch ??
      new FrequencySketch({
        capacity: Math.min(
          MAX_SKETCH_CAPACITY,
          Math.max(16, Math.ceil(maxMemoryBytes / ESTIMATED_ENTRY_BYTES)),
        ),
        seed: options.seed,
      });
  }

  get maxMemoryBytes(): number {
    return this.#maxBytes;
  }

  get probationCapacity(): number {
    return this.#probationCapacity;
  }

  get protectedCapacity(): number {
    return this.#protectedCapacity;
  }

  /**
   * Which segment holds `key`. A pure lookup for diagnostics and tests: it
   * does not count as an access.
   */
  segmentOf(key: CacheKey): 'probation' | 'protected' | undefined {
    const node = this.#index.get(key);
    if (node === undefined) {
      return undefined;
    }
    return node.list === this.#probation ? 'probation' : 'protected';
  }

  stats(): SlruMemoryStoreStats {
    return {
      entries: this.#index.size,
      bytes: this.#probation.bytes + this.#protected.bytes,
      probationBytes: this.#probation.bytes,
      protectedBytes: this.#protected.bytes,
      hits: this.#hits,
      misses: this.#misses,
      admitted: this.#admitted,
      rejected: this.#rejected,
      evicted: this.#evicted,
    };
  }

  async read(key: CacheKey): Promise<CacheEntry | undefined> {
    assertValidKey(key);
    // Misses count too: that is how a key not yet cached builds the
    // frequency it needs to win admission.
    this.#sketch.increment(key);
    const node = this.#index.get(key);
    if (node === undefined) {
      this.#misses += 1;
      return undefined;
    }
    this.#hits += 1;
    if (node.list === this.#probation) {
      this.#probation.remove(node);
      this.#protected.pushMru(node);
      this.#rebalance();
    } else {
      this.#protected.moveToMru(node);
    }
    return { metadata: node.metadata, value: Buffer.from(node.value) };
  }

  async readMetadata(key: CacheKey): Promise<EntryMetadata | undefined> {
    assertValidKey(key);
    return this.#index.get(key)?.metadata;
  }

  async write(
    key: CacheKey,
    value: Uint8Array,
    lifetime: EntryLifetime,
    attributes: EntryAttributes = {},
  ): Promise<EntryMetadata> {
    assertValidKey(key);
    const metadata: EntryMetadata = Object.freeze({
      key,
      createdAt: lifetime.createdAt,
      expiresAt: lifetime.expiresAt,
      staleUntil: lifetime.staleUntil,
      size: value.byteLength,
      checksum: checksumOf(value),
      attributes: Object.freeze({ ...attributes }),
    });
    this.#store(metadata, value);
    return metadata;
  }

  /**
   * Stores an entry whose metadata was already produced (and verified) by
   * another tier, without recomputing the checksum. It follows the same rules
   * as `write`: a resident key is replaced, and a new key must pass admission.
   * It does not count as an access.
   *
   * Synchronous on purpose: a tier coordinator can check that a promotion is
   * still valid and insert it without another operation running in between.
   *
   * @returns whether `metadata.key` is resident afterwards.
   */
  offer(metadata: EntryMetadata, value: Uint8Array): boolean {
    assertValidKey(metadata.key);
    if (metadata.size !== value.byteLength) {
      throw new RangeError(
        `metadata.size ${metadata.size} does not match ${value.byteLength} payload bytes`,
      );
    }
    return this.#store(
      Object.freeze({
        ...metadata,
        attributes: Object.freeze({ ...metadata.attributes }),
      }),
      value,
    );
  }

  async delete(key: CacheKey): Promise<boolean> {
    return this.discard(key);
  }

  /**
   * Synchronous `delete`, so a tier coordinator can drop an entry in the same
   * step as a check.
   *
   * @returns whether something was removed.
   */
  discard(key: CacheKey): boolean {
    assertValidKey(key);
    const node = this.#index.get(key);
    if (node === undefined) {
      return false;
    }
    this.#unlink(node);
    return true;
  }

  async deleteIf(
    key: CacheKey,
    predicate: (metadata: EntryMetadata) => boolean,
  ): Promise<boolean> {
    assertValidKey(key);
    const node = this.#index.get(key);
    if (node === undefined || !predicate(node.metadata)) {
      return false;
    }
    this.#unlink(node);
    return true;
  }

  /** Drops every entry. Frequency history is kept: it describes demand, not data. */
  async clear(): Promise<void> {
    this.#index.clear();
    this.#probation.clear();
    this.#protected.clear();
  }

  async *scan(): AsyncIterable<EntryMetadata> {
    // Live iteration over the index: promotions and demotions never re-insert
    // into it, so moving between segments cannot skip or repeat an entry.
    for (const node of this.#index.values()) {
      yield node.metadata;
    }
  }

  // --- policy ---------------------------------------------------------------

  /** Shared by `write` and `offer`. Returns whether the key is resident. */
  #store(metadata: EntryMetadata, value: Uint8Array): boolean {
    const { key } = metadata;
    const charge = chargeOf(key, value.byteLength, metadata.attributes);

    const existing = this.#index.get(key);
    if (existing !== undefined) {
      // Already resident: it has earned its place, so replace it in its
      // segment without an admission check.
      const list = existing.list;
      list.remove(existing);
      if (charge > this.#capacityOf(list)) {
        // The new value can never fit here. Drop the entry (never serve the
        // old value) instead of flushing the segment around it.
        this.#index.delete(key);
        this.#evicted += 1;
        return false;
      }
      existing.metadata = metadata;
      existing.value = copyBytes(value);
      existing.charge = charge;
      list.pushMru(existing);
      this.#rebalance();
      return this.#index.has(key);
    }

    if (!this.#admit(key, charge)) {
      this.#rejected += 1;
      return false;
    }
    const node = new EntryNode(
      key,
      metadata,
      copyBytes(value),
      charge,
      this.#probation,
    );
    this.#index.set(key, node);
    this.#probation.pushMru(node);
    this.#admitted += 1;
    return true;
  }

  /**
   * Makes room in probation for a new entry of `charge` bytes, if the
   * newcomer deserves it. All-or-nothing: victims are picked from the LRU
   * end until enough bytes would be freed, and they are evicted only if the
   * newcomer's frequency is strictly higher than every one of theirs.
   */
  #admit(key: CacheKey, charge: number): boolean {
    if (charge > this.#probationCapacity) {
      return false;
    }
    const needed = this.#probation.bytes + charge - this.#probationCapacity;
    if (needed <= 0) {
      return true;
    }
    const candidateFrequency = this.#sketch.frequency(key);
    const victims: EntryNode[] = [];
    let freed = 0;
    // Terminates: charge <= capacity, so probation.bytes >= needed.
    for (
      let victim = this.#probation.lru;
      victim !== undefined && freed < needed;
      victim = victim.newer
    ) {
      if (this.#sketch.frequency(victim.key) >= candidateFrequency) {
        return false;
      }
      victims.push(victim);
      freed += victim.charge;
    }
    if (freed < needed) {
      return false; // unreachable while the byte totals are consistent
    }
    for (const victim of victims) {
      this.#evict(victim);
    }
    return true;
  }

  /**
   * Restores the capacity invariants. Protected overflow is demoted to the
   * head of probation; probation overflow is evicted from its tail.
   */
  #rebalance(): void {
    for (
      let node = this.#protected.lru;
      node !== undefined && this.#protected.bytes > this.#protectedCapacity;
      node = this.#protected.lru
    ) {
      this.#protected.remove(node);
      if (node.charge > this.#probationCapacity) {
        // Too big for probation: demoting it would flush the segment.
        this.#index.delete(node.key);
        this.#evicted += 1;
      } else {
        this.#probation.pushMru(node);
      }
    }
    for (
      let node = this.#probation.lru;
      node !== undefined && this.#probation.bytes > this.#probationCapacity;
      node = this.#probation.lru
    ) {
      this.#evict(node);
    }
  }

  #capacityOf(list: LruList): number {
    return list === this.#probation
      ? this.#probationCapacity
      : this.#protectedCapacity;
  }

  #evict(node: EntryNode): void {
    this.#unlink(node);
    this.#evicted += 1;
  }

  #unlink(node: EntryNode): void {
    node.list.remove(node);
    this.#index.delete(node.key);
  }
}

// --- internals -----------------------------------------------------------------

class EntryNode {
  /** Neighbour towards the MRU end. */
  newer: EntryNode | undefined;
  /** Neighbour towards the LRU end. */
  older: EntryNode | undefined;

  constructor(
    readonly key: CacheKey,
    public metadata: EntryMetadata,
    public value: Buffer,
    public charge: number,
    /** The segment this node was last pushed into. */
    public list: LruList,
  ) {}
}

/** Intrusive doubly linked list, MRU at the head, with a running byte total. */
class LruList {
  #mru: EntryNode | undefined;
  #lru: EntryNode | undefined;
  #bytes = 0;

  get bytes(): number {
    return this.#bytes;
  }

  get lru(): EntryNode | undefined {
    return this.#lru;
  }

  pushMru(node: EntryNode): void {
    node.list = this;
    node.newer = undefined;
    node.older = this.#mru;
    if (this.#mru === undefined) {
      this.#lru = node;
    } else {
      this.#mru.newer = node;
    }
    this.#mru = node;
    this.#bytes += node.charge;
  }

  remove(node: EntryNode): void {
    if (node.newer === undefined) {
      this.#mru = node.older;
    } else {
      node.newer.older = node.older;
    }
    if (node.older === undefined) {
      this.#lru = node.newer;
    } else {
      node.older.newer = node.newer;
    }
    node.newer = undefined;
    node.older = undefined;
    this.#bytes -= node.charge;
  }

  moveToMru(node: EntryNode): void {
    if (this.#mru !== node) {
      this.remove(node);
      this.pushMru(node);
    }
  }

  clear(): void {
    this.#mru = undefined;
    this.#lru = undefined;
    this.#bytes = 0;
  }
}

/** Estimated resident size of an entry (strings counted at 2 bytes/char). */
function chargeOf(
  key: CacheKey,
  payloadBytes: number,
  attributes: EntryAttributes,
): number {
  let charge = ENTRY_OVERHEAD_BYTES + payloadBytes + 2 * key.length;
  for (const [name, value] of Object.entries(attributes)) {
    charge += 2 * (name.length + value.length);
  }
  return charge;
}

/**
 * Private copy outside Node's shared `Buffer` pool. A pooled slice would pin
 * its whole 8 KiB slab after eviction and throw the byte accounting off.
 */
function copyBytes(value: Uint8Array): Buffer {
  const copy = Buffer.allocUnsafeSlow(value.byteLength);
  copy.set(value);
  return copy;
}
