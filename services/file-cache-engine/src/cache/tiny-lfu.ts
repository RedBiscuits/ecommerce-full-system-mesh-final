/**
 * TinyLFU frequency sketch: an approximate, aging, fixed-memory popularity
 * counter used to decide whether a new entry is worth admitting to a cache.
 *
 * Scan traffic and one-hit wonders never build up frequency, so an admission
 * policy can turn them away instead of letting them evict entries that
 * are actually re-read.
 *
 * Layout: a count-min sketch of depth 4 with 4-bit counters packed into
 * 16-bit blocks (`Uint16Array`). Each block holds four counters, and lane `i`
 * of every block belongs to hash function `i`, so the four counters of a key
 * never share a nibble even when two hashes pick the same block. 16-bit
 * blocks (rather than 64-bit words) keep every operation inside JS's native
 * 32-bit integer arithmetic; 64-bit words would need `BigInt`, which
 * allocates on every operation.
 *
 * Aging: after `sampleSize` increments, every counter is halved so old
 * popularity fades and the sketch follows the current workload. Halving walks
 * the whole table, so by default it is queued to the next event-loop turn
 * instead of running on the caller's hot path.
 */

/** Hash functions per key; also the number of 4-bit lanes per block. */
const DEPTH = 4;
/** Largest value a 4-bit counter can hold; increments saturate here. */
export const MAX_FREQUENCY = 15;
/** Upper bound on `capacity`; keeps the table at or below 128 MiB. */
export const MAX_SKETCH_CAPACITY = 2 ** 24;
/**
 * Keeps the low three bits of every lane. After `>>> 1`, each lane's top bit
 * holds the low bit of the lane above it, and this mask clears it.
 */
const HALVE_MASK = 0x7777;
const GOLDEN_RATIO = 0x9e3779b9;

/** Runs the halving pass. It receives a callback and calls it once, sync or later. */
export type ResetScheduler = (reset: () => void) => void;

export interface FrequencySketchOptions {
  /**
   * Expected number of entries in the cache this sketch guards. Sizes the
   * table: 4 blocks (8 bytes) per entry, rounded up to a power of two.
   */
  readonly capacity: number;
  /**
   * Increments after which every counter is halved. Default: `10 ×
   * capacity`, the sample length recommended by the TinyLFU paper.
   */
  readonly sampleSize?: number;
  /**
   * Hash seed. Default: random per instance, so which keys collide differs
   * between instances and can't be worked out in advance to inflate a key's
   * frequency. (This is not a cryptographic guarantee.) Fix it only for
   * deterministic tests.
   */
  readonly seed?: number;
  /**
   * Decides when the halving pass runs once the sample is full. Default:
   * an unref'd `setImmediate`, so the O(table) pass never lands inside the
   * `increment` call that filled the sample and never keeps the process
   * alive. Pass `(reset) => reset()` to halve inline.
   */
  readonly scheduleReset?: ResetScheduler;
}

export class FrequencySketch {
  readonly #table: Uint16Array;
  readonly #mask: number;
  readonly #sampleSize: number;
  readonly #seed: number;
  readonly #scheduleReset: ResetScheduler;
  /** Increments that changed at least one counter since the last halving. */
  #size = 0;
  /** The queued halving pass, if any. `clear()` cancels it by replacing it. */
  #pendingReset: (() => void) | undefined;

  constructor(options: FrequencySketchOptions) {
    const { capacity } = options;
    if (
      !Number.isInteger(capacity) ||
      capacity < 1 ||
      capacity > MAX_SKETCH_CAPACITY
    ) {
      throw new RangeError(
        `capacity must be an integer in [1, ${MAX_SKETCH_CAPACITY}]`,
      );
    }
    const sampleSize = options.sampleSize ?? 10 * capacity;
    if (!Number.isSafeInteger(sampleSize) || sampleSize < 1) {
      throw new RangeError('sampleSize must be a positive safe integer');
    }
    const blocks = DEPTH * nextPowerOfTwo(capacity);
    this.#table = new Uint16Array(blocks);
    this.#mask = blocks - 1;
    this.#sampleSize = sampleSize;
    this.#seed = (options.seed ?? Math.random() * 2 ** 32) >>> 0;
    this.#scheduleReset = options.scheduleReset ?? deferToNextTurn;
  }

  /**
   * Records one access to `key`. Each of its four counters is incremented
   * unless it is already at {@link MAX_FREQUENCY}. If all four are already
   * saturated, the access does not count towards the sample.
   */
  increment(key: string): void {
    const hash = hashString(key, this.#seed);
    let changed = false;
    for (let lane = 0; lane < DEPTH; lane++) {
      const index = this.#indexOf(hash, lane);
      const block = this.#table[index] ?? 0;
      const next = incrementCounter(block, lane);
      if (next !== block) {
        this.#table[index] = next;
        changed = true;
      }
    }
    // `>=`, not `===`: increments keep arriving while a reset is queued.
    if (changed && ++this.#size >= this.#sampleSize) {
      this.#requestReset();
    }
  }

  /**
   * Estimated access count of `key` in `[0, MAX_FREQUENCY]`. It never
   * underestimates (until aging halves it), but hash collisions can make
   * it overestimate.
   */
  frequency(key: string): number {
    const hash = hashString(key, this.#seed);
    let min = MAX_FREQUENCY;
    for (let lane = 0; lane < DEPTH; lane++) {
      const block = this.#table[this.#indexOf(hash, lane)] ?? 0;
      min = Math.min(min, counterAt(block, lane));
    }
    return min;
  }

  /** Zeroes every counter and cancels any queued halving. */
  clear(): void {
    this.#table.fill(0);
    this.#size = 0;
    this.#pendingReset = undefined;
  }

  /** Increments recorded in the current sample (resets on halving). */
  get size(): number {
    return this.#size;
  }

  get sampleSize(): number {
    return this.#sampleSize;
  }

  /** Whether the sample is full and a halving pass is queued but not yet run. */
  get isResetPending(): boolean {
    return this.#pendingReset !== undefined;
  }

  #indexOf(hash: number, lane: number): number {
    return mix32(hash + Math.imul(lane + 1, GOLDEN_RATIO)) & this.#mask;
  }

  #requestReset(): void {
    if (this.#pendingReset !== undefined) {
      return;
    }
    const run = (): void => {
      // Stale if `clear()` ran (or a newer reset was queued) meanwhile.
      if (this.#pendingReset !== run) {
        return;
      }
      this.#pendingReset = undefined;
      this.#halve();
    };
    this.#pendingReset = run;
    try {
      this.#scheduleReset(run);
    } catch (error) {
      // Otherwise aging would be switched off for good. The next increment
      // will try to queue the reset again.
      if (this.#pendingReset === run) {
        this.#pendingReset = undefined;
      }
      throw error;
    }
  }

  #halve(): void {
    const table = this.#table;
    let odd = 0;
    for (let i = 0; i < table.length; i++) {
      const block = table[i] ?? 0;
      odd += oddCounters(block);
      table[i] = halveCounters(block);
    }
    // An increment adds ~DEPTH to the counter total, so size ≈ total / 4.
    // Halving makes the total (total - odd) / 2, and size follows.
    this.#size = Math.floor(Math.max(0, this.#size - odd / DEPTH) / 2);
    if (this.#size >= this.#sampleSize) {
      // Can only happen if more than a full sample arrived while queued.
      this.#requestReset();
    }
  }
}

// --- block arithmetic (pure; exported for exhaustive tests) ------------------

/** Value of the 4-bit counter in `lane` (0–3) of a 16-bit block. */
export function counterAt(block: number, lane: number): number {
  return (block >>> (lane << 2)) & 0xf;
}

/**
 * `block` with the counter in `lane` incremented, or `block` unchanged if that
 * counter is already at {@link MAX_FREQUENCY}. The guard means the add can
 * never carry into the neighbouring lane or past bit 15.
 */
export function incrementCounter(block: number, lane: number): number {
  const shift = lane << 2;
  return ((block >>> shift) & 0xf) === MAX_FREQUENCY
    ? block
    : block + (1 << shift);
}

/** Halves (floor) all four counters of a block independently. */
export function halveCounters(block: number): number {
  return (block >>> 1) & HALVE_MASK;
}

// --- helpers -----------------------------------------------------------------

/** How many of the block's four counters are odd (lose ½ when halved). */
function oddCounters(block: number): number {
  return (
    (block & 1) +
    ((block >>> 4) & 1) +
    ((block >>> 8) & 1) +
    ((block >>> 12) & 1)
  );
}

/** Seeded FNV-1a over UTF-16 code units, finalised with `mix32`. */
function hashString(key: string, seed: number): number {
  let hash = 0x811c9dc5 ^ seed;
  for (let i = 0; i < key.length; i++) {
    hash ^= key.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193);
  }
  return mix32(hash);
}

/** MurmurHash3 `fmix32`: a bijective avalanche over 32 bits. */
function mix32(value: number): number {
  let x = value | 0;
  x ^= x >>> 16;
  x = Math.imul(x, 0x85ebca6b);
  x ^= x >>> 13;
  x = Math.imul(x, 0xc2b2ae35);
  x ^= x >>> 16;
  return x >>> 0;
}

function nextPowerOfTwo(value: number): number {
  let power = 1;
  while (power < value) {
    power *= 2;
  }
  return power;
}

function deferToNextTurn(reset: () => void): void {
  setImmediate(reset).unref();
}
