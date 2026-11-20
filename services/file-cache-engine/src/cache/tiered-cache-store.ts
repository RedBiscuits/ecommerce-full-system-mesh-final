import type { CircuitSnapshot } from '../resilience/circuit-breaker';
import { CircuitBreaker } from '../resilience/circuit-breaker';
import { CircuitOpenError, isStorageFailure } from '../resilience/errors';
import type { Clock } from '../shared/clock';
import { systemClock } from '../shared/clock';
import { KeyedMutex } from '../shared/keyed-mutex';
import { RingBuffer } from '../shared/ring-buffer';
import type { TelemetryEvent } from '../telemetry/events';
import { describeError } from '../telemetry/events';
import type { TelemetrySink } from '../telemetry/sink';
import { safeSink } from '../telemetry/sink';
import { assertValidKey, checksumOf } from './cache-key';
import type { SlruMemoryStore } from './slru-memory-store';
import type {
  CacheEntry,
  CacheKey,
  CacheStore,
  EntryAttributes,
  EntryLifetime,
  EntryMetadata,
} from './types';

export interface WriteBehindOptions {
  /**
   * Ring buffer capacity: writes that may wait for L2 at once. When it is
   * full, `write` falls back to writing L2 before it resolves. Default: 1024.
   */
  readonly maxPendingWrites?: number;
  /**
   * Payload bytes that pending writes may hold (they are copies, outside
   * L1's budget). Beyond this, `write` falls back to a synchronous L2 write.
   * Default: 64 MiB.
   */
  readonly maxPendingBytes?: number;
  /** Writes flushed to L2 concurrently per batch. Default: 32. */
  readonly batchSize?: number;
  /**
   * How long a queued write waits before its batch flushes (unless a full
   * batch is ready sooner). Rewrites of a key inside this window collapse
   * into one L2 write. Default: 10 ms.
   */
  readonly flushDelayMs?: number;
}

/** Circuit breaker guarding L2 (see {@link TieredCacheStore}). */
export interface TierCircuitOptions {
  /** Consecutive disk failures that open the circuit. Default: `5`. */
  readonly failureThreshold?: number;
  /** The failures must fall within this sliding window. Default: 10 s. */
  readonly windowMs?: number;
  /** Time spent in L1-only mode before a half-open probe. Default: 30 s. */
  readonly cooldownMs?: number;
  /**
   * Random extra delay added to each cooldown, as a share of `cooldownMs`,
   * so instances that tripped together probe at different times (and never
   * early). Default: `0.2`, so probes come 30 to 36 s after opening.
   */
  readonly jitterRatio?: number;
  /** Which errors count as disk failures. Default: `isStorageFailure`. */
  readonly isFailure?: (error: unknown) => boolean;
}

export interface TieredCacheStoreOptions {
  /** Fast, bounded tier. Its TinyLFU admission decides promotions. */
  readonly l1: SlruMemoryStore;
  /**
   * Durable tier and source of truth, typically `ResilientCacheStore`
   * wrapping `FsCacheStore`.
   */
  readonly l2: CacheStore;
  /**
   * Write-behind tuning, or `false` for write-through: each `write`
   * resolves only after L2 has it. The buffer exists in both modes: in
   * write-through mode it only holds writes accepted while the circuit is
   * open. Default: enabled.
   */
  readonly writeBehind?: WriteBehindOptions | false;
  /**
   * Circuit breaker for L2, or `false` to disable it. While open, the store
   * runs L1-only: reads never touch L2, writes are buffered in memory, and
   * a jittered half-open probe retries L2 after the cooldown.
   * Default: enabled.
   */
  readonly circuit?: TierCircuitOptions | false;
  /**
   * A buffered write could not be persisted: a non-disk error, or the final
   * drain of `close()` still failed. Its caller was already answered, so
   * this (plus `tier.flush` events and `flush`/`close` rejecting) is where
   * the loss surfaces. Applies in both modes, since write-through mode
   * buffers too while the circuit is open. Default: `console.error`.
   */
  readonly onFlushError?: (error: unknown, key: CacheKey) => void;
  /** Receives `tier.*` events (reads, writes, flushes, circuit changes). */
  readonly telemetry?: TelemetrySink;
  readonly clock?: Clock;
  /** Random source for the probe jitter (injectable for tests). */
  readonly random?: () => number;
}

export interface TieredCacheStoreStats {
  /** Reads answered by L1. */
  readonly l1Hits: number;
  /** Reads answered by a write still waiting to reach L2. */
  readonly pendingHits: number;
  /** Reads answered by L2, coalesced readers included. */
  readonly l2Hits: number;
  /** Reads found in no tier. */
  readonly misses: number;
  /** Actual `l2.read` calls. Coalescing keeps this below L1 misses. */
  readonly l2Reads: number;
  /** Readers that joined an L2 read already in flight. */
  readonly coalesced: number;
  /** L2 hits that L1 admitted. */
  readonly promoted: number;
  /** L2 hits that L1 declined (admission) or that were invalidated in flight. */
  readonly notPromoted: number;
  /** L2 reads currently in flight. */
  readonly inFlight: number;
  /** Keys with a write not yet confirmed by L2. */
  readonly pendingWrites: number;
  /** Payload bytes held by those writes. */
  readonly pendingBytes: number;
  /** Capacity of the write-behind ring buffer. */
  readonly maxPendingWrites: number;
  /** Writes merged into a queued write of the same key (saved L2 writes). */
  readonly coalescedWrites: number;
  /** Writes that fell back to a synchronous L2 write (backpressure). */
  readonly syncFallbacks: number;
  /** Queued writes that reached L2. */
  readonly flushed: number;
  /** Writes (queued or fallback) that L2 rejected. */
  readonly flushFailures: number;
  /** Flush batches run. */
  readonly batches: number;
  /** Buffered writes put back for a retry (disk failed, or circuit open). */
  readonly requeued: number;
  /** Reads answered without L2 because its circuit was not closed. */
  readonly shortCircuited: number;
  /** Writes kept in memory only (buffer full while the circuit was open). */
  readonly l1OnlyWrites: number;
  /** The L2 circuit, or `undefined` when disabled. */
  readonly circuit: CircuitSnapshot | undefined;
}

const DEFAULT_MAX_PENDING_WRITES = 1_024;
const DEFAULT_MAX_PENDING_BYTES = 64 * 1024 * 1024;
const DEFAULT_BATCH_SIZE = 32;
const DEFAULT_FLUSH_DELAY_MS = 10;
const TIER_CIRCUIT = 'tier-l2';
/**
 * Key read by the half-open probe when no buffered write can serve as the
 * probe. It never exists. Reading it just proves the disk answers.
 */
const PROBE_KEY = '\u0000tier-l2-probe';

interface FlightResult {
  readonly entry: CacheEntry | undefined;
  /** L2 was skipped: its circuit opened before the read started. */
  readonly shortCircuited: boolean;
}

/** One shared `l2.read` for a key. */
interface Flight {
  readonly promise: Promise<FlightResult>;
  /**
   * Set when a mutation of the key completes while this read is in flight.
   * What it read may be outdated, so it must not be promoted.
   */
  invalidated: boolean;
  /** `#epoch` at start. `clear()` bumps the epoch to void every flight. */
  readonly epoch: number;
}

/** A write accepted by the store but not yet confirmed by L2. */
interface PendingWrite {
  readonly key: CacheKey;
  /** Ring order. `0` for fallback writes, which bypass the ring. */
  readonly seq: number;
  metadata: EntryMetadata;
  value: Buffer;
  /**
   * `queued`: in the ring, and may still absorb newer writes (coalescing).
   * `flushing`: taken by a flush. Its value is fixed from here on.
   * `cancelled`: superseded, deleted or cleared. It must not be written.
   */
  state: 'queued' | 'flushing' | 'cancelled';
  /** Its bytes are released and it is out of the pending index. */
  settled: boolean;
}

interface FlushWaiter {
  /** Failures of writes with `seq <= target` are reported to this waiter. */
  readonly target: number;
  readonly failures: Array<{ key: CacheKey; error: unknown }>;
}

/**
 * Two-tier {@link CacheStore}: a byte-bounded SLRU memory cache (L1) in
 * front of a durable store (L2), with write-behind and a circuit breaker.
 *
 * **Reads** check L1, then writes still waiting for L2, then L2. Concurrent
 * L1 misses for one key share a single `l2.read` (request coalescing). An L2
 * hit is offered to L1, and L1's TinyLFU admission decides whether to
 * promote it. The access is counted once per `read`, by L1's lookup (which
 * counts misses too).
 *
 * **Writes (write-behind)** go to L1 at once and into a bounded ring buffer.
 * `write` resolves without waiting for disk. Batches are flushed to L2 in
 * the background. A pending write is readable from the moment `write`
 * resolves until L2 confirms it, so a read never goes back to an older
 * value, even if L1 declined or evicted the new one. Rapid rewrites of a
 * queued key collapse into one L2 write (coalescing). When the ring or the
 * byte budget is full, `write` writes L2 before resolving (backpressure),
 * and reports failures to its caller. `close()` drains every pending write.
 *
 * **Circuit breaker (L1-only mode).** `failureThreshold` consecutive disk
 * failures (EIO, ENOSPC, ...; see `isStorageFailure`) within `windowMs`
 * open the circuit. While it is open:
 * - reads are served from L1 and the write buffer only (an L2 miss becomes
 *   a plain miss);
 * - writes are accepted into L1 and the buffer, in both modes. They are
 *   replayed to L2 once it recovers. If the buffer is full, the write is
 *   kept in memory only (`l1-only`);
 * - `delete`, `deleteIf` and `clear` reject with `CircuitOpenError` and
 *   change nothing: they cannot be made durable;
 * - buffered writes whose flush failed are kept and retried, not dropped.
 *
 * After `cooldownMs` plus jitter, a `tier.circuit` event announces the
 * half-open state, and one probe (the oldest buffered write, or a metadata
 * read) tries L2. Success closes the circuit and the buffer drains. Failure
 * re-opens it with a fresh jittered cooldown. Only writes, deletes, clears
 * and probes report success to the breaker. Reads can trip it but not
 * reset it, so a disk that still reads but cannot write (ENOSPC) does open
 * the circuit.
 *
 * **Ordering.** Every L2 mutation of a key (flush, fallback write, delete,
 * deleteIf) runs under a per-key FIFO lock. A flush takes its place in that
 * queue as soon as it is taken off the ring. So L2 applies a key's
 * mutations in the order they were issued, and an older value can never
 * land over a newer one or over a delete. An L2 read that was in flight
 * when a mutation of its key happened is never promoted, and later readers
 * never join it.
 *
 * **Durability trade-off.** With write-behind (or while the circuit is
 * open), a resolved `write` means *accepted*, not *on disk*. A write that
 * L2 rejects for a non-disk reason, or that still fails in the final drain
 * of `close()`, is reported (`onFlushError`, `tier.flush` `failed`), and
 * the store rolls back to what L2 holds. Always `await close()` before
 * shutting down, and before closing the L2 store it drains into.
 */
export class TieredCacheStore implements CacheStore {
  readonly #l1: SlruMemoryStore;
  readonly #l2: CacheStore;
  readonly #flights = new Map<CacheKey, Flight>();
  readonly #mutex = new KeyedMutex();
  readonly #clock: Clock;
  readonly #telemetry: TelemetrySink;
  readonly #breaker: CircuitBreaker | undefined;
  readonly #isDiskFailure: (error: unknown) => boolean;
  /** Bumped by `clear()`. Work that started in an older epoch is void. */
  #epoch = 0;

  /** `false`: write-through while the circuit is closed. */
  readonly #writeBehind: boolean;
  readonly #ring: RingBuffer<PendingWrite>;
  /** Buffered writes whose flush failed, retried before the ring (FIFO). */
  #retry: PendingWrite[] = [];
  /** Newest unconfirmed write per key: what reads see before L2. */
  readonly #pending = new Map<CacheKey, PendingWrite>();
  readonly #maxPendingBytes: number;
  readonly #batchSize: number;
  readonly #flushDelayMs: number;
  readonly #onFlushError: (error: unknown, key: CacheKey) => void;
  readonly #waiters = new Set<FlushWaiter>();
  /** Every write not yet settled (queued, flushing, or a fallback). */
  readonly #live = new Set<PendingWrite>();
  /** Fallback writes in progress: awaited by their callers and by `flush`. */
  readonly #syncWrites = new Set<Promise<void>>();
  #pendingBytes = 0;
  #nextSeq = 1;
  #timer: NodeJS.Timeout | undefined;
  #loop: Promise<void> | undefined;
  #clearing: Promise<void> | undefined;
  #closing: Promise<void> | undefined;
  /** `close()` is draining: final attempts bypass the breaker. */
  #draining = false;

  #l1Hits = 0;
  #pendingHits = 0;
  #l2Hits = 0;
  #misses = 0;
  #l2Reads = 0;
  #coalesced = 0;
  #promoted = 0;
  #notPromoted = 0;
  #coalescedWrites = 0;
  #syncFallbacks = 0;
  #flushed = 0;
  #flushFailures = 0;
  #batches = 0;
  #requeued = 0;
  #shortCircuited = 0;
  #l1OnlyWrites = 0;

  constructor(options: TieredCacheStoreOptions) {
    if ((options.l1 as unknown) === options.l2) {
      throw new TypeError('l1 and l2 must be different stores');
    }
    this.#l1 = options.l1;
    this.#l2 = options.l2;
    this.#clock = options.clock ?? systemClock;
    this.#telemetry = safeSink(options.telemetry);
    this.#writeBehind = options.writeBehind !== false;
    const wb = options.writeBehind === false ? undefined : options.writeBehind;
    this.#ring = new RingBuffer(
      positiveInteger(
        wb?.maxPendingWrites ?? DEFAULT_MAX_PENDING_WRITES,
        'maxPendingWrites',
      ),
    );
    this.#maxPendingBytes = positiveInteger(
      wb?.maxPendingBytes ?? DEFAULT_MAX_PENDING_BYTES,
      'maxPendingBytes',
    );
    this.#batchSize = positiveInteger(
      wb?.batchSize ?? DEFAULT_BATCH_SIZE,
      'batchSize',
    );
    this.#flushDelayMs = wb?.flushDelayMs ?? DEFAULT_FLUSH_DELAY_MS;
    if (!Number.isFinite(this.#flushDelayMs) || this.#flushDelayMs < 0) {
      throw new RangeError('flushDelayMs must be a non-negative finite number');
    }
    this.#onFlushError =
      options.onFlushError ??
      ((error, key): void => {
        console.error(`[cache] write-behind flush failed for "${key}":`, error);
      });

    const circuit = options.circuit === false ? undefined : options.circuit;
    this.#isDiskFailure = circuit?.isFailure ?? isStorageFailure;
    this.#breaker =
      options.circuit === false
        ? undefined
        : new CircuitBreaker({
            name: TIER_CIRCUIT,
            failureThreshold: circuit?.failureThreshold ?? 5,
            windowMs: circuit?.windowMs ?? 10_000,
            cooldownMs: circuit?.cooldownMs ?? 30_000,
            jitterRatio: circuit?.jitterRatio ?? 0.2,
            autoHalfOpen: true,
            isFailure: this.#isDiskFailure,
            clock: this.#clock,
            random: options.random,
            onStateChange: (state, error): void => {
              this.#onCircuitChange(state, error);
            },
          });
  }

  get l1(): SlruMemoryStore {
    return this.#l1;
  }

  get l2(): CacheStore {
    return this.#l2;
  }

  /** `close()` was called. Writes are now synchronous (write-through). */
  get isClosed(): boolean {
    return this.#closing !== undefined;
  }

  stats(): TieredCacheStoreStats {
    return {
      l1Hits: this.#l1Hits,
      pendingHits: this.#pendingHits,
      l2Hits: this.#l2Hits,
      misses: this.#misses,
      l2Reads: this.#l2Reads,
      coalesced: this.#coalesced,
      promoted: this.#promoted,
      notPromoted: this.#notPromoted,
      inFlight: this.#flights.size,
      pendingWrites: this.#pending.size,
      pendingBytes: this.#pendingBytes,
      maxPendingWrites: this.#ring.capacity,
      coalescedWrites: this.#coalescedWrites,
      syncFallbacks: this.#syncFallbacks,
      flushed: this.#flushed,
      flushFailures: this.#flushFailures,
      batches: this.#batches,
      requeued: this.#requeued,
      shortCircuited: this.#shortCircuited,
      l1OnlyWrites: this.#l1OnlyWrites,
      circuit: this.#breaker?.snapshot(),
    };
  }

  /** Write-behind buffer capacity (for saturation alerts and gauges). */
  get maxPendingWrites(): number {
    return this.#ring.capacity;
  }

  // --- reads ------------------------------------------------------------------

  async read(key: CacheKey): Promise<CacheEntry | undefined> {
    assertValidKey(key);
    // L1 first: it also records the access in the sketch, hit or miss.
    // L1 never holds an older value than a pending write (writes update it
    // at once; promotions are blocked while a write is pending).
    const hit = await this.#l1.read(key);
    if (hit !== undefined) {
      this.#l1Hits += 1;
      this.#emitRead(key, 'l1', false, false);
      return hit;
    }
    const pending = this.#pending.get(key);
    if (pending !== undefined) {
      this.#pendingHits += 1;
      this.#emitRead(key, 'write-buffer', false, false);
      return copyEntry(pending);
    }
    if (!this.#diskAvailable()) {
      return this.#shortCircuitMiss(key, false);
    }

    let flight = this.#flights.get(key);
    const coalesced = flight !== undefined;
    if (flight === undefined) {
      flight = this.#startFlight(key);
    } else {
      this.#coalesced += 1;
    }
    const { entry, shortCircuited } = await flight.promise;
    if (shortCircuited) {
      return this.#shortCircuitMiss(key, coalesced);
    }
    if (entry === undefined) {
      this.#misses += 1;
      this.#emitRead(key, 'miss', coalesced, false);
      return undefined;
    }
    this.#l2Hits += 1;
    this.#emitRead(key, 'l2', coalesced, false);
    // Coalesced readers share one L2 result: each gets its own copy.
    return copyEntry(entry);
  }

  async readMetadata(key: CacheKey): Promise<EntryMetadata | undefined> {
    assertValidKey(key);
    const local =
      this.#pending.get(key)?.metadata ?? (await this.#l1.readMetadata(key));
    if (local !== undefined || !this.#diskAvailable()) {
      return local;
    }
    try {
      return await this.#guardRead(() => this.#l2.readMetadata(key));
    } catch (error) {
      if (isTierCircuitOpen(error)) {
        return undefined;
      }
      throw error;
    }
  }

  // --- writes -----------------------------------------------------------------

  async write(
    key: CacheKey,
    value: Uint8Array,
    lifetime: EntryLifetime,
    attributes: EntryAttributes = {},
  ): Promise<EntryMetadata> {
    assertValidKey(key);
    const available = this.#diskAvailable();
    if (!this.#writeBehind && available && !this.isClosed) {
      return this.#writeThrough(key, value, lifetime, attributes);
    }
    const metadata: EntryMetadata = Object.freeze({
      key,
      createdAt: lifetime.createdAt,
      expiresAt: lifetime.expiresAt,
      staleUntil: lifetime.staleUntil,
      size: value.byteLength,
      checksum: checksumOf(value),
      attributes: Object.freeze({ ...attributes }),
    });

    if (!this.isClosed) {
      const newest = this.#pending.get(key);
      if (newest?.state === 'queued') {
        // Coalesce: the queued write has not started, so it simply carries
        // the newer value when it flushes.
        const delta = value.byteLength - newest.value.byteLength;
        if (this.#pendingBytes + delta <= this.#maxPendingBytes) {
          newest.metadata = metadata;
          newest.value = Buffer.from(value);
          this.#pendingBytes += delta;
          this.#coalescedWrites += 1;
          this.#publish(metadata, value);
          this.#emitWrite(key, 'coalesced', value.byteLength);
          return metadata;
        }
      } else if (
        !this.#bufferFull() &&
        this.#pendingBytes + value.byteLength <= this.#maxPendingBytes
      ) {
        const record = this.#track(key, metadata, value, this.#nextSeq++);
        this.#ring.push(record);
        this.#publish(metadata, value);
        this.#emitWrite(key, 'buffered', value.byteLength);
        this.#schedule();
        return metadata;
      }
      if (!available) {
        // Buffer full and L2 unreachable: keep it in memory only. A queued
        // older value of the key is superseded, so it must not be written.
        const older = this.#pending.get(key);
        if (older?.state === 'queued') {
          this.#cancel(older);
        }
        this.#publish(metadata, value);
        this.#l1OnlyWrites += 1;
        this.#emitWrite(key, 'l1-only', value.byteLength);
        return metadata;
      }
      this.#syncFallbacks += 1;
    }

    // Backpressure (or closing): this write goes to L2 before resolving.
    // It is pending like any other (visible to reads at once) and ordered
    // behind earlier writes of the key by the lock.
    const record = this.#track(key, metadata, value, 0);
    record.state = 'flushing';
    this.#publish(metadata, value);
    this.#emitWrite(key, 'fallback', value.byteLength);
    const writing = this.#flushNow(record);
    const tracked = writing.then(
      () => undefined,
      () => undefined,
    );
    this.#syncWrites.add(tracked);
    void tracked.finally(() => this.#syncWrites.delete(tracked));
    await writing;
    return metadata;
  }

  /**
   * Waits until every write accepted before this call has reached L2 (or
   * failed). Rejects with an `AggregateError` if any of them failed, and
   * with `CircuitOpenError` if the circuit is open while some are still
   * buffered. They stay buffered and are retried after recovery. Writes that
   * arrive meanwhile do not extend the wait.
   */
  async flush(): Promise<void> {
    const waiter: FlushWaiter = { target: this.#nextSeq - 1, failures: [] };
    this.#waiters.add(waiter);
    try {
      while (this.#hasLiveUpTo(waiter.target)) {
        if (this.#clearing !== undefined) {
          await this.#clearing.catch(() => undefined);
          continue;
        }
        if (!this.#draining && this.#breaker?.state === 'open') {
          throw new CircuitOpenError(TIER_CIRCUIT, this.#breaker.retryAfterMs);
        }
        await this.#startLoop();
      }
      // Fallback writes are awaited by their own callers, but `close()`
      // must not return while one is still writing.
      await Promise.allSettled(this.#syncWrites);
    } finally {
      this.#waiters.delete(waiter);
    }
    if (waiter.failures.length > 0) {
      throw new AggregateError(
        waiter.failures.map((f) => f.error),
        `${waiter.failures.length} write(s) failed to reach L2: ${waiter.failures
          .map((f) => JSON.stringify(f.key))
          .join(', ')}`,
      );
    }
  }

  /**
   * Strict drain for shutdown. From now on, writes skip the buffer and
   * resolve only once L2 has them. Then every buffered write gets a final
   * attempt, even if the circuit is open. Rejects (like `flush`) if any of
   * them could not be persisted, so a lost write is never silent.
   * Idempotent. It does not close L2: do that after this resolves.
   */
  close(): Promise<void> {
    this.#closing ??= (async (): Promise<void> => {
      this.#draining = true;
      try {
        await this.flush();
      } finally {
        this.#breaker?.dispose();
        clearTimeout(this.#timer);
        this.#timer = undefined;
      }
    })();
    return this.#closing;
  }

  // --- deletes ----------------------------------------------------------------

  async delete(key: CacheKey): Promise<boolean> {
    assertValidKey(key);
    this.#assertDiskAvailable();
    // Synchronously: stop serving it from any tier, and make sure no queued
    // write of it reaches L2. The lock below orders the L2 delete after any
    // flush of this key that has already started.
    const hadPending = this.#cancelPending(key);
    this.#invalidateFlight(key);
    const fromL1 = this.#l1.discard(key);
    return this.#mutex.runExclusive(key, async () => {
      try {
        return (
          (await this.#guard(() => this.#l2.delete(key))) ||
          fromL1 ||
          hadPending
        );
      } finally {
        // An L2 read started meanwhile may have promoted the old value.
        this.#invalidateFlight(key);
        if (!this.#pending.has(key)) {
          this.#l1.discard(key);
        }
      }
    });
  }

  async deleteIf(
    key: CacheKey,
    predicate: (metadata: EntryMetadata) => boolean,
  ): Promise<boolean> {
    assertValidKey(key);
    this.#assertDiskAvailable();
    return this.#mutex.runExclusive(key, () =>
      this.#deleteIfLocked(key, predicate, this.#l2, (task) =>
        this.#guard(task),
      ),
    );
  }

  /**
   * Empties every tier and cancels every pending write. A flush batch that
   * is already writing finishes before L2 is cleared, so it cannot land
   * afterwards. Writes that arrive while `clear()` runs are kept: they stay
   * pending and are flushed once it completes. Rejects, changing nothing,
   * while the circuit is open.
   */
  async clear(): Promise<void> {
    this.#assertDiskAvailable();
    const previous = this.#clearing;
    const run = (async (): Promise<void> => {
      await previous?.catch(() => undefined);
      this.#voidEverything();
      this.#cancelAllPending();
      clearTimeout(this.#timer);
      this.#timer = undefined;
      await this.#l1.clear();
      await this.#loop; // stops at the next batch boundary (#clearing is set)
      try {
        await this.#guard(() => this.#l2.clear());
      } finally {
        // Reads that overlapped the L2 clear may have promoted pre-clear
        // values. Writes issued during clear stay pending, so they remain
        // readable.
        this.#voidEverything();
        await this.#l1.clear();
      }
    })();
    const tracked = run.finally(() => {
      if (this.#clearing === tracked) {
        this.#clearing = undefined;
        this.#schedule();
      }
    });
    this.#clearing = tracked;
    return tracked;
  }

  /**
   * Every L2 entry (replaced by a pending write's metadata where one exists),
   * then entries only in L1 or only pending. Memory is bounded by L1 plus
   * the pending buffer.
   */
  scan(): AsyncIterable<EntryMetadata> {
    return this.#scan(this.#l2);
  }

  /**
   * A view of this store for an `ExpirySweeper`. Its scan sees every tier,
   * and its deletions also drop the entry from L1 and cancel pending
   * writes, so memory never keeps what the sweeper removed. It differs from
   * the store itself:
   *
   * - Disk operations go to `disk`, normally the raw `FsCacheStore` under the
   *   resilient decorator, and bypass the circuit. Reclaiming space must
   *   stay possible while L2 is failing (ENOSPC is one way it opens).
   * - A key that is being mutated right now is skipped, not waited for.
   *   An emergency sweep runs *inside* a write that hit a full disk, and
   *   that write holds its key's lock. Waiting would deadlock. Skipping is
   *   safe: the write in progress supersedes the entry.
   *
   * Other methods delegate to the store.
   */
  sweeperView(disk: CacheStore = this.#l2): CacheStore {
    return {
      read: (key) => this.read(key),
      readMetadata: (key) => this.readMetadata(key),
      write: (key, value, lifetime, attributes) =>
        this.write(key, value, lifetime, attributes),
      delete: (key) => this.delete(key),
      clear: () => this.clear(),
      scan: () => this.#scan(disk),
      deleteIf: async (key, predicate) => {
        assertValidKey(key);
        if (this.#mutex.isLocked(key)) {
          return false; // busy: a later sweep will see its new state
        }
        return this.#mutex.runExclusive(key, () =>
          this.#deleteIfLocked(key, predicate, disk, (task) => task()),
        );
      },
    };
  }

  async *#scan(disk: CacheStore): AsyncIterable<EntryMetadata> {
    const notOnDisk = new Set<CacheKey>(this.#pending.keys());
    for await (const metadata of this.#l1.scan()) {
      notOnDisk.add(metadata.key);
    }
    for await (const metadata of disk.scan()) {
      notOnDisk.delete(metadata.key);
      yield this.#pending.get(metadata.key)?.metadata ?? metadata;
    }
    for (const key of notOnDisk) {
      const metadata =
        this.#pending.get(key)?.metadata ?? (await this.#l1.readMetadata(key));
      if (metadata !== undefined) {
        yield metadata;
      }
    }
  }

  /** `deleteIf` body. The caller holds `key`'s lock. */
  async #deleteIfLocked(
    key: CacheKey,
    predicate: (metadata: EntryMetadata) => boolean,
    disk: CacheStore,
    call: <T>(task: () => Promise<T>) => Promise<T>,
  ): Promise<boolean> {
    // A pending write is the current state of the key: judge it, not the
    // disk's older copy. (No flush of this key runs while we hold the lock.)
    const pending = this.#pending.get(key);
    if (pending !== undefined) {
      if (!predicate(pending.metadata)) {
        return false;
      }
      this.#cancel(pending);
      this.#invalidateFlight(key);
      this.#l1.discard(key);
      await call(() => disk.delete(key));
      return true;
    }
    // Otherwise the disk is the source of truth. If the entry goes there, it
    // goes from L1.
    if (await call(() => disk.deleteIf(key, predicate))) {
      this.#invalidateFlight(key);
      this.#l1.discard(key);
      return true;
    }
    // The disk had nothing matching, but L1 may still hold a copy that the
    // disk lost behind our back.
    const removed = await this.#l1.deleteIf(key, predicate);
    if (removed) {
      this.#invalidateFlight(key);
    }
    return removed;
  }

  // --- circuit ----------------------------------------------------------------

  /** L2 may be used by ordinary operations (the circuit is closed). */
  #diskAvailable(): boolean {
    return this.#breaker === undefined || this.#breaker.state === 'closed';
  }

  #assertDiskAvailable(): void {
    if (!this.#diskAvailable()) {
      throw new CircuitOpenError(
        TIER_CIRCUIT,
        this.#breaker?.retryAfterMs ?? 0,
      );
    }
  }

  /** Runs an L2 mutation through the breaker: its outcome counts. */
  #guard<T>(task: () => Promise<T>): Promise<T> {
    return this.#breaker === undefined ? task() : this.#breaker.execute(task);
  }

  /**
   * Runs an L2 read: refused unless the circuit is closed, and a disk
   * failure counts towards opening it. A success does *not* reset the
   * failure streak: a disk that reads fine but cannot write must still trip.
   */
  async #guardRead<T>(task: () => Promise<T>): Promise<T> {
    const breaker = this.#breaker;
    if (breaker === undefined) {
      return task();
    }
    if (breaker.state !== 'closed') {
      throw new CircuitOpenError(TIER_CIRCUIT, breaker.retryAfterMs);
    }
    try {
      return await task();
    } catch (error) {
      breaker.recordFailure(error);
      throw error;
    }
  }

  #onCircuitChange(state: CircuitSnapshot['state'], error: unknown): void {
    const snapshot = this.#breaker?.snapshot();
    this.#emit({
      type: 'tier.circuit',
      at: this.#clock.now(),
      state,
      retryAt: state === 'open' ? snapshot?.retryAt : undefined,
      consecutiveFailures: snapshot?.consecutiveFailures ?? 0,
      error: error === undefined ? undefined : describeError(error),
    });
    if (state !== 'open' && !this.#draining) {
      // Half-open: run the recovery probe. Closed: drain what was buffered.
      // Deferred, so we never re-enter the code that caused the transition.
      setImmediate(() => {
        if (state === 'half-open') {
          void this.#startLoop();
        } else {
          this.#schedule();
        }
      }).unref();
    }
  }

  /** Half-open with nothing buffered: a cheap read decides recovery. */
  async #probeDisk(): Promise<void> {
    try {
      await this.#guard(() => this.#l2.readMetadata(PROBE_KEY));
    } catch {
      // The breaker has recorded it and re-opened.
    }
  }

  #shortCircuitMiss(key: CacheKey, coalesced: boolean): undefined {
    this.#shortCircuited += 1;
    this.#misses += 1;
    this.#emitRead(key, 'miss', coalesced, true);
    return undefined;
  }

  // --- write-behind internals ---------------------------------------------------

  /** Registers a pending write as the newest for its key. */
  #track(
    key: CacheKey,
    metadata: EntryMetadata,
    value: Uint8Array,
    seq: number,
  ): PendingWrite {
    const previous = this.#pending.get(key);
    if (previous?.state === 'queued') {
      this.#cancel(previous); // superseded before it started
    }
    const record: PendingWrite = {
      key,
      seq,
      metadata,
      value: Buffer.from(value), // the caller may reuse its buffer
      state: 'queued',
      settled: false,
    };
    this.#pending.set(key, record);
    this.#live.add(record);
    this.#pendingBytes += value.byteLength;
    return record;
  }

  /** Makes a newly accepted value visible: in L1 (if admitted), never stale. */
  #publish(metadata: EntryMetadata, value: Uint8Array): void {
    this.#invalidateFlight(metadata.key);
    this.#l1.offer(metadata, value);
  }

  /** Starts a flush soon: after `flushDelayMs`, or now if a batch is full. */
  #schedule(): void {
    const queued = this.#ring.size + this.#retry.length;
    if (
      queued === 0 ||
      this.#loop !== undefined ||
      this.#clearing !== undefined ||
      this.#breaker?.state === 'open' // the half-open timer restarts us
    ) {
      return;
    }
    if (queued >= this.#batchSize) {
      void this.#startLoop();
      return;
    }
    // Not unref'd: pending writes are real work, and exiting before the
    // flush would lose them.
    this.#timer ??= setTimeout(() => {
      this.#timer = undefined;
      void this.#startLoop();
    }, this.#flushDelayMs);
  }

  #startLoop(): Promise<void> {
    clearTimeout(this.#timer);
    this.#timer = undefined;
    this.#loop ??= this.#runLoop().finally(() => {
      this.#loop = undefined;
      this.#schedule();
    });
    return this.#loop;
  }

  /**
   * Flushes batch after batch until nothing is queued, a clear starts, or
   * the circuit opens. Half-open: one write (or a metadata read, if nothing
   * is buffered) is the probe. While draining, the circuit is ignored.
   */
  async #runLoop(): Promise<void> {
    for (;;) {
      if (this.#clearing !== undefined) {
        return;
      }
      let limit = this.#batchSize;
      let probe = false;
      if (!this.#draining && this.#breaker !== undefined) {
        const state = this.#breaker.state;
        if (state === 'open') {
          return;
        }
        if (state === 'half-open') {
          limit = 1;
          probe = true;
        }
      }
      const batch = this.#takeBatch(limit);
      if (batch.length === 0) {
        if (probe) {
          await this.#probeDisk();
        }
        return;
      }
      // Each flush requests its key's lock synchronously, right here, so
      // lock order matches queue order.
      await Promise.all(
        batch.map((record) => this.#flushQueued(record, probe)),
      );
      this.#batches += 1;
    }
  }

  /** Up to `limit` queued writes: retries first, then the ring, in order. */
  #takeBatch(limit: number): PendingWrite[] {
    const batch: PendingWrite[] = [];
    while (batch.length < limit) {
      const record = this.#retry.shift() ?? this.#ring.shift();
      if (record === undefined) {
        break;
      }
      if (record.state === 'queued') {
        record.state = 'flushing'; // no more coalescing into it
        batch.push(record);
      }
    }
    return batch;
  }

  /**
   * A buffered write's flush. On a disk failure (or a refusal by the
   * circuit) it is requeued rather than dropped, unless `close()` is
   * draining (that attempt is final) or a newer write of the key already
   * supersedes it.
   */
  #flushQueued(record: PendingWrite, probe: boolean): Promise<void> {
    return this.#mutex.runExclusive(record.key, async () => {
      // Cancelled while waiting for the lock (e.g. by a deleteIf that held
      // it). Writing now would put deleted data back on disk.
      if (record.state === 'cancelled') {
        return;
      }
      const started = performance.now();
      try {
        await (this.#draining
          ? this.#writeRecord(record)
          : this.#guard(() => this.#writeRecord(record)));
        this.#flushed += 1;
        this.#settle(record);
        this.#emitFlush(record, 'ok', started, probe, undefined);
      } catch (error) {
        const retryable =
          !this.#draining &&
          this.#breaker !== undefined &&
          (isTierCircuitOpen(error) || this.#isDiskFailure(error));
        if (retryable && this.#pending.get(record.key) === record) {
          record.state = 'queued';
          this.#retry.push(record);
          this.#requeued += 1;
          this.#emitFlush(record, 'requeued', started, probe, error);
          return;
        }
        if (retryable) {
          this.#cancel(record); // superseded by a newer write: drop it
          return;
        }
        this.#failRecord(record);
        this.#emitFlush(record, 'failed', started, probe, error);
        this.#reportFailure(record, error);
      }
    });
  }

  /** A fallback write: flushed by its caller, and failures go to the caller. */
  #flushNow(record: PendingWrite): Promise<void> {
    return this.#mutex.runExclusive(record.key, async () => {
      if (record.state === 'cancelled') {
        return;
      }
      const started = performance.now();
      try {
        await this.#guard(() => this.#writeRecord(record));
        this.#settle(record);
        this.#emitFlush(record, 'ok', started, false, undefined);
      } catch (error) {
        this.#failRecord(record);
        this.#emitFlush(record, 'failed', started, false, error);
        throw error;
      }
    });
  }

  #writeRecord(record: PendingWrite): Promise<EntryMetadata> {
    const { createdAt, expiresAt, staleUntil } = record.metadata;
    return this.#l2.write(
      record.key,
      record.value,
      { createdAt, expiresAt, staleUntil },
      record.metadata.attributes,
    );
  }

  /** Gives up on a write: roll back to what L2 holds, unless superseded. */
  #failRecord(record: PendingWrite): void {
    this.#flushFailures += 1;
    if (this.#pending.get(record.key) === record) {
      this.#l1.discard(record.key);
      this.#invalidateFlight(record.key);
    }
    this.#settle(record);
  }

  #reportFailure(record: PendingWrite, error: unknown): void {
    for (const waiter of this.#waiters) {
      if (record.seq <= waiter.target) {
        waiter.failures.push({ key: record.key, error });
      }
    }
    try {
      this.#onFlushError(error, record.key);
    } catch {
      // A faulty reporter must not stall the flush loop.
    }
  }

  /**
   * Queued writes include requeued ones (the retry queue). They count
   * against `maxPendingWrites` too, or the buffer could outgrow its bound
   * exactly when the disk is failing.
   */
  #bufferFull(): boolean {
    return this.#ring.size + this.#retry.length >= this.#ring.capacity;
  }

  #hasLiveUpTo(target: number): boolean {
    for (const record of this.#live) {
      if (record.seq > 0 && record.seq <= target) {
        return true;
      }
    }
    return false;
  }

  #cancel(record: PendingWrite): void {
    record.state = 'cancelled';
    this.#settle(record);
  }

  /** Cancels the pending write of `key`, if any. Returns whether there was one. */
  #cancelPending(key: CacheKey): boolean {
    const record = this.#pending.get(key);
    if (record === undefined) {
      return false;
    }
    this.#cancel(record);
    return true;
  }

  /**
   * Cancels every unsettled write, including ones in a batch that is
   * waiting for its lock. One already writing cannot be stopped:
   * `clear()` waits for it before clearing L2.
   */
  #cancelAllPending(): void {
    // Deleting the current entry while iterating a Set is well defined.
    for (const record of this.#live) {
      this.#cancel(record);
    }
    this.#ring.clear();
    this.#retry = [];
  }

  /** Releases a write's bytes and index slot. Idempotent. */
  #settle(record: PendingWrite): void {
    if (record.settled) {
      return;
    }
    record.settled = true;
    this.#live.delete(record);
    if (this.#pending.get(record.key) === record) {
      this.#pending.delete(record.key);
    }
    this.#pendingBytes -= record.value.byteLength;
  }

  // --- write-through and read internals ---------------------------------------

  /**
   * Write-through mode (circuit closed): L2 first, then L1. Resolves once L2
   * has it. A write buffered earlier (while the circuit was open) that has
   * not flushed yet is older, and is superseded once this one lands.
   */
  async #writeThrough(
    key: CacheKey,
    value: Uint8Array,
    lifetime: EntryLifetime,
    attributes: EntryAttributes,
  ): Promise<EntryMetadata> {
    const older = this.#pending.get(key);
    return this.#mutex.runExclusive(key, async () => {
      const epoch = this.#epoch;
      let metadata: EntryMetadata;
      try {
        metadata = await this.#guard(() =>
          this.#l2.write(key, value, lifetime, attributes),
        );
      } catch (error) {
        // The L2 outcome is unknown (the rename may have landed before the
        // failure). Drop L1's copy so it can't contradict L2, unless a
        // buffered write of the key still serves reads.
        this.#invalidateFlight(key);
        if (!this.#pending.has(key)) {
          this.#l1.discard(key);
        }
        throw error;
      }
      if (older !== undefined) {
        this.#cancel(older); // it must not land over this newer value
      }
      this.#invalidateFlight(key);
      if (epoch !== this.#epoch) {
        // A clear() ran meanwhile, so L2's state for this key is unknown.
        this.#l1.discard(key);
      } else if (!this.#pending.has(key)) {
        this.#l1.offer(metadata, value);
      }
      this.#emitWrite(key, 'write-through', value.byteLength);
      return metadata;
    });
  }

  #startFlight(key: CacheKey): Flight {
    const { promise, resolve } = Promise.withResolvers<FlightResult>();
    const flight: Flight = { promise, invalidated: false, epoch: this.#epoch };
    // Registered before the L2 call, so even a synchronous throw from a
    // misbehaving store cleans up the same way.
    this.#flights.set(key, flight);
    resolve(this.#readThrough(key, flight));
    return flight;
  }

  async #readThrough(key: CacheKey, flight: Flight): Promise<FlightResult> {
    try {
      this.#l2Reads += 1;
      let entry: CacheEntry | undefined;
      try {
        entry = await this.#guardRead(() => this.#l2.read(key));
      } catch (error) {
        if (isTierCircuitOpen(error)) {
          return { entry: undefined, shortCircuited: true };
        }
        throw error;
      }
      if (entry !== undefined) {
        // Check and insert in one synchronous step: no mutation can slip in
        // between. (A pending write of the key always invalidates its
        // flight, since `#publish` does, so "not invalidated" implies
        // "nothing pending".)
        const valid = !flight.invalidated && flight.epoch === this.#epoch;
        const admitted = valid && this.#l1.offer(entry.metadata, entry.value);
        if (admitted) {
          this.#promoted += 1;
        } else {
          this.#notPromoted += 1;
        }
        this.#emit({
          type: 'tier.promotion',
          at: this.#clock.now(),
          key,
          admitted,
        });
      }
      return { entry, shortCircuited: false };
    } finally {
      if (this.#flights.get(key) === flight) {
        this.#flights.delete(key);
      }
    }
  }

  /** The in-flight read of `key` may not promote, and nobody may join it. */
  #invalidateFlight(key: CacheKey): void {
    const flight = this.#flights.get(key);
    if (flight !== undefined) {
      flight.invalidated = true;
      this.#flights.delete(key);
    }
  }

  #voidEverything(): void {
    this.#epoch += 1;
    this.#flights.clear();
  }

  // --- telemetry ----------------------------------------------------------------

  #emit(event: TelemetryEvent): void {
    this.#telemetry.emit(event);
  }

  #emitRead(
    key: CacheKey,
    source: 'l1' | 'write-buffer' | 'l2' | 'miss',
    coalesced: boolean,
    shortCircuited: boolean,
  ): void {
    this.#emit({
      type: 'tier.read',
      at: this.#clock.now(),
      key,
      source,
      coalesced,
      shortCircuited,
    });
  }

  #emitWrite(
    key: CacheKey,
    mode: 'buffered' | 'coalesced' | 'write-through' | 'fallback' | 'l1-only',
    bytes: number,
  ): void {
    this.#emit({ type: 'tier.write', at: this.#clock.now(), key, mode, bytes });
  }

  #emitFlush(
    record: PendingWrite,
    outcome: 'ok' | 'requeued' | 'failed',
    started: number,
    probe: boolean,
    error: unknown,
  ): void {
    this.#emit({
      type: 'tier.flush',
      at: this.#clock.now(),
      key: record.key,
      outcome,
      durationMs: performance.now() - started,
      probe,
      error: error === undefined ? undefined : describeError(error),
    });
  }
}

/** A refusal by *this* store's circuit (not a lower-level one). */
function isTierCircuitOpen(error: unknown): boolean {
  return error instanceof CircuitOpenError && error.circuit === TIER_CIRCUIT;
}

function copyEntry(entry: {
  readonly metadata: EntryMetadata;
  readonly value: Uint8Array;
}): CacheEntry {
  return { metadata: { ...entry.metadata }, value: Buffer.from(entry.value) };
}

function positiveInteger(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value < 1) {
    throw new RangeError(`${name} must be a positive safe integer`);
  }
  return value;
}
