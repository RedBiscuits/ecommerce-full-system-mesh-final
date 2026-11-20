import { FileLock } from '../resilience/file-lock';
import type { Clock } from '../shared/clock';
import { systemClock } from '../shared/clock';
import { sleep } from '../shared/sleep';
import type { CacheStore, EntryMetadata } from './types';
import { freshnessAt } from './types';

export interface ExpirySweeperOptions {
  /** Time between the end of one sweep and the start of the next. Default: 60 s. */
  readonly intervalMs?: number;
  /**
   * Randomises each interval by ±ratio so multiple processes sharing a volume
   * do not sweep in lock-step. Default: `0.1`.
   */
  readonly intervalJitterRatio?: number;
  /** Deletions issued before pausing. Default: `100`. */
  readonly batchSize?: number;
  /** Pause between deletion batches, throttling I/O pressure. Default: 25 ms. */
  readonly batchPauseMs?: number;
  /**
   * Optional disk budget. When the live entries exceed it, the sweeper evicts
   * the entries closest to expiry until the total fits. Default: unlimited.
   */
  readonly maxTotalBytes?: number;
  /**
   * Cross-process mutual exclusion. When several processes share the cache
   * directory (cluster workers), only the holder of this lock file sweeps;
   * the others skip their tick. Default: no locking.
   */
  readonly lock?: { readonly path: string; readonly staleAfterMs?: number };
  /** Entries created further in the future than this are evicted. Default: 5 min. */
  readonly maxFutureSkewMs?: number;
  readonly clock?: Clock;
  readonly random?: () => number;
  /**
   * Called when a scheduled sweep throws, and for each entry whose deletion
   * failed (the sweep itself continues). Defaults to `console.error`.
   */
  readonly onError?: (error: unknown) => void;
  /** Called with the report of every completed sweep. */
  readonly onSweep?: (report: SweepReport) => void;
}

export interface SweepReport {
  /** Entries inspected. */
  readonly scanned: number;
  /** Entries removed because they were past their stale window. */
  readonly expired: number;
  /** Entries removed to satisfy `maxTotalBytes`. */
  readonly evicted: number;
  /** Deletions that failed; the sweep carried on past them. */
  readonly failed: number;
  /** Bytes held by live entries after the sweep. */
  readonly liveBytes: number;
  readonly durationMs: number;
  /** `true` when another process held the sweep lock and nothing was done. */
  readonly skipped: boolean;
}

const DEFAULT_INTERVAL_MS = 60_000;
const DEFAULT_JITTER_RATIO = 0.1;
const DEFAULT_BATCH_SIZE = 100;
const DEFAULT_BATCH_PAUSE_MS = 25;
const DEFAULT_MAX_FUTURE_SKEW_MS = 5 * 60_000;

/**
 * Background garbage collector for cold entries.
 *
 * Design notes:
 * - Expiry is *lazy*: reads never delete, and the sweeper is the only
 *   component that removes expired files. This keeps the hot path read-only.
 * - Deletions are batched with pauses between batches so a large backlog of
 *   cold files never turns into an I/O storm.
 * - Only entries past `staleUntil` are removed, so stale-while-revalidate
 *   still has something to serve.
 * - Sweeps never overlap; if one is still running when the timer fires, the
 *   tick is skipped.
 * - The periodic timer is `unref`'d and never keeps the process alive; an
 *   explicitly awaited `sweepOnce()` does, like any other awaited work.
 */
export class ExpirySweeper {
  readonly #store: CacheStore;
  readonly #intervalMs: number;
  readonly #jitterRatio: number;
  readonly #batchSize: number;
  readonly #batchPauseMs: number;
  readonly #maxTotalBytes: number;
  readonly #lock: ExpirySweeperOptions['lock'];
  readonly #maxFutureSkewMs: number;
  readonly #clock: Clock;
  readonly #random: () => number;
  readonly #onError: (error: unknown) => void;
  readonly #onSweep: ((report: SweepReport) => void) | undefined;
  #lastReport: (SweepReport & { readonly completedAt: number }) | undefined;

  #timer: NodeJS.Timeout | undefined;
  #abort: AbortController | undefined;
  #running: Promise<SweepReport> | undefined;

  constructor(store: CacheStore, options: ExpirySweeperOptions = {}) {
    this.#store = store;
    this.#intervalMs = options.intervalMs ?? DEFAULT_INTERVAL_MS;
    this.#jitterRatio = options.intervalJitterRatio ?? DEFAULT_JITTER_RATIO;
    this.#batchSize = Math.max(1, options.batchSize ?? DEFAULT_BATCH_SIZE);
    this.#batchPauseMs = options.batchPauseMs ?? DEFAULT_BATCH_PAUSE_MS;
    this.#maxTotalBytes = options.maxTotalBytes ?? Number.POSITIVE_INFINITY;
    this.#lock = options.lock;
    this.#maxFutureSkewMs =
      options.maxFutureSkewMs ?? DEFAULT_MAX_FUTURE_SKEW_MS;
    this.#clock = options.clock ?? systemClock;
    this.#random = options.random ?? Math.random;
    this.#onError =
      options.onError ??
      ((error): void => {
        console.error('[cache] sweep failed:', error);
      });
    this.#onSweep = options.onSweep;
  }

  /** The most recent completed sweep, with its completion time. */
  get lastReport():
    | (SweepReport & { readonly completedAt: number })
    | undefined {
    return this.#lastReport;
  }

  /** Whether periodic sweeping is scheduled. */
  get isRunning(): boolean {
    return this.#abort !== undefined;
  }

  /** Begins periodic sweeping. Idempotent. */
  start(): void {
    if (this.#abort !== undefined) {
      return;
    }
    this.#abort = new AbortController();
    this.#schedule();
  }

  /**
   * Stops periodic sweeping and waits for an in-progress sweep to wind down.
   * Idempotent.
   */
  async stop(): Promise<void> {
    const abort = this.#abort;
    this.#abort = undefined;
    clearTimeout(this.#timer);
    this.#timer = undefined;
    abort?.abort();
    // Also waits for a manual `sweepOnce()` that was started without
    // `start()`, so "stopped" always means "nothing is running".
    await this.#running?.catch(() => undefined);
  }

  /**
   * Runs one sweep immediately (or joins the sweep already in progress).
   * Safe to call whether or not the sweeper is started.
   */
  sweepOnce(): Promise<SweepReport> {
    if (this.#running === undefined) {
      this.#running = this.#sweep(this.#abort?.signal)
        .then((report) => {
          this.#lastReport = { ...report, completedAt: this.#clock.now() };
          try {
            this.#onSweep?.(report);
          } catch {
            // observers never break the sweeper
          }
          return report;
        })
        .finally(() => {
          this.#running = undefined;
        });
    }
    return this.#running;
  }

  // --- internals ----------------------------------------------------------

  #schedule(): void {
    clearTimeout(this.#timer);
    this.#timer = undefined;
    if (this.#abort === undefined) {
      return;
    }
    const delay = Math.max(0, Math.round(this.#intervalMs * this.#jitter()));
    this.#timer = setTimeout(() => {
      this.#timer = undefined;
      this.sweepOnce()
        .catch((error: unknown) => {
          if (this.#abort !== undefined) {
            this.#onError(error);
          }
        })
        .finally(() => this.#schedule());
    }, delay);
    this.#timer.unref();
  }

  #jitter(): number {
    if (this.#jitterRatio === 0) {
      return 1;
    }
    return 1 + (this.#random() * 2 - 1) * this.#jitterRatio;
  }

  async #sweep(signal: AbortSignal | undefined): Promise<SweepReport> {
    const startedAt = this.#clock.now();
    const empty: SweepReport = {
      scanned: 0,
      expired: 0,
      evicted: 0,
      failed: 0,
      liveBytes: 0,
      durationMs: 0,
      skipped: true,
    };
    if (this.#lock === undefined) {
      return this.#sweepUnlocked(signal, startedAt);
    }
    const lock = await FileLock.tryAcquire(this.#lock.path, {
      staleAfterMs: this.#lock.staleAfterMs,
      clock: this.#clock,
      signal,
    });
    if (lock === undefined) {
      return empty;
    }
    try {
      return await this.#sweepUnlocked(signal, startedAt, lock);
    } finally {
      await lock.release();
    }
  }

  async #sweepUnlocked(
    signal: AbortSignal | undefined,
    startedAt: number,
    lock?: FileLock,
  ): Promise<SweepReport> {
    // Live entries are only retained when a byte budget needs them; without
    // one the sweep runs in O(1) memory regardless of cache size.
    const trackLive = Number.isFinite(this.#maxTotalBytes);
    const live: EntryMetadata[] = [];
    let scanned = 0;
    let expired = 0;
    let evicted = 0;
    let failed = 0;
    let liveBytes = 0;
    let pendingDeletes = 0;

    const isCold = (metadata: EntryMetadata): boolean =>
      freshnessAt(metadata, this.#clock.now(), this.#maxFutureSkewMs) ===
      'expired';

    /**
     * Conditional delete: the decision is re-evaluated against the entry
     * as it is *now*, under the store's per-key lock, so an entry rewritten
     * between scan and delete survives. Failures are counted, not fatal.
     */
    const remove = async (
      metadata: EntryMetadata,
      predicate: (current: EntryMetadata) => boolean,
    ): Promise<boolean> => {
      let removed = false;
      try {
        removed = await this.#store.deleteIf(metadata.key, predicate);
      } catch (error) {
        failed += 1;
        this.#onError(error);
      }
      pendingDeletes += 1;
      if (pendingDeletes >= this.#batchSize) {
        pendingDeletes = 0;
        await sleep(this.#batchPauseMs, { signal });
        // Throws LockLostError if another process took the lock over; the
        // sweep stops rather than running concurrently with it.
        await lock?.refresh();
      }
      return removed;
    };

    for await (const metadata of this.#store.scan()) {
      signal?.throwIfAborted();
      scanned += 1;
      if (isCold(metadata)) {
        if (await remove(metadata, isCold)) {
          expired += 1;
        }
      } else {
        if (trackLive) {
          live.push(metadata);
        }
        liveBytes += metadata.size;
      }
    }

    if (liveBytes > this.#maxTotalBytes) {
      // Evict soonest-to-expire first: they hold the least remaining value.
      live.sort((a, b) => a.expiresAt - b.expiresAt);
      for (const metadata of live) {
        if (liveBytes <= this.#maxTotalBytes) {
          break;
        }
        signal?.throwIfAborted();
        // Only evict the very entry we scanned; a rewrite means new value.
        const unchanged = (current: EntryMetadata): boolean =>
          current.createdAt === metadata.createdAt &&
          current.checksum === metadata.checksum;
        if (await remove(metadata, unchanged)) {
          evicted += 1;
          liveBytes -= metadata.size;
        }
      }
    }

    return {
      scanned,
      expired,
      evicted,
      failed,
      liveBytes,
      durationMs: this.#clock.now() - startedAt,
      skipped: false,
    };
  }
}
