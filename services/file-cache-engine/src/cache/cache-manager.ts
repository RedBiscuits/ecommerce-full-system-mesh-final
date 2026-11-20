import type { RateLimiter } from '../rate-limit/types';
import { LoaderTimeoutError } from '../resilience/errors';
import type { Clock } from '../shared/clock';
import { systemClock } from '../shared/clock';
import { SingleFlight } from '../shared/single-flight';
import { describeError } from '../telemetry/events';
import type { TelemetrySink } from '../telemetry/sink';
import { safeSink } from '../telemetry/sink';
import { assertValidKey, checksumOf } from './cache-key';
import { CacheClosedError, EntryTooLargeError } from './errors';
import type {
  CacheEntry,
  CacheKey,
  CacheStore,
  EntryAttributes,
  EntryLifetime,
  EntryMetadata,
  Freshness,
} from './types';
import { freshnessAt } from './types';

/** Lifetime policy for a single write. */
export interface WriteOptions {
  /** How long the entry is fresh. Defaults to `CacheManagerOptions.ttlMs`. */
  readonly ttlMs?: number;
  /**
   * How long after `ttlMs` the entry may still be served while being
   * refreshed in the background. Defaults to
   * `CacheManagerOptions.staleWhileRevalidateMs`.
   */
  readonly staleWhileRevalidateMs?: number;
  /** Descriptive attributes persisted with the entry. */
  readonly attributes?: EntryAttributes;
}

export interface ReadOptions {
  /**
   * Return entries that are past `expiresAt` but still within the
   * stale-while-revalidate window. Default: `false`.
   */
  readonly allowStale?: boolean;
}

/** A successful read, annotated with its lifecycle state. */
export interface CacheHit extends CacheEntry {
  readonly freshness: Freshness;
}

/** Where a `getOrLoad` result came from. */
export type ResultSource = 'cache' | 'stale' | 'loader';

export interface LoadResult extends CacheHit {
  readonly source: ResultSource;
  /** `false` when the loader's value was returned but not persisted. */
  readonly stored: boolean;
}

/** What a loader may return: raw bytes, or bytes plus per-entry overrides. */
export type LoaderOutput =
  | Uint8Array
  | string
  | (WriteOptions & { readonly value: Uint8Array | string });

export interface LoaderContext {
  readonly key: CacheKey;
  /**
   * The stale entry being revalidated, if any. Lets loaders issue conditional
   * requests (e.g. `If-None-Match` using a stored `etag` attribute).
   */
  readonly stale: CacheEntry | undefined;
  readonly signal: AbortSignal | undefined;
}

/** Produces the value for a key on a miss. Typically an origin fetch. */
export type Loader = (context: LoaderContext) => Promise<LoaderOutput>;

export interface GetOrLoadOptions extends WriteOptions {
  readonly signal?: AbortSignal;
  /**
   * Serve stale entries immediately and refresh them in the background.
   * Default: `true`.
   */
  readonly staleWhileRevalidate?: boolean;
}

export interface BackgroundErrorContext {
  readonly key: CacheKey;
  readonly operation: 'revalidate';
}

export interface StoreErrorContext {
  readonly key: CacheKey;
  readonly operation: 'read' | 'write';
}

export interface CacheManagerOptions {
  /** Default freshness lifetime. Default: 5 minutes. */
  readonly ttlMs?: number;
  /** Default stale-while-revalidate window. Default: 1 minute. */
  readonly staleWhileRevalidateMs?: number;
  /**
   * Randomises each entry's TTL by ±ratio so entries written together do not
   * expire together (which would otherwise cause bursts of refresh writes).
   * `0` disables jitter. Default: `0.1` (±10 %).
   */
  readonly ttlJitterRatio?: number;
  /** Reject payloads larger than this many bytes. Default: unlimited. */
  readonly maxEntryBytes?: number;
  /**
   * Gate every loader invocation through this limiter. Lets the cache bound
   * the rate of origin/disk requests on misses.
   */
  readonly rateLimiter?: RateLimiter;
  /**
   * Abort and fail a loader that has not settled within this long. Without
   * it, one hung loader would block every coalesced caller indefinitely.
   * Default: no limit.
   */
  readonly loaderTimeoutMs?: number;
  /**
   * Entries whose `createdAt` lies further in the future than this are
   * treated as expired (clock jumped backwards, or a bogus file).
   * Default: 5 minutes.
   */
  readonly maxFutureSkewMs?: number;
  readonly clock?: Clock;
  /** Random source in `[0, 1)`; injectable for deterministic tests. */
  readonly random?: () => number;
  /**
   * Called when a background revalidation fails. Defaults to
   * `console.error` so failures are never silently swallowed.
   */
  readonly onBackgroundError?: (
    error: unknown,
    context: BackgroundErrorContext,
  ) => void;
  /**
   * Called when the store fails on a read-through path (`get`, `head`,
   * `getOrLoad`). Those paths *degrade* instead of failing: a failed read is
   * a miss and a failed write still returns the loaded value (`stored:
   * false`). Explicit `put`/`delete`/`clear` always propagate errors.
   * Defaults to `console.error`.
   */
  readonly onStoreError?: (error: unknown, context: StoreErrorContext) => void;
  /**
   * Receives `cache.lookup`, `cache.load`, `cache.write` and
   * `cache.store-error` events with latencies. See `MetricsCollector`.
   */
  readonly telemetry?: TelemetrySink;
}

const DEFAULT_TTL_MS = 5 * 60_000;
const DEFAULT_SWR_MS = 60_000;
const DEFAULT_JITTER_RATIO = 0.1;
const DEFAULT_MAX_FUTURE_SKEW_MS = 5 * 60_000;

/**
 * Policy layer over a {@link CacheStore}.
 *
 * Responsibilities (and nothing else — storage details live in the store):
 * - lifetime computation (TTL, stale window, jitter)
 * - freshness classification on reads
 * - stampede protection on misses: single-flight coalescing,
 *   stale-while-revalidate, and optional rate limiting of the loader
 *
 * The public surface is intentionally resource-oriented: `head`, `get`,
 * `put`, `delete`, `clear` are idempotent and side-effect free beyond their
 * stated purpose; `getOrLoad` is the read-through convenience.
 */
export class CacheManager {
  readonly #store: CacheStore;
  readonly #ttlMs: number;
  readonly #swrMs: number;
  readonly #jitterRatio: number;
  readonly #maxEntryBytes: number;
  readonly #rateLimiter: RateLimiter | undefined;
  readonly #clock: Clock;
  readonly #random: () => number;
  readonly #onBackgroundError: (
    error: unknown,
    context: BackgroundErrorContext,
  ) => void;
  readonly #onStoreError: (error: unknown, context: StoreErrorContext) => void;
  readonly #loaderTimeoutMs: number | undefined;
  readonly #maxFutureSkewMs: number;
  readonly #telemetry: TelemetrySink;
  readonly #loads = new SingleFlight<LoadResult>();
  /** Concurrent lookups of one key share a single store read. */
  readonly #lookups = new SingleFlight<CacheEntry | undefined>();
  /** Aborted by `close()`; composed into every loader signal. */
  readonly #shutdown = new AbortController();
  #closed = false;

  constructor(store: CacheStore, options: CacheManagerOptions = {}) {
    this.#store = store;
    this.#ttlMs = requirePositive(options.ttlMs ?? DEFAULT_TTL_MS, 'ttlMs');
    this.#swrMs = requireNonNegative(
      options.staleWhileRevalidateMs ?? DEFAULT_SWR_MS,
      'staleWhileRevalidateMs',
    );
    this.#jitterRatio = options.ttlJitterRatio ?? DEFAULT_JITTER_RATIO;
    if (this.#jitterRatio < 0 || this.#jitterRatio >= 1) {
      throw new RangeError('ttlJitterRatio must be in [0, 1)');
    }
    this.#maxEntryBytes = options.maxEntryBytes ?? Number.POSITIVE_INFINITY;
    this.#rateLimiter = options.rateLimiter;
    this.#clock = options.clock ?? systemClock;
    this.#random = options.random ?? Math.random;
    this.#onBackgroundError =
      options.onBackgroundError ??
      ((error, context): void => {
        console.error(
          `[cache] background ${context.operation} failed for "${context.key}":`,
          error,
        );
      });
    this.#onStoreError =
      options.onStoreError ??
      ((error, context): void => {
        console.error(
          `[cache] store ${context.operation} failed for "${context.key}" (degraded):`,
          error,
        );
      });
    this.#loaderTimeoutMs = options.loaderTimeoutMs;
    if (this.#loaderTimeoutMs !== undefined) {
      requirePositive(this.#loaderTimeoutMs, 'loaderTimeoutMs');
    }
    this.#maxFutureSkewMs = requireNonNegative(
      options.maxFutureSkewMs ?? DEFAULT_MAX_FUTURE_SKEW_MS,
      'maxFutureSkewMs',
    );
    this.#telemetry = safeSink(options.telemetry);
  }

  /** Metadata only — no payload I/O. Expired entries are reported as such. */
  async head(
    key: CacheKey,
  ): Promise<(EntryMetadata & { freshness: Freshness }) | undefined> {
    this.#assertOpen();
    assertValidKey(key);
    const started = performance.now();
    let metadata: EntryMetadata | undefined;
    try {
      metadata = await this.#store.readMetadata(key);
    } catch (error) {
      this.#reportStoreError(error, key, 'read');
      this.#lookup('head', key, undefined, started);
      return undefined;
    }
    const freshness =
      metadata === undefined ? undefined : this.#freshness(metadata);
    this.#lookup('head', key, freshness, started);
    if (metadata === undefined || freshness === undefined) {
      return undefined;
    }
    return { ...metadata, freshness };
  }

  /**
   * Returns the entry if it is fresh (or stale, when `allowStale` is set).
   * Expired entries are treated as a miss; their removal is left to the
   * sweeper so the read path never performs writes.
   */
  async get(
    key: CacheKey,
    options: ReadOptions = {},
  ): Promise<CacheHit | undefined> {
    this.#assertOpen();
    assertValidKey(key);
    const started = performance.now();
    const entry = await this.#readDegraded(key);
    const freshness =
      entry === undefined ? undefined : this.#freshness(entry.metadata);
    const servable =
      freshness === 'fresh' ||
      (freshness === 'stale' && options.allowStale === true);
    this.#lookup('get', key, servable ? freshness : undefined, started);
    if (entry === undefined || !servable || freshness === undefined) {
      return undefined;
    }
    return { ...entry, freshness };
  }

  /** Atomically creates or replaces the entry. */
  async put(
    key: CacheKey,
    value: Uint8Array | string,
    options: WriteOptions = {},
  ): Promise<EntryMetadata> {
    this.#assertOpen();
    assertValidKey(key);
    const bytes = toBytes(value);
    this.#assertSize(key, bytes);
    const started = performance.now();
    try {
      const metadata = await this.#store.write(
        key,
        bytes,
        this.#lifetime(options),
        options.attributes,
      );
      this.#write('put', key, bytes.byteLength, started);
      return metadata;
    } catch (error) {
      this.#write('put', key, bytes.byteLength, started, error);
      throw error;
    }
  }

  /** Removes the entry. Idempotent. */
  async delete(key: CacheKey): Promise<boolean> {
    this.#assertOpen();
    assertValidKey(key);
    const started = performance.now();
    try {
      const removed = await this.#store.delete(key);
      this.#write('delete', key, undefined, started);
      return removed;
    } catch (error) {
      this.#write('delete', key, undefined, started, error);
      throw error;
    }
  }

  /** Removes every entry. */
  async clear(): Promise<void> {
    this.#assertOpen();
    const started = performance.now();
    try {
      await this.#store.clear();
      this.#write('clear', undefined, undefined, started);
    } catch (error) {
      this.#write('clear', undefined, undefined, started, error);
      throw error;
    }
  }

  /** Whether a loader (miss or background revalidation) is running for `key`. */
  isLoading(key: CacheKey): boolean {
    return this.#loads.isInFlight(key);
  }

  /** Number of loaders currently in flight (misses + background refreshes). */
  get pendingLoads(): number {
    return this.#loads.size;
  }

  get isClosed(): boolean {
    return this.#closed;
  }

  /**
   * Aborts every in-flight loader (their `signal` fires with
   * {@link CacheClosedError}), waits for them to settle, and rejects all
   * further operations. Idempotent. The store is not closed here — it may be
   * shared — see `createFileCache().close()` for the full teardown.
   */
  async close(): Promise<void> {
    if (!this.#closed) {
      this.#closed = true;
      this.#shutdown.abort(new CacheClosedError());
    }
    await this.#loads.drain();
  }

  /**
   * Read-through access with stampede protection:
   *
   * 1. Fresh hit → returned immediately.
   * 2. Stale hit → returned immediately; one background revalidation is
   *    started (coalesced across callers).
   * 3. Miss/expired → callers for the same key share a single loader call,
   *    which is rate-limited if a limiter is configured, then written once.
   */
  async getOrLoad(
    key: CacheKey,
    loader: Loader,
    options: GetOrLoadOptions = {},
  ): Promise<LoadResult> {
    this.#assertOpen();
    assertValidKey(key);
    const started = performance.now();
    const entry = await this.#readDegraded(key);
    if (entry !== undefined) {
      const freshness = this.#freshness(entry.metadata);
      if (freshness === 'fresh') {
        this.#lookup('getOrLoad', key, 'fresh', started);
        return { ...entry, freshness, source: 'cache', stored: true };
      }
      if (freshness === 'stale' && (options.staleWhileRevalidate ?? true)) {
        this.#lookup('getOrLoad', key, 'stale', started);
        this.#revalidateInBackground(key, entry, loader, options);
        return { ...entry, freshness, source: 'stale', stored: true };
      }
    }
    this.#lookup('getOrLoad', key, undefined, started);
    return this.#loads.run(key, () =>
      this.#load(key, entry, loader, options, false),
    );
  }

  // --- internals ----------------------------------------------------------

  #revalidateInBackground(
    key: CacheKey,
    stale: CacheEntry,
    loader: Loader,
    options: GetOrLoadOptions,
  ): void {
    if (this.#loads.isInFlight(key)) {
      return;
    }
    // Background work must not inherit the caller's abort signal.
    const { signal: _ignored, ...detached } = options;
    void this.#loads
      .run(key, () => this.#load(key, stale, loader, detached, true))
      .catch((error: unknown) => {
        this.#onBackgroundError(error, { key, operation: 'revalidate' });
      });
  }

  async #load(
    key: CacheKey,
    stale: CacheEntry | undefined,
    loader: Loader,
    options: GetOrLoadOptions,
    background: boolean,
  ): Promise<LoadResult> {
    const started = performance.now();
    try {
      const result = await this.#loadUnmeasured(key, stale, loader, options);
      this.#telemetry.emit({
        type: 'cache.load',
        at: this.#clock.now(),
        key,
        background,
        durationMs: performance.now() - started,
        outcome: result.stored ? 'stored' : 'unstored',
        source: result.source,
        bytes: result.metadata.size,
        error: undefined,
      });
      return result;
    } catch (error) {
      this.#telemetry.emit({
        type: 'cache.load',
        at: this.#clock.now(),
        key,
        background,
        durationMs: performance.now() - started,
        outcome: 'error',
        source: undefined,
        bytes: undefined,
        error: describeError(error),
      });
      throw error;
    }
  }

  async #loadUnmeasured(
    key: CacheKey,
    stale: CacheEntry | undefined,
    loader: Loader,
    options: GetOrLoadOptions,
  ): Promise<LoadResult> {
    // No re-read here: the leader enters this flight synchronously after its
    // own lookup, and followers join the flight rather than run it, so a
    // second read could never observe anything newer.

    // Don't spend a loader call (or a rate-limit token) on a caller that
    // already gave up while we were checking the store.
    options.signal?.throwIfAborted();
    this.#shutdown.signal.throwIfAborted();
    if (this.#rateLimiter !== undefined) {
      await this.#rateLimiter.acquire(1, { signal: options.signal });
    }

    const output = normaliseOutput(
      await this.#invokeLoader(key, stale, loader, options.signal),
    );
    const writeOptions: WriteOptions = {
      ttlMs: output.ttlMs ?? options.ttlMs,
      staleWhileRevalidateMs:
        output.staleWhileRevalidateMs ?? options.staleWhileRevalidateMs,
      attributes: output.attributes ?? options.attributes,
    };

    if (output.value.byteLength > this.#maxEntryBytes) {
      // Pass-through: the caller still gets its data; we just don't keep it.
      const lifetime = this.#lifetime(writeOptions);
      const metadata: EntryMetadata = {
        key,
        ...lifetime,
        size: output.value.byteLength,
        checksum: checksumOf(output.value),
        attributes: Object.freeze({ ...writeOptions.attributes }),
      };
      return {
        metadata,
        value: asBuffer(output.value),
        freshness: 'fresh',
        source: 'loader',
        stored: false,
      };
    }

    const lifetime = this.#lifetime(writeOptions);
    try {
      const metadata = await this.#store.write(
        key,
        output.value,
        lifetime,
        writeOptions.attributes,
      );
      return {
        metadata,
        value: asBuffer(output.value),
        freshness: 'fresh',
        source: 'loader',
        stored: true,
      };
    } catch (error) {
      // The origin already answered; losing the cache write must not lose
      // the response. Degrade to an unstored result and report.
      this.#reportStoreError(error, key, 'write');
      return {
        metadata: {
          key,
          ...lifetime,
          size: output.value.byteLength,
          checksum: checksumOf(output.value),
          attributes: Object.freeze({ ...writeOptions.attributes }),
        },
        value: asBuffer(output.value),
        freshness: 'fresh',
        source: 'loader',
        stored: false,
      };
    }
  }

  /**
   * Runs the loader under a signal that fires on: caller abort, `close()`,
   * or the loader time budget. Composition is manual (not `AbortSignal.any`)
   * so every listener is removed deterministically in `finally`, and the
   * long-lived shutdown signal never accumulates stale listeners.
   */
  async #invokeLoader(
    key: CacheKey,
    stale: CacheEntry | undefined,
    loader: Loader,
    callerSignal: AbortSignal | undefined,
  ): Promise<LoaderOutput> {
    const controller = new AbortController();
    const sources = [callerSignal, this.#shutdown.signal].filter(
      (signal): signal is AbortSignal => signal !== undefined,
    );
    const forward = (source: AbortSignal) => (): void =>
      controller.abort(source.reason);
    const listeners = sources.map((source) => {
      const listener = forward(source);
      source.addEventListener('abort', listener, { once: true });
      return { source, listener };
    });

    const timeoutMs = this.#loaderTimeoutMs;
    const { promise: expired, reject } = Promise.withResolvers<never>();
    const timer =
      timeoutMs === undefined
        ? undefined
        : setTimeout(() => {
            const error = new LoaderTimeoutError(key, timeoutMs);
            controller.abort(error);
            reject(error);
          }, timeoutMs);

    try {
      return await Promise.race([
        loader({ key, stale, signal: controller.signal }),
        expired,
      ]);
    } finally {
      clearTimeout(timer);
      for (const { source, listener } of listeners) {
        source.removeEventListener('abort', listener);
      }
    }
  }

  #assertOpen(): void {
    if (this.#closed) {
      throw new CacheClosedError();
    }
  }

  /**
   * A read that reports and degrades to a miss instead of throwing.
   * Concurrent callers for the same key share one read: besides saving I/O
   * on hot keys, it guarantees their continuations run in the same microtask
   * batch, so a miss always coalesces into a single loader call — even when
   * the loader fails faster than a disk read.
   */
  #readDegraded(key: CacheKey): Promise<CacheEntry | undefined> {
    return this.#lookups.run(key, async () => {
      try {
        return await this.#store.read(key);
      } catch (error) {
        this.#reportStoreError(error, key, 'read');
        return undefined;
      }
    });
  }

  #reportStoreError(
    error: unknown,
    key: CacheKey,
    operation: 'read' | 'write',
  ): void {
    this.#onStoreError(error, { key, operation });
    this.#telemetry.emit({
      type: 'cache.store-error',
      at: this.#clock.now(),
      operation,
      key,
      error: describeError(error),
    });
  }

  #lookup(
    operation: 'get' | 'head' | 'getOrLoad',
    key: CacheKey,
    freshness: Freshness | undefined,
    started: number,
  ): void {
    this.#telemetry.emit({
      type: 'cache.lookup',
      at: this.#clock.now(),
      operation,
      key,
      outcome:
        freshness === 'fresh'
          ? 'hit'
          : freshness === 'stale'
            ? 'stale'
            : 'miss',
      freshness,
      durationMs: performance.now() - started,
    });
  }

  #write(
    operation: 'put' | 'delete' | 'clear',
    key: CacheKey | undefined,
    bytes: number | undefined,
    started: number,
    error?: unknown,
  ): void {
    this.#telemetry.emit({
      type: 'cache.write',
      at: this.#clock.now(),
      operation,
      key,
      durationMs: performance.now() - started,
      bytes,
      error: error === undefined ? undefined : describeError(error),
    });
  }

  #freshness(metadata: EntryMetadata): Freshness {
    return freshnessAt(metadata, this.#clock.now(), this.#maxFutureSkewMs);
  }

  #lifetime(options: WriteOptions): EntryLifetime {
    const ttl = requirePositive(options.ttlMs ?? this.#ttlMs, 'ttlMs');
    const swr = requireNonNegative(
      options.staleWhileRevalidateMs ?? this.#swrMs,
      'staleWhileRevalidateMs',
    );
    const createdAt = this.#clock.now();
    const expiresAt = createdAt + Math.round(ttl * this.#jitterFactor());
    return { createdAt, expiresAt, staleUntil: expiresAt + swr };
  }

  /** Uniform factor in `[1 - ratio, 1 + ratio]`. */
  #jitterFactor(): number {
    if (this.#jitterRatio === 0) {
      return 1;
    }
    return 1 + (this.#random() * 2 - 1) * this.#jitterRatio;
  }

  #assertSize(key: CacheKey, bytes: Uint8Array): void {
    if (bytes.byteLength > this.#maxEntryBytes) {
      throw new EntryTooLargeError(key, bytes.byteLength, this.#maxEntryBytes);
    }
  }
}

// --- helpers -----------------------------------------------------------------

interface NormalisedOutput extends WriteOptions {
  readonly value: Uint8Array;
}

function normaliseOutput(output: LoaderOutput): NormalisedOutput {
  if (typeof output === 'string' || output instanceof Uint8Array) {
    return { value: toBytes(output) };
  }
  return { ...output, value: toBytes(output.value) };
}

function toBytes(value: Uint8Array | string): Uint8Array {
  return typeof value === 'string' ? Buffer.from(value, 'utf8') : value;
}

/** Zero-copy `Buffer` view over any `Uint8Array`. */
function asBuffer(value: Uint8Array): Buffer {
  return Buffer.isBuffer(value)
    ? value
    : Buffer.from(value.buffer, value.byteOffset, value.byteLength);
}

function requirePositive(value: number, name: string): number {
  if (!Number.isFinite(value) || value <= 0) {
    throw new RangeError(`${name} must be a positive finite number`);
  }
  return value;
}

function requireNonNegative(value: number, name: string): number {
  if (!Number.isFinite(value) || value < 0) {
    throw new RangeError(`${name} must be a non-negative finite number`);
  }
  return value;
}
