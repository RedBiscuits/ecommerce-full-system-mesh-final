/**
 * Public contracts for the cache layer.
 *
 * The design follows a ports-and-adapters split:
 * - {@link CacheStore} is the *port* every storage backend implements
 *   (currently `FsCacheStore` on top of `node:fs`).
 * - `CacheManager` is the policy layer (TTL, staleness, stampede protection)
 *   and depends only on this port (dependency inversion).
 */

/** Opaque, caller-defined identifier for an entry (e.g. a URL or a path). */
export type CacheKey = string;

/**
 * Free-form, string-only attributes persisted with an entry. Useful for
 * HTTP-style caching (`content-type`, `etag`, `last-modified`, ...).
 * Attributes are descriptive only and are never interpreted by the cache.
 */
export type EntryAttributes = Readonly<Record<string, string>>;

/** Header persisted alongside every payload. */
export interface EntryMetadata {
  /** The original key the entry was stored under. */
  readonly key: CacheKey;
  /** Epoch milliseconds at which the entry was written. */
  readonly createdAt: number;
  /** Epoch milliseconds until which the entry is considered fresh. */
  readonly expiresAt: number;
  /**
   * Epoch milliseconds until which a no-longer-fresh entry may still be served
   * while a background refresh runs (`staleUntil >= expiresAt`).
   */
  readonly staleUntil: number;
  /** Payload size in bytes. */
  readonly size: number;
  /** Integrity digest of the payload, formatted as `sha256:<hex>`. */
  readonly checksum: string;
  /** Caller-supplied descriptive attributes. */
  readonly attributes: EntryAttributes;
}

/** Lifecycle state of an entry relative to the current time. */
export type Freshness = 'fresh' | 'stale' | 'expired';

/** A fully materialised entry: metadata plus payload bytes. */
export interface CacheEntry {
  readonly metadata: EntryMetadata;
  /** Raw payload. The cache never decodes, parses, or executes it. */
  readonly value: Buffer;
}

/** Time-based policy applied when an entry is written. */
export interface EntryLifetime {
  readonly createdAt: number;
  readonly expiresAt: number;
  readonly staleUntil: number;
}

/**
 * Storage port. Implementations must guarantee:
 * - **Atomic writes**: readers observe either the previous entry or the new
 *   one in full — never a partial file.
 * - **Integrity**: a payload that does not match its recorded checksum/size is
 *   treated as absent (and may be discarded).
 * - **Idempotent deletes**: deleting a missing entry is not an error.
 */
export interface CacheStore {
  /** Reads metadata and payload. Resolves `undefined` on a miss. */
  read(key: CacheKey): Promise<CacheEntry | undefined>;

  /** Reads only the header, without touching the payload. */
  readMetadata(key: CacheKey): Promise<EntryMetadata | undefined>;

  /** Atomically replaces (or creates) the entry for `key`. */
  write(
    key: CacheKey,
    value: Uint8Array,
    lifetime: EntryLifetime,
    attributes?: EntryAttributes,
  ): Promise<EntryMetadata>;

  /** Removes the entry. Resolves `true` if something was removed. */
  delete(key: CacheKey): Promise<boolean>;

  /**
   * Removes the entry only if `predicate` holds for its *current* metadata,
   * evaluated atomically with respect to other mutations of the same key.
   * Lets a sweeper act on what is on disk *now* rather than on a stale scan.
   */
  deleteIf(
    key: CacheKey,
    predicate: (metadata: EntryMetadata) => boolean,
  ): Promise<boolean>;

  /** Removes every entry. */
  clear(): Promise<void>;

  /**
   * Streams the metadata of every stored entry without loading payloads.
   * Order is unspecified. Entries written or removed during iteration may or
   * may not be observed.
   */
  scan(): AsyncIterable<EntryMetadata>;
}

/**
 * Computes the {@link Freshness} of an entry at a point in time.
 *
 * An entry created further in the future than `maxFutureSkewMs` is treated
 * as expired: it was written by a clock that has since jumped backwards (or
 * by a bogus file), and trusting its lifetime would keep it alive
 * indefinitely.
 */
export function freshnessAt(
  lifetime: Pick<EntryLifetime, 'expiresAt' | 'staleUntil'> &
    Partial<Pick<EntryLifetime, 'createdAt'>>,
  now: number,
  maxFutureSkewMs = Number.POSITIVE_INFINITY,
): Freshness {
  if (
    lifetime.createdAt !== undefined &&
    lifetime.createdAt - now > maxFutureSkewMs
  ) {
    return 'expired';
  }
  if (now < lifetime.expiresAt) {
    return 'fresh';
  }
  if (now < lifetime.staleUntil) {
    return 'stale';
  }
  return 'expired';
}
