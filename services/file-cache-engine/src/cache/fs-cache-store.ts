import { randomBytes } from 'node:crypto';
import type { Dir, Dirent } from 'node:fs';
import type { FileHandle } from 'node:fs/promises';
import { mkdir, open, opendir, rename, rm, unlink } from 'node:fs/promises';
import { join } from 'node:path';

import { KeyedMutex } from '../shared/keyed-mutex';
import { checksumOf, hashKey } from './cache-key';
import {
  PREAMBLE_LENGTH,
  decodeHeader,
  decodePreamble,
  encodeHeader,
} from './entry-codec';
import { CacheIntegrityError } from './errors';
import type {
  CacheEntry,
  CacheKey,
  CacheStore,
  EntryAttributes,
  EntryLifetime,
  EntryMetadata,
} from './types';

export interface FsCacheStoreOptions {
  /** Root directory. Created on open if missing. */
  readonly directory: string;
  /**
   * Flush file data (and the containing directory) to stable storage before a
   * write is acknowledged. Provides durability across power loss at the cost
   * of throughput. Default: `true`.
   */
  readonly fsync?: boolean;
  /**
   * Verify the payload checksum on every read. Detects bit-rot and external
   * tampering at the cost of one hash per read. Default: `true`.
   */
  readonly verifyChecksum?: boolean;
  /**
   * POSIX mode for entry files. Defaults to `0o600`: owner read/write only,
   * never executable — cached content is data, not code.
   */
  readonly fileMode?: number;
  /**
   * Invoked when a stored file fails validation and is discarded. Use it for
   * logging/metrics; the store already handles recovery.
   */
  readonly onCorruptEntry?: (error: CacheIntegrityError) => void;
  /**
   * Remove temp files left by interrupted writes when opening. Diagnostics
   * tooling sets this to `false` to observe them instead. Default: `true`.
   */
  readonly purgeTempFiles?: boolean;
}

/**
 * Size of the first read on every lookup. Entries up to roughly this size
 * (header included) are served with one `read` and no `fstat`.
 */
const SPECULATIVE_READ_BYTES = 16 * 1024;

const ENTRIES_DIR = 'entries';
const TMP_DIR = 'tmp';
const ENTRY_EXTENSION = '.cache';
const TMP_EXTENSION = '.tmp';
const SHARD_LENGTH = 2;

/**
 * File-system backed {@link CacheStore} built on `node:fs`.
 *
 * Layout:
 * ```
 * <directory>/
 *   entries/<hh>/<sha256(key)>.cache   # hh = first two hex chars (sharding)
 *   tmp/<sha256(key)>.<random>.tmp     # in-progress writes (same volume)
 * ```
 *
 * ACID mapping:
 * - **Atomicity** – payload + header are written to a temp file on the same
 *   volume and published with a single `rename(2)`, which POSIX guarantees to
 *   be atomic. A crash mid-write leaves only an orphan temp file, purged on
 *   the next `open()`.
 * - **Consistency** – every read validates magic, header schema, size and
 *   (by default) the SHA-256 checksum. Invalid files are discarded and
 *   reported as a miss, never served.
 * - **Isolation** – mutations for the same key are serialised by an in-process
 *   mutex; readers never block and always see a complete entry.
 * - **Durability** – `fsync` on the file and its directory (enabled by
 *   default) before the write resolves.
 */
export class FsCacheStore implements CacheStore {
  readonly #entriesDir: string;
  readonly #tmpDir: string;
  readonly #fsync: boolean;
  readonly #verifyChecksum: boolean;
  readonly #fileMode: number;
  readonly #onCorruptEntry: ((error: CacheIntegrityError) => void) | undefined;
  readonly #mutex = new KeyedMutex();
  /** Shard directories known to exist; saves one `mkdir` per write. */
  readonly #knownShards = new Set<string>();
  #quarantined = 0;

  private constructor(options: FsCacheStoreOptions) {
    this.#entriesDir = join(options.directory, ENTRIES_DIR);
    this.#tmpDir = join(options.directory, TMP_DIR);
    this.#fsync = options.fsync ?? true;
    this.#verifyChecksum = options.verifyChecksum ?? true;
    this.#fileMode = options.fileMode ?? 0o600;
    this.#onCorruptEntry = options.onCorruptEntry;
  }

  /**
   * Creates the directory layout and purges orphaned temp files left behind
   * by an interrupted write.
   */
  static async open(options: FsCacheStoreOptions): Promise<FsCacheStore> {
    const store = new FsCacheStore(options);
    await store.#ensureLayout();
    if (options.purgeTempFiles ?? true) {
      await store.#purgeTempFiles();
    }
    return store;
  }

  /** Root of the `entries/` tree (exposed for diagnostics and tests). */
  get entriesDirectory(): string {
    return this.#entriesDir;
  }

  /** Files quarantined as corrupt since this store was opened. */
  get quarantinedCount(): number {
    return this.#quarantined;
  }

  async read(key: CacheKey): Promise<CacheEntry | undefined> {
    const path = this.#entryPath(key);
    return this.#readFile(path, key, true);
  }

  async readMetadata(key: CacheKey): Promise<EntryMetadata | undefined> {
    const path = this.#entryPath(key);
    const entry = await this.#readFile(path, key, false);
    return entry?.metadata;
  }

  async write(
    key: CacheKey,
    value: Uint8Array,
    lifetime: EntryLifetime,
    attributes: EntryAttributes = {},
  ): Promise<EntryMetadata> {
    const hash = hashKey(key);
    const metadata: EntryMetadata = {
      key,
      createdAt: lifetime.createdAt,
      expiresAt: lifetime.expiresAt,
      staleUntil: lifetime.staleUntil,
      size: value.byteLength,
      checksum: checksumOf(value),
      attributes: Object.freeze({ ...attributes }),
    };
    const header = encodeHeader(metadata);

    await this.#mutex.runExclusive(hash, async () => {
      const finalPath = this.#pathForHash(hash);
      const shardDir = join(this.#entriesDir, hash.slice(0, SHARD_LENGTH));
      const tmpPath = join(
        this.#tmpDir,
        `${hash}.${randomBytes(6).toString('hex')}${TMP_EXTENSION}`,
      );

      const publish = async (): Promise<void> => {
        if (!this.#knownShards.has(shardDir)) {
          await mkdir(shardDir, { recursive: true });
          this.#knownShards.add(shardDir);
        }
        try {
          await this.#writeTempFile(tmpPath, header, value);
          await rename(tmpPath, finalPath);
          if (this.#fsync) {
            await syncDirectory(shardDir);
          }
        } catch (error) {
          await unlink(tmpPath).catch(() => undefined);
          throw error;
        }
      };

      try {
        await publish();
      } catch (error) {
        // The layout was removed underneath us (operator `rm -rf`, a
        // concurrent `clear()`): rebuild it and try exactly once more.
        if (!isErrno(error, 'ENOENT') && !isErrno(error, 'ENOTDIR')) {
          throw error;
        }
        this.#knownShards.clear();
        await this.#ensureLayout();
        await publish();
      }
    });

    return metadata;
  }

  async delete(key: CacheKey): Promise<boolean> {
    const hash = hashKey(key);
    return this.#mutex.runExclusive(hash, () =>
      unlinkIfExists(this.#pathForHash(hash)),
    );
  }

  async deleteIf(
    key: CacheKey,
    predicate: (metadata: EntryMetadata) => boolean,
  ): Promise<boolean> {
    const hash = hashKey(key);
    return this.#mutex.runExclusive(hash, async () => {
      const path = this.#pathForHash(hash);
      const entry = await this.#readFile(path, key, false);
      if (entry === undefined || !predicate(entry.metadata)) {
        return false;
      }
      return unlinkIfExists(path);
    });
  }

  /**
   * Swaps the entries tree aside in one `rename` (so the cache is observably
   * empty immediately, even with writers racing) and then removes the old
   * tree. A writer that had already resolved its shard path lands in the
   * old tree and is discarded with it — acceptable, since it raced `clear`.
   */
  async clear(): Promise<void> {
    this.#knownShards.clear();
    const aside = `${this.#entriesDir}.clearing-${randomBytes(4).toString('hex')}`;
    try {
      await rename(this.#entriesDir, aside);
    } catch (error) {
      if (!isErrno(error, 'ENOENT')) {
        throw error;
      }
    }
    await mkdir(this.#entriesDir, { recursive: true });
    await removeTree(aside);
  }

  async *scan(): AsyncIterable<EntryMetadata> {
    for await (const shard of safeOpendir(this.#entriesDir)) {
      if (!shard.isDirectory()) {
        continue;
      }
      const shardDir = join(this.#entriesDir, shard.name);
      for await (const file of safeOpendir(shardDir)) {
        if (!file.isFile() || !file.name.endsWith(ENTRY_EXTENSION)) {
          continue;
        }
        const entry = await this.#readFile(
          join(shardDir, file.name),
          undefined,
          false,
        );
        if (entry !== undefined) {
          yield entry.metadata;
        }
      }
    }
  }

  // --- internals ----------------------------------------------------------

  async #ensureLayout(): Promise<void> {
    await mkdir(this.#entriesDir, { recursive: true });
    await mkdir(this.#tmpDir, { recursive: true });
  }

  #entryPath(key: CacheKey): string {
    return this.#pathForHash(hashKey(key));
  }

  #pathForHash(hash: string): string {
    return join(
      this.#entriesDir,
      hash.slice(0, SHARD_LENGTH),
      `${hash}${ENTRY_EXTENSION}`,
    );
  }

  async #writeTempFile(
    tmpPath: string,
    header: Buffer,
    value: Uint8Array,
  ): Promise<void> {
    // 'wx' => O_CREAT | O_EXCL: never clobber a concurrent temp file.
    const handle = await open(tmpPath, 'wx', this.#fileMode);
    try {
      await writevFully(handle, [header, value]);
      if (this.#fsync) {
        await handle.sync();
      }
    } finally {
      await handle.close();
    }
  }

  /**
   * Reads an entry file. Returns `undefined` on ENOENT. Files that fail
   * validation are discarded (self-healing) and reported via `onCorruptEntry`;
   * callers see them as a miss.
   */
  async #readFile(
    path: string,
    expectedKey: CacheKey | undefined,
    includePayload: boolean,
  ): Promise<CacheEntry | undefined> {
    let handle: FileHandle;
    try {
      handle = await open(path, 'r');
    } catch (error) {
      if (isErrno(error, 'ENOENT') || isErrno(error, 'ENOTDIR')) {
        return undefined;
      }
      throw error;
    }

    let corruption: CacheIntegrityError | undefined;
    try {
      const entry = await this.#parseEntry(handle, path, includePayload);
      if (expectedKey !== undefined && entry.metadata.key !== expectedKey) {
        throw new CacheIntegrityError(
          path,
          'stored key does not match the requested key',
        );
      }
      return entry;
    } catch (error) {
      if (error instanceof CacheIntegrityError) {
        corruption = error;
      } else if (isErrno(error, 'EISDIR')) {
        // Something put a directory where an entry belongs.
        corruption = new CacheIntegrityError(path, 'entry path is a directory');
      } else {
        throw error;
      }
    } finally {
      await handle.close();
    }

    // Self-heal: quarantine the unreadable file so it is not re-validated on
    // every access. The entry is reported as a miss.
    await rm(path, { recursive: true, force: true }).catch(() => undefined);
    this.#quarantined += 1;
    this.#onCorruptEntry?.(corruption);
    return undefined;
  }

  async #parseEntry(
    handle: FileHandle,
    path: string,
    includePayload: boolean,
  ): Promise<CacheEntry> {
    // One speculative read covers preamble + header + (for small entries)
    // the whole payload, and tells us the file size for free when it hits
    // EOF — so the common case costs a single read and no fstat.
    const first = await readExactly(handle, 0, SPECULATIVE_READ_BYTES);
    const fileSize =
      first.length < SPECULATIVE_READ_BYTES
        ? first.length
        : (await handle.stat()).size;

    const headerLength = decodePreamble(first, path);
    const payloadOffset = PREAMBLE_LENGTH + headerLength;
    const headerBytes =
      first.length >= payloadOffset
        ? first.subarray(PREAMBLE_LENGTH, payloadOffset)
        : await readExactly(handle, PREAMBLE_LENGTH, headerLength);
    if (headerBytes.length !== headerLength) {
      throw new CacheIntegrityError(path, 'truncated header');
    }
    const metadata = decodeHeader(headerBytes, path);

    if (fileSize !== payloadOffset + metadata.size) {
      // Catches truncation *and* trailing garbage without reading the body.
      throw new CacheIntegrityError(
        path,
        `file size ${fileSize} does not match header (${payloadOffset + metadata.size})`,
      );
    }

    if (!includePayload) {
      return { metadata, value: Buffer.alloc(0) };
    }

    const payloadEnd = payloadOffset + metadata.size;
    const value =
      first.length >= payloadEnd
        ? // Copy out so the entry does not pin the speculative buffer.
          Buffer.from(first.subarray(payloadOffset, payloadEnd))
        : await readExactly(handle, payloadOffset, metadata.size);
    if (value.length !== metadata.size) {
      throw new CacheIntegrityError(path, 'truncated payload');
    }
    if (this.#verifyChecksum && checksumOf(value) !== metadata.checksum) {
      throw new CacheIntegrityError(path, 'checksum mismatch');
    }
    return { metadata, value };
  }

  async #purgeTempFiles(): Promise<void> {
    for await (const file of safeOpendir(this.#tmpDir)) {
      if (file.isFile() && file.name.endsWith(TMP_EXTENSION)) {
        await unlinkIfExists(join(this.#tmpDir, file.name)).catch(
          () => undefined,
        );
      }
    }
  }
}

// --- fs helpers -------------------------------------------------------------

/**
 * Writes every buffer with as few syscalls as possible (`writev`), looping on
 * short writes: a write may legally land fewer bytes than requested, so a
 * single call is not sufficient for a correctness guarantee.
 */
async function writevFully(
  handle: FileHandle,
  buffers: readonly Uint8Array[],
): Promise<void> {
  let pending = buffers.filter((b) => b.byteLength > 0);
  while (pending.length > 0) {
    const { bytesWritten } = await handle.writev(pending);
    if (bytesWritten <= 0) {
      throw new Error('Short write: file handle made no progress');
    }
    pending = dropWritten(pending, bytesWritten);
  }
}

/** Returns the remainder of `buffers` after `count` bytes were consumed. */
function dropWritten(
  buffers: readonly Uint8Array[],
  count: number,
): Uint8Array[] {
  let remaining = count;
  const rest: Uint8Array[] = [];
  for (const buffer of buffers) {
    if (remaining >= buffer.byteLength) {
      remaining -= buffer.byteLength;
    } else {
      rest.push(remaining > 0 ? buffer.subarray(remaining) : buffer);
      remaining = 0;
    }
  }
  return rest;
}

/**
 * Reads up to `length` bytes starting at `position`. Returns fewer bytes only
 * when EOF is hit; callers compare the length to detect truncation.
 */
async function readExactly(
  handle: FileHandle,
  position: number,
  length: number,
): Promise<Buffer> {
  const buffer = Buffer.allocUnsafe(length);
  let offset = 0;
  while (offset < length) {
    const { bytesRead } = await handle.read(
      buffer,
      offset,
      length - offset,
      position + offset,
    );
    if (bytesRead === 0) {
      break;
    }
    offset += bytesRead;
  }
  return offset === length ? buffer : buffer.subarray(0, offset);
}

/**
 * `rm -rf` that tolerates writers still landing files while the tree is
 * being torn down (`ENOTEMPTY`/`EBUSY`), retrying a bounded number of times.
 */
async function removeTree(path: string): Promise<void> {
  const attempts = 5;
  for (let attempt = 1; ; attempt += 1) {
    try {
      await rm(path, { recursive: true, force: true, maxRetries: 2 });
      return;
    } catch (error) {
      if (
        attempt >= attempts ||
        (!isErrno(error, 'ENOTEMPTY') && !isErrno(error, 'EBUSY'))
      ) {
        throw error;
      }
    }
  }
}

/** `fsync` a directory so a completed `rename` survives power loss. */
async function syncDirectory(path: string): Promise<void> {
  let handle: FileHandle;
  try {
    handle = await open(path, 'r');
  } catch {
    // Some platforms (notably Windows) cannot open directories; the rename
    // itself has already succeeded, so degrade gracefully.
    return;
  }
  try {
    await handle.sync();
  } catch (error) {
    if (!isErrno(error, 'EINVAL') && !isErrno(error, 'EPERM')) {
      throw error;
    }
  } finally {
    await handle.close();
  }
}

async function unlinkIfExists(path: string): Promise<boolean> {
  try {
    await unlink(path);
    return true;
  } catch (error) {
    if (isErrno(error, 'ENOENT')) {
      return false;
    }
    throw error;
  }
}

/** `opendir` that yields nothing if the directory does not exist. */
async function* safeOpendir(path: string): AsyncIterable<Dirent> {
  let dir: Dir;
  try {
    dir = await opendir(path);
  } catch (error) {
    if (isErrno(error, 'ENOENT')) {
      return;
    }
    throw error;
  }
  yield* dir;
}

function isErrno(error: unknown, code: string): boolean {
  return (
    typeof error === 'object' &&
    error !== null &&
    'code' in error &&
    error.code === code
  );
}
