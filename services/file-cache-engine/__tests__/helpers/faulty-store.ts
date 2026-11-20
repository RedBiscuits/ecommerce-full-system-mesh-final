import { checksumOf } from '../../src/cache/cache-key';
import type {
  CacheEntry,
  CacheKey,
  CacheStore,
  EntryAttributes,
  EntryLifetime,
  EntryMetadata,
} from '../../src/cache/types';

export type StoreOp =
  | 'read'
  | 'readMetadata'
  | 'write'
  | 'delete'
  | 'deleteIf'
  | 'clear'
  | 'scan';

/** Builds a Node-style system error with an errno `code`. */
export function errno(code: string, message = code): NodeJS.ErrnoException {
  const error: NodeJS.ErrnoException = new Error(message);
  error.code = code;
  return error;
}

type Fault = { kind: 'throw'; error: unknown } | { kind: 'hang' };

/**
 * In-memory {@link CacheStore} with scripted fault injection. Each operation
 * consumes the next fault queued for it (if any) before doing real work.
 */
export class FaultyStore implements CacheStore {
  readonly entries = new Map<CacheKey, CacheEntry>();
  readonly calls: Record<StoreOp, number> = {
    read: 0,
    readMetadata: 0,
    write: 0,
    delete: 0,
    deleteIf: 0,
    clear: 0,
    scan: 0,
  };
  readonly #faults = new Map<StoreOp, Fault[]>();

  /** Queue `times` throws of `error` for `op`. */
  failNext(op: StoreOp, error: unknown, times = 1): this {
    const queue = this.#faults.get(op) ?? [];
    for (let i = 0; i < times; i += 1) {
      queue.push({ kind: 'throw', error });
    }
    this.#faults.set(op, queue);
    return this;
  }

  /** Queue `times` never-settling calls for `op`. */
  hangNext(op: StoreOp, times = 1): this {
    const queue = this.#faults.get(op) ?? [];
    for (let i = 0; i < times; i += 1) {
      queue.push({ kind: 'hang' });
    }
    this.#faults.set(op, queue);
    return this;
  }

  pendingFaults(op: StoreOp): number {
    return this.#faults.get(op)?.length ?? 0;
  }

  async read(key: CacheKey): Promise<CacheEntry | undefined> {
    await this.#gate('read');
    return this.entries.get(key);
  }

  async readMetadata(key: CacheKey): Promise<EntryMetadata | undefined> {
    await this.#gate('readMetadata');
    return this.entries.get(key)?.metadata;
  }

  async write(
    key: CacheKey,
    value: Uint8Array,
    lifetime: EntryLifetime,
    attributes: EntryAttributes = {},
  ): Promise<EntryMetadata> {
    await this.#gate('write');
    const metadata: EntryMetadata = {
      key,
      ...lifetime,
      size: value.byteLength,
      checksum: checksumOf(value),
      attributes: Object.freeze({ ...attributes }),
    };
    this.entries.set(key, { metadata, value: Buffer.from(value) });
    return metadata;
  }

  async delete(key: CacheKey): Promise<boolean> {
    await this.#gate('delete');
    return this.entries.delete(key);
  }

  async deleteIf(
    key: CacheKey,
    predicate: (metadata: EntryMetadata) => boolean,
  ): Promise<boolean> {
    await this.#gate('deleteIf');
    const entry = this.entries.get(key);
    if (entry === undefined || !predicate(entry.metadata)) {
      return false;
    }
    return this.entries.delete(key);
  }

  async clear(): Promise<void> {
    await this.#gate('clear');
    this.entries.clear();
  }

  async *scan(): AsyncIterable<EntryMetadata> {
    await this.#gate('scan');
    const snapshot = Array.from(this.entries.values());
    for (const entry of snapshot) {
      yield entry.metadata;
    }
  }

  async #gate(op: StoreOp): Promise<void> {
    this.calls[op] += 1;
    const fault = this.#faults.get(op)?.shift();
    if (fault === undefined) {
      return;
    }
    if (fault.kind === 'hang') {
      await new Promise<never>(() => undefined);
      return;
    }
    throw fault.error;
  }
}
