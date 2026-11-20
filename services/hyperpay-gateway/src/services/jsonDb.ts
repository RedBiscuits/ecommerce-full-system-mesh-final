/**
 * src/services/jsonDb.ts
 *
 * Lightweight, generic JSON-file database.
 *
 * The DB file path is resolved once at module load from `config.dbPath`
 * (which reads `process.env.DB_PATH`).  Set DB_PATH to a temp file in tests
 * for full isolation — `tests/helpers/jestSetup.ts` does this for you.
 *
 * Collections are stored as arrays inside a single JSON file:
 *   { "payments": [ {..., _id, _createdAt}, ... ], "other": [...] }
 *
 * Consistency guarantees (within a single process):
 *   - Every operation runs under an async mutex, so read-modify-write
 *     sequences (e.g. "find existing, else insert") never interleave.
 *   - Writes are atomic at the filesystem level (write temp file → rename),
 *     so a crash mid-write can never leave a truncated/corrupt db.json.
 *
 * ─── Transactions ────────────────────────────────────────────────────────────
 *
 * `db.transaction(work)` loads the file once, hands `work` a handle whose
 * reads and writes operate on that in-memory snapshot, and — only if `work`
 * resolves — writes the whole snapshot back in one atomic rename. Either
 * every change in the transaction lands on disk or none does:
 *
 *   await db.transaction(async (tx) => {
 *     const payment = await tx.insert('payments', record);
 *     await tx.insert('outbox_events', eventFor(payment));   // same commit
 *   });
 *
 * This is what makes the transactional outbox possible on a flat file: the
 * state change and the event describing it are a single write, so there is
 * no window in which one exists without the other. Rollback is free — a
 * throwing `work` simply discards the snapshot. `tx.onCommit(fn)` runs `fn`
 * after the rename succeeded (used to nudge the outbox publisher).
 *
 * The mutex is held for the duration of `work`, so `work` must not call the
 * top-level `db.*` API (that would wait for the lock it already holds). The
 * handle detects this and throws instead of deadlocking.
 */

import fs from 'fs/promises';
import path from 'path';
import crypto from 'crypto';
import { AsyncLocalStorage } from 'async_hooks';
import { config } from '../config/env';

// ─── Types ────────────────────────────────────────────────────────────────────

/** Fields injected automatically on every insert. */
export interface DbRecord {
  _id: string;
  _createdAt: string; // ISO-8601
  /** Set on every `update`; absent on records that were never modified. */
  _updatedAt?: string; // ISO-8601
}

export type Predicate<T extends DbRecord> = (record: T) => boolean;

type DbFile = Record<string, DbRecord[]>;

/**
 * The read/write surface shared by the top-level `db` and a transaction
 * handle. Code that must work inside *or* outside a transaction (the vault,
 * the outbox) is written against this interface.
 */
export interface DbAccess {
  insert<T extends object>(collection: string, data: T): Promise<T & DbRecord>;
  update<T extends DbRecord>(
    collection: string,
    id: string,
    patch: Partial<Omit<T, keyof DbRecord>>,
  ): Promise<T | null>;
  /** Delete every record matching the predicate; resolves with the number removed. */
  remove<T extends DbRecord>(collection: string, predicate: Predicate<T>): Promise<number>;
  findAll<T extends DbRecord>(collection: string): Promise<T[]>;
  findById<T extends DbRecord>(collection: string, id: string): Promise<T | null>;
  findWhere<T extends DbRecord>(collection: string, predicate: Predicate<T>): Promise<T[]>;
  findOne<T extends DbRecord>(collection: string, predicate: Predicate<T>): Promise<T | null>;
}

/** Handle passed to `db.transaction()`. Reads see the transaction's own writes. */
export interface DbTransaction extends DbAccess {
  /**
   * Run `callback` once the transaction has been durably committed. Never
   * runs if the transaction is rolled back. Errors thrown by the callback
   * are logged, not propagated — the commit has already happened.
   */
  onCommit(callback: () => void): void;
}

export type TransactionWork<T> = (tx: DbTransaction) => Promise<T> | T;

// ─── Internal helpers ─────────────────────────────────────────────────────────

function resolvedPath(): string {
  return path.isAbsolute(config.dbPath)
    ? config.dbPath
    : path.resolve(process.cwd(), config.dbPath);
}

async function readFile(): Promise<DbFile> {
  try {
    const raw = await fs.readFile(resolvedPath(), 'utf-8');
    return JSON.parse(raw) as DbFile;
  } catch (err: unknown) {
    if ((err as NodeJS.ErrnoException).code === 'ENOENT') return {};
    throw err;
  }
}

/** Atomic write: data lands in a temp file first, then is renamed into place. */
async function writeFile(data: DbFile): Promise<void> {
  const target = resolvedPath();
  const tmp = `${target}.${process.pid}.${crypto.randomUUID()}.tmp`;
  try {
    await fs.writeFile(tmp, JSON.stringify(data, null, 2), 'utf-8');
    await fs.rename(tmp, target);
  } catch (err) {
    await fs.rm(tmp, { force: true });
    throw err;
  }
}

/**
 * Minimal async mutex. Operations are chained onto a single promise so they
 * execute strictly one after another, regardless of how many callers there
 * are. A failing operation does not block the ones queued behind it.
 */
let chain: Promise<unknown> = Promise.resolve();

/**
 * Marks the async context of a running transaction so a nested call to the
 * top-level API (which would wait forever for the lock it already holds) can
 * be reported as a bug instead of deadlocking the process.
 */
const transactionContext = new AsyncLocalStorage<true>();

function withLock<T>(operation: () => Promise<T>): Promise<T> {
  if (transactionContext.getStore()) {
    return Promise.reject(
      new Error('jsonDb: db.* called inside db.transaction() — use the transaction handle (tx.*) instead'),
    );
  }
  const run = chain.then(operation, operation);
  chain = run.catch(() => undefined);
  return run;
}

// ─── Transaction handle ───────────────────────────────────────────────────────

/**
 * Pure, in-memory mutations over a loaded snapshot. Everything resolves
 * immediately; the promises exist only so the handle satisfies `DbAccess`.
 */
interface TransactionState {
  tx: DbTransaction;
  isDirty: () => boolean;
  commitHooks: Array<() => void>;
}

function makeTransaction(file: DbFile): TransactionState {
  let dirty = false;
  const commitHooks: Array<() => void> = [];

  const records = <T extends DbRecord>(collection: string): T[] => (file[collection] ?? []) as T[];

  const tx: DbTransaction = {
    insert<T extends object>(collection: string, data: T): Promise<T & DbRecord> {
      const target = (file[collection] ??= []);
      const record: T & DbRecord = {
        ...data,
        _id: crypto.randomUUID(),
        _createdAt: new Date().toISOString(),
      };
      target.push(record);
      dirty = true;
      return Promise.resolve(record);
    },

    update<T extends DbRecord>(collection: string, id: string, patch: Partial<Omit<T, keyof DbRecord>>): Promise<T | null> {
      const target = records<T>(collection);
      const index = target.findIndex((r) => r._id === id);
      if (index === -1) return Promise.resolve(null);

      const current = target[index];
      const updated: T = {
        ...current,
        ...patch,
        _id: current._id,
        _createdAt: current._createdAt,
        _updatedAt: new Date().toISOString(),
      };
      target[index] = updated;
      dirty = true;
      return Promise.resolve(updated);
    },

    remove<T extends DbRecord>(collection: string, predicate: Predicate<T>): Promise<number> {
      const target = records<T>(collection);
      const kept = target.filter((r) => !predicate(r));
      const removed = target.length - kept.length;
      if (removed > 0) {
        file[collection] = kept;
        dirty = true;
      }
      return Promise.resolve(removed);
    },

    findAll<T extends DbRecord>(collection: string): Promise<T[]> {
      return Promise.resolve([...records<T>(collection)]);
    },

    findById<T extends DbRecord>(collection: string, id: string): Promise<T | null> {
      return Promise.resolve(records<T>(collection).find((r) => r._id === id) ?? null);
    },

    findWhere<T extends DbRecord>(collection: string, predicate: Predicate<T>): Promise<T[]> {
      return Promise.resolve(records<T>(collection).filter(predicate));
    },

    findOne<T extends DbRecord>(collection: string, predicate: Predicate<T>): Promise<T | null> {
      return Promise.resolve(records<T>(collection).find(predicate) ?? null);
    },

    onCommit(callback: () => void): void {
      commitHooks.push(callback);
    },
  };

  return { tx, isDirty: (): boolean => dirty, commitHooks };
}

// ─── Public API ───────────────────────────────────────────────────────────────

/**
 * Run `work` against a consistent snapshot and commit all of its writes in
 * one atomic file write. If `work` throws, nothing is written. Read-only
 * transactions do not touch the disk.
 */
function transaction<T>(work: TransactionWork<T>): Promise<T> {
  return withLock(async () => {
    const file = await readFile();
    const { tx, isDirty, commitHooks } = makeTransaction(file);

    const result = await transactionContext.run(true, async () => work(tx));

    if (isDirty()) await writeFile(file);

    for (const hook of commitHooks) {
      try {
        hook();
      } catch (err) {
        console.error('[JsonDb] onCommit hook failed:', err);
      }
    }
    return result;
  });
}

/** Insert a document; returns the persisted record (data + _id + _createdAt). */
function insert<T extends object>(collection: string, data: T): Promise<T & DbRecord> {
  return transaction((tx) => tx.insert(collection, data));
}

/**
 * Shallow-merge `patch` into the record with the given `_id`.
 * Returns the updated record, or `null` if no such record exists.
 * `_id` and `_createdAt` are immutable; `_updatedAt` is stamped automatically.
 */
function update<T extends DbRecord>(
  collection: string,
  id: string,
  patch: Partial<Omit<T, keyof DbRecord>>,
): Promise<T | null> {
  return transaction((tx) => tx.update<T>(collection, id, patch));
}

/** Delete every record matching the predicate; returns how many were removed. */
function remove<T extends DbRecord>(collection: string, predicate: Predicate<T>): Promise<number> {
  return transaction((tx) => tx.remove<T>(collection, predicate));
}

/** Return all documents in a collection (empty array if none). */
function findAll<T extends DbRecord>(collection: string): Promise<T[]> {
  return withLock(async () => {
    const file = await readFile();
    return (file[collection] ?? []) as T[];
  });
}

/** Find a single document by its auto-generated _id. */
async function findById<T extends DbRecord>(
  collection: string,
  id: string,
): Promise<T | null> {
  const records = await findAll<T>(collection);
  return records.find((r) => r._id === id) ?? null;
}

/** Find all documents satisfying the predicate. */
async function findWhere<T extends DbRecord>(
  collection: string,
  predicate: Predicate<T>,
): Promise<T[]> {
  const records = await findAll<T>(collection);
  return records.filter(predicate);
}

/** Find the first document satisfying the predicate, or `null`. */
async function findOne<T extends DbRecord>(
  collection: string,
  predicate: Predicate<T>,
): Promise<T | null> {
  const records = await findAll<T>(collection);
  return records.find(predicate) ?? null;
}

/** Clear a collection (useful in tests). */
function clear(collection: string): Promise<void> {
  return withLock(async () => {
    const file = await readFile();
    file[collection] = [];
    await writeFile(file);
  });
}

export const db = { insert, update, remove, findAll, findById, findWhere, findOne, clear, transaction };

/** Shape of the top-level API — `DbAccess` plus `clear` and `transaction`. */
export type Db = typeof db;
