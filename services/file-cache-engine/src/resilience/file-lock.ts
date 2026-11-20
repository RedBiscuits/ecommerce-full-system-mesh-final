import { randomBytes } from 'node:crypto';
import type { FileHandle } from 'node:fs/promises';
import { open, readFile, rename, stat, unlink, utimes } from 'node:fs/promises';
import { hostname } from 'node:os';

import type { Clock } from '../shared/clock';
import { systemClock } from '../shared/clock';
import { sleep } from '../shared/sleep';
import { isErrno, LockLostError, LockTimeoutError } from './errors';

export interface FileLockOptions {
  /**
   * A lock whose file is older than this is considered abandoned (holder
   * crashed without releasing) and may be broken. Holders that run longer
   * must call `refresh()` periodically. Default: 60 s.
   */
  readonly staleAfterMs?: number;
  /** How long to keep trying before giving up. Default: `0` (single attempt). */
  readonly timeoutMs?: number;
  /** Pause between attempts. Default: 50 ms. */
  readonly retryDelayMs?: number;
  readonly clock?: Clock;
  readonly signal?: AbortSignal;
}

interface LockRecord {
  readonly pid: number;
  readonly host: string;
  readonly token: string;
  readonly acquiredAt: number;
}

type Liveness = 'held' | 'stale' | 'gone';

/**
 * Cross-process advisory lock backed by an `O_EXCL` lock file.
 *
 * Coordinates processes that share one cache directory (cluster workers,
 * sidecars) for operations that must not overlap, such as sweeping.
 *
 * Recovery from abandoned locks (holder crashed without releasing):
 * - a lock whose owning PID is dead on this host is broken immediately;
 * - otherwise a lock older than `staleAfterMs` is broken. Holders that run
 *   longer must `refresh()` periodically.
 *
 * Breaking is done by *renaming* the stale file aside and re-checking it,
 * so two breakers cannot both remove a lock that a third party just
 * re-created. Every record carries a random token; `refresh()` verifies it
 * and throws {@link LockLostError} if the lock was taken over, letting a
 * holder stop gracefully instead of working unprotected.
 */
export class FileLock {
  readonly #path: string;
  readonly #token: string;
  #released = false;

  private constructor(path: string, token: string) {
    this.#path = path;
    this.#token = token;
  }

  get path(): string {
    return this.#path;
  }

  /** Attempts once (or until `timeoutMs`); resolves `undefined` when busy. */
  static async tryAcquire(
    path: string,
    options: FileLockOptions = {},
  ): Promise<FileLock | undefined> {
    const clock = options.clock ?? systemClock;
    const staleAfterMs = options.staleAfterMs ?? 60_000;
    const timeoutMs = options.timeoutMs ?? 0;
    const retryDelayMs = options.retryDelayMs ?? 50;
    const deadline = clock.now() + timeoutMs;

    for (;;) {
      options.signal?.throwIfAborted();
      const token = randomBytes(8).toString('hex');
      if (await FileLock.#create(path, token, clock)) {
        return new FileLock(path, token);
      }
      const liveness = await FileLock.#liveness(path, staleAfterMs, clock);
      if (liveness === 'gone') {
        continue; // released between our attempts: just try again
      }
      if (liveness === 'stale') {
        await FileLock.#break(path, staleAfterMs, clock);
        continue;
      }
      if (clock.now() >= deadline) {
        return undefined;
      }
      await sleep(retryDelayMs, { signal: options.signal });
    }
  }

  /** Like {@link tryAcquire} but throws {@link LockTimeoutError} when busy. */
  static async acquire(
    path: string,
    options: FileLockOptions = {},
  ): Promise<FileLock> {
    const lock = await FileLock.tryAcquire(path, options);
    if (lock === undefined) {
      throw new LockTimeoutError(path, options.timeoutMs ?? 0);
    }
    return lock;
  }

  /**
   * Touches the lock so long-running holders are not mistaken for dead.
   * @throws {LockLostError} if the lock was broken and re-acquired by
   *   someone else in the meantime.
   */
  async refresh(): Promise<void> {
    if (this.#released) {
      throw new LockLostError(this.#path);
    }
    if (!(await this.isHeld())) {
      this.#released = true;
      throw new LockLostError(this.#path);
    }
    const now = new Date();
    await utimes(this.#path, now, now);
  }

  /** Whether the lock file still carries this holder's token. */
  async isHeld(): Promise<boolean> {
    if (this.#released) {
      return false;
    }
    const record = await FileLock.#readRecord(this.#path);
    return record?.token === this.#token;
  }

  /** Idempotent. */
  async release(): Promise<void> {
    if (this.#released) {
      return;
    }
    this.#released = true;
    await unlink(this.#path).catch((error: unknown) => {
      if (!isErrno(error, 'ENOENT')) {
        throw error;
      }
    });
  }

  /** Runs `task` under the lock, releasing it afterwards. */
  static async withLock<T>(
    path: string,
    options: FileLockOptions,
    task: (lock: FileLock) => Promise<T>,
  ): Promise<T> {
    const lock = await FileLock.acquire(path, options);
    try {
      return await task(lock);
    } finally {
      await lock.release();
    }
  }

  // --- internals ----------------------------------------------------------

  static async #create(
    path: string,
    token: string,
    clock: Clock,
  ): Promise<boolean> {
    let handle: FileHandle;
    try {
      handle = await open(path, 'wx', 0o600);
    } catch (error) {
      if (isErrno(error, 'EEXIST')) {
        return false;
      }
      throw error;
    }
    try {
      const record: LockRecord = {
        pid: process.pid,
        host: hostname(),
        token,
        acquiredAt: clock.now(),
      };
      await handle.writeFile(JSON.stringify(record), 'utf8');
      return true;
    } finally {
      await handle.close();
    }
  }

  static async #readRecord(path: string): Promise<LockRecord | undefined> {
    try {
      const parsed: unknown = JSON.parse(await readFile(path, 'utf8'));
      if (
        typeof parsed === 'object' &&
        parsed !== null &&
        'pid' in parsed &&
        typeof parsed.pid === 'number' &&
        'host' in parsed &&
        typeof parsed.host === 'string' &&
        'token' in parsed &&
        typeof parsed.token === 'string'
      ) {
        return {
          pid: parsed.pid,
          host: parsed.host,
          token: parsed.token,
          acquiredAt: 0,
        };
      }
    } catch {
      // Unreadable or half-written: caller falls back to the age check.
    }
    return undefined;
  }

  static async #liveness(
    path: string,
    staleAfterMs: number,
    clock: Clock,
  ): Promise<Liveness> {
    const record = await FileLock.#readRecord(path);
    if (
      record !== undefined &&
      record.host === hostname() &&
      !isProcessAlive(record.pid)
    ) {
      return 'stale';
    }
    try {
      const { mtimeMs } = await stat(path);
      return clock.now() - mtimeMs >= staleAfterMs ? 'stale' : 'held';
    } catch (error) {
      if (isErrno(error, 'ENOENT')) {
        return 'gone';
      }
      throw error;
    }
  }

  /**
   * Moves the stale file aside (only one breaker can win the rename), then
   * re-checks the moved file: if it is in fact fresh — a new holder slipped
   * in between our check and the rename — it is put back.
   */
  static async #break(
    path: string,
    staleAfterMs: number,
    clock: Clock,
  ): Promise<void> {
    const aside = `${path}.broken-${randomBytes(4).toString('hex')}`;
    try {
      await rename(path, aside);
    } catch (error) {
      if (isErrno(error, 'ENOENT')) {
        return; // someone else already broke it
      }
      throw error;
    }
    const liveness = await FileLock.#liveness(aside, staleAfterMs, clock);
    if (liveness === 'held') {
      await rename(aside, path).catch(() => undefined);
      return;
    }
    await unlink(aside).catch(() => undefined);
  }
}

function isProcessAlive(pid: number): boolean {
  if (pid === process.pid) {
    return true;
  }
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    // EPERM: exists but owned by another user → alive.
    return isErrno(error, 'EPERM');
  }
}
