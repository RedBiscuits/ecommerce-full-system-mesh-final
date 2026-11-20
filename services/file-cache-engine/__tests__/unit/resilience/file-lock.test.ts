import { readFile, stat, utimes, writeFile } from 'node:fs/promises';
import { join } from 'node:path';

import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import {
  LockLostError,
  LockTimeoutError,
} from '../../../src/resilience/errors';
import { FileLock } from '../../../src/resilience/file-lock';
import { FakeClock } from '../../helpers/fake-clock';
import { makeTempDir, type TempDir } from '../../helpers/temp-dir';

describe('FileLock', () => {
  let dir: TempDir;
  let path: string;

  beforeEach(async () => {
    dir = await makeTempDir();
    path = join(dir.path, 'sweep.lock');
  });

  afterEach(async () => {
    await dir.cleanup();
  });

  it('is exclusive: a second acquirer is refused while the first holds it', async () => {
    const first = await FileLock.tryAcquire(path);
    expect(first).toBeDefined();
    await expect(FileLock.tryAcquire(path)).resolves.toBeUndefined();
    await expect(FileLock.acquire(path)).rejects.toBeInstanceOf(
      LockTimeoutError,
    );

    await first?.release();
    await expect(FileLock.tryAcquire(path)).resolves.toBeDefined();
  });

  it('records the owner and releases idempotently', async () => {
    const lock = await FileLock.acquire(path);
    const record: unknown = JSON.parse(await readFile(path, 'utf8'));
    expect(record).toMatchObject({
      pid: process.pid,
      host: expect.any(String),
    });
    await lock.release();
    await lock.release();
    await expect(stat(path)).rejects.toMatchObject({ code: 'ENOENT' });
  });

  it('waits up to timeoutMs for the holder to release', async () => {
    const holder = await FileLock.acquire(path);
    setTimeout(() => void holder.release(), 60);
    const started = Date.now();
    const lock = await FileLock.acquire(path, {
      timeoutMs: 2_000,
      retryDelayMs: 10,
    });
    expect(Date.now() - started).toBeGreaterThanOrEqual(40);
    await lock.release();
  });

  it('breaks a lock whose owning process is dead on this host', async () => {
    await writeFile(
      path,
      JSON.stringify({
        pid: 2 ** 22 - 1,
        host: (await import('node:os')).hostname(),
        token: 'dead',
        acquiredAt: 0,
      }),
    );
    const lock = await FileLock.tryAcquire(path, { staleAfterMs: 60_000 });
    expect(lock).toBeDefined();
    await lock?.release();
  });

  it('breaks a lock older than staleAfterMs, even with an unreadable record', async () => {
    await writeFile(path, 'garbage');
    const old = new Date(Date.now() - 120_000);
    await utimes(path, old, old);
    const clock = new FakeClock(Date.now());

    expect(
      await FileLock.tryAcquire(path, { staleAfterMs: 300_000, clock }),
    ).toBeUndefined();
    const lock = await FileLock.tryAcquire(path, {
      staleAfterMs: 60_000,
      clock,
    });
    expect(lock).toBeDefined();
    await lock?.release();
  });

  it('refresh keeps a long-running holder from looking stale', async () => {
    const lock = await FileLock.acquire(path);
    const before = (await stat(path)).mtimeMs;
    await new Promise((r) => setTimeout(r, 20));
    await lock.refresh();
    expect((await stat(path)).mtimeMs).toBeGreaterThanOrEqual(before);
    await lock.release();
  });

  it('detects a takeover: refresh throws LockLostError once the lock was broken', async () => {
    const clock = new FakeClock(Date.now());
    const victim = await FileLock.acquire(path, { clock });
    expect(await victim.isHeld()).toBe(true);

    // Simulate the victim stalling long enough to look abandoned.
    const old = new Date(Date.now() - 120_000);
    await utimes(path, old, old);
    const thief = await FileLock.acquire(path, { staleAfterMs: 60_000, clock });

    expect(await victim.isHeld()).toBe(false);
    await expect(victim.refresh()).rejects.toBeInstanceOf(LockLostError);
    await victim.release(); // must not remove the thief's lock
    expect(await thief.isHeld()).toBe(true);
    await thief.release();
  });

  it('does not leave "broken" leftovers behind after a stale break', async () => {
    await writeFile(path, 'garbage');
    const old = new Date(Date.now() - 120_000);
    await utimes(path, old, old);
    const lock = await FileLock.acquire(path, {
      staleAfterMs: 60_000,
      clock: new FakeClock(Date.now()),
    });
    await lock.release();
    const { readdir } = await import('node:fs/promises');
    await expect(readdir(dir.path)).resolves.toEqual([]);
  });

  it('withLock always releases, even when the task throws', async () => {
    await expect(
      FileLock.withLock(path, {}, async () => {
        throw new Error('task failed');
      }),
    ).rejects.toThrow('task failed');
    await expect(stat(path)).rejects.toMatchObject({ code: 'ENOENT' });
  });

  it('survives contention: many racers, exactly one holder at a time', async () => {
    let holders = 0;
    let maxHolders = 0;
    const racers = Array.from({ length: 8 }, async () => {
      const lock = await FileLock.acquire(path, {
        timeoutMs: 5_000,
        retryDelayMs: 2,
      });
      holders += 1;
      maxHolders = Math.max(maxHolders, holders);
      await new Promise((r) => setTimeout(r, 5));
      holders -= 1;
      await lock.release();
    });
    await Promise.all(racers);
    expect(maxHolders).toBe(1);
  });

  it('respects an abort signal while waiting', async () => {
    const holder = await FileLock.acquire(path);
    const controller = new AbortController();
    const pending = FileLock.acquire(path, {
      timeoutMs: 10_000,
      retryDelayMs: 5,
      signal: controller.signal,
    });
    setTimeout(() => controller.abort(new Error('stop waiting')), 15);
    await expect(pending).rejects.toThrow('stop waiting');
    await holder.release();
  });
});
