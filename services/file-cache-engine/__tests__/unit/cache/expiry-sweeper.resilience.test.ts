import { writeFile } from 'node:fs/promises';
import { join } from 'node:path';

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { ExpirySweeper } from '../../../src/cache/expiry-sweeper';
import { FsCacheStore } from '../../../src/cache/fs-cache-store';
import { FileLock } from '../../../src/resilience/file-lock';
import { FakeClock } from '../../helpers/fake-clock';
import { FaultyStore, errno } from '../../helpers/faulty-store';
import { makeTempDir, type TempDir } from '../../helpers/temp-dir';

describe('ExpirySweeper – fault tolerance', () => {
  let clock: FakeClock;

  beforeEach(() => {
    clock = new FakeClock();
  });

  it('counts failed deletions and keeps sweeping', async () => {
    const store = new FaultyStore();
    for (const key of ['a', 'b', 'c']) {
      await store.write(key, Buffer.from('x'), {
        createdAt: 0,
        expiresAt: 1,
        staleUntil: 2,
      });
    }
    store.failNext('deleteIf', errno('EIO'));
    const onError = vi.fn<(error: unknown) => void>();
    const sweeper = new ExpirySweeper(store, {
      clock,
      batchPauseMs: 0,
      onError,
    });

    const report = await sweeper.sweepOnce();
    expect(report).toMatchObject({
      scanned: 3,
      expired: 2,
      failed: 1,
      skipped: false,
    });
    expect(onError).toHaveBeenCalledWith(
      expect.objectContaining({ code: 'EIO' }),
    );
    expect(store.entries.size).toBe(1);
  });

  it('re-evaluates coldness at delete time so a rewritten entry survives', async () => {
    const store = new FaultyStore();
    await store.write('hot-again', Buffer.from('x'), {
      createdAt: 0,
      expiresAt: 1,
      staleUntil: 2,
    });
    // Between scan and delete, a writer refreshes the entry.
    const originalScan = store.scan.bind(store);
    store.scan = async function* (): AsyncIterable<
      import('../../../src/cache/types').EntryMetadata
    > {
      for await (const metadata of originalScan()) {
        await store.write(metadata.key, Buffer.from('fresh'), {
          createdAt: clock.now(),
          expiresAt: clock.now() + 1e6,
          staleUntil: clock.now() + 1e6,
        });
        yield metadata;
      }
    };
    const sweeper = new ExpirySweeper(store, { clock, batchPauseMs: 0 });
    const report = await sweeper.sweepOnce();
    expect(report.expired).toBe(0);
    expect(store.entries.get('hot-again')?.value.toString()).toBe('fresh');
  });

  it('evicts for the byte budget only when the entry is unchanged since the scan', async () => {
    const store = new FaultyStore();
    await store.write('big', Buffer.alloc(100), {
      createdAt: 0,
      expiresAt: 1e6,
      staleUntil: 1e6,
    });
    const originalScan = store.scan.bind(store);
    store.scan = async function* (): AsyncIterable<
      import('../../../src/cache/types').EntryMetadata
    > {
      for await (const metadata of originalScan()) {
        yield metadata;
        await store.write('big', Buffer.alloc(100, 1), {
          createdAt: 5,
          expiresAt: 1e6,
          staleUntil: 1e6,
        });
      }
    };
    const sweeper = new ExpirySweeper(store, {
      clock,
      batchPauseMs: 0,
      maxTotalBytes: 10,
    });
    const report = await sweeper.sweepOnce();
    expect(report.evicted).toBe(0);
    expect(store.entries.has('big')).toBe(true);
  });

  it('evicts entries created implausibly far in the future', async () => {
    const store = new FaultyStore();
    await store.write('from-the-future', Buffer.from('x'), {
      createdAt: clock.now() + 3_600_000,
      expiresAt: clock.now() + 7_200_000,
      staleUntil: clock.now() + 7_200_000,
    });
    const sweeper = new ExpirySweeper(store, {
      clock,
      batchPauseMs: 0,
      maxFutureSkewMs: 60_000,
    });
    expect((await sweeper.sweepOnce()).expired).toBe(1);
  });

  it('a scan failure surfaces through onError and the timer keeps going', async () => {
    vi.useFakeTimers();
    const store = new FaultyStore();
    store.failNext('scan', errno('EIO'));
    const onError = vi.fn<(error: unknown) => void>();
    const sweeper = new ExpirySweeper(store, {
      clock,
      intervalMs: 10,
      intervalJitterRatio: 0,
      onError,
    });
    sweeper.start();
    await vi.advanceTimersByTimeAsync(10);
    await vi.waitFor(() => expect(onError).toHaveBeenCalledOnce());
    expect(sweeper.isRunning).toBe(true);
    await sweeper.stop();
    vi.useRealTimers();
  });

  describe('cross-process lock', () => {
    let dir: TempDir;

    beforeEach(async () => {
      dir = await makeTempDir();
    });

    afterEach(async () => {
      await dir.cleanup();
    });

    it('skips the sweep while another process holds the lock', async () => {
      const store = await FsCacheStore.open({
        directory: dir.path,
        fsync: false,
      });
      await store.write('cold', Buffer.from('x'), {
        createdAt: 0,
        expiresAt: 1,
        staleUntil: 2,
      });
      const lockPath = join(dir.path, 'sweep.lock');
      const other = await FileLock.acquire(lockPath);
      const sweeper = new ExpirySweeper(store, {
        clock,
        batchPauseMs: 0,
        lock: { path: lockPath },
      });

      expect(await sweeper.sweepOnce()).toMatchObject({
        skipped: true,
        scanned: 0,
      });
      await expect(store.readMetadata('cold')).resolves.toBeDefined();

      await other.release();
      expect(await sweeper.sweepOnce()).toMatchObject({
        skipped: false,
        expired: 1,
      });
      await expect(FileLock.tryAcquire(lockPath)).resolves.toBeDefined(); // released after sweep
    });

    it('breaks an abandoned lock and proceeds', async () => {
      const store = await FsCacheStore.open({
        directory: dir.path,
        fsync: false,
      });
      await store.write('cold', Buffer.from('x'), {
        createdAt: 0,
        expiresAt: 1,
        staleUntil: 2,
      });
      const lockPath = join(dir.path, 'sweep.lock');
      await writeFile(lockPath, 'left behind by a crashed worker');
      const { utimes } = await import('node:fs/promises');
      const old = new Date(Date.now() - 600_000);
      await utimes(lockPath, old, old);

      const sweeper = new ExpirySweeper(store, {
        clock: new FakeClock(Date.now()),
        batchPauseMs: 0,
        lock: { path: lockPath, staleAfterMs: 60_000 },
      });
      expect(await sweeper.sweepOnce()).toMatchObject({
        skipped: false,
        expired: 1,
      });
    });

    it('stops the sweep when the lock is taken over mid-way', async () => {
      const store = new FaultyStore();
      for (let i = 0; i < 6; i += 1) {
        await store.write(`k${i}`, Buffer.from('x'), {
          createdAt: 0,
          expiresAt: 1,
          staleUntil: 2,
        });
      }
      const lockPath = join(dir.path, 'sweep.lock');
      const sweeper = new ExpirySweeper(store, {
        clock,
        batchSize: 2,
        batchPauseMs: 0,
        lock: { path: lockPath },
        onError: (): void => undefined,
      });
      // Steal the lock as soon as the first batch pause happens.
      const originalDeleteIf = store.deleteIf.bind(store);
      let stolen = false;
      store.deleteIf = async (key, predicate): Promise<boolean> => {
        const removed = await originalDeleteIf(key, predicate);
        if (!stolen) {
          stolen = true;
          await writeFile(
            lockPath,
            JSON.stringify({
              pid: 1,
              host: 'elsewhere',
              token: 'thief',
              acquiredAt: 0,
            }),
          );
        }
        return removed;
      };
      await expect(sweeper.sweepOnce()).rejects.toThrow(/no longer held/);
      expect(store.entries.size).toBeGreaterThan(0); // stopped early
    });
  });
});
