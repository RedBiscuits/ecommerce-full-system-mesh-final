import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { ExpirySweeper } from '../../../src/cache/expiry-sweeper';
import { FsCacheStore } from '../../../src/cache/fs-cache-store';
import type { EntryMetadata } from '../../../src/cache/types';
import { FakeClock } from '../../helpers/fake-clock';
import { makeTempDir, type TempDir } from '../../helpers/temp-dir';

describe('ExpirySweeper', () => {
  let dir: TempDir;
  let store: FsCacheStore;
  let clock: FakeClock;

  const write = (
    key: string,
    expiresIn: number,
    swr = 0,
    size = 1,
  ): Promise<EntryMetadata> =>
    store.write(key, Buffer.alloc(size), {
      createdAt: clock.now(),
      expiresAt: clock.now() + expiresIn,
      staleUntil: clock.now() + expiresIn + swr,
    });

  const keys = async (): Promise<string[]> => {
    const out: string[] = [];
    for await (const m of store.scan()) {
      out.push(m.key);
    }
    return out.sort();
  };

  beforeEach(async () => {
    dir = await makeTempDir();
    store = await FsCacheStore.open({ directory: dir.path, fsync: false });
    clock = new FakeClock();
  });

  afterEach(async () => {
    vi.useRealTimers();
    await dir.cleanup();
  });

  it('removes only entries past their stale window', async () => {
    await write('fresh', 1_000);
    await write('stale-but-servable', -10, 1_000);
    await write('expired', -10);
    const sweeper = new ExpirySweeper(store, { clock, batchPauseMs: 0 });

    const report = await sweeper.sweepOnce();

    expect(report).toMatchObject({ scanned: 3, expired: 1, evicted: 0 });
    await expect(keys()).resolves.toEqual(['fresh', 'stale-but-servable']);
  });

  it('evicts soonest-to-expire entries when over the byte budget', async () => {
    await write('expires-late', 3_000, 0, 10);
    await write('expires-soon', 1_000, 0, 10);
    await write('expires-mid', 2_000, 0, 10);
    const sweeper = new ExpirySweeper(store, {
      clock,
      batchPauseMs: 0,
      maxTotalBytes: 15,
    });

    const report = await sweeper.sweepOnce();

    expect(report).toMatchObject({ expired: 0, evicted: 2, liveBytes: 10 });
    await expect(keys()).resolves.toEqual(['expires-late']);
  });

  it('throttles deletions into paused batches instead of storming the disk', async () => {
    for (let i = 0; i < 5; i += 1) {
      await write(`cold-${i}`, -10);
    }
    const deleteSpy = vi.spyOn(store, 'deleteIf');
    vi.useFakeTimers();
    const sweeper = new ExpirySweeper(store, {
      clock,
      batchSize: 2,
      batchPauseMs: 100,
    });

    const sweep = sweeper.sweepOnce();
    await vi.waitFor(() => expect(deleteSpy).toHaveBeenCalledTimes(2));
    // Nothing more happens until the pause elapses.
    await vi.advanceTimersByTimeAsync(99);
    expect(deleteSpy).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(1);
    await vi.waitFor(() => expect(deleteSpy).toHaveBeenCalledTimes(4));
    await vi.advanceTimersByTimeAsync(100);

    const report = await sweep;
    expect(report.expired).toBe(5);
  }, 15_000);

  it('runs periodically once started and can be stopped', async () => {
    vi.useFakeTimers();
    const sweepSpy = vi.spyOn(store, 'scan');
    const sweeper = new ExpirySweeper(store, {
      clock,
      intervalMs: 100,
      intervalJitterRatio: 0,
      batchPauseMs: 0,
    });

    sweeper.start();
    sweeper.start(); // idempotent
    expect(sweeper.isRunning).toBe(true);
    expect(sweepSpy).not.toHaveBeenCalled();

    // Each sweep performs real fs I/O, so wait for completion rather than
    // assuming timer-only progress (`vi.waitFor` advances fake timers).
    await vi.waitFor(() => expect(sweepSpy).toHaveBeenCalledTimes(1));
    await vi.waitFor(() => expect(sweepSpy).toHaveBeenCalledTimes(2));

    await sweeper.stop();
    expect(sweeper.isRunning).toBe(false);
    const callsAtStop = sweepSpy.mock.calls.length;
    await vi.advanceTimersByTimeAsync(5_000);
    expect(sweepSpy).toHaveBeenCalledTimes(callsAtStop);
  });

  it('never overlaps sweeps: concurrent sweepOnce calls share one run', async () => {
    await write('expired', -10);
    const scanSpy = vi.spyOn(store, 'scan');
    const sweeper = new ExpirySweeper(store, { clock, batchPauseMs: 0 });

    const [a, b] = await Promise.all([
      sweeper.sweepOnce(),
      sweeper.sweepOnce(),
    ]);
    expect(a).toBe(b);
    expect(scanSpy).toHaveBeenCalledTimes(1);
  });

  it('reports scheduled-sweep failures through onError and keeps going', async () => {
    vi.useFakeTimers();
    const onError = vi.fn<(error: unknown) => void>();
    vi.spyOn(store, 'scan').mockImplementationOnce(() => {
      throw new Error('disk gone');
    });
    const sweeper = new ExpirySweeper(store, {
      clock,
      intervalMs: 10,
      intervalJitterRatio: 0,
      onError,
    });

    sweeper.start();
    await vi.advanceTimersByTimeAsync(10);
    expect(onError).toHaveBeenCalledOnce();
    await vi.advanceTimersByTimeAsync(10);
    expect(sweeper.isRunning).toBe(true);
    await sweeper.stop();
  });
});
