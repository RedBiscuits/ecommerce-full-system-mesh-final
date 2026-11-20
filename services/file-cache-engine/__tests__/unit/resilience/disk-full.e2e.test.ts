import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import type { ResilienceEvent } from '../../../src/resilience/resilient-store';
import { createFileCache, type FileCache } from '../../../src/main';
import { FakeClock } from '../../helpers/fake-clock';
import { errno } from '../../helpers/faulty-store';
import { makeTempDir, type TempDir } from '../../helpers/temp-dir';

const fsHooks = vi.hoisted(() => ({
  writeErrors: [] as NodeJS.ErrnoException[],
}));

vi.mock('node:fs/promises', async (importOriginal) => {
  const actual = await importOriginal<typeof import('node:fs/promises')>();
  return {
    ...actual,
    open: async (
      ...args: Parameters<typeof actual.open>
    ): ReturnType<typeof actual.open> => {
      const handle = await actual.open(...args);
      const originalWritev = handle.writev.bind(handle);
      // The store lands data with a single writev; inject failures there.
      handle.writev = (async (...writeArgs: unknown[]) => {
        const writeError = fsHooks.writeErrors.shift();
        if (writeError !== undefined) {
          throw writeError;
        }
        return Reflect.apply(originalWritev, handle, writeArgs);
      }) as typeof handle.writev;
      const originalWrite = handle.write.bind(handle);
      handle.write = (async (...writeArgs: unknown[]) => {
        const error = fsHooks.writeErrors.shift();
        if (error !== undefined) {
          throw error;
        }
        return Reflect.apply(originalWrite, handle, writeArgs);
      }) as typeof handle.write;
      return handle;
    },
  };
});

describe('disk full – end to end through createFileCache', () => {
  let dir: TempDir;
  let fileCache: FileCache | undefined;
  let events: ResilienceEvent[];

  beforeEach(async () => {
    dir = await makeTempDir();
    events = [];
  });

  afterEach(async () => {
    fsHooks.writeErrors.length = 0;
    await fileCache?.close();
    fileCache = undefined;
    await dir.cleanup();
  });

  it('reclaims cold entries on ENOSPC and completes the write', async () => {
    const clock = new FakeClock();
    fileCache = await createFileCache({
      directory: dir.path,
      store: { fsync: false },
      sweeper: { batchPauseMs: 0 },
      resilience: { onEvent: (event): number => events.push(event) },
      clock,
      ttlMs: 1_000,
      staleWhileRevalidateMs: 0,
      ttlJitterRatio: 0,
    });
    const { cache, sweeper } = fileCache;
    const sweepSpy = vi.spyOn(sweeper, 'sweepOnce');

    // Fill with entries that will be cold by the time the disk "fills up".
    for (let i = 0; i < 5; i += 1) {
      await cache.put(`cold-${i}`, 'x'.repeat(100));
    }
    clock.advance(2_000);

    fsHooks.writeErrors.push(errno('ENOSPC'));
    const result = await cache.getOrLoad('hot', async () => 'fresh-value');

    expect(result.stored).toBe(true);
    expect(result.value.toString()).toBe('fresh-value');
    expect(sweepSpy).toHaveBeenCalledOnce();
    expect(events).toContainEqual(
      expect.objectContaining({ type: 'capacity-exhausted', reclaimed: true }),
    );
    for (let i = 0; i < 5; i += 1) {
      await expect(cache.head(`cold-${i}`)).resolves.toBeUndefined();
    }
    expect((await cache.get('hot'))?.value.toString()).toBe('fresh-value');
    expect(fileCache.health().store?.lastFailure).toMatchObject({
      kind: 'capacity',
    });
  });

  it('still answers the caller when the disk stays full', async () => {
    fileCache = await createFileCache({
      directory: dir.path,
      store: { fsync: false },
      sweeper: false,
      resilience: {
        onEvent: (event): number => events.push(event),
        reclaimCooldownMs: 0,
      },
      onStoreError: (): void => undefined,
    });
    fsHooks.writeErrors.push(errno('ENOSPC'), errno('ENOSPC'));

    const result = await fileCache.cache.getOrLoad(
      'k',
      async () => 'served-anyway',
    );
    expect(result.stored).toBe(false);
    expect(result.value.toString()).toBe('served-anyway');
    await expect(fileCache.cache.head('k')).resolves.toBeUndefined();

    // An explicit put is allowed to fail loudly.
    fsHooks.writeErrors.push(errno('ENOSPC'), errno('ENOSPC'));
    await expect(fileCache.cache.put('k', 'v')).rejects.toMatchObject({
      kind: 'capacity',
    });
  });
});
