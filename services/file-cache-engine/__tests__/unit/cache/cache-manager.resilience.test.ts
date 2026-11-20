import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import type {
  CacheManagerOptions,
  Loader,
} from '../../../src/cache/cache-manager';
import { CacheManager } from '../../../src/cache/cache-manager';
import { LoaderTimeoutError } from '../../../src/resilience/errors';
import { FakeClock } from '../../helpers/fake-clock';
import { FaultyStore, errno } from '../../helpers/faulty-store';

describe('CacheManager – degradation under store failures', () => {
  let store: FaultyStore;
  let clock: FakeClock;
  let onStoreError: ReturnType<
    typeof vi.fn<NonNullable<CacheManagerOptions['onStoreError']>>
  >;
  let cache: CacheManager;

  beforeEach(() => {
    store = new FaultyStore();
    clock = new FakeClock();
    onStoreError = vi.fn<NonNullable<CacheManagerOptions['onStoreError']>>();
    cache = new CacheManager(store, {
      clock,
      ttlMs: 10_000,
      staleWhileRevalidateMs: 0,
      ttlJitterRatio: 0,
      onStoreError,
    });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('get/head report read failures and degrade to a miss', async () => {
    await cache.put('k', 'v');
    store.failNext('read', errno('EIO'));
    store.failNext('readMetadata', errno('EIO'));

    await expect(cache.get('k')).resolves.toBeUndefined();
    await expect(cache.head('k')).resolves.toBeUndefined();
    expect(onStoreError).toHaveBeenCalledTimes(2);
    expect(onStoreError.mock.calls[0]?.[1]).toEqual({
      key: 'k',
      operation: 'read',
    });

    // Healthy again: the entry was never touched.
    expect((await cache.get('k'))?.value.toString()).toBe('v');
  });

  it('getOrLoad treats a failed read as a miss and still serves the loader', async () => {
    await cache.put('k', 'cached');
    store.failNext('read', errno('EIO'), 2); // initial read + double-check
    const loader = vi.fn<Loader>(async () => 'origin');

    const result = await cache.getOrLoad('k', loader);
    expect(result.source).toBe('loader');
    expect(result.value.toString()).toBe('origin');
    expect(loader).toHaveBeenCalledOnce();
  });

  it('getOrLoad returns the loaded value unstored when the write fails', async () => {
    store.failNext('write', errno('ENOSPC'));
    const result = await cache.getOrLoad('k', async () => 'origin');

    expect(result).toMatchObject({
      source: 'loader',
      stored: false,
      freshness: 'fresh',
    });
    expect(result.value.toString()).toBe('origin');
    expect(result.metadata).toMatchObject({ key: 'k', size: 6 });
    expect(onStoreError).toHaveBeenCalledWith(
      expect.objectContaining({ code: 'ENOSPC' }),
      { key: 'k', operation: 'write' },
    );
    await expect(cache.get('k')).resolves.toBeUndefined();
  });

  it('explicit put / delete / clear still propagate store errors', async () => {
    store.failNext('write', errno('EROFS'));
    await expect(cache.put('k', 'v')).rejects.toMatchObject({ code: 'EROFS' });
    store.failNext('delete', errno('EIO'));
    await expect(cache.delete('k')).rejects.toMatchObject({ code: 'EIO' });
    store.failNext('clear', errno('EIO'));
    await expect(cache.clear()).rejects.toMatchObject({ code: 'EIO' });
    expect(onStoreError).not.toHaveBeenCalled();
  });

  it('coalesced callers all receive the degraded result, with one loader call', async () => {
    store.failNext('write', errno('ENOSPC'));
    const loader = vi.fn<Loader>(async () => 'origin');
    const results = await Promise.all(
      Array.from({ length: 10 }, () => cache.getOrLoad('k', loader)),
    );
    expect(
      results.every(
        (r) => r.stored === false && r.value.toString() === 'origin',
      ),
    ).toBe(true);
    expect(loader).toHaveBeenCalledOnce();
  });

  it('a store that hangs on write does not hang the caller when a loader timeout is set', async () => {
    vi.useFakeTimers();
    const timed = new CacheManager(store, {
      clock,
      loaderTimeoutMs: 50,
      onStoreError,
    });
    const loader = vi.fn<Loader>(() => new Promise(() => undefined)); // never settles
    const outcome = timed.getOrLoad('k', loader).then(
      () => 'resolved',
      (e: unknown) => e,
    );
    await vi.advanceTimersByTimeAsync(50);
    await expect(outcome).resolves.toBeInstanceOf(LoaderTimeoutError);
  });
});

describe('CacheManager – loader timeout', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it('aborts the loader signal, rejects every coalesced caller and frees the key', async () => {
    vi.useFakeTimers();
    const store = new FaultyStore();
    const cache = new CacheManager(store, { loaderTimeoutMs: 100 });
    let seenSignal: AbortSignal | undefined;
    const loader = vi.fn<Loader>(({ signal }) => {
      seenSignal = signal;
      return new Promise(() => undefined);
    });

    const outcomes = Promise.all(
      Array.from({ length: 3 }, () =>
        cache.getOrLoad('slow', loader).then(
          () => 'resolved',
          (e: unknown) => e,
        ),
      ),
    );
    await vi.advanceTimersByTimeAsync(100);
    const results = await outcomes;
    expect(results.every((r) => r instanceof LoaderTimeoutError)).toBe(true);
    expect(seenSignal?.aborted).toBe(true);
    expect(seenSignal?.reason).toBeInstanceOf(LoaderTimeoutError);
    expect(loader).toHaveBeenCalledOnce();
    expect(cache.isLoading('slow')).toBe(false);

    // The key is not poisoned: the next call runs a fresh loader.
    await expect(
      cache.getOrLoad('slow', async () => 'fast'),
    ).resolves.toMatchObject({ source: 'loader' });
  });

  it('combines the caller signal with the timeout', async () => {
    const store = new FaultyStore();
    const cache = new CacheManager(store, { loaderTimeoutMs: 10_000 });
    const controller = new AbortController();
    const invoked = Promise.withResolvers<AbortSignal | undefined>();
    const pending = cache.getOrLoad(
      'k',
      ({ signal }) => {
        invoked.resolve(signal);
        return new Promise((_, reject) =>
          signal?.addEventListener('abort', () => reject(signal.reason)),
        );
      },
      { signal: controller.signal },
    );
    const seenSignal = await invoked.promise;
    controller.abort(new Error('client gone'));
    await expect(pending).rejects.toThrow('client gone');
    expect(seenSignal?.aborted).toBe(true);
  });

  it('does not invoke the loader for a caller that already aborted', async () => {
    const store = new FaultyStore();
    const cache = new CacheManager(store, {});
    const controller = new AbortController();
    const loader = vi.fn<Loader>(async () => 'never');
    const pending = cache.getOrLoad('k', loader, { signal: controller.signal });
    controller.abort(new Error('client gone'));
    await expect(pending).rejects.toThrow('client gone');
    expect(loader).not.toHaveBeenCalled();
    expect(cache.isLoading('k')).toBe(false);
  });

  it('rejects a non-positive loader timeout', () => {
    expect(
      () => new CacheManager(new FaultyStore(), { loaderTimeoutMs: 0 }),
    ).toThrow(RangeError);
  });
});

describe('CacheManager – clock skew', () => {
  it('treats entries created too far in the future as expired', async () => {
    const store = new FaultyStore();
    const clock = new FakeClock();
    const cache = new CacheManager(store, {
      clock,
      maxFutureSkewMs: 60_000,
      ttlJitterRatio: 0,
    });

    await cache.put('k', 'v', { ttlMs: 1_000_000 });
    clock.advance(-30_000); // clock drifted back a little: still fine
    expect((await cache.get('k'))?.freshness).toBe('fresh');

    clock.advance(-60_001); // now > 60 s "in the future": distrust it
    await expect(cache.get('k', { allowStale: true })).resolves.toBeUndefined();
    expect((await cache.head('k'))?.freshness).toBe('expired');

    const result = await cache.getOrLoad('k', async () => 'reloaded');
    expect(result.source).toBe('loader');
  });
});
