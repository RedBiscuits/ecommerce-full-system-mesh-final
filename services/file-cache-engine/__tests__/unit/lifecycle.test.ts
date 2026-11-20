/**
 * Lifecycle audit: nothing the cache owns may keep the event loop alive
 * while idle, and every `close()`/`stop()` must release its handles.
 *
 * `process.getActiveResourcesInfo()` lists only resources that keep the
 * loop alive — `unref`'d timers are excluded — so counting `Timeout`
 * entries is a faithful "would this block process exit?" oracle. The runner
 * owns timers of its own that come and go, so baselines are taken inside
 * each test body and teardown is asserted as "no increase".
 */
import type { ServerResponse } from 'node:http';

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { CacheManager } from '../../src/cache/cache-manager';
import { CacheClosedError } from '../../src/cache/errors';
import { ExpirySweeper } from '../../src/cache/expiry-sweeper';
import { createHttpMiddleware } from '../../src/http/create-http-middleware';
import { createRateLimitMiddleware } from '../../src/http/rate-limit-middleware';
import { captureResponse } from '../../src/http/response-capture';
import {
  createFileCache,
  KeyedRateLimiter,
  TokenBucketRateLimiter,
  type FileCache,
} from '../../src/main';
import { StoreUnavailableError } from '../../src/resilience/errors';
import { FileLock } from '../../src/resilience/file-lock';
import { ResilientCacheStore } from '../../src/resilience/resilient-store';
import { FaultyStore, errno } from '../helpers/faulty-store';
import { startServer, type TestServer } from '../helpers/http-server';
import { makeTempDir, type TempDir } from '../helpers/temp-dir';

const refTimers = (): number =>
  process.getActiveResourcesInfo().filter((r) => r === 'Timeout').length;

describe('lifecycle – event loop hygiene and teardown', () => {
  let dir: TempDir;

  beforeEach(async () => {
    dir = await makeTempDir();
  });

  afterEach(async () => {
    await dir.cleanup();
  });

  describe('createFileCache', () => {
    let fileCache: FileCache | undefined;

    afterEach(async () => {
      await fileCache?.close();
      fileCache = undefined;
    });

    it('runs its background sweeper without holding the event loop open', async () => {
      const baseline = refTimers();
      fileCache = await createFileCache({
        directory: dir.path,
        store: { fsync: false },
        sweeper: { intervalMs: 10 },
        rateLimiter: { capacity: 10, refillPerSecond: 10 },
      });
      await fileCache.cache.put('k', 'v');
      expect(fileCache.sweeper.isRunning).toBe(true);
      expect(refTimers()).toBeLessThanOrEqual(baseline);
    });

    it('close() tears everything down in order and is idempotent', async () => {
      const baseline = refTimers();
      fileCache = await createFileCache({
        directory: dir.path,
        store: { fsync: false },
        sweeper: { intervalMs: 10 },
        rateLimiter: { capacity: 1, refillPerSecond: 0.001 },
        loaderTimeoutMs: 60_000,
        onStoreError: (): void => undefined,
      });
      const { cache, sweeper, rateLimiter } = fileCache;

      // Put work in flight: a loader waiting on its signal, and a request
      // queued on the (now empty) rate limiter.
      const loaderSignals: AbortSignal[] = [];
      const inFlight = cache
        .getOrLoad('slow', ({ signal }) => {
          loaderSignals.push(signal!);
          return new Promise((_, reject) =>
            signal?.addEventListener('abort', () => reject(signal.reason)),
          );
        })
        .catch((error: unknown) => error);
      await vi.waitFor(() => expect(loaderSignals).toHaveLength(1));
      const queued = rateLimiter!.acquire().then(
        () => 'admitted',
        (error: unknown) => error,
      );
      expect(refTimers()).toBeGreaterThan(baseline); // the queued waiter

      await fileCache.close();
      await fileCache.close(); // idempotent

      expect(sweeper.isRunning).toBe(false);
      expect(cache.isClosed).toBe(true);
      expect(loaderSignals[0]?.reason).toBeInstanceOf(CacheClosedError);
      await expect(inFlight).resolves.toBeInstanceOf(CacheClosedError);
      await expect(queued).resolves.toMatchObject({ reason: 'aborted' });
      expect(cache.pendingLoads).toBe(0);
      expect(refTimers()).toBeLessThanOrEqual(baseline);

      await expect(cache.get('k')).rejects.toBeInstanceOf(CacheClosedError);
      await expect(cache.put('k', 'v')).rejects.toBeInstanceOf(
        CacheClosedError,
      );
      await expect(
        cache.getOrLoad('k', async () => 'v'),
      ).rejects.toBeInstanceOf(CacheClosedError);
    });

    it('leaves a caller-supplied rate limiter alone on close', async () => {
      const shared = new TokenBucketRateLimiter({
        capacity: 1,
        refillPerSecond: 1,
      });
      fileCache = await createFileCache({
        directory: dir.path,
        sweeper: false,
        rateLimiter: shared,
      });
      await fileCache.close();
      expect(shared.isClosed).toBe(false);
      expect(shared.tryAcquire()).toBe(true);
    });
  });

  describe('ExpirySweeper', () => {
    it('stop() interrupts a sweep paused between batches and releases the lock', async () => {
      const baseline = refTimers();
      const store = new FaultyStore();
      for (let i = 0; i < 10; i += 1) {
        await store.write(`cold-${i}`, Buffer.from('x'), {
          createdAt: 0,
          expiresAt: 1,
          staleUntil: 2,
        });
      }
      const lockPath = `${dir.path}/sweep.lock`;
      const sweeper = new ExpirySweeper(store, {
        intervalMs: 1,
        intervalJitterRatio: 0,
        batchSize: 2,
        batchPauseMs: 60_000, // would take minutes to finish
        lock: { path: lockPath },
        onError: (): void => undefined,
      });

      sweeper.start();
      await vi.waitFor(() =>
        expect(store.calls.deleteIf).toBeGreaterThanOrEqual(2),
      );
      // The in-sweep pause is real work and rightly keeps the loop alive.
      await vi.waitFor(() => expect(refTimers()).toBeGreaterThan(baseline));

      const started = Date.now();
      await sweeper.stop();
      expect(Date.now() - started).toBeLessThan(1_000);
      expect(sweeper.isRunning).toBe(false);
      expect(refTimers()).toBeLessThanOrEqual(baseline);
      const lock = await FileLock.tryAcquire(lockPath);
      expect(lock).toBeDefined();
      await lock?.release();
    });

    it('stop() waits for a manual sweepOnce() too', async () => {
      const store = new FaultyStore();
      await store.write('cold', Buffer.from('x'), {
        createdAt: 0,
        expiresAt: 1,
        staleUntil: 2,
      });
      const sweeper = new ExpirySweeper(store, { batchPauseMs: 0 });
      const sweep = sweeper.sweepOnce();
      await sweeper.stop();
      await expect(sweep).resolves.toMatchObject({ expired: 1 });
    });
  });

  describe('rate limiters', () => {
    it('TokenBucket.close() rejects waiters, clears timers and refuses new work', async () => {
      const baseline = refTimers();
      const limiter = new TokenBucketRateLimiter({
        capacity: 1,
        refillPerSecond: 0.001,
      });
      await limiter.acquire();
      const waiting = Promise.all(
        Array.from({ length: 3 }, () =>
          limiter.acquire(1, { timeoutMs: 60_000 }).then(
            () => 'admitted',
            (e: unknown) => e,
          ),
        ),
      );
      expect(limiter.queueLength).toBe(3);
      expect(refTimers()).toBeGreaterThan(baseline);

      limiter.close();
      limiter.close();
      const outcomes = await waiting;
      expect(
        outcomes.every(
          (o) => o instanceof Error && 'reason' in o && o.reason === 'aborted',
        ),
      ).toBe(true);
      expect(limiter.queueLength).toBe(0);
      expect(refTimers()).toBeLessThanOrEqual(baseline);
      expect(limiter.tryAcquire()).toBe(false);
      await expect(limiter.acquire()).rejects.toMatchObject({
        reason: 'aborted',
      });
    });

    it('KeyedRateLimiter.close() closes every bucket and stays usable', async () => {
      const baseline = refTimers();
      const keyed = new KeyedRateLimiter({
        capacity: 1,
        refillPerSecond: 0.001,
      });
      await keyed.acquire('a');
      const waiting = keyed.acquire('a').then(
        () => 'admitted',
        (e: unknown) => e,
      );
      keyed.close();
      await expect(waiting).resolves.toMatchObject({ reason: 'aborted' });
      expect(keyed.size).toBe(0);
      expect(keyed.tryAcquire('b')).toBe(true);
      expect(refTimers()).toBeLessThanOrEqual(baseline);
    });
  });

  describe('CacheManager', () => {
    it('loader timeout timers never outlive the loader', async () => {
      const baseline = refTimers();
      const cache = new CacheManager(new FaultyStore(), {
        loaderTimeoutMs: 60_000,
      });
      await cache.getOrLoad('k', async () => 'fast');
      await cache.getOrLoad('k2', async () => 'fast');
      expect(refTimers()).toBeLessThanOrEqual(baseline);
      await cache.close();
    });

    it('close() removes its listeners from a long-lived caller signal', async () => {
      const cache = new CacheManager(new FaultyStore(), {});
      const appShutdown = new AbortController();
      const addSpy = vi.spyOn(appShutdown.signal, 'addEventListener');
      const removeSpy = vi.spyOn(appShutdown.signal, 'removeEventListener');
      for (let i = 0; i < 25; i += 1) {
        await cache.getOrLoad(`k${i}`, async () => 'v', {
          signal: appShutdown.signal,
        });
      }
      expect(addSpy).toHaveBeenCalledTimes(25);
      expect(removeSpy).toHaveBeenCalledTimes(25);
      await cache.close();
    });

    it('close() resolves only after in-flight loaders settle', async () => {
      const cache = new CacheManager(new FaultyStore(), {});
      let settled = false;
      const load = cache
        .getOrLoad(
          'k',
          ({ signal }) =>
            new Promise((_, reject) =>
              signal?.addEventListener('abort', () => {
                setTimeout(() => {
                  settled = true;
                  reject(signal.reason);
                }, 20);
              }),
            ),
        )
        .catch(() => undefined);
      await vi.waitFor(() => expect(cache.isLoading('k')).toBe(true));
      await cache.close();
      expect(settled).toBe(true);
      await load;
    });
  });

  describe('ResilientCacheStore', () => {
    it('close() cuts a retry backoff short', async () => {
      const baseline = refTimers();
      const inner = new FaultyStore();
      inner.failNext('read', errno('EMFILE'), 5);
      const backingOff = Promise.withResolvers<void>();
      const store = new ResilientCacheStore(inner, {
        retry: { maxAttempts: 5, baseDelayMs: 60_000, jitterRatio: 0 },
        onEvent: (event): void => {
          if (event.type === 'retry') {
            backingOff.resolve();
          }
        },
      });
      const pending = store.read('k').then(
        () => 'resolved',
        (e: unknown) => e,
      );
      await backingOff.promise;
      await new Promise((r) => setImmediate(r)); // let the backoff timer register
      expect(refTimers()).toBeGreaterThan(baseline); // backing off

      store.close();
      const error = await pending;
      expect(error).toBeInstanceOf(StoreUnavailableError);
      expect(refTimers()).toBeLessThanOrEqual(baseline);
      expect(inner.calls.read).toBe(1);
    });
  });

  describe('HTTP', () => {
    let server: TestServer | undefined;

    afterEach(async () => {
      await server?.close();
      server = undefined;
    });

    it('captureResponse detaches on abort: patches restored, buffers dropped, listeners gone', async () => {
      let res: ServerResponse | undefined;
      let outcome: Promise<unknown> | undefined;
      // Node registers internal listeners of its own; only ours must vanish.
      let before = { finish: 0, close: 0 };
      const controller = new AbortController();
      server = await startServer([], (_req, response) => {
        res = response;
        before = {
          finish: response.listenerCount('finish'),
          close: response.listenerCount('close'),
        };
        outcome = captureResponse(response, {
          maxBytes: 1 << 20,
          signal: controller.signal,
        });
        response.write('x'.repeat(1000));
        controller.abort();
        response.end('y');
      });
      await fetch(server.url);

      await expect(outcome).resolves.toEqual({ kind: 'aborted' });
      expect(res?.listenerCount('finish')).toBe(before.finish);
      expect(res?.listenerCount('close')).toBe(before.close);
      expect(res?.write).toBe(Object.getPrototypeOf(res).write);
      expect(res?.end).toBe(Object.getPrototypeOf(res).end);
    });

    it('middleware factories expose close() for the limiter they own', async () => {
      const baseline = refTimers();
      const owned = createRateLimitMiddleware({
        limiter: { capacity: 1, refillPerSecond: 1 },
      });
      expect(owned.limiter).toBeInstanceOf(KeyedRateLimiter);
      owned.limiter.tryAcquire('a');
      owned.close();
      expect(owned.limiter.size).toBe(0);

      const shared = new KeyedRateLimiter({ capacity: 1, refillPerSecond: 1 });
      shared.tryAcquire('a');
      const borrowed = createRateLimitMiddleware({ limiter: shared });
      borrowed.close();
      expect(shared.size).toBe(1); // untouched

      const stack = createHttpMiddleware({
        rateLimit: { limiter: { capacity: 1, refillPerSecond: 1 } },
      });
      stack.rateLimit?.limiter.tryAcquire('a');
      stack.close();
      expect(stack.rateLimit?.limiter.size).toBe(0);
      expect(refTimers()).toBeLessThanOrEqual(baseline);
    });
  });
});
