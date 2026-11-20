import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { CacheManager } from '../../../src/cache/cache-manager';
import { createCacheMiddleware } from '../../../src/http/cache-middleware';
import { ResilientCacheStore } from '../../../src/resilience/resilient-store';
import { FakeClock } from '../../helpers/fake-clock';
import { FaultyStore, errno } from '../../helpers/faulty-store';
import {
  startServer,
  type Handler,
  type TestServer,
} from '../../helpers/http-server';

describe('cache middleware – resilience', () => {
  let store: FaultyStore;
  let clock: FakeClock;
  let server: TestServer | undefined;
  let hits: number;

  beforeEach(() => {
    store = new FaultyStore();
    clock = new FakeClock();
    hits = 0;
  });

  afterEach(async () => {
    await server?.close();
    server = undefined;
  });

  const keyFor = (url: string): string => {
    const { host, pathname } = new URL(url);
    return `GET ${host}${pathname}`;
  };

  async function serve(cache: CacheManager, handler: Handler): Promise<string> {
    server = await startServer(
      [
        createCacheMiddleware({
          cache,
          clock,
          vary: [],
          onError: (): void => undefined,
        }),
      ],
      handler,
    );
    return server.url;
  }

  it('re-elects a leader when the first one disconnects mid-response', async () => {
    const cache = new CacheManager(store, {
      clock,
      onStoreError: (): void => undefined,
    });
    const leaderGone = new AbortController();
    const url = await serve(cache, (_req, res) => {
      hits += 1;
      const attempt = hits;
      res.setHeader('content-type', 'text/plain');
      if (attempt === 1) {
        // First origin call: stream a bit, then the client goes away.
        res.write('partial');
        setTimeout(() => leaderGone.abort(), 20);
        res.on('close', () => {
          if (!res.writableEnded) {
            res.destroy();
          }
        });
        return;
      }
      setTimeout(() => res.end(`attempt-${attempt}`), 30);
    });

    const leader = fetch(url, { signal: leaderGone.signal }).catch(
      () => undefined,
    );
    await vi.waitFor(() => expect(cache.isLoading(keyFor(url))).toBe(true), {
      interval: 2,
    });
    const followers = Promise.all(Array.from({ length: 5 }, () => fetch(url)));

    await leader;
    const responses = await followers;
    const bodies = await Promise.all(responses.map((r) => r.text()));
    expect(bodies.every((b) => b === 'attempt-2')).toBe(true);
    // Exactly one new leader ran origin for the whole group.
    expect(hits).toBe(2);
  });

  it('falls back to origin for everyone once the loader keeps timing out', async () => {
    const cache = new CacheManager(store, {
      clock,
      loaderTimeoutMs: 40,
      onStoreError: (): void => undefined,
    });
    const url = await serve(cache, (_req, res) => {
      hits += 1;
      setTimeout(() => res.end('slow-but-fine'), 120);
    });

    const responses = await Promise.all(
      Array.from({ length: 4 }, () => fetch(url)),
    );
    const bodies = await Promise.all(responses.map((r) => r.text()));
    expect(bodies.every((b) => b === 'slow-but-fine')).toBe(true);
    expect(responses.every((r) => r.status === 200)).toBe(true);
    expect(cache.isLoading(keyFor(url))).toBe(false);
    expect(store.entries.size).toBe(0); // timed-out responses are never stored
  });

  it('keeps serving from origin while the store circuit is open, then recovers', async () => {
    const resilient = new ResilientCacheStore(store, {
      clock,
      retry: { maxAttempts: 1 },
      circuit: { failureThreshold: 1, cooldownMs: 1_000 },
    });
    const cache = new CacheManager(resilient, {
      clock,
      ttlJitterRatio: 0,
      onStoreError: (): void => undefined,
    });
    store.failNext('read', errno('EIO'));
    const url = await serve(cache, (_req, res) => {
      hits += 1;
      res.end(`origin-${hits}`);
    });

    const first = await fetch(url);
    expect(first.headers.get('x-cache')).toBe('MISS');
    await expect(first.text()).resolves.toBe('origin-1');
    expect(resilient.health().circuit?.state).toBe('open');

    const callsBefore = store.calls.read;
    const whileOpen = await fetch(url);
    await expect(whileOpen.text()).resolves.toBe('origin-2');
    expect(store.calls.read).toBe(callsBefore); // fast-failed, disk untouched

    clock.advance(1_000);
    const probe = await fetch(url);
    await expect(probe.text()).resolves.toBe('origin-3');
    await vi.waitFor(() => expect(store.entries.size).toBe(1));
    const hit = await fetch(url);
    expect(hit.headers.get('x-cache')).toBe('HIT');
    await expect(hit.text()).resolves.toBe('origin-3');
  });
});
