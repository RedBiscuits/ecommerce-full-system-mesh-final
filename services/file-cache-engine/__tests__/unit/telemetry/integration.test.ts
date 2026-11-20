import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { CacheManager } from '../../../src/cache/cache-manager';
import { createHttpMiddleware } from '../../../src/http/create-http-middleware';
import { defaultRoute } from '../../../src/http/telemetry-middleware';
import type { TelemetryEvent } from '../../../src/telemetry/events';
import { FakeClock } from '../../helpers/fake-clock';
import { FaultyStore, errno } from '../../helpers/faulty-store';
import { startServer, type TestServer } from '../../helpers/http-server';

describe('telemetry – emitted by the cache manager', () => {
  let store: FaultyStore;
  let events: TelemetryEvent[];
  let cache: CacheManager;

  beforeEach(() => {
    store = new FaultyStore();
    events = [];
    cache = new CacheManager(store, {
      clock: new FakeClock(),
      ttlMs: 1_000,
      staleWhileRevalidateMs: 500,
      ttlJitterRatio: 0,
      onStoreError: (): void => undefined,
      telemetry: { emit: (e): number => events.push(e) },
    });
  });

  afterEach(async () => {
    await cache.close();
  });

  const ofType = <T extends TelemetryEvent['type']>(
    type: T,
  ): Extract<TelemetryEvent, { type: T }>[] =>
    events.filter(
      (e): e is Extract<TelemetryEvent, { type: T }> => e.type === type,
    );

  it('reports lookups with outcome and latency for get/head/getOrLoad', async () => {
    await cache.put('k', 'v');
    await cache.get('k');
    await cache.get('missing');
    await cache.head('k');
    await cache.getOrLoad('k', async () => 'x');

    const lookups = ofType('cache.lookup');
    expect(lookups.map((l) => [l.operation, l.outcome])).toEqual([
      ['get', 'hit'],
      ['get', 'miss'],
      ['head', 'hit'],
      ['getOrLoad', 'hit'],
    ]);
    expect(lookups.every((l) => l.durationMs >= 0 && l.key !== '')).toBe(true);
    expect(ofType('cache.write')).toMatchObject([
      { operation: 'put', bytes: 1, error: undefined },
    ]);
  });

  it('reports loads: stored, unstored (write failure), error, and background', async () => {
    await cache.getOrLoad('a', async () => 'v');
    store.failNext('write', errno('ENOSPC'));
    await cache.getOrLoad('b', async () => 'v');
    await cache
      .getOrLoad('c', async () => {
        throw new Error('origin down');
      })
      .catch(() => undefined);

    const loads = ofType('cache.load');
    expect(loads.map((l) => l.outcome)).toEqual([
      'stored',
      'unstored',
      'error',
    ]);
    expect(loads[2]?.error).toContain('origin down');
    expect(ofType('cache.store-error')).toMatchObject([
      { operation: 'write', key: 'b' },
    ]);
    expect(
      ofType('cache.lookup').filter(
        (l) => l.operation === 'getOrLoad' && l.outcome === 'miss',
      ),
    ).toHaveLength(3);
  });

  it('marks background revalidations and reports stale lookups', async () => {
    const clock = new FakeClock();
    const swr = new CacheManager(store, {
      clock,
      ttlMs: 1_000,
      staleWhileRevalidateMs: 500,
      ttlJitterRatio: 0,
      telemetry: { emit: (e): number => events.push(e) },
    });
    await swr.put('k', 'v1');
    clock.advance(1_001);
    await swr.getOrLoad('k', async () => 'v2');
    await vi.waitFor(() => expect(ofType('cache.load')).toHaveLength(1));
    expect(ofType('cache.load')[0]).toMatchObject({
      background: true,
      outcome: 'stored',
    });
    expect(ofType('cache.lookup').at(-1)).toMatchObject({
      operation: 'getOrLoad',
      outcome: 'stale',
    });
    await swr.close();
  });

  it('reports failed reads as store errors and the lookup as a miss', async () => {
    store.failNext('read', errno('EIO'));
    await cache.get('k');
    expect(ofType('cache.store-error')).toMatchObject([
      { operation: 'read', error: expect.stringContaining('EIO') },
    ]);
    expect(ofType('cache.lookup')[0]?.outcome).toBe('miss');
  });

  it('a throwing sink never breaks cache operations', async () => {
    const hostile = new CacheManager(store, {
      telemetry: {
        emit: (): void => {
          throw new Error('observer bug');
        },
      },
    });
    await expect(hostile.put('k', 'v')).resolves.toBeDefined();
    await expect(
      hostile.getOrLoad('k', async () => 'v'),
    ).resolves.toMatchObject({ source: 'cache' });
    await hostile.close();
  });
});

describe('telemetry – HTTP middleware', () => {
  let server: TestServer | undefined;
  let events: TelemetryEvent[];

  beforeEach(() => {
    events = [];
  });

  afterEach(async () => {
    await server?.close();
    server = undefined;
  });

  it('defaultRoute collapses ids into templates', () => {
    const route = (url: string): string =>
      defaultRoute({ url, method: 'GET' } as never);
    expect(
      route('/users/42/orders/9f8e7d6c-1234-4abc-8def-0123456789ab?x=1'),
    ).toBe('GET /users/:id/orders/:id');
    expect(route('/files/0123456789abcdef0123')).toBe('GET /files/:id');
    expect(route('/')).toBe('GET /');
  });

  it('emits one http.request per request with route, status, latency, cache status and client key', async () => {
    const store = new FaultyStore();
    const cache = new CacheManager(store, { ttlJitterRatio: 0 });
    const http = createHttpMiddleware({
      telemetry: { emit: (e): number => events.push(e) },
      rateLimit: { limiter: { capacity: 2, refillPerSecond: 0.001 } },
      cache: { cache, vary: [] },
    });
    server = await startServer(http.stack, (req, res) => {
      if (req.url?.startsWith('/boom')) {
        res.statusCode = 500;
      }
      res.end('body');
    });
    const { url } = server;

    await fetch(`${url}/items/1`);
    await vi.waitFor(() =>
      expect(cache.isLoading('GET ' + new URL(url).host + '/items/1')).toBe(
        false,
      ),
    );
    await fetch(`${url}/items/1`);
    const limited = await fetch(`${url}/boom`);
    expect(limited.status).toBe(429);

    const requests = events.filter(
      (e): e is Extract<TelemetryEvent, { type: 'http.request' }> =>
        e.type === 'http.request',
    );
    expect(requests.map((r) => [r.route, r.status, r.cacheStatus])).toEqual([
      ['GET /items/:id', 200, 'MISS'],
      ['GET /items/:id', 200, 'HIT'],
      ['GET /boom', 429, undefined],
    ]);
    expect(
      requests.every(
        (r) =>
          r.durationMs >= 0 && r.clientKey === 'net:127.0.0.1' && !r.aborted,
      ),
    ).toBe(true);

    const decisions = events.filter(
      (e): e is Extract<TelemetryEvent, { type: 'rate-limit' }> =>
        e.type === 'rate-limit',
    );
    expect(decisions.map((d) => d.admitted)).toEqual([true, true, false]);
    expect(decisions[2]).toMatchObject({
      key: 'net:127.0.0.1',
      route: 'GET /boom',
    });
    http.close();
    await cache.close();
  });

  it('marks aborted requests', async () => {
    const http = createHttpMiddleware({
      telemetry: { emit: (e): number => events.push(e) },
    });
    const controller = new AbortController();
    server = await startServer(http.stack, (_req, res) => {
      res.write('partial');
      setTimeout(() => controller.abort(), 20);
      res.on('close', () => {
        if (!res.writableEnded) {
          res.destroy();
        }
      });
    });
    await fetch(server.url, { signal: controller.signal }).catch(
      () => undefined,
    );
    await vi.waitFor(() => expect(events).toHaveLength(1));
    expect(events[0]).toMatchObject({ type: 'http.request', aborted: true });
  });

  it('supports a custom route resolver', async () => {
    const http = createHttpMiddleware({
      telemetry: {
        telemetry: { emit: (e): number => events.push(e) },
        routeResolver: () => 'custom',
      },
    });
    server = await startServer(http.stack, (_req, res) => res.end());
    await fetch(server.url);
    await vi.waitFor(() => expect(events).toHaveLength(1));
    expect(events[0]).toMatchObject({ route: 'custom' });
  });
});
