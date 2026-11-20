import { writeFile } from 'node:fs/promises';
import { join } from 'node:path';

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { hashKey } from '../../../src/cache/cache-key';
import { createHttpMiddleware } from '../../../src/http/create-http-middleware';
import { createFileCache, type FileCache } from '../../../src/main';
import type { TelemetryEvent } from '../../../src/telemetry/events';
import { FakeClock } from '../../helpers/fake-clock';
import { startServer, type TestServer } from '../../helpers/http-server';
import { makeTempDir, type TempDir } from '../../helpers/temp-dir';

const refTimers = (): number =>
  process.getActiveResourcesInfo().filter((r) => r === 'Timeout').length;

describe('createFileCache – observability wiring', () => {
  let dir: TempDir;
  let fileCache: FileCache | undefined;
  let events: TelemetryEvent[];
  let server: TestServer | undefined;

  beforeEach(async () => {
    dir = await makeTempDir();
    events = [];
  });

  afterEach(async () => {
    await server?.close();
    server = undefined;
    await fileCache?.close();
    fileCache = undefined;
    await dir.cleanup();
  });

  it('feeds the built-in metrics and the user sink from every component', async () => {
    const clock = new FakeClock();
    fileCache = await createFileCache({
      directory: dir.path,
      store: { fsync: false },
      sweeper: { batchPauseMs: 0 },
      clock,
      ttlMs: 1_000,
      staleWhileRevalidateMs: 0,
      ttlJitterRatio: 0,
      telemetry: { emit: (e): number => events.push(e) },
    });
    const { cache, sweeper, metrics, fsStore } = fileCache;

    await cache.getOrLoad('k', async () => 'v'); // miss + load
    await cache.get('k'); // hit
    await cache.put('corrupt', 'v');
    const hash = hashKey('corrupt');
    await writeFile(
      join(fsStore.entriesDirectory, hash.slice(0, 2), `${hash}.cache`),
      'garbage',
    );
    await cache.get('corrupt'); // quarantined → miss
    clock.advance(2_000);
    await sweeper.sweepOnce();

    const snapshot = metrics!.snapshot();
    expect(snapshot.cache).toMatchObject({
      lookups: 3,
      hits: 1,
      misses: 2,
      loads: 1,
      writes: 1,
      corruptEntries: 1,
    });
    expect(snapshot.cache.lastSweep).toMatchObject({ expired: 1 });
    expect(events.map((e) => e.type)).toEqual(
      expect.arrayContaining([
        'cache.lookup',
        'cache.load',
        'cache.write',
        'cache.corrupt-entry',
        'cache.sweep',
      ]),
    );
  });

  it('forwards resilience events (retry / failure) to telemetry', async () => {
    fileCache = await createFileCache({
      directory: dir.path,
      store: { fsync: false },
      sweeper: false,
      resilience: { retry: { maxAttempts: 2, baseDelayMs: 1, jitterRatio: 0 } },
      onStoreError: (): void => undefined,
      telemetry: { emit: (e): number => events.push(e) },
    });
    const emfile = Object.assign(new Error('too many files'), {
      code: 'EMFILE',
    });
    const eio = Object.assign(new Error('io'), { code: 'EIO' });
    vi.spyOn(fileCache.fsStore, 'read')
      .mockRejectedValueOnce(emfile)
      .mockRejectedValueOnce(eio);

    await fileCache.cache.get('k');
    expect(events.map((e) => e.type)).toEqual(
      expect.arrayContaining([
        'store.retry',
        'store.failure',
        'cache.store-error',
      ]),
    );
    expect(fileCache.metrics!.snapshot().store).toMatchObject({
      retries: 1,
      failures: 1,
    });
  });

  it('diagnostics() combines checks, store health, metrics and alerts', async () => {
    fileCache = await createFileCache({
      directory: dir.path,
      store: { fsync: false },
      sweeper: { intervalMs: 50, batchPauseMs: 0 },
      alertThresholds: { minSamples: 1, maxHttpErrorRatio: 0.5 },
    });
    await fileCache.cache.put('k', 'v');
    await vi.waitFor(() => expect(fileCache!.sweeper.lastReport).toBeDefined());

    const report = await fileCache.diagnostics();
    expect(report.status).toBe('ok');
    expect(report.ready).toBe(true);
    expect(report.checks.map((c) => c.name).sort()).toEqual(
      [
        'directory.writable',
        'disk.space',
        'metrics.alerts',
        'store.circuit',
        'store.integrity',
        'sweeper',
        'sweeper.lock',
        'tmp.orphans',
      ].sort(),
    );
    expect(report.store?.circuit?.state).toBe('closed');
    expect(report.metrics?.cache.writes).toBe(1);

    // Feed an alert-worthy signal through the combined sink and re-check.
    fileCache.telemetry.emit({
      type: 'http.request',
      at: 1,
      method: 'GET',
      route: 'GET /x',
      status: 500,
      durationMs: 1,
      cacheStatus: undefined,
      bytes: 1,
      aborted: false,
      clientKey: undefined,
    });
    const degraded = await fileCache.diagnostics();
    expect(degraded.alerts.map((a) => a.code)).toContain('http.error-ratio');
    expect(degraded.status).toBe('down'); // critical alert
    expect(
      await fileCache.diagnostics({ thresholds: { maxHttpErrorRatio: 1 } }),
    ).toMatchObject({ status: 'ok' });
  });

  it('metrics can be disabled and the user sink still works', async () => {
    fileCache = await createFileCache({
      directory: dir.path,
      sweeper: false,
      metrics: false,
      telemetry: { emit: (e): number => events.push(e) },
    });
    await fileCache.cache.put('k', 'v');
    expect(fileCache.metrics).toBeUndefined();
    expect(events).toHaveLength(1);
    expect((await fileCache.diagnostics()).metrics).toBeUndefined();
  });

  it('serves readiness and diagnostics through the HTTP stack with request metrics', async () => {
    fileCache = await createFileCache({
      directory: dir.path,
      store: { fsync: false },
      sweeper: false,
    });
    const http = createHttpMiddleware({
      telemetry: fileCache.telemetry,
      diagnostics: { diagnostics: () => fileCache!.diagnostics() },
      cache: { cache: fileCache.cache, vary: [] },
    });
    server = await startServer(http.stack, (_req, res) => res.end('app'));

    await fetch(`${server.url}/api/things/7`);
    expect((await fetch(`${server.url}/-/ready`)).status).toBe(200);
    const full = (await (
      await fetch(`${server.url}/-/diagnostics`)
    ).json()) as { metrics: { http: { routes: Record<string, unknown> } } };
    expect(Object.keys(full.metrics.http.routes)).toContain(
      'GET /api/things/:id',
    );
    expect(fileCache.metrics!.snapshot().http.requests).toBeGreaterThanOrEqual(
      2,
    );
    http.close();
  });

  it("close() disables the event-loop monitor and leaves no ref'd timers", async () => {
    const baseline = refTimers();
    fileCache = await createFileCache({
      directory: dir.path,
      sweeper: { intervalMs: 10 },
    });
    expect(fileCache.metrics!.snapshot().process.eventLoop).toBeDefined();
    await fileCache.close();
    expect(fileCache.metrics!.snapshot().process.eventLoop).toBeUndefined();
    expect(refTimers()).toBeLessThanOrEqual(baseline);
  });
});
