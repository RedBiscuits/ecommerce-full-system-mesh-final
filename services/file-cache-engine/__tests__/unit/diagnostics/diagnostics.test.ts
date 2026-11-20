import { chmod, mkdir, rm, utimes, writeFile } from 'node:fs/promises';
import { join } from 'node:path';

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { ExpirySweeper } from '../../../src/cache/expiry-sweeper';
import { FsCacheStore } from '../../../src/cache/fs-cache-store';
import type { DiagnosticCheck } from '../../../src/diagnostics/diagnostics';
import { runDiagnostics } from '../../../src/diagnostics/diagnostics';
import type { StoreHealth } from '../../../src/resilience/resilient-store';
import { MetricsCollector } from '../../../src/telemetry/metrics';
import { FakeClock } from '../../helpers/fake-clock';
import { FaultyStore } from '../../helpers/faulty-store';
import { makeTempDir, type TempDir } from '../../helpers/temp-dir';

const isRoot = typeof process.getuid === 'function' && process.getuid() === 0;

describe('runDiagnostics', () => {
  let dir: TempDir;
  let store: FsCacheStore;

  beforeEach(async () => {
    dir = await makeTempDir();
    store = await FsCacheStore.open({ directory: dir.path, fsync: false });
  });

  afterEach(async () => {
    await chmod(join(dir.path, 'tmp'), 0o700).catch(() => undefined);
    await dir.cleanup();
  });

  const check = (
    report: Awaited<ReturnType<typeof runDiagnostics>>,
    name: string,
  ): DiagnosticCheck | undefined => report.checks.find((c) => c.name === name);

  it('passes on a healthy directory and reports every check', async () => {
    await store.write('k', Buffer.from('v'), {
      createdAt: 0,
      expiresAt: 1e12,
      staleUntil: 1e12,
    });
    const report = await runDiagnostics({ directory: dir.path, store });

    expect(report.status).toBe('ok');
    expect(report.ready).toBe(true);
    expect(report.checks.map((c) => c.name).sort()).toEqual(
      [
        'directory.writable',
        'disk.space',
        'sweeper.lock',
        'store.integrity',
        'tmp.orphans',
      ].sort(),
    );
    expect(
      report.checks.every((c) => c.status === 'pass' && c.durationMs >= 0),
    ).toBe(true);
    expect(check(report, 'store.integrity')?.details).toMatchObject({
      scanned: 1,
      corrupt: 0,
    });
    expect(check(report, 'disk.space')?.details).toMatchObject({
      freeBytes: expect.any(Number),
    });
    expect(report.metrics).toBeUndefined();
    expect(report.alerts).toEqual([]);
  });

  it('runs with no options at all', async () => {
    const report = await runDiagnostics();
    expect(report).toMatchObject({ status: 'ok', ready: true, checks: [] });
  });

  it.skipIf(isRoot)(
    'fails readiness when the directory is not writable',
    async () => {
      await chmod(join(dir.path, 'tmp'), 0o500);
      const report = await runDiagnostics({ directory: dir.path });
      expect(check(report, 'directory.writable')?.status).toBe('fail');
      expect(report.status).toBe('down');
      expect(report.ready).toBe(false);
    },
  );

  it('warns about stuck temp files and abandoned sweep locks', async () => {
    const clock = new FakeClock(Date.now());
    const old = new Date(Date.now() - 3_600_000);
    await writeFile(join(dir.path, 'tmp', 'stuck.tmp'), 'x');
    await utimes(join(dir.path, 'tmp', 'stuck.tmp'), old, old);
    await writeFile(join(dir.path, 'sweep.lock'), 'x');
    await utimes(join(dir.path, 'sweep.lock'), old, old);

    const report = await runDiagnostics({ directory: dir.path, clock });
    expect(check(report, 'tmp.orphans')).toMatchObject({
      status: 'warn',
      details: { stuck: 1 },
    });
    expect(check(report, 'sweeper.lock')).toMatchObject({
      status: 'warn',
      details: { held: true },
    });
    expect(report.status).toBe('degraded');
    expect(report.ready).toBe(true);
  });

  it('treats a fresh sweep lock as an active sweep', async () => {
    await writeFile(join(dir.path, 'sweep.lock'), 'x');
    const report = await runDiagnostics({ directory: dir.path });
    expect(check(report, 'sweeper.lock')).toMatchObject({
      status: 'pass',
      details: { held: true },
    });
  });

  it('reports corrupt entries found by the integrity sample', async () => {
    await store.write('good', Buffer.from('v'), {
      createdAt: 0,
      expiresAt: 1e12,
      staleUntil: 1e12,
    });
    await store.write('bad', Buffer.from('v'), {
      createdAt: 0,
      expiresAt: 1e12,
      staleUntil: 1e12,
    });
    const { hashKey } = await import('../../../src/cache/cache-key');
    const hash = hashKey('bad');
    await writeFile(
      join(store.entriesDirectory, hash.slice(0, 2), `${hash}.cache`),
      'garbage',
    );

    const report = await runDiagnostics({ store });
    expect(check(report, 'store.integrity')).toMatchObject({
      status: 'warn',
      details: { scanned: 1, corrupt: 1 },
    });
  });

  it('honours the integrity sample size', async () => {
    for (let i = 0; i < 5; i += 1) {
      await store.write(`k${i}`, Buffer.from('v'), {
        createdAt: 0,
        expiresAt: 1e12,
        staleUntil: 1e12,
      });
    }
    const report = await runDiagnostics({ store, integritySampleSize: 2 });
    expect(check(report, 'store.integrity')?.details).toMatchObject({
      scanned: 2,
    });
  });

  it('maps circuit state to fail / warn / pass', async () => {
    const health = (state: 'open' | 'half-open' | 'closed'): StoreHealth => ({
      circuit: {
        name: 'c',
        state,
        consecutiveFailures: 1,
        openedAt: 1,
        retryAt: undefined,
        lastError: undefined,
      },
      lastFailure: { at: 1, kind: 'unknown', error: new Error('x') },
      lastReclaimAt: undefined,
    });
    expect(
      check(
        await runDiagnostics({ storeHealth: () => health('open') }),
        'store.circuit',
      )?.status,
    ).toBe('fail');
    expect(
      check(
        await runDiagnostics({ storeHealth: () => health('half-open') }),
        'store.circuit',
      )?.status,
    ).toBe('warn');
    expect(
      check(
        await runDiagnostics({ storeHealth: () => health('closed') }),
        'store.circuit',
      )?.status,
    ).toBe('pass');
    expect(
      check(
        await runDiagnostics({ storeHealth: () => undefined }),
        'store.circuit',
      )?.status,
    ).toBe('pass');
  });

  it('assesses the sweeper: stopped, stale, failing deletes, healthy', async () => {
    const clock = new FakeClock();
    const faulty = new FaultyStore();
    const sweeper = new ExpirySweeper(faulty, { clock, batchPauseMs: 0 });
    expect(
      check(await runDiagnostics({ sweeper, clock }), 'sweeper')?.status,
    ).toBe('warn');

    sweeper.start();
    await sweeper.sweepOnce();
    expect(
      check(
        await runDiagnostics({ sweeper, sweepIntervalMs: 1_000, clock }),
        'sweeper',
      )?.status,
    ).toBe('pass');
    clock.advance(10_000);
    expect(
      check(
        await runDiagnostics({ sweeper, sweepIntervalMs: 1_000, clock }),
        'sweeper',
      )?.message,
    ).toContain('No sweep completed recently');
    await sweeper.stop();
  });

  it('includes metrics and alerts, failing readiness on critical alerts', async () => {
    const metrics = new MetricsCollector({ eventLoop: false });
    metrics.emit({ type: 'store.circuit', at: 1, state: 'open', error: 'EIO' });
    const report = await runDiagnostics({ metrics: () => metrics.snapshot() });
    expect(report.metrics).toBeDefined();
    expect(report.alerts.map((a) => a.code)).toEqual(['store.circuit-open']);
    expect(check(report, 'metrics.alerts')?.status).toBe('fail');
    expect(report.ready).toBe(false);
  });

  it('a throwing or hanging check becomes a fail without stopping the others', async () => {
    const hanging = new ExpirySweeper(new FaultyStore(), {});
    Object.defineProperty(hanging, 'lastReport', {
      get: () => {
        throw new Error('boom');
      },
    });
    const report = await runDiagnostics({
      directory: dir.path,
      sweeper: hanging,
      timeoutMs: 200,
    });
    expect(check(report, 'sweeper')).toMatchObject({
      status: 'fail',
      message: expect.stringContaining('boom'),
    });
    expect(check(report, 'directory.writable')?.status).toBe('pass');
  });

  it('times out a check that never settles', async () => {
    const hang = (): AsyncIterable<never> => ({
      [Symbol.asyncIterator]: () => ({
        next: () => new Promise(() => undefined),
      }),
    });
    Object.defineProperty(store, 'scan', { value: hang });
    const report = await runDiagnostics({ store, timeoutMs: 50 });
    expect(check(report, 'store.integrity')).toMatchObject({
      status: 'fail',
      message: expect.stringContaining('did not complete'),
    });
  });

  it('a missing directory is reported, not thrown', async () => {
    await rm(dir.path, { recursive: true, force: true });
    await mkdir(dir.path);
    const report = await runDiagnostics({ directory: dir.path });
    expect(check(report, 'directory.writable')?.status).toBe('fail');
    expect(check(report, 'tmp.orphans')?.status).toBe('warn');
    expect(report.ready).toBe(false);
    vi.restoreAllMocks();
  });
});
