import { afterEach, describe, expect, it, vi } from 'vitest';

import type { DiagnosticsReport } from '../../../src/diagnostics/diagnostics';
import { createDiagnosticsMiddleware } from '../../../src/diagnostics/middleware';
import { startServer, type TestServer } from '../../helpers/http-server';

const report = (status: DiagnosticsReport['status']): DiagnosticsReport => ({
  status,
  ready: status !== 'down',
  checkedAt: 1,
  durationMs: 1,
  checks: [
    {
      name: 'directory.writable',
      status: status === 'down' ? 'fail' : 'pass',
      message: '',
      durationMs: 0,
      details: {},
    },
  ],
  alerts: [],
  metrics: undefined,
  store: undefined,
});

describe('createDiagnosticsMiddleware', () => {
  let server: TestServer | undefined;

  afterEach(async () => {
    await server?.close();
    server = undefined;
  });

  it('serves liveness, readiness and the full report; other paths fall through', async () => {
    const diagnostics = vi.fn<() => Promise<DiagnosticsReport>>(async () =>
      report('ok'),
    );
    server = await startServer(
      [createDiagnosticsMiddleware({ diagnostics })],
      (_req, res) => res.end('app'),
    );
    const { url } = server;

    const live = await fetch(`${url}/-/live`);
    expect(live.status).toBe(200);
    expect(live.headers.get('cache-control')).toBe('no-store');
    await expect(live.json()).resolves.toMatchObject({ status: 'ok' });

    const ready = await fetch(`${url}/-/ready`);
    expect(ready.status).toBe(200);
    await expect(ready.json()).resolves.toEqual({
      status: 'ok',
      ready: true,
      checks: { 'directory.writable': 'pass' },
    });

    const full = await fetch(`${url}/-/diagnostics?verbose=1`);
    expect(full.status).toBe(200);
    await expect(full.json()).resolves.toMatchObject({
      status: 'ok',
      checks: expect.any(Array),
    });

    await expect((await fetch(`${url}/other`)).text()).resolves.toBe('app');
    await expect(
      (await fetch(`${url}/-/ready`, { method: 'POST' })).text(),
    ).resolves.toBe('app');
    expect(diagnostics).toHaveBeenCalledTimes(1); // cached across the two probes
  });

  it('answers 503 when not ready and when the report itself fails', async () => {
    let fail = false;
    server = await startServer(
      [
        createDiagnosticsMiddleware({
          diagnostics: async () => {
            if (fail) {
              throw new Error('probe exploded');
            }
            return report('down');
          },
          cacheMs: 0,
        }),
      ],
      (_req, res) => res.end(),
    );
    expect((await fetch(`${server.url}/-/ready`)).status).toBe(503);
    expect((await fetch(`${server.url}/-/diagnostics`)).status).toBe(503);
    fail = true;
    const broken = await fetch(`${server.url}/-/ready`);
    expect(broken.status).toBe(503);
    await expect(broken.json()).resolves.toMatchObject({
      ready: false,
      error: expect.stringContaining('probe exploded'),
    });
    expect((await fetch(`${server.url}/-/diagnostics`)).status).toBe(500);
  });

  it('gates the full report behind authorize and supports custom paths', async () => {
    server = await startServer(
      [
        createDiagnosticsMiddleware({
          diagnostics: async () => report('degraded'),
          authorize: (req) => req.headers['x-admin'] === 'yes',
          livenessPath: '/healthz',
          readinessPath: '/readyz',
          diagnosticsPath: '/admin/diag',
        }),
      ],
      (_req, res) => res.end('app'),
    );
    expect((await fetch(`${server.url}/healthz`)).status).toBe(200);
    expect((await fetch(`${server.url}/readyz`)).status).toBe(200); // degraded is still ready
    expect((await fetch(`${server.url}/admin/diag`)).status).toBe(403);
    expect(
      (
        await fetch(`${server.url}/admin/diag`, {
          headers: { 'x-admin': 'yes' },
        })
      ).status,
    ).toBe(200);
    await expect((await fetch(`${server.url}/-/live`)).text()).resolves.toBe(
      'app',
    );
  });

  it('re-runs after the cache window and drops a rejected report from the cache', async () => {
    vi.useFakeTimers({ toFake: ['Date'] });
    let calls = 0;
    server = await startServer(
      [
        createDiagnosticsMiddleware({
          diagnostics: async () => {
            calls += 1;
            if (calls === 1) {
              throw new Error('first');
            }
            return report('ok');
          },
          cacheMs: 1_000,
        }),
      ],
      (_req, res) => res.end(),
    );
    expect((await fetch(`${server.url}/-/ready`)).status).toBe(503);
    expect((await fetch(`${server.url}/-/ready`)).status).toBe(200); // rejected report not cached
    expect((await fetch(`${server.url}/-/ready`)).status).toBe(200);
    expect(calls).toBe(2);
    vi.advanceTimersByTime(1_001);
    await fetch(`${server.url}/-/ready`);
    expect(calls).toBe(3);
    vi.useRealTimers();
  });
});
