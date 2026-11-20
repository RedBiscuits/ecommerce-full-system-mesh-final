import { writeFile } from 'node:fs/promises';
import { join } from 'node:path';

import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import { FsCacheStore } from '../../../src/cache/fs-cache-store';
import {
  formatReport,
  parseArgs,
  runCli,
} from '../../../src/diagnostics/cli';
import { makeTempDir, type TempDir } from '../../helpers/temp-dir';

describe('diagnose CLI', () => {
  let dir: TempDir;

  beforeEach(async () => {
    dir = await makeTempDir();
  });

  afterEach(async () => {
    await dir.cleanup();
  });

  it('parses arguments and rejects bad usage', () => {
    expect(parseArgs(['--dir', '/x', '--json', '--sample', '5'])).toEqual({
      directory: '/x',
      json: true,
      sampleSize: 5,
    });
    expect(parseArgs(['-d', '/x'])).toMatchObject({
      directory: '/x',
      json: false,
      sampleSize: 100,
    });
    expect(parseArgs([])).toContain('--dir is required');
    expect(parseArgs(['--dir', '/x', '--sample', 'zero'])).toContain(
      'Invalid --sample',
    );
    expect(parseArgs(['--wat'])).toContain('Unknown argument');
    expect(parseArgs(['--help'])).toContain('Usage:');
  });

  it('reports a healthy directory with exit code 0, in text and JSON', async () => {
    const store = await FsCacheStore.open({
      directory: dir.path,
      fsync: false,
    });
    await store.write('k', Buffer.from('v'), {
      createdAt: 0,
      expiresAt: 1e12,
      staleUntil: 1e12,
    });

    const text = await runCli(['--dir', dir.path]);
    expect(text.exitCode).toBe(0);
    expect(text.output).toContain('Status: OK (ready: yes)');
    expect(text.output).toContain('[PASS] store.integrity');

    const json = await runCli(['--dir', dir.path, '--json']);
    expect(json.exitCode).toBe(0);
    expect(JSON.parse(json.output)).toMatchObject({
      status: 'ok',
      ready: true,
    });
  });

  it('exits 1 on a degraded directory and lists the warning', async () => {
    await FsCacheStore.open({ directory: dir.path });
    const old = new Date(Date.now() - 3_600_000);
    await writeFile(join(dir.path, 'tmp', 'stuck.tmp'), 'x');
    const { utimes } = await import('node:fs/promises');
    await utimes(join(dir.path, 'tmp', 'stuck.tmp'), old, old);

    const result = await runCli(['--dir', dir.path]);
    expect(result.exitCode).toBe(1);
    expect(result.output).toContain('[WARN] tmp.orphans');
  });

  it('exits 0 for --help and 3 for usage errors', async () => {
    expect((await runCli(['--help'])).exitCode).toBe(0);
    expect((await runCli([])).exitCode).toBe(3);
  });

  it('formatReport renders alerts', () => {
    const output = formatReport({
      status: 'degraded',
      ready: true,
      checkedAt: 0,
      durationMs: 1,
      checks: [],
      metrics: undefined,
      store: undefined,
      alerts: [
        {
          code: 'cache.hit-ratio',
          severity: 'warning',
          message: 'low',
          value: 0.1,
          threshold: 0.2,
        },
      ],
    });
    expect(output).toContain('[WARNING] cache.hit-ratio: low (0.1 vs 0.2)');
  });
});
