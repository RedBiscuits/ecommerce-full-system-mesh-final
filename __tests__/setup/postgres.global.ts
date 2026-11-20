/**
 * Vitest global setup: provide a real PostgreSQL to the opt-in real-database
 * suites (billing-credits/__tests__/postgres-concurrency.test.ts), so ONE
 * `npm test` at the repository root runs every test.
 *
 *  - TEST_DATABASE_URL already set → used as is (CI / your own server).
 *  - TEST_PG=off                   → nothing is provisioned; those suites skip.
 *  - otherwise, if the PostgreSQL server binaries are installed (initdb,
 *    pg_ctl; PATH or PG_BIN), a throwaway cluster is created in a temp
 *    directory on a free localhost port, and removed after the run.
 *    Without binaries the suites skip, as before.
 */
import { execFile, spawnSync } from 'node:child_process';
import fs from 'node:fs';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import { promisify } from 'node:util';

const run = promisify(execFile);

function findBinary(name: string): string | null {
  const dirs = [process.env.PG_BIN, ...(process.env.PATH ?? '').split(path.delimiter), '/usr/lib/postgresql/16/bin', '/usr/lib/postgresql/17/bin', '/usr/local/pgsql/bin']
    .filter((d): d is string => !!d);
  for (const dir of dirs) {
    const candidate = path.join(dir, name);
    if (fs.existsSync(candidate)) return candidate;
  }
  return null;
}

function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.unref();
    server.on('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const { port } = server.address() as net.AddressInfo;
      server.close(() => resolve(port));
    });
  });
}

/** Remove clusters left behind by a run that was killed (their postmaster is gone). */
function sweepStaleClusters(): void {
  for (const name of fs.readdirSync(os.tmpdir())) {
    if (!name.startsWith('ksa-pg-')) continue;
    const dir = path.join(os.tmpdir(), name);
    const pidFile = path.join(dir, 'postmaster.pid');
    let alive = false;
    try {
      const pid = Number(fs.readFileSync(pidFile, 'utf8').split('\n')[0]);
      if (pid > 0) {
        process.kill(pid, 0);
        alive = true;
      }
    } catch {
      alive = false;
    }
    if (!alive) fs.rmSync(dir, { recursive: true, force: true });
  }
}

export default async function setup(): Promise<(() => Promise<void>) | void> {
  if (process.env.TEST_DATABASE_URL || process.env.TEST_PG === 'off') return;
  sweepStaleClusters();

  const initdb = findBinary('initdb');
  const pgCtl = findBinary('pg_ctl');
  if (!initdb || !pgCtl) {
    console.info('[postgres] initdb/pg_ctl not found: real-PostgreSQL suites will be skipped (set TEST_DATABASE_URL to run them).');
    return;
  }
  if (typeof process.getuid === 'function' && process.getuid() === 0) {
    console.info('[postgres] running as root (PostgreSQL refuses to): real-PostgreSQL suites will be skipped.');
    return;
  }

  const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ksa-pg-'));
  const port = await freePort();
  try {
    await run(initdb, ['-D', dataDir, '-U', 'postgres', '--auth=trust', '-E', 'UTF8', '--no-sync', '--locale=C'], { timeout: 60_000 });
    await run(
      pgCtl,
      ['-D', dataDir, '-l', path.join(dataDir, 'server.log'), '-w', '-t', '60', '-o', `-p ${port} -k ${dataDir} -c listen_addresses=127.0.0.1 -c fsync=off -c max_connections=100`, 'start'],
      { timeout: 90_000 }
    );
  } catch (err) {
    console.warn(`[postgres] could not start a throwaway PostgreSQL (${(err as Error).message.split('\n')[0]}); real-PostgreSQL suites will be skipped.`);
    fs.rmSync(dataDir, { recursive: true, force: true });
    return;
  }

  process.env.TEST_DATABASE_URL = `postgres://postgres@127.0.0.1:${port}/postgres`;
  console.info(`[postgres] throwaway PostgreSQL on 127.0.0.1:${port} for the real-database suites`);

  let stopped = false;
  const stop = (): void => {
    if (stopped) return;
    stopped = true;
    spawnSync(pgCtl, ['-D', dataDir, '-m', 'immediate', 'stop'], { timeout: 30_000 });
    fs.rmSync(dataDir, { recursive: true, force: true });
    delete process.env.TEST_DATABASE_URL;
  };
  // An interrupted run (Ctrl-C, CI timeout) must not leave a server behind.
  process.once('exit', stop);
  for (const signal of ['SIGINT', 'SIGTERM'] as const) {
    process.once(signal, () => {
      stop();
      process.exit(130);
    });
  }

  return async () => stop();
}
