/**
 * Run EVERY test of the monorepo, in parallel per service.
 *
 *   npm test                 all groups in parallel (one vitest process per service)
 *   npm test -- <filter…>    plain `vitest run <filter…>` in this process (ad-hoc runs)
 *   npm run test:serial      everything in ONE vitest process, one file after another
 *
 * Each group (a service, `shared`, and the platform tests in __tests__/) runs in its own vitest
 * process with the repository's vitest config — files inside a group stay sequential exactly as
 * before (the suites assume it), groups run side by side. Groups are discovered from the test
 * files themselves, so a new service gets its own group automatically.
 *
 * PostgreSQL for the real-database suites is provisioned ONCE (see __tests__/setup/postgres.global.ts)
 * and every group gets its own database on it, so groups can never interfere.
 *
 * A test file that fails under parallel load is re-run ONCE, alone, after the parallel phase (the
 * machine is quiet then). If it passes it is reported as flaky under load; if it fails again the
 * run fails. TEST_NO_RERUN=1 disables the re-run.
 *
 * Env: TEST_CONCURRENCY (default: CPU count), TEST_PG=off, TEST_NO_RERUN=1.
 */
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { Client } from 'pg';
import provisionPostgres from '../__tests__/setup/postgres.global';

const ROOT = path.resolve(__dirname, '..');
const VITEST = path.join(ROOT, 'node_modules', 'vitest', 'vitest.mjs');

interface Group {
  name: string;
  files: string[];
}

/** A slice of a group's files run by one vitest process (heavy services are split in several). */
interface Shard {
  group: string;
  index: number;
  files: string[];
  weight: number;
}

const TIMINGS_FILE = path.join(ROOT, 'node_modules', '.cache', 'ksa-test-timings.json');

/** Seconds a file took last time (from the previous run's JSON reports); a guess otherwise. */
function loadTimings(): Record<string, number> {
  try {
    return JSON.parse(fs.readFileSync(TIMINGS_FILE, 'utf8')) as Record<string, number>;
  } catch {
    return {};
  }
}

function saveTimings(timings: Record<string, number>): void {
  try {
    fs.mkdirSync(path.dirname(TIMINGS_FILE), { recursive: true });
    fs.writeFileSync(TIMINGS_FILE, JSON.stringify(timings, null, 2));
  } catch {
    // best effort
  }
}

function guessSeconds(file: string): number {
  if (file.startsWith('__tests__/platform')) return 40;
  if (/services\/(wallet-ledger|billing-credits)\//.test(file)) return 12;
  if (file.includes('postgres')) return 30;
  return 3;
}

/**
 * Split groups into shards of roughly `target` seconds (files stay whole and keep their order
 * within a shard), then order all shards longest first: a greedy longest-processing-time schedule
 * over the worker slots keeps every core busy and the slowest work off the tail.
 */
export function planShards(groups: Group[], timings: Record<string, number>, concurrency: number): Shard[] {
  // A file costs its share of its last shard's wall time (test bodies + start-up, collection and
  // cold starts, see runShard); a shard also pays a fixed process start-up, so shards are kept
  // large: just enough of them to fill the worker slots twice.
  const SHARD_OVERHEAD = 10;
  const cost = (f: string) => timings[f] ?? guessSeconds(f);
  const total = groups.reduce((s, g) => s + g.files.reduce((t, f) => t + cost(f), 0), 0);
  const target = Math.max(SHARD_OVERHEAD * 6, total / (concurrency * 2));
  const shards: Shard[] = [];
  for (const group of groups) {
    const groupCost = group.files.reduce((t, f) => t + cost(f), 0);
    const count = Math.max(1, Math.min(group.files.length, Math.round(groupCost / target)));
    const bins: Shard[] = Array.from({ length: count }, (_, index) => ({ group: group.name, index, files: [], weight: 0 }));
    for (const file of [...group.files].sort((a, b) => cost(b) - cost(a))) {
      const lightest = bins.reduce((min, b) => (b.weight < min.weight ? b : min), bins[0]);
      lightest.files.push(file);
      lightest.weight += cost(file);
    }
    for (const bin of bins) bin.files.sort();
    shards.push(...bins);
  }
  return shards.sort((a, b) => b.weight - a.weight);
}

interface GroupResult {
  name: string;
  files: number;
  tests: number;
  passed: number;
  failed: number;
  skipped: number;
  durationMs: number;
  exitCode: number;
  output: string;
}

/** Heaviest groups first (longest-processing-time scheduling keeps the slowest group off the tail). */
const WEIGHT: Record<string, number> = {
  platform: 100,
  'billing-credits': 90,
  'wallet-ledger': 85,
  'file-cache-engine': 50,
  'zatca-tax': 40,
  'checkout-coordinator': 20,
  'hyperpay-gateway': 15,
  shared: 10,
};

function walk(dir: string, out: string[] = []): string[] {
  if (!fs.existsSync(dir)) return out;
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    if (entry.name === 'node_modules' || entry.name === 'dist' || entry.name.startsWith('.')) continue;
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) walk(full, out);
    else if (entry.name.endsWith('.test.ts')) out.push(path.relative(ROOT, full).split(path.sep).join('/'));
  }
  return out;
}

/** Same include patterns as vitest.config.ts, grouped by service. */
export function discoverGroups(): Group[] {
  const files = [
    ...walk(path.join(ROOT, '__tests__')),
    ...walk(path.join(ROOT, 'shared')).filter((f) => f.includes('/__tests__/')),
    ...walk(path.join(ROOT, 'services')).filter((f) => f.includes('/__tests__/')),
  ];
  const groups = new Map<string, string[]>();
  for (const file of files) {
    const name = file.startsWith('services/') ? file.split('/')[1] : file.startsWith('shared/') ? 'shared' : 'platform';
    groups.set(name, [...(groups.get(name) ?? []), file]);
  }
  return [...groups.entries()]
    .map(([name, groupFiles]) => ({ name, files: groupFiles.sort() }))
    .sort((a, b) => (WEIGHT[b.name] ?? b.files.length) - (WEIGHT[a.name] ?? a.files.length));
}

async function createDatabases(baseUrl: string, names: string[]): Promise<Map<string, string>> {
  const urls = new Map<string, string>();
  const client = new Client({ connectionString: baseUrl });
  await client.connect();
  try {
    for (const name of names) {
      const db = `ksa_${name.replace(/[^a-z0-9]/gi, '_').toLowerCase()}`;
      await client.query(`DROP DATABASE IF EXISTS ${db}`);
      await client.query(`CREATE DATABASE ${db}`);
      const url = new URL(baseUrl);
      url.pathname = `/${db}`;
      urls.set(name, url.toString());
    }
  } finally {
    await client.end();
  }
  return urls;
}

interface ShardRun extends GroupResult {
  fileSeconds: Record<string, number>;
  /** Test files with at least one failure (or that did not run at all). */
  failedFiles: string[];
}

function runShard(group: Shard, env: NodeJS.ProcessEnv): Promise<ShardRun> {
  const report = path.join(os.tmpdir(), `ksa-vitest-${process.pid}-${group.group}-${group.index}.json`);
  const started = Date.now();
  return new Promise((resolve) => {
    const child = spawn(
      process.execPath,
      [VITEST, 'run', ...group.files, '--reporter=default', '--reporter=json', `--outputFile.json=${report}`],
      { cwd: ROOT, env: { ...process.env, ...env, FORCE_COLOR: '0' }, stdio: ['ignore', 'pipe', 'pipe'] }
    );
    let output = '';
    child.stdout.on('data', (d: Buffer) => (output += d.toString()));
    child.stderr.on('data', (d: Buffer) => (output += d.toString()));
    child.on('close', (code) => {
      let counts = { tests: 0, passed: 0, failed: 0, skipped: 0, files: group.files.length };
      const fileSeconds: Record<string, number> = {};
      let failedFiles: string[] = [];
      try {
        const json = JSON.parse(fs.readFileSync(report, 'utf8')) as {
          numTotalTests: number;
          numPassedTests: number;
          numFailedTests: number;
          numPendingTests: number;
          numTodoTests: number;
          testResults: Array<{ name: string; startTime: number; endTime: number; status: string }>;
        };
        failedFiles = json.testResults
          .filter((r) => r.status !== 'passed')
          .map((r) => path.relative(ROOT, r.name).split(path.sep).join('/'));
        const reported = new Set(json.testResults.map((r) => path.relative(ROOT, r.name).split(path.sep).join('/')));
        failedFiles.push(...group.files.filter((f) => !reported.has(f)));
        for (const r of json.testResults) {
          fileSeconds[path.relative(ROOT, r.name).split(path.sep).join('/')] = Math.max(0.1, (r.endTime - r.startTime) / 1000);
        }
        counts = {
          tests: json.numTotalTests,
          passed: json.numPassedTests,
          failed: json.numFailedTests,
          skipped: json.numPendingTests + json.numTodoTests,
          files: json.testResults.length,
        };
        fs.rmSync(report, { force: true });
      } catch {
        // no report: the run crashed; the exit code says so
      }
      // Self-calibrating costs: the shard's wall time not explained by test bodies (start-up,
      // module collection / transformation, cold starts) is shared out over its files.
      const durationMs = Date.now() - started;
      const measured = Object.values(fileSeconds).reduce((a, b) => a + b, 0);
      const files = Object.keys(fileSeconds);
      const overhead = files.length > 0 ? Math.max(0, durationMs / 1000 - measured) / files.length : 0;
      for (const f of files) fileSeconds[f] += overhead;
      if ((code ?? 1) !== 0 && failedFiles.length === 0) failedFiles = [...group.files];
      resolve({ name: group.group, ...counts, durationMs, exitCode: code ?? 1, output, fileSeconds, failedFiles });
    });
  });
}

async function runAll(): Promise<number> {
  const groups = discoverGroups();
  const concurrency = Math.max(1, Number(process.env.TEST_CONCURRENCY) || os.cpus().length);
  const timings = loadTimings();
  const shards = planShards(groups, timings, concurrency);
  console.log(
    `Running ${groups.reduce((n, g) => n + g.files.length, 0)} test files of ${groups.length} groups (${groups.map((g) => g.name).join(', ')}) ` +
      `as ${shards.length} parallel shards, ${concurrency} at a time`
  );

  // One PostgreSQL for the whole run; a database per shard, so no two processes share one.
  const teardown = await provisionPostgres();
  let databases = new Map<string, string>();
  const shardKey = (sh: Shard) => `${sh.group}_${sh.index}`;
  if (process.env.TEST_DATABASE_URL) {
    try {
      databases = await createDatabases(process.env.TEST_DATABASE_URL, shards.map(shardKey));
    } catch (err) {
      console.warn(`[postgres] could not create per-shard databases (${(err as Error).message}); real-database suites share one`);
    }
  }

  const started = Date.now();
  const runs: ShardRun[] = [];
  const queue = [...shards];
  const workers = Array.from({ length: Math.min(concurrency, queue.length) }, async () => {
    for (let shard = queue.shift(); shard; shard = queue.shift()) {
      const env: NodeJS.ProcessEnv = {};
      if (process.env.TEST_DATABASE_URL) env.TEST_DATABASE_URL = databases.get(shardKey(shard)) ?? process.env.TEST_DATABASE_URL;
      const run = await runShard(shard, env);
      runs.push(run);
      Object.assign(timings, run.fileSeconds);
    }
  });
  await Promise.all(workers);

  // Failed files get one quiet re-run each, alone; only a second failure fails the run.
  const flaky: string[] = [];
  for (const run of runs.filter((r) => r.exitCode !== 0 || r.failed > 0)) {
    const shard = shards.find((sh) => sh.group === run.name && run.failedFiles.every((f) => sh.files.includes(f)));
    if (process.env.TEST_NO_RERUN === '1' || run.failedFiles.length === 0 || !shard) {
      console.log(`\n──────── ${run.name} output ────────\n${run.output}\n──────── end ${run.name} ────────\n`);
      continue;
    }
    console.log(`↻ re-running ${run.failedFiles.length} failed file(s) of ${run.name} alone: ${run.failedFiles.join(', ')}`);
    const env: NodeJS.ProcessEnv = {};
    if (process.env.TEST_DATABASE_URL) env.TEST_DATABASE_URL = databases.get(shardKey(shard)) ?? process.env.TEST_DATABASE_URL;
    const rerun = await runShard({ ...shard, files: run.failedFiles }, env);
    if (rerun.exitCode === 0 && rerun.failed === 0) {
      flaky.push(...run.failedFiles);
      run.passed += run.failed;
      run.failed = 0;
      run.exitCode = 0;
    } else {
      console.log(`\n──────── ${run.name} output (parallel run) ────────\n${run.output}`);
      console.log(`──────── ${run.name} output (re-run alone) ────────\n${rerun.output}\n──────── end ${run.name} ────────\n`);
    }
  }
  if (typeof teardown === 'function') await teardown();
  saveTimings(timings);

  // Report per service (group), whatever the sharding.
  const results: GroupResult[] = groups.map((g) => {
    const own = runs.filter((r) => r.name === g.name);
    return {
      name: g.name,
      files: own.reduce((s, r) => s + r.files, 0),
      tests: own.reduce((s, r) => s + r.tests, 0),
      passed: own.reduce((s, r) => s + r.passed, 0),
      failed: own.reduce((s, r) => s + r.failed, 0),
      skipped: own.reduce((s, r) => s + r.skipped, 0),
      durationMs: own.reduce((s, r) => s + r.durationMs, 0),
      exitCode: own.some((r) => r.exitCode !== 0) ? 1 : 0,
      output: '',
    };
  });
  for (const r of results) {
    const ok = r.exitCode === 0 && r.failed === 0;
    const shardCount = runs.filter((x) => x.name === r.name).length;
    console.log(
      `${ok ? '✓' : '✗'} ${r.name.padEnd(22)} ${String(r.files).padStart(3)} files  ${String(r.passed).padStart(4)} passed` +
        `${r.failed ? `  ${r.failed} FAILED` : ''}${r.skipped ? `  ${r.skipped} skipped` : ''}  ` +
        `${(r.durationMs / 1000).toFixed(1)}s${shardCount > 1 ? ` over ${shardCount} shards` : ''}`
    );
  }

  const total = (k: 'files' | 'tests' | 'passed' | 'failed' | 'skipped') => results.reduce((s, r) => s + r[k], 0);
  const failedGroups = results.filter((r) => r.exitCode !== 0 || r.failed > 0);
  console.log(
    `\n Test Files  ${total('files')} (${results.length} groups, ${runs.length} shards)\n      Tests  ${total('passed')} passed` +
      `${total('failed') ? ` | ${total('failed')} failed` : ''}${total('skipped') ? ` | ${total('skipped')} skipped` : ''} (${total('tests')})\n` +
      `   Duration  ${((Date.now() - started) / 1000).toFixed(1)}s wall clock (${(runs.reduce((s, r) => s + r.durationMs, 0) / 1000).toFixed(1)}s of shard time)\n`
  );
  if (flaky.length > 0) {
    console.log(`⚠ flaky under parallel load (failed in parallel, passed when re-run alone): ${flaky.join(', ')}`);
  }
  if (failedGroups.length > 0) console.log(`FAILED: ${failedGroups.map((g) => g.name).join(', ')}`);
  return failedGroups.length > 0 ? 1 : 0;
}

function runPlain(args: string[]): Promise<number> {
  return new Promise((resolve) => {
    const child = spawn(process.execPath, [VITEST, 'run', ...args], { cwd: ROOT, stdio: 'inherit' });
    child.on('close', (code) => resolve(code ?? 1));
  });
}

if (require.main === module) {
  const args = process.argv.slice(2);
  (args.length > 0 ? runPlain(args) : runAll())
    .then((code) => process.exit(code))
    .catch((err) => {
      console.error(err);
      process.exit(1);
    });
}
