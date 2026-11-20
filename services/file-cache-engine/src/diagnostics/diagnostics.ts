import { randomBytes } from 'node:crypto';
import { readdir, rm, stat, statfs, writeFile } from 'node:fs/promises';
import { join } from 'node:path';

import type { ExpirySweeper } from '../cache/expiry-sweeper';
import type { TieredCacheStoreStats } from '../cache/tiered-cache-store';
import type { FsCacheStore } from '../cache/fs-cache-store';
import type { StoreHealth } from '../resilience/resilient-store';
import type { Clock } from '../shared/clock';
import { systemClock } from '../shared/clock';
import type { Alert, AlertThresholds } from '../telemetry/alerts';
import { evaluateAlerts } from '../telemetry/alerts';
import { describeError } from '../telemetry/events';
import type { MetricsSnapshot } from '../telemetry/metrics';

export type CheckStatus = 'pass' | 'warn' | 'fail';

export interface DiagnosticCheck {
  readonly name: string;
  readonly status: CheckStatus;
  readonly message: string;
  readonly durationMs: number;
  readonly details: Readonly<Record<string, unknown>>;
}

/** `ok` → all good; `degraded` → serving with warnings; `down` → not ready. */
export type OverallStatus = 'ok' | 'degraded' | 'down';

export interface DiagnosticsReport {
  readonly status: OverallStatus;
  /** `true` unless a check failed: the cache can serve (possibly degraded). */
  readonly ready: boolean;
  readonly checkedAt: number;
  readonly durationMs: number;
  readonly checks: readonly DiagnosticCheck[];
  readonly alerts: readonly Alert[];
  readonly metrics: MetricsSnapshot | undefined;
  readonly store: StoreHealth | undefined;
}

export interface DiagnosticsOptions {
  /** Cache root. Enables directory, disk, temp-file and lock checks. */
  readonly directory?: string;
  /** Enables the integrity sample scan. */
  readonly store?: FsCacheStore;
  readonly sweeper?: ExpirySweeper;
  /** Expected sweep interval; the sweeper is "stale" after 3× this. */
  readonly sweepIntervalMs?: number;
  readonly storeHealth?: () => StoreHealth | undefined;
  /** Tier counters: enables the `tiering` check (L2 circuit, buffer). */
  readonly tiering?: () => TieredCacheStoreStats | undefined;
  /** Write-behind buffer fill ratio that warns. Default: `0.8`. */
  readonly writeBufferWarnRatio?: number;
  readonly metrics?: () => MetricsSnapshot;
  readonly thresholds?: AlertThresholds;
  /** Entries inspected by the integrity sample. Default: `100`. */
  readonly integritySampleSize?: number;
  /** Free-space ratio below which the disk check warns / fails. */
  readonly minFreeDiskRatio?: {
    readonly warn?: number;
    readonly fail?: number;
  };
  /** Temp files older than this count as stuck writes. Default: 5 min. */
  readonly tempFileMaxAgeMs?: number;
  /** Upper bound for the whole run. Default: 10 s. */
  readonly timeoutMs?: number;
  readonly clock?: Clock;
}

const TMP_DIR = 'tmp';
const SWEEP_LOCK = 'sweep.lock';

/**
 * Runs every applicable health check and folds the result into one report.
 * Checks are independent: a throwing check becomes a `fail` entry and the
 * others still run. Safe to call from a readiness probe — the only writes
 * are a probe file in `tmp/` that is removed again.
 */
export async function runDiagnostics(
  options: DiagnosticsOptions = {},
): Promise<DiagnosticsReport> {
  const clock = options.clock ?? systemClock;
  const startedAt = clock.now();
  const started = performance.now();
  const timeoutMs = options.timeoutMs ?? 10_000;

  const checks: Promise<DiagnosticCheck>[] = [];
  const run = (
    name: string,
    task: () => Promise<Omit<DiagnosticCheck, 'name' | 'durationMs'>>,
  ): void => {
    checks.push(timed(name, task, timeoutMs));
  };

  if (options.directory !== undefined) {
    const directory = options.directory;
    run('directory.writable', () => checkWritable(directory));
    run('disk.space', () =>
      checkDiskSpace(directory, options.minFreeDiskRatio),
    );
    run('tmp.orphans', () =>
      checkTempFiles(directory, options.tempFileMaxAgeMs ?? 5 * 60_000, clock),
    );
    run('sweeper.lock', () => checkLock(directory, clock));
  }
  if (options.store !== undefined) {
    const store = options.store;
    run('store.integrity', () =>
      checkIntegrity(store, options.integritySampleSize ?? 100),
    );
  }
  if (options.storeHealth !== undefined) {
    const health = options.storeHealth;
    run('store.circuit', async () => checkCircuit(health()));
  }
  if (options.tiering !== undefined) {
    const tiering = options.tiering;
    run('tiering', async () =>
      checkTiering(tiering(), options.writeBufferWarnRatio ?? 0.8, clock),
    );
  }
  if (options.sweeper !== undefined) {
    const sweeper = options.sweeper;
    run('sweeper', async () =>
      checkSweeper(sweeper, options.sweepIntervalMs, clock),
    );
  }

  const metrics = options.metrics?.();
  const alerts =
    metrics === undefined ? [] : evaluateAlerts(metrics, options.thresholds);
  if (metrics !== undefined) {
    run('metrics.alerts', async () => ({
      status: alerts.some((a) => a.severity === 'critical')
        ? 'fail'
        : alerts.length > 0
          ? 'warn'
          : 'pass',
      message:
        alerts.length === 0
          ? 'No alerts'
          : `${alerts.length} alert(s): ${alerts.map((a) => a.code).join(', ')}`,
      details: { alerts: alerts.length },
    }));
  }

  const results = await Promise.all(checks);
  const status: OverallStatus = results.some((c) => c.status === 'fail')
    ? 'down'
    : results.some((c) => c.status === 'warn')
      ? 'degraded'
      : 'ok';

  return {
    status,
    ready: status !== 'down',
    checkedAt: startedAt,
    durationMs: Math.round(performance.now() - started),
    checks: results,
    alerts,
    metrics,
    store: options.storeHealth?.(),
  };
}

// --- checks -------------------------------------------------------------------

type CheckBody = Omit<DiagnosticCheck, 'name' | 'durationMs'>;

async function timed(
  name: string,
  task: () => Promise<CheckBody>,
  timeoutMs: number,
): Promise<DiagnosticCheck> {
  const started = performance.now();
  let timer: NodeJS.Timeout | undefined;
  const timeout = new Promise<CheckBody>((resolve) => {
    timer = setTimeout(
      () =>
        resolve({
          status: 'fail',
          message: `Check did not complete within ${timeoutMs} ms`,
          details: {},
        }),
      timeoutMs,
    );
    timer.unref();
  });
  try {
    const body = await Promise.race([task(), timeout]);
    return {
      name,
      ...body,
      durationMs: Math.round(performance.now() - started),
    };
  } catch (error) {
    return {
      name,
      status: 'fail',
      message: describeError(error),
      details: {},
      durationMs: Math.round(performance.now() - started),
    };
  } finally {
    clearTimeout(timer);
  }
}

async function checkWritable(directory: string): Promise<CheckBody> {
  const probe = join(
    directory,
    TMP_DIR,
    `.probe-${randomBytes(4).toString('hex')}`,
  );
  try {
    await writeFile(probe, 'ok', { mode: 0o600 });
    const info = await stat(probe);
    await rm(probe, { force: true });
    return {
      status: 'pass',
      message: 'Cache directory is writable',
      details: { directory, probeBytes: info.size },
    };
  } catch (error) {
    await rm(probe, { force: true }).catch(() => undefined);
    return {
      status: 'fail',
      message: `Cache directory is not writable: ${describeError(error)}`,
      details: { directory },
    };
  }
}

async function checkDiskSpace(
  directory: string,
  thresholds: DiagnosticsOptions['minFreeDiskRatio'],
): Promise<CheckBody> {
  const warnBelow = thresholds?.warn ?? 0.1;
  const failBelow = thresholds?.fail ?? 0.02;
  const fs = await statfs(directory);
  const total = fs.blocks * fs.bsize;
  const free = fs.bavail * fs.bsize;
  const freeRatio = total === 0 ? 1 : free / total;
  const details = {
    totalBytes: total,
    freeBytes: free,
    freeRatio: Math.round(freeRatio * 1000) / 1000,
  };
  if (freeRatio < failBelow) {
    return { status: 'fail', message: 'Disk is (almost) full', details };
  }
  if (freeRatio < warnBelow) {
    return { status: 'warn', message: 'Disk space is running low', details };
  }
  return { status: 'pass', message: 'Disk space is sufficient', details };
}

async function checkTempFiles(
  directory: string,
  maxAgeMs: number,
  clock: Clock,
): Promise<CheckBody> {
  const tmpDir = join(directory, TMP_DIR);
  let names: string[];
  try {
    names = await readdir(tmpDir);
  } catch {
    return {
      status: 'warn',
      message: 'Temp directory is missing',
      details: { tmpDir },
    };
  }
  const tmpFiles = names.filter((name) => name.endsWith('.tmp'));
  let stuck = 0;
  for (const name of tmpFiles) {
    const info = await stat(join(tmpDir, name)).catch(() => undefined);
    if (info !== undefined && clock.now() - info.mtimeMs > maxAgeMs) {
      stuck += 1;
    }
  }
  const details = { tmpFiles: tmpFiles.length, stuck };
  return stuck > 0
    ? {
        status: 'warn',
        message: `${stuck} temp file(s) look stuck (writer died mid-write?)`,
        details,
      }
    : { status: 'pass', message: 'No stuck temp files', details };
}

async function checkLock(directory: string, clock: Clock): Promise<CheckBody> {
  const path = join(directory, SWEEP_LOCK);
  const info = await stat(path).catch(() => undefined);
  if (info === undefined) {
    return {
      status: 'pass',
      message: 'No sweep in progress',
      details: { held: false },
    };
  }
  const ageMs = Math.max(0, Math.round(clock.now() - info.mtimeMs));
  return ageMs > 10 * 60_000
    ? {
        status: 'warn',
        message: 'Sweep lock looks abandoned',
        details: { held: true, ageMs },
      }
    : {
        status: 'pass',
        message: 'Sweep lock is held by an active sweep',
        details: { held: true, ageMs },
      };
}

async function checkIntegrity(
  store: FsCacheStore,
  sampleSize: number,
): Promise<CheckBody> {
  const quarantinedBefore = store.quarantinedCount;
  let scanned = 0;
  let bytes = 0;
  for await (const metadata of store.scan()) {
    scanned += 1;
    bytes += metadata.size;
    if (scanned >= sampleSize) {
      break;
    }
  }
  const corrupt = store.quarantinedCount - quarantinedBefore;
  const details = { scanned, corrupt, sampledBytes: bytes };
  return corrupt > 0
    ? {
        status: 'warn',
        message: `${corrupt} corrupt entr(y/ies) quarantined during sample`,
        details,
      }
    : {
        status: 'pass',
        message: `Sampled ${scanned} entr(y/ies), all valid`,
        details,
      };
}

function checkCircuit(health: StoreHealth | undefined): CheckBody {
  const circuit = health?.circuit;
  if (circuit === undefined) {
    return {
      status: 'pass',
      message: 'No circuit breaker configured',
      details: {},
    };
  }
  const details = {
    state: circuit.state,
    consecutiveFailures: circuit.consecutiveFailures,
    lastFailure:
      health?.lastFailure === undefined
        ? undefined
        : { at: health.lastFailure.at, kind: health.lastFailure.kind },
  };
  if (circuit.state === 'open') {
    return {
      status: 'fail',
      message: 'Store circuit is open: cache is bypassed',
      details,
    };
  }
  if (circuit.state === 'half-open') {
    return {
      status: 'warn',
      message: 'Store circuit is half-open: probing recovery',
      details,
    };
  }
  return { status: 'pass', message: 'Store circuit is closed', details };
}

/**
 * An open L2 circuit is `warn`, not `fail`: the cache keeps serving from
 * memory (L1-only mode), so the instance stays ready. Taking it out of
 * rotation would turn a degraded cache into an outage.
 */
function checkTiering(
  stats: TieredCacheStoreStats | undefined,
  warnRatio: number,
  clock: Clock,
): CheckBody {
  if (stats === undefined) {
    return { status: 'pass', message: 'Tiering is off', details: {} };
  }
  const fill = stats.pendingWrites / stats.maxPendingWrites;
  const circuit = stats.circuit;
  const details = {
    circuit: circuit?.state,
    consecutiveFailures: circuit?.consecutiveFailures,
    retryInMs:
      circuit?.retryAt === undefined
        ? undefined
        : Math.max(0, circuit.retryAt - clock.now()),
    pendingWrites: stats.pendingWrites,
    maxPendingWrites: stats.maxPendingWrites,
    l1OnlyWrites: stats.l1OnlyWrites,
  };
  if (circuit?.state === 'open') {
    return {
      status: 'warn',
      message: 'Disk circuit is open: serving from memory only (L1-only mode)',
      details,
    };
  }
  if (circuit?.state === 'half-open') {
    return {
      status: 'warn',
      message: 'Disk circuit is half-open: probing the disk',
      details,
    };
  }
  if (fill >= warnRatio) {
    return {
      status: 'warn',
      message: `Write-behind buffer is ${Math.round(fill * 100)} % full`,
      details,
    };
  }
  return { status: 'pass', message: 'Tiers are healthy', details };
}

function checkSweeper(
  sweeper: ExpirySweeper,
  intervalMs: number | undefined,
  clock: Clock,
): CheckBody {
  const last = sweeper.lastReport;
  const details = {
    running: sweeper.isRunning,
    lastSweep:
      last === undefined
        ? undefined
        : {
            completedAt: last.completedAt,
            expired: last.expired,
            evicted: last.evicted,
            failed: last.failed,
            skipped: last.skipped,
          },
  };
  if (!sweeper.isRunning) {
    return {
      status: 'warn',
      message: 'Sweeper is not running; cold entries are not reclaimed',
      details,
    };
  }
  if (
    intervalMs !== undefined &&
    last !== undefined &&
    clock.now() - last.completedAt > intervalMs * 3
  ) {
    return { status: 'warn', message: 'No sweep completed recently', details };
  }
  if (last !== undefined && last.failed > 0) {
    return {
      status: 'warn',
      message: `Last sweep failed to delete ${last.failed} entr(y/ies)`,
      details,
    };
  }
  return { status: 'pass', message: 'Sweeper is healthy', details };
}
