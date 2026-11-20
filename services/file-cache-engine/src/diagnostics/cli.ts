import { FsCacheStore } from '../cache/fs-cache-store';
import type { DiagnosticsReport } from './diagnostics';
import { runDiagnostics } from './diagnostics';

export interface CliOptions {
  readonly directory: string;
  readonly json: boolean;
  readonly sampleSize: number;
}

export interface CliResult {
  /** `0` ok, `1` degraded, `2` down, `3` usage error. */
  readonly exitCode: number;
  readonly output: string;
}

const USAGE = `Usage: diagnose --dir <cache-directory> [--json] [--sample <n>]

Checks a cache directory offline: writability, disk space, stuck temp
files, sweep lock, and an integrity sample. Exit code: 0 ok, 1 degraded,
2 down, 3 usage error.`;

/** Parses argv (without the node/script prefix). */
export function parseArgs(argv: readonly string[]): CliOptions | string {
  let directory: string | undefined;
  let json = false;
  let sampleSize = 100;
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    switch (arg) {
      case '--dir':
      case '-d':
        directory = argv[++i];
        break;
      case '--json':
        json = true;
        break;
      case '--sample': {
        const value = Number.parseInt(argv[++i] ?? '', 10);
        if (!Number.isInteger(value) || value <= 0) {
          return 'Invalid --sample value';
        }
        sampleSize = value;
        break;
      }
      case '--help':
      case '-h':
        return USAGE;
      default:
        return `Unknown argument: ${String(arg)}\n\n${USAGE}`;
    }
  }
  if (directory === undefined || directory.length === 0) {
    return `--dir is required\n\n${USAGE}`;
  }
  return { directory, json, sampleSize };
}

/** Human-readable rendering of a report. */
export function formatReport(report: DiagnosticsReport): string {
  const icon = { pass: 'PASS', warn: 'WARN', fail: 'FAIL' } as const;
  const lines = [
    `Status: ${report.status.toUpperCase()} (ready: ${report.ready ? 'yes' : 'no'}) – ${report.checks.length} check(s) in ${report.durationMs} ms`,
    '',
  ];
  for (const check of report.checks) {
    lines.push(
      `  [${icon[check.status]}] ${check.name.padEnd(20)} ${check.message}`,
    );
    const details = Object.entries(check.details).filter(
      ([, v]) => v !== undefined,
    );
    if (details.length > 0) {
      lines.push(
        `         ${details.map(([k, v]) => `${k}=${JSON.stringify(v)}`).join('  ')}`,
      );
    }
  }
  if (report.alerts.length > 0) {
    lines.push('', 'Alerts:');
    for (const alert of report.alerts) {
      lines.push(
        `  [${alert.severity.toUpperCase()}] ${alert.code}: ${alert.message} (${String(alert.value)} vs ${String(alert.threshold)})`,
      );
    }
  }
  return lines.join('\n');
}

/** Runs the offline diagnostics for a directory and renders the result. */
export async function runCli(argv: readonly string[]): Promise<CliResult> {
  const parsed = parseArgs(argv);
  if (typeof parsed === 'string') {
    return { exitCode: parsed === USAGE ? 0 : 3, output: parsed };
  }
  // Observe, don't repair: keep orphaned temp files so they show up.
  const store = await FsCacheStore.open({
    directory: parsed.directory,
    purgeTempFiles: false,
  });
  const report = await runDiagnostics({
    directory: parsed.directory,
    store,
    integritySampleSize: parsed.sampleSize,
  });
  const exitCode =
    report.status === 'ok' ? 0 : report.status === 'degraded' ? 1 : 2;
  return {
    exitCode,
    output: parsed.json
      ? JSON.stringify(report, null, 2)
      : formatReport(report),
  };
}
