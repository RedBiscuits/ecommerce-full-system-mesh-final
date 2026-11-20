import type {
  IncomingMessage,
  Middleware,
  ServerResponse,
} from '../http/types';
import { describeError } from '../telemetry/events';
import type { DiagnosticsReport } from './diagnostics';

export interface DiagnosticsMiddlewareOptions {
  /** Produces the full report (typically `fileCache.diagnostics`). */
  readonly diagnostics: () => Promise<DiagnosticsReport>;
  /** Liveness: "the process is up". Default: `/-/live`. */
  readonly livenessPath?: string;
  /** Readiness: 200 when `report.ready`, 503 otherwise. Default: `/-/ready`. */
  readonly readinessPath?: string;
  /** Full JSON report. Default: `/-/diagnostics`. */
  readonly diagnosticsPath?: string;
  /**
   * Gate for the full report (it includes routes, client keys and error
   * messages). Default: allow. Return `false` to answer 403.
   */
  readonly authorize?: (req: IncomingMessage) => boolean;
  /** Cache the report for this long to absorb probe storms. Default: 2 s. */
  readonly cacheMs?: number;
}

/**
 * Serves liveness, readiness and a diagnostics report. Readiness reflects
 * whether the cache can serve at all; a degraded cache is still ready.
 * Any other path falls through to `next()`.
 */
export function createDiagnosticsMiddleware(
  options: DiagnosticsMiddlewareOptions,
): Middleware {
  const livenessPath = options.livenessPath ?? '/-/live';
  const readinessPath = options.readinessPath ?? '/-/ready';
  const diagnosticsPath = options.diagnosticsPath ?? '/-/diagnostics';
  const authorize = options.authorize ?? ((): boolean => true);
  const cacheMs = options.cacheMs ?? 2_000;

  let cached:
    | { readonly at: number; readonly report: Promise<DiagnosticsReport> }
    | undefined;
  const report = (): Promise<DiagnosticsReport> => {
    const now = Date.now();
    if (cached === undefined || now - cached.at > cacheMs) {
      const pending = options.diagnostics();
      cached = { at: now, report: pending };
      pending.catch(() => {
        cached = undefined;
      });
    }
    return cached.report;
  };

  return (req, res, next): void => {
    const path = (req.url ?? '/').split('?')[0];
    if (req.method !== 'GET' && req.method !== 'HEAD') {
      next();
      return;
    }
    if (path === livenessPath) {
      json(res, 200, {
        status: 'ok',
        uptimeMs: Math.round(process.uptime() * 1000),
      });
      return;
    }
    if (path === readinessPath) {
      report().then(
        (r) =>
          json(res, r.ready ? 200 : 503, {
            status: r.status,
            ready: r.ready,
            checks: summarise(r),
          }),
        (error: unknown) =>
          json(res, 503, {
            status: 'down',
            ready: false,
            error: describeError(error),
          }),
      );
      return;
    }
    if (path === diagnosticsPath) {
      if (!authorize(req)) {
        json(res, 403, { error: 'Forbidden' });
        return;
      }
      report().then(
        (r) => json(res, r.ready ? 200 : 503, r),
        (error: unknown) => json(res, 500, { error: describeError(error) }),
      );
      return;
    }
    next();
  };
}

function summarise(report: DiagnosticsReport): Record<string, string> {
  const out: Record<string, string> = {};
  for (const check of report.checks) {
    out[check.name] = check.status;
  }
  return out;
}

function json(res: ServerResponse, status: number, body: unknown): void {
  if (res.headersSent) {
    return;
  }
  res.statusCode = status;
  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  res.setHeader('Cache-Control', 'no-store');
  res.end(JSON.stringify(body));
}
