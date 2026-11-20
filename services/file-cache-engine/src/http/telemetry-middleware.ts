import type { TelemetrySink } from '../telemetry/sink';
import { safeSink } from '../telemetry/sink';
import { getCacheStatus } from './cache-middleware';
import { getClientIdentity } from './client-identity';
import type { IncomingMessage, Middleware, ServerResponse } from './types';

export interface TelemetryMiddlewareOptions {
  readonly telemetry: TelemetrySink;
  /**
   * Maps a request to its API/route template so latencies aggregate per
   * endpoint rather than per URL. Default: {@link defaultRoute}, which
   * collapses numeric, UUID and long hex path segments into `:id`. Express
   * users can return `req.baseUrl + req.route.path` instead.
   */
  readonly routeResolver?: (req: IncomingMessage) => string;
}

const routes = new WeakMap<IncomingMessage, string>();

/** The route label assigned to `req` by the telemetry middleware. */
export function routeOf(req: IncomingMessage): string | undefined {
  return routes.get(req);
}

/** `GET /users/:id` style template derived from the raw URL. */
export function defaultRoute(req: IncomingMessage): string {
  const path = (req.url ?? '/').split('?')[0] ?? '/';
  const template = path
    .split('/')
    .map((segment) =>
      /^(\d+|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|[0-9a-f]{16,})$/i.test(
        segment,
      )
        ? ':id'
        : segment,
    )
    .join('/');
  return `${req.method ?? 'GET'} ${template || '/'}`;
}

/**
 * Emits one `http.request` event per request with its latency, status,
 * route, cache outcome and (if resolved) client key. Mount it first so the
 * timer covers the whole stack.
 */
export function createTelemetryMiddleware(
  options: TelemetryMiddlewareOptions,
): Middleware {
  const telemetry = safeSink(options.telemetry);
  const resolveRoute = options.routeResolver ?? defaultRoute;

  return (req, res, next): void => {
    const started = performance.now();
    routes.set(req, resolveRoute(req));
    let reported = false;
    const report = (aborted: boolean): void => {
      if (reported) {
        return;
      }
      reported = true;
      res.off('finish', onFinish);
      res.off('close', onClose);
      telemetry.emit({
        type: 'http.request',
        at: Date.now(),
        method: req.method ?? 'GET',
        route: routes.get(req) ?? resolveRoute(req),
        status: res.statusCode,
        durationMs: performance.now() - started,
        cacheStatus: getCacheStatus(res),
        bytes: contentLength(res),
        aborted,
        clientKey: getClientIdentity(req)?.key,
      });
    };
    const onFinish = (): void => report(false);
    const onClose = (): void => report(!res.writableFinished);
    res.once('finish', onFinish);
    res.once('close', onClose);
    next();
  };
}

function contentLength(res: ServerResponse): number | undefined {
  const value = res.getHeader('content-length');
  const parsed = Number.parseInt(String(value ?? ''), 10);
  return Number.isFinite(parsed) ? parsed : undefined;
}
