import type { CacheManager } from '../cache/cache-manager';
import type { CacheMiddlewareOptions } from './cache-middleware';
import { createCacheMiddleware } from './cache-middleware';
import type { ClientIdentityOptions } from './client-identity';
import { ClientIdentityResolver } from './client-identity';
import type {
  RateLimitMiddleware,
  RateLimitMiddlewareOptions,
} from './rate-limit-middleware';
import { createRateLimitMiddleware } from './rate-limit-middleware';
import type { DiagnosticsMiddlewareOptions } from '../diagnostics/middleware';
import { createDiagnosticsMiddleware } from '../diagnostics/middleware';
import type { TelemetrySink } from '../telemetry/sink';
import type { TelemetryMiddlewareOptions } from './telemetry-middleware';
import { createTelemetryMiddleware } from './telemetry-middleware';
import type { Middleware } from './types';

export interface HttpMiddlewareOptions {
  /** Client identification policy (proxy trust, headers, fingerprints). */
  readonly identity?: ClientIdentityOptions | ClientIdentityResolver;
  /** Per-client throttling; omit to disable. */
  readonly rateLimit?: Omit<RateLimitMiddlewareOptions, 'identity'>;
  /** GET response caching; omit to disable. */
  readonly cache?:
    | CacheManager
    | (Omit<CacheMiddlewareOptions, 'cache'> & {
        readonly cache: CacheManager;
      });
  /**
   * Per-request latency/status telemetry (mounted first so it times the
   * whole stack) and rate-limit decision events. Pass `fileCache.telemetry`
   * to feed the built-in metrics.
   */
  readonly telemetry?:
    | TelemetrySink
    | (Omit<TelemetryMiddlewareOptions, 'telemetry'> & {
        readonly telemetry: TelemetrySink;
      });
  /** Liveness / readiness / diagnostics endpoints; omit to disable. */
  readonly diagnostics?: DiagnosticsMiddlewareOptions;
}

export interface HttpMiddleware {
  readonly identity: ClientIdentityResolver;
  readonly telemetry: Middleware | undefined;
  readonly diagnostics: Middleware | undefined;
  /** Resolves the client identity up front (optional but recommended). */
  readonly identify: Middleware;
  readonly rateLimit: RateLimitMiddleware | undefined;
  readonly cache: Middleware | undefined;
  /**
   * Ordered stack: telemetry → diagnostics → identify → rateLimit → cache.
   * Telemetry first so it times everything; diagnostics before throttling
   * so probes are never rate limited; throttling before the cache so
   * abusive clients cannot use cheap hits to hammer the server. Pass
   * straight to `app.use(...stack)`.
   */
  readonly stack: readonly Middleware[];
  /** Releases limiter state this factory owns. Idempotent. */
  close(): void;
}

/**
 * Factory that wires identity resolution, rate limiting and GET caching into
 * one consistent middleware stack sharing a single identity resolver.
 */
export function createHttpMiddleware(
  options: HttpMiddlewareOptions = {},
): HttpMiddleware {
  const identity =
    options.identity instanceof ClientIdentityResolver
      ? options.identity
      : new ClientIdentityResolver(options.identity);

  const sink =
    options.telemetry === undefined
      ? undefined
      : 'emit' in options.telemetry
        ? options.telemetry
        : options.telemetry.telemetry;
  const telemetry =
    options.telemetry === undefined
      ? undefined
      : createTelemetryMiddleware(
          'emit' in options.telemetry
            ? { telemetry: options.telemetry }
            : options.telemetry,
        );
  const diagnostics =
    options.diagnostics === undefined
      ? undefined
      : createDiagnosticsMiddleware(options.diagnostics);

  const rateLimit =
    options.rateLimit === undefined
      ? undefined
      : createRateLimitMiddleware({
          telemetry: sink,
          ...options.rateLimit,
          identity,
        });

  const cache =
    options.cache === undefined
      ? undefined
      : createCacheMiddleware(
          isCacheManager(options.cache)
            ? { cache: options.cache }
            : options.cache,
        );

  const identify = identity.middleware();
  const stack: Middleware[] = [];
  if (telemetry !== undefined) {
    stack.push(telemetry);
  }
  if (diagnostics !== undefined) {
    stack.push(diagnostics);
  }
  stack.push(identify);
  if (rateLimit !== undefined) {
    stack.push(rateLimit);
  }
  if (cache !== undefined) {
    stack.push(cache);
  }

  return {
    identity,
    telemetry,
    diagnostics,
    identify,
    rateLimit,
    cache,
    stack,
    close: (): void => rateLimit?.close(),
  };
}

function isCacheManager(
  value: NonNullable<HttpMiddlewareOptions['cache']>,
): value is CacheManager {
  return !('cache' in value);
}
