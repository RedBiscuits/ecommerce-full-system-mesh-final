import { RateLimitExceededError } from '../rate-limit/errors';
import type { KeyedRateLimiterOptions } from '../rate-limit/keyed-rate-limiter';
import { KeyedRateLimiter } from '../rate-limit/keyed-rate-limiter';
import type { TokenBucketRateLimiter } from '../rate-limit/token-bucket';
import type { TelemetrySink } from '../telemetry/sink';
import { safeSink } from '../telemetry/sink';
import type { ClientIdentity } from './client-identity';
import { ClientIdentityResolver } from './client-identity';
import { routeOf } from './telemetry-middleware';
import type { IncomingMessage, Middleware, ServerResponse } from './types';

/** Snapshot of a client's quota, used for headers and the 429 hook. */
export interface RateLimitInfo {
  readonly key: string;
  readonly limit: number;
  readonly remaining: number;
  /** Milliseconds until the request could be admitted. */
  readonly retryAfterMs: number;
}

export type RateLimitHeaderStyle = 'standard' | 'legacy' | 'both' | false;

export interface RateLimitMiddlewareOptions {
  /** Shared limiter instance, or options to create a dedicated one. */
  readonly limiter: KeyedRateLimiter | KeyedRateLimiterOptions;
  /** Identity resolver; share one instance across middlewares. */
  readonly identity?: ClientIdentityResolver;
  /**
   * Derives the bucket key. Return `undefined` to exempt the request.
   * Default: `identity.key` (API key → credentials → session → network).
   */
  readonly keyResolver?: (
    identity: ClientIdentity,
    req: IncomingMessage,
  ) => string | undefined;
  /** Tokens consumed per request. Default: `1`. */
  readonly cost?: number | ((req: IncomingMessage) => number);
  /** Requests for which limiting is skipped entirely (health checks, ...). */
  readonly skip?: (req: IncomingMessage) => boolean;
  /**
   * Instead of rejecting immediately, wait up to this long for capacity
   * (smooths short bursts at the cost of latency). Default: `0` (fail fast).
   */
  readonly waitTimeoutMs?: number;
  /**
   * `standard` → IETF `RateLimit-*` fields; `legacy` → `X-RateLimit-*`;
   * `both`; or `false` to emit none. `Retry-After` is always set on 429.
   * Default: `standard`.
   */
  readonly headers?: RateLimitHeaderStyle;
  /** Receives a `rate-limit` event per decision. */
  readonly telemetry?: TelemetrySink;
  /** Custom 429 response. Default: JSON body `{ error, retryAfterMs }`. */
  readonly onLimited?: (
    req: IncomingMessage,
    res: ServerResponse,
    info: RateLimitInfo,
  ) => void | Promise<void>;
}

/** The middleware function plus its lifecycle surface. */
export interface RateLimitMiddleware extends Middleware {
  readonly limiter: KeyedRateLimiter;
  /**
   * Rejects waiting requests and drops bucket state. Only affects a limiter
   * this factory created; a caller-supplied limiter is left untouched.
   */
  close(): void;
}

/**
 * Throttles requests per client with a 429 + `Retry-After` on overflow.
 *
 * HTTP requests fail fast by default: a queued request still occupies a
 * socket and a worker, so making clients back off is cheaper than making
 * them wait. `waitTimeoutMs` opts into bounded waiting.
 */
export function createRateLimitMiddleware(
  options: RateLimitMiddlewareOptions,
): RateLimitMiddleware {
  const ownsLimiter = !(options.limiter instanceof KeyedRateLimiter);
  const limiter =
    options.limiter instanceof KeyedRateLimiter
      ? options.limiter
      : new KeyedRateLimiter(options.limiter);
  const identity = options.identity ?? new ClientIdentityResolver();
  const keyResolver =
    options.keyResolver ?? ((client: ClientIdentity): string => client.key);
  const costOf =
    typeof options.cost === 'function'
      ? options.cost
      : (): number => (typeof options.cost === 'number' ? options.cost : 1);
  const waitTimeoutMs = options.waitTimeoutMs ?? 0;
  const headerStyle = options.headers ?? 'standard';
  const onLimited = options.onLimited ?? defaultOnLimited;
  const telemetry = safeSink(options.telemetry);

  const middleware: Middleware = (req, res, next): void => {
    handle(req, res).then(
      (admitted) => {
        if (admitted) {
          next();
        }
      },
      (error: unknown) => next(error),
    );
  };

  return Object.assign(middleware, {
    limiter,
    close: (): void => {
      if (ownsLimiter) {
        limiter.close();
      }
    },
  });

  async function handle(
    req: IncomingMessage,
    res: ServerResponse,
  ): Promise<boolean> {
    if (options.skip?.(req) === true) {
      return true;
    }
    const client = await identity.resolve(req);
    const key = keyResolver(client, req);
    if (key === undefined) {
      return true;
    }
    const bucket = limiter.limiter(key);
    const cost = costOf(req);

    let admitted = bucket.tryAcquire(cost);
    if (!admitted && waitTimeoutMs > 0) {
      admitted = await bucket
        .acquire(cost, { timeoutMs: waitTimeoutMs })
        .then(() => true)
        .catch((error: unknown) => {
          if (error instanceof RateLimitExceededError) {
            return false;
          }
          throw error;
        });
    }

    const info = snapshot(key, bucket, cost);
    applyHeaders(res, info, headerStyle);
    telemetry.emit({
      type: 'rate-limit',
      at: Date.now(),
      key,
      admitted,
      remaining: info.remaining,
      retryAfterMs: info.retryAfterMs,
      route: routeOf(req),
    });
    if (admitted) {
      return true;
    }

    const retryAfterSeconds = Math.max(1, Math.ceil(info.retryAfterMs / 1000));
    res.statusCode = 429;
    res.setHeader('Retry-After', String(retryAfterSeconds));
    res.setHeader('Cache-Control', 'no-store');
    await onLimited(req, res, info);
    if (!res.writableEnded) {
      res.end();
    }
    return false;
  }
}

function snapshot(
  key: string,
  bucket: TokenBucketRateLimiter,
  cost: number,
): RateLimitInfo {
  return {
    key,
    limit: bucket.capacity,
    remaining: Math.floor(bucket.availableTokens),
    retryAfterMs: bucket.estimateWaitMs(cost),
  };
}

function applyHeaders(
  res: ServerResponse,
  info: RateLimitInfo,
  style: RateLimitHeaderStyle,
): void {
  if (style === false) {
    return;
  }
  const resetSeconds = Math.ceil(info.retryAfterMs / 1000);
  if (style === 'standard' || style === 'both') {
    res.setHeader('RateLimit-Limit', String(info.limit));
    res.setHeader('RateLimit-Remaining', String(info.remaining));
    res.setHeader('RateLimit-Reset', String(resetSeconds));
  }
  if (style === 'legacy' || style === 'both') {
    res.setHeader('X-RateLimit-Limit', String(info.limit));
    res.setHeader('X-RateLimit-Remaining', String(info.remaining));
    res.setHeader('X-RateLimit-Reset', String(resetSeconds));
  }
}

function defaultOnLimited(
  _req: IncomingMessage,
  res: ServerResponse,
  info: RateLimitInfo,
): void {
  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  res.end(
    JSON.stringify({
      error: 'Too Many Requests',
      retryAfterMs: Math.ceil(info.retryAfterMs),
    }),
  );
}
