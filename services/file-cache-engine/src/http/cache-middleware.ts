import type { CacheHit, CacheManager } from '../cache/cache-manager';
import type { EntryAttributes } from '../cache/types';
import type { Clock } from '../shared/clock';
import { systemClock } from '../shared/clock';
import type { StoredHeaders } from './headers';
import {
  directiveMs,
  headerValue,
  isNotModified,
  notModifiedHeaders,
  parseCacheControl,
  storableHeaders,
} from './headers';
import type { CapturedResponse } from './response-capture';
import { captureResponse } from './response-capture';
import type {
  IncomingMessage,
  Middleware,
  NextFunction,
  ServerResponse,
} from './types';

/** Value of the diagnostic header (`X-Cache` by default). */
export type CacheStatus = 'HIT' | 'STALE' | 'MISS' | 'BYPASS';

const statuses = new WeakMap<ServerResponse, CacheStatus>();

/** The cache outcome recorded for `res`, independent of the header. */
export function getCacheStatus(res: ServerResponse): CacheStatus | undefined {
  return statuses.get(res);
}

export interface CacheMiddlewareOptions {
  readonly cache: CacheManager;
  /**
   * Builds the cache key. Return `undefined` to bypass the cache.
   * Default: `GET <host><url>` plus the `vary` request headers.
   */
  readonly keyResolver?: (req: IncomingMessage) => string | undefined;
  /**
   * Request headers that select a distinct variant. Default:
   * `['accept-encoding']` so compressed and identity bodies never mix.
   */
  readonly vary?: readonly string[];
  /**
   * Requests whose responses are private and must never be cached.
   * Default: anything carrying `Authorization` or `Cookie`.
   */
  readonly isPrivate?: (req: IncomingMessage) => boolean;
  /**
   * Response statuses eligible for caching. Default: the heuristically
   * cacheable set from RFC 9110 §15.1 — 200, 203, 204, 301, 404, 410.
   */
  readonly cacheableStatuses?: readonly number[];
  /**
   * Honour the response's `Cache-Control` (`no-store`, `private`,
   * `s-maxage`/`max-age`, `stale-while-revalidate`). Default: `true`.
   */
  readonly respectCacheControl?: boolean;
  /** Fallback freshness when the response does not specify one. */
  readonly ttlMs?: number;
  readonly staleWhileRevalidateMs?: number;
  /** Responses larger than this are streamed through but never stored. */
  readonly maxEntryBytes?: number;
  /** Diagnostic header name, or `false` to omit it. Default: `X-Cache`. */
  readonly statusHeader?: string | false;
  /**
   * Cache failures (disk errors, ...) are reported here and the request is
   * served from origin — the cache must never turn into an outage.
   * Default: `console.error`.
   */
  readonly onError?: (error: unknown, req: IncomingMessage) => void;
  readonly clock?: Clock;
}

const DEFAULT_VARY: readonly string[] = ['accept-encoding'];
const DEFAULT_CACHEABLE_STATUSES: readonly number[] = [
  200, 203, 204, 301, 404, 410,
];
const DEFAULT_MAX_ENTRY_BYTES = 5 * 1024 * 1024;

/**
 * Caches `GET` responses (and answers `HEAD` from them).
 *
 * Stampede handling for a miss on a hot key:
 * - the first request ("leader") runs the downstream handler; its response
 *   streams to the client and is captured on the way out;
 * - concurrent requests for the same key join that in-flight load and are
 *   answered with the captured response once it completes;
 * - if the leader's response turns out to be uncacheable (private, error,
 *   `no-store`, ...), followers run the handler themselves — a response is
 *   never replayed to anyone it was not produced for;
 * - when an entry is stale and a revalidation is already in flight, the
 *   stale copy is served immediately; otherwise the request revalidates.
 */
export function createCacheMiddleware(
  options: CacheMiddlewareOptions,
): Middleware {
  const { cache } = options;
  const vary = options.vary ?? DEFAULT_VARY;
  const keyResolver =
    options.keyResolver ??
    ((req: IncomingMessage): string => defaultKey(req, vary));
  const isPrivate = options.isPrivate ?? defaultIsPrivate;
  const cacheableStatuses = new Set(
    options.cacheableStatuses ?? DEFAULT_CACHEABLE_STATUSES,
  );
  const respectCacheControl = options.respectCacheControl ?? true;
  const maxEntryBytes = options.maxEntryBytes ?? DEFAULT_MAX_ENTRY_BYTES;
  const statusHeader = options.statusHeader ?? 'X-Cache';
  // Never persist our own diagnostic header; replays set their own.
  const excludedHeaders = new Set(
    statusHeader === false ? [] : [statusHeader.toLowerCase()],
  );
  const clock = options.clock ?? systemClock;
  const onError =
    options.onError ??
    ((error: unknown, req: IncomingMessage): void => {
      console.error(
        `[cache] ${req.method ?? ''} ${req.url ?? ''} failed:`,
        error,
      );
    });

  const markStatus = (res: ServerResponse, status: CacheStatus): void => {
    statuses.set(res, status);
    if (statusHeader !== false && !res.headersSent) {
      res.setHeader(statusHeader, status);
    }
  };

  return (req, res, next): void => {
    handle(req, res, next).catch((error: unknown) => next(error));
  };

  async function handle(
    req: IncomingMessage,
    res: ServerResponse,
    next: NextFunction,
  ): Promise<void> {
    const method = req.method ?? 'GET';
    if (method !== 'GET' && method !== 'HEAD') {
      next();
      return;
    }
    const key = keyResolver(req);
    const requestControl = parseCacheControl(
      headerValue(req.headers, 'cache-control'),
    );
    if (key === undefined || isPrivate(req) || requestControl.has('no-store')) {
      markStatus(res, 'BYPASS');
      next();
      return;
    }

    // `no-cache` on the request forces revalidation but still allows storing.
    const forceRevalidate =
      requestControl.has('no-cache') ||
      headerValue(req.headers, 'pragma') === 'no-cache';

    if (forceRevalidate) {
      if (method === 'HEAD') {
        markStatus(res, 'MISS');
        next();
        return;
      }
      await revalidateFromOrigin(key, req, res, next);
      return;
    }

    let hit: CacheHit | undefined;
    try {
      hit = await cache.get(key, { allowStale: true });
    } catch (error) {
      onError(error, req);
      markStatus(res, 'BYPASS');
      next();
      return;
    }
    if (hit?.freshness === 'fresh') {
      replay(req, res, hit, 'HIT');
      return;
    }
    if (hit?.freshness === 'stale' && cache.isLoading(key)) {
      replay(req, res, hit, 'STALE');
      return;
    }

    if (method === 'HEAD') {
      // A HEAD miss has no body to store; let origin answer it.
      markStatus(res, 'MISS');
      next();
      return;
    }

    // Miss (or stale without an in-flight refresh): load through the cache.
    // A follower whose leader vanished (client disconnect, loader timeout)
    // re-joins once so the group elects a new leader instead of stampeding.
    for (let election = 0; ; election += 1) {
      let ranDownstream = false;
      try {
        const result = await cache.getOrLoad(
          key,
          async ({ signal }) => {
            ranDownstream = true;
            markStatus(res, 'MISS');
            const outcome = await runDownstream(res, next, signal);
            if (outcome.kind !== 'cacheable') {
              throw new Uncacheable(outcome.kind);
            }
            return outcome.output;
          },
          { staleWhileRevalidate: false },
        );
        if (!ranDownstream) {
          replay(req, res, result, 'HIT');
        }
        return;
      } catch (error) {
        if (ranDownstream) {
          // The client has already been answered by the downstream handler;
          // only unexpected failures are worth reporting.
          if (!(error instanceof Uncacheable)) {
            onError(error, req);
          }
          return;
        }
        const retriable =
          error instanceof Uncacheable &&
          (error.reason === 'aborted' || error.reason === 'timeout');
        if (retriable && election < MAX_ELECTIONS && !res.destroyed) {
          continue;
        }
        if (!(error instanceof Uncacheable)) {
          onError(error, req);
        }
        markStatus(res, 'BYPASS');
        next();
        return;
      }
    }
  }

  /**
   * `Cache-Control: no-cache` on the request: the client insists on an
   * origin response, so skip the lookup but still refresh the entry.
   */
  async function revalidateFromOrigin(
    key: string,
    req: IncomingMessage,
    res: ServerResponse,
    next: NextFunction,
  ): Promise<void> {
    markStatus(res, 'MISS');
    const outcome = await runDownstream(res, next, undefined);
    if (outcome.kind !== 'cacheable') {
      return;
    }
    try {
      await cache.put(key, outcome.output.value, {
        ttlMs: outcome.output.ttlMs,
        staleWhileRevalidateMs: outcome.output.staleWhileRevalidateMs,
        attributes: outcome.output.attributes,
      });
    } catch (error) {
      onError(error, req);
    }
  }

  /**
   * Runs the rest of the chain with the response captured, under the
   * loader's abort signal (the manager's `loaderTimeoutMs` or a caller
   * abort). Never throws; the outcome says whether and why it is unusable.
   */
  async function runDownstream(
    res: ServerResponse,
    next: NextFunction,
    signal: AbortSignal | undefined,
  ): Promise<DownstreamOutcome> {
    // Headers already present belong to *this* request (rate-limit quota,
    // request id, ...) — set by upstream middleware, not by the origin — and
    // must not be stored and replayed to other clients.
    const preset = { ...res.getHeaders() };

    // Arm the capture before handing over; whatever the chain writes from
    // here on (including framework error handlers) is observed. Error paths
    // surface as non-cacheable statuses and are filtered below.
    const captured = captureResponse(res, { maxBytes: maxEntryBytes, signal });
    next();
    const outcome = await captured;
    if (outcome.kind === 'aborted' && signal?.aborted === true) {
      return { kind: 'timeout' }; // detached by the loader budget, not the client
    }
    if (outcome.kind !== 'complete') {
      return { kind: outcome.kind };
    }
    const policy = cacheability(outcome.response);
    if (policy === undefined) {
      return { kind: 'policy' };
    }
    const exclude = new Set(excludedHeaders);
    for (const [name, value] of Object.entries(preset)) {
      if (String(outcome.response.headers[name]) === String(value)) {
        exclude.add(name.toLowerCase());
      }
    }
    return {
      kind: 'cacheable',
      output: {
        value: outcome.response.body,
        attributes: encodeAttributes(outcome.response, exclude),
        ttlMs: policy.ttlMs,
        staleWhileRevalidateMs: policy.staleWhileRevalidateMs,
      },
    };
  }

  function cacheability(
    response: CapturedResponse,
  ):
    | { ttlMs: number | undefined; staleWhileRevalidateMs: number | undefined }
    | undefined {
    if (!cacheableStatuses.has(response.status)) {
      return undefined;
    }
    const headers = response.headers;
    if (headers['set-cookie'] !== undefined) {
      return undefined;
    }
    if (String(headers['vary'] ?? '').trim() === '*') {
      return undefined;
    }
    if (!respectCacheControl) {
      return {
        ttlMs: options.ttlMs,
        staleWhileRevalidateMs: options.staleWhileRevalidateMs,
      };
    }
    const control = parseCacheControl(
      headers['cache-control'] === undefined
        ? undefined
        : String(headers['cache-control']),
    );
    if (
      control.has('no-store') ||
      control.has('private') ||
      control.has('no-cache')
    ) {
      return undefined;
    }
    const ttlMs =
      directiveMs(control, 's-maxage') ??
      directiveMs(control, 'max-age') ??
      options.ttlMs;
    if (ttlMs !== undefined && ttlMs <= 0) {
      return undefined;
    }
    return {
      ttlMs,
      staleWhileRevalidateMs:
        directiveMs(control, 'stale-while-revalidate') ??
        options.staleWhileRevalidateMs,
    };
  }

  function replay(
    req: IncomingMessage,
    res: ServerResponse,
    hit: CacheHit,
    status: CacheStatus,
  ): void {
    if (res.headersSent || res.destroyed) {
      return;
    }
    const stored = decodeAttributes(hit.metadata.attributes);
    const age = Math.max(
      0,
      Math.floor((clock.now() - hit.metadata.createdAt) / 1000),
    );

    if (isNotModified(req.headers, stored.headers)) {
      res.statusCode = 304;
      setHeaders(res, notModifiedHeaders(stored.headers));
      res.setHeader('Age', String(age));
      markStatus(res, status);
      res.end();
      return;
    }

    res.statusCode = stored.status;
    setHeaders(res, stored.headers);
    res.setHeader('Age', String(age));
    res.setHeader('Content-Length', String(hit.metadata.size));
    markStatus(res, status);
    if (req.method === 'HEAD') {
      res.end();
    } else {
      res.end(hit.value);
    }
  }
}

/** How many times a follower re-joins after its leader vanished. */
const MAX_ELECTIONS = 2;

type UncacheableReason = 'policy' | 'oversized' | 'aborted' | 'timeout';

interface CacheableOutput {
  readonly value: Buffer;
  readonly attributes: EntryAttributes;
  readonly ttlMs: number | undefined;
  readonly staleWhileRevalidateMs: number | undefined;
}

type DownstreamOutcome =
  | { readonly kind: 'cacheable'; readonly output: CacheableOutput }
  | { readonly kind: UncacheableReason };

/** Thrown inside the loader to signal "serve from origin, do not store". */
class Uncacheable extends Error {
  constructor(readonly reason: UncacheableReason) {
    super(`Response is not cacheable (${reason})`);
    this.name = 'Uncacheable';
  }
}

function defaultKey(req: IncomingMessage, vary: readonly string[]): string {
  const host = headerValue(req.headers, 'host') ?? '';
  const variants = vary
    .map((name) => {
      const value = headerValue(req.headers, name);
      return value === undefined
        ? ''
        : `${name.toLowerCase()}=${value.toLowerCase().replace(/\s+/g, '')}`;
    })
    .filter((part) => part.length > 0);
  return `GET ${host}${req.url ?? '/'}${variants.length > 0 ? ` | ${variants.join('; ')}` : ''}`;
}

function defaultIsPrivate(req: IncomingMessage): boolean {
  return (
    req.headers['authorization'] !== undefined ||
    req.headers['cookie'] !== undefined
  );
}

function encodeAttributes(
  response: CapturedResponse,
  exclude: ReadonlySet<string>,
): EntryAttributes {
  return {
    status: String(response.status),
    headers: JSON.stringify(storableHeaders(response.headers, exclude)),
  };
}

function decodeAttributes(attributes: EntryAttributes): {
  status: number;
  headers: StoredHeaders;
} {
  const status = Number.parseInt(attributes['status'] ?? '', 10);
  const headers: Record<string, string | string[]> = {};
  try {
    const parsed: unknown = JSON.parse(attributes['headers'] ?? '{}');
    if (typeof parsed === 'object' && parsed !== null) {
      for (const [name, value] of Object.entries(parsed)) {
        if (typeof value === 'string' || isStringArray(value)) {
          headers[name] = value;
        }
      }
    }
  } catch {
    // A malformed header blob degrades to "no headers"; the body is intact.
  }
  return { status: Number.isInteger(status) ? status : 200, headers };
}

function isStringArray(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((v) => typeof v === 'string');
}

function setHeaders(res: ServerResponse, headers: StoredHeaders): void {
  for (const [name, value] of Object.entries(headers)) {
    res.setHeader(name, value);
  }
}
