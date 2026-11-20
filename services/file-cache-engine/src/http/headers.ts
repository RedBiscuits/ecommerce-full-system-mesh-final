import type { IncomingHttpHeaders, OutgoingHttpHeaders } from 'node:http';

/** Returns the first value of a request header, trimmed, or `undefined`. */
export function headerValue(
  headers: IncomingHttpHeaders,
  name: string,
): string | undefined {
  const raw = headers[name.toLowerCase()];
  const value = Array.isArray(raw) ? raw[0] : raw;
  const trimmed = value?.trim();
  return trimmed === undefined || trimmed.length === 0 ? undefined : trimmed;
}

/** Parsed `Cache-Control` directives (names lower-cased, values unquoted). */
export type CacheControl = ReadonlyMap<string, string | undefined>;

export function parseCacheControl(value: string | undefined): CacheControl {
  const directives = new Map<string, string | undefined>();
  if (value === undefined) {
    return directives;
  }
  for (const part of value.split(',')) {
    const [rawName, ...rest] = part.split('=');
    const name = rawName?.trim().toLowerCase();
    if (name === undefined || name.length === 0) {
      continue;
    }
    const rawValue = rest.join('=').trim();
    directives.set(
      name,
      rawValue.length === 0 ? undefined : rawValue.replace(/^"(.*)"$/, '$1'),
    );
  }
  return directives;
}

/** Reads a non-negative integer directive (`max-age=60`) as milliseconds. */
export function directiveMs(
  control: CacheControl,
  name: string,
): number | undefined {
  const raw = control.get(name);
  if (raw === undefined) {
    return undefined;
  }
  const seconds = Number.parseInt(raw, 10);
  return Number.isSafeInteger(seconds) && seconds >= 0
    ? seconds * 1000
    : undefined;
}

/**
 * Headers that describe the connection rather than the representation and
 * must never be stored or replayed (RFC 9110 §7.6.1), plus headers that are
 * recomputed on replay.
 */
const NON_STORABLE_HEADERS: ReadonlySet<string> = new Set([
  'connection',
  'keep-alive',
  'proxy-authenticate',
  'proxy-authorization',
  'te',
  'trailer',
  'transfer-encoding',
  'upgrade',
  'content-length',
  'set-cookie',
  'set-cookie2',
]);

export type StoredHeaders = Readonly<Record<string, string | string[]>>;

/**
 * Normalises outgoing headers into a storable, JSON-safe map, dropping
 * connection-level headers and any names in `exclude`.
 */
export function storableHeaders(
  headers: OutgoingHttpHeaders,
  exclude: ReadonlySet<string> = new Set(),
): StoredHeaders {
  const result: Record<string, string | string[]> = {};
  for (const [name, value] of Object.entries(headers)) {
    const key = name.toLowerCase();
    if (
      value === undefined ||
      NON_STORABLE_HEADERS.has(key) ||
      exclude.has(key)
    ) {
      continue;
    }
    result[key] = Array.isArray(value) ? value.map(String) : String(value);
  }
  return result;
}

/** Headers that may be sent with a `304 Not Modified` (RFC 9110 §15.4.5). */
const NOT_MODIFIED_HEADERS: readonly string[] = [
  'cache-control',
  'content-location',
  'date',
  'etag',
  'expires',
  'last-modified',
  'vary',
];

export function notModifiedHeaders(stored: StoredHeaders): StoredHeaders {
  const result: Record<string, string | string[]> = {};
  for (const name of NOT_MODIFIED_HEADERS) {
    const value = stored[name];
    if (value !== undefined) {
      result[name] = value;
    }
  }
  return result;
}

/**
 * Evaluates `If-None-Match` / `If-Modified-Since` against a stored response
 * (RFC 9110 §13.1). `If-None-Match` takes precedence when present.
 */
export function isNotModified(
  request: IncomingHttpHeaders,
  stored: StoredHeaders,
): boolean {
  const ifNoneMatch = headerValue(request, 'if-none-match');
  if (ifNoneMatch !== undefined) {
    const etag = single(stored['etag']);
    if (etag === undefined) {
      return false;
    }
    if (ifNoneMatch === '*') {
      return true;
    }
    const target = weakEtag(etag);
    return ifNoneMatch
      .split(',')
      .map((tag) => weakEtag(tag.trim()))
      .includes(target);
  }

  const ifModifiedSince = headerValue(request, 'if-modified-since');
  const lastModified = single(stored['last-modified']);
  if (ifModifiedSince === undefined || lastModified === undefined) {
    return false;
  }
  const since = Date.parse(ifModifiedSince);
  const modified = Date.parse(lastModified);
  return (
    Number.isFinite(since) && Number.isFinite(modified) && modified <= since
  );
}

function weakEtag(tag: string): string {
  return tag.startsWith('W/') ? tag.slice(2) : tag;
}

function single(value: string | string[] | undefined): string | undefined {
  return Array.isArray(value) ? value[0] : value;
}
