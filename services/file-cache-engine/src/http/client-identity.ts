import { createHash, randomUUID } from 'node:crypto';
import { isIP } from 'node:net';

import { headerValue } from './headers';
import type { IncomingMessage, Middleware } from './types';

/** Where the resolved client IP came from. */
export type IpSource = 'socket' | 'header' | 'override';

/**
 * Everything we can (safely) learn about the caller of a request.
 *
 * Secrets are never stored verbatim: API keys, bearer tokens and session
 * cookies are reduced to a SHA-256 fingerprint that is stable per caller but
 * cannot be used to impersonate them.
 */
export interface ClientIdentity {
  /** Best-guess client IP after honouring the proxy trust policy. */
  readonly ip: string | undefined;
  readonly ipSource: IpSource;
  /** IP of the direct peer (the last proxy, or the client itself). */
  readonly remoteAddress: string | undefined;
  /**
   * Rate-limit friendly network key: the IP itself for IPv4, the /64 (by
   * default) prefix for IPv6 — individual IPv6 addresses are effectively
   * unlimited per host, so limiting per address is trivially bypassed.
   */
  readonly network: string | undefined;
  /** ISO 3166-1 alpha-2 country code, if an edge/CDN provided one. */
  readonly country: string | undefined;
  readonly userAgent: string | undefined;
  /** Primary `Accept-Language` tag (e.g. `en-GB`). */
  readonly language: string | undefined;
  /** Client hint `Sec-CH-UA-Platform`, unquoted. */
  readonly platform: string | undefined;
  /** Client hint `Sec-CH-UA-Mobile`. */
  readonly mobile: boolean | undefined;
  /** `x-request-id` / `x-correlation-id`, or a generated UUID. */
  readonly requestId: string;
  /** Fingerprint of the API key header, if present. */
  readonly apiKeyFingerprint: string | undefined;
  /** Fingerprint of the `Authorization` credentials, if present. */
  readonly credentialFingerprint: string | undefined;
  /** Fingerprint of the configured session cookie, if present. */
  readonly sessionFingerprint: string | undefined;
  /** Results of custom extractors, keyed by extractor name. */
  readonly extra: Readonly<Record<string, string>>;
  /**
   * Stable key for per-client throttling, in order of preference:
   * API key → credentials → session → network → `"anonymous"`.
   */
  readonly key: string;
}

/**
 * Proxy trust policy, mirroring Express' `trust proxy` semantics:
 * - `false`  – never trust forwarding headers; use the socket address.
 * - `true`   – trust all hops; the left-most `X-Forwarded-For` entry wins.
 * - `n`      – exactly `n` trusted proxies in front of us; take the address
 *              `n` hops from the right.
 */
export type TrustProxy = boolean | number;

export interface ClientIdentityOptions {
  /** Default: `false`. Do not enable unless a proxy you control sets the headers. */
  readonly trustProxy?: TrustProxy;
  /**
   * Single-value IP headers consulted (in order) before `X-Forwarded-For`
   * when `trustProxy` is enabled. Default: CDN-specific headers.
   */
  readonly ipHeaders?: readonly string[];
  /** Headers consulted (in order) for the country code. */
  readonly countryHeaders?: readonly string[];
  /** Header carrying an API key. Default: `x-api-key`. */
  readonly apiKeyHeader?: string;
  /** Name of the session cookie to fingerprint. Default: none. */
  readonly sessionCookie?: string;
  /** Prefix length for IPv6 `network` keys. Default: `64`. */
  readonly ipv6PrefixLength?: number;
  /** Escape hatch, e.g. `(req) => req.ip` when Express already resolved it. */
  readonly ipResolver?: (req: IncomingMessage) => string | undefined;
  /**
   * Optional country lookup (e.g. a GeoIP database) used when no header
   * supplies one.
   */
  readonly countryResolver?: (
    ip: string,
  ) => string | undefined | Promise<string | undefined>;
  /** Additional identifiers, keyed by name, surfaced under `extra`. */
  readonly extractors?: Readonly<
    Record<string, (req: IncomingMessage) => string | undefined>
  >;
}

const DEFAULT_IP_HEADERS: readonly string[] = [
  'cf-connecting-ip',
  'true-client-ip',
  'x-real-ip',
];

const DEFAULT_COUNTRY_HEADERS: readonly string[] = [
  'cf-ipcountry',
  'cloudfront-viewer-country',
  'x-vercel-ip-country',
  'fastly-country-code',
  'x-appengine-country',
  'x-country-code',
];

const identities = new WeakMap<IncomingMessage, ClientIdentity>();

/**
 * Returns the identity attached to `req` by {@link ClientIdentityResolver},
 * or `undefined` if it has not been resolved yet.
 */
export function getClientIdentity(
  req: IncomingMessage,
): ClientIdentity | undefined {
  return identities.get(req);
}

/**
 * Resolves and memoises a {@link ClientIdentity} per request. State lives in
 * a `WeakMap` keyed by the request object rather than on the request itself,
 * so no framework types are extended or monkey-patched.
 */
export class ClientIdentityResolver {
  readonly #trustProxy: TrustProxy;
  readonly #ipHeaders: readonly string[];
  readonly #countryHeaders: readonly string[];
  readonly #apiKeyHeader: string;
  readonly #sessionCookie: string | undefined;
  readonly #ipv6PrefixLength: number;
  readonly #ipResolver: ClientIdentityOptions['ipResolver'];
  readonly #countryResolver: ClientIdentityOptions['countryResolver'];
  readonly #extractors: NonNullable<ClientIdentityOptions['extractors']>;

  constructor(options: ClientIdentityOptions = {}) {
    this.#trustProxy = options.trustProxy ?? false;
    this.#ipHeaders = options.ipHeaders ?? DEFAULT_IP_HEADERS;
    this.#countryHeaders = options.countryHeaders ?? DEFAULT_COUNTRY_HEADERS;
    this.#apiKeyHeader = options.apiKeyHeader ?? 'x-api-key';
    this.#sessionCookie = options.sessionCookie;
    this.#ipv6PrefixLength = options.ipv6PrefixLength ?? 64;
    this.#ipResolver = options.ipResolver;
    this.#countryResolver = options.countryResolver;
    this.#extractors = options.extractors ?? {};
  }

  /** Resolves (once) and attaches the identity for `req`. */
  async resolve(req: IncomingMessage): Promise<ClientIdentity> {
    const existing = identities.get(req);
    if (existing !== undefined) {
      return existing;
    }
    const identity = await this.#build(req);
    identities.set(req, identity);
    return identity;
  }

  /** Middleware form: resolves the identity up front for later handlers. */
  middleware(): Middleware {
    return (req, _res, next): void => {
      this.resolve(req).then(
        () => next(),
        (error: unknown) => next(error),
      );
    };
  }

  // --- internals ----------------------------------------------------------

  async #build(req: IncomingMessage): Promise<ClientIdentity> {
    const { headers } = req;
    const remoteAddress = normaliseIp(req.socket?.remoteAddress);
    const { ip, ipSource } = this.#resolveIp(req, remoteAddress);
    const network =
      ip === undefined ? undefined : networkOf(ip, this.#ipv6PrefixLength);

    const apiKeyFingerprint = fingerprint(
      headerValue(headers, this.#apiKeyHeader),
    );
    const credentialFingerprint = fingerprint(
      headerValue(headers, 'authorization'),
    );
    const sessionFingerprint =
      this.#sessionCookie === undefined
        ? undefined
        : fingerprint(
            readCookie(headerValue(headers, 'cookie'), this.#sessionCookie),
          );

    const extra: Record<string, string> = {};
    for (const [name, extract] of Object.entries(this.#extractors)) {
      const value = extract(req);
      if (value !== undefined) {
        extra[name] = value;
      }
    }

    return {
      ip,
      ipSource,
      remoteAddress,
      network,
      country: await this.#resolveCountry(req, ip),
      userAgent: headerValue(headers, 'user-agent'),
      language: primaryLanguage(headerValue(headers, 'accept-language')),
      platform: unquote(headerValue(headers, 'sec-ch-ua-platform')),
      mobile: parseMobileHint(headerValue(headers, 'sec-ch-ua-mobile')),
      requestId:
        headerValue(headers, 'x-request-id') ??
        headerValue(headers, 'x-correlation-id') ??
        randomUUID(),
      apiKeyFingerprint,
      credentialFingerprint,
      sessionFingerprint,
      extra: Object.freeze(extra),
      key: throttleKey({
        apiKeyFingerprint,
        credentialFingerprint,
        sessionFingerprint,
        network,
      }),
    };
  }

  #resolveIp(
    req: IncomingMessage,
    remoteAddress: string | undefined,
  ): { ip: string | undefined; ipSource: IpSource } {
    const override = normaliseIp(this.#ipResolver?.(req));
    if (override !== undefined) {
      return { ip: override, ipSource: 'override' };
    }
    if (this.#trustProxy === false || this.#trustProxy === 0) {
      return { ip: remoteAddress, ipSource: 'socket' };
    }

    for (const name of this.#ipHeaders) {
      const candidate = normaliseIp(headerValue(req.headers, name));
      if (candidate !== undefined) {
        return { ip: candidate, ipSource: 'header' };
      }
    }

    const chain = forwardedChain(req.headers, remoteAddress);
    const picked =
      this.#trustProxy === true
        ? chain[0]
        : chain[Math.max(0, chain.length - 1 - this.#trustProxy)];
    return picked === undefined || picked === remoteAddress
      ? { ip: remoteAddress, ipSource: 'socket' }
      : { ip: picked, ipSource: 'header' };
  }

  async #resolveCountry(
    req: IncomingMessage,
    ip: string | undefined,
  ): Promise<string | undefined> {
    for (const name of this.#countryHeaders) {
      const code = headerValue(req.headers, name)?.toUpperCase();
      if (code !== undefined && /^[A-Z]{2}$/.test(code)) {
        return code;
      }
    }
    if (ip !== undefined && this.#countryResolver !== undefined) {
      const code = (await this.#countryResolver(ip))?.toUpperCase();
      if (code !== undefined && /^[A-Z]{2}$/.test(code)) {
        return code;
      }
    }
    return undefined;
  }
}

// --- helpers ------------------------------------------------------------------

/**
 * Builds the hop chain `[client, proxy1, ..., directPeer]` from
 * `Forwarded` (RFC 7239) or `X-Forwarded-For`, ending with the socket peer.
 */
function forwardedChain(
  headers: IncomingMessage['headers'],
  remoteAddress: string | undefined,
): string[] {
  const hops: string[] = [];
  const forwarded = headers['forwarded'];
  if (forwarded !== undefined) {
    for (const element of forwarded.split(',')) {
      for (const pair of element.split(';')) {
        const [name, value] = pair.split('=');
        if (name?.trim().toLowerCase() === 'for' && value !== undefined) {
          const ip = normaliseIp(unquote(value.trim()));
          if (ip !== undefined) {
            hops.push(ip);
          }
        }
      }
    }
  } else {
    const xff = headers['x-forwarded-for'];
    const joined = Array.isArray(xff) ? xff.join(',') : xff;
    for (const part of joined?.split(',') ?? []) {
      const ip = normaliseIp(part.trim());
      if (ip !== undefined) {
        hops.push(ip);
      }
    }
  }
  if (remoteAddress !== undefined) {
    hops.push(remoteAddress);
  }
  return hops;
}

/**
 * Validates an IP literal, stripping IPv4-mapped prefixes, brackets and
 * ports. Returns `undefined` for anything that is not a valid address.
 */
export function normaliseIp(value: string | undefined): string | undefined {
  if (value === undefined) {
    return undefined;
  }
  let candidate = value.trim();
  if (candidate.startsWith('[')) {
    const end = candidate.indexOf(']');
    candidate = end === -1 ? candidate.slice(1) : candidate.slice(1, end);
  } else if (/^\d{1,3}(\.\d{1,3}){3}:\d+$/.test(candidate)) {
    candidate = candidate.slice(0, candidate.lastIndexOf(':'));
  }
  if (/^::ffff:\d{1,3}(\.\d{1,3}){3}$/i.test(candidate)) {
    candidate = candidate.slice(7);
  }
  return isIP(candidate) === 0 ? undefined : candidate.toLowerCase();
}

/** IPv4 → the address; IPv6 → canonical `prefix/len` for the given length. */
export function networkOf(ip: string, ipv6PrefixLength: number): string {
  if (isIP(ip) !== 6) {
    return ip;
  }
  const groups = expandIpv6(ip);
  const keepBits = Math.min(128, Math.max(0, ipv6PrefixLength));
  const masked = groups.map((group, index) => {
    const bitsBefore = index * 16;
    const bits = Math.min(16, Math.max(0, keepBits - bitsBefore));
    const mask = bits === 0 ? 0 : (0xffff << (16 - bits)) & 0xffff;
    return (group & mask).toString(16);
  });
  return `${masked.join(':')}/${keepBits}`;
}

function expandIpv6(ip: string): number[] {
  let address = ip;
  // Embedded IPv4 (e.g. ::ffff:1.2.3.4) → two hex groups.
  const v4 = /(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/.exec(address);
  if (v4 !== null) {
    const [a, b, c, d] = v4.slice(1, 5).map(Number);
    const hi = ((a ?? 0) << 8) | (b ?? 0);
    const lo = ((c ?? 0) << 8) | (d ?? 0);
    address =
      address.slice(0, v4.index) + `${hi.toString(16)}:${lo.toString(16)}`;
  }
  const [head = '', tail = ''] = address.split('::');
  const headGroups = head.length === 0 ? [] : head.split(':');
  const tailGroups = tail.length === 0 ? [] : tail.split(':');
  const fill = 8 - headGroups.length - tailGroups.length;
  const groups = [
    ...headGroups,
    ...Array(Math.max(0, fill)).fill('0'),
    ...tailGroups,
  ];
  return groups.map((group) => Number.parseInt(group, 16) || 0);
}

function throttleKey(
  parts: Pick<
    ClientIdentity,
    | 'apiKeyFingerprint'
    | 'credentialFingerprint'
    | 'sessionFingerprint'
    | 'network'
  >,
): string {
  if (parts.apiKeyFingerprint !== undefined) {
    return `key:${parts.apiKeyFingerprint}`;
  }
  if (parts.credentialFingerprint !== undefined) {
    return `cred:${parts.credentialFingerprint}`;
  }
  if (parts.sessionFingerprint !== undefined) {
    return `sess:${parts.sessionFingerprint}`;
  }
  if (parts.network !== undefined) {
    return `net:${parts.network}`;
  }
  return 'anonymous';
}

function fingerprint(secret: string | undefined): string | undefined {
  if (secret === undefined) {
    return undefined;
  }
  return createHash('sha256').update(secret).digest('hex').slice(0, 32);
}

function readCookie(
  header: string | undefined,
  name: string,
): string | undefined {
  if (header === undefined) {
    return undefined;
  }
  for (const pair of header.split(';')) {
    const index = pair.indexOf('=');
    if (index === -1) {
      continue;
    }
    if (pair.slice(0, index).trim() === name) {
      const value = pair.slice(index + 1).trim();
      return value.length === 0 ? undefined : value;
    }
  }
  return undefined;
}

function primaryLanguage(header: string | undefined): string | undefined {
  const first = header?.split(',')[0]?.split(';')[0]?.trim();
  return first === undefined || first.length === 0 ? undefined : first;
}

function parseMobileHint(value: string | undefined): boolean | undefined {
  if (value === '?1') {
    return true;
  }
  if (value === '?0') {
    return false;
  }
  return undefined;
}

function unquote(value: string | undefined): string | undefined {
  return value?.replace(/^"(.*)"$/, '$1');
}
