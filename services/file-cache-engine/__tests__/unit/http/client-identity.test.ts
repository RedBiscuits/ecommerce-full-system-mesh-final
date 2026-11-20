import { afterEach, describe, expect, it } from 'vitest';

import type { ClientIdentity } from '../../../src/http/client-identity';
import {
  ClientIdentityResolver,
  getClientIdentity,
  networkOf,
  normaliseIp,
} from '../../../src/http/client-identity';
import { startServer, type TestServer } from '../../helpers/http-server';

describe('ClientIdentityResolver', () => {
  let server: TestServer | undefined;

  afterEach(async () => {
    await server?.close();
    server = undefined;
  });

  /** Spins up a server that resolves the identity and echoes it as JSON. */
  async function identify(
    resolver: ClientIdentityResolver,
    headers: Record<string, string> = {},
  ): Promise<ClientIdentity> {
    server = await startServer([resolver.middleware()], (req, res) => {
      res.setHeader('content-type', 'application/json');
      res.end(JSON.stringify(getClientIdentity(req)));
    });
    const response = await fetch(server.url, { headers });
    return (await response.json()) as ClientIdentity;
  }

  it('uses the socket address and ignores forwarding headers by default', async () => {
    const identity = await identify(new ClientIdentityResolver(), {
      'x-forwarded-for': '203.0.113.9',
      'cf-connecting-ip': '198.51.100.1',
    });
    expect(identity.ip).toBe('127.0.0.1');
    expect(identity.ipSource).toBe('socket');
    expect(identity.remoteAddress).toBe('127.0.0.1');
    expect(identity.network).toBe('127.0.0.1');
    expect(identity.key).toBe('net:127.0.0.1');
  });

  it('trusts all hops → left-most X-Forwarded-For entry', async () => {
    const identity = await identify(
      new ClientIdentityResolver({ trustProxy: true }),
      { 'x-forwarded-for': '203.0.113.9, 10.0.0.1' },
    );
    expect(identity.ip).toBe('203.0.113.9');
    expect(identity.ipSource).toBe('header');
  });

  it('trusts exactly n hops → the address n hops from the right', async () => {
    // chain: [spoofed, client, proxy, socket]; with 2 trusted hops the
    // client is the entry two from the right.
    const identity = await identify(
      new ClientIdentityResolver({ trustProxy: 2 }),
      { 'x-forwarded-for': '1.1.1.1, 203.0.113.9, 10.0.0.1' },
    );
    expect(identity.ip).toBe('203.0.113.9');
  });

  it('prefers CDN single-value headers when proxies are trusted', async () => {
    const identity = await identify(
      new ClientIdentityResolver({ trustProxy: true }),
      {
        'cf-connecting-ip': '198.51.100.1',
        'x-forwarded-for': '203.0.113.9',
      },
    );
    expect(identity.ip).toBe('198.51.100.1');
  });

  it('parses RFC 7239 Forwarded with quoted, bracketed IPv6 and ports', async () => {
    const identity = await identify(
      new ClientIdentityResolver({ trustProxy: true }),
      { forwarded: 'for="[2001:db8::1]:4711";proto=https, for=10.0.0.1' },
    );
    expect(identity.ip).toBe('2001:db8::1');
    expect(identity.network).toBe('2001:db8:0:0:0:0:0:0/64');
    expect(identity.key).toBe('net:2001:db8:0:0:0:0:0:0/64');
  });

  it('ignores garbage in forwarding headers', async () => {
    const identity = await identify(
      new ClientIdentityResolver({ trustProxy: true }),
      { 'x-forwarded-for': 'not-an-ip, <script>' },
    );
    expect(identity.ip).toBe('127.0.0.1');
    expect(identity.ipSource).toBe('socket');
  });

  it('accepts an explicit ip resolver override', async () => {
    const identity = await identify(
      new ClientIdentityResolver({
        ipResolver: (): string => '::ffff:9.9.9.9',
      }),
    );
    expect(identity.ip).toBe('9.9.9.9');
    expect(identity.ipSource).toBe('override');
  });

  it('reads the country from edge headers and validates the format', async () => {
    expect(
      (await identify(new ClientIdentityResolver(), { 'cf-ipcountry': 'de' }))
        .country,
    ).toBe('DE');
    await server?.close();
    expect(
      (
        await identify(new ClientIdentityResolver(), {
          'x-country-code': 'Germany',
        })
      ).country,
    ).toBeUndefined();
  });

  it('falls back to a custom (async) country resolver', async () => {
    const identity = await identify(
      new ClientIdentityResolver({
        countryResolver: async (ip): Promise<string | undefined> =>
          ip === '127.0.0.1' ? 'nl' : undefined,
      }),
    );
    expect(identity.country).toBe('NL');
  });

  it('fingerprints credentials instead of exposing them', async () => {
    const identity = await identify(
      new ClientIdentityResolver({ sessionCookie: 'sid' }),
      {
        'x-api-key': 'sample-api-key-val',
        authorization: 'Bearer token',
        cookie: 'theme=dark; sid=abc123',
      },
    );
    expect(identity.apiKeyFingerprint).toMatch(/^[0-9a-f]{32}$/);
    expect(identity.credentialFingerprint).toMatch(/^[0-9a-f]{32}$/);
    expect(identity.sessionFingerprint).toMatch(/^[0-9a-f]{32}$/);
    expect(JSON.stringify(identity)).not.toContain('sample-api-key-val');
    expect(JSON.stringify(identity)).not.toContain('token');
    expect(JSON.stringify(identity)).not.toContain('abc123');
    expect(identity.key).toBe(`key:${identity.apiKeyFingerprint}`);
  });

  it('collects user agent, language, client hints, request id and extras', async () => {
    const identity = await identify(
      new ClientIdentityResolver({
        extractors: {
          tenant: (req): string | undefined =>
            req.headers['x-tenant']?.toString(),
          missing: (): undefined => undefined,
        },
      }),
      {
        'user-agent': 'test-agent/1.0',
        'accept-language': 'en-GB,en;q=0.8',
        'sec-ch-ua-platform': '"macOS"',
        'sec-ch-ua-mobile': '?1',
        'x-request-id': 'req-42',
        'x-tenant': 'acme',
      },
    );
    expect(identity).toMatchObject({
      userAgent: 'test-agent/1.0',
      language: 'en-GB',
      platform: 'macOS',
      mobile: true,
      requestId: 'req-42',
      extra: { tenant: 'acme' },
    });
  });

  it('generates a request id when none is supplied', async () => {
    const identity = await identify(new ClientIdentityResolver());
    expect(identity.requestId).toMatch(/^[0-9a-f-]{36}$/);
  });

  it('memoises per request', async () => {
    const resolver = new ClientIdentityResolver();
    let first: ClientIdentity | undefined;
    let second: ClientIdentity | undefined;
    server = await startServer([], async (req, res) => {
      first = await resolver.resolve(req);
      second = await resolver.resolve(req);
      res.end();
    });
    await fetch(server.url);
    expect(first).toBeDefined();
    expect(first).toBe(second);
  });
});

describe('normaliseIp / networkOf', () => {
  it('normalises IPv4-mapped, bracketed and ported literals', () => {
    expect(normaliseIp('::ffff:10.0.0.1')).toBe('10.0.0.1');
    expect(normaliseIp('[2001:DB8::1]')).toBe('2001:db8::1');
    expect(normaliseIp('10.0.0.1:8080')).toBe('10.0.0.1');
    expect(normaliseIp(' 10.0.0.1 ')).toBe('10.0.0.1');
    expect(normaliseIp('999.1.1.1')).toBeUndefined();
    expect(normaliseIp('')).toBeUndefined();
    expect(normaliseIp(undefined)).toBeUndefined();
  });

  it('derives IPv6 network prefixes of arbitrary length', () => {
    expect(networkOf('2001:db8:abcd:1234:5678::1', 64)).toBe(
      '2001:db8:abcd:1234:0:0:0:0/64',
    );
    expect(networkOf('2001:db8:abcd:1234:5678::1', 48)).toBe(
      '2001:db8:abcd:0:0:0:0:0/48',
    );
    expect(networkOf('2001:db8:abcd:1234:5678::1', 52)).toBe(
      '2001:db8:abcd:1000:0:0:0:0/52',
    );
    expect(networkOf('::ffff:1.2.3.4', 128)).toBe('0:0:0:0:0:ffff:102:304/128');
    expect(networkOf('10.0.0.1', 64)).toBe('10.0.0.1');
  });
});
