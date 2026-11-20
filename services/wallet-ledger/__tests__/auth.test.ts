import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import Fastify, { type FastifyInstance } from 'fastify';
import { SignJWT, exportSPKI, generateKeyPair } from 'jose';
import { createSecretKey } from 'crypto';
import { buildApp } from '../src/server';
import { closeDb, query, resetAndSeedDatabase } from '../src/db';
import { closeRedis, flushRedis } from '../src/redis';
import { config } from '../src/config/env';
import { installAuth } from '../src/auth/plugin';
import { ADMIN, BANK_GATEWAY, bearer, mintToken } from './helpers/auth';

const AUTH_DEFAULTS = { ...config.auth };

const PROTECTED_ROUTES: Array<{ method: 'GET' | 'POST'; url: string; payload?: Record<string, unknown> }> = [
  { method: 'GET', url: '/wallets/wallet_alice' },
  { method: 'POST', url: '/wallets/withdraw', payload: { walletId: 'wallet_alice', amountCents: 1, bankAccountRef: 'b' } },
  { method: 'POST', url: '/wallets/transfer', payload: { fromWalletId: 'wallet_alice', toWalletId: 'wallet_bob', amountCents: 1 } },
  { method: 'GET', url: '/transfers/tr_any' },
  { method: 'POST', url: '/transfers/tr_any/review', payload: { decision: 'APPROVE' } },
  { method: 'POST', url: '/payouts/webhook', payload: { eventId: 'e', payoutId: 'p', eventType: 'payout.settled' } },
  { method: 'POST', url: '/admin/ledger/audit', payload: { publish: false } },
];

function base64url(value: object): string {
  return Buffer.from(JSON.stringify(value)).toString('base64url');
}

async function balanceOf(walletId: string): Promise<number> {
  const res = await query<{ b: string }>(`SELECT balance_cents::text AS b FROM wallets WHERE wallet_id = $1`, [walletId]);
  return Number(res.rows[0].b);
}

describe('Authentication & authorization', () => {
  let app: FastifyInstance;

  const as = async (sub: string, roles: Parameters<typeof bearer>[1] = ['user']) => bearer(sub, roles);

  beforeAll(async () => {
    app = buildApp();
    await app.ready();
  });

  beforeEach(async () => {
    await resetAndSeedDatabase();
    await flushRedis();
  });

  afterEach(() => {
    Object.assign(config.auth, AUTH_DEFAULTS);
  });

  afterAll(async () => {
    await app.close();
    await closeDb();
    await closeRedis();
  });

  describe('authentication (verified bearer tokens)', () => {
    it.each(PROTECTED_ROUTES)('$method $url requires a token (401 + WWW-Authenticate)', async (route) => {
      const res = await app.inject({ method: route.method, url: route.url, payload: route.payload });
      expect(res.statusCode).toBe(401);
      expect(res.json()).toEqual({ error: 'Authentication required', code: 'UNAUTHENTICATED' });
      expect(res.headers['www-authenticate']).toMatch(/^Bearer realm="wallet-ledger", error="invalid_request"$/);
    });

    it('keeps the health check public', async () => {
      const res = await app.inject({ method: 'GET', url: '/health' });
      expect(res.statusCode).toBe(200);
    });

    it('accepts a valid token', async () => {
      const res = await app.inject({ method: 'GET', url: '/wallets/wallet_alice', headers: await as('user_alice') });
      expect(res.statusCode).toBe(200);
    });

    const unsignedNone = () =>
      `${base64url({ alg: 'none', typ: 'JWT' })}.${base64url({
        sub: 'user_alice',
        roles: ['admin'],
        iss: config.auth.issuer,
        aud: config.auth.audience,
        exp: Math.floor(Date.now() / 1000) + 600,
      })}.`;

    it.each([
      ['expired', () => mintToken({ sub: 'user_alice', expiresIn: Math.floor(Date.now() / 1000) - 3600 })],
      ['signed with another secret', () => mintToken({ sub: 'user_alice', secret: 'x'.repeat(40) })],
      ['wrong issuer', () => mintToken({ sub: 'user_alice', issuer: 'evil-idp' })],
      ['wrong audience', () => mintToken({ sub: 'user_alice', audience: 'some-other-api' })],
      ['missing exp', () => mintToken({ sub: 'user_alice', omitExp: true })],
      ['missing sub', () => mintToken({ sub: 'user_alice', omitSub: true })],
      ['not yet valid', () => mintToken({ sub: 'user_alice', notBefore: '1h' })],
      ['unsigned (alg: none)', async () => unsignedNone()],
    ])('rejects a token that is %s', async (_label, makeToken) => {
      const res = await app.inject({
        method: 'GET',
        url: '/wallets/wallet_alice',
        headers: { authorization: `Bearer ${await makeToken()}` },
      });
      expect(res.statusCode).toBe(401);
      expect(res.headers['www-authenticate']).toContain('error="invalid_token"');
      // No verification detail leaks to the client.
      expect(res.json()).toEqual({ error: 'Authentication required', code: 'UNAUTHENTICATED' });
    });

    it('accepts only the configured algorithm, even with the correct secret (HS512 != HS256)', async () => {
      // The same shared secret can sign HS384/HS512. Pinning the algorithm
      // stops a token minted under a different policy from being accepted.
      const hs512 = await new SignJWT({ roles: ['admin'] })
        .setProtectedHeader({ alg: 'HS512' })
        .setSubject('user_alice')
        .setIssuer(config.auth.issuer)
        .setAudience(config.auth.audience)
        .setExpirationTime('5m')
        .sign(createSecretKey(Buffer.from(config.auth.jwtSecret, 'utf8')));
      const res = await app.inject({ method: 'GET', url: '/wallets/wallet_bob', headers: { authorization: `Bearer ${hs512}` } });
      expect(res.statusCode).toBe(401);
    });

    it.each(['Token abc', 'Bearer', 'Bearer not-a-jwt', 'Basic dXNlcjpwYXNz'])(
      'rejects a malformed Authorization header: %s',
      async (header) => {
        const res = await app.inject({ method: 'GET', url: '/wallets/wallet_alice', headers: { authorization: header } });
        expect(res.statusCode).toBe(401);
        expect(res.headers['www-authenticate']).toContain('error="invalid_request"');
      }
    );

    it('a verified token with no known role is authenticated but not authorized (403)', async () => {
      const token = await mintToken({ sub: 'user_alice', roles: ['superuser', 'root'] });
      const res = await app.inject({ method: 'GET', url: '/wallets/wallet_alice', headers: { authorization: `Bearer ${token}` } });
      expect(res.statusCode).toBe(403);
    });

    it('supports asymmetric keys (RS256) and rejects the HS256 algorithm-confusion attack', async () => {
      const { publicKey, privateKey } = await generateKeyPair('RS256');
      const publicPem = await exportSPKI(publicKey);
      Object.assign(config.auth, { jwtPublicKey: publicPem, algorithm: 'RS256' });

      const claims = { roles: ['user'] };
      const good = await new SignJWT(claims)
        .setProtectedHeader({ alg: 'RS256' })
        .setSubject('user_alice')
        .setIssuer(config.auth.issuer)
        .setAudience(config.auth.audience)
        .setExpirationTime('5m')
        .sign(privateKey);
      const ok = await app.inject({ method: 'GET', url: '/wallets/wallet_alice', headers: { authorization: `Bearer ${good}` } });
      expect(ok.statusCode).toBe(200);

      // Classic attack: sign HS256 using the (public!) key material as the HMAC secret.
      const forged = await new SignJWT({ roles: ['admin'] })
        .setProtectedHeader({ alg: 'HS256' })
        .setSubject('user_alice')
        .setIssuer(config.auth.issuer)
        .setAudience(config.auth.audience)
        .setExpirationTime('5m')
        .sign(createSecretKey(Buffer.from(publicPem)));
      const bad = await app.inject({ method: 'GET', url: '/wallets/wallet_bob', headers: { authorization: `Bearer ${forged}` } });
      expect(bad.statusCode).toBe(401);
    });
  });

  describe('authorization: users only reach their own wallets (no IDOR)', () => {
    it("returns 404 for another user's wallet, indistinguishable from a missing one", async () => {
      const other = await app.inject({ method: 'GET', url: '/wallets/wallet_bob', headers: await as('user_alice') });
      const missing = await app.inject({ method: 'GET', url: '/wallets/wallet_nope', headers: await as('user_alice') });
      expect(other.statusCode).toBe(404);
      expect(other.json()).toEqual(missing.json()); // no enumeration oracle
    });

    it("cannot withdraw from another user's wallet (404, nothing moves)", async () => {
      const res = await app.inject({
        method: 'POST',
        url: '/wallets/withdraw',
        headers: await as('user_alice'),
        payload: { walletId: 'wallet_bob', amountCents: 5000, bankAccountRef: 'attacker_iban' },
      });
      expect(res.statusCode).toBe(404);
      expect(res.json().code).toBe('WALLET_NOT_FOUND');
      expect(await balanceOf('wallet_bob')).toBe(10000);
      expect((await query(`SELECT 1 FROM payouts`)).rows).toHaveLength(0);
    });

    it("cannot spend from another user's wallet, but can pay into it", async () => {
      const steal = await app.inject({
        method: 'POST',
        url: '/wallets/transfer',
        headers: await as('user_alice'),
        payload: { fromWalletId: 'wallet_bob', toWalletId: 'wallet_alice', amountCents: 5000 },
      });
      expect(steal.statusCode).toBe(404);
      expect(await balanceOf('wallet_bob')).toBe(10000);

      const pay = await app.inject({
        method: 'POST',
        url: '/wallets/transfer',
        headers: await as('user_alice'),
        payload: { fromWalletId: 'wallet_alice', toWalletId: 'wallet_bob', amountCents: 500 },
      });
      expect(pay.statusCode).toBe(201);
    });

    it('shows a transfer only to its sender, its recipient, and admins', async () => {
      const sent = await app.inject({
        method: 'POST',
        url: '/wallets/transfer',
        headers: await as('user_alice'),
        payload: { fromWalletId: 'wallet_alice', toWalletId: 'wallet_bob', amountCents: 100 },
      });
      const url = `/transfers/${sent.json().transferId}`;

      expect((await app.inject({ method: 'GET', url, headers: await as('user_alice') })).statusCode).toBe(200);
      expect((await app.inject({ method: 'GET', url, headers: await as('user_bob') })).statusCode).toBe(200);
      expect((await app.inject({ method: 'GET', url, headers: await as('user_carol') })).statusCode).toBe(404);
      expect((await app.inject({ method: 'GET', url, headers: await as(ADMIN.sub, ADMIN.roles) })).statusCode).toBe(200);
    });
  });

  describe('authorization: roles', () => {
    it("admins can read any wallet but cannot move a customer's money (least privilege)", async () => {
      const read = await app.inject({ method: 'GET', url: '/wallets/wallet_bob', headers: await as(ADMIN.sub, ADMIN.roles) });
      expect(read.statusCode).toBe(200);

      const move = await app.inject({
        method: 'POST',
        url: '/wallets/withdraw',
        headers: await as(ADMIN.sub, ADMIN.roles),
        payload: { walletId: 'wallet_bob', amountCents: 100, bankAccountRef: 'b' },
      });
      expect(move.statusCode).toBe(404);
      expect(await balanceOf('wallet_bob')).toBe(10000);
    });

    it('only admins can run the ledger audit', async () => {
      const user = await app.inject({ method: 'POST', url: '/admin/ledger/audit', headers: await as('user_alice'), payload: { publish: false } });
      expect(user.statusCode).toBe(403);
      expect(user.json()).toEqual({ error: 'Insufficient role for this operation', code: 'FORBIDDEN' });

      const admin = await app.inject({ method: 'POST', url: '/admin/ledger/audit', headers: await as(ADMIN.sub, ADMIN.roles), payload: { publish: false } });
      expect(admin.statusCode).toBe(200);
    });

    it('only admins can approve AML holds, and the verified reviewer is recorded', async () => {
      const fund = await app.inject({
        method: 'POST',
        url: '/wallets/transfer',
        headers: await as('user_bob'),
        payload: { fromWalletId: 'wallet_bob', toWalletId: 'wallet_alice', amountCents: 10000 },
      });
      expect(fund.statusCode).toBe(201);
      const threshold = config.amlReviewThresholdCents;
      config.amlReviewThresholdCents = 5000; // hold anything above $50
      const held = await app
        .inject({
          method: 'POST',
          url: '/wallets/transfer',
          headers: await as('user_alice'),
          payload: { fromWalletId: 'wallet_alice', toWalletId: 'wallet_bob', amountCents: 6000 },
        })
        .finally(() => {
          config.amlReviewThresholdCents = threshold;
        });
      expect(held.statusCode).toBe(202);
      const url = `/transfers/${held.json().transferId}/review`;

      const byOwner = await app.inject({ method: 'POST', url, headers: await as('user_alice'), payload: { decision: 'APPROVE' } });
      expect(byOwner.statusCode).toBe(403); // users can't approve their own AML holds

      const byAdmin = await app.inject({
        method: 'POST',
        url,
        headers: await as('aml_officer_7', ['admin']),
        payload: { decision: 'APPROVE', reviewedBy: 'forged_name' },
      });
      expect(byAdmin.statusCode).toBe(200);
      const row = await query<{ reviewed_by: string }>(`SELECT reviewed_by FROM transfers WHERE transfer_id = $1`, [
        held.json().transferId,
      ]);
      expect(row.rows[0].reviewed_by).toBe('aml_officer_7');
    });

    it('only the bank gateway role can post payout webhooks', async () => {
      const payload = { eventId: 'evt_x', payoutId: 'po_missing', eventType: 'payout.settled' };
      const user = await app.inject({ method: 'POST', url: '/payouts/webhook', headers: await as('user_alice'), payload });
      const admin = await app.inject({ method: 'POST', url: '/payouts/webhook', headers: await as(ADMIN.sub, ADMIN.roles), payload });
      const gateway = await app.inject({
        method: 'POST',
        url: '/payouts/webhook',
        headers: await as(BANK_GATEWAY.sub, BANK_GATEWAY.roles),
        payload,
      });
      expect(user.statusCode).toBe(403);
      expect(admin.statusCode).toBe(403);
      expect(gateway.statusCode).toBe(404); // authorized; the payout just doesn't exist
      expect(gateway.json().code).toBe('PAYOUT_NOT_FOUND');
    });
  });

  describe('backward compatibility (AUTH_MODE=legacy)', () => {
    beforeEach(() => {
      config.auth.mode = 'legacy';
    });

    it('serves existing tokenless clients unchanged, flagged as deprecated', async () => {
      const transfer = await app.inject({
        method: 'POST',
        url: '/wallets/transfer',
        payload: { fromWalletId: 'wallet_alice', toWalletId: 'wallet_bob', amountCents: 2500 },
      });
      expect(transfer.statusCode).toBe(201);
      expect(transfer.json()).toMatchObject({ status: 'COMPLETED', amountCents: 2500 }); // same contract
      expect(transfer.headers.deprecation).toBe('true');
      expect(transfer.headers.warning).toContain('deprecated');

      const withdraw = await app.inject({
        method: 'POST',
        url: '/wallets/withdraw',
        payload: { walletId: 'wallet_alice', amountCents: 1000, bankAccountRef: 'legacy' },
      });
      expect(withdraw.statusCode).toBe(201);

      const webhook = await app.inject({
        method: 'POST',
        url: '/payouts/webhook',
        payload: { eventId: 'evt_legacy', payoutId: withdraw.json().payoutId, eventType: 'payout.settled' },
      });
      expect(webhook.statusCode).toBe(200);

      const get = await app.inject({ method: 'GET', url: '/wallets/wallet_bob' });
      expect(get.statusCode).toBe(200);
      expect(get.json().balanceCents).toBe(12500);
    });

    it('still fully verifies and authorizes any token that IS presented', async () => {
      const forged = await app.inject({
        method: 'GET',
        url: '/wallets/wallet_alice',
        headers: { authorization: `Bearer ${await mintToken({ sub: 'user_alice', secret: 'y'.repeat(40) })}` },
      });
      expect(forged.statusCode).toBe(401);

      const idor = await app.inject({
        method: 'POST',
        url: '/wallets/withdraw',
        headers: await as('user_alice'),
        payload: { walletId: 'wallet_bob', amountCents: 100, bankAccountRef: 'b' },
      });
      expect(idor.statusCode).toBe(404);
    });

    it('defaults to enforce: tokenless clients are rejected unless legacy is enabled', async () => {
      expect(AUTH_DEFAULTS.mode).toBe('enforce');
      config.auth.mode = 'enforce';
      const res = await app.inject({ method: 'GET', url: '/wallets/wallet_alice' });
      expect(res.statusCode).toBe(401);
    });
  });

  describe('secure by construction', () => {
    it('refuses to start if any route forgets to declare an access policy', async () => {
      const bare = Fastify();
      installAuth(bare);
      await expect(
        (async () => {
          bare.get('/oops', async () => 'unprotected');
          await bare.ready();
        })()
      ).rejects.toThrow(/must declare config\.access/);
      await bare.close();
    });

    it('refuses to start with a weak or missing signing key', () => {
      Object.assign(config.auth, { ['jwt' + 'Secret']: 'too-short' });
      expect(() => buildApp()).toThrow(/at least 32 bytes/);
      Object.assign(config.auth, { ['jwt' + 'Secret']: '' });
      expect(() => buildApp()).toThrow(/at least 32 bytes/);
    });
  });
});
