import { createSecretKey } from 'crypto';
import { SignJWT } from 'jose';
import type { FastifyInstance, InjectOptions, LightMyRequestResponse } from 'fastify';
import { config } from '../../src/config/env';
import { query } from '../../src/db';
import type { Role } from '../../src/auth/principal';

export interface MintOptions {
  sub: string;
  roles?: string[];
  expiresIn?: string | number;
  issuer?: string;
  audience?: string;
  secret?: string;
  notBefore?: string | number;
  omitExp?: boolean;
  omitSub?: boolean;
}

/** Mint a real HS256 JWT with the test secret (same library the server verifies with). */
export async function mintToken(opts: MintOptions): Promise<string> {
  let jwt = new SignJWT({ roles: opts.roles ?? ['user'] })
    .setProtectedHeader({ alg: 'HS256', typ: 'JWT' })
    .setIssuedAt()
    .setIssuer(opts.issuer ?? config.auth.issuer)
    .setAudience(opts.audience ?? config.auth.audience);
  if (!opts.omitSub) jwt = jwt.setSubject(opts.sub);
  if (!opts.omitExp) jwt = jwt.setExpirationTime(opts.expiresIn ?? '5m');
  if (opts.notBefore !== undefined) jwt = jwt.setNotBefore(opts.notBefore);
  return jwt.sign(createSecretKey(Buffer.from(opts.secret ?? config.auth.jwtSecret, 'utf8')));
}

export async function bearer(sub: string, roles: Role[] = ['user']): Promise<{ authorization: string }> {
  return { authorization: `Bearer ${await mintToken({ sub, roles })}` };
}

export const ADMIN = { sub: 'staff_admin', roles: ['admin'] as Role[] };
export const BANK_GATEWAY = { sub: 'svc_bank_gateway', roles: ['payout_webhook'] as Role[] };

async function ownerOf(walletId: unknown): Promise<string> {
  if (typeof walletId !== 'string') return 'user_unknown';
  const res = await query<{ owner_id: string }>(`SELECT owner_id FROM wallets WHERE wallet_id = $1`, [walletId]);
  return res.rows[0]?.owner_id ?? 'user_unknown';
}

/**
 * "Legitimate actor" injector for business-logic suites: each request carries
 * the token of the party entitled to make it (the source wallet's owner for
 * money movement, an admin for reviews/audits/reads, the bank gateway for
 * webhooks). Authorization boundaries themselves are tested with explicit
 * principals in auth.test.ts.
 */
export function actAs(app: FastifyInstance): { inject(opts: InjectOptions): Promise<LightMyRequestResponse> } {
  return {
    async inject(opts: InjectOptions): Promise<LightMyRequestResponse> {
      const url = String(opts.url ?? '');
      const method = String(opts.method ?? 'GET').toUpperCase();
      const payload = (opts.payload ?? {}) as Record<string, unknown>;

      let sub: string;
      let roles: Role[];
      if (method === 'POST' && url === '/wallets/withdraw') {
        ({ sub, roles } = { sub: await ownerOf(payload.walletId), roles: ['user'] });
      } else if (method === 'POST' && url === '/wallets/transfer') {
        ({ sub, roles } = { sub: await ownerOf(payload.fromWalletId), roles: ['user'] });
      } else if (url === '/payouts/webhook') {
        ({ sub, roles } = BANK_GATEWAY);
      } else {
        ({ sub, roles } = ADMIN); // reviews, audits, and reads
      }

      const headers = { ...(opts.headers ?? {}), ...(await bearer(sub, roles)) };
      return app.inject({ ...opts, headers });
    },
  };
}
