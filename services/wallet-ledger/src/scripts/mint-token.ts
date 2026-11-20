/**
 * Development / integration-testing helper: mint an access token signed with
 * the configured HS256 secret (AUTH_JWT_SECRET).
 *
 *   npm run token:mint -- --sub user_alice --roles user --ttl 15m
 *   npm run token:mint -- --sub ops_jane --roles admin
 *   npm run token:mint -- --sub svc_bank_gateway --roles payout_webhook
 *
 * In production, tokens come from your identity provider; this script refuses
 * to run when NODE_ENV=production.
 */
import { createSecretKey } from 'crypto';
import { SignJWT } from 'jose';
import { config } from '../config/env';
import { ROLES } from '../auth/principal';

function arg(name: string, fallback?: string): string | undefined {
  const idx = process.argv.indexOf(`--${name}`);
  return idx >= 0 ? process.argv[idx + 1] : fallback;
}

async function main(): Promise<void> {
  if (config.nodeEnv === 'production') {
    throw new Error('Refusing to mint tokens in production: use the identity provider');
  }
  const sub = arg('sub');
  if (!sub) throw new Error('Usage: --sub <owner_id> [--roles user,admin,payout_webhook] [--ttl 15m]');
  const roles = (arg('roles', 'user') ?? 'user').split(',').map((r) => r.trim());
  const unknown = roles.filter((r) => !(ROLES as readonly string[]).includes(r));
  if (unknown.length > 0) throw new Error(`Unknown role(s): ${unknown.join(', ')}; expected ${ROLES.join(', ')}`);
  if (Buffer.byteLength(config.auth.jwtSecret, 'utf8') < 32) {
    throw new Error('AUTH_JWT_SECRET must be set (>= 32 bytes) to mint development tokens');
  }

  const token = await new SignJWT({ roles })
    .setProtectedHeader({ alg: 'HS256', typ: 'JWT' })
    .setSubject(sub)
    .setIssuer(config.auth.issuer)
    .setAudience(config.auth.audience)
    .setIssuedAt()
    .setExpirationTime(arg('ttl', '15m') ?? '15m')
    .sign(createSecretKey(Buffer.from(config.auth.jwtSecret, 'utf8')));
  process.stdout.write(`${token}\n`);
}

main().catch((err: unknown) => {
  process.stderr.write(`${err instanceof Error ? err.message : String(err)}\n`);
  process.exitCode = 1;
});
