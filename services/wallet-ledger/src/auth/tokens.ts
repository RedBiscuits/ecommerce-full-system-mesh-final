import { createSecretKey } from 'crypto';
import { errors as joseErrors, importSPKI, jwtVerify } from 'jose';
import type { KeyLike } from 'jose';
import { config } from '../config/env';
import { Principal, ROLES, Role } from './principal';

export class AuthenticationError extends Error {
  public readonly statusCode = 401;
  public readonly code = 'UNAUTHENTICATED';
  constructor(
    message: string,
    /** RFC 6750 error code for the WWW-Authenticate header. */
    public readonly bearerError: 'invalid_request' | 'invalid_token' = 'invalid_token'
  ) {
    super(message);
    this.name = 'AuthenticationError';
  }
}

export class AuthorizationError extends Error {
  public readonly statusCode = 403;
  public readonly code = 'FORBIDDEN';
  constructor(message = 'Insufficient role for this operation') {
    super(message);
    this.name = 'AuthorizationError';
  }
}

const ASYMMETRIC = new Set(['RS256', 'RS384', 'RS512', 'PS256', 'PS384', 'PS512', 'ES256', 'ES384', 'ES512', 'EdDSA']);
const SYMMETRIC = new Set(['HS256', 'HS384', 'HS512']);

let cachedKey: { fingerprint: string; key: Promise<KeyLike> } | null = null;

/**
 * Validate the auth configuration (called at app startup so a misconfigured
 * deployment fails fast instead of rejecting every request at runtime).
 */
export function assertAuthConfigured(): void {
  const { jwtSecret, jwtPublicKey, algorithm } = config.auth;
  if (jwtPublicKey) {
    if (!ASYMMETRIC.has(algorithm)) throw new Error(`AUTH_JWT_ALGORITHM ${algorithm} is not an asymmetric algorithm`);
    return;
  }
  if (!SYMMETRIC.has(algorithm)) throw new Error(`AUTH_JWT_ALGORITHM ${algorithm} requires AUTH_JWT_PUBLIC_KEY`);
  if (Buffer.byteLength(jwtSecret, 'utf8') < 32) {
    throw new Error('AUTH_JWT_SECRET must be at least 32 bytes (or configure AUTH_JWT_PUBLIC_KEY)');
  }
}

function verificationKey(): Promise<KeyLike> {
  const { jwtSecret, jwtPublicKey, algorithm } = config.auth;
  const fingerprint = `${algorithm}|${jwtPublicKey || jwtSecret}`;
  if (cachedKey && cachedKey.fingerprint === fingerprint) return cachedKey.key;
  const key: Promise<KeyLike> = jwtPublicKey
    ? importSPKI(jwtPublicKey, algorithm)
    : Promise.resolve(createSecretKey(Buffer.from(jwtSecret, 'utf8')));
  cachedKey = { fingerprint, key };
  return key;
}

/**
 * Verify a bearer token and map it to a Principal. Enforces: signature with
 * the single configured algorithm (no `alg: none`, no algorithm confusion),
 * issuer, audience, expiry (required) with bounded clock skew, and `sub`.
 */
export async function verifyAccessToken(token: string): Promise<Principal> {
  let payload;
  try {
    ({ payload } = await jwtVerify(token, await verificationKey(), {
      algorithms: [config.auth.algorithm],
      issuer: config.auth.issuer,
      audience: config.auth.audience,
      clockTolerance: config.auth.clockToleranceSec,
      requiredClaims: ['sub', 'exp'],
    }));
  } catch (err) {
    // Never leak verification details to the client; classify for logs only.
    const reason = err instanceof joseErrors.JWTExpired ? 'token expired' : 'invalid token';
    throw new AuthenticationError(reason);
  }

  const subject = payload.sub;
  if (typeof subject !== 'string' || subject.length === 0 || subject.length > 100) {
    throw new AuthenticationError('invalid token');
  }

  const claimed = Array.isArray(payload.roles) ? payload.roles : [];
  const roles = new Set<Role>(
    claimed.filter((r: unknown): r is Role => typeof r === 'string' && (ROLES as readonly string[]).includes(r))
  );

  return { kind: 'token', subject, roles };
}
