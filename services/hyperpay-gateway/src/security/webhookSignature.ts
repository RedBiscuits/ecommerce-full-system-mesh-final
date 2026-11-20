/**
 * src/security/webhookSignature.ts
 *
 * HMAC-SHA256 webhook signatures (pure functions — no Express here).
 *
 * Header format (`X-Webhook-Signature`):
 *
 *   t=<unix-seconds>,v1=<hex hmac>[,v1=<hex hmac>…]
 *
 * The signed payload is `<t>.<raw request body>`. Binding the timestamp into
 * the MAC means a captured request cannot be replayed after `toleranceSec`,
 * and the MAC is computed over the *raw bytes* the sender transmitted — never
 * over a re-serialised object, which would silently break on key order or
 * whitespace. Several `v1` values are accepted so a sender can rotate
 * secrets without a gap.
 *
 * `sign()` is the reference implementation for upstream services (and tests).
 */

import crypto from 'crypto';

export const SIGNATURE_HEADER = 'x-webhook-signature';
export const SIGNATURE_VERSION = 'v1';

export interface ParsedSignature {
  timestamp: number;
  signatures: string[];
}

export type VerificationFailure =
  | 'missing'
  | 'malformed'
  | 'timestamp_out_of_tolerance'
  | 'mismatch';

export type VerificationResult = { ok: true } | { ok: false; reason: VerificationFailure };

export interface VerifyOptions {
  secret: string;
  /** Max |now − t| in seconds. */
  toleranceSec: number;
  /** Injectable clock (ms since epoch). */
  now?: () => number;
}

// ─── Signing ──────────────────────────────────────────────────────────────────

export function computeSignature(secret: string, timestamp: number, rawBody: Buffer | string): string {
  return crypto
    .createHmac('sha256', secret)
    .update(`${timestamp}.`)
    .update(rawBody)
    .digest('hex');
}

/** Build a complete header value for `rawBody` at `timestamp` (unix seconds). */
export function sign(secret: string, rawBody: Buffer | string, timestamp: number): string {
  return `t=${timestamp},${SIGNATURE_VERSION}=${computeSignature(secret, timestamp, rawBody)}`;
}

// ─── Parsing ──────────────────────────────────────────────────────────────────

export function parseSignatureHeader(header: string | undefined): ParsedSignature | null {
  if (!header) return null;

  let timestamp: number | undefined;
  const signatures: string[] = [];

  for (const part of header.split(',')) {
    const eq = part.indexOf('=');
    if (eq === -1) return null;
    const key = part.slice(0, eq).trim();
    const value = part.slice(eq + 1).trim();

    if (key === 't') {
      if (timestamp !== undefined || !/^\d+$/.test(value)) return null;
      timestamp = Number(value);
    } else if (key === SIGNATURE_VERSION) {
      if (!/^[0-9a-f]{64}$/i.test(value)) return null;
      signatures.push(value.toLowerCase());
    }
    // Unknown keys are ignored for forward compatibility.
  }

  if (timestamp === undefined || signatures.length === 0) return null;
  return { timestamp, signatures };
}

// ─── Verification ─────────────────────────────────────────────────────────────

function constantTimeEqualHex(a: string, b: string): boolean {
  const bufA = Buffer.from(a, 'hex');
  const bufB = Buffer.from(b, 'hex');
  return bufA.length === bufB.length && crypto.timingSafeEqual(bufA, bufB);
}

export function verify(
  header: string | undefined,
  rawBody: Buffer | string,
  { secret, toleranceSec, now = Date.now }: VerifyOptions,
): VerificationResult {
  if (!header) return { ok: false, reason: 'missing' };

  const parsed = parseSignatureHeader(header);
  if (!parsed) return { ok: false, reason: 'malformed' };

  const ageSec = Math.abs(Math.floor(now() / 1000) - parsed.timestamp);
  if (ageSec > toleranceSec) return { ok: false, reason: 'timestamp_out_of_tolerance' };

  const expected = computeSignature(secret, parsed.timestamp, rawBody);
  const matched = parsed.signatures.some((candidate) => constantTimeEqualHex(candidate, expected));
  return matched ? { ok: true } : { ok: false, reason: 'mismatch' };
}
