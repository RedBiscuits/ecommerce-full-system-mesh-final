/* eslint-disable @typescript-eslint/no-unused-vars */
import { describe, it, test, expect, beforeEach, afterEach, beforeAll, afterAll, vi } from 'vitest';
/**
 * tests/unit/security/webhookSignature.test.ts
 *
 * The HMAC scheme itself: signing, header parsing, verification, replay window.
 */

import crypto from 'crypto';
import {
  computeSignature,
  parseSignatureHeader,
  sign,
  verify,
  SIGNATURE_VERSION,
} from '../../../src/security/webhookSignature';

const SIG_KEY = 'sig_key_test';
const BODY = '{"resourcePath":"/v1/checkouts/abc/payment","id":"abc"}';
const T = 1_800_000_000; // fixed "now" in unix seconds
const nowMs = (): number => T * 1000;
const opts = { secret: SIG_KEY, toleranceSec: 300, now: nowMs };

describe('computeSignature() / sign()', () => {
  it('is HMAC-SHA256 over "<t>.<body>" in hex', () => {
    const expected = crypto.createHmac('sha256', SIG_KEY).update(`${T}.${BODY}`).digest('hex');
    expect(computeSignature(SIG_KEY, T, BODY)).toBe(expected);
    expect(sign(SIG_KEY, BODY, T)).toBe(`t=${T},${SIGNATURE_VERSION}=${expected}`);
  });

  it('treats Buffer and string bodies identically', () => {
    expect(computeSignature(SIG_KEY, T, Buffer.from(BODY))).toBe(computeSignature(SIG_KEY, T, BODY));
  });

  it('changes with the timestamp, body and secret', () => {
    const base = computeSignature(SIG_KEY, T, BODY);
    expect(computeSignature(SIG_KEY, T + 1, BODY)).not.toBe(base);
    expect(computeSignature(SIG_KEY, T, `${BODY} `)).not.toBe(base);
    expect(computeSignature('other', T, BODY)).not.toBe(base);
  });
});

describe('parseSignatureHeader()', () => {
  const hex = 'a'.repeat(64);

  it('parses timestamp and signature', () => {
    expect(parseSignatureHeader(`t=${T},v1=${hex}`)).toEqual({ timestamp: T, signatures: [hex] });
  });

  it('accepts several v1 entries (secret rotation) and ignores unknown keys', () => {
    const other = 'b'.repeat(64);
    expect(parseSignatureHeader(`t=${T},v1=${hex},v0=ignored,v1=${other}`)).toEqual({
      timestamp: T,
      signatures: [hex, other],
    });
  });

  it('tolerates whitespace and upper-case hex', () => {
    expect(parseSignatureHeader(` t=${T} , v1=${hex.toUpperCase()} `)).toEqual({ timestamp: T, signatures: [hex] });
  });

  it.each([
    ['', 'empty'],
    ['garbage', 'no key/value'],
    [`v1=${hex}`, 'missing t'],
    [`t=${T}`, 'missing v1'],
    [`t=abc,v1=${hex}`, 'non-numeric t'],
    [`t=${T},t=${T},v1=${hex}`, 'duplicate t'],
    [`t=${T},v1=zz`, 'non-hex signature'],
    [`t=${T},v1=${hex.slice(1)}`, 'wrong signature length'],
  ])('rejects %p (%s)', (header) => {
    expect(parseSignatureHeader(header)).toBeNull();
  });

  it('returns null for undefined', () => {
    expect(parseSignatureHeader(undefined)).toBeNull();
  });
});

describe('verify()', () => {
  it('accepts a freshly signed body', () => {
    expect(verify(sign(SIG_KEY, BODY, T), BODY, opts)).toEqual({ ok: true });
  });

  it('reports a missing header', () => {
    expect(verify(undefined, BODY, opts)).toEqual({ ok: false, reason: 'missing' });
    expect(verify('', BODY, opts)).toEqual({ ok: false, reason: 'missing' });
  });

  it('reports a malformed header', () => {
    expect(verify('t=1,v1=nothex', BODY, opts)).toEqual({ ok: false, reason: 'malformed' });
  });

  it('rejects a signature made with a different secret', () => {
    expect(verify(sign('wrong', BODY, T), BODY, opts)).toEqual({ ok: false, reason: 'mismatch' });
  });

  it('rejects a tampered body', () => {
    const header = sign(SIG_KEY, BODY, T);
    expect(verify(header, BODY.replace('abc', 'xyz'), opts)).toEqual({ ok: false, reason: 'mismatch' });
  });

  it('rejects a body that differs only in whitespace — the MAC is over raw bytes', () => {
    const header = sign(SIG_KEY, BODY, T);
    expect(verify(header, `${BODY}\n`, opts)).toEqual({ ok: false, reason: 'mismatch' });
  });

  it('rejects a replay older than the tolerance window', () => {
    const old = T - 301;
    expect(verify(sign(SIG_KEY, BODY, old), BODY, opts)).toEqual({ ok: false, reason: 'timestamp_out_of_tolerance' });
  });

  it('accepts a timestamp exactly at the tolerance edge, on either side', () => {
    expect(verify(sign(SIG_KEY, BODY, T - 300), BODY, opts)).toEqual({ ok: true });
    expect(verify(sign(SIG_KEY, BODY, T + 300), BODY, opts)).toEqual({ ok: true });
  });

  it('rejects a timestamp too far in the future', () => {
    expect(verify(sign(SIG_KEY, BODY, T + 301), BODY, opts)).toEqual({ ok: false, reason: 'timestamp_out_of_tolerance' });
  });

  it('checks the timestamp before doing any MAC work', () => {
    // Out-of-window AND wrong secret → the timestamp reason wins.
    expect(verify(sign('wrong', BODY, T - 9999), BODY, opts)).toEqual({ ok: false, reason: 'timestamp_out_of_tolerance' });
  });

  it('accepts when any of several v1 signatures matches (rotation)', () => {
    const stale = computeSignature('old-secret', T, BODY);
    const current = computeSignature(SIG_KEY, T, BODY);
    expect(verify(`t=${T},v1=${stale},v1=${current}`, BODY, opts)).toEqual({ ok: true });
  });

  it('rejects a signature header whose MAC was computed for a different timestamp than t', () => {
    const mac = computeSignature(SIG_KEY, T - 5, BODY);
    expect(verify(`t=${T},v1=${mac}`, BODY, opts)).toEqual({ ok: false, reason: 'mismatch' });
  });

  it('uses the real clock by default', () => {
    const now = Math.floor(Date.now() / 1000);
    expect(verify(sign(SIG_KEY, BODY, now), BODY, { secret: SIG_KEY, toleranceSec: 60 })).toEqual({ ok: true });
  });
});
