/* eslint-disable @typescript-eslint/no-unused-vars */
import { describe, it, test, expect, beforeEach, afterEach, beforeAll, afterAll, vi } from 'vitest';
/**
 * tests/unit/security/vaultCrypto.test.ts
 *
 * The vault's cryptographic core: key parsing, AES-256-GCM envelopes, AAD
 * binding, tamper detection, keyring rotation and keyed fingerprints.
 */

import crypto from 'crypto';
import {
  createKeyring,
  generateKey,
  parseKey,
  parseRetiredKeys,
  VaultCryptoError,
} from '../../../src/security/vaultCrypto';
import type { Envelope } from '../../../src/security/vaultCrypto';

const KEY_A = crypto.randomBytes(32).toString('base64');
const KEY_B = crypto.randomBytes(32).toString('hex');
const AAD = 'hyperpay-vault:v1:fingerprint-abc';
const PLAINTEXT_PAYLOAD = Buffer.from(JSON.stringify({ registrationId: '8ac7a4a0deadbeef', last4Digits: '5556' }));

function tamper(envelope: Envelope, field: keyof Envelope): Envelope {
  const bytes = Buffer.from(envelope[field], 'base64');
  bytes[0] = (bytes[0] ?? 0) ^ 0xff;
  return { ...envelope, [field]: bytes.toString('base64') };
}

describe('parseKey()', () => {
  it('accepts 32 bytes as base64', () => {
    expect(parseKey(KEY_A, 'k')).toEqual(Buffer.from(KEY_A, 'base64'));
  });

  it('accepts 32 bytes as hex, case-insensitively', () => {
    expect(parseKey(KEY_B.toUpperCase(), 'k')).toEqual(Buffer.from(KEY_B, 'hex'));
  });

  it('trims whitespace', () => {
    expect(parseKey(`  ${KEY_A}\n`, 'k')).toEqual(Buffer.from(KEY_A, 'base64'));
  });

  it.each([
    ['', 'empty'],
    ['short', 'too short'],
    [crypto.randomBytes(16).toString('base64'), '16 bytes base64'],
    [crypto.randomBytes(31).toString('hex'), '31 bytes hex'],
    [crypto.randomBytes(33).toString('base64'), '33 bytes base64'],
    ['not-base64-or-hex-at-all-really-not-!!!!!!!!', 'garbage'],
  ])('rejects %p (%s) with a descriptive error', (raw) => {
    expect(() => parseKey(raw, 'VAULT_MASTER_KEY')).toThrow(VaultCryptoError);
    expect(() => parseKey(raw, 'VAULT_MASTER_KEY')).toThrow(/VAULT_MASTER_KEY must be a 32-byte key/);
  });

  it('generateKey() produces a key parseKey accepts', () => {
    expect(parseKey(generateKey(), 'k')).toHaveLength(32);
    expect(generateKey()).not.toBe(generateKey());
  });
});

describe('parseRetiredKeys()', () => {
  it('parses id=key pairs, ignoring blanks', () => {
    const keys = parseRetiredKeys(` v1=${KEY_A} , , v0=${KEY_B} `);
    expect([...keys.keys()]).toEqual(['v1', 'v0']);
    expect(keys.get('v0')).toEqual(Buffer.from(KEY_B, 'hex'));
  });

  it('returns an empty map for an empty string', () => {
    expect(parseRetiredKeys('').size).toBe(0);
  });

  it.each([[`=${KEY_A}`], [KEY_A], [`v1=${KEY_A},v1=${KEY_B}`], ['v1=short']])('rejects %p', (raw) => {
    expect(() => parseRetiredKeys(raw)).toThrow(VaultCryptoError);
  });
});

describe('createKeyring()', () => {
  it('rejects an empty active key id', () => {
    expect(() => createKeyring({ activeKeyId: ' ', activeKey: KEY_A })).toThrow(/VAULT_MASTER_KEY_ID/);
  });

  it('rejects an active key id that is also retired', () => {
    expect(() => createKeyring({ activeKeyId: 'v1', activeKey: KEY_A, retiredKeys: `v1=${KEY_B}` })).toThrow(/also appears/);
  });

  it('lists the active and retired key ids', () => {
    const ring = createKeyring({ activeKeyId: 'v2', activeKey: KEY_A, retiredKeys: `v1=${KEY_B}` });
    expect(ring.activeKeyId).toBe('v2');
    expect([...ring.keyIds].sort()).toEqual(['v1', 'v2']);
  });
});

describe('encrypt() / decrypt()', () => {
  const ring = createKeyring({ activeKeyId: 'v1', activeKey: KEY_A });

  it('round-trips and stamps the active key id', () => {
    const envelope = ring.encrypt(PLAINTEXT_PAYLOAD, AAD);

    expect(envelope.keyId).toBe('v1');
    expect(Buffer.from(envelope.iv, 'base64')).toHaveLength(12);
    expect(Buffer.from(envelope.authTag, 'base64')).toHaveLength(16);
    expect(ring.decrypt(envelope, AAD)).toEqual(PLAINTEXT_PAYLOAD);
  });

  it('never exposes the plaintext in the envelope', () => {
    const envelope = ring.encrypt(PLAINTEXT_PAYLOAD, AAD);
    const serialised = JSON.stringify(envelope);
    expect(serialised).not.toContain('8ac7a4a0deadbeef');
    expect(serialised).not.toContain('5556');
    expect(Buffer.from(envelope.ciphertext, 'base64').equals(PLAINTEXT_PAYLOAD)).toBe(false);
  });

  it('uses a fresh IV every time — identical plaintexts yield different ciphertexts', () => {
    const a = ring.encrypt(PLAINTEXT_PAYLOAD, AAD);
    const b = ring.encrypt(PLAINTEXT_PAYLOAD, AAD);
    expect(a.iv).not.toBe(b.iv);
    expect(a.ciphertext).not.toBe(b.ciphertext);
  });

  it.each(['ciphertext', 'authTag', 'iv'] as const)('detects a tampered %s', (field) => {
    const envelope = ring.encrypt(PLAINTEXT_PAYLOAD, AAD);
    expect(() => ring.decrypt(tamper(envelope, field), AAD)).toThrow(/failed authentication/);
  });

  it('refuses to decrypt under different associated data (envelope swapped between records)', () => {
    const envelope = ring.encrypt(PLAINTEXT_PAYLOAD, AAD);
    expect(() => ring.decrypt(envelope, 'hyperpay-vault:v1:fingerprint-xyz')).toThrow(/failed authentication/);
  });

  it('refuses a truncated ciphertext', () => {
    const envelope = ring.encrypt(PLAINTEXT_PAYLOAD, AAD);
    const truncated = { ...envelope, ciphertext: Buffer.from(envelope.ciphertext, 'base64').subarray(0, 4).toString('base64') };
    expect(() => ring.decrypt(truncated, AAD)).toThrow(VaultCryptoError);
  });

  it('rejects a malformed envelope before touching the cipher', () => {
    const envelope = ring.encrypt(PLAINTEXT_PAYLOAD, AAD);
    expect(() => ring.decrypt({ ...envelope, iv: 'AAAA' }, AAD)).toThrow(/malformed/);
    expect(() => ring.decrypt({ ...envelope, authTag: 'AAAA' }, AAD)).toThrow(/malformed/);
  });

  it('fails with a clear message for an unknown key id', () => {
    const envelope = ring.encrypt(PLAINTEXT_PAYLOAD, AAD);
    expect(() => ring.decrypt({ ...envelope, keyId: 'v9' }, AAD)).toThrow(/No key "v9" in the keyring/);
  });

  it('cannot decrypt with a different keyring holding a different key', () => {
    const other = createKeyring({ activeKeyId: 'v1', activeKey: KEY_B });
    expect(() => other.decrypt(ring.encrypt(PLAINTEXT_PAYLOAD, AAD), AAD)).toThrow(/failed authentication/);
  });
});

describe('key rotation', () => {
  it('a rotated keyring decrypts old envelopes with the retired key and seals new ones with the active key', () => {
    const oldRing = createKeyring({ activeKeyId: 'v1', activeKey: KEY_A });
    const sealedUnderV1 = oldRing.encrypt(PLAINTEXT_PAYLOAD, AAD);

    const newRing = createKeyring({ activeKeyId: 'v2', activeKey: KEY_B, retiredKeys: `v1=${KEY_A}` });

    expect(newRing.decrypt(sealedUnderV1, AAD)).toEqual(PLAINTEXT_PAYLOAD);
    const resealed = newRing.encrypt(newRing.decrypt(sealedUnderV1, AAD), AAD);
    expect(resealed.keyId).toBe('v2');
    expect(newRing.decrypt(resealed, AAD)).toEqual(PLAINTEXT_PAYLOAD);
    // The old keyring cannot read what the new one sealed.
    expect(() => oldRing.decrypt(resealed, AAD)).toThrow(/No key "v2"/);
  });
});

describe('fingerprint()', () => {
  it('is deterministic for the same keyring and value', () => {
    const ring = createKeyring({ activeKeyId: 'v1', activeKey: KEY_A });
    expect(ring.fingerprint('token-1')).toBe(ring.fingerprint('token-1'));
    expect(ring.fingerprint('token-1')).toMatch(/^[0-9a-f]{64}$/);
  });

  it('differs per value', () => {
    const ring = createKeyring({ activeKeyId: 'v1', activeKey: KEY_A });
    expect(ring.fingerprint('token-1')).not.toBe(ring.fingerprint('token-2'));
  });

  it('is keyed — a different master key gives a different fingerprint, and it is not a plain SHA-256', () => {
    const a = createKeyring({ activeKeyId: 'v1', activeKey: KEY_A });
    const b = createKeyring({ activeKeyId: 'v1', activeKey: KEY_B });
    expect(a.fingerprint('token-1')).not.toBe(b.fingerprint('token-1'));
    expect(a.fingerprint('token-1')).not.toBe(crypto.createHash('sha256').update('token-1').digest('hex'));
  });
});
