/**
 * src/security/vaultCrypto.ts
 *
 * Cryptographic primitives for the payment-method vault (pure — no I/O).
 *
 *   - AES-256-GCM, fresh 96-bit IV per record, 128-bit auth tag. Any change to
 *     the ciphertext, tag, IV or associated data makes decryption fail.
 *   - Associated data (AAD) binds a ciphertext to the record it belongs to, so
 *     an envelope copied from one vault record onto another will not decrypt.
 *   - A keyring with one *active* key (encrypts) and any number of *retired*
 *     keys (decrypt only). Every envelope records the `keyId` it was sealed
 *     with, which makes key rotation a re-encrypt loop rather than a flag day.
 *   - Keyed fingerprints (HMAC-SHA256 under a key derived from the active
 *     master key via HKDF) for equality lookups — e.g. "do we already hold
 *     this registration token?" — without storing the value or a plain hash.
 */

import crypto from 'crypto';

// ─── Types ────────────────────────────────────────────────────────────────────

export interface Envelope {
  /** Which keyring entry sealed this envelope. */
  keyId: string;
  /** base64, 12 bytes */
  iv: string;
  /** base64, 16 bytes */
  authTag: string;
  /** base64 */
  ciphertext: string;
}

export interface KeyringOptions {
  activeKeyId: string;
  /** 32 bytes, base64 or hex. */
  activeKey: string;
  /** Decrypt-only keys, `id=key,id=key`. */
  retiredKeys?: string;
}

export interface Keyring {
  readonly activeKeyId: string;
  readonly keyIds: readonly string[];
  encrypt(plaintext: Buffer, aad: string): Envelope;
  decrypt(envelope: Envelope, aad: string): Buffer;
  /** Deterministic keyed hash of `value`, hex. Safe to store and index. */
  fingerprint(value: string): string;
}

export class VaultCryptoError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'VaultCryptoError';
  }
}

// ─── Constants ────────────────────────────────────────────────────────────────

const ALGORITHM = 'aes-256-gcm';
const KEY_BYTES = 32;
const IV_BYTES = 12;
const TAG_BYTES = 16;
const FINGERPRINT_INFO = 'hyperpay-vault/fingerprint/v1';

// ─── Key parsing ──────────────────────────────────────────────────────────────

/** Accepts a 32-byte key as base64 (44 chars) or hex (64 chars). */
export function parseKey(raw: string, label: string): Buffer {
  const value = raw.trim();
  let key: Buffer | undefined;

  if (/^[0-9a-f]{64}$/i.test(value)) {
    key = Buffer.from(value, 'hex');
  } else if (/^[A-Za-z0-9+/]{43}=$/.test(value)) {
    key = Buffer.from(value, 'base64');
  }

  if (!key || key.length !== KEY_BYTES) {
    throw new VaultCryptoError(
      `${label} must be a ${KEY_BYTES}-byte key encoded as base64 (44 chars) or hex (64 chars).`,
    );
  }
  return key;
}

/** Parse `id=key,id=key` into a map. */
export function parseRetiredKeys(raw: string): Map<string, Buffer> {
  const keys = new Map<string, Buffer>();
  for (const entry of raw.split(',').map((e) => e.trim()).filter(Boolean)) {
    const eq = entry.indexOf('=');
    const id = eq === -1 ? '' : entry.slice(0, eq).trim();
    if (!id) throw new VaultCryptoError(`VAULT_RETIRED_KEYS entry "${entry}" must look like "id=key".`);
    if (keys.has(id)) throw new VaultCryptoError(`VAULT_RETIRED_KEYS lists key id "${id}" twice.`);
    keys.set(id, parseKey(entry.slice(eq + 1), `VAULT_RETIRED_KEYS[${id}]`));
  }
  return keys;
}

/** Generate a fresh key in the format VAULT_MASTER_KEY expects. */
export function generateKey(): string {
  return crypto.randomBytes(KEY_BYTES).toString('base64');
}

// ─── Keyring ──────────────────────────────────────────────────────────────────

export function createKeyring(options: KeyringOptions): Keyring {
  const activeKeyId = options.activeKeyId.trim();
  if (!activeKeyId) throw new VaultCryptoError('VAULT_MASTER_KEY_ID must not be empty.');

  const keys = parseRetiredKeys(options.retiredKeys ?? '');
  if (keys.has(activeKeyId)) {
    throw new VaultCryptoError(`Active key id "${activeKeyId}" also appears in VAULT_RETIRED_KEYS.`);
  }
  const activeKey = parseKey(options.activeKey, 'VAULT_MASTER_KEY');
  keys.set(activeKeyId, activeKey);

  const fingerprintKey = Buffer.from(
    crypto.hkdfSync('sha256', activeKey, Buffer.alloc(0), FINGERPRINT_INFO, KEY_BYTES),
  );

  return {
    activeKeyId,
    keyIds: [...keys.keys()],

    encrypt(plaintext, aad): Envelope {
      const iv = crypto.randomBytes(IV_BYTES);
      const cipher = crypto.createCipheriv(ALGORITHM, activeKey, iv, { authTagLength: TAG_BYTES });
      cipher.setAAD(Buffer.from(aad, 'utf-8'));
      const ciphertext = Buffer.concat([cipher.update(plaintext), cipher.final()]);
      return {
        keyId: activeKeyId,
        iv: iv.toString('base64'),
        authTag: cipher.getAuthTag().toString('base64'),
        ciphertext: ciphertext.toString('base64'),
      };
    },

    decrypt(envelope, aad): Buffer {
      const key = keys.get(envelope.keyId);
      if (!key) {
        throw new VaultCryptoError(
          `No key "${envelope.keyId}" in the keyring — add it to VAULT_RETIRED_KEYS to read this record.`,
        );
      }
      const iv = Buffer.from(envelope.iv, 'base64');
      const authTag = Buffer.from(envelope.authTag, 'base64');
      if (iv.length !== IV_BYTES || authTag.length !== TAG_BYTES) {
        throw new VaultCryptoError('Envelope is malformed.');
      }
      try {
        const decipher = crypto.createDecipheriv(ALGORITHM, key, iv, { authTagLength: TAG_BYTES });
        decipher.setAAD(Buffer.from(aad, 'utf-8'));
        decipher.setAuthTag(authTag);
        return Buffer.concat([decipher.update(Buffer.from(envelope.ciphertext, 'base64')), decipher.final()]);
      } catch {
        throw new VaultCryptoError('Envelope failed authentication — wrong key, tampered data, or mismatched record.');
      }
    },

    fingerprint(value): string {
      return crypto.createHmac('sha256', fingerprintKey).update(value, 'utf-8').digest('hex');
    },
  };
}
