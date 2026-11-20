/**
 * src/services/vault.ts
 *
 * Encrypted storage for tokenised payment methods.
 *
 * When a checkout is created with `createRegistration=true`, OPPWA's payment
 * resource carries a `registrationId` — a token that can charge the stored
 * card again — together with the card's brand, last four digits and expiry.
 * None of that belongs in the payments collection in clear. This module:
 *
 *   - `paymentMethodFrom(resource)`  pulls the tokenisation data out of a
 *     payment resource;
 *   - `redactPaymentResource(resource)` returns the resource with that data
 *     removed (the only shape the processor is allowed to persist);
 *   - `vault.store()`                 seals it with AES-256-GCM into the
 *     `payment_methods` collection, once per token (idempotent by keyed
 *     fingerprint — replays and redeliveries never create duplicates). It
 *     takes part in the caller's DB transaction when given one, so the
 *     vault record, the payment record and the TOKEN_REGISTERED outbox
 *     event are a single atomic write;
 *   - `vault.describe()` / `vault.findByCustomer()` expose masked data
 *     (brand, last4, expiry) — the token stays sealed;
 *   - `vault.unseal()`                decrypts the method for *in-process*
 *     use only (charging the card via paymentOperations.ts). It is not
 *     exposed over HTTP — the token never leaves the service. Every unseal
 *     is logged by vault id;
 *   - `vault.rotateKeys()`            re-seals every record under the active
 *     key after the master key changes.
 *
 * Plaintext columns are limited to what lookups need: keyed fingerprints of
 * the token and the customer e-mail, and transaction references.
 *
 * If no master key is configured the vault is *disabled*: tokenisation data
 * is discarded with a warning rather than stored in clear. Production refuses
 * to start in that state (see config/env.ts).
 */

import { config } from '../config/env';
import { createKeyring } from '../security/vaultCrypto';
import { VAULT_COLLECTION } from './collections';
import { db } from './jsonDb';
import type { Envelope, Keyring } from '../security/vaultCrypto';
import type { Db, DbAccess, DbRecord, DbTransaction } from './jsonDb';
import type {
  MaskedPaymentMethod,
  OppwaPaymentResource,
  SafeOppwaPaymentResource,
  VaultedPaymentMethod,
} from '../types/hyperpay';

// ─── Types ────────────────────────────────────────────────────────────────────

/** Row in the `payment_methods` collection. Nothing here is sensitive on its own. */
export interface VaultRecord {
  version: 1;
  /** Keyed fingerprint of the registration token — the idempotency key. */
  tokenFingerprint: string;
  /** Keyed fingerprint of the normalised customer e-mail, for lookups. */
  customerKey?: string;
  /** Transaction that first produced the token. */
  transactionId: string;
  /** Most recent transaction seen with this token. */
  lastTransactionId: string;
  envelope: Envelope;
}

export type PersistedVaultRecord = VaultRecord & DbRecord;

export interface StoreContext {
  transactionId: string;
  customerEmail?: string;
}

export interface StoreResult {
  vaultId: string;
  /** `false` when the token was already vaulted (replay / redelivery). */
  created: boolean;
  /** What may be told about the stored method (brand, last4, expiry) — never the token. */
  masked: MaskedPaymentMethod;
}

export interface RotationSummary {
  activeKeyId: string;
  scanned: number;
  reEncrypted: number;
}

export interface VaultService {
  readonly enabled: boolean;
  /**
   * Seal a payment method. Resolves `null` when the vault is disabled.
   * Pass the caller's transaction handle to make the vault write part of a
   * larger atomic commit; without one, the write is its own transaction.
   */
  store(method: VaultedPaymentMethod, context: StoreContext, tx?: DbTransaction): Promise<StoreResult | null>;
  describe(vaultId: string): Promise<MaskedPaymentMethod | null>;
  findByCustomer(email: string): Promise<MaskedPaymentMethod[]>;
  /**
   * Decrypt a payment method for in-process use. Callers must never log,
   * persist or return the result — pass it straight to the gateway client.
   */
  unseal(vaultId: string): Promise<VaultedPaymentMethod | null>;
  rotateKeys(): Promise<RotationSummary>;
}

export class VaultDisabledError extends Error {
  constructor() {
    super('The payment-method vault is disabled: VAULT_MASTER_KEY is not configured.');
    this.name = 'VaultDisabledError';
  }
}

// ─── Constants ────────────────────────────────────────────────────────────────

export { VAULT_COLLECTION } from './collections';
const RECORD_VERSION = 1;

// ─── Resource helpers (pure) ──────────────────────────────────────────────────

/** Extract the tokenisation data, or `null` if the payment was not tokenised. */
export function paymentMethodFrom(resource: OppwaPaymentResource): VaultedPaymentMethod | null {
  const registrationId = resource.registrationId?.trim();
  if (!registrationId) return null;

  const card = resource.card;
  return {
    registrationId,
    paymentBrand: resource.paymentBrand ?? card?.type ?? 'UNKNOWN',
    last4Digits: card?.last4Digits,
    expiryMonth: card?.expiryMonth,
    expiryYear: card?.expiryYear,
    holder: card?.holder,
  };
}

/** Strip everything that belongs in the vault. The return type cannot carry it. */
export function redactPaymentResource(resource: OppwaPaymentResource): SafeOppwaPaymentResource {
  const safe: OppwaPaymentResource = { ...resource };
  delete safe.card;
  delete safe.registrationId;
  return safe;
}

// ─── Internal helpers ─────────────────────────────────────────────────────────

function aadFor(tokenFingerprint: string): string {
  return `hyperpay-vault:v${RECORD_VERSION}:${tokenFingerprint}`;
}

function normaliseEmail(email: string): string {
  return email.trim().toLowerCase();
}

function withoutUndefined<T extends object>(value: T): T {
  return Object.fromEntries(Object.entries(value).filter(([, v]) => v !== undefined)) as T;
}

// ─── Factory ──────────────────────────────────────────────────────────────────

export function createVault(keyring: Keyring | null, database: Db = db): VaultService {
  let warnedDisabled = false;

  function requireKeyring(): Keyring {
    if (!keyring) throw new VaultDisabledError();
    return keyring;
  }

  function seal(ring: Keyring, method: VaultedPaymentMethod, tokenFingerprint: string): Envelope {
    const plaintext = Buffer.from(JSON.stringify(withoutUndefined(method)), 'utf-8');
    return ring.encrypt(plaintext, aadFor(tokenFingerprint));
  }

  function open(ring: Keyring, record: VaultRecord): VaultedPaymentMethod {
    const plaintext = ring.decrypt(record.envelope, aadFor(record.tokenFingerprint));
    return JSON.parse(plaintext.toString('utf-8')) as VaultedPaymentMethod;
  }

  function mask(record: PersistedVaultRecord, method: VaultedPaymentMethod): MaskedPaymentMethod {
    return withoutUndefined({
      vaultId: record._id,
      paymentBrand: method.paymentBrand,
      last4Digits: method.last4Digits,
      expiryMonth: method.expiryMonth,
      expiryYear: method.expiryYear,
      transactionId: record.transactionId,
      createdAt: record._createdAt,
    });
  }

  /** The store logic, against either the top-level db or a transaction handle. */
  async function storeWith(ring: Keyring, access: DbAccess, method: VaultedPaymentMethod, context: StoreContext): Promise<StoreResult> {
    const tokenFingerprint = ring.fingerprint(method.registrationId);
    const customerKey = context.customerEmail
      ? ring.fingerprint(normaliseEmail(context.customerEmail))
      : undefined;

    const existing = await access.findOne<PersistedVaultRecord>(
      VAULT_COLLECTION,
      (r) => r.tokenFingerprint === tokenFingerprint,
    );

    if (existing) {
      const updated = await access.update<PersistedVaultRecord>(VAULT_COLLECTION, existing._id, {
        lastTransactionId: context.transactionId,
        customerKey: existing.customerKey ?? customerKey,
      });
      console.log(`[Vault] token already vaulted vaultId=${existing._id} transactionId=${context.transactionId}`);
      return { vaultId: existing._id, created: false, masked: mask(updated ?? existing, method) };
    }

    const record: VaultRecord = withoutUndefined({
      version: RECORD_VERSION,
      tokenFingerprint,
      customerKey,
      transactionId: context.transactionId,
      lastTransactionId: context.transactionId,
      envelope: seal(ring, method, tokenFingerprint),
    });
    const saved = await access.insert<VaultRecord>(VAULT_COLLECTION, record);
    console.log(
      `[Vault] sealed payment method vaultId=${saved._id} keyId=${ring.activeKeyId} ` +
        `brand=${method.paymentBrand} transactionId=${context.transactionId}`,
    );
    return { vaultId: saved._id, created: true, masked: mask(saved, method) };
  }

  return {
    enabled: keyring !== null,

    async store(method, context, tx): Promise<StoreResult | null> {
      if (!keyring) {
        if (!warnedDisabled) {
          warnedDisabled = true;
          console.warn('[Vault] VAULT_MASTER_KEY is not set — registration tokens are being DISCARDED.');
        }
        return null;
      }
      const ring = keyring;
      return tx ? storeWith(ring, tx, method, context) : database.transaction((own) => storeWith(ring, own, method, context));
    },

    async describe(vaultId): Promise<MaskedPaymentMethod | null> {
      const ring = requireKeyring();
      const record = await database.findById<PersistedVaultRecord>(VAULT_COLLECTION, vaultId);
      return record ? mask(record, open(ring, record)) : null;
    },

    async findByCustomer(email): Promise<MaskedPaymentMethod[]> {
      const ring = requireKeyring();
      const customerKey = ring.fingerprint(normaliseEmail(email));
      const records = await database.findWhere<PersistedVaultRecord>(
        VAULT_COLLECTION,
        (r) => r.customerKey === customerKey,
      );
      return records.map((record) => mask(record, open(ring, record)));
    },

    async unseal(vaultId): Promise<VaultedPaymentMethod | null> {
      const ring = requireKeyring();
      const record = await database.findById<PersistedVaultRecord>(VAULT_COLLECTION, vaultId);
      if (!record) return null;
      console.warn(`[Vault] UNSEAL vaultId=${vaultId} transactionId=${record.transactionId}`);
      return open(ring, record);
    },

    async rotateKeys(): Promise<RotationSummary> {
      const ring = requireKeyring();
      const records = await database.findAll<PersistedVaultRecord>(VAULT_COLLECTION);
      let reEncrypted = 0;

      for (const record of records) {
        if (record.envelope.keyId === ring.activeKeyId) continue;
        const envelope = seal(ring, open(ring, record), record.tokenFingerprint);
        await database.update<PersistedVaultRecord>(VAULT_COLLECTION, record._id, { envelope });
        reEncrypted += 1;
      }

      console.log(`[Vault] key rotation: ${reEncrypted}/${records.length} record(s) re-sealed under keyId=${ring.activeKeyId}`);
      return { activeKeyId: ring.activeKeyId, scanned: records.length, reEncrypted };
    },
  };
}

// ─── Default instance ─────────────────────────────────────────────────────────

function keyringFromConfig(): Keyring | null {
  const { masterKey, masterKeyId, retiredKeys } = config.vault;
  if (!masterKey) return null;
  return createKeyring({ activeKeyId: masterKeyId, activeKey: masterKey, retiredKeys });
}

export const vault: VaultService = createVault(keyringFromConfig());
