/**
 * Keys of the event-signing scheme: one secret per stream, held by the stream's issuer and by
 * every service in its audience, and nobody else.
 *
 * Production: each service gets only its own keys, as JSON in `EVENT_SIGNING_KEYS`
 *   [{ "keyId": "ledger.payment.settled@v2", "streamId": "ledger.payment.settled", "secret": "…" }]
 * (a secrets manager writes it; rotation = ship v2 to everyone, then flip the issuer to sign with
 * it — verifiers accept every key they hold for the stream).
 *
 * Sandbox / tests: all keys are derived deterministically from one master secret
 * (`EVENT_SIGNING_SECRET`), so the in-process platform can hand every service exactly the keys the
 * authorization matrix says it may hold, without any key distribution.
 */
import crypto from 'crypto';
import { EVENT_STREAMS, SERVICES, streamsFor, type EventStream, type ServiceId } from './streams';

export interface EventSigningKey {
  keyId: string;
  streamId: string;
  /** Raw secret (>= 32 bytes recommended). */
  secret: Buffer;
  /** Only one key per stream signs; every held key of the stream verifies. */
  signing: boolean;
}

export const DEFAULT_KEY_VERSION = 1;

export function keyIdFor(streamId: string, version: number = DEFAULT_KEY_VERSION): string {
  return `${streamId}@v${version}`;
}

/** Deterministic sandbox secret for a key id (HMAC of the master secret). */
export function deriveSecret(masterSecret: string, keyId: string): Buffer {
  return crypto.createHmac('sha256', masterSecret).update(`ksa-event-key:${keyId}`, 'utf8').digest();
}

/**
 * The keys one service holds: every stream it issues or is in the audience of.
 * `version` picks which key version is used for signing (and is also trusted for verification).
 */
export function derivedKeyring(service: ServiceId, masterSecret: string, versions: number[] = [DEFAULT_KEY_VERSION]): EventSigningKey[] {
  const keys: EventSigningKey[] = [];
  for (const stream of streamsFor(service)) {
    for (const version of versions) {
      const keyId = keyIdFor(stream.id, version);
      keys.push({ keyId, streamId: stream.id, secret: deriveSecret(masterSecret, keyId), signing: version === Math.max(...versions) });
    }
  }
  return keys;
}

export class KeyringError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'KeyringError';
  }
}

/** The keys one service holds, indexed by key id and by stream. */
export class Keyring {
  readonly #byId = new Map<string, EventSigningKey>();
  readonly #byStream = new Map<string, EventSigningKey[]>();

  constructor(keys: readonly EventSigningKey[]) {
    for (const key of keys) this.add(key);
  }

  add(key: EventSigningKey): void {
    if (key.secret.byteLength === 0) throw new KeyringError(`Key ${key.keyId} has an empty secret`);
    this.#byId.set(key.keyId, key);
    const forStream = this.#byStream.get(key.streamId) ?? [];
    forStream.push(key);
    this.#byStream.set(key.streamId, forStream);
  }

  /** The key a given key id refers to, if this service holds it. */
  byId(keyId: string): EventSigningKey | undefined {
    return this.#byId.get(keyId);
  }

  /** Every key this service holds for a stream (current + the ones still being rotated out). */
  forStream(streamId: string): EventSigningKey[] {
    return this.#byStream.get(streamId) ?? [];
  }

  /** The key to sign a stream's events with. */
  signingKey(streamId: string): EventSigningKey | undefined {
    const keys = this.forStream(streamId);
    return keys.find((k) => k.signing) ?? keys[0];
  }

  get keyIds(): string[] {
    return [...this.#byId.keys()].sort();
  }

  get size(): number {
    return this.#byId.size;
  }
}

export interface KeyringEnv {
  EVENT_SIGNING_KEYS?: string;
  EVENT_SIGNING_SECRET?: string;
  EVENT_SIGNING_KEY_VERSION?: string;
}

/** Sandbox default: every service derives its keys from the same master secret. */
export const DEFAULT_MASTER_SECRET = 'ksa-platform-dev-event-signing-secret';

/**
 * The keyring of `service`: `EVENT_SIGNING_KEYS` when present (production: only what was handed
 * to this service), otherwise derived from the master secret for the streams the matrix allows.
 */
export function keyringFor(service: ServiceId, env: KeyringEnv = process.env): Keyring {
  const explicit = env.EVENT_SIGNING_KEYS?.trim();
  if (explicit) {
    let parsed: unknown;
    try {
      parsed = JSON.parse(explicit);
    } catch (err) {
      throw new KeyringError(`EVENT_SIGNING_KEYS is not valid JSON: ${(err as Error).message}`);
    }
    if (!Array.isArray(parsed)) throw new KeyringError('EVENT_SIGNING_KEYS must be a JSON array of { keyId, streamId, secret }');
    return new Keyring(
      parsed.map((raw, i) => {
        const entry = raw as { keyId?: unknown; streamId?: unknown; secret?: unknown; signing?: unknown };
        if (typeof entry.keyId !== 'string' || typeof entry.streamId !== 'string' || typeof entry.secret !== 'string') {
          throw new KeyringError(`EVENT_SIGNING_KEYS[${i}] needs keyId, streamId and secret`);
        }
        if (!EVENT_STREAMS[entry.streamId]) throw new KeyringError(`EVENT_SIGNING_KEYS[${i}] names unknown stream ${entry.streamId}`);
        return {
          keyId: entry.keyId,
          streamId: entry.streamId,
          secret: Buffer.from(entry.secret, 'utf8'),
          signing: entry.signing !== false,
        };
      })
    );
  }
  const version = Number(env.EVENT_SIGNING_KEY_VERSION ?? DEFAULT_KEY_VERSION);
  if (!Number.isInteger(version) || version < 1) throw new KeyringError('EVENT_SIGNING_KEY_VERSION must be a positive integer');
  const versions = version === DEFAULT_KEY_VERSION ? [DEFAULT_KEY_VERSION] : [DEFAULT_KEY_VERSION, version];
  return new Keyring(derivedKeyring(service, env.EVENT_SIGNING_SECRET ?? DEFAULT_MASTER_SECRET, versions));
}

/** Which streams a service is entitled to, for diagnostics / the topology report. */
export function keyDistribution(): Array<{ stream: EventStream; keyId: string; holders: ServiceId[] }> {
  return Object.values(EVENT_STREAMS).map((stream) => ({
    stream,
    keyId: keyIdFor(stream.id),
    holders: [stream.issuer, ...stream.audience.filter((s) => s !== stream.issuer)],
  }));
}

export { SERVICES };
