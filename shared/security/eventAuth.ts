/**
 * Authenticated service-to-service events.
 *
 * Every message a service publishes carries an HMAC-SHA256 signature made with the key of its
 * stream, plus the headers that say who issued it, which stream it belongs to and which services
 * it is addressed to:
 *
 *   x-signature-alg   HMAC-SHA256
 *   x-key-id          ledger.payment.settled@v1      ← which shared key signed it
 *   x-event-stream    ledger.payment.settled
 *   x-event-issuer    wallet-ledger
 *   x-event-audience  zatca-tax,checkout-coordinator ← who may consume it
 *   x-signed-at       2026-11-15T18:00:00.000Z
 *   x-signature       base64url(HMAC-SHA256(key, canonical string))
 *
 * A consumer accepts a delivery only when ALL of the following hold:
 *   1. it is signed (in `enforce` mode; `permissive` also accepts unsigned during a rollout);
 *   2. the key id names a key THIS service holds — a service outside the audience never gets the
 *      key, so it cannot accept the event even if someone binds its queue to the exchange;
 *   3. the key belongs to the stream the headers name, and the event type really belongs to that
 *      stream (a valid key of one stream cannot sign another stream's traffic);
 *   4. the issuer is the stream's issuer in the authorization matrix;
 *   5. this service is in the event's audience header AND in the matrix's audience;
 *   6. the HMAC over (key id, stream, issuer, audience, event type, event id, signed-at,
 *      sha256(body)) verifies in constant time — so nothing above and no byte of the body can be
 *      changed in flight;
 *   7. it is not dated in the future beyond the allowed clock skew (and not older than
 *      `maxAgeMs`, when one is configured).
 *
 * A failure is an `EventAuthError`: non-retryable, so the delivery is dead-lettered immediately
 * with the reason (`UNSIGNED_EVENT`, `UNKNOWN_KEY_ID`, `AUDIENCE_DENIED`, `SIGNATURE_INVALID`, …)
 * instead of burning retries. Retry / DLQ copies keep the headers, so a replay verifies again.
 */
import crypto from 'crypto';
import { Keyring, keyIdFor, keyringFor, type EventSigningKey } from './keys';
import { isAudience, streamForEventType, streamById, type EventStream, type ServiceId } from './streams';

export const SIGNATURE_HEADERS = {
  alg: 'x-signature-alg',
  keyId: 'x-key-id',
  stream: 'x-event-stream',
  issuer: 'x-event-issuer',
  audience: 'x-event-audience',
  signedAt: 'x-signed-at',
  signature: 'x-signature',
} as const;

export const SIGNATURE_ALGORITHM = 'HMAC-SHA256';
const CANONICAL_VERSION = 'ksa-event-sig-v1';

/** How strictly deliveries are checked. */
export type EventSignatureMode = 'enforce' | 'permissive' | 'off';

export class EventAuthError extends Error {
  /** Retrying cannot fix a bad signature: the consumer dead-letters it at once. */
  public readonly retryable = false;
  constructor(
    public readonly reason: string,
    message: string
  ) {
    super(message);
    this.name = 'EventAuthError';
  }
}

export interface SignInput {
  eventType: string;
  /** The AMQP messageId (the event id); bound into the signature. */
  eventId: string;
  body: Buffer;
  /** Override the clock (tests). */
  signedAt?: Date;
}

export interface VerifyInput {
  headers: Record<string, unknown>;
  content: Buffer;
  /** AMQP properties of the delivery. */
  messageId?: string;
  eventType?: string;
}

export interface VerifiedEvent {
  signed: boolean;
  keyId?: string;
  streamId?: string;
  issuer?: string;
  audience?: string[];
}

export interface EventAuthenticatorOptions {
  keyring?: Keyring;
  mode?: EventSignatureMode;
  /** Accept signatures dated at most this far in the future. Default 5 min. */
  clockSkewMs?: number;
  /** Reject signatures older than this. Default 0 = no age limit (DLQ replays stay verifiable). */
  maxAgeMs?: number;
  now?: () => number;
}

function headerString(headers: Record<string, unknown>, name: string): string | undefined {
  const value = headers[name];
  if (typeof value === 'string') return value.length > 0 ? value : undefined;
  // amqplib may hand back Buffers for long strings.
  if (Buffer.isBuffer(value)) return value.toString('utf8');
  return undefined;
}

/** The exact bytes the HMAC is computed over. */
export function canonicalString(input: {
  keyId: string;
  streamId: string;
  issuer: string;
  audience: readonly string[];
  eventType: string;
  eventId: string;
  signedAt: string;
  body: Buffer;
}): string {
  return [
    CANONICAL_VERSION,
    SIGNATURE_ALGORITHM,
    input.keyId,
    input.streamId,
    input.issuer,
    [...input.audience].sort().join(','),
    input.eventType,
    input.eventId,
    input.signedAt,
    crypto.createHash('sha256').update(input.body).digest('hex'),
  ].join('\n');
}

function hmac(secret: Buffer, canonical: string): string {
  return crypto.createHmac('sha256', secret).update(canonical, 'utf8').digest('base64url');
}

function timingSafeEqual(a: string, b: string): boolean {
  const left = Buffer.from(a, 'utf8');
  const right = Buffer.from(b, 'utf8');
  if (left.byteLength !== right.byteLength) {
    // Still do a comparison of equal length so the work does not depend on the content.
    crypto.timingSafeEqual(left, left);
    return false;
  }
  return crypto.timingSafeEqual(left, right);
}

export function signatureModeFromEnv(env: NodeJS.ProcessEnv = process.env): EventSignatureMode {
  const raw = (env.EVENT_SIGNATURE_MODE ?? 'enforce').trim().toLowerCase();
  if (raw === 'enforce' || raw === 'permissive' || raw === 'off') return raw;
  throw new EventAuthError('INVALID_CONFIG', `EVENT_SIGNATURE_MODE must be enforce | permissive | off, got ${raw}`);
}

/**
 * One service's view of the scheme: the keys it holds, what it may publish and what it may accept.
 */
export class EventAuthenticator {
  readonly keyring: Keyring;
  readonly mode: EventSignatureMode;
  readonly #clockSkewMs: number;
  readonly #maxAgeMs: number;
  readonly #now: () => number;

  constructor(
    readonly service: ServiceId,
    options: EventAuthenticatorOptions = {}
  ) {
    this.keyring = options.keyring ?? keyringFor(service);
    this.mode = options.mode ?? signatureModeFromEnv();
    this.#clockSkewMs = options.clockSkewMs ?? Number(process.env.EVENT_SIGNATURE_CLOCK_SKEW_MS ?? 300_000);
    this.#maxAgeMs = options.maxAgeMs ?? Number(process.env.EVENT_SIGNATURE_MAX_AGE_MS ?? 0);
    this.#now = options.now ?? Date.now;
  }

  /** The streams this service may publish, with the key id it signs them with. */
  issuedStreams(): Array<{ stream: EventStream; keyId: string | undefined }> {
    return this.keyring.keyIds
      .map((id) => this.keyring.byId(id) as EventSigningKey)
      .filter((key) => streamById(key.streamId)?.issuer === this.service && key.signing)
      .map((key) => ({ stream: streamById(key.streamId) as EventStream, keyId: key.keyId }));
  }

  /** Headers that authenticate an event this service publishes. */
  signHeaders(input: SignInput): Record<string, string> {
    if (this.mode === 'off') return {};
    const stream = streamForEventType(input.eventType);
    if (!stream) {
      throw new EventAuthError('UNKNOWN_EVENT_STREAM', `No stream declares event type ${input.eventType} (see shared/security/streams.ts)`);
    }
    if (stream.issuer !== this.service) {
      throw new EventAuthError(
        'NOT_THE_ISSUER',
        `${this.service} may not publish ${input.eventType}: stream ${stream.id} is issued by ${stream.issuer}`
      );
    }
    const key = this.keyring.signingKey(stream.id);
    if (!key) {
      throw new EventAuthError('NO_SIGNING_KEY', `${this.service} holds no signing key for stream ${stream.id} (expected ${keyIdFor(stream.id)})`);
    }
    const signedAt = (input.signedAt ?? new Date(this.#now())).toISOString();
    const audience = [...stream.audience].sort();
    const signature = hmac(
      key.secret,
      canonicalString({
        keyId: key.keyId,
        streamId: stream.id,
        issuer: stream.issuer,
        audience,
        eventType: input.eventType,
        eventId: input.eventId,
        signedAt,
        body: input.body,
      })
    );
    return {
      [SIGNATURE_HEADERS.alg]: SIGNATURE_ALGORITHM,
      [SIGNATURE_HEADERS.keyId]: key.keyId,
      [SIGNATURE_HEADERS.stream]: stream.id,
      [SIGNATURE_HEADERS.issuer]: stream.issuer,
      [SIGNATURE_HEADERS.audience]: audience.join(','),
      [SIGNATURE_HEADERS.signedAt]: signedAt,
      [SIGNATURE_HEADERS.signature]: signature,
    };
  }

  /**
   * Like {@link signHeaders}, but leaves an event type that no stream declares unsigned (the
   * HyperPay outbox also carries legacy event types that no platform consumer subscribes to).
   * An event type that IS declared must still be issued by this service, with its key.
   */
  signHeadersIfIssued(input: SignInput): Record<string, string> {
    if (this.mode === 'off') return {};
    return streamForEventType(input.eventType) ? this.signHeaders(input) : {};
  }

  /** Throws an `EventAuthError` unless this service may accept the delivery. */
  verifyDelivery(input: VerifyInput): VerifiedEvent {
    if (this.mode === 'off') return { signed: false };
    const headers = input.headers ?? {};
    const signature = headerString(headers, SIGNATURE_HEADERS.signature);
    if (!signature) {
      if (this.mode === 'permissive') return { signed: false };
      throw new EventAuthError('UNSIGNED_EVENT', `Unsigned event rejected by ${this.service} (no ${SIGNATURE_HEADERS.signature} header)`);
    }
    const alg = headerString(headers, SIGNATURE_HEADERS.alg);
    if (alg !== SIGNATURE_ALGORITHM) {
      throw new EventAuthError('UNSUPPORTED_ALGORITHM', `Signature algorithm ${alg ?? '(none)'} is not supported (expected ${SIGNATURE_ALGORITHM})`);
    }
    const keyId = headerString(headers, SIGNATURE_HEADERS.keyId);
    if (!keyId) throw new EventAuthError('MISSING_KEY_ID', `Signed event without a ${SIGNATURE_HEADERS.keyId} header`);
    const streamId = headerString(headers, SIGNATURE_HEADERS.stream);
    const issuer = headerString(headers, SIGNATURE_HEADERS.issuer);
    const audienceHeader = headerString(headers, SIGNATURE_HEADERS.audience);
    const signedAt = headerString(headers, SIGNATURE_HEADERS.signedAt);
    const eventType = input.eventType ?? '';
    const eventId = input.messageId ?? '';
    if (!streamId || !issuer || !audienceHeader || !signedAt) {
      throw new EventAuthError('INCOMPLETE_SIGNATURE_HEADERS', `Signed event is missing stream / issuer / audience / signed-at headers`);
    }

    const key = this.keyring.byId(keyId);
    if (!key) {
      throw new EventAuthError(
        'UNKNOWN_KEY_ID',
        `${this.service} does not hold key ${keyId} (stream ${streamId}): it is not in that stream's audience`
      );
    }
    if (key.streamId !== streamId) {
      throw new EventAuthError('STREAM_MISMATCH', `Key ${keyId} belongs to stream ${key.streamId}, not ${streamId}`);
    }
    const stream = streamById(streamId);
    if (!stream) throw new EventAuthError('UNKNOWN_EVENT_STREAM', `Unknown stream ${streamId}`);
    const declared = streamForEventType(eventType);
    if (!declared || declared.id !== stream.id) {
      throw new EventAuthError(
        'EVENT_TYPE_NOT_IN_STREAM',
        `Event type ${eventType || '(none)'} does not belong to stream ${stream.id} (${declared ? declared.id : 'no stream'})`
      );
    }
    if (issuer !== stream.issuer) {
      throw new EventAuthError('ISSUER_MISMATCH', `Stream ${stream.id} is issued by ${stream.issuer}, the event claims ${issuer}`);
    }
    const audience = audienceHeader.split(',').map((a) => a.trim()).filter(Boolean);
    if (!audience.includes(this.service)) {
      throw new EventAuthError('AUDIENCE_DENIED', `${this.service} is not in the audience of this event (${audience.join(', ')})`);
    }
    if (!isAudience(stream, this.service)) {
      throw new EventAuthError('AUDIENCE_DENIED', `${this.service} is not in the audience of stream ${stream.id} (${stream.audience.join(', ')})`);
    }

    const expected = hmac(
      key.secret,
      canonicalString({ keyId, streamId, issuer, audience, eventType, eventId, signedAt, body: input.content })
    );
    if (!timingSafeEqual(expected, signature)) {
      throw new EventAuthError('SIGNATURE_INVALID', `HMAC does not verify for event ${eventId || '(no id)'} of stream ${stream.id}`);
    }

    const signedMs = Date.parse(signedAt);
    if (Number.isNaN(signedMs)) throw new EventAuthError('SIGNATURE_INVALID', `${SIGNATURE_HEADERS.signedAt} is not a timestamp`);
    const now = this.#now();
    if (signedMs - now > this.#clockSkewMs) {
      throw new EventAuthError('SIGNATURE_NOT_YET_VALID', `Event is dated ${signedAt}, more than the allowed skew into the future`);
    }
    if (this.#maxAgeMs > 0 && now - signedMs > this.#maxAgeMs) {
      throw new EventAuthError('SIGNATURE_EXPIRED', `Event signed at ${signedAt} is older than ${this.#maxAgeMs}ms`);
    }

    return { signed: true, keyId, streamId, issuer, audience };
  }
}

/** Cached per service: building a keyring derives HMACs. */
const authenticators = new Map<string, EventAuthenticator>();

/** The authenticator of a service, from the environment (keys, mode, skew). */
export function eventAuthFor(service: ServiceId, options?: EventAuthenticatorOptions): EventAuthenticator {
  if (options) return new EventAuthenticator(service, options);
  const cached = authenticators.get(service);
  if (cached) return cached;
  const created = new EventAuthenticator(service);
  authenticators.set(service, created);
  return created;
}

/** Forget cached authenticators (tests that change the environment). */
export function resetEventAuthCache(): void {
  authenticators.clear();
}

// ─── wiring helpers ──────────────────────────────────────────────────────────

/** `OutboxRelayOptions.signHeaders` for a service's authenticator. */
export function outboxSigner(auth: EventAuthenticator): (row: { eventId: string; eventType: string }, body: Buffer) => Record<string, string> {
  return (row, body) => auth.signHeaders({ eventType: row.eventType, eventId: row.eventId, body });
}

/** `ReliableConsumerOptions.verify` for a service's authenticator. */
export function deliveryVerifier(
  auth: EventAuthenticator
): (delivery: { headers: Record<string, unknown>; content: Buffer; messageId?: string; eventType?: string }) => void {
  return (delivery) => {
    auth.verifyDelivery(delivery);
  };
}

/**
 * AMQP publish properties for one event, signed by `auth` (the path producers that do not go
 * through the shared outbox relay use: the HyperPay publisher, the ledger's invariant bus, and
 * tests that put events on the broker by hand).
 */
export function signedPublishProperties(
  auth: EventAuthenticator,
  input: { eventType: string; eventId: string; body: Buffer; headers?: Record<string, unknown> }
): { messageId: string; type: string; contentType: string; persistent: true; headers: Record<string, unknown> } {
  return {
    persistent: true,
    contentType: 'application/json',
    messageId: input.eventId,
    type: input.eventType,
    headers: { ...(input.headers ?? {}), ...auth.signHeaders(input) },
  };
}
