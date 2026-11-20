import { describe, expect, it } from 'vitest';
import crypto from 'node:crypto';
import {
  CONSUMER_SERVICE,
  EVENT_STREAMS,
  EventAuthError,
  EventAuthenticator,
  Keyring,
  SERVICES,
  SIGNATURE_HEADERS,
  canonicalString,
  derivedKeyring,
  eventAuthFor,
  keyIdFor,
  keyringFor,
  signedPublishProperties,
  streamForEventType,
  streamsFor,
} from '../security';
import { CONSUMERS, CONSUMER_BINDINGS, EVENT_TYPES } from '../contracts';

const MASTER = 'test-master-secret';

function authFor(service: (typeof SERVICES)[keyof typeof SERVICES], overrides: Partial<ConstructorParameters<typeof EventAuthenticator>[1]> = {}) {
  return new EventAuthenticator(service, { keyring: new Keyring(derivedKeyring(service, MASTER)), mode: 'enforce', ...overrides });
}

/** A signed delivery as it arrives at a consumer. */
function deliver(
  auth: EventAuthenticator,
  input: { eventType: string; eventId: string; body?: unknown; signedAt?: Date }
): { headers: Record<string, unknown>; content: Buffer; messageId: string; eventType: string } {
  const content = Buffer.from(JSON.stringify(input.body ?? { eventId: input.eventId, eventType: input.eventType }));
  return {
    headers: auth.signHeaders({ eventType: input.eventType, eventId: input.eventId, body: content, signedAt: input.signedAt }),
    content,
    messageId: input.eventId,
    eventType: input.eventType,
  };
}

/** The reason an `EventAuthError` carries (what the DLQ records and operators search for). */
function reasonOf(fn: () => unknown): string {
  try {
    fn();
  } catch (err) {
    expect(err).toBeInstanceOf(EventAuthError);
    return (err as EventAuthError).reason;
  }
  throw new Error('expected the delivery to be refused');
}

describe('service-to-service event authentication (authorization matrix + HMAC-SHA256)', () => {
  describe('the authorization matrix', () => {
    it('declares an issuer, an audience and a key for every event type the platform publishes', () => {
      const declared = Object.values(EVENT_TYPES);
      for (const eventType of declared) {
        const stream = streamForEventType(eventType);
        expect(stream, `no stream declares ${eventType}`).toBeDefined();
        expect(stream!.audience.length).toBeGreaterThan(0);
        expect(stream!.issuer).toBeTruthy();
      }
      expect(streamForEventType('ledger.invariant.wallet_drift')?.id).toBe('ledger.invariant');
      expect(streamForEventType('something.else')).toBeUndefined();
    });

    it('gives every consumer queue exactly the keys of the streams it is bound to — and nothing more', () => {
      for (const [queue, bindings] of Object.entries(CONSUMER_BINDINGS)) {
        const service = CONSUMER_SERVICE[queue];
        expect(service, `queue ${queue} has no owning service`).toBeDefined();
        const held = new Set(streamsFor(service).map((s) => s.id));
        for (const binding of bindings) {
          const stream = Object.values(EVENT_STREAMS).find((s) => s.exchange === binding.exchange && s.routingKeys.includes(binding.routingKey));
          expect(stream, `${queue} binds ${binding.exchange}/${binding.routingKey}, which no stream declares`).toBeDefined();
          expect(stream!.audience, `${service} consumes ${stream!.id} but is not in its audience`).toContain(service);
          expect(held.has(stream!.id)).toBe(true);
        }
      }
    });

    it('hands each service only its own keys: the file cache cannot even name HyperPay\'s key', () => {
      const fileCache = keyringFor(SERVICES.fileCache, { EVENT_SIGNING_SECRET: MASTER });
      expect(fileCache.byId(keyIdFor('hyperpay.payment.result'))).toBeUndefined();
      expect(fileCache.byId(keyIdFor('zatca.invoice.reported'))).toBeDefined();
      const billing = keyringFor(SERVICES.billing, { EVENT_SIGNING_SECRET: MASTER });
      expect(billing.byId(keyIdFor('hyperpay.payment.result'))).toBeDefined();
      expect(billing.byId(keyIdFor('ledger.payment.settled'))).toBeUndefined();
    });

    it('derives the same secret for everyone holding a key, and different secrets per key', () => {
      const billing = keyringFor(SERVICES.billing, { EVENT_SIGNING_SECRET: MASTER });
      const hyperpay = keyringFor(SERVICES.hyperpay, { EVENT_SIGNING_SECRET: MASTER });
      const keyId = keyIdFor('hyperpay.payment.result');
      expect(billing.byId(keyId)!.secret.equals(hyperpay.byId(keyId)!.secret)).toBe(true);
      expect(billing.byId(keyId)!.secret.equals(billing.byId(keyIdFor('checkout.payment.completed'))!.secret)).toBe(false);
      // A different master secret (a different environment) yields different keys.
      const other = keyringFor(SERVICES.billing, { EVENT_SIGNING_SECRET: 'another-environment' });
      expect(other.byId(keyId)!.secret.equals(billing.byId(keyId)!.secret)).toBe(false);
    });

    it('loads the keys a service was handed in production (EVENT_SIGNING_KEYS)', () => {
      const keyring = keyringFor(SERVICES.zatca, {
        EVENT_SIGNING_KEYS: JSON.stringify([
          { keyId: 'ledger.payment.settled@v7', streamId: 'ledger.payment.settled', secret: 'x'.repeat(40) },
        ]),
      });
      expect(keyring.keyIds).toEqual(['ledger.payment.settled@v7']);
      expect(() => keyringFor(SERVICES.zatca, { EVENT_SIGNING_KEYS: '[{"keyId":"k","streamId":"nope","secret":"s"}]' })).toThrow(/unknown stream/);
      expect(() => keyringFor(SERVICES.zatca, { EVENT_SIGNING_KEYS: 'not json' })).toThrow(/not valid JSON/);
    });
  });

  describe('signing', () => {
    it('signs with the stream key and states the key id, stream, issuer and audience', () => {
      const ledger = authFor(SERVICES.ledger);
      const headers = ledger.signHeaders({
        eventType: EVENT_TYPES.ledgerPaymentSettled,
        eventId: 'evt_1',
        body: Buffer.from('{"a":1}'),
        signedAt: new Date('2026-11-15T10:00:00.000Z'),
      });
      expect(headers).toMatchObject({
        [SIGNATURE_HEADERS.alg]: 'HMAC-SHA256',
        [SIGNATURE_HEADERS.keyId]: 'ledger.payment.settled@v1',
        [SIGNATURE_HEADERS.stream]: 'ledger.payment.settled',
        [SIGNATURE_HEADERS.issuer]: SERVICES.ledger,
        [SIGNATURE_HEADERS.audience]: 'checkout-coordinator,zatca-tax',
        [SIGNATURE_HEADERS.signedAt]: '2026-11-15T10:00:00.000Z',
      });
      const expected = crypto
        .createHmac('sha256', new Keyring(derivedKeyring(SERVICES.ledger, MASTER)).byId('ledger.payment.settled@v1')!.secret)
        .update(
          canonicalString({
            keyId: 'ledger.payment.settled@v1',
            streamId: 'ledger.payment.settled',
            issuer: SERVICES.ledger,
            audience: [SERVICES.zatca, SERVICES.checkout],
            eventType: EVENT_TYPES.ledgerPaymentSettled,
            eventId: 'evt_1',
            signedAt: '2026-11-15T10:00:00.000Z',
            body: Buffer.from('{"a":1}'),
          }),
          'utf8'
        )
        .digest('base64url');
      expect(headers[SIGNATURE_HEADERS.signature]).toBe(expected);
    });

    it('refuses to sign an event the service is not the issuer of, or has no key for', () => {
      const billing = authFor(SERVICES.billing);
      expect(reasonOf(() => billing.signHeaders({ eventType: EVENT_TYPES.ledgerPaymentSettled, eventId: 'e', body: Buffer.alloc(0) }))).toBe(
        'NOT_THE_ISSUER'
      );
      expect(reasonOf(() => billing.signHeaders({ eventType: 'made.up.event', eventId: 'e', body: Buffer.alloc(0) }))).toBe('UNKNOWN_EVENT_STREAM');
      // …but an undeclared legacy event type may stay unsigned where that is explicitly allowed.
      expect(billing.signHeadersIfIssued({ eventType: 'made.up.event', eventId: 'e', body: Buffer.alloc(0) })).toEqual({});
    });
  });

  describe('verifying', () => {
    it('accepts an event signed by its issuer and addressed to this service', () => {
      const hyperpay = authFor(SERVICES.hyperpay);
      const billing = authFor(SERVICES.billing);
      const result = billing.verifyDelivery(deliver(hyperpay, { eventType: EVENT_TYPES.paymentCompleted, eventId: 'evt_pay' }));
      expect(result).toMatchObject({ signed: true, keyId: 'hyperpay.payment.result@v1', issuer: SERVICES.hyperpay });
    });

    it('refuses a service that does not hold the key, even when the message reaches its queue', () => {
      const hyperpay = authFor(SERVICES.hyperpay);
      const delivery = deliver(hyperpay, { eventType: EVENT_TYPES.paymentCompleted, eventId: 'evt_pay' });
      for (const service of [SERVICES.ledger, SERVICES.zatca, SERVICES.fileCache, SERVICES.checkout]) {
        const err = (() => {
          try {
            authFor(service).verifyDelivery(delivery);
            return null;
          } catch (e) {
            return e as EventAuthError;
          }
        })();
        expect(err, `${service} accepted a HyperPay event`).toBeInstanceOf(EventAuthError);
        expect(err!.reason).toBe('UNKNOWN_KEY_ID');
        expect(err!.retryable).toBe(false);
      }
    });

    it('refuses an unsigned event (enforce), accepts it in permissive mode, skips checks when off', () => {
      const billing = authFor(SERVICES.billing);
      const unsigned = { headers: {}, content: Buffer.from('{}'), messageId: 'e', eventType: EVENT_TYPES.paymentCompleted };
      expect(reasonOf(() => billing.verifyDelivery(unsigned))).toBe('UNSIGNED_EVENT');
      expect(authFor(SERVICES.billing, { mode: 'permissive' }).verifyDelivery(unsigned)).toEqual({ signed: false });
      expect(authFor(SERVICES.billing, { mode: 'off' }).verifyDelivery(unsigned)).toEqual({ signed: false });
      // Permissive still verifies what IS signed.
      const forged = deliver(authFor(SERVICES.hyperpay), { eventType: EVENT_TYPES.paymentCompleted, eventId: 'e' });
      forged.content = Buffer.from('{"tampered":true}');
      expect(reasonOf(() => authFor(SERVICES.billing, { mode: 'permissive' }).verifyDelivery(forged))).toBe('SIGNATURE_INVALID');
    });

    it('detects any change to the body, the event id, the event type or the headers', () => {
      const hyperpay = authFor(SERVICES.hyperpay);
      const billing = authFor(SERVICES.billing);
      const original = deliver(hyperpay, { eventType: EVENT_TYPES.paymentCompleted, eventId: 'evt_pay', body: { amount: '100.00' } });

      const tamperedBody = { ...original, content: Buffer.from(JSON.stringify({ amount: '900.00' })) };
      expect(reasonOf(() => billing.verifyDelivery(tamperedBody))).toBe('SIGNATURE_INVALID');
      expect(reasonOf(() => billing.verifyDelivery({ ...original, messageId: 'evt_other' }))).toBe('SIGNATURE_INVALID');
      const widerAudience = { ...original, headers: { ...original.headers, [SIGNATURE_HEADERS.audience]: `${SERVICES.billing},${SERVICES.zatca}` } };
      expect(reasonOf(() => billing.verifyDelivery(widerAudience))).toBe('SIGNATURE_INVALID');
      const otherSignature = { ...original, headers: { ...original.headers, [SIGNATURE_HEADERS.signature]: 'AAAA' } };
      expect(reasonOf(() => billing.verifyDelivery(otherSignature))).toBe('SIGNATURE_INVALID');
      const otherAlg = { ...original, headers: { ...original.headers, [SIGNATURE_HEADERS.alg]: 'MD5' } };
      expect(reasonOf(() => billing.verifyDelivery(otherAlg))).toBe('UNSUPPORTED_ALGORITHM');
    });

    it('refuses an event whose type does not belong to the signed stream, or whose issuer is wrong', () => {
      const ledger = authFor(SERVICES.ledger);
      const zatca = authFor(SERVICES.zatca);
      const settled = deliver(ledger, { eventType: EVENT_TYPES.ledgerPaymentSettled, eventId: 'evt_s' });
      // The key of ledger.payment.settled cannot be used to smuggle another stream's event type.
      expect(reasonOf(() => zatca.verifyDelivery({ ...settled, eventType: EVENT_TYPES.merchantSettlementPosted }))).toBe('EVENT_TYPE_NOT_IN_STREAM');
      const otherIssuer = { ...settled, headers: { ...settled.headers, [SIGNATURE_HEADERS.issuer]: SERVICES.billing } };
      expect(reasonOf(() => zatca.verifyDelivery(otherIssuer))).toBe('ISSUER_MISMATCH');
      const otherStream = { ...settled, headers: { ...settled.headers, [SIGNATURE_HEADERS.stream]: 'ledger.payment.released' } };
      expect(reasonOf(() => zatca.verifyDelivery(otherStream))).toBe('STREAM_MISMATCH');
    });

    it('refuses an event this service is not the audience of, even with a valid signature', () => {
      // The ledger holds the zatca.settlement-documents.reported key; the file cache holds it too.
      const zatca = authFor(SERVICES.zatca);
      const delivery = deliver(zatca, { eventType: EVENT_TYPES.settlementDocumentsReported, eventId: 'evt_docs' });
      const narrowed = {
        ...delivery,
        headers: { ...delivery.headers, [SIGNATURE_HEADERS.audience]: SERVICES.fileCache },
      };
      // The ledger is in the matrix's audience but this copy is addressed to the file cache only.
      expect(reasonOf(() => authFor(SERVICES.ledger).verifyDelivery(narrowed))).toBe('AUDIENCE_DENIED');
      expect(authFor(SERVICES.fileCache).verifyDelivery(delivery).signed).toBe(true);
    });

    it('rejects signatures dated in the future, and (when configured) stale ones', () => {
      const ledger = authFor(SERVICES.ledger);
      const future = deliver(ledger, { eventType: EVENT_TYPES.ledgerPaymentSettled, eventId: 'e', signedAt: new Date(Date.now() + 20 * 60_000) });
      expect(reasonOf(() => authFor(SERVICES.zatca).verifyDelivery(future))).toBe('SIGNATURE_NOT_YET_VALID');

      const old = deliver(ledger, { eventType: EVENT_TYPES.ledgerPaymentSettled, eventId: 'e', signedAt: new Date(Date.now() - 3 * 3600_000) });
      // Off by default, so a DLQ replay days later still verifies…
      expect(authFor(SERVICES.zatca).verifyDelivery(old).signed).toBe(true);
      // …unless a maximum age is configured.
      expect(reasonOf(() => authFor(SERVICES.zatca, { maxAgeMs: 60_000 }).verifyDelivery(old))).toBe('SIGNATURE_EXPIRED');
    });

    it('supports key rotation: verifiers accept every version they hold, issuers sign with the newest', () => {
      const v1 = derivedKeyring(SERVICES.ledger, MASTER, [1]);
      const both = derivedKeyring(SERVICES.ledger, MASTER, [1, 2]);
      const oldIssuer = new EventAuthenticator(SERVICES.ledger, { keyring: new Keyring(v1), mode: 'enforce' });
      const newIssuer = new EventAuthenticator(SERVICES.ledger, { keyring: new Keyring(both), mode: 'enforce' });
      const consumerWithBoth = new EventAuthenticator(SERVICES.zatca, {
        keyring: new Keyring(derivedKeyring(SERVICES.zatca, MASTER, [1, 2])),
        mode: 'enforce',
      });
      const consumerWithV1 = new EventAuthenticator(SERVICES.zatca, {
        keyring: new Keyring(derivedKeyring(SERVICES.zatca, MASTER, [1])),
        mode: 'enforce',
      });

      const signedWithV2 = deliver(newIssuer, { eventType: EVENT_TYPES.ledgerPaymentSettled, eventId: 'evt_new' });
      expect(signedWithV2.headers[SIGNATURE_HEADERS.keyId]).toBe('ledger.payment.settled@v2');
      expect(consumerWithBoth.verifyDelivery(signedWithV2).keyId).toBe('ledger.payment.settled@v2');
      // A consumer that has not received v2 yet refuses it (ship the key first, then flip).
      expect(reasonOf(() => consumerWithV1.verifyDelivery(signedWithV2))).toBe('UNKNOWN_KEY_ID');
      // Events still signed with v1 keep verifying during the rollout.
      expect(consumerWithBoth.verifyDelivery(deliver(oldIssuer, { eventType: EVENT_TYPES.ledgerPaymentSettled, eventId: 'evt_old' })).keyId).toBe(
        'ledger.payment.settled@v1'
      );
    });
  });

  describe('wiring helpers', () => {
    it('builds AMQP publish properties a consumer of that stream accepts', () => {
      const body = Buffer.from(JSON.stringify({ eventId: 'evt_x', eventType: EVENT_TYPES.invoiceReported }));
      const properties = signedPublishProperties(authFor(SERVICES.zatca), {
        eventType: EVENT_TYPES.invoiceReported,
        eventId: 'evt_x',
        body,
        headers: { 'x-aggregate-id': 'inv_1' },
      });
      expect(properties).toMatchObject({ messageId: 'evt_x', type: EVENT_TYPES.invoiceReported, persistent: true });
      expect(properties.headers['x-aggregate-id']).toBe('inv_1');
      const verified = authFor(SERVICES.fileCache).verifyDelivery({
        headers: properties.headers,
        content: body,
        messageId: 'evt_x',
        eventType: EVENT_TYPES.invoiceReported,
      });
      expect(verified.streamId).toBe('zatca.invoice.reported');
    });

    it('caches one authenticator per service and reports what it may publish', () => {
      expect(eventAuthFor(SERVICES.zatca)).toBe(eventAuthFor(SERVICES.zatca));
      const issued = eventAuthFor(SERVICES.ledger)
        .issuedStreams()
        .map((s) => s.stream.id)
        .sort();
      expect(issued).toEqual([
        'ledger.invariant',
        'ledger.merchant-settlement.collected',
        'ledger.merchant-settlement.posted',
        'ledger.payment.refunded',
        'ledger.payment.released',
        'ledger.payment.settled',
      ]);
      expect(eventAuthFor(SERVICES.fileCache).issuedStreams()).toEqual([]);
    });

    it('every integration consumer queue is owned by a service (used to pick the verifying keyring)', () => {
      expect(Object.keys(CONSUMER_SERVICE).sort()).toEqual(Object.values(CONSUMERS).sort());
    });
  });
});
