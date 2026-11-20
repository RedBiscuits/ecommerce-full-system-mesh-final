import { afterEach, beforeAll, describe, expect, it } from 'vitest';
import type { Channel, ChannelModel } from 'amqplib';
import { bootPlatform, warmUpPlatform, type PlatformRuntime } from '../scripts/platform';
import { buildCheckout, postWebhook } from './helpers/checkout';
import { publishSigned, publishUnsigned } from './helpers/events';
import { CONSUMERS, EVENT_TYPES, EXCHANGES, ROUTING_KEYS } from '../shared/contracts';
import { ReliableConsumer, resolveAmqpConnect, type DeliverySettled } from '../shared/messaging';
import {
  EVENT_STREAMS,
  EventAuthenticator,
  Keyring,
  SERVICES,
  SIGNATURE_HEADERS,
  derivedKeyring,
  deliveryVerifier,
  eventAuthFor,
  keyIdFor,
  signedPublishProperties,
} from '../shared/security';
import { diagnoseIntegration } from '../scripts/cluster-doctor';

const RETRY = { maxAttempts: 3, baseDelayMs: 20, maxDelayMs: 60, minDelayMs: 5 };
const FAST = { retry: RETRY, ttlPumpIntervalMs: 10, relayPollIntervalMs: 20, log: () => undefined, httpLogs: false };

type SettledLog = Record<string, DeliverySettled[]>;
function recordDeliveries(platform: PlatformRuntime): SettledLog {
  const log: SettledLog = {};
  for (const [name, consumer] of Object.entries(platform.consumers)) {
    log[name] = [];
    consumer.on('settled', (e: DeliverySettled) => log[name].push(e));
  }
  return log;
}

async function brokerClient(platform: PlatformRuntime): Promise<{ conn: ChannelModel; ch: Channel; close: () => Promise<void> }> {
  const conn: ChannelModel = await resolveAmqpConnect('fake')(platform.url);
  const ch = await conn.createConfirmChannel();
  ch.on('error', () => undefined);
  return {
    conn,
    ch,
    close: async () => {
      await (ch as unknown as { waitForConfirms(): Promise<void> }).waitForConfirms().catch(() => undefined);
      await conn.close().catch(() => undefined);
    },
  };
}

describe('service-to-service authentication of the events on the broker', () => {
  beforeAll(() => warmUpPlatform(), 180_000);

  let platform: PlatformRuntime;

  afterEach(async () => {
    await platform?.shutdown();
  });

  it('signs every event a service publishes, addressed to exactly the services allowed to consume it', async () => {
    platform = await bootPlatform(FAST);
    // A tap on every stream of the order flow: what the producers actually put on the wire.
    const { ch, close } = await brokerClient(platform);
    const taps: Array<{ stream: string; queue: string }> = [];
    for (const stream of ['hyperpay.payment.result', 'billing.order-credits.settled', 'ledger.payment.settled', 'zatca.invoice.reported']) {
      const { exchange, routingKeys } = EVENT_STREAMS[stream];
      const queue = `tap.${stream}`;
      await ch.assertQueue(queue, { durable: false, autoDelete: true });
      await ch.bindQueue(queue, exchange, routingKeys[0]);
      taps.push({ stream, queue });
    }

    const checkout = buildCheckout();
    await postWebhook(platform, checkout.body);
    await platform.settle();

    for (const tap of taps) {
      const msg = await ch.get(tap.queue, { noAck: true });
      expect(msg, `nothing was published on ${tap.stream}`).toBeTruthy();
      const headers = (msg as { properties: { headers: Record<string, string> } }).properties.headers;
      const stream = EVENT_STREAMS[tap.stream];
      expect(headers[SIGNATURE_HEADERS.alg]).toBe('HMAC-SHA256');
      expect(headers[SIGNATURE_HEADERS.keyId]).toBe(keyIdFor(stream.id));
      expect(headers[SIGNATURE_HEADERS.stream]).toBe(stream.id);
      expect(headers[SIGNATURE_HEADERS.issuer]).toBe(stream.issuer);
      expect(headers[SIGNATURE_HEADERS.audience].split(',').sort()).toEqual([...stream.audience].sort());
      expect(headers[SIGNATURE_HEADERS.signature]).toMatch(/^[A-Za-z0-9_-]{43}$/);

      // Only the services in the audience can verify it; nobody else can.
      const delivery = {
        headers,
        content: (msg as { content: Buffer }).content,
        messageId: (msg as { properties: { messageId?: string } }).properties.messageId,
        eventType: (msg as { properties: { type?: string } }).properties.type,
      };
      for (const service of Object.values(SERVICES)) {
        const auth = eventAuthFor(service);
        if (stream.audience.includes(service)) {
          expect(auth.verifyDelivery(delivery), `${service} should accept ${stream.id}`).toMatchObject({ signed: true, streamId: stream.id });
        } else {
          expect(() => auth.verifyDelivery(delivery), `${service} must not be able to verify ${stream.id}`).toThrow();
        }
      }
    }
    await close();
    expect((await platform.verifyOrderConsistency(checkout.orderId)).ok).toBe(true);
  });

  it('refuses a forged or unsigned event at the consumer, parks it, and keeps the real flow running', async () => {
    platform = await bootPlatform(FAST);
    const log = recordDeliveries(platform);
    const { ch, close } = await brokerClient(platform);

    const genuine = buildCheckout();
    const forgedEvent = {
      eventId: 'evt_forged_payment',
      eventType: EVENT_TYPES.paymentCompleted,
      occurredAt: new Date().toISOString(),
      correlationId: 'ord_forged',
      source: 'webhook',
      origin: 'checkout',
      transactionId: 'pay_forged',
      status: 'SUCCESS',
      amount: '999999.00',
      currency: 'SAR',
      resultCode: '000.100.110',
      dbRecordId: 'rec_forged',
      order: { orderId: 'ord_forged', merchantId: 'txp_riyadh_retail', creditOrgId: null, customer: { name: 'Mallory' }, lines: genuine.body.cart },
    };
    // 1. no signature at all
    publishUnsigned(ch, EXCHANGES.hyperpay.name, ROUTING_KEYS.paymentResult, forgedEvent, { messageId: 'evt_forged_unsigned' });
    // 2. a made-up signature with a real key id
    ch.publish(EXCHANGES.hyperpay.name, ROUTING_KEYS.paymentResult, Buffer.from(JSON.stringify(forgedEvent)), {
      persistent: true,
      messageId: 'evt_forged_signature',
      type: EVENT_TYPES.paymentCompleted,
      headers: {
        [SIGNATURE_HEADERS.alg]: 'HMAC-SHA256',
        [SIGNATURE_HEADERS.keyId]: keyIdFor('hyperpay.payment.result'),
        [SIGNATURE_HEADERS.stream]: 'hyperpay.payment.result',
        [SIGNATURE_HEADERS.issuer]: SERVICES.hyperpay,
        [SIGNATURE_HEADERS.audience]: SERVICES.billing,
        [SIGNATURE_HEADERS.signedAt]: new Date().toISOString(),
        [SIGNATURE_HEADERS.signature]: 'ZmFrZS1zaWduYXR1cmUtZm9yLWEtZm9yZ2VkLWV2ZW50XzAwMDAwMDA',
      },
    });
    // 3. correctly signed — but by a service that holds a different key (another environment's secret)
    const impostor = new EventAuthenticator(SERVICES.hyperpay, {
      keyring: new Keyring(derivedKeyring(SERVICES.hyperpay, 'stolen-but-wrong-master-secret')),
      mode: 'enforce',
    });
    const impostorBody = Buffer.from(JSON.stringify({ ...forgedEvent, eventId: 'evt_impostor' }));
    ch.publish(EXCHANGES.hyperpay.name, ROUTING_KEYS.paymentResult, impostorBody, {
      ...signedPublishProperties(impostor, { eventType: EVENT_TYPES.paymentCompleted, eventId: 'evt_impostor', body: impostorBody }),
    });
    // 4. a genuine event of another stream, replayed onto this exchange: billing does not hold
    //    that stream's key (it is not in its audience), so it cannot accept it either.
    publishSigned(ch, EXCHANGES.hyperpay.name, ROUTING_KEYS.paymentResult, {
      eventId: 'evt_wrong_stream',
      eventType: EVENT_TYPES.invoiceReported,
    });
    await close();

    await postWebhook(platform, genuine.body);
    await platform.settle();

    const parked = await platform.consumers[CONSUMERS.billingPaymentResults].inspectDeadLetters();
    expect(parked.map((m) => m.messageId)).toEqual(['evt_forged_unsigned', 'evt_forged_signature', 'evt_impostor', 'evt_wrong_stream']);
    expect(parked.map((m) => m.reason?.replace(/^UNTRUSTED_EVENT: ([A-Z_]+).*/s, '$1'))).toEqual([
      'UNSIGNED_EVENT',
      'SIGNATURE_INVALID',
      'SIGNATURE_INVALID',
      'UNKNOWN_KEY_ID',
    ]);
    // Not one of them reached the handler: no order, no credits, no ledger entry for ord_forged.
    const trace = await platform.traceOrder('ord_forged');
    expect(trace.billing.settlements).toEqual([]);
    expect(trace.wallet.journal).toEqual([]);
    expect(log[CONSUMERS.billingPaymentResults].filter((e) => e.outcome === 'dead-lettered')).toHaveLength(4);
    // …while the genuine payment went through the whole flow.
    expect((await platform.verifyOrderConsistency(genuine.orderId)).ok).toBe(true);

    const health = await diagnoseIntegration(platform);
    expect(health.eventSecurity.mode).toBe('enforce');
    expect(health.eventSecurity.untrustedParked.map((p) => p.messageId)).toEqual([
      'evt_forged_unsigned',
      'evt_forged_signature',
      'evt_impostor',
      'evt_wrong_stream',
    ]);
    expect(health.problems.some((p) => p.startsWith('event authentication:'))).toBe(true);
    expect(health.eventSecurity.keyIds[SERVICES.fileCache]).not.toContain(keyIdFor('hyperpay.payment.result'));
  });

  it('a service that binds to a stream it has no key for cannot consume it (audience enforced by the key)', async () => {
    platform = await bootPlatform(FAST);
    // The file cache is not in the audience of hyperpay.payment.result, so it does not hold the
    // key. Even wired to that exchange, every delivery is refused.
    const rogue = new ReliableConsumer<unknown>({
      name: 'file-cache.rogue-payment-tap',
      bindings: [{ exchange: EXCHANGES.hyperpay.name, exchangeType: 'direct', routingKey: ROUTING_KEYS.paymentResult }],
      decode: (raw) => raw,
      verify: deliveryVerifier(eventAuthFor(SERVICES.fileCache)),
      handler: async () => 'HANDLED',
      url: platform.url,
      connect: resolveAmqpConnect('fake'),
      retry: RETRY,
      ttlPumpIntervalMs: 10,
    });
    await rogue.start();
    const settled = new Promise<DeliverySettled>((resolve) => rogue.once('settled', resolve));
    try {
      const checkout = buildCheckout();
      await postWebhook(platform, checkout.body);
      await platform.settle();

      const outcome = await settled;
      expect(outcome.outcome).toBe('dead-lettered');
      expect(outcome.reason).toMatch(/^UNTRUSTED_EVENT: UNKNOWN_KEY_ID/);
      expect(rogue.stats().acked).toBe(0);
      // The authorized consumer (billing) processed the very same event normally.
      expect((await platform.verifyOrderConsistency(checkout.orderId)).ok).toBe(true);
    } finally {
      await rogue.close();
    }
  });

  it('permissive mode accepts unsigned events during a rollout, but still rejects bad signatures', async () => {
    platform = await bootPlatform({ ...FAST, eventAuth: { mode: 'permissive' } });
    const { ch, close } = await brokerClient(platform);
    const checkout = buildCheckout();
    // An unsigned copy of a real payment event (an old producer that has not been upgraded yet).
    await postWebhook(platform, checkout.body);
    await platform.settle();
    const log = recordDeliveries(platform);
    const { outbox: hyperpayOutbox } = await import('../services/hyperpay-gateway/src/services/outbox');
    const { toWireEvent } = await import('../services/hyperpay-gateway/src/messaging/publisher');
    const [payment] = (await hyperpayOutbox.list()).filter((r) => r.eventType === 'PAYMENT_COMPLETED');
    publishUnsigned(ch, EXCHANGES.hyperpay.name, ROUTING_KEYS.paymentResult, { ...toWireEvent(payment) }, { messageId: payment._id });
    await close();
    await platform.settle();

    expect(log[CONSUMERS.billingPaymentResults].map((e) => e.outcome)).toEqual(['acked']);
    expect(log[CONSUMERS.billingPaymentResults].map((e) => e.result)).toEqual(['DUPLICATE_IGNORED']);
    expect((await platform.verifyOrderConsistency(checkout.orderId)).ok).toBe(true);
  });

  it('a service whose keys do not match is cut out of the flow (misconfigured rotation)', async () => {
    // ZATCA is configured with a keyring that contains none of its streams' keys.
    const strangerKeys = new Keyring(derivedKeyring(SERVICES.fileCache, 'some-other-environment-secret'));
    platform = await bootPlatform({
      ...FAST,
      eventAuth: { overrides: { [SERVICES.zatca]: new EventAuthenticator(SERVICES.zatca, { keyring: strangerKeys, mode: 'enforce' }) } },
    });
    const checkout = buildCheckout();
    await postWebhook(platform, checkout.body);
    await platform.settle();

    // Billing and the ledger did their part: they hold the right keys.
    const trace = await platform.traceOrder(checkout.orderId);
    expect(trace.billing.settlements).toHaveLength(1);
    expect(trace.wallet.journal).toHaveLength(3);
    // ZATCA cannot verify the ledger's settlement: it is parked, nothing is invoiced or cached.
    const parked = await platform.consumers[CONSUMERS.zatcaLedgerSettlements].inspectDeadLetters();
    expect(parked).toHaveLength(1);
    expect(parked[0].reason).toMatch(/^UNTRUSTED_EVENT: UNKNOWN_KEY_ID/);
    expect(trace.zatca.invoices).toEqual([]);
    expect(trace.fileCache).toEqual([]);
    expect((await platform.verifyOrderConsistency(checkout.orderId)).ok).toBe(false);
  });
});
