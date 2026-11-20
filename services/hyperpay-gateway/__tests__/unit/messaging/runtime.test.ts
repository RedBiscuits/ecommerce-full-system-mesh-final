import { describe, it, expect, beforeEach, afterEach, afterAll } from 'vitest';
/**
 * tests/unit/messaging/runtime.test.ts
 *
 * The service's broker side on the in-process RabbitMQ (@onify/fake-amqplib):
 * topology reconciled on connect, committed outbox rows published with
 * publisher confirms to the `hyperpay` exchange, and every queue bound to
 * `payment.result` receiving its own copy (direct-exchange semantics) —
 * including rows committed while the broker was unreachable.
 */
import type { Channel, ChannelModel } from 'amqplib';
import { startHyperPayMessaging, type HyperPayMessaging } from '../../../src/messaging/runtime';
import { ingestPaymentWebhook } from '../../../src/services/paymentWebhook';
import { db } from '../../../src/services/jsonDb';
import type { DbRecord } from '../../../src/services/jsonDb';
import { outbox, OUTBOX_COLLECTION } from '../../../src/services/outbox';
import { PAYMENTS_COLLECTION } from '../../../src/services/collections';
import { resolveAmqpConnect } from '../../../../../shared/messaging';
import { cleanupTestDb } from '../../helpers/testDb';
import { waitFor } from '../../helpers/waitFor';

const connect = resolveAmqpConnect('fake');
let seq = 0;

function notification(id: string): Record<string, unknown> {
  return {
    id,
    amount: '115.00',
    currency: 'SAR',
    merchantTransactionId: `ord-${id}`,
    result: { code: '000.100.110', description: 'ok' },
    cart: { items: [{ merchantItemId: 'A', name: 'Item', quantity: 1, price: '100.00' }] },
  };
}

describe('HyperPay messaging runtime (outbox → hyperpay exchange)', () => {
  let url: string;
  let keepAlive: ChannelModel;
  let inspector: Channel;
  let messaging: HyperPayMessaging | null = null;

  async function drain(queue: string) {
    const out: Array<{ body: any; messageId?: string; type?: string }> = [];
    for (;;) {
      const msg = await inspector.get(queue, { noAck: true });
      if (!msg) return out;
      out.push({ body: JSON.parse(msg.content.toString()), messageId: msg.properties.messageId, type: msg.properties.type });
    }
  }

  beforeEach(async () => {
    url = `amqp://hyperpay-runtime-${process.pid}-${++seq}`;
    keepAlive = await connect(url);
    inspector = await keepAlive.createChannel();
    inspector.on('error', () => undefined);
    await db.clear(PAYMENTS_COLLECTION);
    await db.clear(OUTBOX_COLLECTION);
  });

  afterEach(async () => {
    await messaging?.stop();
    messaging = null;
    await keepAlive.close().catch(() => undefined);
  });

  afterAll(() => cleanupTestDb());

  it('declares the topology and publishes committed events to every queue bound to payment.result', async () => {
    messaging = await startHyperPayMessaging({ url, connect, outbox: { pollIntervalMs: 20 } });
    // A downstream consumer's queue (e.g. billing) bound next to hyperpay.results / hyperpay.events.
    await inspector.assertQueue('downstream.payments', { durable: true });
    await inspector.bindQueue('downstream.payments', 'hyperpay', 'payment.result');

    const recorded = await ingestPaymentWebhook(notification('pay-A'));
    await messaging.flush();
    await waitFor(async () => (await outbox.stats()).published === 1, { label: 'published' });

    for (const queue of ['hyperpay.results', 'hyperpay.events', 'downstream.payments']) {
      const messages = await drain(queue);
      expect(messages).toHaveLength(1);
      expect(messages[0]).toMatchObject({
        messageId: recorded.eventId,
        type: 'PAYMENT_COMPLETED',
        body: { eventId: recorded.eventId, status: 'SUCCESS', order: { orderId: 'ord-pay-A' } },
      });
    }
    const [row] = await db.findAll<DbRecord & { status: string }>(OUTBOX_COLLECTION);
    expect(row.status).toBe('PUBLISHED');
  });

  it('keeps events committed while the broker is unreachable and publishes them after reconnecting', async () => {
    const first = await ingestPaymentWebhook(notification('pay-B'));
    const second = await ingestPaymentWebhook(notification('pay-C'));
    expect((await outbox.stats()).pending).toBe(2);

    messaging = await startHyperPayMessaging({ url, connect, outbox: { pollIntervalMs: 20 } });
    await messaging.flush();
    await waitFor(async () => (await outbox.stats()).pending === 0, { label: 'backlog drained' });
    expect((await drain('hyperpay.events')).map((m) => m.messageId)).toEqual([first.eventId, second.eventId]);
  });
});
