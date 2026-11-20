import { describe, it, expect, beforeEach, afterAll, vi } from 'vitest';
/**
 * tests/unit/services/paymentWebhook.test.ts
 *
 * Ingestion of signed HyperPay payment notifications against the real JSON DB:
 *   - result codes map to SUCCESS / PENDING / FAILED
 *   - payment record + PAYMENT_COMPLETED outbox row commit together (or not at all)
 *   - the order context (cart → halalas lines, merchant, credit org) travels with the event
 *   - card data / registration tokens never reach the payments collection or the bus in clear
 *   - duplicates are acknowledged without a new event; PENDING → final progresses;
 *     contradicting final outcomes are recorded as conflicts, never announced
 */

import { db } from '../../../src/services/jsonDb';
import type { DbRecord } from '../../../src/services/jsonDb';
import { outbox, OUTBOX_COLLECTION } from '../../../src/services/outbox';
import { PAYMENTS_COLLECTION, VAULT_COLLECTION } from '../../../src/services/collections';
import { statusFromResultCode } from '../../../src/services/resultCodes';
import { ingestPaymentWebhook, WebhookPayloadError } from '../../../src/services/paymentWebhook';
import { createVault, vault } from '../../../src/services/vault';
import { createKeyring } from '../../../src/security/vaultCrypto';
import { toWireEvent } from '../../../src/messaging/publisher';
import { cleanupTestDb, readTestDb } from '../../helpers/testDb';
import type { PaymentCompletedEvent } from '../../../src/types/events';

function notification(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    id: '8ac7a4a18f0c4e8b018f0c9b2c3d4e5f',
    resourcePath: '/v1/checkouts/C-1/payment',
    paymentType: 'DB',
    paymentBrand: 'MADA',
    amount: '207.00',
    currency: 'SAR',
    merchantTransactionId: 'ord-1001',
    result: { code: '000.100.110', description: 'Request successfully processed' },
    timestamp: '2026-11-15 10:00:00+0000',
    customer: { givenName: 'Noura', surname: 'Al-Qahtani', email: 'noura@example.sa', mobile: '+966500000000' },
    customParameters: { merchantId: 'txp_riyadh_retail', creditOrgId: 'org_acme' },
    cart: {
      items: [{ merchantItemId: 'RTR', name: 'Router', nameAr: 'موجه', quantity: '2', price: '100.00' }],
    },
    ...overrides,
  };
}

async function clearAll(): Promise<void> {
  for (const c of [PAYMENTS_COLLECTION, OUTBOX_COLLECTION, VAULT_COLLECTION]) await db.clear(c);
}

const events = async (): Promise<PaymentCompletedEvent[]> =>
  (await outbox.list()).filter((r) => r.eventType === 'PAYMENT_COMPLETED').map((r) => toWireEvent(r) as PaymentCompletedEvent);

describe('OPPWA result codes', () => {
  it.each([
    ['000.000.000', 'SUCCESS'],
    ['000.100.110', 'SUCCESS'],
    ['000.300.000', 'SUCCESS'],
    ['000.600.000', 'SUCCESS'],
    ['000.400.000', 'SUCCESS'],
    ['000.400.100', 'SUCCESS'],
    ['000.200.000', 'PENDING'],
    ['800.400.500', 'PENDING'],
    ['100.400.500', 'PENDING'],
    ['000.400.030', 'FAILED'],
    ['800.100.151', 'FAILED'],
    ['100.396.101', 'FAILED'],
    ['900.100.100', 'FAILED'],
  ])('%s → %s', (code, status) => {
    expect(statusFromResultCode(code)).toBe(status);
  });
});

describe('ingestPaymentWebhook', () => {
  beforeEach(async () => {
    await clearAll();
  });

  afterAll(() => {
    cleanupTestDb();
  });

  it('records the payment and its PAYMENT_COMPLETED event in one commit, carrying the order context', async () => {
    const result = await ingestPaymentWebhook(notification());
    expect(result).toMatchObject({ outcome: 'RECORDED', status: 'SUCCESS', orderId: 'ord-1001', paymentId: '8ac7a4a18f0c4e8b018f0c9b2c3d4e5f' });

    const [payment] = await db.findAll<DbRecord & { _id: string; status: string; orderId: string; lastEventId: string; amount: string }>(PAYMENTS_COLLECTION);
    expect(payment).toMatchObject({ status: 'SUCCESS', orderId: 'ord-1001', lastEventId: result.eventId, amount: '207.00' });

    const [row] = await outbox.list();
    expect(row).toMatchObject({
      _id: result.eventId,
      status: 'PENDING',
      exchange: 'hyperpay',
      routingKey: 'payment.result',
      aggregateId: payment._id,
      correlationId: 'ord-1001',
    });
    const [wire] = await events();
    expect(wire).toMatchObject({
      eventId: result.eventId,
      eventType: 'PAYMENT_COMPLETED',
      source: 'webhook',
      origin: 'checkout',
      status: 'SUCCESS',
      amount: '207.00',
      currency: 'SAR',
      paymentBrand: 'MADA',
      order: {
        orderId: 'ord-1001',
        merchantId: 'txp_riyadh_retail',
        creditOrgId: 'org_acme',
        customer: { name: 'Noura Al-Qahtani', email: 'noura@example.sa', phone: '+966500000000' },
        lines: [{ sku: 'RTR', nameEn: 'Router', nameAr: 'موجه', quantity: 2, unitPriceHalalas: 10000 }],
      },
    });
  });

  const tokenised = {
    registrationId: '8ac7a49f-secret-registration-token',
    card: { bin: '440647', last4Digits: '0366', holder: 'Noura Q', expiryMonth: '05', expiryYear: '2030', type: 'DEBIT' },
  };

  it('discards a registration token when the vault is disabled (never stored in clear)', async () => {
    expect(vault.enabled).toBe(false);
    await ingestPaymentWebhook(notification(tokenised));
    const [payment] = await db.findAll<DbRecord & { vaultId?: string; raw: Record<string, unknown> }>(PAYMENTS_COLLECTION);
    expect(payment.vaultId).toBeUndefined();
    expect(await db.findAll(VAULT_COLLECTION)).toEqual([]);
    const disk = JSON.stringify(readTestDb());
    expect(disk).not.toContain('secret-registration-token');
    expect(disk).not.toContain('Noura Q');
  });

  it('seals a registration token in the vault (same commit) and keeps card data out of the record and the event', async () => {
    const keyed = createVault(createKeyring({ activeKeyId: 'test', activeKey: Buffer.alloc(32, 7).toString('base64') }));
    vi.spyOn(vault, 'store').mockImplementation((method, context, tx) => keyed.store(method, context, tx));
    const result = await ingestPaymentWebhook(
      notification(tokenised)
    );
    vi.restoreAllMocks();
    const [payment] = await db.findAll<DbRecord & { vaultId?: string; raw: Record<string, unknown> }>(PAYMENTS_COLLECTION);
    expect(payment.vaultId).toMatch(/.+/);
    expect(payment.raw).not.toHaveProperty('card');
    expect(payment.raw).not.toHaveProperty('registrationId');
    expect(await db.findAll(VAULT_COLLECTION)).toHaveLength(1);

    const disk = JSON.stringify(readTestDb());
    expect(disk).not.toContain('secret-registration-token');
    expect(disk).not.toContain('Noura Q');
    const [wire] = await events();
    expect(wire).toMatchObject({ eventId: result.eventId, vaultId: payment.vaultId });
    expect(JSON.stringify(wire)).not.toContain('0366');
  });

  it('acknowledges a redelivery (same payment, same status) without a new event', async () => {
    const first = await ingestPaymentWebhook(notification());
    const again = await ingestPaymentWebhook(notification());
    expect(again).toMatchObject({ outcome: 'DUPLICATE', eventId: first.eventId, dbRecordId: first.dbRecordId });
    expect(await events()).toHaveLength(1);
    expect(await db.findAll(PAYMENTS_COLLECTION)).toHaveLength(1);
  });

  it('records exactly one payment and one event for a burst of concurrent deliveries', async () => {
    const results = await Promise.all(Array.from({ length: 10 }, () => ingestPaymentWebhook(notification())));
    expect(results.filter((r) => r.outcome === 'RECORDED')).toHaveLength(1);
    expect(results.filter((r) => r.outcome === 'DUPLICATE')).toHaveLength(9);
    expect(await events()).toHaveLength(1);
  });

  it('progresses PENDING → SUCCESS with a new event', async () => {
    const pending = await ingestPaymentWebhook(notification({ result: { code: '000.200.000', description: 'pending' } }));
    expect(pending.status).toBe('PENDING');
    const done = await ingestPaymentWebhook(notification());
    expect(done).toMatchObject({ outcome: 'STATUS_UPDATED', status: 'SUCCESS', dbRecordId: pending.dbRecordId });
    expect((await events()).map((e) => e.status)).toEqual(['PENDING', 'SUCCESS']);
  });

  it('never overwrites a final outcome with a contradicting one: conflict recorded, no event', async () => {
    const ok = await ingestPaymentWebhook(notification());
    const contradiction = await ingestPaymentWebhook(notification({ result: { code: '800.100.151', description: 'declined' } }));
    expect(contradiction).toMatchObject({ outcome: 'CONFLICT', status: 'SUCCESS', eventId: ok.eventId });
    const [payment] = await db.findAll<DbRecord & { status: string; statusConflicts: unknown[] }>(PAYMENTS_COLLECTION);
    expect(payment.status).toBe('SUCCESS');
    expect(payment.statusConflicts).toMatchObject([{ reportedStatus: 'FAILED', resultCode: '800.100.151' }]);
    expect(await events()).toHaveLength(1);
  });

  it('rolls the payment back when its event cannot be recorded (no payment without an event)', async () => {
    const spy = vi.spyOn(outbox, 'enqueue').mockRejectedValueOnce(new Error('disk full'));
    await expect(ingestPaymentWebhook(notification())).rejects.toThrow('disk full');
    spy.mockRestore();
    expect(await db.findAll(PAYMENTS_COLLECTION)).toEqual([]);
    expect(await outbox.list()).toEqual([]);
  });

  it('announces payments without a cart / order id without commerce context', async () => {
    const result = await ingestPaymentWebhook(notification({ cart: undefined, merchantTransactionId: undefined }));
    const [wire] = await events();
    expect(wire.order).toBeUndefined();
    expect(wire.correlationId).toBe(`webhook:${result.dbRecordId}`);
  });

  it('falls back to the default merchant and a generic customer name', async () => {
    await ingestPaymentWebhook(notification({ customParameters: {}, customer: undefined }));
    const [wire] = await events();
    expect(wire.order).toMatchObject({ merchantId: 'txp_riyadh_retail', creditOrgId: null, customer: { name: 'Walk-in Customer' } });
  });

  it.each([
    [{ amount: '12.345' }, /amount: invalid amount/],
    [{ result: undefined }, /result: Required/],
    [{ id: '' }, /id: String must contain at least 1/],
    [{ cart: { items: [{ name: 'x', quantity: '0', price: '1.00' }] } }, /invalid quantity/],
    [{ currency: 'SARX' }, /currency/],
  ])('rejects an invalid notification %#', async (overrides, message) => {
    const err = await ingestPaymentWebhook(notification(overrides)).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(WebhookPayloadError);
    expect((err as WebhookPayloadError).details.join('; ')).toMatch(message);
    expect(await db.findAll(PAYMENTS_COLLECTION)).toEqual([]);
  });
});
