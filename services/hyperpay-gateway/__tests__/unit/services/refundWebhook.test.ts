import { describe, it, expect, beforeEach, afterAll } from 'vitest';
/**
 * Ingestion of signed HyperPay REFUND (RF) notifications against the real JSON DB:
 *   - a refund of a captured order payment is recorded with a REFUND_PROCESSED outbox row
 *   - the refundable amount is enforced (captured − successful / pending refunds), per payment
 *   - refunds of unknown / failed / foreign-currency payments are refused, nothing recorded
 *   - duplicates, PENDING → final progressions and contradicting outcomes behave like payments
 */
import { db } from '../../../src/services/jsonDb';
import { outbox, OUTBOX_COLLECTION } from '../../../src/services/outbox';
import { PAYMENTS_COLLECTION, REFUNDS_COLLECTION, VAULT_COLLECTION } from '../../../src/services/collections';
import { ingestPaymentWebhook } from '../../../src/services/paymentWebhook';
import { RefundRefusedError, ingestRefundWebhook, isRefundNotification, refundsOfPayment } from '../../../src/services/refundWebhook';
import { toWireEvent } from '../../../src/messaging/publisher';
import { cleanupTestDb } from '../../helpers/testDb';
import type { RefundProcessedEvent } from '../../../src/types/events';
import { HyperPayRefundProcessedSchema } from '../../../../../shared/contracts';

const PAYMENT_ID = '8ac7a4a18f0c4e8b018f0c9b2c3d4e5f';

function payment(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    id: PAYMENT_ID,
    resourcePath: '/v1/checkouts/C-1/payment',
    paymentType: 'DB',
    paymentBrand: 'MADA',
    amount: '207.00',
    currency: 'SAR',
    merchantTransactionId: 'ord-1001',
    result: { code: '000.100.110', description: 'ok' },
    customer: { givenName: 'Noura', surname: 'Al-Qahtani', email: 'noura@example.sa' },
    customParameters: { merchantId: 'txp_riyadh_retail', creditOrgId: 'org_acme' },
    cart: { items: [{ merchantItemId: 'RTR', name: 'Router', nameAr: 'موجه', quantity: '2', price: '100.00' }] },
    ...overrides,
  };
}

function refund(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    id: 'rf_1',
    resourcePath: `/v1/payments/${PAYMENT_ID}/refunds/rf_1`,
    paymentType: 'RF',
    referencedId: PAYMENT_ID,
    amount: '50.00',
    currency: 'SAR',
    result: { code: '000.100.110', description: 'ok' },
    reason: 'damaged item',
    ...overrides,
  };
}

const events = async (): Promise<RefundProcessedEvent[]> =>
  (await outbox.list()).filter((r) => r.eventType === 'REFUND_PROCESSED').map((r) => toWireEvent(r) as RefundProcessedEvent);

describe('ingestRefundWebhook (RF notifications)', () => {
  beforeEach(async () => {
    for (const c of [PAYMENTS_COLLECTION, REFUNDS_COLLECTION, OUTBOX_COLLECTION, VAULT_COLLECTION]) await db.clear(c);
    await ingestPaymentWebhook(payment());
  });

  afterAll(() => cleanupTestDb());

  it('tells refunds apart from payments', () => {
    expect(isRefundNotification(refund())).toBe(true);
    expect(isRefundNotification(payment())).toBe(false);
  });

  it('records a refund of a captured payment with a REFUND_PROCESSED event the platform can validate', async () => {
    const result = await ingestRefundWebhook(refund());
    expect(result).toMatchObject({
      outcome: 'RECORDED',
      refundId: 'rf_1',
      paymentId: PAYMENT_ID,
      orderId: 'ord-1001',
      status: 'SUCCESS',
      amountHalalas: 5000,
      refundedTotalHalalas: 5000,
      refundableHalalas: 15700,
    });
    const [event] = await events();
    expect(event.eventId).toBe(result.eventId);
    expect(event).toMatchObject({
      eventType: 'REFUND_PROCESSED',
      transactionId: 'rf_1',
      referencedPaymentId: PAYMENT_ID,
      amount: '50.00',
      referencedAmount: '207.00',
      refundedTotal: '50.00',
      orderId: 'ord-1001',
      merchantId: 'txp_riyadh_retail',
      reason: 'damaged item',
      status: 'SUCCESS',
    });
    expect(event.order?.lines[0]).toMatchObject({ sku: 'RTR', quantity: 2, unitPriceHalalas: 10000 });
    expect(HyperPayRefundProcessedSchema.safeParse(event).success).toBe(true);
    expect((await outbox.list())[1]).toMatchObject({ exchange: 'hyperpay', routingKey: 'refund.processed', aggregateType: 'refund' });
  });

  it('never lets the refunds of a payment exceed what was captured (successful and pending ones count)', async () => {
    await ingestRefundWebhook(refund());
    await ingestRefundWebhook(refund({ id: 'rf_pending', resourcePath: '/x/rf_pending', amount: '100.00', result: { code: '000.200.000', description: 'pending' } }));
    const err = await ingestRefundWebhook(refund({ id: 'rf_3', resourcePath: '/x/rf_3', amount: '57.01' })).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(RefundRefusedError);
    expect((err as RefundRefusedError).code).toBe('REFUND_EXCEEDS_REFUNDABLE');
    expect((err as RefundRefusedError).statusCode).toBe(422);
    // Exactly the remainder is fine; a failed refund does not consume the budget.
    await ingestRefundWebhook(refund({ id: 'rf_failed', resourcePath: '/x/rf_failed', amount: '57.00', result: { code: '800.100.151', description: 'declined' } }));
    const last = await ingestRefundWebhook(refund({ id: 'rf_4', resourcePath: '/x/rf_4', amount: '57.00' }));
    expect(last).toMatchObject({ outcome: 'RECORDED', refundedTotalHalalas: 20700, refundableHalalas: 0 });
    expect((await refundsOfPayment(PAYMENT_ID)).map((r) => r.refundId).sort()).toEqual(['rf_1', 'rf_4', 'rf_failed', 'rf_pending']);
    expect((await events()).filter((e) => e.status === 'FAILED')).toHaveLength(1); // announced (consumers ignore it)
  });

  it('refuses refunds of unknown, uncaptured or foreign-currency payments without recording anything', async () => {
    await expect(ingestRefundWebhook(refund({ referencedId: 'nope' }))).rejects.toMatchObject({ code: 'ORIGINAL_PAYMENT_UNKNOWN', statusCode: 409 });
    await ingestPaymentWebhook(payment({ id: 'pay_failed', resourcePath: '/v1/checkouts/C-2/payment', merchantTransactionId: 'ord-1002', result: { code: '800.100.151', description: 'declined' } }));
    await expect(ingestRefundWebhook(refund({ referencedId: 'pay_failed' }))).rejects.toMatchObject({ code: 'ORIGINAL_PAYMENT_NOT_CAPTURED', statusCode: 422 });
    await expect(ingestRefundWebhook(refund({ currency: 'USD' }))).rejects.toMatchObject({ code: 'CURRENCY_MISMATCH' });
    await expect(ingestRefundWebhook(refund({ amount: '0.00' }))).rejects.toMatchObject({ code: 'INVALID_REFUND_AMOUNT' });
    await expect(ingestRefundWebhook({ ...refund(), referencedId: undefined })).rejects.toMatchObject({ code: 'INVALID_REFUND_NOTIFICATION', statusCode: 400 });
    expect(await refundsOfPayment(PAYMENT_ID)).toEqual([]);
    expect(await events()).toEqual([]);
  });

  it('is idempotent: a retry is a DUPLICATE, PENDING → SUCCESS progresses once, a contradiction is a CONFLICT', async () => {
    const pending = await ingestRefundWebhook(refund({ result: { code: '000.200.000', description: 'pending' } }));
    expect(pending).toMatchObject({ outcome: 'RECORDED', status: 'PENDING' });
    expect(await ingestRefundWebhook(refund({ result: { code: '000.200.000', description: 'pending' } }))).toMatchObject({ outcome: 'DUPLICATE', eventId: pending.eventId });
    const done = await ingestRefundWebhook(refund());
    expect(done).toMatchObject({ outcome: 'STATUS_UPDATED', status: 'SUCCESS' });
    expect(done.eventId).not.toBe(pending.eventId);
    expect(await ingestRefundWebhook(refund())).toMatchObject({ outcome: 'DUPLICATE', eventId: done.eventId });
    const conflict = await ingestRefundWebhook(refund({ result: { code: '800.100.151', description: 'declined' } }));
    expect(conflict).toMatchObject({ outcome: 'CONFLICT', status: 'SUCCESS' });
    expect(await events()).toHaveLength(2);
    const [record] = await refundsOfPayment(PAYMENT_ID);
    expect(record.statusConflicts).toHaveLength(1);
  });
});
