import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import type { FastifyInstance } from 'fastify';
import { buildApp } from '../src/server';
import { query, resetAndSeedDatabase } from '../src/db';
import { closeRedis, flushRedis } from '../src/redis';
import { streamHoldService } from '../src/services/StreamHoldService';
import { orderCreditSettlementService } from '../src/integration/OrderCreditSettlementService';
import { BILLING_REFUND_CONSUMER, orderCreditRefundService } from '../src/integration/OrderCreditRefundService';
import { expectWalletInvariants } from './helpers/wallet';
import { HyperPayPaymentCompleted, HyperPayRefundProcessed, computeOrderTotals, halalasToSar, type OrderLine } from '../../../shared/contracts';
import { NonRetryableError, readCheckpoint } from '../../../shared/messaging';

const LINES: OrderLine[] = [{ sku: 'RTR', nameEn: 'Router', nameAr: 'موجه', quantity: 2, unitPriceHalalas: 10000 }];
const exec = { query: (t: string, p?: unknown[]) => query(t, p) };

let seq = 0;
function paymentEvent(orderId: string, credits: number): HyperPayPaymentCompleted {
  const n = ++seq;
  return {
    eventId: `evt_pay_${n}`,
    eventType: 'PAYMENT_COMPLETED',
    occurredAt: new Date().toISOString(),
    correlationId: orderId,
    source: 'webhook',
    origin: 'checkout',
    transactionId: `pay_${orderId}`,
    status: 'SUCCESS',
    amount: halalasToSar(computeOrderTotals(LINES, credits * 100).totalHalalas),
    currency: 'SAR',
    resultCode: '000.100.110',
    dbRecordId: `rec_${n}`,
    paymentBrand: 'VISA',
    order: { orderId, merchantId: 'txp_riyadh_retail', creditOrgId: 'org_acme', customer: { name: 'Faisal' }, lines: LINES },
  };
}

function refundEvent(
  orderId: string,
  amountHalalas: number,
  refundedTotalHalalas: number,
  overrides: Partial<{ refundId: string; eventId: string; status: 'SUCCESS' | 'PENDING' | 'FAILED'; capturedHalalas: number; paymentId: string }> = {}
): HyperPayRefundProcessed {
  const n = ++seq;
  const refundId = overrides.refundId ?? `rf_${n}`;
  return {
    eventId: overrides.eventId ?? `evt_rf_${n}`,
    eventType: 'REFUND_PROCESSED',
    occurredAt: new Date().toISOString(),
    correlationId: orderId,
    source: 'webhook',
    transactionId: refundId,
    referencedPaymentId: overrides.paymentId ?? `pay_${orderId}`,
    amount: halalasToSar(amountHalalas),
    referencedAmount: halalasToSar(overrides.capturedHalalas ?? 20700),
    refundedTotal: halalasToSar(refundedTotalHalalas),
    currency: 'SAR',
    status: overrides.status ?? 'SUCCESS',
    resultCode: '000.100.110',
    dbRecordId: `rec_${n}`,
    orderId,
    merchantId: 'txp_riyadh_retail',
    reason: 'damaged item',
  };
}

async function spendable(orgId: string): Promise<number> {
  const res = await query<{ s: string }>(`SELECT (SUM(available_credits) - SUM(reserved_credits))::text AS s FROM org_wallet_stripes WHERE org_id = $1`, [orgId]);
  return Number(res.rows[0].s);
}

/** 2 × 100.00 SAR with 20 credits (20.00 SAR allowance): 180.00 + 27.00 VAT = 207.00 captured. */
async function captureOrder(orderId = `ord_rf_${++seq}`, credits = 20): Promise<string> {
  await streamHoldService.createOrderCreditHold({ orgId: 'org_acme', orderId, credits });
  const result = await orderCreditSettlementService.handlePaymentCompleted(paymentEvent(orderId, credits));
  expect(result.outcome).toBe('CAPTURED');
  return orderId;
}

describe('Customer refunds: promotional credits restored pro rata (exactly once)', () => {
  let app: FastifyInstance;

  beforeAll(async () => {
    app = buildApp();
    await app.ready();
  });

  beforeEach(async () => {
    await resetAndSeedDatabase();
    await flushRedis();
  });

  afterAll(async () => {
    await app.close();
    await closeRedis();
  });

  it('restores ⌊captured × refunded / paid⌋ credits as a PROMOTIONAL grant, and announces it', async () => {
    const orderId = await captureOrder();
    expect(await spendable('org_acme')).toBe(1480);

    // 50.00 of 207.00 refunded → ⌊20 × 5000 / 20700⌋ = 4 credits back
    const result = await orderCreditRefundService.handleRefundProcessed(refundEvent(orderId, 5000, 5000, { refundId: 'rf_a' }));
    expect(result).toMatchObject({ outcome: 'RESTORED', restoredCredits: 4, cumulativeRestoredCredits: 4 });
    expect(await spendable('org_acme')).toBe(1484);
    const grant = await query<{ source: string; initial_credits: string; priority: number; invoice_id: string }>(
      `SELECT source, initial_credits, priority, invoice_id FROM credit_grants WHERE grant_id = $1`,
      [result.grantId]
    );
    expect(grant.rows[0]).toMatchObject({ source: 'PROMOTIONAL', priority: 1, invoice_id: 'refund:rf_a' });
    expect(Number(grant.rows[0].initial_credits)).toBe(4);
    await expectWalletInvariants('org_acme');

    const outbox = await query<{ event_id: string; routing_key: string; body: any }>(`SELECT event_id, routing_key, body FROM integration_outbox WHERE event_type = 'billing.order-credits.restored'`);
    expect(outbox.rows).toHaveLength(1);
    expect(outbox.rows[0].routing_key).toBe('billing.order-credits.restored');
    expect(outbox.rows[0].body.payload).toMatchObject({
      refund: { refundId: 'rf_a', orderId, amountHalalas: 5000, capturedHalalas: 20700, refundedTotalHalalas: 5000, reason: 'damaged item' },
      credits: { outcome: 'RESTORED', orgId: 'org_acme', capturedCredits: 20, restoredCredits: 4, cumulativeRestoredCredits: 4 },
    });
  });

  it('a final refund restores every remaining credit; partial refunds never restore more than captured', async () => {
    const orderId = await captureOrder();
    await orderCreditRefundService.handleRefundProcessed(refundEvent(orderId, 5000, 5000));
    await orderCreditRefundService.handleRefundProcessed(refundEvent(orderId, 100, 5100)); // ⌊20 × 5100/20700⌋ = 4 → nothing more
    const third = await orderCreditRefundService.handleRefundProcessed(refundEvent(orderId, 15600, 20700));
    expect(third).toMatchObject({ outcome: 'RESTORED', restoredCredits: 16, cumulativeRestoredCredits: 20 });
    expect(await spendable('org_acme')).toBe(1500);
    const trace = await orderCreditRefundService.getOrderRefunds(orderId);
    expect(trace.refunds.map((r) => [r.restoredCredits, r.cumulativeRestoredCredits])).toEqual([
      [4, 4],
      [0, 4],
      [16, 20],
    ]);
    expect(trace.refunds[1].grantId).toBeNull();
    await expectWalletInvariants('org_acme');
  });

  it('is idempotent: redeliveries and re-announcements restore nothing twice', async () => {
    const orderId = await captureOrder();
    const event = refundEvent(orderId, 10000, 10000, { refundId: 'rf_dup' });
    const first = await orderCreditRefundService.handleRefundProcessed(event);
    expect(first.restoredCredits).toBe(9);
    expect((await orderCreditRefundService.handleRefundProcessed(event)).outcome).toBe('DUPLICATE_IGNORED');
    const again = await orderCreditRefundService.handleRefundProcessed({ ...event, eventId: 'evt_rf_again' });
    expect(again).toMatchObject({ outcome: 'DUPLICATE_IGNORED', restoredEventId: first.restoredEventId });
    expect(await spendable('org_acme')).toBe(1489);
    expect(Number((await query(`SELECT COUNT(*)::int AS n FROM integration_outbox WHERE event_type = 'billing.order-credits.restored'`)).rows[0].n)).toBe(1);
    expect((await readCheckpoint(exec, { consumer: BILLING_REFUND_CONSUMER, eventId: 'evt_rf_again', step: 'done' }))?.outcome).toBe('DUPLICATE_IGNORED');
  });

  it('ignores PENDING / FAILED refunds and refuses inconsistent or unknown ones (dead letter)', async () => {
    const orderId = await captureOrder();
    expect((await orderCreditRefundService.handleRefundProcessed(refundEvent(orderId, 5000, 5000, { status: 'PENDING' }))).outcome).toBe('IGNORED_NOT_SUCCESSFUL');
    expect((await orderCreditRefundService.handleRefundProcessed(refundEvent(orderId, 5000, 5000, { status: 'FAILED' }))).outcome).toBe('IGNORED_NOT_SUCCESSFUL');
    expect(await spendable('org_acme')).toBe(1480);

    await expect(orderCreditRefundService.handleRefundProcessed(refundEvent(orderId, 5000, 30000))).rejects.toMatchObject({ reason: 'REFUND_AMOUNT_INVALID' });
    await expect(orderCreditRefundService.handleRefundProcessed(refundEvent('ord_unknown', 5000, 5000))).rejects.toMatchObject({ reason: 'ORDER_NOT_CAPTURED' });
    const wrongPayment = orderCreditRefundService.handleRefundProcessed(refundEvent(orderId, 5000, 5000, { paymentId: 'pay_other' }));
    await expect(wrongPayment).rejects.toBeInstanceOf(NonRetryableError);
    await expect(orderCreditRefundService.handleRefundProcessed(refundEvent(orderId, 5000, 5000, { paymentId: 'pay_other' }))).rejects.toMatchObject({
      reason: 'REFUND_PAYMENT_MISMATCH',
    });
  });

  it('an order without credits is acknowledged with NONE (the ledger still needs the event)', async () => {
    const orderId = `ord_nocredits_${++seq}`;
    const event = paymentEvent(orderId, 0);
    event.order!.creditOrgId = null;
    await orderCreditSettlementService.handlePaymentCompleted(event);
    const result = await orderCreditRefundService.handleRefundProcessed(refundEvent(orderId, 1000, 1000, { capturedHalalas: 23000 }));
    expect(result).toMatchObject({ outcome: 'NONE', restoredCredits: 0 });
    expect(Number((await query(`SELECT COUNT(*)::int AS n FROM integration_outbox WHERE event_type = 'billing.order-credits.restored'`)).rows[0].n)).toBe(1);
  });

  it('exposes the refunds of an order over HTTP', async () => {
    const orderId = await captureOrder();
    await orderCreditRefundService.handleRefundProcessed(refundEvent(orderId, 5000, 5000, { refundId: 'rf_http' }));
    const res = await app.inject({ method: 'GET', url: `/credits/orders/${orderId}/refunds` });
    expect(res.statusCode).toBe(200);
    expect(res.json().refunds).toMatchObject([{ refundId: 'rf_http', restoredCredits: 4 }]);
    expect(res.json().checkpoints.map((c: { consumer: string }) => c.consumer)).toEqual([BILLING_REFUND_CONSUMER]);
  });
});
