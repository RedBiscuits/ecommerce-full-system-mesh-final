import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import type { FastifyInstance } from 'fastify';
import { buildApp } from '../src/server';
import { closeDb, query, resetAndSeedDatabase } from '../src/db';
import { closeRedis, flushRedis } from '../src/redis';
import { paymentSettlementService } from '../src/services/PaymentSettlementService';
import { LEDGER_REFUND_CONSUMER, OrderRefundService, SettlementInFlightError } from '../src/services/OrderRefundService';
import { MerchantSettlementService } from '../src/services/MerchantSettlementService';
import { ledgerAuditService } from '../src/services/LedgerAuditService';
import { walletService } from '../src/services/WalletService';
import { ADMIN, bearer } from './helpers/auth';
import {
  EVENT_TYPES,
  PRODUCERS,
  buildIntegrationEvent,
  checkoutHoldKey,
  computeOrderTotals,
  proRataRefundSplit,
  type OrderCreditsRestoredEvent,
  type OrderCreditsSettledEvent,
  type OrderLine,
  type SettlementFee,
  type SettlementFeeCalculatedEvent,
} from '../../../shared/contracts';
import { NonRetryableError, readCheckpoint } from '../../../shared/messaging';

const MERCHANT = 'txp_riyadh_retail';
const REVENUE = 'wallet_merchant_riyadh_retail';
const PAYOUT = 'wallet_payout_riyadh_retail';
const VAT = 'wallet_vat_payable_sar';
const CLEARING = 'wallet_treasury_sar';
const LINES: OrderLine[] = [{ sku: 'RTR', nameEn: 'Router', nameAr: 'موجه', quantity: 2, unitPriceHalalas: 10000 }];
const MIXED: OrderLine[] = [
  { sku: 'S1', nameEn: 'Standard', nameAr: 'س', quantity: 1, unitPriceHalalas: 10000 },
  { sku: 'Z1', nameEn: 'Zero', nameAr: 'ص', quantity: 1, unitPriceHalalas: 10000, vatCategory: 'Z', vatExemptionCode: 'VATEX-SA-EDU' },
  { sku: 'E1', nameEn: 'Exempt', nameAr: 'م', quantity: 1, unitPriceHalalas: 5000, vatCategory: 'E', vatExemptionCode: 'VATEX-SA-29' },
];
const exec = { query: (t: string, p?: unknown[]) => query(t, p) };

let seq = 0;
async function capture(options: { lines?: OrderLine[]; allowance?: number; tender?: 'CARD' | 'WALLET'; orderId?: string } = {}): Promise<{ orderId: string; paymentId: string; gross: number; net: number; vat: number }> {
  const n = ++seq;
  const lines = options.lines ?? LINES;
  const allowance = options.allowance ?? 2000;
  const orderId = options.orderId ?? `ord_rf_${n}`;
  const paymentId = `pay_${orderId}`;
  const totals = computeOrderTotals(lines, allowance);
  const tender = options.tender ?? 'CARD';
  const checkoutId = tender === 'WALLET' ? `chk_${n}` : undefined;
  if (tender === 'WALLET') {
    await paymentSettlementService.placeTenderHold({
      holdKey: checkoutHoldKey(checkoutId as string, 'remainder'),
      orderId,
      merchantId: MERCHANT,
      tender: 'WALLET',
      sourceWalletId: 'wallet_noura_sar',
      amountHalalas: totals.totalHalalas,
      checkoutId,
    });
  }
  const event = buildIntegrationEvent({
    eventId: `evt_billing_${n}`,
    eventType: EVENT_TYPES.orderCreditsSettled,
    producer: PRODUCERS.billing,
    correlationId: orderId,
    causationId: null,
    aggregateId: orderId,
    payload: {
      payment: { paymentId, paymentEventId: `evt_pay_${n}`, status: 'SUCCESS' as const, amountHalalas: totals.totalHalalas, currency: 'SAR' as const, resultCode: '000.100.110', paymentBrand: 'MADA', tender },
      order: { orderId, merchantId: MERCHANT, creditOrgId: allowance > 0 ? 'org_acme' : null, customer: { name: 'Noura' }, lines, ...(checkoutId ? { checkoutId } : {}) },
      credits: { outcome: allowance > 0 ? ('CAPTURED' as const) : ('NONE' as const), orgId: allowance > 0 ? 'org_acme' : null, holdIds: [], capturedCredits: allowance / 100, releasedCredits: 0, halalasPerCredit: 100, allowanceHalalas: allowance },
    },
  }) as OrderCreditsSettledEvent;
  const result = await paymentSettlementService.handleOrderCreditsSettled(event);
  expect(result.outcome).toBe('CAPTURED');
  return { orderId, paymentId, gross: totals.totalHalalas, net: totals.taxableHalalas, vat: totals.vatHalalas };
}

function restored(
  order: { orderId: string; paymentId: string; gross: number },
  amount: number,
  refundedTotal: number,
  overrides: Partial<{ refundId: string; eventId: string; restoredCredits: number; capturedHalalas: number; paymentId: string }> = {}
): OrderCreditsRestoredEvent {
  const n = ++seq;
  const refundId = overrides.refundId ?? `rf_${n}`;
  return buildIntegrationEvent({
    eventId: overrides.eventId ?? `evt_restored_${n}`,
    eventType: EVENT_TYPES.orderCreditsRestored,
    producer: PRODUCERS.billing,
    correlationId: order.orderId,
    causationId: `evt_rf_${n}`,
    aggregateId: order.orderId,
    payload: {
      refund: {
        refundId,
        refundEventId: `evt_rf_${n}`,
        paymentId: overrides.paymentId ?? order.paymentId,
        orderId: order.orderId,
        merchantId: MERCHANT,
        currency: 'SAR' as const,
        amountHalalas: amount,
        capturedHalalas: overrides.capturedHalalas ?? order.gross,
        refundedTotalHalalas: refundedTotal,
        reason: 'damaged item',
        refundedAt: new Date().toISOString(),
      },
      credits: { outcome: 'RESTORED' as const, orgId: 'org_acme', capturedCredits: 20, restoredCredits: overrides.restoredCredits ?? 4, cumulativeRestoredCredits: 4, grantId: 'grant_x' },
    },
  }) as OrderCreditsRestoredEvent;
}

async function journal(ref: string) {
  const res = await query<{ wallet_id: string; signed: string }>(`SELECT wallet_id, signed_amount_cents::text AS signed FROM ledger_entries WHERE transaction_ref = $1 ORDER BY wallet_id`, [ref]);
  return res.rows.map((r) => [r.wallet_id, Number(r.signed)]);
}
const balance = async (walletId: string) => (await walletService.getWallet(walletId))?.balanceCents ?? NaN;
const vatBalance = async () => Number((await query<{ b: string }>(`SELECT COALESCE(SUM(balance_cents), 0)::text AS b FROM wallet_balance_shards WHERE wallet_id = $1`, [VAT])).rows[0].b);

describe('Customer refunds: pro-rata reversal of a captured order (exactly once)', () => {
  let app: FastifyInstance;
  let service: OrderRefundService;

  beforeAll(async () => {
    app = buildApp();
    await app.ready();
  });

  beforeEach(async () => {
    await resetAndSeedDatabase();
    await flushRedis();
    service = new OrderRefundService();
  });

  afterAll(async () => {
    await app.close();
    await closeDb();
    await closeRedis();
  });

  it('posts ONE balanced 3-leg reversal: merchant revenue DEBIT net share, VAT payable DEBIT VAT share, clearing CREDIT the refund', async () => {
    const order = await capture(); // 180.00 + 27.00 = 207.00
    const vatBefore = await vatBalance();
    const result = await service.handleOrderCreditsRestored(restored(order, 5000, 5000, { refundId: 'rf_a' }));
    expect(result).toMatchObject({ outcome: 'REFUNDED', journalRef: 'rfd_rf_a', netHalalas: 4348, vatHalalas: 652, merchantAccount: 'REVENUE' });
    expect(await journal('rfd_rf_a')).toEqual([
      [REVENUE, -4348],
      [CLEARING, 5000],
      [VAT, -652],
    ]);
    expect(await balance(REVENUE)).toBe(18000 - 4348);
    expect((await vatBalance()) - vatBefore).toBe(-652);

    const trace = await service.getOrderRefunds(order.orderId);
    expect(trace.totals).toEqual({ grossHalalas: 20700, refundedGrossHalalas: 5000, refundedNetHalalas: 4348, refundedVatHalalas: 652 });
    expect(trace.refunds[0].split).toEqual([{ vatCategory: 'S', vatRateBps: 1500, vatExemptionCode: null, netHalalas: 4348, vatHalalas: 652 }]);

    const out = await query<{ body: any }>(`SELECT body FROM integration_outbox WHERE event_type = $1`, [EVENT_TYPES.ledgerPaymentRefunded]);
    expect(out.rows).toHaveLength(1);
    expect(out.rows[0].body.payload).toMatchObject({
      refund: { refundId: 'rf_a', amountHalalas: 5000 },
      journalRef: 'rfd_rf_a',
      netHalalas: 4348,
      vatHalalas: 652,
      merchantAccount: 'REVENUE',
      tender: 'CARD',
      refundedToWalletId: CLEARING,
      remaining: { grossHalalas: 15700, netHalalas: 13652, vatHalalas: 2048 },
    });
    expect((await ledgerAuditService.runAudit({ publish: false })).ok).toBe(true);
  });

  it('splits pro rata over what is still refundable per VAT category, and a final refund reverses exactly what remains', async () => {
    const order = await capture({ lines: MIXED, allowance: 2500 }); // S 100 + Z 100 + E 50, allowance 25 → 238.50
    expect(order).toMatchObject({ gross: 23850, net: 22500, vat: 1350 });
    const first = await service.handleOrderCreditsRestored(restored(order, 10000, 10000, { refundId: 'rf_m1' }));
    const split1 = (await service.getOrderRefunds(order.orderId)).refunds[0].split;
    // The same split the shared arithmetic gives for the untouched capture.
    const expected = proRataRefundSplit(
      computeOrderTotals(MIXED, 2500).categories.map((c) => ({ vatCategory: c.vatCategory, vatRateBps: c.vatRateBps, vatExemptionCode: MIXED.find((l) => (l.vatCategory ?? 'S') === c.vatCategory)?.vatExemptionCode ?? null, netHalalas: c.taxableHalalas, vatHalalas: c.vatHalalas })),
      10000
    );
    expect(split1).toEqual(expected);
    expect(split1.reduce((s, l) => s + l.netHalalas + l.vatHalalas, 0)).toBe(10000);
    expect(first.netHalalas! + first.vatHalalas!).toBe(10000);

    const final = await service.handleOrderCreditsRestored(restored(order, 13850, 23850, { refundId: 'rf_m2' }));
    expect(final.outcome).toBe('REFUNDED');
    const trace = await service.getOrderRefunds(order.orderId);
    expect(trace.totals).toEqual({ grossHalalas: 23850, refundedGrossHalalas: 23850, refundedNetHalalas: 22500, refundedVatHalalas: 1350 });
    expect(await balance(REVENUE)).toBe(0);
    expect(await vatBalance()).toBe(0);
    const state = await query<{ refund_state: Array<{ vatCategory: string; taxableHalalas: number; vatHalalas: number; refundedNetHalalas: number; refundedVatHalalas: number }> }>(
      `SELECT refund_state FROM order_settlements WHERE order_id = $1`,
      [order.orderId]
    );
    for (const c of state.rows[0].refund_state) {
      expect(c.refundedNetHalalas).toBe(c.taxableHalalas);
      expect(c.refundedVatHalalas).toBe(c.vatHalalas);
    }
    expect((await ledgerAuditService.runAudit({ publish: false })).ok).toBe(true);
  });

  it('refunds a wallet-paid order back to the customer\'s wallet', async () => {
    const before = await balance('wallet_noura_sar');
    const order = await capture({ tender: 'WALLET' });
    expect(await balance('wallet_noura_sar')).toBe(before - 20700);
    const result = await service.handleOrderCreditsRestored(restored(order, 20700, 20700, { refundId: 'rf_w' }));
    expect(result.outcome).toBe('REFUNDED');
    expect(await journal('rfd_rf_w')).toEqual([
      [REVENUE, -18000],
      ['wallet_noura_sar', 20700],
      [VAT, -2700],
    ]);
    expect(await balance('wallet_noura_sar')).toBe(before);
    expect((await ledgerAuditService.runAudit({ publish: false })).ok).toBe(true);
  });

  it('never reverses twice: redeliveries and re-announcements change nothing', async () => {
    const order = await capture();
    const event = restored(order, 5000, 5000, { refundId: 'rf_dup' });
    expect((await service.handleOrderCreditsRestored(event)).outcome).toBe('REFUNDED');
    expect((await service.handleOrderCreditsRestored(event)).outcome).toBe('DUPLICATE_IGNORED');
    expect((await service.handleOrderCreditsRestored({ ...event, eventId: 'evt_again' })).outcome).toBe('DUPLICATE_IGNORED');
    expect(Number((await query(`SELECT COUNT(DISTINCT transaction_ref)::int AS n FROM ledger_entries WHERE transaction_ref LIKE 'rfd_%'`)).rows[0].n)).toBe(1);
    expect(await balance(REVENUE)).toBe(18000 - 4348);
    expect((await readCheckpoint(exec, { consumer: LEDGER_REFUND_CONSUMER, eventId: 'evt_again', step: 'done' }))?.outcome).toBe('DUPLICATE_IGNORED');
  });

  it('refuses more than the capture, unknown orders and mismatched payments (dead letter, nothing moves)', async () => {
    const order = await capture();
    await service.handleOrderCreditsRestored(restored(order, 20000, 20000));
    await expect(service.handleOrderCreditsRestored(restored(order, 701, 20701))).rejects.toMatchObject({ reason: 'REFUND_EXCEEDS_CAPTURE' });
    await expect(service.handleOrderCreditsRestored(restored({ orderId: 'ord_nope', paymentId: 'p', gross: 100 }, 1, 1))).rejects.toMatchObject({ reason: 'ORDER_NOT_SETTLED' });
    await expect(service.handleOrderCreditsRestored(restored(order, 100, 20100, { paymentId: 'pay_other' }))).rejects.toMatchObject({ reason: 'REFUND_PAYMENT_MISMATCH' });
    await expect(service.handleOrderCreditsRestored(restored(order, 100, 20100, { capturedHalalas: 99999 }))).rejects.toMatchObject({ reason: 'REFUND_CAPTURE_MISMATCH' });
    expect((await service.getOrderRefunds(order.orderId)).totals?.refundedGrossHalalas).toBe(20000);
    expect((await ledgerAuditService.runAudit({ publish: false })).ok).toBe(true);
  });

  describe('with merchant settlement', () => {
    const fee = (gross: number): SettlementFee => ({
      ruleId: 'std-tiered', ruleVersion: 1, tiers: [{ fromHalalas: 0, toHalalas: null, bps: 0, baseHalalas: gross, feeHalalas: 0 }],
      percentageFeeHalalas: 0, perTransactionHalalas: 0, fixedFeeHalalas: 0, minimumFeeHalalas: 0, netHalalas: 0, vatRateBps: 1500, vatHalalas: 0, totalHalalas: 0, capped: false,
    });
    const feeEvent = (batchId: string, gross: number, items: number): SettlementFeeCalculatedEvent =>
      buildIntegrationEvent({
        eventId: `evt_fee_${batchId}`,
        eventType: EVENT_TYPES.settlementFeeCalculated,
        producer: PRODUCERS.billing,
        correlationId: batchId,
        causationId: null,
        aggregateId: batchId,
        payload: { batchId, merchantId: MERCHANT, currency: 'SAR' as const, itemCount: items, grossHalalas: gross, fee: fee(gross), payoutHalalas: gross, calculatedAt: new Date().toISOString() },
      }) as SettlementFeeCalculatedEvent;

    it('a refund before the sweep reduces what the sweep claims; the batch still reconciles with the ledger', async () => {
      const order = await capture();
      await service.handleOrderCreditsRestored(restored(order, 5000, 5000));
      const settlements = new MerchantSettlementService();
      const batch = await settlements.sweep({ merchantId: MERCHANT });
      expect(batch).toMatchObject({ itemCount: 1, grossHalalas: 18000 - 4348 });
      const posted = await settlements.handleFeeCalculated(feeEvent(batch.batchId, batch.grossHalalas, 1));
      expect(posted.outcome).toBe('POSTED');
      expect(await balance(REVENUE)).toBe(0);
      expect(await balance(PAYOUT)).toBe(18000 - 4348);
    });

    it('a refund after the sweep is taken from the merchant\'s payout wallet, and one during the sweep waits', async () => {
      const order = await capture();
      const settlements = new MerchantSettlementService();
      const batch = await settlements.sweep({ merchantId: MERCHANT });
      // COLLECTED, fee not yet priced: the refund is retried later (transient).
      await expect(service.handleOrderCreditsRestored(restored(order, 5000, 5000, { refundId: 'rf_wait' }))).rejects.toBeInstanceOf(SettlementInFlightError);
      expect((await service.getOrderRefunds(order.orderId)).refunds).toEqual([]);

      await settlements.handleFeeCalculated(feeEvent(batch.batchId, batch.grossHalalas, 1));
      expect(await balance(PAYOUT)).toBe(18000);
      const result = await service.handleOrderCreditsRestored(restored(order, 5000, 5000, { refundId: 'rf_wait' }));
      expect(result).toMatchObject({ outcome: 'REFUNDED', merchantAccount: 'PAYOUT' });
      expect(await journal('rfd_rf_wait')).toEqual([
        [PAYOUT, -4348],
        [CLEARING, 5000],
        [VAT, -652],
      ]);
      expect(await balance(PAYOUT)).toBe(18000 - 4348);
      expect((await ledgerAuditService.runAudit({ publish: false })).ok).toBe(true);
    });

    it('refuses to overdraw the merchant when the money is already gone (operator action)', async () => {
      const order = await capture();
      const settlements = new MerchantSettlementService();
      const batch = await settlements.sweep({ merchantId: MERCHANT });
      await settlements.handleFeeCalculated(feeEvent(batch.batchId, batch.grossHalalas, 1));
      // The payout left the platform.
      await query(`UPDATE wallets SET balance_cents = 0, ledger_balance_cents = 0 WHERE wallet_id = $1`, [PAYOUT]);
      const err = await service.handleOrderCreditsRestored(restored(order, 5000, 5000)).catch((e: unknown) => e);
      expect(err).toBeInstanceOf(NonRetryableError);
      expect((err as NonRetryableError).reason).toBe('MERCHANT_BALANCE_INSUFFICIENT');
    });
  });

  it('exposes an order\'s refunds over HTTP (admin)', async () => {
    const order = await capture();
    await service.handleOrderCreditsRestored(restored(order, 5000, 5000, { refundId: 'rf_http' }));
    const res = await app.inject({ method: 'GET', url: `/payments/orders/${order.orderId}/refunds`, headers: await bearer(ADMIN.sub, ADMIN.roles) });
    expect(res.statusCode).toBe(200);
    expect(res.json().refunds).toMatchObject([{ refundId: 'rf_http', journalRef: 'rfd_rf_http', netHalalas: 4348, vatHalalas: 652 }]);
    expect(res.json().totals.refundedGrossHalalas).toBe(5000);
    expect((await app.inject({ method: 'GET', url: `/payments/orders/${order.orderId}/refunds`, headers: await bearer('user_noura', ['user']) })).statusCode).toBe(403);
  });
});
