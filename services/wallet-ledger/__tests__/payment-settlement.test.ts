import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import type { FastifyInstance } from 'fastify';
import { buildApp } from '../src/server';
import { closeDb, query, resetAndSeedDatabase } from '../src/db';
import { closeRedis, flushRedis } from '../src/redis';
import { paymentSettlementService } from '../src/services/PaymentSettlementService';
import { ledgerAuditService } from '../src/services/LedgerAuditService';
import { walletService } from '../src/services/WalletService';
import { ADMIN, bearer } from './helpers/auth';
import { InsufficientFundsError, WalletNotFoundError, SystemAccountNotAllowedError } from '../src/types/wallet';
import { checkoutHoldKey } from '../../../shared/contracts';
import {
  EVENT_TYPES,
  OrderCreditsSettledEvent,
  OrderLine,
  PRODUCERS,
  buildIntegrationEvent,
  computeOrderTotals,
} from '../../../shared/contracts';
import { NonRetryableError, listCheckpoints } from '../../../shared/messaging';

const MERCHANT = 'txp_riyadh_retail';
/** Opening SAR journal: Noura's wallet 500.00 against the SAR clearing account. */
const SEEDED_TREASURY = -50000;
const NON_SEED = `transaction_ref <> 'seed_init'`;
const LINES: OrderLine[] = [{ sku: 'RTR', nameEn: 'Router', nameAr: 'موجه', quantity: 2, unitPriceHalalas: 10000 }];

let seq = 0;
function settledEvent(
  overrides: {
    orderId?: string;
    paymentId?: string;
    eventId?: string;
    status?: 'SUCCESS' | 'FAILED';
    lines?: OrderLine[];
    allowanceHalalas?: number;
    amountHalalas?: number;
    merchantId?: string;
    tender?: 'CARD' | 'WALLET';
    checkoutId?: string;
  } = {}
): OrderCreditsSettledEvent {
  const n = ++seq;
  const lines = overrides.lines ?? LINES;
  const allowance = overrides.allowanceHalalas ?? 0;
  const orderId = overrides.orderId ?? `ord_${n}`;
  return buildIntegrationEvent({
    eventId: overrides.eventId ?? `evt_billing_${n}`,
    eventType: EVENT_TYPES.orderCreditsSettled,
    producer: PRODUCERS.billing,
    correlationId: orderId,
    causationId: `evt_payment_${n}`,
    aggregateId: orderId,
    payload: {
      payment: {
        paymentId: overrides.paymentId ?? `pay_${n}`,
        paymentEventId: `evt_payment_${n}`,
        status: overrides.status ?? 'SUCCESS',
        amountHalalas: overrides.amountHalalas ?? computeOrderTotals(lines, allowance).totalHalalas,
        currency: 'SAR',
        resultCode: overrides.status === 'FAILED' ? '800.100.151' : '000.100.110',
        paymentBrand: 'MADA',
        tender: overrides.tender ?? 'CARD',
      },
      order: {
        orderId,
        merchantId: overrides.merchantId ?? MERCHANT,
        creditOrgId: allowance > 0 ? 'org_acme' : null,
        customer: { name: 'Noura Al-Qahtani' },
        lines,
        ...(overrides.checkoutId ? { checkoutId: overrides.checkoutId } : {}),
      },
      credits: {
        outcome: allowance > 0 ? 'CAPTURED' : 'NONE',
        orgId: allowance > 0 ? 'org_acme' : null,
        holdIds: [],
        capturedCredits: allowance / 100,
        releasedCredits: 0,
        halalasPerCredit: 100,
        allowanceHalalas: allowance,
      },
    },
  }) as OrderCreditsSettledEvent;
}

async function journal(ref: string) {
  const res = await query<{ wallet_id: string; signed: string }>(
    `SELECT wallet_id, signed_amount_cents::text AS signed FROM ledger_entries WHERE transaction_ref = $1 ORDER BY wallet_id`,
    [ref]
  );
  return res.rows.map((r) => [r.wallet_id, Number(r.signed)]);
}

async function count(sql: string, params: unknown[] = []): Promise<number> {
  return Number((await query<{ n: string }>(sql, params)).rows[0].n);
}

async function outbox(eventType: string) {
  const res = await query<{ event_id: string; body: any; exchange: string; routing_key: string }>(
    `SELECT event_id, body, exchange, routing_key FROM integration_outbox WHERE event_type = $1 ORDER BY seq`,
    [eventType]
  );
  return res.rows;
}

describe('Card payment settlement (two-phase hold, 3-leg SAR journal, exactly once)', () => {
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
    await closeDb();
    await closeRedis();
  });

  it('seeds SAR: currency, system accounts, VAT payable, merchant revenue + payout wallets and the USD peg', async () => {
    expect(await count(`SELECT COUNT(*)::text AS n FROM currencies WHERE code = 'SAR' AND minor_units = 2`)).toBe(1);
    const accounts = await query<{ wallet_id: string; account_type: string }>(
      `SELECT wallet_id, account_type FROM wallets WHERE currency = 'SAR' ORDER BY wallet_id`
    );
    expect(accounts.rows).toEqual([
      { wallet_id: 'wallet_escrow_sar', account_type: 'ESCROW' },
      { wallet_id: 'wallet_fee_treasury_sar', account_type: 'FEE_TREASURY' },
      { wallet_id: 'wallet_fx_pool_sar', account_type: 'FX_POOL' },
      { wallet_id: 'wallet_merchant_riyadh_retail', account_type: 'USER' },
      { wallet_id: 'wallet_noura_sar', account_type: 'USER' },
      { wallet_id: 'wallet_payout_riyadh_retail', account_type: 'USER' },
      { wallet_id: 'wallet_treasury_sar', account_type: 'CLEARING' },
      { wallet_id: 'wallet_vat_payable_sar', account_type: 'CLEARING' },
    ]);
    const rate = await query<{ r: string }>(`SELECT rate_scaled::text AS r FROM fx_rates WHERE base_currency = 'USD' AND quote_currency = 'SAR'`);
    expect(rate.rows[0].r).toBe('375000000');
  });

  it('captures: gateway clearing DEBIT gross, merchant revenue CREDIT net, VAT payable CREDIT 15 %, balanced', async () => {
    const event = settledEvent({ allowanceHalalas: 2000 });
    const result = await paymentSettlementService.handleOrderCreditsSettled(event);
    expect(result).toMatchObject({ outcome: 'CAPTURED', grossHalalas: 20700, netRevenueHalalas: 18000, vatHalalas: 2700 });

    expect(await journal(result.journalRef!)).toEqual([
      ['wallet_merchant_riyadh_retail', 18000],
      ['wallet_treasury_sar', -20700],
      ['wallet_vat_payable_sar', 2700],
    ]);
    expect((await walletService.getWallet('wallet_merchant_riyadh_retail'))?.balanceCents).toBe(18000);
    expect((await walletService.getWallet('wallet_vat_payable_sar'))?.balanceCents).toBe(2700);
    expect((await walletService.getWallet('wallet_treasury_sar'))?.balanceCents).toBe(SEEDED_TREASURY - 20700);

    const [announced] = await outbox(EVENT_TYPES.ledgerPaymentSettled);
    expect(announced).toMatchObject({
      event_id: result.eventId,
      exchange: 'ledger.events',
      routing_key: 'ledger.payment.settled',
      body: {
        eventType: 'ledger.payment.settled',
        correlationId: event.payload.order.orderId,
        causationId: event.eventId,
        payload: { grossHalalas: 20700, netRevenueHalalas: 18000, vatHalalas: 2700, allowanceHalalas: 2000, journalRef: result.journalRef },
      },
    });
    const checkpoints = await listCheckpoints({ query: (t, p) => query(t, p) }, { eventId: event.eventId });
    expect(checkpoints).toMatchObject([{ consumer: 'ledger.order-settlements', step: 'done', outcome: 'CAPTURED' }]);
  });

  it('leaves the ledger fully consistent for the audit (global zero-sum, no drift, balanced journals)', async () => {
    for (let i = 0; i < 5; i++) await paymentSettlementService.handleOrderCreditsSettled(settledEvent({ allowanceHalalas: i * 100 }));
    const report = await ledgerAuditService.runAudit({ publish: false });
    expect(report.ok).toBe(true);
  });

  it('posts no VAT leg for a zero-rated order (2 legs, still balanced)', async () => {
    const lines: OrderLine[] = [
      { sku: 'EXP', nameEn: 'Export', nameAr: 'تصدير', quantity: 1, unitPriceHalalas: 5000, vatCategory: 'Z', vatExemptionCode: 'VATEX-SA-32' },
    ];
    const result = await paymentSettlementService.handleOrderCreditsSettled(settledEvent({ lines }));
    expect(result).toMatchObject({ outcome: 'CAPTURED', vatHalalas: 0, netRevenueHalalas: 5000 });
    expect(await journal(result.journalRef!)).toEqual([
      ['wallet_merchant_riyadh_retail', 5000],
      ['wallet_treasury_sar', -5000],
    ]);
  });

  it('releases on a failed payment: the hold is dropped and nothing is booked', async () => {
    const event = settledEvent({ status: 'FAILED' });
    const result = await paymentSettlementService.handleOrderCreditsSettled(event);
    expect(result.outcome).toBe('RELEASED');
    const view = await paymentSettlementService.getOrderSettlement(event.payload.order.orderId);
    expect(view.holds).toMatchObject([{ status: 'RELEASED', journalRef: null }]);
    expect(view.settlement).toBeNull();
    expect(await count(`SELECT COUNT(*)::text AS n FROM ledger_entries WHERE currency = 'SAR' AND ${NON_SEED}`)).toBe(0);
    expect(await outbox(EVENT_TYPES.ledgerPaymentReleased)).toHaveLength(1);
  });

  it('captures a hold that was authorized earlier (two-phase), and refuses a mismatching authorization', async () => {
    const event = settledEvent();
    const hold = await paymentSettlementService.placeHold({
      paymentId: event.payload.payment.paymentId,
      orderId: event.payload.order.orderId,
      merchantId: MERCHANT,
      amountHalalas: event.payload.payment.amountHalalas,
    });
    expect(hold.status).toBe('HELD');
    // Idempotent per payment.
    const replay = { paymentId: event.payload.payment.paymentId, orderId: hold.orderId, merchantId: MERCHANT, amountHalalas: hold.amountCents };
    expect((await paymentSettlementService.placeHold(replay)).holdId).toBe(hold.holdId);
    await expect(paymentSettlementService.placeHold({ ...replay, amountHalalas: 1 })).rejects.toMatchObject({ reason: 'HOLD_MISMATCH' });

    const result = await paymentSettlementService.handleOrderCreditsSettled(event);
    expect(result).toMatchObject({ outcome: 'CAPTURED', holdId: hold.holdId });
  });

  describe('never twice', () => {
    it('ignores a redelivered event (same id) and a re-announcement (new id) of the same payment', async () => {
      const event = settledEvent();
      const first = await paymentSettlementService.handleOrderCreditsSettled(event);
      expect((await paymentSettlementService.handleOrderCreditsSettled(event)).outcome).toBe('DUPLICATE_IGNORED');
      const reannounced = { ...event, eventId: 'evt_billing_again' };
      expect(await paymentSettlementService.handleOrderCreditsSettled(reannounced)).toMatchObject({
        outcome: 'DUPLICATE_IGNORED',
        journalRef: first.journalRef,
      });
      expect(await count(`SELECT COUNT(DISTINCT transaction_ref)::text AS n FROM ledger_entries WHERE currency = 'SAR' AND ${NON_SEED}`)).toBe(1);
      expect(await outbox(EVENT_TYPES.ledgerPaymentSettled)).toHaveLength(1);
    });

    it('books exactly one journal when the same settlement arrives 10 times concurrently', async () => {
      const base = settledEvent();
      const results = await Promise.allSettled(
        Array.from({ length: 10 }, (_, i) => paymentSettlementService.handleOrderCreditsSettled({ ...base, eventId: `evt_dup_${i}` }))
      );
      const outcomes = results.map((r) => (r.status === 'fulfilled' ? r.value.outcome : 'ERR'));
      expect(outcomes.filter((o) => o === 'CAPTURED')).toHaveLength(1);
      expect(outcomes.filter((o) => o === 'DUPLICATE_IGNORED')).toHaveLength(9);
      expect(await count(`SELECT COUNT(*)::text AS n FROM ledger_entries WHERE currency = 'SAR' AND ${NON_SEED}`)).toBe(3);
      expect((await walletService.getWallet('wallet_merchant_riyadh_retail'))?.balanceCents).toBe(23000 - 3000);
    });

    it('refuses a second successful payment for an already-settled order (refund case)', async () => {
      await paymentSettlementService.handleOrderCreditsSettled(settledEvent({ orderId: 'ord_x', paymentId: 'pay_a' }));
      await expect(
        paymentSettlementService.handleOrderCreditsSettled(settledEvent({ orderId: 'ord_x', paymentId: 'pay_b' }))
      ).rejects.toMatchObject({ reason: 'ORDER_ALREADY_SETTLED' });
    });

    it('refuses contradicting outcomes for one payment (FAILED after capture, SUCCESS after release)', async () => {
      await paymentSettlementService.handleOrderCreditsSettled(settledEvent({ orderId: 'ord_c', paymentId: 'pay_c' }));
      await expect(
        paymentSettlementService.handleOrderCreditsSettled(settledEvent({ orderId: 'ord_c', paymentId: 'pay_c', status: 'FAILED' }))
      ).rejects.toMatchObject({ reason: 'HOLD_ALREADY_CAPTURED' });

      await paymentSettlementService.handleOrderCreditsSettled(settledEvent({ orderId: 'ord_r', paymentId: 'pay_r', status: 'FAILED' }));
      await expect(
        paymentSettlementService.handleOrderCreditsSettled(settledEvent({ orderId: 'ord_r', paymentId: 'pay_r' }))
      ).rejects.toMatchObject({ reason: 'HOLD_ALREADY_RELEASED' });
    });
  });

  describe('business rules (non-retryable: the whole transaction rolls back)', () => {
    it('rejects an amount that does not match lines − allowance + VAT, leaving no trace', async () => {
      const event = settledEvent({ allowanceHalalas: 2000, amountHalalas: 23000 });
      const err = await paymentSettlementService.handleOrderCreditsSettled(event).catch((e: unknown) => e);
      expect(err).toBeInstanceOf(NonRetryableError);
      expect(err).toMatchObject({ reason: 'AMOUNT_MISMATCH' });
      expect(await count(`SELECT COUNT(*)::text AS n FROM payment_holds`)).toBe(0);
      expect(await count(`SELECT COUNT(*)::text AS n FROM integration_checkpoints`)).toBe(0);
      expect(await count(`SELECT COUNT(*)::text AS n FROM ledger_entries WHERE currency = 'SAR' AND ${NON_SEED}`)).toBe(0);
    });

    it('rejects an unknown merchant and an allowance larger than the order', async () => {
      await expect(paymentSettlementService.handleOrderCreditsSettled(settledEvent({ merchantId: 'txp_nobody' }))).rejects.toMatchObject({
        reason: 'MERCHANT_NOT_FOUND',
      });
      await expect(
        paymentSettlementService.handleOrderCreditsSettled(settledEvent({ allowanceHalalas: 99999, amountHalalas: 100 }))
      ).rejects.toMatchObject({ reason: 'ALLOWANCE_EXCEEDS_NET' });
    });

    it('enforces one settlement per order and gross = net + VAT in the schema itself', async () => {
      await paymentSettlementService.handleOrderCreditsSettled(settledEvent({ orderId: 'ord_schema', paymentId: 'pay_schema' }));
      await expect(
        query(
          `INSERT INTO order_settlements (order_id, payment_id, hold_id, merchant_id, currency, journal_ref, gross_cents,
             net_revenue_cents, vat_cents, source_event_id, settlement_event_id)
           SELECT 'ord_schema', 'pay_other', hold_id, merchant_id, 'SAR', 'x', 10, 5, 5, 'e', 'e2' FROM payment_holds LIMIT 1`
        )
      ).rejects.toThrow(/duplicate key|unique/i);
    });
  });

  describe('admin API', () => {
    it('places holds and shows an order settlement to admins only', async () => {
      const payload = { paymentId: 'pay_api', orderId: 'ord_api', merchantId: MERCHANT, amountHalalas: 23000 };
      const denied = await app.inject({ method: 'POST', url: '/payments/holds', payload, headers: await bearer('user_alice') });
      expect(denied.statusCode).toBe(403);
      const placed = await app.inject({ method: 'POST', url: '/payments/holds', payload, headers: await bearer(ADMIN.sub, ADMIN.roles) });
      expect(placed.statusCode).toBe(201);
      expect(placed.json()).toMatchObject({ paymentId: 'pay_api', status: 'HELD', amountCents: 23000 });
      const bad = await app.inject({ method: 'POST', url: '/payments/holds', payload: { ...payload, merchantId: 'nope' }, headers: await bearer(ADMIN.sub, ADMIN.roles) });
      expect(bad.statusCode).toBe(422);

      await paymentSettlementService.handleOrderCreditsSettled(settledEvent({ orderId: 'ord_api', paymentId: 'pay_api' }));
      const view = await app.inject({ method: 'GET', url: '/payments/orders/ord_api/settlement', headers: await bearer(ADMIN.sub, ADMIN.roles) });
      expect(view.statusCode).toBe(200);
      expect(view.json()).toMatchObject({ settlement: { grossHalalas: 23000, vatHalalas: 3000 }, holds: [{ status: 'CAPTURED' }] });
      expect(view.json().journal).toHaveLength(3);
    });
  });

  describe('split-tender remainder holds (checkout coordinator)', () => {
    const NOURA = 'wallet_noura_sar';
    const ESCROW = 'wallet_escrow_sar';
    const balance = async (walletId: string) => (await walletService.getWallet(walletId))?.balanceCents;
    const walletHold = (checkoutId: string, amountHalalas: number, orderId = `ord_${checkoutId}`) => ({
      holdKey: checkoutHoldKey(checkoutId, 'remainder'),
      orderId,
      merchantId: MERCHANT,
      tender: 'WALLET' as const,
      sourceWalletId: NOURA,
      amountHalalas,
      checkoutId,
    });

    it('a WALLET hold moves the remainder from the customer into escrow, once per key', async () => {
      const first = await paymentSettlementService.placeTenderHold(walletHold('co_1', 20000));
      expect(first).toMatchObject({ status: 'HELD', tender: 'WALLET', created: true, amountCents: 20000 });
      expect(await journal(first.reserveJournalRef!)).toEqual([
        [ESCROW, 20000],
        [NOURA, -20000],
      ]);
      const replay = await paymentSettlementService.placeTenderHold(walletHold('co_1', 20000));
      expect(replay).toMatchObject({ holdId: first.holdId, created: false });
      expect(await balance(NOURA)).toBe(30000); // debited exactly once
      expect(await balance(ESCROW)).toBe(20000);
      await expect(paymentSettlementService.placeTenderHold(walletHold('co_1', 20001))).rejects.toMatchObject({ reason: 'HOLD_MISMATCH' });
    });

    it('refuses a remainder the wallet cannot cover, leaving no hold and no money moved', async () => {
      await expect(paymentSettlementService.placeTenderHold(walletHold('co_poor', 50001))).rejects.toBeInstanceOf(InsufficientFundsError);
      expect(await paymentSettlementService.getHold(checkoutHoldKey('co_poor', 'remainder'))).toBeNull();
      expect(await balance(NOURA)).toBe(50000);
      expect(await balance(ESCROW)).toBe(0);
      await expect(paymentSettlementService.placeTenderHold({ ...walletHold('co_x', 100), sourceWalletId: 'wallet_nobody' })).rejects.toBeInstanceOf(
        WalletNotFoundError
      );
      await expect(paymentSettlementService.placeTenderHold({ ...walletHold('co_y', 100), sourceWalletId: 'wallet_treasury_sar' })).rejects.toBeInstanceOf(
        SystemAccountNotAllowedError
      );
    });

    it('releasing refunds the customer exactly once; a released key never places again', async () => {
      const hold = await paymentSettlementService.placeTenderHold(walletHold('co_rel', 12345));
      const released = await paymentSettlementService.releaseTenderHold({ ...walletHold('co_rel', 12345), reason: 'CANCELLED' });
      expect(released).toMatchObject({ outcome: 'RELEASED', hold: { status: 'RELEASED', releaseJournalRef: `rls_${hold.holdId}` } });
      expect(await paymentSettlementService.releaseTenderHold({ ...walletHold('co_rel', 12345), reason: 'again' })).toMatchObject({
        outcome: 'ALREADY_RELEASED',
      });
      expect(await balance(NOURA)).toBe(50000);
      expect(await balance(ESCROW)).toBe(0);
      await expect(paymentSettlementService.placeTenderHold(walletHold('co_rel', 12345))).rejects.toMatchObject({ reason: 'HOLD_RELEASED' });
      expect(await balance(NOURA)).toBe(50000);
    });

    it('a release that arrives before its placement tombstones the key, so the late placement is refused (no orphan)', async () => {
      expect(await paymentSettlementService.releaseTenderHold({ ...walletHold('co_race', 5000), reason: 'compensation' })).toMatchObject({
        outcome: 'TOMBSTONED',
        hold: { status: 'RELEASED' },
      });
      await expect(paymentSettlementService.placeTenderHold(walletHold('co_race', 5000))).rejects.toMatchObject({ reason: 'HOLD_RELEASED' });
      expect(await balance(NOURA)).toBe(50000);
      expect(await count(`SELECT COUNT(*)::text AS n FROM payment_holds WHERE status = 'HELD'`)).toBe(0);
    });

    it('debits once when the same hold is placed 8 times concurrently', async () => {
      const results = await Promise.all(Array.from({ length: 8 }, () => paymentSettlementService.placeTenderHold(walletHold('co_burst', 7000))));
      expect(results.filter((r) => r.created)).toHaveLength(1);
      expect(new Set(results.map((r) => r.holdId)).size).toBe(1);
      expect(await balance(NOURA)).toBe(43000);
    });

    it('captures the checkout\'s WALLET hold from escrow: 3 legs, escrow back to zero, customer debited once', async () => {
      const lines: OrderLine[] = [
        { sku: 'S', nameEn: 'Router', nameAr: 'موجه', quantity: 1, unitPriceHalalas: 10000 },
        { sku: 'Z', nameEn: 'Export', nameAr: 'تصدير', quantity: 1, unitPriceHalalas: 10000, vatCategory: 'Z', vatExemptionCode: 'VATEX-SA-32' },
        { sku: 'E', nameEn: 'Finance', nameAr: 'تمويل', quantity: 1, unitPriceHalalas: 5000, vatCategory: 'E', vatExemptionCode: 'VATEX-SA-29' },
      ];
      const gross = computeOrderTotals(lines, 2500).totalHalalas; // 238.50
      await paymentSettlementService.placeTenderHold(walletHold('co_cap', gross, 'ord_cap'));
      const event = settledEvent({ orderId: 'ord_cap', paymentId: 'wallet:co_cap', lines, allowanceHalalas: 2500, tender: 'WALLET', checkoutId: 'co_cap' });
      const result = await paymentSettlementService.handleOrderCreditsSettled(event);
      expect(result).toMatchObject({ outcome: 'CAPTURED', tender: 'WALLET', grossHalalas: 23850, netRevenueHalalas: 22500, vatHalalas: 1350 });
      expect(await journal(result.journalRef!)).toEqual([
        [ESCROW, -23850],
        ['wallet_merchant_riyadh_retail', 22500],
        ['wallet_vat_payable_sar', 1350],
      ]);
      expect(await balance(ESCROW)).toBe(0);
      expect(await balance(NOURA)).toBe(50000 - 23850);
      const hold = await paymentSettlementService.getHold(checkoutHoldKey('co_cap', 'remainder'));
      expect(hold).toMatchObject({ status: 'CAPTURED', paymentId: 'wallet:co_cap' });
      await expect(paymentSettlementService.releaseTenderHold({ ...walletHold('co_cap', gross, 'ord_cap'), reason: 'too late' })).rejects.toMatchObject({
        reason: 'HOLD_ALREADY_CAPTURED',
      });
      expect((await ledgerAuditService.runAudit({ publish: false })).ok).toBe(true);
    });

    it('captures the checkout\'s CARD authorization when the gateway payment names the checkout', async () => {
      const gross = computeOrderTotals(LINES, 1000).totalHalalas;
      const placed = await paymentSettlementService.placeTenderHold({
        holdKey: checkoutHoldKey('co_card', 'remainder'),
        orderId: 'ord_card',
        merchantId: MERCHANT,
        tender: 'CARD',
        amountHalalas: gross,
        checkoutId: 'co_card',
      });
      expect(await balance(NOURA)).toBe(50000); // a card authorization moves no wallet money
      const result = await paymentSettlementService.handleOrderCreditsSettled(
        settledEvent({ orderId: 'ord_card', paymentId: 'pay_gw_1', allowanceHalalas: 1000, checkoutId: 'co_card' })
      );
      expect(result).toMatchObject({ outcome: 'CAPTURED', holdId: placed.holdId, tender: 'CARD' });
      expect((await journal(result.journalRef!))[1]).toEqual(['wallet_treasury_sar', -gross]);
      expect(await paymentSettlementService.getHold(checkoutHoldKey('co_card', 'remainder'))).toMatchObject({ paymentId: 'pay_gw_1' });
    });

    it('refuses a checkout payment whose hold is missing or does not match', async () => {
      await expect(
        paymentSettlementService.handleOrderCreditsSettled(settledEvent({ orderId: 'ord_nohold', checkoutId: 'co_missing' }))
      ).rejects.toMatchObject({ reason: 'CHECKOUT_HOLD_NOT_FOUND' });
      await paymentSettlementService.placeTenderHold({
        holdKey: checkoutHoldKey('co_mm', 'remainder'),
        orderId: 'ord_mm',
        merchantId: MERCHANT,
        tender: 'CARD',
        amountHalalas: 100,
        checkoutId: 'co_mm',
      });
      await expect(
        paymentSettlementService.handleOrderCreditsSettled(settledEvent({ orderId: 'ord_mm', checkoutId: 'co_mm' }))
      ).rejects.toMatchObject({ reason: 'HOLD_MISMATCH' });
    });

    it('lets only admins and the checkout coordinator place / release holds over HTTP', async () => {
      const payload = walletHold('co_http', 1000);
      const asUser = await app.inject({ method: 'POST', url: '/payments/holds', payload, headers: await bearer('user_noura') });
      expect(asUser.statusCode).toBe(403);
      const coordinator = await bearer('svc_checkout_coordinator', ['checkout_coordinator']);
      const placed = await app.inject({ method: 'POST', url: '/payments/holds', payload, headers: coordinator });
      expect(placed.statusCode).toBe(201);
      const replay = await app.inject({ method: 'POST', url: '/payments/holds', payload, headers: coordinator });
      expect(replay.statusCode).toBe(200);
      const tooMuch = await app.inject({ method: 'POST', url: '/payments/holds', payload: walletHold('co_http2', 999999), headers: coordinator });
      expect(tooMuch.statusCode).toBe(409);
      expect(tooMuch.json().code).toBe('INSUFFICIENT_FUNDS');
      const released = await app.inject({ method: 'POST', url: '/payments/holds/release', payload: { ...payload, reason: 'CANCELLED' }, headers: coordinator });
      expect(released.statusCode).toBe(200);
      expect(released.json().outcome).toBe('RELEASED');
      const readBack = await app.inject({ method: 'GET', url: `/payments/holds/${encodeURIComponent(payload.holdKey)}`, headers: coordinator });
      expect(readBack.json()).toMatchObject({ status: 'RELEASED', tender: 'WALLET' });
      // The coordinator role reads no wallets.
      expect((await app.inject({ method: 'GET', url: `/wallets/${NOURA}`, headers: coordinator })).statusCode).toBe(404);
    });
  });
});
