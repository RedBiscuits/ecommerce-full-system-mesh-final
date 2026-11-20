import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import type { FastifyInstance } from 'fastify';
import { buildApp } from '../src/server';
import { closeDb, query, resetAndSeedDatabase, withTransaction } from '../src/db';
import { closeRedis, flushRedis } from '../src/redis';
import { paymentSettlementService } from '../src/services/PaymentSettlementService';
import {
  LEDGER_SETTLEMENT_DOCUMENTS_CONSUMER,
  LEDGER_SETTLEMENT_FEE_CONSUMER,
  MerchantSettlementService,
  SettlementRequestError,
  type SettlementBatchView,
} from '../src/services/MerchantSettlementService';
import { ledgerAuditService } from '../src/services/LedgerAuditService';
import { walletService } from '../src/services/WalletService';
import { ADMIN, bearer } from './helpers/auth';
import {
  EVENT_TYPES,
  PRODUCERS,
  buildIntegrationEvent,
  computeOrderTotals,
  type OrderCreditsSettledEvent,
  type OrderLine,
  type SettlementDocumentsReportedEvent,
  type SettlementFee,
  type SettlementFeeCalculatedEvent,
} from '../../../shared/contracts';
import { NonRetryableError, readCheckpoint } from '../../../shared/messaging';

const MERCHANT = 'txp_riyadh_retail';
const REVENUE = 'wallet_merchant_riyadh_retail';
const PAYOUT = 'wallet_payout_riyadh_retail';
const FEES = 'wallet_fee_treasury_sar';
const LINES: OrderLine[] = [{ sku: 'RTR', nameEn: 'Router', nameAr: 'موجه', quantity: 2, unitPriceHalalas: 10000 }];
const exec = { query: (text: string, params?: unknown[]) => query(text, params) };

let seq = 0;
/** A real capture through the order flow's ledger step (revenue 180.00 SAR with a 20-credit allowance). */
async function captureOrder(allowanceHalalas = 2000): Promise<string> {
  const n = ++seq;
  const orderId = `ord_stl_${n}`;
  const event = buildIntegrationEvent({
    eventId: `evt_billing_stl_${n}`,
    eventType: EVENT_TYPES.orderCreditsSettled,
    producer: PRODUCERS.billing,
    correlationId: orderId,
    causationId: null,
    aggregateId: orderId,
    payload: {
      payment: {
        paymentId: `pay_stl_${n}`,
        paymentEventId: `evt_pay_stl_${n}`,
        status: 'SUCCESS' as const,
        amountHalalas: computeOrderTotals(LINES, allowanceHalalas).totalHalalas,
        currency: 'SAR' as const,
        resultCode: '000.100.110',
        paymentBrand: 'MADA',
        tender: 'CARD' as const,
      },
      order: { orderId, merchantId: MERCHANT, creditOrgId: null, customer: { name: 'Noura' }, lines: LINES },
      credits: {
        outcome: 'CAPTURED' as const,
        orgId: 'org_acme',
        holdIds: [],
        capturedCredits: allowanceHalalas / 100,
        releasedCredits: 0,
        halalasPerCredit: 100,
        allowanceHalalas,
      },
    },
  }) as OrderCreditsSettledEvent;
  const result = await paymentSettlementService.handleOrderCreditsSettled(event);
  expect(result.outcome).toBe('CAPTURED');
  return orderId;
}

/**
 * Bulk history: `count` captured orders of `netEach` revenue, one per `spacingMs` from `startAt`,
 * each with its own balanced capture journal (clearing DEBIT, merchant revenue CREDIT) — in one
 * transaction, inside PostgreSQL.
 */
async function seedHistory(count: number, options: { netEach?: number; startAt?: Date; spacingMs?: number; prefix?: string } = {}): Promise<void> {
  const net = options.netEach ?? 10_000;
  const start = (options.startAt ?? new Date(Date.now() - 10 * 24 * 3600_000)).toISOString();
  const spacing = options.spacingMs ?? 60_000;
  const p = options.prefix ?? `bulk${++seq}`;
  await withTransaction(async (client) => {
    await client.query(
      `INSERT INTO payment_holds (hold_id, hold_key, payment_id, order_id, merchant_id, currency, tender, amount_cents, status, journal_ref, created_at)
       SELECT 'ph_${p}_' || g, 'payment:${p}_' || g, 'pay_${p}_' || g, 'ord_${p}_' || g, $1, 'SAR', 'CARD', $2, 'CAPTURED', 'cap_${p}_' || g,
              $3::timestamptz + (g * $4 || ' milliseconds')::interval
       FROM generate_series(1, $5) AS g`,
      [MERCHANT, net, start, spacing, count]
    );
    await client.query(
      `INSERT INTO ledger_entries (entry_id, transaction_ref, wallet_id, currency, direction, signed_amount_cents)
       SELECT 'le_${p}_d_' || g, 'cap_${p}_' || g, 'wallet_treasury_sar', 'SAR', 'DEBIT', -$1::bigint FROM generate_series(1, $2) AS g
       UNION ALL
       SELECT 'le_${p}_c_' || g, 'cap_${p}_' || g, $3, 'SAR', 'CREDIT', $1::bigint FROM generate_series(1, $2) AS g`,
      [net, count, REVENUE]
    );
    await client.query(
      `INSERT INTO order_settlements (order_id, payment_id, hold_id, merchant_id, currency, journal_ref, gross_cents, net_revenue_cents,
         vat_cents, allowance_cents, captured_credits, source_event_id, settlement_event_id, created_at)
       SELECT 'ord_${p}_' || g, 'pay_${p}_' || g, 'ph_${p}_' || g, $1, 'SAR', 'cap_${p}_' || g, $2, $2, 0, 0, 0,
              'evt_src_${p}_' || g, 'evt_set_${p}_' || g, $3::timestamptz + (g * $4 || ' milliseconds')::interval
       FROM generate_series(1, $5) AS g`,
      [MERCHANT, net, start, spacing, count]
    );
    await client.query(`UPDATE wallets SET balance_cents = ledger_balance_cents WHERE wallet_id = $1`, [REVENUE]);
  });
}

function feeFor(gross: number, itemCount: number): SettlementFee {
  const net = Math.round(gross * 0.02) + 50 * itemCount;
  const vat = computeOrderTotals([{ sku: 'F', nameEn: 'f', nameAr: 'f', quantity: 1, unitPriceHalalas: net }], 0).vatHalalas;
  return {
    ruleId: 'std-tiered',
    ruleVersion: 1,
    tiers: [{ fromHalalas: 0, toHalalas: null, bps: 200, baseHalalas: gross, feeHalalas: net - 50 * itemCount }],
    percentageFeeHalalas: net - 50 * itemCount,
    perTransactionHalalas: 50,
    fixedFeeHalalas: 50 * itemCount,
    minimumFeeHalalas: 0,
    netHalalas: net,
    vatRateBps: 1500,
    vatHalalas: vat,
    totalHalalas: net + vat,
    capped: false,
  };
}

function feeEvent(batch: SettlementBatchView, overrides: { eventId?: string; grossHalalas?: number; fee?: SettlementFee; payoutHalalas?: number } = {}): SettlementFeeCalculatedEvent {
  const gross = overrides.grossHalalas ?? batch.grossHalalas;
  const fee = overrides.fee ?? feeFor(gross, batch.itemCount);
  return buildIntegrationEvent({
    eventId: overrides.eventId ?? `evt_fee_${batch.batchId}`,
    eventType: EVENT_TYPES.settlementFeeCalculated,
    producer: PRODUCERS.billing,
    correlationId: batch.batchId,
    causationId: `evt_collected_${batch.batchId}`,
    aggregateId: batch.batchId,
    payload: {
      batchId: batch.batchId,
      merchantId: batch.merchantId,
      currency: 'SAR' as const,
      itemCount: batch.itemCount,
      grossHalalas: gross,
      fee,
      payoutHalalas: overrides.payoutHalalas ?? gross - fee.totalHalalas,
      calculatedAt: new Date().toISOString(),
    },
  }) as SettlementFeeCalculatedEvent;
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

const balance = async (walletId: string) => (await walletService.getWallet(walletId))?.balanceCents ?? NaN;

describe('Merchant settlement batches (sweep, 3-leg settlement journal, exactly once)', () => {
  let app: FastifyInstance;
  let service: MerchantSettlementService;

  beforeAll(async () => {
    app = buildApp();
    await app.ready();
  });

  beforeEach(async () => {
    await resetAndSeedDatabase();
    await flushRedis();
    service = new MerchantSettlementService({ chunkSize: 2 });
  });

  afterAll(async () => {
    await app.close();
    await closeDb();
    await closeRedis();
  });

  describe('sweep', () => {
    it('sweeps the merchant\'s settled revenue since the beginning, in keyset chunks, and announces the batch', async () => {
      for (let i = 0; i < 5; i++) await captureOrder();
      const batch = await service.sweep({ merchantId: MERCHANT });
      expect(batch).toMatchObject({ status: 'COLLECTED', itemCount: 5, grossHalalas: 5 * 18000, window: { from: null }, hasMore: false });
      expect(batch.chunks).toBe(3); // 2 + 2 + 1 (< chunk: window exhausted)
      expect(await count(`SELECT COUNT(*) AS n FROM order_settlements WHERE settlement_batch_id = $1`, [batch.batchId])).toBe(5);

      const events = await query<{ event_id: string; routing_key: string; body: any }>(
        `SELECT event_id, routing_key, body FROM integration_outbox WHERE event_type = $1`,
        [EVENT_TYPES.merchantSettlementCollected]
      );
      expect(events.rows).toHaveLength(1);
      expect(events.rows[0].routing_key).toBe('ledger.merchant-settlement.collected');
      expect(events.rows[0].body.payload).toMatchObject({ batchId: batch.batchId, itemCount: 5, grossHalalas: 90000, hasMore: false });
    });

    it('claims every entry exactly once: a second sweep finds nothing, a later one only the new revenue', async () => {
      await captureOrder();
      await captureOrder();
      const first = await service.sweep({ merchantId: MERCHANT });
      expect(first.itemCount).toBe(2);
      const second = await service.sweep({ merchantId: MERCHANT });
      expect(second).toMatchObject({ status: 'EMPTY', itemCount: 0, grossHalalas: 0 });
      await captureOrder();
      const third = await service.sweep({ merchantId: MERCHANT });
      expect(third).toMatchObject({ status: 'COLLECTED', itemCount: 1, grossHalalas: 18000 });
      // Only COLLECTED batches are announced.
      expect(await count(`SELECT COUNT(*) AS n FROM integration_outbox WHERE event_type = $1`, [EVENT_TYPES.merchantSettlementCollected])).toBe(2);
      expect(await count(`SELECT COUNT(*) AS n FROM order_settlements WHERE settlement_batch_id IS NULL`)).toBe(0);
    });

    it('sweeps a time window [from, to); the rest stays for a later (since-the-beginning) batch; nothing twice', async () => {
      const start = new Date('2026-01-01T00:00:00Z');
      await seedHistory(30, { startAt: start, spacingMs: 3600_000, netEach: 1_000 }); // hourly, 01:00 … 06:00 next day
      const from = new Date('2026-01-01T05:00:00Z');
      const to = new Date('2026-01-01T15:00:00Z');
      const windowed = await service.sweep({ merchantId: MERCHANT, from, to }, { chunkSize: 4 });
      expect(windowed).toMatchObject({ status: 'COLLECTED', itemCount: 10, grossHalalas: 10_000 }); // 05:00 … 14:00
      expect(windowed.window).toEqual({ from: from.toISOString(), to: to.toISOString() });
      expect(windowed.firstSettledAt).toBe('2026-01-01T05:00:00.000Z');
      expect(windowed.lastSettledAt).toBe('2026-01-01T14:00:00.000Z');

      const rest = await service.sweep({ merchantId: MERCHANT }, { chunkSize: 7 });
      expect(rest).toMatchObject({ status: 'COLLECTED', itemCount: 20, grossHalalas: 20_000 });
      const perBatch = await query<{ b: string; n: string }>(
        `SELECT settlement_batch_id AS b, COUNT(*)::text AS n FROM order_settlements GROUP BY settlement_batch_id ORDER BY n`
      );
      expect(perBatch.rows.map((r) => Number(r.n))).toEqual([10, 20]);
    });

    it('caps a batch at maxItems and leaves the remainder for the next batch (hasMore)', async () => {
      await seedHistory(9, { netEach: 500 });
      const capped = await service.sweep({ merchantId: MERCHANT, maxItems: 4 }, { chunkSize: 3 });
      expect(capped).toMatchObject({ status: 'COLLECTED', itemCount: 4, grossHalalas: 2000, hasMore: true });
      const next = await service.sweep({ merchantId: MERCHANT, maxItems: 4 });
      expect(next).toMatchObject({ itemCount: 4, hasMore: true });
      const last = await service.sweep({ merchantId: MERCHANT, maxItems: 4 });
      expect(last).toMatchObject({ itemCount: 1, hasMore: false });
    });

    it('is idempotent per key and keeps one collecting batch per merchant', async () => {
      await captureOrder();
      const opened = await service.openBatch({ merchantId: MERCHANT, idempotencyKey: 'eod-2026-11-15' });
      expect(opened.created).toBe(true);
      const again = await service.openBatch({ merchantId: MERCHANT, idempotencyKey: 'eod-2026-11-15' });
      expect(again).toMatchObject({ created: false, batch: { batchId: opened.batch.batchId } });
      await expect(service.openBatch({ merchantId: MERCHANT })).rejects.toMatchObject({ statusCode: 409, code: 'SETTLEMENT_IN_PROGRESS' });
      // Concurrent collectors of one batch share the run (and would be safe across processes anyway).
      const [a, b] = await Promise.all([service.collect(opened.batch.batchId), service.collect(opened.batch.batchId)]);
      expect(a).toEqual(b);
      expect(a.itemCount).toBe(1);
      await expect(service.openBatch({ merchantId: 'txp_nobody' })).rejects.toMatchObject({ statusCode: 404 });
      await expect(service.openBatch({ merchantId: MERCHANT, from: '2026-02-01', to: '2026-01-01' })).rejects.toBeInstanceOf(SettlementRequestError);
    });

    it('sweeps a long history in bounded chunks without stalling the event loop, and pages the items', async () => {
      await seedHistory(3_000, { netEach: 1_234, spacingMs: 1_000 });
      let maxLag = 0;
      let last = performance.now();
      const timer = setInterval(() => {
        const now = performance.now();
        maxLag = Math.max(maxLag, now - last - 5);
        last = now;
      }, 5);
      // Another request keeps using the ledger while the sweep runs: it gets in between chunks.
      let done = false;
      let interleaved = 0;
      const otherTraffic = (async () => {
        while (!done) {
          await walletService.getWallet('wallet_noura_sar');
          interleaved += 1;
        }
      })();
      const progress: number[] = [];
      const batch = await service.sweep({ merchantId: MERCHANT }, { chunkSize: 250, onChunk: (p) => progress.push(p.claimed) });
      done = true;
      await otherTraffic;
      clearInterval(timer);

      expect(batch).toMatchObject({ status: 'COLLECTED', itemCount: 3_000, grossHalalas: 3_000 * 1_234 });
      expect(batch.chunks).toBe(13); // 12 full chunks + the closing one
      expect(progress).toEqual(Array(12).fill(250));
      // No step blocked the process for long (one bounded chunk at most), and other requests were
      // served during the sweep instead of queueing behind it.
      expect(maxLag).toBeLessThan(2_000);
      expect(interleaved).toBeGreaterThanOrEqual(6);

      let after: string | undefined;
      let seen = 0;
      let sum = 0;
      do {
        const page = await service.listBatchItems(batch.batchId, { after, limit: 700 });
        seen += page.items.length;
        sum += page.items.reduce((s, i) => s + i.netRevenueHalalas, 0);
        after = page.next ?? undefined;
      } while (after);
      expect(seen).toBe(3_000);
      expect(sum).toBe(batch.grossHalalas);
    });

    it('resumes an interrupted sweep without claiming anything twice', async () => {
      await seedHistory(50, { netEach: 100 });
      const controller = new AbortController();
      const opened = await service.openBatch({ merchantId: MERCHANT });
      await expect(
        service.collect(opened.batch.batchId, {
          chunkSize: 10,
          signal: controller.signal,
          onChunk: (p) => {
            if (p.chunk === 2) controller.abort(new Error('process crashed'));
          },
        })
      ).rejects.toThrow('process crashed');
      const partial = await service.getBatch(opened.batch.batchId);
      expect(partial).toMatchObject({ status: 'COLLECTING', itemCount: 20 });

      const [resumed] = await service.resumeCollecting({ chunkSize: 10 });
      expect(resumed).toMatchObject({ batchId: opened.batch.batchId, status: 'COLLECTED', itemCount: 50, grossHalalas: 5_000 });
      expect(await count(`SELECT COUNT(*) AS n FROM order_settlements WHERE settlement_batch_id = $1`, [opened.batch.batchId])).toBe(50);
    });

    it('a swept entry can never be moved to another batch or un-swept (database trigger)', async () => {
      await captureOrder();
      const batch = await service.sweep({ merchantId: MERCHANT });
      await expect(query(`UPDATE order_settlements SET settlement_batch_id = NULL WHERE settlement_batch_id = $1`, [batch.batchId])).rejects.toThrow(
        /SETTLEMENT_ENTRY_ALREADY_SWEPT/
      );
    });
  });

  describe('posting the settlement journal', () => {
    it('posts ONE balanced 3-leg journal: merchant revenue DEBIT gross, platform fees CREDIT fee, payout CREDIT the rest', async () => {
      for (let i = 0; i < 3; i++) await captureOrder();
      const batch = await service.sweep({ merchantId: MERCHANT });
      const event = feeEvent(batch);
      const fee = event.payload.fee;
      const feesBefore = await balance(FEES);

      const result = await service.handleFeeCalculated(event);
      expect(result.outcome).toBe('POSTED');
      expect(await journal(result.journalRef!)).toEqual([
        [FEES, fee.totalHalalas],
        [REVENUE, -54000],
        [PAYOUT, 54000 - fee.totalHalalas],
      ]);
      expect(await balance(REVENUE)).toBe(0);
      expect(await balance(PAYOUT)).toBe(54000 - fee.totalHalalas);
      expect((await balance(FEES)) - feesBefore).toBe(fee.totalHalalas);

      const posted = await service.getBatch(batch.batchId);
      expect(posted).toMatchObject({
        status: 'POSTED',
        journalRef: `stl_${batch.batchId}`,
        feeNetHalalas: fee.netHalalas,
        feeVatHalalas: fee.vatHalalas,
        feeTotalHalalas: fee.totalHalalas,
        payoutHalalas: 54000 - fee.totalHalalas,
      });
      const out = await query<{ body: any }>(`SELECT body FROM integration_outbox WHERE event_type = $1`, [EVENT_TYPES.merchantSettlementPosted]);
      expect(out.rows).toHaveLength(1);
      expect(out.rows[0].body.payload).toMatchObject({
        batchId: batch.batchId,
        merchantName: 'Riyadh Orbit E-Commerce Co.',
        journalRef: `stl_${batch.batchId}`,
        accounts: { revenueWalletId: REVENUE, feeWalletId: FEES, payoutWalletId: PAYOUT },
      });
      expect(out.rows[0].body.payload.journal.map((l: { role: string; direction: string }) => `${l.role}:${l.direction}`)).toEqual([
        'MERCHANT_REVENUE:DEBIT',
        'PLATFORM_FEES:CREDIT',
        'MERCHANT_PAYOUT:CREDIT',
      ]);
      expect((await ledgerAuditService.runAudit({ publish: false })).ok).toBe(true);
    });

    it('never posts twice: redeliveries and re-announcements change nothing', async () => {
      await captureOrder();
      const batch = await service.sweep({ merchantId: MERCHANT });
      const event = feeEvent(batch);
      expect((await service.handleFeeCalculated(event)).outcome).toBe('POSTED');
      expect((await service.handleFeeCalculated(event)).outcome).toBe('DUPLICATE_IGNORED');
      expect((await service.handleFeeCalculated({ ...event, eventId: 'evt_fee_again' })).outcome).toBe('ALREADY_POSTED');
      await Promise.all([service.handleFeeCalculated({ ...event, eventId: 'evt_x1' }), service.handleFeeCalculated({ ...event, eventId: 'evt_x2' })]);
      expect(await count(`SELECT COUNT(DISTINCT transaction_ref) AS n FROM ledger_entries WHERE transaction_ref LIKE 'stl_%'`)).toBe(1);
      expect(await count(`SELECT COUNT(*) AS n FROM integration_outbox WHERE event_type = $1`, [EVENT_TYPES.merchantSettlementPosted])).toBe(1);
      expect((await readCheckpoint(exec, { consumer: LEDGER_SETTLEMENT_FEE_CONSUMER, eventId: 'evt_fee_again', step: 'done' }))?.outcome).toBe('ALREADY_POSTED');
    });

    it('refuses a fee priced for other totals or with broken arithmetic (dead letter, nothing moves)', async () => {
      await captureOrder();
      const batch = await service.sweep({ merchantId: MERCHANT });
      await expect(service.handleFeeCalculated(feeEvent(batch, { eventId: 'evt_bad_gross', grossHalalas: batch.grossHalalas + 1 }))).rejects.toMatchObject({
        reason: 'BATCH_MISMATCH',
      });
      await expect(service.handleFeeCalculated(feeEvent(batch, { eventId: 'evt_bad_payout', payoutHalalas: 1 }))).rejects.toMatchObject({
        reason: 'FEE_ARITHMETIC_MISMATCH',
      });
      expect(await count(`SELECT COUNT(*) AS n FROM ledger_entries WHERE transaction_ref LIKE 'stl_%'`)).toBe(0);
      // The failed deliveries left no checkpoint: a corrected event is processed normally.
      expect(await readCheckpoint(exec, { consumer: LEDGER_SETTLEMENT_FEE_CONSUMER, eventId: 'evt_bad_gross', step: 'done' })).toBeNull();
      expect((await service.handleFeeCalculated(feeEvent(batch))).outcome).toBe('POSTED');
    });

    it('cross-checks the batch against the ledger: tampered revenue records are refused', async () => {
      await captureOrder();
      await captureOrder();
      const batch = await service.sweep({ merchantId: MERCHANT });
      await query(`ALTER TABLE order_settlements DISABLE TRIGGER USER`);
      try {
        await query(`UPDATE order_settlements SET net_revenue_cents = net_revenue_cents + 1, gross_cents = gross_cents + 1 WHERE settlement_batch_id = $1`, [
          batch.batchId,
        ]);
      } finally {
        await query(`ALTER TABLE order_settlements ENABLE TRIGGER USER`);
      }
      await expect(service.handleFeeCalculated(feeEvent(batch))).rejects.toMatchObject({ reason: 'SETTLEMENT_LEDGER_MISMATCH' });
    });

    it('refuses to settle more than the merchant revenue wallet holds', async () => {
      await captureOrder();
      const batch = await service.sweep({ merchantId: MERCHANT });
      // The revenue was moved elsewhere in the meantime (e.g. a manual transfer).
      await withTransaction(async (client) => {
        await client.query(`UPDATE wallets SET balance_cents = balance_cents - 1000, version = version + 1 WHERE wallet_id = $1`, [REVENUE]);
        await client.query(
          `INSERT INTO ledger_entries (entry_id, transaction_ref, wallet_id, currency, direction, signed_amount_cents)
           VALUES ('le_drain_1', 'drain_1', $1, 'SAR', 'DEBIT', -1000), ('le_drain_2', 'drain_1', 'wallet_treasury_sar', 'SAR', 'CREDIT', 1000)`,
          [REVENUE]
        );
      });
      const err = await service.handleFeeCalculated(feeEvent(batch)).catch((e: unknown) => e);
      expect(err).toBeInstanceOf(NonRetryableError);
      expect((err as NonRetryableError).reason).toBe('INSUFFICIENT_MERCHANT_REVENUE');
      expect((await service.getBatch(batch.batchId))?.status).toBe('COLLECTED');
    });

    it('closes the batch once ZATCA reported its documents (exactly once)', async () => {
      await captureOrder();
      const batch = await service.sweep({ merchantId: MERCHANT });
      await service.handleFeeCalculated(feeEvent(batch));
      const posted = (await service.getBatch(batch.batchId)) as SettlementBatchView;
      const event = {
        eventId: 'evt_docs_1',
        eventType: EVENT_TYPES.settlementDocumentsReported,
        eventVersion: 1,
        occurredAt: new Date().toISOString(),
        producer: PRODUCERS.zatca,
        correlationId: batch.batchId,
        causationId: null,
        aggregateId: batch.batchId,
        payload: {
          batchId: batch.batchId,
          merchantId: MERCHANT,
          taxpayerId: 'txp_ksa_platform',
          journalRef: posted.journalRef as string,
          settlement: {} as never,
          documents: [],
          reportedAt: new Date().toISOString(),
        },
      } as SettlementDocumentsReportedEvent;
      expect((await service.handleDocumentsReported(event)).outcome).toBe('COMPLETED');
      expect((await service.handleDocumentsReported(event)).outcome).toBe('DUPLICATE_IGNORED');
      expect((await service.handleDocumentsReported({ ...event, eventId: 'evt_docs_2' })).outcome).toBe('ALREADY_COMPLETED');
      expect((await service.getBatch(batch.batchId))?.status).toBe('COMPLETED');
      expect((await readCheckpoint(exec, { consumer: LEDGER_SETTLEMENT_DOCUMENTS_CONSUMER, eventId: 'evt_docs_1', step: 'done' }))?.outcome).toBe('COMPLETED');
      await expect(
        service.handleDocumentsReported({ ...event, eventId: 'evt_docs_3', payload: { ...event.payload, journalRef: 'stl_other' } })
      ).rejects.toMatchObject({ reason: 'BATCH_MISMATCH' });
    });
  });

  describe('HTTP API (finance operations)', () => {
    it('opens, sweeps and reads batches; admin only', async () => {
      await captureOrder();
      await captureOrder();
      const admin = await bearer(ADMIN.sub, ADMIN.roles);
      const forbidden = await app.inject({
        method: 'POST',
        url: `/settlements/merchants/${MERCHANT}/batches`,
        headers: await bearer('user_noura', ['user']),
        payload: {},
      });
      expect(forbidden.statusCode).toBe(403);

      const res = await app.inject({ method: 'POST', url: `/settlements/merchants/${MERCHANT}/batches`, headers: admin, payload: { wait: true } });
      expect(res.statusCode).toBe(201);
      const batch = res.json() as SettlementBatchView;
      expect(batch).toMatchObject({ status: 'COLLECTED', itemCount: 2, grossHalalas: 36000 });

      const got = await app.inject({ method: 'GET', url: `/settlements/batches/${batch.batchId}`, headers: admin });
      expect(got.json().batch.batchId).toBe(batch.batchId);
      const items = await app.inject({ method: 'GET', url: `/settlements/batches/${batch.batchId}/items?limit=1`, headers: admin });
      expect(items.json().items).toHaveLength(1);
      expect(items.json().next).toBeTruthy();
      const list = await app.inject({ method: 'GET', url: `/settlements/merchants/${MERCHANT}/batches`, headers: admin });
      expect(list.json().batches.map((b: SettlementBatchView) => b.batchId)).toEqual([batch.batchId]);

      // Background sweep: 202 while collecting; the batch is complete once it finished.
      await captureOrder();
      const bg = await app.inject({ method: 'POST', url: `/settlements/merchants/${MERCHANT}/batches`, headers: { ...admin, 'idempotency-key': 'k1' }, payload: {} });
      expect(bg.statusCode).toBe(202);
      const id = (bg.json() as SettlementBatchView).batchId;
      for (let i = 0; i < 100 && (await service.getBatch(id))?.status === 'COLLECTING'; i++) await new Promise((r) => setTimeout(r, 20));
      expect(await service.getBatch(id)).toMatchObject({ status: 'COLLECTED', itemCount: 1 });
      const replay = await app.inject({ method: 'POST', url: `/settlements/merchants/${MERCHANT}/batches`, headers: { ...admin, 'idempotency-key': 'k1' }, payload: {} });
      expect(replay.statusCode).toBe(200);
      expect(replay.json().batchId).toBe(id);

      const bad = await app.inject({ method: 'POST', url: `/settlements/merchants/${MERCHANT}/batches`, headers: admin, payload: { from: 'not-a-date' } });
      expect(bad.statusCode).toBe(400);
      expect((await app.inject({ method: 'GET', url: `/settlements/batches/nope`, headers: admin })).statusCode).toBe(404);
    });
  });
});
