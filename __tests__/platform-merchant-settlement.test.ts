import { afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import type { ChannelModel, ConfirmChannel } from 'amqplib';
import { bootPlatform, warmUpPlatform, type PlatformRuntime } from '../scripts/platform';
import { MERCHANT_ID, buildCheckout, placeCreditHold, postWebhook } from './helpers/checkout';
import { CONSUMERS, EXCHANGES, ROUTING_KEYS } from '../shared/contracts';
import { resolveAmqpConnect, type DeliverySettled } from '../shared/messaging';
import { publishSigned, type RawEvent } from './helpers/events';
import { query as walletQuery } from '../services/wallet-ledger/src/db';
import { query as billingQuery } from '../services/billing-credits/src/db';
import { ledgerAuditService } from '../services/wallet-ledger/src/services/LedgerAuditService';
import { ADMIN, bearer } from '../services/wallet-ledger/__tests__/helpers/auth';
import { PLATFORM_TAXPAYER_ID } from '../services/zatca-tax/src/db';
import type { SettlementBatchView } from '../services/wallet-ledger/src/services/MerchantSettlementService';
import type { SettlementStatement } from '../services/file-cache-engine/src/integration/settlement-statements';
import type { ZatcaGatewayResponse } from '../services/zatca-tax/src/services/ReportingService';

const RETRY = { maxAttempts: 3, baseDelayMs: 20, maxDelayMs: 60, minDelayMs: 5 };
const FAST = { retry: RETRY, ttlPumpIntervalMs: 10, relayPollIntervalMs: 20, log: () => undefined, httpLogs: false };

const ok: ZatcaGatewayResponse = {
  httpStatus: 200,
  reportingStatus: 'REPORTED',
  clearanceStatus: 'CLEARED',
  validationResults: { status: 'PASS', warningMessages: [], errorMessages: [] },
};
const down: ZatcaGatewayResponse = {
  httpStatus: 503,
  reportingStatus: 'NOT_REPORTED',
  validationResults: { status: 'ERROR', warningMessages: [], errorMessages: [{ code: 'SERVICE_UNAVAILABLE', message: 'Fatoora is down' }] },
};

type SettledLog = Record<string, DeliverySettled[]>;
function recordDeliveries(platform: PlatformRuntime): SettledLog {
  const log: SettledLog = {};
  for (const [name, consumer] of Object.entries(platform.consumers)) {
    log[name] = [];
    consumer.on('settled', (e: DeliverySettled) => log[name].push(e));
  }
  return log;
}
const outcomes = (log: SettledLog, consumer: string) => log[consumer].map((e) => e.outcome);

async function brokerClient(platform: PlatformRuntime): Promise<{ ch: ConfirmChannel; close: () => Promise<void> }> {
  const conn: ChannelModel = await resolveAmqpConnect('fake')(platform.url);
  const ch = await conn.createConfirmChannel();
  ch.on('error', () => undefined);
  return {
    ch,
    close: async () => {
      await ch.waitForConfirms().catch(() => undefined);
      await conn.close().catch(() => undefined);
    },
  };
}

/** Run N paid orders through the real order flow (webhook → billing → ledger → ZATCA → PDF). */
async function sellOrders(platform: PlatformRuntime, count: number, options: { credits?: number } = {}): Promise<string[]> {
  const orderIds: string[] = [];
  for (let i = 0; i < count; i++) {
    const checkout = options.credits
      ? buildCheckout({ credits: options.credits, creditOrgId: 'org_acme' })
      : buildCheckout();
    if (options.credits) await placeCreditHold(platform, checkout.orderId, 'org_acme', options.credits);
    const res = await postWebhook(platform, checkout.body);
    expect(res.statusCode).toBe(202);
    orderIds.push(checkout.orderId);
  }
  await platform.settle();
  return orderIds;
}

async function sweepOverHttp(platform: PlatformRuntime, body: Record<string, unknown> = {}): Promise<SettlementBatchView> {
  const res = await platform.wallet.app.inject({
    method: 'POST',
    url: `/settlements/merchants/${MERCHANT_ID}/batches`,
    headers: await bearer(ADMIN.sub, ADMIN.roles),
    payload: { wait: true, ...body },
  });
  expect([200, 201]).toContain(res.statusCode);
  return res.json() as SettlementBatchView;
}

async function fetchStatement(platform: PlatformRuntime, batchId: string): Promise<SettlementStatement | undefined> {
  return (await platform.fileCache.settlements.statements.fetch(MERCHANT_ID, batchId))?.statement;
}

const balance = async (walletId: string): Promise<number> =>
  Number((await walletQuery<{ b: string }>(`SELECT balance_cents::text AS b FROM wallets WHERE wallet_id = $1`, [walletId])).rows[0]?.b ?? NaN);
const feeTreasury = async (): Promise<number> =>
  Number((await walletQuery<{ b: string }>(`SELECT COALESCE(SUM(balance_cents), 0)::text AS b FROM wallet_balance_shards WHERE wallet_id = 'wallet_fee_treasury_sar'`)).rows[0].b);

describe('merchant settlement batch over RabbitMQ (ledger sweep → billing fee → 3-leg journal → ZATCA → file cache)', () => {
  beforeAll(() => warmUpPlatform(), 180_000);

  let platform: PlatformRuntime;

  afterEach(async () => {
    await platform.shutdown();
  });

  describe('happy path', () => {
    beforeEach(async () => {
      platform = await bootPlatform(FAST);
    });

    it('settles the merchant\'s revenue end to end and every service agrees to the halala', async () => {
      await sellOrders(platform, 2); // 2 × 200.00 SAR revenue
      await sellOrders(platform, 1, { credits: 20 }); // 180.00 SAR revenue (20 credits as allowance)
      const feesBefore = await feeTreasury();
      const log = recordDeliveries(platform);

      const batch = await sweepOverHttp(platform);
      expect(batch).toMatchObject({ status: 'COLLECTED', itemCount: 3, grossHalalas: 58000, window: { from: null } });
      await platform.settle();

      const report = await platform.verifySettlementConsistency(batch.batchId);
      expect(report.checks.filter((c) => !c.ok)).toEqual([]);
      expect(report).toMatchObject({ ok: true, status: 'COMPLETED' });

      // billing: 2.50 % of 580.00 = 14.50 + 3 × 0.50 = 16.00 SAR net fee, 2.40 VAT → 18.40 SAR
      const trace = await platform.traceSettlement(batch.batchId);
      expect(trace.billing.quote?.fee).toMatchObject({ ruleId: 'std-tiered', percentageFeeHalalas: 1450, fixedFeeHalalas: 150, netHalalas: 1600, vatHalalas: 240, totalHalalas: 1840 });
      expect(trace.wallet.batch).toMatchObject({ status: 'COMPLETED', feeTotalHalalas: 1840, payoutHalalas: 56160, journalRef: `stl_${batch.batchId}` });
      // ledger: ONE balanced 3-leg settlement journal
      expect(trace.wallet.journal).toEqual([
        { walletId: 'wallet_fee_treasury_sar', currency: 'SAR', direction: 'CREDIT', signedAmountCents: 1840 },
        { walletId: 'wallet_merchant_riyadh_retail', currency: 'SAR', direction: 'DEBIT', signedAmountCents: -58000 },
        { walletId: 'wallet_payout_riyadh_retail', currency: 'SAR', direction: 'CREDIT', signedAmountCents: 56160 },
      ]);
      expect(await balance('wallet_merchant_riyadh_retail')).toBe(0);
      expect(await balance('wallet_payout_riyadh_retail')).toBe(56160);
      expect((await feeTreasury()) - feesBefore).toBe(1840);
      expect((await ledgerAuditService.runAudit({ publish: false })).ok).toBe(true);

      // ZATCA: the platform's fee invoice (its revenue) and the payout credit note, cleared, chained
      const [feeInvoice, creditNote] = trace.zatca.invoices;
      expect(feeInvoice).toMatchObject({ taxpayer_id: PLATFORM_TAXPAYER_ID, invoice_type_code: '388', invoice_subtype: '0100000', taxable_amount_halalas: 1600, vat_amount_halalas: 240, total_amount_halalas: 1840, status: 'CLEARED', icv: 1 });
      expect(creditNote).toMatchObject({ invoice_type_code: '381', total_amount_halalas: 56160, vat_amount_halalas: 0, billing_reference_id: feeInvoice.id, status: 'CLEARED', icv: 2 });

      // file cache: the statement is FINAL with the documents, kept 72 h, served over HTTP
      const statement = await fetchStatement(platform, batch.batchId);
      expect(statement).toMatchObject({
        status: 'FINAL',
        itemCount: 3,
        amounts: { gross: '580.00', feeNet: '16.00', feeVat: '2.40', feeTotal: '18.40', netPayout: '561.60' },
        journal: { transactionId: `stl_${batch.batchId}` },
      });
      expect(statement?.tax?.documents.map((d) => [d.role, d.invoiceNumber])).toEqual([
        ['FEE_INVOICE', feeInvoice.invoice_number],
        ['PAYOUT_CREDIT_NOTE', creditNote.invoice_number],
      ]);
      expect(Date.parse(statement!.expiresAt) - Date.parse(statement!.firstWrittenAt)).toBe(72 * 3600_000);

      // every step ran exactly once, each consumer on its own queue
      for (const consumer of [
        CONSUMERS.billingSettlementFees,
        CONSUMERS.ledgerSettlementFees,
        CONSUMERS.zatcaMerchantSettlements,
        CONSUMERS.ledgerSettlementDocuments,
      ]) {
        expect(outcomes(log, consumer)).toEqual(['acked']);
      }
      expect(log[CONSUMERS.fileCacheSettlementStatements].map((e) => e.result).sort()).toEqual(['STATEMENT_FINAL', 'STATEMENT_PENDING'].sort());

      // The merchant's order invoices and the platform's settlement documents are separate chains.
      expect((await platform.zatca.context.invoiceService.verifyHashChain(MERCHANT_ID)).length).toBe(3);
    });

    it('serves the statement and the documents from the file cache over HTTP, stampede-free', async () => {
      await sellOrders(platform, 1);
      const batch = await sweepOverHttp(platform);
      await platform.settle();
      const { createServer } = await import('node:http');
      const server = createServer(platform.fileCache.http);
      await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
      const port = (server.address() as { port: number }).port;
      try {
        const before = platform.fileCache.settlements.statements.stats().storeReads;
        const responses = await Promise.all(
          Array.from({ length: 100 }, () => fetch(`http://127.0.0.1:${port}/settlements/${MERCHANT_ID}/${batch.batchId}`))
        );
        expect(responses.every((r) => r.status === 200 && r.headers.get('x-settlement-status') === 'FINAL')).toBe(true);
        const bodies = await Promise.all(responses.map((r) => r.json() as Promise<SettlementStatement>));
        expect(new Set(bodies.map((b) => b.journal.transactionId))).toEqual(new Set([`stl_${batch.batchId}`]));
        expect(platform.fileCache.settlements.statements.stats().storeReads - before).toBeLessThanOrEqual(3);

        const doc = bodies[0].tax!.documents[0];
        const pdf = await fetch(`http://127.0.0.1:${port}/documents/${PLATFORM_TAXPAYER_ID}/${doc.invoiceId}.pdf`);
        expect(pdf.status).toBe(200);
        expect(Buffer.from(await pdf.arrayBuffer()).subarray(0, 5).toString('ascii')).toBe('%PDF-');
        const xml = await fetch(`http://127.0.0.1:${port}/documents/${PLATFORM_TAXPAYER_ID}/${doc.invoiceId}.xml`);
        expect(await xml.text()).toContain('<cbc:InvoiceTypeCode name="0100000">388</cbc:InvoiceTypeCode>');
      } finally {
        await new Promise((resolve) => server.close(resolve));
      }
    });

    it('settles a time window, then the rest since the beginning: every entry in exactly one batch, chain intact', async () => {
      await sellOrders(platform, 2);
      await new Promise((r) => setTimeout(r, 20));
      const cutoff = new Date().toISOString();
      await new Promise((r) => setTimeout(r, 20));
      await sellOrders(platform, 2, { credits: 10 });

      const first = await sweepOverHttp(platform, { to: cutoff });
      expect(first).toMatchObject({ itemCount: 2, grossHalalas: 40000, window: { from: null, to: cutoff } });
      await platform.settle();
      const second = await sweepOverHttp(platform);
      expect(second).toMatchObject({ itemCount: 2, grossHalalas: 2 * 19000 });
      await platform.settle();
      const empty = await sweepOverHttp(platform);
      expect(empty.status).toBe('EMPTY');
      await platform.settle();

      for (const batch of [first, second]) {
        const report = await platform.verifySettlementConsistency(batch.batchId);
        expect(report.checks.filter((c) => !c.ok)).toEqual([]);
      }
      const unswept = await walletQuery<{ n: number }>(`SELECT COUNT(*)::int AS n FROM order_settlements WHERE settlement_batch_id IS NULL`);
      expect(unswept.rows[0].n).toBe(0);
      const chain = await platform.zatca.context.invoiceService.verifyHashChain(PLATFORM_TAXPAYER_ID);
      expect(chain).toMatchObject({ ok: true, length: 4, lastIcv: 4 });
      expect((await platform.verifySettlementConsistency(empty.batchId)).ok).toBe(false); // nothing to settle: no journal, no documents
    });
  });

  describe('failures, retries and exactly-once', () => {
    it('the statement stays PENDING until ZATCA accepted the documents; a Fatoora outage is retried with jitter', async () => {
      platform = await bootPlatform({ ...FAST, retry: { maxAttempts: 5, baseDelayMs: 20, maxDelayMs: 80, minDelayMs: 5 } });
      await sellOrders(platform, 2);
      // Fatoora answers only once we let it, and then is down twice before it recovers.
      let calls = 0;
      let releaseSettlement!: () => void;
      const settlementGate = new Promise<void>((resolve) => (releaseSettlement = resolve));
      platform.zatca.context.reportingService.setTransport(async () => {
        calls += 1;
        await settlementGate;
        return calls <= 2 ? down : ok;
      });
      const log = recordDeliveries(platform);

      const batch = await sweepOverHttp(platform);
      // Wait until the ledger posted and the file cache wrote the pending statement.
      let statement: SettlementStatement | undefined;
      for (let i = 0; i < 200 && statement === undefined; i++) {
        statement = await fetchStatement(platform, batch.batchId);
        if (!statement) await new Promise((r) => setTimeout(r, 20));
      }
      expect(statement?.status).toBe('PENDING_TAX_REPORTING');
      expect(statement?.tax).toBeNull();
      expect((await platform.settlements.getBatch(batch.batchId))?.status).toBe('POSTED');

      releaseSettlement();
      await platform.settle();
      expect(outcomes(log, CONSUMERS.zatcaMerchantSettlements)).toEqual(['retry-scheduled', 'retry-scheduled', 'acked']);
      const delays = log[CONSUMERS.zatcaMerchantSettlements].filter((e) => e.outcome === 'retry-scheduled').map((e) => e.delayMs ?? -1);
      expect(delays.every((d) => d >= 5 && d <= 80)).toBe(true);
      expect((await fetchStatement(platform, batch.batchId))?.status).toBe('FINAL');
      const report = await platform.verifySettlementConsistency(batch.batchId);
      expect(report.checks.filter((c) => !c.ok)).toEqual([]);
      // The retries re-submitted; they never issued again.
      expect((await platform.zatca.context.invoiceService.getTaxpayer(PLATFORM_TAXPAYER_ID)).last_icv).toBe(2);
    });

    it('a message that cannot be processed lands in that consumer\'s DLQ; a replay re-runs only that consumer', async () => {
      platform = await bootPlatform(FAST);
      await sellOrders(platform, 2);
      await billingQuery(`UPDATE settlement_fee_rules SET active = FALSE`); // misconfiguration: no pricing rule
      const log = recordDeliveries(platform);

      const batch = await sweepOverHttp(platform);
      await platform.settle();
      expect(outcomes(log, CONSUMERS.billingSettlementFees)).toEqual(['dead-lettered']); // non-retryable: no budget burnt
      const [parked] = await platform.consumers[CONSUMERS.billingSettlementFees].inspectDeadLetters();
      expect(parked.reason).toMatch(/^NON_RETRYABLE: NO_FEE_RULE/);
      expect((await platform.settlements.getBatch(batch.batchId))?.status).toBe('COLLECTED'); // nothing posted
      expect(await fetchStatement(platform, batch.batchId)).toBeUndefined();

      await billingQuery(`UPDATE settlement_fee_rules SET active = TRUE WHERE rule_id = 'std-tiered'`);
      expect(await platform.consumers[CONSUMERS.billingSettlementFees].replayDeadLetters()).toMatchObject({ replayed: 1, remaining: 0 });
      await platform.settle();

      expect(outcomes(log, CONSUMERS.billingSettlementFees)).toEqual(['dead-lettered', 'acked']);
      for (const consumer of [CONSUMERS.ledgerSettlementFees, CONSUMERS.zatcaMerchantSettlements, CONSUMERS.ledgerSettlementDocuments]) {
        expect(outcomes(log, consumer)).toEqual(['acked']);
      }
      const report = await platform.verifySettlementConsistency(batch.batchId);
      expect(report.checks.filter((c) => !c.ok)).toEqual([]);
    });

    it('duplicate and replayed events are idempotent at every step: one journal, one fee, the same ICVs', async () => {
      platform = await bootPlatform(FAST);
      await sellOrders(platform, 2);
      const batch = await sweepOverHttp(platform);
      await platform.settle();
      const trace = await platform.traceSettlement(batch.batchId);
      const log = recordDeliveries(platform);

      // Re-announce every event of the batch exactly as its producer published it (at-least-once delivery).
      const outbox = async (query: (t: string, p?: unknown[]) => Promise<{ rows: any[] }>, type: string) =>
        (await query(`SELECT body FROM integration_outbox WHERE event_type = $1`, [type])).rows.map((r) => r.body);
      const collected = await outbox(walletQuery, 'ledger.merchant-settlement.collected');
      const priced = await outbox(billingQuery, 'billing.settlement-fee.calculated');
      const posted = await outbox(walletQuery, 'ledger.merchant-settlement.posted');
      const zatcaDb = await (await import('../services/zatca-tax/src/db')).getDb();
      const reported = (await zatcaDb.query<{ body: unknown }>(`SELECT body FROM integration_outbox WHERE event_type = 'zatca.settlement-documents.reported'`)).rows.map((r) => r.body);
      const { ch, close } = await brokerClient(platform);
      for (let i = 0; i < 2; i++) {
        // Signed by their issuers, exactly as the outbox relays put them on the broker.
        publishSigned(ch, EXCHANGES.ledger.name, ROUTING_KEYS.merchantSettlementCollected, collected[0]);
        publishSigned(ch, EXCHANGES.billing.name, ROUTING_KEYS.settlementFeeCalculated, priced[0]);
        publishSigned(ch, EXCHANGES.ledger.name, ROUTING_KEYS.merchantSettlementPosted, posted[0]);
        publishSigned(ch, EXCHANGES.zatca.name, ROUTING_KEYS.settlementDocumentsReported, reported[0] as RawEvent);
      }
      await close();
      await platform.settle();

      expect(log[CONSUMERS.billingSettlementFees].map((e) => e.result)).toEqual(['DUPLICATE_IGNORED', 'DUPLICATE_IGNORED']);
      expect(log[CONSUMERS.ledgerSettlementFees].map((e) => e.result)).toEqual(['DUPLICATE_IGNORED', 'DUPLICATE_IGNORED']);
      expect(log[CONSUMERS.zatcaMerchantSettlements].map((e) => e.result)).toEqual(['DUPLICATE_IGNORED', 'DUPLICATE_IGNORED']);
      expect(log[CONSUMERS.ledgerSettlementDocuments].map((e) => e.result)).toEqual(['DUPLICATE_IGNORED', 'DUPLICATE_IGNORED']);
      expect(log[CONSUMERS.fileCacheSettlementStatements].map((e) => e.result).sort()).toEqual(['ALREADY_FINAL', 'ALREADY_FINAL', 'ALREADY_PRESENT', 'ALREADY_PRESENT']);

      const after = await platform.traceSettlement(batch.batchId);
      expect(after.wallet.journalsForBatch).toBe(1);
      expect(after.wallet.journal).toEqual(trace.wallet.journal);
      expect(after.zatca.invoices.map((i) => [i.id, i.icv])).toEqual(trace.zatca.invoices.map((i) => [i.id, i.icv]));
      expect((await billingQuery(`SELECT COUNT(*)::int AS n FROM settlement_fee_quotes`)).rows[0].n).toBe(1);
      expect((await platform.verifySettlementConsistency(batch.batchId)).ok).toBe(true);

      // A retried HTTP request with the same idempotency key opens no second batch.
      const again = await sweepOverHttp(platform, { idempotencyKey: 'daily-1' });
      const replay = await sweepOverHttp(platform, { idempotencyKey: 'daily-1' });
      expect(replay.batchId).toBe(again.batchId);
    });
  });
});
