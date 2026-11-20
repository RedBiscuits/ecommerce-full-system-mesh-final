import { afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import type { ChannelModel, ConfirmChannel } from 'amqplib';
import { bootPlatform, warmUpPlatform, type PlatformRuntime } from '../scripts/platform';
import { diagnoseIntegration } from '../scripts/cluster-doctor';
import { MERCHANT_ID, buildCheckout, placeCreditHold, postWebhook, spendableCredits } from './helpers/checkout';
import { publishSigned } from './helpers/events';
import { CONSUMERS, EXCHANGES, ROUTING_KEYS } from '../shared/contracts';
import { resolveAmqpConnect, type DeliverySettled } from '../shared/messaging';
import { query as walletQuery } from '../services/wallet-ledger/src/db';
import { ledgerAuditService } from '../services/wallet-ledger/src/services/LedgerAuditService';
import { outbox as hyperpayOutbox } from '../services/hyperpay-gateway/src/services/outbox';
import { toWireEvent } from '../services/hyperpay-gateway/src/messaging/publisher';
import { ADMIN, bearer } from '../services/wallet-ledger/__tests__/helpers/auth';
import type { ZatcaGatewayResponse } from '../services/zatca-tax/src/services/ReportingService';

const RETRY = { maxAttempts: 3, baseDelayMs: 20, maxDelayMs: 60, minDelayMs: 5 };
const FAST = { retry: RETRY, ttlPumpIntervalMs: 10, relayPollIntervalMs: 20, log: () => undefined, httpLogs: false };
const REFUND_CONSUMERS = [CONSUMERS.billingRefunds, CONSUMERS.ledgerOrderRefunds, CONSUMERS.zatcaLedgerRefunds, CONSUMERS.fileCacheCreditNotes];

type SettledLog = Record<string, DeliverySettled[]>;
function recordDeliveries(platform: PlatformRuntime): SettledLog {
  const log: SettledLog = {};
  for (const [name, consumer] of Object.entries(platform.consumers)) {
    log[name] = [];
    consumer.on('settled', (e: DeliverySettled) => log[name].push(e));
  }
  return log;
}

/** An OPPWA refund (RF) notification for a captured payment. */
function refundNotification(paymentId: string, refundId: string, amount: string, overrides: Record<string, unknown> = {}) {
  return {
    id: refundId,
    resourcePath: `/v1/payments/${paymentId}/refunds/${refundId}`,
    paymentType: 'RF',
    referencedId: paymentId,
    amount,
    currency: 'SAR',
    result: { code: '000.100.110', description: 'ok' },
    reason: 'damaged item',
    ...overrides,
  };
}

const balance = async (walletId: string): Promise<number> =>
  Number((await walletQuery<{ b: string }>(`SELECT balance_cents::text AS b FROM wallets WHERE wallet_id = $1`, [walletId])).rows[0]?.b ?? NaN);
const sharded = async (walletId: string): Promise<number> =>
  Number((await walletQuery<{ b: string }>(`SELECT COALESCE(SUM(balance_cents), 0)::text AS b FROM wallet_balance_shards WHERE wallet_id = $1`, [walletId])).rows[0].b);

describe('customer refunds over the signed queues (gateway → billing → ledger → ZATCA → file cache)', () => {
  beforeAll(() => warmUpPlatform(), 180_000);

  let platform: PlatformRuntime;

  beforeEach(async () => {
    platform = await bootPlatform(FAST);
  });

  afterEach(async () => {
    await platform.shutdown();
  });

  it('a partial refund then the final one: credits restored pro rata, reversals exact, credit notes chained, PDFs refreshed', async () => {
    // 2 × 100.00 SAR, 20 credits → 180.00 revenue + 27.00 VAT = 207.00 paid.
    const checkout = buildCheckout({ credits: 20, creditOrgId: 'org_acme' });
    await placeCreditHold(platform, checkout.orderId, 'org_acme', 20);
    await postWebhook(platform, checkout.body);
    await platform.settle();
    const creditsBefore = await spendableCredits(platform, 'org_acme');
    const vatBefore = await sharded('wallet_vat_payable_sar');
    const clearingBefore = await sharded('wallet_treasury_sar');
    const pdfBefore = await platform.fileCache.pdfs.worker.get(MERCHANT_ID, (await platform.traceOrder(checkout.orderId)).zatca.invoices[0].id);
    const log = recordDeliveries(platform);

    // 50.00 SAR back to the customer.
    const partial = await postWebhook(platform, refundNotification(checkout.paymentId, 'rf_partial', '50.00'));
    expect(partial.statusCode).toBe(202);
    expect(partial.json()).toMatchObject({ status: 'ACCEPTED', kind: 'REFUND', outcome: 'RECORDED', amountHalalas: 5000, refundableHalalas: 15700 });
    await platform.settle();

    let report = await platform.verifyOrderConsistency(checkout.orderId);
    expect(report.checks.filter((c) => !c.ok)).toEqual([]);
    let trace = await platform.traceOrder(checkout.orderId);
    expect(trace.refunds.wallet.refunds).toMatchObject([{ refundId: 'rf_partial', amountHalalas: 5000, netHalalas: 4348, vatHalalas: 652, merchantAccount: 'REVENUE', journalRef: 'rfd_rf_partial' }]);
    expect(trace.refunds.billing.refunds).toMatchObject([{ restoredCredits: 4, cumulativeRestoredCredits: 4 }]);
    expect(await spendableCredits(platform, 'org_acme')).toBe(creditsBefore + 4);
    expect(await balance('wallet_merchant_riyadh_retail')).toBe(18000 - 4348);
    expect((await sharded('wallet_vat_payable_sar')) - vatBefore).toBe(-652);
    expect((await sharded('wallet_treasury_sar')) - clearingBefore).toBe(5000);
    expect(trace.refunds.zatca.creditNotes.map((n) => [n.invoice_type_code, n.icv, n.total_amount_halalas, n.status])).toEqual([['381', 2, 5000, 'REPORTED']]);
    // The invoice's cached PDF was invalidated and re-rendered with the credit applied.
    const pdfAfter = await platform.fileCache.pdfs.worker.get(MERCHANT_ID, trace.zatca.invoices[0].id);
    expect(pdfAfter?.metadata.attributes['x-credited-halalas']).toBe('5000');
    expect(pdfAfter?.metadata.checksum).not.toBe(pdfBefore?.metadata.checksum);
    for (const consumer of REFUND_CONSUMERS) expect(log[consumer].map((e) => e.outcome)).toEqual(['acked']);

    // The rest: 157.00 SAR. Everything is reversed exactly, all 20 credits are back.
    const final = await postWebhook(platform, refundNotification(checkout.paymentId, 'rf_final', '157.00'));
    expect(final.json()).toMatchObject({ outcome: 'RECORDED', refundedTotalHalalas: 20700, refundableHalalas: 0 });
    await platform.settle();
    report = await platform.verifyOrderConsistency(checkout.orderId);
    expect(report.checks.filter((c) => !c.ok)).toEqual([]);
    trace = await platform.traceOrder(checkout.orderId);
    expect(trace.refunds.wallet.totals).toEqual({ grossHalalas: 20700, refundedGrossHalalas: 20700, refundedNetHalalas: 18000, refundedVatHalalas: 2700 });
    expect(await balance('wallet_merchant_riyadh_retail')).toBe(0);
    expect((await sharded('wallet_vat_payable_sar')) - vatBefore).toBe(-2700);
    expect(await spendableCredits(platform, 'org_acme')).toBe(creditsBefore + 20);
    expect(trace.refunds.zatca.creditNotes.map((n) => n.icv)).toEqual([2, 3]);
    expect((await platform.zatca.context.invoiceService.verifyHashChain(MERCHANT_ID))).toMatchObject({ ok: true, length: 3 });
    expect((await ledgerAuditService.runAudit({ publish: false })).ok).toBe(true);

    // Nothing more can be refunded; the gateway refuses before anything is recorded or announced.
    const over = await postWebhook(platform, refundNotification(checkout.paymentId, 'rf_over', '0.01'));
    expect(over.statusCode).toBe(422);
    expect(over.json().error).toBe('REFUND_EXCEEDS_REFUNDABLE');

    const health = await diagnoseIntegration(platform);
    expect(health.refunds).toEqual({ refunds: 2, orders: 1, violations: [], awaitingCreditNotes: 0 });
    expect(health.problems).toEqual([]);
  });

  it('repeated requests and redelivered events are idempotent at every service', async () => {
    const checkout = buildCheckout({ credits: 10, creditOrgId: 'org_acme' });
    await placeCreditHold(platform, checkout.orderId, 'org_acme', 10);
    await postWebhook(platform, checkout.body);
    await platform.settle();
    const notification = refundNotification(checkout.paymentId, 'rf_dup', '30.00');
    const first = await postWebhook(platform, notification);
    expect(first.statusCode).toBe(202);
    await platform.settle();
    const before = await platform.traceOrder(checkout.orderId);
    const log = recordDeliveries(platform);

    // 1. the gateway gets the same webhook again (OPPWA retry)
    const retry = await postWebhook(platform, notification);
    expect(retry.statusCode).toBe(200);
    expect(retry.json()).toMatchObject({ status: 'DUPLICATE', eventId: first.json().eventId });
    // 2. the broker redelivers every stage event (same ids), signed as the producers sign them
    const conn: ChannelModel = await resolveAmqpConnect('fake')(platform.url);
    const ch: ConfirmChannel = await conn.createConfirmChannel();
    const [refundEvent] = (await hyperpayOutbox.list()).filter((r) => r.eventType === 'REFUND_PROCESSED');
    const { query: billingQuery } = await import('../services/billing-credits/src/db');
    const restored = (await billingQuery<{ body: any }>(`SELECT body FROM integration_outbox WHERE event_type = 'billing.order-credits.restored'`)).rows[0].body;
    const refunded = (await walletQuery<{ body: any }>(`SELECT body FROM integration_outbox WHERE event_type = 'ledger.payment.refunded'`)).rows[0].body;
    const zatcaDb = await (await import('../services/zatca-tax/src/db')).getDb();
    const creditNote = (await zatcaDb.query<{ body: any }>(`SELECT body FROM integration_outbox WHERE event_type = 'zatca.credit-note.reported'`)).rows[0].body;
    for (let i = 0; i < 2; i++) {
      publishSigned(ch, EXCHANGES.hyperpay.name, ROUTING_KEYS.refundProcessed, { ...toWireEvent(refundEvent) });
      publishSigned(ch, EXCHANGES.billing.name, ROUTING_KEYS.orderCreditsRestored, restored);
      publishSigned(ch, EXCHANGES.ledger.name, ROUTING_KEYS.ledgerPaymentRefunded, refunded);
      publishSigned(ch, EXCHANGES.zatca.name, ROUTING_KEYS.creditNoteReported, creditNote);
    }
    await ch.waitForConfirms();
    await conn.close();
    await platform.settle();

    expect(log[CONSUMERS.billingRefunds].map((e) => e.result)).toEqual(['DUPLICATE_IGNORED', 'DUPLICATE_IGNORED']);
    expect(log[CONSUMERS.ledgerOrderRefunds].map((e) => e.result)).toEqual(['DUPLICATE_IGNORED', 'DUPLICATE_IGNORED']);
    expect(log[CONSUMERS.zatcaLedgerRefunds].map((e) => e.result)).toEqual(['DUPLICATE_IGNORED', 'DUPLICATE_IGNORED']);
    expect(log[CONSUMERS.fileCacheCreditNotes].map((e) => e.result)).toEqual(['ALREADY_CACHED', 'ALREADY_CACHED']);
    const after = await platform.traceOrder(checkout.orderId);
    expect(after.refunds.wallet).toEqual(before.refunds.wallet);
    expect(after.refunds.billing.refunds).toEqual(before.refunds.billing.refunds);
    expect(after.refunds.zatca.creditNotes.map((n) => [n.id, n.icv])).toEqual(before.refunds.zatca.creditNotes.map((n) => [n.id, n.icv]));
    expect((await platform.verifyOrderConsistency(checkout.orderId)).ok).toBe(true);
  });

  it('a refund after the merchant was settled comes out of the payout wallet; the settlement stays consistent', async () => {
    const checkout = buildCheckout(); // 200.00 revenue + 30.00 VAT
    await postWebhook(platform, checkout.body);
    await platform.settle();
    const sweep = await platform.wallet.app.inject({
      method: 'POST',
      url: `/settlements/merchants/${MERCHANT_ID}/batches`,
      headers: await bearer(ADMIN.sub, ADMIN.roles),
      payload: { wait: true },
    });
    const batch = sweep.json();
    await platform.settle();
    expect((await platform.verifySettlementConsistency(batch.batchId)).ok).toBe(true);
    const payoutBefore = await balance('wallet_payout_riyadh_retail');

    await postWebhook(platform, refundNotification(checkout.paymentId, 'rf_after_settlement', '115.00'));
    await platform.settle();
    const trace = await platform.traceOrder(checkout.orderId);
    expect(trace.refunds.wallet.refunds).toMatchObject([{ merchantAccount: 'PAYOUT', netHalalas: 10000, vatHalalas: 1500, amountHalalas: 11500 }]);
    expect(await balance('wallet_payout_riyadh_retail')).toBe(payoutBefore - 10000);
    expect((await platform.verifyOrderConsistency(checkout.orderId)).checks.filter((c) => !c.ok)).toEqual([]);
    // The batch that was posted is untouched and still reconciles.
    expect((await platform.verifySettlementConsistency(batch.batchId)).checks.filter((c) => !c.ok)).toEqual([]);
    expect((await diagnoseIntegration(platform)).refunds.violations).toEqual([]);

    // A refund before the next sweep reduces what that sweep claims.
    const second = buildCheckout();
    await postWebhook(platform, second.body);
    await platform.settle();
    await postWebhook(platform, refundNotification(second.paymentId, 'rf_before_sweep', '23.00'));
    await platform.settle();
    const next = await platform.wallet.app.inject({
      method: 'POST',
      url: `/settlements/merchants/${MERCHANT_ID}/batches`,
      headers: await bearer(ADMIN.sub, ADMIN.roles),
      payload: { wait: true },
    });
    expect(next.json()).toMatchObject({ itemCount: 1, grossHalalas: 20000 - 2000 });
    await platform.settle();
    expect((await platform.verifySettlementConsistency(next.json().batchId)).checks.filter((c) => !c.ok)).toEqual([]);
    expect((await ledgerAuditService.runAudit({ publish: false })).ok).toBe(true);
  });

  it('a rejected credit note parks at ZATCA only: money and credits were already returned, the doctor shows the refund awaiting its document', async () => {
    const rejected: ZatcaGatewayResponse = { httpStatus: 400, reportingStatus: 'NOT_REPORTED', validationResults: { status: 'ERROR', warningMessages: [], errorMessages: [{ code: 'BR-KSA-99', message: 'rejected' }] } };
    const checkout = buildCheckout({ credits: 5, creditOrgId: 'org_acme' });
    await placeCreditHold(platform, checkout.orderId, 'org_acme', 5);
    await postWebhook(platform, checkout.body);
    await platform.settle();
    platform.zatca.context.reportingService.setTransport(async () => rejected);
    const log = recordDeliveries(platform);

    await postWebhook(platform, refundNotification(checkout.paymentId, 'rf_rejected', '20.00'));
    await platform.settle();
    expect(log[CONSUMERS.billingRefunds].map((e) => e.outcome)).toEqual(['acked']);
    expect(log[CONSUMERS.ledgerOrderRefunds].map((e) => e.outcome)).toEqual(['acked']);
    expect(log[CONSUMERS.zatcaLedgerRefunds].map((e) => e.outcome)).toEqual(['dead-lettered']);
    expect(log[CONSUMERS.fileCacheCreditNotes]).toEqual([]);
    const [parked] = await platform.consumers[CONSUMERS.zatcaLedgerRefunds].inspectDeadLetters();
    expect(parked.reason).toMatch(/^NON_RETRYABLE: ZATCA_REJECTED/);
    const health = await diagnoseIntegration(platform);
    expect(health.refunds).toMatchObject({ refunds: 1, awaitingCreditNotes: 1, violations: [] });
    expect(health.problems).toEqual([`${CONSUMERS.zatcaLedgerRefunds}: 1 message(s) parked in ${CONSUMERS.zatcaLedgerRefunds}.dlq`]);
    const trace = await platform.traceOrder(checkout.orderId);
    expect(trace.refunds.wallet.totals?.refundedGrossHalalas).toBe(2000);
    expect(trace.refunds.zatca.creditNotes.map((n) => n.status)).toEqual(['REJECTED']);
  });
});
