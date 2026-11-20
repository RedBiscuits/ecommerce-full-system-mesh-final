import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import type { Channel, ChannelModel, ConfirmChannel } from 'amqplib';
import { bootPlatform, warmUpPlatform, type PlatformRuntime } from '../scripts/platform';
import {
  DEFAULT_LINES,
  MERCHANT_ID,
  RESULT,
  buildCheckout,
  placeCreditHold,
  postWebhook,
  spendableCredits,
} from './helpers/checkout';
import { CONSUMERS, EVENT_TYPES, EXCHANGES, ROUTING_KEYS, computeOrderTotals } from '../shared/contracts';
import { resolveAmqpConnect, type DeliverySettled, retryCeiling } from '../shared/messaging';
import { type ServiceId } from '../shared/security';
import { publishSigned, publishUnsigned, type RawEvent } from './helpers/events';
import { sign } from '../services/hyperpay-gateway/src/security/webhookSignature';
import { ZATCA_GENESIS_PIH } from '../services/zatca-tax/src/db';
import { outbox as hyperpayOutbox } from '../services/hyperpay-gateway/src/services/outbox';
import { toWireEvent } from '../services/hyperpay-gateway/src/messaging/publisher';
import { startHyperPayMessaging } from '../services/hyperpay-gateway/src/messaging/runtime';
import { query as billingQuery } from '../services/billing-credits/src/db';
import { query as walletQuery } from '../services/wallet-ledger/src/db';
import { orderCreditSettlementService } from '../services/billing-credits/src/integration/OrderCreditSettlementService';
import { paymentSettlementService } from '../services/wallet-ledger/src/services/PaymentSettlementService';
import type { ZatcaGatewayResponse } from '../services/zatca-tax/src/services/ReportingService';

const RETRY = { maxAttempts: 3, baseDelayMs: 20, maxDelayMs: 60, minDelayMs: 5 };
const FAST = { retry: RETRY, ttlPumpIntervalMs: 10, relayPollIntervalMs: 20, log: () => undefined };

const ok200: ZatcaGatewayResponse = {
  httpStatus: 200,
  reportingStatus: 'REPORTED',
  validationResults: { status: 'PASS', warningMessages: [], errorMessages: [] },
};
const unavailable503: ZatcaGatewayResponse = {
  httpStatus: 503,
  reportingStatus: 'NOT_REPORTED',
  validationResults: { status: 'ERROR', warningMessages: [], errorMessages: [{ code: 'SERVICE_UNAVAILABLE', message: 'Fatoora is down' }] },
};

type SettledLog = Record<string, DeliverySettled[]>;

/** Record every delivery outcome of every integration consumer. */
function recordDeliveries(platform: PlatformRuntime): SettledLog {
  const log: SettledLog = {};
  for (const [name, consumer] of Object.entries(platform.consumers)) {
    log[name] = [];
    consumer.on('settled', (e: DeliverySettled) => log[name].push(e));
  }
  return log;
}

const outcomes = (log: SettledLog, consumer: string) => log[consumer].map((e) => e.outcome);

/** A raw client on the platform broker (to redeliver / inject messages like a misbehaving producer). */
async function brokerClient(platform: PlatformRuntime): Promise<{ ch: ConfirmChannel; close: () => Promise<void> }> {
  const conn: ChannelModel = await resolveAmqpConnect('fake')(platform.url);
  const ch = await conn.createConfirmChannel();
  ch.on('error', () => undefined);
  return {
    ch,
    close: async () => {
      await ch.waitForConfirms().catch(() => undefined); // every injected message is on the broker
      await conn.close().catch(() => undefined);
    },
  };
}

/**
 * Publish exactly as the issuing service would: signed with its stream's key (service-to-service
 * authentication is enforced, so an unsigned copy would be dead-lettered, see publishUnsigned).
 */
function publish(
  ch: Channel,
  exchange: string,
  routingKey: string,
  body: RawEvent,
  messageId?: string,
  options: { issuer?: ServiceId; content?: Buffer; eventType?: string } = {}
) {
  publishSigned(ch, exchange, routingKey, body, { messageId, ...options });
}

async function outboxBodies(query: (t: string, p?: unknown[]) => Promise<{ rows: any[] }>, eventType: string) {
  const res = await query(`SELECT body FROM integration_outbox WHERE event_type = $1 ORDER BY seq`, [eventType]);
  return res.rows.map((r) => r.body);
}

async function taxpayerLastIcv(platform: PlatformRuntime): Promise<number> {
  return (await platform.zatca.context.invoiceService.getTaxpayer(MERCHANT_ID)).last_icv;
}

describe('platform order flow over RabbitMQ (webhook → billing → ledger → ZATCA → file cache)', () => {
  // One-off cold start of the embedded databases (seconds each), outside any single test's budget.
  beforeAll(() => warmUpPlatform(), 180_000);

  let platform: PlatformRuntime;

  beforeEach(async () => {
    platform = await bootPlatform(FAST);
  });

  afterEach(async () => {
    vi.restoreAllMocks();
    await platform.shutdown();
  });

  describe('happy paths', () => {
    it('captures the credit hold, posts a balanced 3-leg journal, issues + reports the invoice and caches its PDF', async () => {
      const checkout = buildCheckout({ credits: 20, creditOrgId: 'org_acme' });
      const before = await spendableCredits(platform, 'org_acme');
      await placeCreditHold(platform, checkout.orderId, 'org_acme', 20);
      expect(await spendableCredits(platform, 'org_acme')).toBe(before - 20); // reserved

      const res = await postWebhook(platform, checkout.body);
      expect(res.statusCode).toBe(202);
      expect(res.json()).toMatchObject({ status: 'ACCEPTED', outcome: 'RECORDED', orderId: checkout.orderId, paymentStatus: 'SUCCESS' });

      await platform.settle();
      const report = await platform.verifyOrderConsistency(checkout.orderId);
      expect(report.checks.filter((c) => !c.ok)).toEqual([]);
      expect(report).toMatchObject({ ok: true, outcome: 'CAPTURED' });

      const trace = await platform.traceOrder(checkout.orderId);
      // billing: the hold is captured, the credits are spent, and become a 20.00 SAR allowance
      expect(trace.billing.settlements).toMatchObject([
        { paymentStatus: 'SUCCESS', outcome: 'CAPTURED', capturedCredits: 20, allowanceHalalas: 2000 },
      ]);
      expect(await spendableCredits(platform, 'org_acme')).toBe(before - 20);
      // ledger: 2 x 100.00 SAR − 20.00 allowance = 180.00 revenue + 27.00 VAT = 207.00 charged
      expect(trace.wallet.holds).toMatchObject([{ status: 'CAPTURED', amountCents: 20700 }]);
      expect(trace.wallet.journal).toEqual([
        { walletId: 'wallet_merchant_riyadh_retail', currency: 'SAR', direction: 'CREDIT', signedAmountCents: 18000 },
        { walletId: 'wallet_treasury_sar', currency: 'SAR', direction: 'DEBIT', signedAmountCents: -20700 },
        { walletId: 'wallet_vat_payable_sar', currency: 'SAR', direction: 'CREDIT', signedAmountCents: 2700 },
      ]);
      const merchant = await walletQuery(`SELECT balance_cents::text AS b FROM wallets WHERE wallet_id = 'wallet_merchant_riyadh_retail'`);
      expect(merchant.rows[0].b).toBe('18000');
      // ZATCA: simplified tax invoice with the credit allowance, first link of the hash chain, reported
      const [invoice] = trace.zatca.invoices;
      expect(invoice).toMatchObject({
        invoice_type_code: '388',
        invoice_subtype: '0200000',
        payment_means_code: '48',
        status: 'REPORTED',
        icv: 1,
        previous_invoice_hash: ZATCA_GENESIS_PIH,
        customer_name: 'Noura Al-Qahtani',
        subtotal_halalas: 20000,
        document_allowance_halalas: 2000,
        taxable_amount_halalas: 18000,
        vat_amount_halalas: 2700,
        total_amount_halalas: 20700,
        source_order_id: checkout.orderId,
      });
      // file cache: the rendered PDF, keyed by taxpayer + invoice, stamped with hash and ICV
      expect(trace.fileCache).toHaveLength(1);
      expect(trace.fileCache[0].attributes).toMatchObject({
        'content-type': 'application/pdf',
        'x-icv': '1',
        'x-invoice-hash': invoice.invoice_hash,
        'x-order-id': checkout.orderId,
      });
      const pdf = await platform.fileCache.pdfs.worker.get(MERCHANT_ID, invoice.id);
      expect(pdf?.pdf.subarray(0, 5).toString('ascii')).toBe('%PDF-');
      // every consumer left a checkpoint for the order; ZATCA one per step
      expect(trace.checkpoints.map((c) => `${c.consumer}:${c.step}:${c.outcome}`).sort()).toEqual([
        'billing.payment-results:done:CAPTURED',
        'ledger.order-settlements:done:CAPTURED',
        'zatca.ledger-settlements:done:REPORTED',
        'zatca.ledger-settlements:issued:ISSUED',
      ]);
    });

    it('an order without credits settles with no allowance (NONE) and the full price', async () => {
      const checkout = buildCheckout();
      expect((await postWebhook(platform, checkout.body)).statusCode).toBe(202);
      await platform.settle();
      const report = await platform.verifyOrderConsistency(checkout.orderId);
      expect(report.ok).toBe(true);
      const trace = await platform.traceOrder(checkout.orderId);
      expect(trace.billing.settlements).toMatchObject([{ outcome: 'NONE', capturedCredits: 0, allowanceHalalas: 0 }]);
      expect(trace.zatca.invoices[0]).toMatchObject({ document_allowance_halalas: 0, total_amount_halalas: 23000, vat_amount_halalas: 3000 });
    });

    it('a failed payment releases the credit hold and the ledger hold; nothing is booked, invoiced or cached', async () => {
      const checkout = buildCheckout({ credits: 30, creditOrgId: 'org_acme', result: RESULT.declined });
      const before = await spendableCredits(platform, 'org_acme');
      await placeCreditHold(platform, checkout.orderId, 'org_acme', 30);
      expect(await spendableCredits(platform, 'org_acme')).toBe(before - 30);

      expect((await postWebhook(platform, checkout.body)).statusCode).toBe(202);
      await platform.settle();

      const report = await platform.verifyOrderConsistency(checkout.orderId);
      expect(report.checks.filter((c) => !c.ok)).toEqual([]);
      expect(report).toMatchObject({ ok: true, outcome: 'RELEASED' });
      expect(await spendableCredits(platform, 'org_acme')).toBe(before); // reservation returned
      const trace = await platform.traceOrder(checkout.orderId);
      expect(trace.billing.settlements).toMatchObject([{ paymentStatus: 'FAILED', outcome: 'RELEASED', releasedCredits: 30 }]);
      expect(trace.wallet.holds).toMatchObject([{ status: 'RELEASED', amountCents: checkout.amountHalalas }]);
      expect(await outboxBodies(walletQuery, 'ledger.payment.released')).toMatchObject([
        { payload: { orderId: checkout.orderId, paymentId: checkout.paymentId, amountHalalas: checkout.amountHalalas } },
      ]);
      expect(await taxpayerLastIcv(platform)).toBe(0);
    });

    it('a customer retry after a declined card: first hold released, second payment captured, one invoice', async () => {
      const orderId = 'ord_retry_after_decline';
      const declined = buildCheckout({ orderId, credits: 10, creditOrgId: 'org_acme', result: RESULT.declined });
      await placeCreditHold(platform, orderId, 'org_acme', 10);
      await postWebhook(platform, declined.body);
      await platform.settle();

      // The storefront re-places the hold for the new attempt.
      const hold = await placeCreditHold(platform, orderId, 'org_acme', 10);
      expect(hold.created).toBe(true);
      const paid = buildCheckout({ orderId, credits: 10, creditOrgId: 'org_acme' });
      await postWebhook(platform, paid.body);
      await platform.settle();

      const report = await platform.verifyOrderConsistency(orderId);
      expect(report.checks.filter((c) => !c.ok)).toEqual([]);
      const trace = await platform.traceOrder(orderId);
      expect(trace.wallet.holds.map((h) => h.status)).toEqual(['RELEASED', 'CAPTURED']);
      expect(trace.billing.settlements.map((s) => s.outcome)).toEqual(['RELEASED', 'CAPTURED']);
      expect(trace.zatca.invoices).toHaveLength(1);
    });

    it('a PENDING notification is recorded but only the final outcome moves money', async () => {
      const pending = buildCheckout({ result: RESULT.pending });
      expect((await postWebhook(platform, pending.body)).json()).toMatchObject({ outcome: 'RECORDED', paymentStatus: 'PENDING' });
      await platform.settle();
      expect((await platform.traceOrder(pending.orderId)).billing.settlements).toEqual([]);

      const final = { ...pending.body, result: RESULT.success };
      const res = await postWebhook(platform, final);
      expect(res.statusCode).toBe(202);
      expect(res.json()).toMatchObject({ outcome: 'STATUS_UPDATED', paymentStatus: 'SUCCESS' });
      await platform.settle();
      expect((await platform.verifyOrderConsistency(pending.orderId)).ok).toBe(true);
    });

    it('keeps one gapless ICV / PIH chain across many concurrent orders', async () => {
      const checkouts = Array.from({ length: 6 }, (_, i) =>
        buildCheckout({
          lines: [{ sku: `SKU-${i}`, nameEn: `Item ${i}`, nameAr: `صنف ${i}`, quantity: 1 + i, unitPriceHalalas: 1999 + i * 137 }],
        })
      );
      const responses = await Promise.all(checkouts.map((c) => postWebhook(platform, c.body)));
      expect(responses.map((r) => r.statusCode)).toEqual([202, 202, 202, 202, 202, 202]);
      await platform.settle();

      for (const c of checkouts) {
        const report = await platform.verifyOrderConsistency(c.orderId);
        expect(report.checks.filter((x) => !x.ok)).toEqual([]);
      }
      const chain = await platform.zatca.context.invoiceService.verifyHashChain(MERCHANT_ID);
      expect(chain).toMatchObject({ ok: true, length: 6, lastIcv: 6, breaks: [] });
      const icvs = (await platform.zatca.context.invoiceService.listInvoices({ taxpayerId: MERCHANT_ID })).map((i) => i.icv).sort();
      expect(icvs).toEqual([1, 2, 3, 4, 5, 6]);
    });
  });

  describe('idempotency: no step is ever done twice', () => {
    it('duplicate webhook deliveries (sequential and concurrent) produce exactly one of everything', async () => {
      const checkout = buildCheckout({ credits: 5, creditOrgId: 'org_acme' });
      await placeCreditHold(platform, checkout.orderId, 'org_acme', 5);
      const log = recordDeliveries(platform);

      const first = await postWebhook(platform, checkout.body);
      expect(first.statusCode).toBe(202);
      const again = await postWebhook(platform, checkout.body);
      const burst = await Promise.all([1, 2, 3, 4].map(() => postWebhook(platform, checkout.body)));
      for (const res of [again, ...burst]) {
        expect(res.statusCode).toBe(200);
        expect(res.json()).toMatchObject({ status: 'DUPLICATE', eventId: first.json().eventId });
      }
      await platform.settle();

      const events = (await hyperpayOutbox.list()).filter((r) => r.eventType === 'PAYMENT_COMPLETED');
      expect(events).toHaveLength(1);
      expect(outcomes(log, CONSUMERS.billingPaymentResults)).toEqual(['acked']);
      expect(outcomes(log, CONSUMERS.ledgerOrderSettlements)).toEqual(['acked']);
      expect(outcomes(log, CONSUMERS.zatcaLedgerSettlements)).toEqual(['acked']);
      expect(outcomes(log, CONSUMERS.fileCacheInvoicePdfs)).toEqual(['acked']);
      const trace = await platform.traceOrder(checkout.orderId);
      expect(trace.hyperpay.payments).toHaveLength(1);
      expect(trace.billing.settlements).toHaveLength(1);
      expect(trace.wallet.journal).toHaveLength(3);
      expect(trace.zatca.invoices).toHaveLength(1);
      expect(await taxpayerLastIcv(platform)).toBe(1);
    });

    it('broker redeliveries of every stage event are acknowledged without effect', async () => {
      const checkout = buildCheckout({ credits: 15, creditOrgId: 'org_acme' });
      await placeCreditHold(platform, checkout.orderId, 'org_acme', 15);
      await postWebhook(platform, checkout.body);
      await platform.settle();
      const before = await platform.traceOrder(checkout.orderId);
      const log = recordDeliveries(platform);

      // Redeliver each stage's event exactly as it was published (same messageId / eventId).
      const { ch, close } = await brokerClient(platform);
      const [payment] = (await hyperpayOutbox.list()).filter((r) => r.eventType === 'PAYMENT_COMPLETED');
      const paymentWire = toWireEvent(payment);
      const [billingEvt] = await outboxBodies(billingQuery, 'billing.order-credits.settled');
      const [ledgerEvt] = await outboxBodies(walletQuery, 'ledger.payment.settled');
      const zatcaDb = await import('../services/zatca-tax/src/db').then((m) => m.getDb());
      const [zatcaEvt] = (await zatcaDb.query<{ body: any }>(`SELECT body FROM integration_outbox ORDER BY seq`)).rows.map((r) => r.body);
      for (let i = 0; i < 2; i++) {
        publish(ch, EXCHANGES.hyperpay.name, ROUTING_KEYS.paymentResult, { ...paymentWire }, paymentWire.eventId);
        publish(ch, EXCHANGES.billing.name, ROUTING_KEYS.orderCreditsSettled, billingEvt, billingEvt.eventId);
        publish(ch, EXCHANGES.ledger.name, ROUTING_KEYS.ledgerPaymentSettled, ledgerEvt, ledgerEvt.eventId);
        publish(ch, EXCHANGES.zatca.name, ROUTING_KEYS.invoiceReported, zatcaEvt, zatcaEvt.eventId);
      }
      await close();
      await platform.settle();

      expect(log[CONSUMERS.billingPaymentResults].map((e) => e.result)).toEqual(['DUPLICATE_IGNORED', 'DUPLICATE_IGNORED']);
      expect(log[CONSUMERS.ledgerOrderSettlements].map((e) => e.result)).toEqual(['DUPLICATE_IGNORED', 'DUPLICATE_IGNORED']);
      expect(log[CONSUMERS.zatcaLedgerSettlements].map((e) => e.result)).toEqual(['DUPLICATE_IGNORED', 'DUPLICATE_IGNORED']);
      expect(log[CONSUMERS.fileCacheInvoicePdfs].map((e) => e.result)).toEqual(['ALREADY_CACHED', 'ALREADY_CACHED']);

      const after = await platform.traceOrder(checkout.orderId);
      expect(after.billing.settlements).toEqual(before.billing.settlements);
      expect(after.wallet.journal).toEqual(before.wallet.journal);
      expect(after.zatca.invoices.map((i) => [i.id, i.icv, i.invoice_hash])).toEqual(
        before.zatca.invoices.map((i) => [i.id, i.icv, i.invoice_hash])
      );
      expect(after.fileCache).toEqual(before.fileCache);
      expect(await taxpayerLastIcv(platform)).toBe(1);
      expect((await platform.verifyOrderConsistency(checkout.orderId)).ok).toBe(true);
    });

    it('a re-announcement of the same facts under NEW event ids changes nothing either (business keys)', async () => {
      const checkout = buildCheckout({ credits: 8, creditOrgId: 'org_acme' });
      await placeCreditHold(platform, checkout.orderId, 'org_acme', 8);
      await postWebhook(platform, checkout.body);
      await platform.settle();
      const before = await platform.traceOrder(checkout.orderId);
      const log = recordDeliveries(platform);

      const { ch, close } = await brokerClient(platform);
      const [payment] = (await hyperpayOutbox.list()).filter((r) => r.eventType === 'PAYMENT_COMPLETED');
      const [billingEvt] = await outboxBodies(billingQuery, 'billing.order-credits.settled');
      const [ledgerEvt] = await outboxBodies(walletQuery, 'ledger.payment.settled');
      publish(ch, EXCHANGES.hyperpay.name, ROUTING_KEYS.paymentResult, { ...toWireEvent(payment), eventId: 'evt-reannounced-payment' });
      publish(ch, EXCHANGES.billing.name, ROUTING_KEYS.orderCreditsSettled, { ...billingEvt, eventId: 'evt-reannounced-billing' });
      publish(ch, EXCHANGES.ledger.name, ROUTING_KEYS.ledgerPaymentSettled, { ...ledgerEvt, eventId: 'evt-reannounced-ledger' });
      await close();
      await platform.settle();

      expect(log[CONSUMERS.billingPaymentResults].map((e) => e.result)).toEqual(['DUPLICATE_IGNORED']);
      expect(log[CONSUMERS.ledgerOrderSettlements].map((e) => e.result)).toEqual(['DUPLICATE_IGNORED']);
      // ZATCA reuses the order's invoice: no second invoice, no new ICV, same announcement id.
      expect(log[CONSUMERS.zatcaLedgerSettlements].map((e) => e.result)).toEqual(['REPORTED']);
      const after = await platform.traceOrder(checkout.orderId);
      expect(after.billing.settlements).toEqual(before.billing.settlements);
      expect(after.wallet.journal).toEqual(before.wallet.journal);
      expect(after.zatca.invoices.map((i) => [i.id, i.icv])).toEqual([[before.zatca.invoices[0].id, 1]]);
      expect(await taxpayerLastIcv(platform)).toBe(1);
      expect((await platform.verifyOrderConsistency(checkout.orderId)).ok).toBe(true);
    });
  });

  describe('failure isolation, retries with jitter, DLQ and per-consumer replay', () => {
    it('a transient Fatoora outage is retried for the ZATCA consumer only, and never burns a second ICV', async () => {
      let calls = 0;
      platform.zatca.context.reportingService.setTransport(async () => (++calls <= 2 ? unavailable503 : ok200));
      const checkout = buildCheckout();
      const log = recordDeliveries(platform);

      await postWebhook(platform, checkout.body);
      await platform.settle();

      expect(outcomes(log, CONSUMERS.zatcaLedgerSettlements)).toEqual(['retry-scheduled', 'retry-scheduled', 'acked']);
      const retries = log[CONSUMERS.zatcaLedgerSettlements].filter((e) => e.outcome === 'retry-scheduled');
      retries.forEach((e, i) => {
        expect(e.reason).toMatch(/^ZATCA_TRANSIENT_ERROR/);
        expect(e.retryQueue).toBe(`${CONSUMERS.zatcaLedgerSettlements}.retry.${i + 1}`);
        expect(e.delayMs).toBeGreaterThanOrEqual(RETRY.minDelayMs);
        expect(e.delayMs).toBeLessThanOrEqual(retryCeiling(i + 1, RETRY));
      });
      // The upstream consumers did their part exactly once.
      expect(outcomes(log, CONSUMERS.billingPaymentResults)).toEqual(['acked']);
      expect(outcomes(log, CONSUMERS.ledgerOrderSettlements)).toEqual(['acked']);
      expect(outcomes(log, CONSUMERS.fileCacheInvoicePdfs)).toEqual(['acked']);

      const trace = await platform.traceOrder(checkout.orderId);
      expect(trace.zatca.invoices.map((i) => [i.icv, i.status])).toEqual([[1, 'REPORTED']]);
      const logs = await platform.zatca.context.reportingService.listSubmissionLogs(trace.zatca.invoices[0].id);
      expect(logs.map((l) => l.outcome)).toEqual(['TRANSIENT_ERROR', 'TRANSIENT_ERROR', 'REPORTED']);
      expect(await taxpayerLastIcv(platform)).toBe(1);
      expect((await platform.verifyOrderConsistency(checkout.orderId)).ok).toBe(true);
    });

    it('exhausted retries park the message in the ZATCA DLQ; a replay finishes it for that consumer only', async () => {
      let fatooraUp = false;
      platform.zatca.context.reportingService.setTransport(async () => (fatooraUp ? ok200 : unavailable503));
      const checkout = buildCheckout({ credits: 12, creditOrgId: 'org_acme' });
      await placeCreditHold(platform, checkout.orderId, 'org_acme', 12);
      const log = recordDeliveries(platform);

      await postWebhook(platform, checkout.body);
      await platform.settle();

      const zatcaConsumer = platform.consumers[CONSUMERS.zatcaLedgerSettlements];
      expect(outcomes(log, CONSUMERS.zatcaLedgerSettlements)).toEqual(['retry-scheduled', 'retry-scheduled', 'dead-lettered']);
      const [parked] = await zatcaConsumer.inspectDeadLetters();
      expect(parked.attempt).toBe(RETRY.maxAttempts);
      expect(parked.reason).toMatch(/^RETRIES_EXHAUSTED after 3 attempt\(s\): ZATCA_TRANSIENT_ERROR/);
      expect((await zatcaConsumer.queueDepths()).deadLetter).toBe(1);

      // Billing and the ledger are done; the invoice holds its ICV, unreported; nothing cached.
      let trace = await platform.traceOrder(checkout.orderId);
      expect(trace.wallet.settlement).not.toBeNull();
      expect(trace.zatca.invoices.map((i) => [i.icv, i.status])).toEqual([[1, 'ISSUED']]);
      expect(trace.fileCache).toEqual([]);
      const report = await platform.verifyOrderConsistency(checkout.orderId);
      expect(report).toMatchObject({ ok: false, outcome: 'INCOMPLETE' });
      expect(report.checks.filter((c) => !c.ok).map((c) => c.name)).toContain('invoice reported to Fatoora');

      // Fatoora is back: replay the ZATCA DLQ. Only the ZATCA consumer (and what follows it) runs.
      fatooraUp = true;
      expect(await zatcaConsumer.replayDeadLetters()).toMatchObject({ replayed: 1, remaining: 0 });
      await platform.settle();

      expect(outcomes(log, CONSUMERS.billingPaymentResults)).toEqual(['acked']);
      expect(outcomes(log, CONSUMERS.ledgerOrderSettlements)).toEqual(['acked']);
      expect(outcomes(log, CONSUMERS.zatcaLedgerSettlements)).toEqual(['retry-scheduled', 'retry-scheduled', 'dead-lettered', 'acked']);
      expect(outcomes(log, CONSUMERS.fileCacheInvoicePdfs)).toEqual(['acked']);
      trace = await platform.traceOrder(checkout.orderId);
      expect(trace.zatca.invoices.map((i) => [i.icv, i.status])).toEqual([[1, 'REPORTED']]);
      expect(await taxpayerLastIcv(platform)).toBe(1);
      expect((await platform.verifyOrderConsistency(checkout.orderId)).ok).toBe(true);
    });

    it('a transient ledger failure is retried by the ledger consumer alone and still posts exactly one journal', async () => {
      const real = paymentSettlementService.handleOrderCreditsSettled.bind(paymentSettlementService);
      const spy = vi
        .spyOn(paymentSettlementService, 'handleOrderCreditsSettled')
        .mockImplementationOnce(async () => {
          throw new Error('could not serialize access due to concurrent update (simulated)');
        })
        .mockImplementation(real);
      const billingSpy = vi.spyOn(orderCreditSettlementService, 'handlePaymentOutcome');
      const checkout = buildCheckout({ credits: 3, creditOrgId: 'org_acme' });
      await placeCreditHold(platform, checkout.orderId, 'org_acme', 3);
      const log = recordDeliveries(platform);

      await postWebhook(platform, checkout.body);
      await platform.settle();

      expect(spy).toHaveBeenCalledTimes(2);
      expect(billingSpy).toHaveBeenCalledTimes(1);
      expect(outcomes(log, CONSUMERS.ledgerOrderSettlements)).toEqual(['retry-scheduled', 'acked']);
      const trace = await platform.traceOrder(checkout.orderId);
      expect(trace.wallet.journal).toHaveLength(3);
      expect((await platform.verifyOrderConsistency(checkout.orderId)).ok).toBe(true);
    });

    it('dead-letters poison messages immediately and keeps the flow going for everyone else', async () => {
      const log = recordDeliveries(platform);
      const { ch, close } = await brokerClient(platform);
      // Authentic (signed by their issuers) but unusable bodies.
      publish(ch, EXCHANGES.hyperpay.name, ROUTING_KEYS.paymentResult, {}, 'evt-garbage', {
        eventType: EVENT_TYPES.paymentCompleted,
        content: Buffer.from('%%% not json'),
      });
      publish(ch, EXCHANGES.billing.name, ROUTING_KEYS.orderCreditsSettled, { eventId: 'evt-bad-shape', eventType: EVENT_TYPES.orderCreditsSettled, payload: {} }, 'evt-bad-shape');
      // Not signed at all: refused by the consumer's service-to-service authentication.
      publishUnsigned(ch, EXCHANGES.hyperpay.name, ROUTING_KEYS.paymentResult, { eventId: 'evt-unsigned', eventType: EVENT_TYPES.paymentCompleted }, { messageId: 'evt-unsigned' });
      await close();

      const checkout = buildCheckout();
      await postWebhook(platform, checkout.body);
      await platform.settle();

      const billingDlq = await platform.consumers[CONSUMERS.billingPaymentResults].inspectDeadLetters();
      expect(billingDlq.map((m) => [m.messageId, m.attempt])).toEqual([
        ['evt-garbage', 1],
        ['evt-unsigned', 1],
      ]);
      expect(billingDlq[0].reason).toMatch(/^MALFORMED_MESSAGE/);
      expect(billingDlq[1].reason).toMatch(/^UNTRUSTED_EVENT: UNSIGNED_EVENT/);
      const ledgerDlq = await platform.consumers[CONSUMERS.ledgerOrderSettlements].inspectDeadLetters();
      expect(ledgerDlq.map((m) => m.messageId)).toEqual(['evt-bad-shape']);
      expect(ledgerDlq[0].reason).toMatch(/^SCHEMA_INVALID: /);
      expect(log[CONSUMERS.billingPaymentResults].filter((e) => e.outcome === 'dead-lettered')).toHaveLength(2);
      expect((await platform.verifyOrderConsistency(checkout.orderId)).ok).toBe(true);
    });

    it('dead-letters a mispriced payment at the ledger (business rule): nothing is booked or invoiced', async () => {
      // Charged the undiscounted price although 20 credits were applied at checkout.
      const fullPrice = computeOrderTotals(DEFAULT_LINES, 0).totalHalalas;
      const checkout = buildCheckout({ credits: 20, creditOrgId: 'org_acme', amountHalalas: fullPrice });
      await placeCreditHold(platform, checkout.orderId, 'org_acme', 20);
      const log = recordDeliveries(platform);

      await postWebhook(platform, checkout.body);
      await platform.settle();

      expect(outcomes(log, CONSUMERS.ledgerOrderSettlements)).toEqual(['dead-lettered']); // no retry budget burnt
      const [parked] = await platform.consumers[CONSUMERS.ledgerOrderSettlements].inspectDeadLetters();
      expect(parked.reason).toMatch(/^NON_RETRYABLE: AMOUNT_MISMATCH: Order .* paid 23000 halalas .* total 20700/);
      const trace = await platform.traceOrder(checkout.orderId);
      expect(trace.wallet.settlement).toBeNull();
      expect(trace.wallet.journal).toEqual([]);
      expect(trace.zatca.invoices).toEqual([]);
      expect(await taxpayerLastIcv(platform)).toBe(0);
      expect((await platform.verifyOrderConsistency(checkout.orderId)).ok).toBe(false);
    });

    it('a second successful payment for an already-paid order is parked for a refund, never booked twice', async () => {
      const orderId = 'ord_double_charge';
      await postWebhook(platform, buildCheckout({ orderId }).body);
      await platform.settle();
      await postWebhook(platform, buildCheckout({ orderId }).body); // a different payment id, same order
      await platform.settle();

      const [parked] = await platform.consumers[CONSUMERS.billingPaymentResults].inspectDeadLetters();
      expect(parked.reason).toMatch(/^NON_RETRYABLE: ORDER_ALREADY_CAPTURED/);
      const trace = await platform.traceOrder(orderId);
      expect(trace.hyperpay.payments).toHaveLength(2);
      expect(trace.wallet.journal).toHaveLength(3);
      expect(trace.zatca.invoices).toHaveLength(1);
      expect(await taxpayerLastIcv(platform)).toBe(1);
    });
  });

  describe('webhook ingress and outbox durability', () => {
    it('rejects unsigned, forged, tampered and stale webhooks without recording anything', async () => {
      const checkout = buildCheckout();
      const raw = JSON.stringify(checkout.body);
      const now = Math.floor(Date.now() / 1000);
      const cases = [
        { signature: null, reason: 'missing' },
        { signature: sign('not-the-secret', raw, now), reason: 'mismatch' },
        { signature: sign(platform.webhookSecret, raw.replace('"SAR"', '"USD"'), now), reason: 'mismatch' },
        { signature: sign(platform.webhookSecret, raw, now - 3600), reason: 'timestamp_out_of_tolerance' },
        { signature: 't=abc,v1=xyz', reason: 'malformed' },
      ];
      for (const c of cases) {
        const res = await postWebhook(platform, checkout.body, { raw, signature: c.signature });
        expect(res.statusCode).toBe(401);
        expect(res.json()).toEqual({ error: 'INVALID_WEBHOOK_SIGNATURE', reason: c.reason });
      }
      // Signed but not a payment notification.
      const invalid = await postWebhook(platform, { id: 'x', amount: 'lots' });
      expect(invalid.statusCode).toBe(400);
      expect(invalid.json().error).toBe('INVALID_PAYMENT_NOTIFICATION');
      expect(await hyperpayOutbox.list()).toEqual([]);
    });

    it('the signature covers the raw bytes: an equivalent re-serialisation does not verify', async () => {
      const checkout = buildCheckout();
      const raw = JSON.stringify(checkout.body, null, 2); // what the sender actually sent
      const res = await postWebhook(platform, checkout.body, { raw });
      expect(res.statusCode).toBe(202); // signed over these exact bytes
      const reordered = JSON.stringify(checkout.body); // same JSON, different bytes
      const forgedBySerialising = await postWebhook(platform, checkout.body, {
        raw: reordered,
        signature: sign(platform.webhookSecret, raw, Math.floor(Date.now() / 1000)),
      });
      expect(forgedBySerialising.statusCode).toBe(401);
    });

    it('accepts webhooks while the broker is unreachable and delivers them once it is back (no dual write)', async () => {
      await platform.hyperpay.messaging.stop();
      const checkouts = [buildCheckout(), buildCheckout()];
      for (const c of checkouts) expect((await postWebhook(platform, c.body)).statusCode).toBe(202);
      expect((await hyperpayOutbox.stats()).pending).toBe(2);

      const { resolveAmqpConnect: connectFor } = await import('../shared/messaging');
      const revived = await startHyperPayMessaging({
        url: platform.url,
        connect: connectFor('fake'),
        outbox: { pollIntervalMs: 20 },
      });
      try {
        await revived.flush();
        await platform.settle();
        for (const c of checkouts) expect((await platform.verifyOrderConsistency(c.orderId)).ok).toBe(true);
      } finally {
        await revived.stop();
      }
    });
  });
});
