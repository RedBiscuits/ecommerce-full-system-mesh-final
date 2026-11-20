import { spawnSync } from 'node:child_process';
import { afterEach, beforeAll, describe, expect, it } from 'vitest';
import { bootPlatform, warmUpPlatform, type PlatformOptions, type PlatformRuntime } from '../scripts/platform';
import { MERCHANT_ID, RESULT, buildCheckout, postWebhook, spendableCredits } from './helpers/checkout';
import { computeOrderTotals, halalasToSar, type OrderLine } from '../shared/contracts';
import { query as walletQuery } from '../services/wallet-ledger/src/db';
import { walletService } from '../services/wallet-ledger/src/services/WalletService';
import { ledgerAuditService } from '../services/wallet-ledger/src/services/LedgerAuditService';
import { query as billingQuery } from '../services/billing-credits/src/db';
import type { CheckoutView } from '../services/checkout-coordinator/src/services/CheckoutCoordinator';
import type { HttpTransport } from '../services/checkout-coordinator/src/clients/http';

const FAST: PlatformOptions = {
  retry: { maxAttempts: 3, baseDelayMs: 10, maxDelayMs: 30, minDelayMs: 2 },
  ttlPumpIntervalMs: 10,
  relayPollIntervalMs: 20,
  checkout: { retry: { maxAttempts: 3, baseDelayMs: 1, maxDelayMs: 5, minDelayMs: 0 } },
  log: () => undefined,
};

/** S 15 % + Z 0 % + E 0 % (VATEX-SA-29). */
const MIXED: OrderLine[] = [
  { sku: 'RTR-AX6000', nameEn: 'Wi-Fi 6 Router', nameAr: 'موجه واي فاي 6', quantity: 1, unitPriceHalalas: 10000, vatCategory: 'S' },
  { sku: 'EXP-LIC', nameEn: 'Export licence', nameAr: 'ترخيص تصدير', quantity: 1, unitPriceHalalas: 10000, vatCategory: 'Z', vatExemptionCode: 'VATEX-SA-32' },
  { sku: 'FIN-FEE', nameEn: 'Financing fee', nameAr: 'رسوم تمويل', quantity: 1, unitPriceHalalas: 5000, vatCategory: 'E', vatExemptionCode: 'VATEX-SA-29' },
];

const NOURA = 'wallet_noura_sar';
let seq = 0;

async function balance(walletId: string): Promise<number> {
  return (await walletService.getWallet(walletId))?.balanceCents ?? NaN;
}

describe('split-tender checkout: promotional credits + wallet / card remainder', () => {
  // One-off cold start of the embedded databases (seconds each), outside any single test's budget.
  beforeAll(() => warmUpPlatform(), 180_000);

  let platform: PlatformRuntime;

  async function boot(options: PlatformOptions = {}) {
    platform = await bootPlatform({ ...FAST, ...options });
  }

  afterEach(async () => {
    await platform?.shutdown();
  });

  function checkoutRequest(overrides: Record<string, unknown> = {}) {
    const n = ++seq;
    return {
      checkoutId: `co_split_${n}`,
      orderId: `ord_split_${n}`,
      customer: { name: 'Noura Al-Qahtani', email: 'noura@example.sa' },
      lines: MIXED,
      promotion: { orgId: 'org_acme', credits: 25 },
      remainder: { tender: 'WALLET', walletId: NOURA },
      ...overrides,
    };
  }

  async function startCheckout(body: Record<string, unknown>) {
    const res = await platform.checkout.context.app.inject({ method: 'POST', url: '/checkouts', payload: body });
    return { status: res.statusCode, checkout: res.json() as CheckoutView };
  }

  it('WALLET remainder: reserves both, confirms, captures both exactly once and invoices the promotion per VAT category', async () => {
    await boot();
    const credits = await spendableCredits(platform, 'org_acme');
    const body = checkoutRequest();

    const { status, checkout } = await startCheckout(body);
    expect(status).toBe(201);
    // 100.00 S + 100.00 Z + 50.00 E − 25.00 promotion (10 / 10 / 5) + 13.50 VAT on the S part.
    expect(checkout).toMatchObject({ status: 'RESERVED', payment: { tender: 'WALLET', amountHalalas: 23850, amountSar: '238.50' } });
    expect(await spendableCredits(platform, 'org_acme')).toBe(credits - 25);
    expect(await balance(NOURA)).toBe(50000 - 23850);
    expect(await balance('wallet_escrow_sar')).toBe(23850);

    const confirm = await platform.checkout.context.app.inject({ method: 'POST', url: `/checkouts/${checkout.checkoutId}/confirm` });
    expect(confirm.statusCode).toBe(202);
    await platform.settle();

    const report = await platform.verifyOrderConsistency(checkout.orderId);
    expect(report.checks.filter((c) => !c.ok)).toEqual([]);
    expect(report).toMatchObject({ ok: true, outcome: 'CAPTURED' });
    const trace = await platform.traceOrder(checkout.orderId);
    expect(trace.checkouts).toMatchObject([{ status: 'COMPLETED', holds: { credits: { status: 'CAPTURED' }, remainder: { status: 'CAPTURED' } } }]);
    expect(trace.billing.settlements).toMatchObject([{ outcome: 'CAPTURED', capturedCredits: 25, allowanceHalalas: 2500 }]);
    expect(trace.wallet.journal).toEqual([
      { walletId: 'wallet_escrow_sar', currency: 'SAR', direction: 'DEBIT', signedAmountCents: -23850 },
      { walletId: 'wallet_merchant_riyadh_retail', currency: 'SAR', direction: 'CREDIT', signedAmountCents: 22500 },
      { walletId: 'wallet_vat_payable_sar', currency: 'SAR', direction: 'CREDIT', signedAmountCents: 1350 },
    ]);
    expect(await balance('wallet_escrow_sar')).toBe(0);
    expect(await balance(NOURA)).toBe(50000 - 23850); // debited exactly once
    expect(await spendableCredits(platform, 'org_acme')).toBe(credits - 25);

    const [invoice] = trace.zatca.invoices;
    expect(invoice).toMatchObject({
      status: 'REPORTED',
      payment_means_code: '30',
      document_allowance_halalas: 2500,
      document_allowance_reason: 'Promotional credit (25 credits)',
      taxable_amount_halalas: 22500,
      vat_amount_halalas: 1350,
      total_amount_halalas: 23850,
    });
    expect(invoice.lines.map((l) => [l.vat_category, l.vat_rate_bps, l.vat_exemption_code])).toEqual([
      ['S', 1500, null],
      ['Z', 0, 'VATEX-SA-32'],
      ['E', 0, 'VATEX-SA-29'],
    ]);
    const xml = Buffer.from(invoice.xml_ubl_base64!, 'base64').toString('utf8');
    expect(xml.match(/<cbc:AllowanceChargeReason>Promotional credit \(25 credits\)<\/cbc:AllowanceChargeReason>/g)).toHaveLength(3);
    expect(xml).toContain('<cbc:TaxExemptionReasonCode>VATEX-SA-29</cbc:TaxExemptionReasonCode>');

    const pdf = await platform.fileCache.pdfs.worker.get(MERCHANT_ID, invoice.id);
    expect(pdf?.pdf.subarray(0, 5).toString('ascii')).toBe('%PDF-');
    if (spawnSync('pdftotext', ['-v']).error === undefined) {
      const text = spawnSync('pdftotext', ['-', '-'], { input: pdf!.pdf }).stdout.toString();
      expect(text).toContain('Allowance – Promotional credit (25 credits): -25.00 SAR');
      expect(text).toContain('E 0.00% VATEX-SA-29 | Net: 50.00 | Allowance: -5.00 | Taxable: 45.00 | VAT: 0.00');
      expect(text).toContain('Grand Total (Incl. VAT): 238.50 SAR');
    }
    expect((await ledgerAuditService.runAudit({ publish: false })).ok).toBe(true);
  });

  it('CARD remainder: the gateway payment that names the checkout captures its own authorization', async () => {
    await boot();
    const body = checkoutRequest({ remainder: { tender: 'CARD' } });
    const { checkout } = await startCheckout(body);
    expect(checkout).toMatchObject({ status: 'RESERVED', payment: { tender: 'CARD', amountHalalas: 23850 } });
    expect(await balance(NOURA)).toBe(50000); // an authorization moves no wallet money

    const payment = buildCheckout({ orderId: checkout.orderId, lines: MIXED, credits: 25, creditOrgId: 'org_acme' });
    (payment.body.customParameters as Record<string, string>).checkoutId = checkout.checkoutId;
    expect(payment.amountHalalas).toBe(23850);
    expect((await postWebhook(platform, payment.body)).statusCode).toBe(202);
    await platform.settle();

    const report = await platform.verifyOrderConsistency(checkout.orderId);
    expect(report.checks.filter((c) => !c.ok)).toEqual([]);
    const trace = await platform.traceOrder(checkout.orderId);
    expect(trace.checkouts[0]).toMatchObject({ status: 'COMPLETED', payment: { paymentId: payment.paymentId } });
    expect(trace.wallet.holds).toMatchObject([{ holdKey: `checkout:${checkout.checkoutId}:remainder`, tender: 'CARD', status: 'CAPTURED', paymentId: payment.paymentId }]);
    expect(trace.zatca.invoices[0]).toMatchObject({ payment_means_code: '48', document_allowance_halalas: 2500, total_amount_halalas: 23850 });
  });

  it('CARD declined: billing and the ledger release both reservations, the checkout fails, nothing is orphaned', async () => {
    await boot();
    const credits = await spendableCredits(platform, 'org_acme');
    const { checkout } = await startCheckout(checkoutRequest({ remainder: { tender: 'CARD' } }));
    const declined = buildCheckout({ orderId: checkout.orderId, lines: MIXED, credits: 25, creditOrgId: 'org_acme', result: RESULT.declined });
    (declined.body.customParameters as Record<string, string>).checkoutId = checkout.checkoutId;
    await postWebhook(platform, declined.body);
    await platform.settle();

    const report = await platform.verifyOrderConsistency(checkout.orderId);
    expect(report.checks.filter((c) => !c.ok)).toEqual([]);
    expect(report.outcome).toBe('RELEASED');
    expect((await platform.checkout.context.coordinator.getCheckout(checkout.checkoutId))?.status).toBe('PAYMENT_FAILED');
    expect(await spendableCredits(platform, 'org_acme')).toBe(credits);
  });

  it('insufficient wallet balance: the credit hold fails for the same reason and no reservation survives', async () => {
    await boot();
    const credits = await spendableCredits(platform, 'org_acme');
    const expensive: OrderLine[] = [{ ...MIXED[0], unitPriceHalalas: 60000 }];
    const { status, checkout } = await startCheckout(checkoutRequest({ lines: expensive }));

    expect(status).toBe(422);
    expect(checkout).toMatchObject({
      status: 'FAILED',
      failure: { code: 'INSUFFICIENT_FUNDS' },
      holds: { credits: { status: 'RELEASED' }, remainder: { status: 'RELEASED' } },
    });
    expect(await spendableCredits(platform, 'org_acme')).toBe(credits);
    expect(await balance(NOURA)).toBe(50000);
    const creditHold = await billingQuery(`SELECT h.status, k.status AS key_status, k.reason FROM credit_holds h JOIN order_credit_hold_keys k ON k.hold_id = h.hold_id WHERE k.hold_key = $1`, [
      checkout.holds.credits.key,
    ]);
    expect(creditHold.rows[0]).toMatchObject({ status: 'RELEASED', key_status: 'CANCELLED' });
    expect(creditHold.rows[0].reason).toContain('INSUFFICIENT_FUNDS');
    const report = await platform.verifyOrderConsistency(checkout.orderId);
    expect(report.checks.filter((c) => !c.ok)).toEqual([]);
    expect(report.outcome).toBe('RELEASED');
  });

  it('idempotent retries: the same checkout posted 6 times concurrently reserves credits and funds once', async () => {
    await boot();
    const credits = await spendableCredits(platform, 'org_acme');
    const body = checkoutRequest();
    const responses = await Promise.all(Array.from({ length: 6 }, () => startCheckout(body)));
    expect(responses.filter((r) => r.status === 201)).toHaveLength(1);
    expect(responses.filter((r) => r.status === 200)).toHaveLength(5);
    expect(new Set(responses.map((r) => r.checkout.holds.remainder.holdId)).size).toBe(1);
    expect(await spendableCredits(platform, 'org_acme')).toBe(credits - 25);
    expect(await balance(NOURA)).toBe(50000 - 23850);

    // Confirmed twice, redelivered pipeline events: still one capture.
    const { checkoutId, orderId } = responses[0].checkout;
    await platform.checkout.context.app.inject({ method: 'POST', url: `/checkouts/${checkoutId}/confirm` });
    await platform.checkout.context.app.inject({ method: 'POST', url: `/checkouts/${checkoutId}/confirm` });
    await platform.settle();
    expect((await platform.verifyOrderConsistency(orderId)).ok).toBe(true);
    expect(await balance(NOURA)).toBe(50000 - 23850);
  });

  it('a wallet call that times out after committing is retried with its key: one hold, one debit', async () => {
    let timeouts = 1;
    await boot({
      checkoutTransports: {
        wallet: (base: HttpTransport): HttpTransport => async (req) => {
          const res = await base(req); // the ledger committed the hold…
          if (req.path === '/payments/holds' && timeouts-- > 0) throw new Error('socket timeout'); // …but the answer is lost
          return res;
        },
      },
    });
    const { status, checkout } = await startCheckout(checkoutRequest());
    expect(status).toBe(201);
    expect(checkout.status).toBe('RESERVED');
    expect(await balance(NOURA)).toBe(50000 - 23850);
    const held = await walletQuery(`SELECT COUNT(*)::int AS n FROM payment_holds WHERE order_id = $1`, [checkout.orderId]);
    expect(held.rows[0].n).toBe(1);
  });

  it('cancelling refunds the wallet and releases the credits; the order can be checked out again', async () => {
    await boot();
    const credits = await spendableCredits(platform, 'org_acme');
    const body = checkoutRequest();
    const { checkout } = await startCheckout(body);
    const cancel = await platform.checkout.context.app.inject({ method: 'POST', url: `/checkouts/${checkout.checkoutId}/cancel` });
    expect(cancel.json()).toMatchObject({ status: 'CANCELLED', holds: { credits: { status: 'RELEASED' }, remainder: { status: 'RELEASED' } } });
    expect(await balance(NOURA)).toBe(50000);
    expect(await balance('wallet_escrow_sar')).toBe(0);
    expect(await spendableCredits(platform, 'org_acme')).toBe(credits);
    expect((await platform.verifyOrderConsistency(checkout.orderId)).ok).toBe(true);

    const retry = await startCheckout({ ...body, checkoutId: `${body.checkoutId}_again` });
    expect(retry.checkout.status).toBe('RESERVED');
  });

  it('expires unpaid checkouts: both reservations are released', async () => {
    await boot({ checkout: { ...FAST.checkout, reservationTtlSeconds: 0 } });
    const credits = await spendableCredits(platform, 'org_acme');
    const { checkout } = await startCheckout(checkoutRequest());
    expect(checkout.status).toBe('RESERVED');
    const sweep = await platform.checkout.context.app.inject({ method: 'POST', url: '/checkouts/maintenance' });
    expect(sweep.json().expired).toEqual([checkout.checkoutId]);
    expect(await balance(NOURA)).toBe(50000);
    expect(await spendableCredits(platform, 'org_acme')).toBe(credits);
    expect((await platform.verifyOrderConsistency(checkout.orderId)).ok).toBe(true);
  });

  it('conserves every halala and credit across many mixed-category split-tender checkouts (banker\'s rounding, exact splits)', async () => {
    await boot();
    const creditsBefore = await spendableCredits(platform, 'org_acme');
    const rand = (() => {
      let a = 7;
      return () => {
        a = (a * 1103515245 + 12345) % 2147483648;
        return a / 2147483648;
      };
    })();
    const categories = [
      {},
      { vatCategory: 'Z', vatExemptionCode: 'VATEX-SA-32' },
      { vatCategory: 'E', vatExemptionCode: 'VATEX-SA-29' },
    ] as const;
    const orders: Array<{ orderId: string; total: number; net: number; vat: number; credits: number }> = [];
    for (let i = 0; i < 6; i++) {
      const lines: OrderLine[] = Array.from({ length: 1 + Math.floor(rand() * 3) }, (_, j) => ({
        sku: `SKU-${i}-${j}`,
        nameEn: `Item ${j}`,
        nameAr: `صنف ${j}`,
        quantity: 1 + Math.floor(rand() * 3),
        unitPriceHalalas: 333 + Math.floor(rand() * 2500), // awkward amounts: rounding everywhere
        ...categories[Math.floor(rand() * 3)],
      }));
      const net = lines.reduce((s, l) => s + l.quantity * l.unitPriceHalalas, 0);
      const credits = Math.max(1, Math.floor((net / 100) * rand() * 0.5));
      const { checkout } = await startCheckout(checkoutRequest({ lines, promotion: { orgId: 'org_acme', credits } }));
      expect(checkout.status).toBe('RESERVED');
      const totals = computeOrderTotals(lines, credits * 100);
      expect(checkout.payment.amountHalalas).toBe(totals.totalHalalas);
      expect(checkout.payment.amountSar).toBe(halalasToSar(totals.totalHalalas));
      await platform.checkout.context.app.inject({ method: 'POST', url: `/checkouts/${checkout.checkoutId}/confirm` });
      orders.push({ orderId: checkout.orderId, total: totals.totalHalalas, net: totals.taxableHalalas, vat: totals.vatHalalas, credits });
    }
    await platform.settle();

    for (const o of orders) {
      const report = await platform.verifyOrderConsistency(o.orderId);
      expect(report.checks.filter((c) => !c.ok)).toEqual([]);
    }
    const sum = (f: (o: (typeof orders)[number]) => number) => orders.reduce((s, o) => s + f(o), 0);
    expect(await balance(NOURA)).toBe(50000 - sum((o) => o.total));
    expect(await balance('wallet_escrow_sar')).toBe(0);
    expect(await balance('wallet_merchant_riyadh_retail')).toBe(sum((o) => o.net));
    expect(await balance('wallet_vat_payable_sar')).toBe(sum((o) => o.vat));
    expect(await spendableCredits(platform, 'org_acme')).toBe(creditsBefore - sum((o) => o.credits));
    const sar = await walletQuery(`SELECT COALESCE(SUM(signed_amount_cents), 0)::text AS s FROM ledger_entries WHERE currency = 'SAR'`);
    expect(sar.rows[0].s).toBe('0');
    const invoices = await platform.zatca.context.invoiceService.listInvoices({ taxpayerId: MERCHANT_ID });
    expect(invoices.reduce((s, i) => s + i.total_amount_halalas, 0)).toBe(sum((o) => o.total));
    expect(invoices.reduce((s, i) => s + i.vat_amount_halalas, 0)).toBe(sum((o) => o.vat));
    expect((await platform.zatca.context.invoiceService.verifyHashChain(MERCHANT_ID)).ok).toBe(true);
  });
});
