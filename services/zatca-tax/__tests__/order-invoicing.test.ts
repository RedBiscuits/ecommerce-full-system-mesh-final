import { describe, it, expect, beforeEach } from 'vitest';
import { spawnSync } from 'node:child_process';
import { buildApp, type AppContext } from '../src/server';
import { ZATCA_GENESIS_PIH, getDb, resetDatabaseAndSeed } from '../src/db';
import { renderReportedInvoicePdf } from '../src/integration/invoicePdf';
import type { ZatcaGatewayResponse } from '../src/services/ReportingService';
import {
  EVENT_TYPES,
  InvoiceReportedSchema,
  LedgerPaymentSettledEvent,
  OrderLine,
  PRODUCERS,
  buildIntegrationEvent,
  computeOrderTotals,
} from '../../../shared/contracts';
import { NonRetryableError, listCheckpoints } from '../../../shared/messaging';

const TAXPAYER = 'txp_riyadh_retail';
const LINES: OrderLine[] = [
  { sku: 'CHG-65W', nameEn: '65W USB-C Fast Charger', nameAr: 'شاحن سريع 65 واط', quantity: 3, unitPriceHalalas: 12000 },
  { sku: 'EDU-1', nameEn: 'Course', nameAr: 'دورة', quantity: 1, unitPriceHalalas: 5000, vatCategory: 'Z', vatExemptionCode: 'VATEX-SA-EDU' },
];

const OK: ZatcaGatewayResponse = {
  httpStatus: 200,
  reportingStatus: 'REPORTED',
  validationResults: { status: 'PASS', warningMessages: [], errorMessages: [] },
};
const DOWN: ZatcaGatewayResponse = {
  httpStatus: 503,
  reportingStatus: 'NOT_REPORTED',
  validationResults: { status: 'ERROR', warningMessages: [], errorMessages: [] },
};
const REJECTED: ZatcaGatewayResponse = {
  httpStatus: 400,
  reportingStatus: 'NOT_REPORTED',
  validationResults: { status: 'ERROR', warningMessages: [], errorMessages: [{ code: 'BR-KSA-98', message: 'invalid' }] },
};

let seq = 0;
function settled(overrides: Partial<{ orderId: string; eventId: string; allowance: number; net: number; journalRef: string; merchantId: string; lines: OrderLine[]; tender: 'CARD' | 'WALLET' }> = {}): LedgerPaymentSettledEvent {
  const n = ++seq;
  const allowance = overrides.allowance ?? 1000;
  const lines = overrides.lines ?? LINES;
  const totals = computeOrderTotals(lines, allowance);
  const orderId = overrides.orderId ?? `ord_z_${n}`;
  return buildIntegrationEvent({
    eventId: overrides.eventId ?? `evt_ledger_${n}`,
    eventType: EVENT_TYPES.ledgerPaymentSettled,
    producer: PRODUCERS.ledger,
    correlationId: orderId,
    causationId: `evt_billing_${n}`,
    aggregateId: orderId,
    payload: {
      orderId,
      paymentId: `pay_${n}`,
      holdId: `ph_${n}`,
      merchantId: overrides.merchantId ?? TAXPAYER,
      currency: 'SAR',
      journalRef: overrides.journalRef ?? `cap_ph_${n}`,
      grossHalalas: totals.totalHalalas,
      netRevenueHalalas: overrides.net ?? totals.taxableHalalas,
      vatHalalas: totals.vatHalalas,
      allowanceHalalas: allowance,
      capturedCredits: allowance / 100,
      paymentBrand: 'MADA',
      tender: overrides.tender ?? 'CARD',
      order: { orderId, merchantId: TAXPAYER, creditOrgId: 'org_acme', customer: { name: 'Reem', email: 'reem@example.sa' }, lines },
      settledAt: new Date().toISOString(),
    },
  }) as LedgerPaymentSettledEvent;
}

async function lastIcv(ctx: AppContext): Promise<number> {
  return (await ctx.invoiceService.getTaxpayer(TAXPAYER)).last_icv;
}

describe('Order invoicing: settled ledger payment → simplified tax invoice → Fatoora (per-step checkpoints)', () => {
  let ctx: AppContext;

  beforeEach(async () => {
    await resetDatabaseAndSeed();
    ctx = await buildApp();
    ctx.reportingService.setTransport(async () => OK);
  });

  it('issues a 388/0200000 invoice with the credit allowance, chains it, reports it and announces it', async () => {
    const event = settled();
    const result = await ctx.orderInvoicingService.handleLedgerPaymentSettled(event);
    expect(result).toMatchObject({ outcome: 'REPORTED', icv: 1 });

    const invoice = await ctx.invoiceService.getInvoice(result.invoiceId!);
    const totals = computeOrderTotals(LINES, 1000);
    expect(invoice).toMatchObject({
      invoice_type_code: '388',
      invoice_subtype: '0200000',
      payment_means_code: '48',
      egs_device_id: 'egs_ryd_main_01',
      status: 'REPORTED',
      icv: 1,
      previous_invoice_hash: ZATCA_GENESIS_PIH,
      document_allowance_halalas: 1000,
      taxable_amount_halalas: totals.taxableHalalas,
      vat_amount_halalas: totals.vatHalalas,
      total_amount_halalas: totals.totalHalalas,
      source_order_id: event.payload.orderId,
      source_journal_ref: event.payload.journalRef,
      source_event_id: event.eventId,
    });
    expect(invoice.lines.map((l) => [l.sku, l.vat_category, l.vat_exemption_code])).toEqual([
      ['CHG-65W', 'S', null],
      ['EDU-1', 'Z', 'VATEX-SA-EDU'],
    ]);

    const db = await getDb();
    const outbox = await db.query<{ event_id: string; exchange: string; routing_key: string; body: any }>(
      `SELECT event_id, exchange, routing_key, body FROM integration_outbox`
    );
    expect(outbox.rows).toHaveLength(1);
    expect(outbox.rows[0]).toMatchObject({ event_id: result.eventId, exchange: 'zatca.events', routing_key: 'zatca.invoice.reported' });
    // The announced document is complete enough to render the PDF without the ZATCA database.
    const announced = InvoiceReportedSchema.parse(outbox.rows[0].body);
    expect(announced.payload).toMatchObject({ icv: 1, status: 'REPORTED', orderId: event.payload.orderId, totals: { allowanceHalalas: 1000 } });
    const pdf = await renderReportedInvoicePdf(announced.payload);
    expect(pdf.subarray(0, 5).toString('ascii')).toBe('%PDF-');

    const checkpoints = await listCheckpoints(db, { eventId: event.eventId });
    expect(checkpoints.map((c) => [c.step, c.outcome]).sort()).toEqual([
      ['done', 'REPORTED'],
      ['issued', 'ISSUED'],
    ]);
  });

  it.skipIf(spawnSync('pdftotext', ['-v']).error !== undefined)(
    'marks the promotion as the allowance on the PDF, with the mixed S / Z / E VAT breakdown (pdftotext)',
    async () => {
      const result = await ctx.orderInvoicingService.handleLedgerPaymentSettled(settled({ allowance: 2000 }));
      const db = await getDb();
      const body = (await db.query<{ body: any }>(`SELECT body FROM integration_outbox`)).rows[0].body;
      const pdf = await renderReportedInvoicePdf(InvoiceReportedSchema.parse(body).payload);
      const text = spawnSync('pdftotext', ['-', '-'], { input: pdf }).stdout.toString();
      expect(text).toContain('SIMPLIFIED TAX INVOICE (B2C - 0200000)');
      expect(text).toContain('Allowance – Promotional credit (20 credits): -20.00 SAR');
      // 3 × 120.00 S + 50.00 Z: the 20.00 promotion splits 17.56 / 2.44 (proportional, exact).
      expect(text).toContain('S 15.00% | Net: 360.00 | Allowance: -17.56 | Taxable: 342.44 | VAT: 51.37');
      expect(text).toContain('Z 0.00% VATEX-SA-EDU | Net: 50.00 | Allowance: -2.44 | Taxable: 47.56 | VAT: 0.00');
      expect(text).toContain(`ICV (Invoice Counter Value): ${result.icv}`);
      expect(text).toContain('Previous Invoice Hash (PIH)');
    }
  );

  it('invoices a mixed S (15 %) / Z (0 %) / E (0 %, VATEX-SA-29) order with the promotion split per category, exactly', async () => {
    const lines: OrderLine[] = [
      { sku: 'RTR', nameEn: 'Router', nameAr: 'موجه', quantity: 1, unitPriceHalalas: 10000, vatCategory: 'S' },
      { sku: 'EXP', nameEn: 'Export licence', nameAr: 'ترخيص تصدير', quantity: 1, unitPriceHalalas: 10000, vatCategory: 'Z', vatExemptionCode: 'VATEX-SA-32' },
      { sku: 'FIN', nameEn: 'Financing fee', nameAr: 'رسوم تمويل', quantity: 1, unitPriceHalalas: 5000, vatCategory: 'E', vatExemptionCode: 'VATEX-SA-29' },
    ];
    const event = settled({ lines, allowance: 2500, tender: 'WALLET' });
    expect(event.payload).toMatchObject({ grossHalalas: 23850, netRevenueHalalas: 22500, vatHalalas: 1350 });
    const result = await ctx.orderInvoicingService.handleLedgerPaymentSettled(event);
    const invoice = await ctx.invoiceService.getInvoice(result.invoiceId!);
    expect(invoice).toMatchObject({
      payment_means_code: '30', // wallet balance: credit transfer
      subtotal_halalas: 25000,
      document_allowance_halalas: 2500,
      document_allowance_reason: 'Promotional credit (25 credits)',
      taxable_amount_halalas: 22500,
      vat_amount_halalas: 1350,
      total_amount_halalas: 23850,
    });

    // The invoice reported to Fatoora (UBL) marks the promotion as allowances per VAT category…
    const xml = Buffer.from(invoice.xml_ubl_base64!, 'base64').toString('utf8');
    const allowances = [...xml.matchAll(/<cac:AllowanceCharge>([\s\S]*?)<\/cac:AllowanceCharge>/g)].map((m) => m[1]);
    expect(allowances).toHaveLength(3);
    for (const a of allowances) {
      expect(a).toContain('<cbc:ChargeIndicator>false</cbc:ChargeIndicator>');
      expect(a).toContain('<cbc:AllowanceChargeReasonCode>95</cbc:AllowanceChargeReasonCode>');
      expect(a).toContain('<cbc:AllowanceChargeReason>Promotional credit (25 credits)</cbc:AllowanceChargeReason>');
    }
    expect(allowances.map((a) => [/<cbc:Amount currencyID="SAR">([\d.]+)</.exec(a)![1], /<cbc:ID>([SZE])</.exec(a)![1]])).toEqual([
      ['10.00', 'S'],
      ['10.00', 'Z'],
      ['5.00', 'E'],
    ]);
    // …and states the VAT breakdown per category, with the exemption reasons.
    const subtotals = [...xml.matchAll(/<cac:TaxSubtotal>([\s\S]*?)<\/cac:TaxSubtotal>/g)].map((m) => m[1]);
    expect(subtotals.map((t) => [/<cbc:TaxableAmount currencyID="SAR">([\d.]+)</.exec(t)![1], /<cbc:TaxAmount currencyID="SAR">([\d.]+)</.exec(t)![1]])).toEqual([
      ['90.00', '13.50'],
      ['90.00', '0.00'],
      ['45.00', '0.00'],
    ]);
    expect(subtotals[2]).toContain('<cbc:TaxExemptionReasonCode>VATEX-SA-29</cbc:TaxExemptionReasonCode>');
    expect(subtotals[1]).toContain('<cbc:TaxExemptionReasonCode>VATEX-SA-32</cbc:TaxExemptionReasonCode>');
    expect(xml).toContain('<cbc:AllowanceTotalAmount currencyID="SAR">25.00</cbc:AllowanceTotalAmount>');
    expect(xml).toContain('<cbc:PayableAmount currencyID="SAR">238.50</cbc:PayableAmount>');

    // The announced document carries the same breakdown (what the PDF is rendered from).
    const db = await getDb();
    const body = (await db.query<{ body: any }>(`SELECT body FROM integration_outbox`)).rows[0].body;
    const announced = InvoiceReportedSchema.parse(body).payload;
    expect(announced.allowanceReason).toBe('Promotional credit (25 credits)');
    expect(announced.vatBreakdown).toEqual([
      { vatCategory: 'S', vatRateBps: 1500, exemptionCode: null, lineNetHalalas: 10000, allowanceHalalas: 1000, taxableHalalas: 9000, vatHalalas: 1350 },
      { vatCategory: 'Z', vatRateBps: 0, exemptionCode: 'VATEX-SA-32', lineNetHalalas: 10000, allowanceHalalas: 1000, taxableHalalas: 9000, vatHalalas: 0 },
      { vatCategory: 'E', vatRateBps: 0, exemptionCode: 'VATEX-SA-29', lineNetHalalas: 5000, allowanceHalalas: 500, taxableHalalas: 4500, vatHalalas: 0 },
    ]);
    expect(announced.vatBreakdown.reduce((sum, c) => sum + c.vatHalalas, 0)).toBe(announced.totals.vatHalalas);
    expect(announced.vatBreakdown.reduce((sum, c) => sum + c.taxableHalalas, 0)).toBe(announced.totals.taxableHalalas);
  });

  it('ignores a redelivery and re-uses the order invoice for a re-announcement (one invoice, one ICV per order)', async () => {
    const event = settled();
    const first = await ctx.orderInvoicingService.handleLedgerPaymentSettled(event);
    expect((await ctx.orderInvoicingService.handleLedgerPaymentSettled(event)).outcome).toBe('DUPLICATE_IGNORED');
    const again = await ctx.orderInvoicingService.handleLedgerPaymentSettled({ ...event, eventId: 'evt_reannounced' });
    expect(again).toMatchObject({ outcome: 'REPORTED', invoiceId: first.invoiceId, icv: 1 });
    expect(await lastIcv(ctx)).toBe(1);
    const db = await getDb();
    expect((await db.query(`SELECT 1 FROM integration_outbox`)).rows).toHaveLength(1);
  });

  it('a Fatoora outage keeps the invoice ISSUED with its ICV; the retry only reports (no second ICV)', async () => {
    ctx.reportingService.setTransport(async () => DOWN);
    const event = settled();
    await expect(ctx.orderInvoicingService.handleLedgerPaymentSettled(event)).rejects.toThrow(/^ZATCA_TRANSIENT_ERROR/);
    const [issued] = await ctx.invoiceService.listInvoices({ taxpayerId: TAXPAYER });
    expect(issued).toMatchObject({ status: 'ISSUED', icv: 1 });
    await expect(ctx.orderInvoicingService.handleLedgerPaymentSettled(event)).rejects.toThrow(/^ZATCA_TRANSIENT_ERROR/);

    ctx.reportingService.setTransport(async () => OK);
    const result = await ctx.orderInvoicingService.handleLedgerPaymentSettled(event);
    expect(result).toMatchObject({ outcome: 'REPORTED', invoiceId: issued.id, icv: 1 });
    expect(await lastIcv(ctx)).toBe(1);
    const logs = await ctx.reportingService.listSubmissionLogs(issued.id);
    expect(logs.map((l) => l.outcome)).toEqual(['TRANSIENT_ERROR', 'TRANSIENT_ERROR', 'REPORTED']);
  });

  it('a Fatoora rejection is not retried (dead-letter for a human): the invoice keeps its place in the chain', async () => {
    ctx.reportingService.setTransport(async () => REJECTED);
    const err = await ctx.orderInvoicingService.handleLedgerPaymentSettled(settled()).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(NonRetryableError);
    expect(err).toMatchObject({ reason: 'ZATCA_REJECTED' });
    expect(await lastIcv(ctx)).toBe(1);
    expect((await ctx.invoiceService.verifyHashChain(TAXPAYER)).ok).toBe(true);
  });

  it('never issues when the invoice would disagree with what the ledger booked', async () => {
    const err = await ctx.orderInvoicingService.handleLedgerPaymentSettled(settled({ net: 1 })).catch((e: unknown) => e);
    expect(err).toMatchObject({ reason: 'LEDGER_TAX_MISMATCH' });
    expect(await lastIcv(ctx)).toBe(0);
    expect(await ctx.invoiceService.listInvoices({ taxpayerId: TAXPAYER })).toEqual([]);
    const db = await getDb();
    expect((await db.query(`SELECT 1 FROM integration_checkpoints`)).rows).toEqual([]);
  });

  it('refuses an unknown seller and an order already invoiced for another journal', async () => {
    await expect(ctx.orderInvoicingService.handleLedgerPaymentSettled(settled({ merchantId: 'txp_unknown' }))).rejects.toMatchObject({
      reason: 'TAXPAYER_NOT_FOUND',
    });
    await ctx.orderInvoicingService.handleLedgerPaymentSettled(settled({ orderId: 'ord_j', journalRef: 'cap_a' }));
    await expect(
      ctx.orderInvoicingService.handleLedgerPaymentSettled(settled({ orderId: 'ord_j', journalRef: 'cap_b' }))
    ).rejects.toMatchObject({ reason: 'ORDER_ALREADY_INVOICED' });
  });

  describe('hash chain integrity', () => {
    it('keeps ICV 1..n gapless and every PIH linked, across concurrent settlements', async () => {
      const events = Array.from({ length: 8 }, () => settled());
      const results = await Promise.all(events.map((e) => ctx.orderInvoicingService.handleLedgerPaymentSettled(e)));
      expect(results.map((r) => r.icv).sort((a, b) => a! - b!)).toEqual([1, 2, 3, 4, 5, 6, 7, 8]);
      const report = await ctx.invoiceService.verifyHashChain(TAXPAYER);
      expect(report).toEqual({ taxpayerId: TAXPAYER, ok: true, length: 8, lastIcv: 8, breaks: [] });
    });

    it('the database refuses a repeated ICV, a repeated PIH and a second invoice for one order', async () => {
      await ctx.orderInvoicingService.handleLedgerPaymentSettled(settled({ orderId: 'ord_a' }));
      await ctx.orderInvoicingService.handleLedgerPaymentSettled(settled({ orderId: 'ord_b' }));
      const db = await getDb();
      const [first, second] = (
        await db.query<{ id: string; icv: number; previous_invoice_hash: string }>(
          `SELECT id, icv, previous_invoice_hash FROM invoices ORDER BY icv`
        )
      ).rows;
      await expect(db.query(`UPDATE invoices SET icv = $1 WHERE id = $2`, [first.icv, second.id])).rejects.toThrow(/uq_invoices_taxpayer_icv/);
      await expect(db.query(`UPDATE invoices SET previous_invoice_hash = $1 WHERE id = $2`, [first.previous_invoice_hash, second.id])).rejects.toThrow(
        /uq_invoices_taxpayer_pih/
      );
      await expect(db.query(`UPDATE invoices SET source_order_id = 'ord_a' WHERE id = $1`, [second.id])).rejects.toThrow(/uq_invoices_source_order/);
    });

    it('detects tampering, gaps and a moved chain head', async () => {
      for (let i = 0; i < 3; i++) await ctx.orderInvoicingService.handleLedgerPaymentSettled(settled());
      const db = await getDb();
      await db.query(`UPDATE invoices SET total_amount_halalas = total_amount_halalas + 1 WHERE icv = 2`);
      const tampered = await ctx.invoiceService.verifyHashChain(TAXPAYER);
      expect(tampered.ok).toBe(false);
      expect(tampered.breaks).toEqual([expect.stringMatching(/^ICV 2 .*stored hash does not match the document$/)]);

      await db.query(`UPDATE taxpayers SET last_icv = 7 WHERE id = $1`, [TAXPAYER]);
      const moved = await ctx.invoiceService.verifyHashChain(TAXPAYER);
      expect(moved.breaks).toContain('taxpayer.last_icv=7 but the chain has 3 documents');
    });

    it('exposes the chain check and the order invoicing over HTTP', async () => {
      const event = settled({ orderId: 'ord_http' });
      await ctx.orderInvoicingService.handleLedgerPaymentSettled(event);
      const chain = await ctx.app.inject({ method: 'GET', url: `/api/v1/taxpayers/${TAXPAYER}/hash-chain` });
      expect(chain.statusCode).toBe(200);
      expect(chain.json()).toMatchObject({ ok: true, length: 1 });
      const order = await ctx.app.inject({ method: 'GET', url: '/api/v1/orders/ord_http/invoicing' });
      expect(order.statusCode).toBe(200);
      expect(order.json().invoices).toHaveLength(1);
      expect(order.json().checkpoints.map((c: { step: string }) => c.step).sort()).toEqual(['done', 'issued']);
    });
  });
});
