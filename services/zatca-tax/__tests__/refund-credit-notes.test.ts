import { describe, it, expect, beforeEach } from 'vitest';
import { buildApp, type AppContext } from '../src/server';
import { getDb, resetDatabaseAndSeed } from '../src/db';
import { renderInvoiceDocumentPdf } from '../src/integration/invoicePdf';
import { ZATCA_REFUND_CONSUMER } from '../src/integration/RefundInvoicingService';
import { TaxCalculationEngine } from '../src/services/TaxCalculationEngine';
import type { ZatcaGatewayResponse } from '../src/services/ReportingService';
import {
  CreditNoteReportedSchema,
  EVENT_TYPES,
  PRODUCERS,
  buildIntegrationEvent,
  computeOrderTotals,
  proRataRefundSplit,
  type LedgerPaymentRefundedEvent,
  type LedgerPaymentSettledEvent,
  type OrderLine,
  type RefundSplitLine,
} from '../../../shared/contracts';
import { NonRetryableError } from '../../../shared/messaging';

const TAXPAYER = 'txp_riyadh_retail';
const LINES: OrderLine[] = [
  { sku: 'CHG-65W', nameEn: '65W USB-C Fast Charger', nameAr: 'شاحن سريع 65 واط', quantity: 3, unitPriceHalalas: 12000 },
  { sku: 'EDU-1', nameEn: 'Course', nameAr: 'دورة', quantity: 1, unitPriceHalalas: 5000, vatCategory: 'Z', vatExemptionCode: 'VATEX-SA-EDU' },
];
const ALLOWANCE = 1000;
const OK: ZatcaGatewayResponse = { httpStatus: 200, reportingStatus: 'REPORTED', validationResults: { status: 'PASS', warningMessages: [], errorMessages: [] } };
const DOWN: ZatcaGatewayResponse = { httpStatus: 503, reportingStatus: 'NOT_REPORTED', validationResults: { status: 'ERROR', warningMessages: [], errorMessages: [] } };

let seq = 0;
function settled(orderId: string): LedgerPaymentSettledEvent {
  const n = ++seq;
  const totals = computeOrderTotals(LINES, ALLOWANCE);
  return buildIntegrationEvent({
    eventId: `evt_ledger_${n}`,
    eventType: EVENT_TYPES.ledgerPaymentSettled,
    producer: PRODUCERS.ledger,
    correlationId: orderId,
    causationId: null,
    aggregateId: orderId,
    payload: {
      orderId,
      paymentId: `pay_${orderId}`,
      holdId: `ph_${n}`,
      merchantId: TAXPAYER,
      currency: 'SAR',
      journalRef: `cap_ph_${n}`,
      grossHalalas: totals.totalHalalas,
      netRevenueHalalas: totals.taxableHalalas,
      vatHalalas: totals.vatHalalas,
      allowanceHalalas: ALLOWANCE,
      capturedCredits: 10,
      paymentBrand: 'MADA',
      tender: 'CARD',
      order: { orderId, merchantId: TAXPAYER, creditOrgId: 'org_acme', customer: { name: 'Reem' }, lines: LINES },
      settledAt: new Date().toISOString(),
    },
  }) as LedgerPaymentSettledEvent;
}

/** The ledger's split: pro rata over what is still refundable of each category component. */
function split(amount: number, refundedSoFar: RefundSplitLine[] = []): RefundSplitLine[] {
  const remaining = computeOrderTotals(LINES, ALLOWANCE).categories.map((c) => {
    const done = refundedSoFar.filter((l) => l.vatCategory === c.vatCategory);
    return {
      vatCategory: c.vatCategory,
      vatRateBps: c.vatRateBps,
      vatExemptionCode: LINES.find((l) => (l.vatCategory ?? 'S') === c.vatCategory)?.vatExemptionCode ?? null,
      netHalalas: c.taxableHalalas - done.reduce((s, l) => s + l.netHalalas, 0),
      vatHalalas: c.vatHalalas - done.reduce((s, l) => s + l.vatHalalas, 0),
    };
  });
  return proRataRefundSplit(remaining, amount);
}

function refunded(orderId: string, amount: number, refundedTotal: number, lines: RefundSplitLine[], overrides: Partial<{ refundId: string; eventId: string; net: number; vat: number }> = {}): LedgerPaymentRefundedEvent {
  const n = ++seq;
  const gross = computeOrderTotals(LINES, ALLOWANCE).totalHalalas;
  const net = overrides.net ?? lines.reduce((s, l) => s + l.netHalalas, 0);
  const vat = overrides.vat ?? lines.reduce((s, l) => s + l.vatHalalas, 0);
  const refundId = overrides.refundId ?? `rf_${n}`;
  return buildIntegrationEvent({
    eventId: overrides.eventId ?? `evt_refunded_${n}`,
    eventType: EVENT_TYPES.ledgerPaymentRefunded,
    producer: PRODUCERS.ledger,
    correlationId: orderId,
    causationId: null,
    aggregateId: orderId,
    payload: {
      refund: { refundId, refundEventId: `evt_rf_${n}`, paymentId: `pay_${orderId}`, orderId, merchantId: TAXPAYER, currency: 'SAR', amountHalalas: amount, capturedHalalas: gross, refundedTotalHalalas: refundedTotal, reason: 'damaged item', refundedAt: new Date().toISOString() },
      captureJournalRef: 'cap_ph_1',
      journalRef: `rfd_${refundId}`,
      netHalalas: net,
      vatHalalas: vat,
      lines,
      merchantAccount: 'REVENUE',
      tender: 'CARD',
      refundedToWalletId: 'wallet_treasury_sar',
      remaining: { grossHalalas: gross - refundedTotal, netHalalas: 0, vatHalalas: 0 },
      credits: { outcome: 'RESTORED', orgId: 'org_acme', capturedCredits: 10, restoredCredits: 2, cumulativeRestoredCredits: 2, grantId: 'g' },
      postedAt: new Date().toISOString(),
    },
  }) as LedgerPaymentRefundedEvent;
}

async function outboxEvents(type: string) {
  const db = await getDb();
  return (await db.query<{ body: unknown }>(`SELECT body FROM integration_outbox WHERE event_type = $1 ORDER BY seq`, [type])).rows.map((r) => r.body);
}

describe('Refund credit notes: pro-rata 381 against the order invoice (ICV chain, exactly once)', () => {
  let ctx: AppContext;
  let orderId: string;

  beforeEach(async () => {
    await resetDatabaseAndSeed();
    ctx = await buildApp();
    ctx.reportingService.setTransport(async () => OK);
    orderId = `ord_cn_${++seq}`;
    const issued = await ctx.orderInvoicingService.handleLedgerPaymentSettled(settled(orderId));
    expect(issued.outcome).toBe('REPORTED');
  });

  it('issues a credit note whose lines carry the pro-rata VAT the ledger reversed, referencing the invoice, and announces it', async () => {
    const lines = split(10000);
    const result = await ctx.refundInvoicingService.handleLedgerPaymentRefunded(refunded(orderId, 10000, 10000, lines, { refundId: 'rf_a' }));
    expect(result).toMatchObject({ outcome: 'REPORTED', icv: 2 });
    const [note] = (await ctx.refundInvoicingService.getOrderCreditNotes(orderId)).creditNotes;
    const [invoice] = (await ctx.orderInvoicingService.getOrderInvoicing(orderId)).invoices;
    expect(note).toMatchObject({
      invoice_type_code: '381',
      invoice_subtype: '0200000',
      billing_reference_id: invoice.id,
      source_refund_id: 'rf_a',
      source_order_id: orderId,
      taxable_amount_halalas: lines.reduce((s, l) => s + l.netHalalas, 0),
      vat_amount_halalas: lines.reduce((s, l) => s + l.vatHalalas, 0),
      total_amount_halalas: 10000,
      status: 'REPORTED',
      icv: 2,
      previous_invoice_hash: invoice.invoice_hash,
    });
    expect(note.invoice_number).toMatch(/^CN-/);
    expect(note.instruction_note).toContain('Customer refund rf_a: 100.00 SAR refunded (partial');
    expect(note.lines.map((l) => [l.sku, l.vat_category, l.vat_exemption_code, l.unit_price_halalas, l.vat_amount_halalas])).toEqual(
      lines.map((l) => [`REFUND-${l.vatCategory}`, l.vatCategory, l.vatExemptionCode, l.netHalalas, l.vatHalalas])
    );
    // The breakdown (UBL TaxSubtotal) describes the document as issued, not a recomputation.
    const breakdown = TaxCalculationEngine.breakdownForInvoice(note);
    expect(breakdown.find((c) => c.vatCategory === 'S')?.vatAmountHalalas).toBe(lines.find((l) => l.vatCategory === 'S')?.vatHalalas);
    const xml = Buffer.from(note.xml_ubl_base64 as string, 'base64').toString('utf8');
    expect(xml).toContain('<cbc:InvoiceTypeCode name="0200000">381</cbc:InvoiceTypeCode>');
    expect(xml).toContain(`<cbc:ID>${invoice.id}</cbc:ID>`);
    expect(xml).toContain('<cbc:InstructionNote>Customer refund rf_a');

    const [announced] = await outboxEvents(EVENT_TYPES.creditNoteReported);
    const parsed = CreditNoteReportedSchema.parse(announced);
    expect(parsed.payload).toMatchObject({
      originalInvoiceId: invoice.id,
      originalInvoiceNumber: invoice.invoice_number,
      creditedTotalHalalas: 10000,
      creditNote: { invoiceId: note.id, billingReference: { invoiceId: invoice.id, invoiceNumber: invoice.invoice_number }, totals: { totalHalalas: 10000 } },
      invoice: { invoiceId: invoice.id, invoiceHash: invoice.invoice_hash },
    });
    expect(parsed.payload.creditNotes).toEqual([{ invoiceId: note.id, invoiceNumber: note.invoice_number, icv: 2, totalHalalas: 10000, refundId: 'rf_a', reportedAt: note.zatca_reported_at }]);
    // Both PDFs render from the event alone (the invoice one with the credits applied).
    expect((await renderInvoiceDocumentPdf(parsed.payload.creditNote)).subarray(0, 5).toString('ascii')).toBe('%PDF-');
    expect((await renderInvoiceDocumentPdf(parsed.payload.invoice, { creditNotes: parsed.payload.creditNotes })).byteLength).toBeGreaterThan(0);
    expect((await ctx.invoiceService.verifyHashChain(TAXPAYER)).ok).toBe(true);
  });

  it('credits the whole invoice over several refunds, exactly, and refuses to go beyond it', async () => {
    const gross = computeOrderTotals(LINES, ALLOWANCE).totalHalalas;
    const first = split(12345);
    await ctx.refundInvoicingService.handleLedgerPaymentRefunded(refunded(orderId, 12345, 12345, first));
    const second = split(gross - 12345, first);
    await ctx.refundInvoicingService.handleLedgerPaymentRefunded(refunded(orderId, gross - 12345, gross, second));
    const notes = (await ctx.refundInvoicingService.getOrderCreditNotes(orderId)).creditNotes;
    expect(notes.map((n) => n.icv)).toEqual([2, 3]);
    const totals = computeOrderTotals(LINES, ALLOWANCE);
    expect(notes.reduce((s, n) => s + n.taxable_amount_halalas, 0)).toBe(totals.taxableHalalas);
    expect(notes.reduce((s, n) => s + n.vat_amount_halalas, 0)).toBe(totals.vatHalalas);
    expect(notes.reduce((s, n) => s + n.total_amount_halalas, 0)).toBe(gross);
    expect(notes[1].instruction_note).toContain('(final;');
    await expect(ctx.refundInvoicingService.handleLedgerPaymentRefunded(refunded(orderId, 1, gross + 1, [{ ...first[0], netHalalas: 1, vatHalalas: 0 }]))).rejects.toMatchObject({
      reason: 'CREDIT_NOTES_EXCEED_INVOICE',
    });
    expect((await ctx.invoiceService.getTaxpayer(TAXPAYER)).last_icv).toBe(3);
  });

  it('never issues twice: redeliveries, re-announcements and a Fatoora outage keep one credit note and one ICV', async () => {
    let calls = 0;
    ctx.reportingService.setTransport(async () => (++calls <= 1 ? DOWN : OK));
    const event = refunded(orderId, 5000, 5000, split(5000), { refundId: 'rf_once' });
    await expect(ctx.refundInvoicingService.handleLedgerPaymentRefunded(event)).rejects.toThrow(/ZATCA_TRANSIENT_ERROR/);
    expect(await outboxEvents(EVENT_TYPES.creditNoteReported)).toHaveLength(0);
    expect((await ctx.refundInvoicingService.handleLedgerPaymentRefunded(event)).outcome).toBe('REPORTED');
    expect((await ctx.refundInvoicingService.handleLedgerPaymentRefunded(event)).outcome).toBe('DUPLICATE_IGNORED');
    const again = await ctx.refundInvoicingService.handleLedgerPaymentRefunded({ ...event, eventId: 'evt_refunded_again' });
    expect(again.icv).toBe(2);
    expect((await ctx.refundInvoicingService.getOrderCreditNotes(orderId)).creditNotes).toHaveLength(1);
    expect(await outboxEvents(EVENT_TYPES.creditNoteReported)).toHaveLength(1);
    expect((await ctx.invoiceService.getTaxpayer(TAXPAYER)).last_icv).toBe(2);
  });

  it('dead-letters a refund the credit note cannot reproduce, or of an order never invoiced', async () => {
    const lines = split(5000);
    const wrong = refunded(orderId, 5000, 5000, lines, { vat: lines.reduce((s, l) => s + l.vatHalalas, 0) + 1 });
    const err = await ctx.refundInvoicingService.handleLedgerPaymentRefunded(wrong).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(NonRetryableError);
    expect((err as NonRetryableError).reason).toBe('LEDGER_TAX_MISMATCH');
    // A VAT far from the pro-rata share is refused by the engine itself.
    const farOff = refunded(orderId, 5000, 5000, lines.map((l) => (l.vatCategory === 'S' ? { ...l, netHalalas: l.netHalalas - 500, vatHalalas: l.vatHalalas + 500 } : l)));
    await expect(ctx.refundInvoicingService.handleLedgerPaymentRefunded(farOff)).rejects.toMatchObject({ reason: 'INVALID_LINE_VAT' });
    await expect(ctx.refundInvoicingService.handleLedgerPaymentRefunded(refunded('ord_never', 100, 100, lines))).rejects.toMatchObject({ reason: 'ORDER_NOT_INVOICED' });
    expect((await ctx.invoiceService.getTaxpayer(TAXPAYER)).last_icv).toBe(1);
  });

  it('exposes an order\'s credit notes over HTTP', async () => {
    await ctx.refundInvoicingService.handleLedgerPaymentRefunded(refunded(orderId, 5000, 5000, split(5000)));
    const res = await ctx.app.inject({ method: 'GET', url: `/api/v1/orders/${orderId}/credit-notes` });
    expect(res.statusCode).toBe(200);
    expect(res.json().creditNotes).toHaveLength(1);
    expect(res.json().checkpoints.map((c: { consumer: string; step: string }) => `${c.consumer}:${c.step}`)).toEqual([`${ZATCA_REFUND_CONSUMER}:issued`, `${ZATCA_REFUND_CONSUMER}:done`]);
  });
});
