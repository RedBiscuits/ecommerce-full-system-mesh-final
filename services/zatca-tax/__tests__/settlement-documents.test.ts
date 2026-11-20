import { describe, it, expect, beforeEach } from 'vitest';
import { spawnSync } from 'node:child_process';
import { buildApp, type AppContext } from '../src/server';
import { PLATFORM_TAXPAYER_ID, getDb, resetDatabaseAndSeed } from '../src/db';
import { renderInvoiceDocumentPdf } from '../src/integration/invoicePdf';
import { ZATCA_SETTLEMENT_DOCUMENTS_CONSUMER } from '../src/integration/SettlementInvoicingService';
import type { SubmissionMode, ZatcaGatewayResponse } from '../src/services/ReportingService';
import {
  EVENT_TYPES,
  PRODUCERS,
  SettlementDocumentsReportedSchema,
  buildIntegrationEvent,
  computeOrderTotals,
  type MerchantSettlementPostedEvent,
  type SettlementFee,
} from '../../../shared/contracts';
import { NonRetryableError, listCheckpoints } from '../../../shared/messaging';

const OK: ZatcaGatewayResponse = {
  httpStatus: 200,
  reportingStatus: 'REPORTED',
  clearanceStatus: 'CLEARED',
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
  clearanceStatus: 'NOT_CLEARED',
  validationResults: { status: 'ERROR', warningMessages: [], errorMessages: [{ code: 'BR-KSA-98', message: 'invalid' }] },
};

function fee(net: number): SettlementFee {
  const vat = computeOrderTotals([{ sku: 'F', nameEn: 'f', nameAr: 'f', quantity: 1, unitPriceHalalas: net }], 0).vatHalalas;
  return {
    ruleId: 'std-tiered',
    ruleVersion: 1,
    tiers: [{ fromHalalas: 0, toHalalas: null, bps: 250, baseHalalas: 60000, feeHalalas: net - 150 }],
    percentageFeeHalalas: net - 150,
    perTransactionHalalas: 50,
    fixedFeeHalalas: 150,
    minimumFeeHalalas: 0,
    netHalalas: net,
    vatRateBps: 1500,
    vatHalalas: vat,
    totalHalalas: net + vat,
    capped: false,
  };
}

let seq = 0;
function posted(overrides: Partial<{ batchId: string; eventId: string; merchantId: string; gross: number; feeNet: number; feeVat: number; journalRef: string }> = {}): MerchantSettlementPostedEvent {
  const n = ++seq;
  const batchId = overrides.batchId ?? `stb_z_${n}`;
  const gross = overrides.gross ?? 60000;
  const f = fee(overrides.feeNet ?? 1650);
  if (overrides.feeVat !== undefined) {
    f.vatHalalas = overrides.feeVat;
    f.totalHalalas = f.netHalalas + overrides.feeVat;
  }
  return buildIntegrationEvent({
    eventId: overrides.eventId ?? `evt_posted_${n}`,
    eventType: EVENT_TYPES.merchantSettlementPosted,
    producer: PRODUCERS.ledger,
    correlationId: batchId,
    causationId: `evt_fee_${n}`,
    aggregateId: batchId,
    payload: {
      batchId,
      merchantId: overrides.merchantId ?? 'txp_riyadh_retail',
      merchantName: 'Riyadh Orbit E-Commerce Co.',
      currency: 'SAR' as const,
      window: { from: null, to: new Date().toISOString() },
      itemCount: 3,
      grossHalalas: gross,
      fee: f,
      payoutHalalas: gross - f.totalHalalas,
      journalRef: overrides.journalRef ?? `stl_${batchId}`,
      journal: [
        { walletId: 'wallet_merchant_riyadh_retail', role: 'MERCHANT_REVENUE' as const, direction: 'DEBIT' as const, amountHalalas: gross },
        { walletId: 'wallet_fee_treasury_sar', role: 'PLATFORM_FEES' as const, direction: 'CREDIT' as const, amountHalalas: f.totalHalalas },
        { walletId: 'wallet_payout_riyadh_retail', role: 'MERCHANT_PAYOUT' as const, direction: 'CREDIT' as const, amountHalalas: gross - f.totalHalalas },
      ],
      accounts: { revenueWalletId: 'wallet_merchant_riyadh_retail', feeWalletId: 'wallet_fee_treasury_sar', payoutWalletId: 'wallet_payout_riyadh_retail' },
      firstSettledAt: new Date(Date.now() - 3600_000).toISOString(),
      lastSettledAt: new Date().toISOString(),
      postedAt: new Date().toISOString(),
    },
  }) as MerchantSettlementPostedEvent;
}

async function outboxEvents() {
  const db = await getDb();
  return (await db.query<{ event_id: string; event_type: string; body: unknown }>(`SELECT event_id, event_type, body FROM integration_outbox ORDER BY seq`)).rows;
}

describe('Settlement documents: platform fee invoice + payout credit note (ICV / PIH, clearance, exactly once)', () => {
  let ctx: AppContext;
  const submissions: Array<{ mode: SubmissionMode | undefined; uuid: string }> = [];

  beforeEach(async () => {
    await resetDatabaseAndSeed();
    ctx = await buildApp();
    submissions.length = 0;
    ctx.reportingService.setTransport(async (payload, context) => {
      submissions.push({ mode: context?.mode, uuid: payload.uuid });
      return OK;
    });
  });

  it('issues the fee invoice (our net revenue + VAT) and the payout credit note, consecutive in the platform chain, cleared', async () => {
    const event = posted();
    const result = await ctx.settlementInvoicingService.handleSettlementPosted(event);
    expect(result.outcome).toBe('REPORTED');
    expect(result.documents?.map((d) => [d.role, d.icv, d.status])).toEqual([
      ['FEE_INVOICE', 1, 'CLEARED'],
      ['PAYOUT_CREDIT_NOTE', 2, 'CLEARED'],
    ]);

    const [feeDoc, note] = (await ctx.settlementInvoicingService.getSettlementDocuments(event.payload.batchId)).invoices;
    expect(feeDoc).toMatchObject({
      taxpayer_id: PLATFORM_TAXPAYER_ID,
      invoice_type_code: '388',
      invoice_subtype: '0100000',
      payment_means_code: '97',
      customer_name: 'Riyadh Orbit E-Commerce Co.',
      customer_vat_number: '310123456700003',
      taxable_amount_halalas: 1650,
      vat_amount_halalas: 248,
      total_amount_halalas: 1898,
      status: 'CLEARED',
      zatca_reporting_status: 'CLEARED',
    });
    expect(feeDoc.lines[0]).toMatchObject({ vat_category: 'S', unit_price_halalas: 1650, vat_amount_halalas: 248 });
    expect(note).toMatchObject({
      invoice_type_code: '381',
      invoice_subtype: '0100000',
      billing_reference_id: feeDoc.id,
      taxable_amount_halalas: 60000 - 1898,
      vat_amount_halalas: 0,
      total_amount_halalas: 60000 - 1898,
      previous_invoice_hash: feeDoc.invoice_hash,
      icv: 2,
    });
    expect(note.invoice_number).toMatch(/^CN-/);
    expect(note.instruction_note).toContain(event.payload.batchId);
    expect(note.lines[0]).toMatchObject({ vat_category: 'O', vat_exemption_code: 'VATEX-SA-OOS' });

    // Standard documents go to the clearance API, fee invoice first.
    expect(submissions.map((s) => [s.mode, s.uuid])).toEqual([
      ['CLEARANCE', feeDoc.uuid],
      ['CLEARANCE', note.uuid],
    ]);
    const chain = await ctx.invoiceService.verifyHashChain(PLATFORM_TAXPAYER_ID);
    expect(chain).toMatchObject({ ok: true, length: 2, lastIcv: 2 });
    // The merchant's own sales chain is untouched.
    expect((await ctx.invoiceService.getTaxpayer('txp_riyadh_retail')).last_icv).toBe(0);

    // UBL: buyer VAT, billing reference and reason for the credit note.
    const noteXml = Buffer.from(note.xml_ubl_base64 as string, 'base64').toString('utf8');
    expect(noteXml).toContain('<cbc:InvoiceTypeCode name="0100000">381</cbc:InvoiceTypeCode>');
    expect(noteXml).toContain(`<cbc:ID>${feeDoc.id}</cbc:ID>`);
    expect(noteXml).toContain('<cbc:CompanyID>310123456700003</cbc:CompanyID>');
    expect(noteXml).toMatch(/<cbc:InstructionNote>Settlement payout of batch/);
    expect(noteXml).toContain('<cbc:TaxExemptionReasonCode>VATEX-SA-OOS</cbc:TaxExemptionReasonCode>');

    const [announced] = await outboxEvents();
    expect(announced.event_type).toBe(EVENT_TYPES.settlementDocumentsReported);
    const parsed = SettlementDocumentsReportedSchema.parse(announced.body);
    expect(parsed.payload.documents.map((d) => d.role)).toEqual(['FEE_INVOICE', 'PAYOUT_CREDIT_NOTE']);
    expect(parsed.payload.documents[1].document.billingReference).toEqual({ invoiceId: feeDoc.id, invoiceNumber: feeDoc.invoice_number });
    expect(parsed.payload.settlement.batchId).toBe(event.payload.batchId);
    expect(Buffer.from(parsed.payload.documents[0].document.xmlUblBase64, 'base64').toString('utf8')).toContain('<Invoice');
  });

  it('renders both documents from the event alone (titles, buyer VAT, reason, billing reference)', async () => {
    await ctx.settlementInvoicingService.handleSettlementPosted(posted());
    const parsed = SettlementDocumentsReportedSchema.parse((await outboxEvents())[0].body);
    const texts: string[] = [];
    for (const { document } of parsed.payload.documents) {
      const pdf = await renderInvoiceDocumentPdf(document);
      expect(pdf.subarray(0, 5).toString('ascii')).toBe('%PDF-');
      const out = spawnSync('pdftotext', ['-', '-'], { input: pdf });
      if (out.status === 0) texts.push(out.stdout.toString('utf8'));
    }
    if (texts.length === 2) {
      expect(texts[0]).toContain('TAX INVOICE (B2B - 0100000)');
      expect(texts[0]).toContain('Customer VAT Registration Number: 310123456700003');
      expect(texts[0]).toContain('Total VAT: 2.48 SAR');
      expect(texts[1]).toContain('CREDIT NOTE (381 - 0100000)');
      expect(texts[1]).toContain('Reason for Issuance: Settlement payout');
      expect(texts[1]).toContain(`Billing Reference Invoice: ${parsed.payload.documents[0].document.invoiceNumber}`);
    }
  });

  it('never documents a batch twice: redeliveries and re-announcements reuse the same ICVs', async () => {
    const event = posted();
    await ctx.settlementInvoicingService.handleSettlementPosted(event);
    expect((await ctx.settlementInvoicingService.handleSettlementPosted(event)).outcome).toBe('DUPLICATE_IGNORED');
    const again = await ctx.settlementInvoicingService.handleSettlementPosted({ ...event, eventId: 'evt_posted_again' });
    expect(again.documents?.map((d) => d.icv)).toEqual([1, 2]);
    expect((await ctx.invoiceService.getTaxpayer(PLATFORM_TAXPAYER_ID)).last_icv).toBe(2);
    expect((await outboxEvents()).length).toBe(1); // same deterministic event id: announced once
    expect(submissions).toHaveLength(2);

    const checkpoints = await listCheckpoints(await getDb(), { correlationId: event.payload.batchId });
    expect(checkpoints.filter((c) => c.consumer === ZATCA_SETTLEMENT_DOCUMENTS_CONSUMER).map((c) => `${c.eventId}:${c.step}`).sort()).toEqual(
      [`${event.eventId}:issued`, `${event.eventId}:done`, 'evt_posted_again:issued', 'evt_posted_again:done'].sort()
    );
  });

  it('a Fatoora outage retries the submission only: the ICVs are kept, nothing is issued twice', async () => {
    let calls = 0;
    ctx.reportingService.setTransport(async () => (++calls <= 3 ? DOWN : OK));
    const event = posted();
    await expect(ctx.settlementInvoicingService.handleSettlementPosted(event)).rejects.toThrow(/ZATCA_TRANSIENT_ERROR/);
    await expect(ctx.settlementInvoicingService.handleSettlementPosted(event)).rejects.toThrow(/ZATCA_TRANSIENT_ERROR/);
    expect(await outboxEvents()).toHaveLength(0); // not reported yet: nothing announced
    await expect(ctx.settlementInvoicingService.handleSettlementPosted(event)).rejects.toThrow(/ZATCA_TRANSIENT_ERROR/);
    const result = await ctx.settlementInvoicingService.handleSettlementPosted(event);
    expect(result.documents?.map((d) => [d.icv, d.status])).toEqual([
      [1, 'CLEARED'],
      [2, 'CLEARED'],
    ]);
    expect((await ctx.invoiceService.getTaxpayer(PLATFORM_TAXPAYER_ID)).last_icv).toBe(2);
    expect(await outboxEvents()).toHaveLength(1);
  });

  it('dead-letters a rejection and a fee the invoice cannot reproduce', async () => {
    ctx.reportingService.setTransport(async () => REJECTED);
    await expect(ctx.settlementInvoicingService.handleSettlementPosted(posted())).rejects.toMatchObject({ reason: 'ZATCA_REJECTED' });

    ctx.reportingService.setTransport(async () => OK);
    const wrong = posted({ feeNet: 1000, feeVat: 149 }); // the engine computes 150
    const err = await ctx.settlementInvoicingService.handleSettlementPosted(wrong).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(NonRetryableError);
    expect((err as NonRetryableError).reason).toBe('LEDGER_TAX_MISMATCH');
    // Nothing was issued for it: the transaction rolled back (no ICV burnt).
    expect((await ctx.settlementInvoicingService.getSettlementDocuments(wrong.payload.batchId)).invoices).toHaveLength(0);
    expect((await ctx.invoiceService.getTaxpayer(PLATFORM_TAXPAYER_ID)).last_icv).toBe(2);
  });

  it('a merchant without a VAT registration gets simplified documents, reported instead of cleared', async () => {
    const result = await ctx.settlementInvoicingService.handleSettlementPosted(posted({ merchantId: 'txp_unregistered_shop' }));
    expect(result.outcome).toBe('REPORTED');
    const docs = (await ctx.settlementInvoicingService.getSettlementDocuments(result.batchId)).invoices;
    expect(docs.map((d) => [d.invoice_type_code, d.invoice_subtype, d.status, d.customer_vat_number])).toEqual([
      ['388', '0200000', 'REPORTED', null],
      ['381', '0200000', 'REPORTED', null],
    ]);
    expect(submissions.map((s) => s.mode)).toEqual(['REPORTING', 'REPORTING']);
  });

  it('exposes the documents of a batch over HTTP, and the clearance API refuses simplified invoices', async () => {
    const event = posted();
    await ctx.settlementInvoicingService.handleSettlementPosted(event);
    const res = await ctx.app.inject({ method: 'GET', url: `/api/v1/settlements/${event.payload.batchId}/documents` });
    expect(res.statusCode).toBe(200);
    expect(res.json().invoices.map((i: { invoice_type_code: string }) => i.invoice_type_code)).toEqual(['388', '381']);

    const draft = await ctx.invoiceService.createDraftInvoice({
      taxpayerId: 'txp_riyadh_retail',
      customerName: 'Walk-in',
      lines: [{ itemNameAr: 'س', itemNameEn: 'x', sku: 'X', quantity: 1, unitPriceHalalas: 100 }],
    });
    await ctx.invoiceService.issueInvoice(draft.id);
    const refused = await ctx.app.inject({ method: 'POST', url: `/api/v1/invoices/${draft.id}/clearance` });
    expect(refused.statusCode).toBe(400);
    expect(refused.json().error).toBe('CANNOT_CLEAR_SIMPLIFIED_INVOICE');
  });
});
