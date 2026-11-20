import crypto from 'node:crypto';
import type { PGlite } from '@electric-sql/pglite';
import { InvoiceService, ZatcaExecutor } from '../services/InvoiceService';
import { ReportingService } from '../services/ReportingService';
import { TaxCalculationEngine } from '../services/TaxCalculationEngine';
import type { InvoiceLineInput, InvoiceWithLines, VatExemptionCode } from '../types/zatca';
import { invoiceDocumentSnapshot } from './OrderInvoicingService';
import {
  CONSUMERS,
  EVENT_TYPES,
  EXCHANGES,
  PRODUCERS,
  ROUTING_KEYS,
  buildIntegrationEvent,
  deterministicEventId,
  halalasToSar,
  type CreditNoteReportedPayload,
  type LedgerPaymentRefundedEvent,
} from '../../../../shared/contracts';
import {
  NonRetryableError,
  claimCheckpoint,
  completeCheckpoint,
  enqueueIntegrationEvent,
  listCheckpoints,
  readCheckpoint,
  type Checkpoint,
} from '../../../../shared/messaging';

export const ZATCA_REFUND_CONSUMER = CONSUMERS.zatcaLedgerRefunds;

export type RefundInvoicingOutcome = 'REPORTED' | 'REPORTED_WITH_WARNINGS' | 'CLEARED' | 'DUPLICATE_IGNORED';

export interface RefundInvoicingResult {
  outcome: RefundInvoicingOutcome;
  refundId: string;
  orderId: string;
  creditNoteId?: string;
  icv?: number;
  eventId?: string;
}

const ACCEPTED = new Set(['REPORTED', 'REPORTED_WITH_WARNINGS', 'CLEARED']);
const PERMANENT_ERRORS = /^(INVALID_|EMPTY_LINE_ITEMS|TAXPAYER_NOT_FOUND|TAXPAYER_CSID_REVOKED|CANNOT_)/;

const CATEGORY_LABEL: Record<string, { en: string; ar: string }> = {
  S: { en: 'standard-rated items', ar: 'سلع خاضعة للنسبة الأساسية' },
  Z: { en: 'zero-rated items', ar: 'سلع خاضعة لنسبة الصفر' },
  E: { en: 'exempt items', ar: 'سلع معفاة' },
  O: { en: 'out-of-scope items', ar: 'سلع خارج نطاق الضريبة' },
};

/**
 * ZATCA's part of a customer refund: a credit note (381) against the order's invoice.
 *
 * The credit note carries one line per VAT category of the refund split the ledger posted, with
 * the pro-rata VAT the ledger reversed (the same share of the VAT the invoice charged), so the
 * credit note, the reversal journal and the money returned to the customer agree to the halala.
 * Σ credit notes of an invoice can never exceed the invoice total (checked here, and by the
 * ledger against the capture). One credit note per refund id (unique index), consecutive ICVs
 * in the merchant's chain, reported (simplified) or cleared (standard) like the invoice was.
 *
 * Per-step checkpoints (consumer zatca.ledger-refunds, per event): 'issued' (draft + issue +
 * checkpoint in one transaction under the taxpayer lock), then the submission (retried alone on a
 * Fatoora outage: no ICV is burned), then 'done' (outbox event + checkpoint).
 */
export class RefundInvoicingService {
  constructor(
    private readonly db: PGlite,
    private readonly invoices: InvoiceService,
    private readonly reporting: ReportingService
  ) {}

  async handleLedgerPaymentRefunded(event: LedgerPaymentRefundedEvent): Promise<RefundInvoicingResult> {
    const p = event.payload;
    const { refund } = p;
    const key = { consumer: ZATCA_REFUND_CONSUMER, eventId: event.eventId };

    // ── step 1: issue the credit note exactly once ────────────────────────────
    const issued = await this.withPermanentErrors(() =>
      this.db.transaction(async (tx): Promise<{ duplicate: true } | { duplicate: false; creditNoteId: string }> => {
        if (await readCheckpoint(tx, { ...key, step: 'done' })) return { duplicate: true };
        const prior = await readCheckpoint(tx, { ...key, step: 'issued' });
        if (prior) return { duplicate: false, creditNoteId: String(prior.detail.creditNoteId) };

        const taxpayer = await this.invoices.getTaxpayer(refund.merchantId, tx);
        const existing = await tx.query<{ id: string; status: string }>(
          `SELECT id, status FROM invoices WHERE taxpayer_id = $1 AND source_refund_id = $2 AND invoice_type_code = '381'`,
          [taxpayer.id, refund.refundId]
        );
        let creditNoteId: string;
        if (existing.rows.length > 0) {
          creditNoteId = existing.rows[0].id;
          if (existing.rows[0].status === 'DRAFT') await this.invoices.issueInvoiceWithin(tx, creditNoteId);
        } else {
          creditNoteId = await this.issueCreditNote(tx, event, taxpayer.id);
        }
        const note = await this.invoices.getInvoice(creditNoteId, tx);
        await claimCheckpoint(tx, { ...key, step: 'issued', correlationId: refund.orderId });
        await completeCheckpoint(tx, {
          ...key,
          step: 'issued',
          outcome: 'ISSUED',
          detail: { creditNoteId, icv: note.icv, invoiceHash: note.invoice_hash, billingReferenceId: note.billing_reference_id },
        });
        return { duplicate: false, creditNoteId };
      })
    );
    if (issued.duplicate) return { outcome: 'DUPLICATE_IGNORED', refundId: refund.refundId, orderId: refund.orderId };
    const { creditNoteId } = issued;

    // ── step 2: submit once; ── step 3: announce once ──────────────────────────
    let current = await this.invoices.getInvoice(creditNoteId);
    if (current.status === 'ISSUED') {
      current = await this.withPermanentErrors(() =>
        current.invoice_subtype === '0100000' ? this.reporting.clearStandardInvoice(creditNoteId) : this.reporting.reportSimplifiedInvoice(creditNoteId)
      );
    }
    if (current.status === 'REJECTED') {
      throw new NonRetryableError('ZATCA_REJECTED', `Fatoora rejected credit note ${creditNoteId} (ICV ${current.icv}): ${JSON.stringify(current.zatca_errors)}`);
    }
    if (!ACCEPTED.has(current.status)) {
      throw new Error(`ZATCA_TRANSIENT_ERROR: Fatoora did not accept credit note ${creditNoteId} yet (ICV ${current.icv})`);
    }

    const eventId = deterministicEventId(EVENT_TYPES.creditNoteReported, refund.refundId);
    await this.db.transaction(async (tx) => {
      if (!(await claimCheckpoint(tx, { ...key, step: 'done', correlationId: refund.orderId }))) return;
      const note = await this.invoices.getInvoice(creditNoteId, tx);
      const original = await this.invoices.getInvoice(note.billing_reference_id as string, tx);
      const taxpayer = await this.invoices.getTaxpayer(note.taxpayer_id, tx);
      const credited = await this.creditedTotal(tx, original.id);
      const notes = await tx.query<{ id: string; invoice_number: string; icv: number; total_amount_halalas: string | number; source_refund_id: string | null; zatca_reported_at: Date | string | null }>(
        `SELECT id, invoice_number, icv, total_amount_halalas, source_refund_id, zatca_reported_at FROM invoices
         WHERE billing_reference_id = $1 AND invoice_type_code = '381' AND status <> 'REJECTED' ORDER BY icv`,
        [original.id]
      );
      const payload: CreditNoteReportedPayload = {
        refund,
        journalRef: p.journalRef,
        originalInvoiceId: original.id,
        originalInvoiceNumber: original.invoice_number,
        creditedTotalHalalas: credited,
        creditNote: {
          ...invoiceDocumentSnapshot(note, taxpayer, { invoiceId: original.id, invoiceNumber: original.invoice_number }),
          xmlUblBase64: note.xml_ubl_base64 as string,
        },
        invoice: invoiceDocumentSnapshot(original, taxpayer),
        creditNotes: notes.rows.map((n) => ({
          invoiceId: n.id,
          invoiceNumber: n.invoice_number,
          icv: Number(n.icv),
          totalHalalas: Number(n.total_amount_halalas),
          refundId: n.source_refund_id,
          reportedAt: n.zatca_reported_at ? new Date(n.zatca_reported_at).toISOString() : null,
        })),
        reportedAt: new Date().toISOString(),
      };
      await enqueueIntegrationEvent(tx, {
        exchange: EXCHANGES.zatca.name,
        routingKey: ROUTING_KEYS.creditNoteReported,
        event: buildIntegrationEvent({
          eventId,
          eventType: EVENT_TYPES.creditNoteReported,
          producer: PRODUCERS.zatca,
          correlationId: refund.orderId,
          causationId: event.eventId,
          aggregateId: original.id,
          payload,
        }),
      });
      await completeCheckpoint(tx, { ...key, step: 'done', outcome: note.status, detail: { creditNoteId, icv: note.icv, eventId, creditedTotalHalalas: credited } });
    });

    return { outcome: current.status as RefundInvoicingOutcome, refundId: refund.refundId, orderId: refund.orderId, creditNoteId, icv: current.icv ?? undefined, eventId };
  }

  /** Credit notes issued for an order's refunds, with the consumer checkpoints. */
  async getOrderCreditNotes(orderId: string): Promise<{ creditNotes: InvoiceWithLines[]; checkpoints: Checkpoint[] }> {
    const res = await this.db.query<{ id: string }>(
      `SELECT n.id FROM invoices n JOIN invoices i ON i.id = n.billing_reference_id
       WHERE i.source_order_id = $1 AND n.invoice_type_code = '381' AND n.source_refund_id IS NOT NULL
       ORDER BY n.icv NULLS LAST, n.created_at`,
      [orderId]
    );
    const creditNotes: InvoiceWithLines[] = [];
    for (const row of res.rows) creditNotes.push(await this.invoices.getInvoice(row.id));
    return { creditNotes, checkpoints: await listCheckpoints(this.db, { correlationId: orderId, consumer: ZATCA_REFUND_CONSUMER }) };
  }

  // ── internals ──────────────────────────────────────────────────────────────

  private async issueCreditNote(tx: ZatcaExecutor, event: LedgerPaymentRefundedEvent, taxpayerId: string): Promise<string> {
    const p = event.payload;
    const { refund } = p;
    const originalRes = await tx.query<{ id: string }>(
      `SELECT id FROM invoices WHERE taxpayer_id = $1 AND source_order_id = $2 AND invoice_type_code = '388'`,
      [taxpayerId, refund.orderId]
    );
    if (originalRes.rows.length === 0) {
      throw new NonRetryableError('ORDER_NOT_INVOICED', `Order ${refund.orderId} has no invoice to credit for refund ${refund.refundId}`);
    }
    const original = await this.invoices.getInvoice(originalRes.rows[0].id, tx);
    if (!ACCEPTED.has(original.status)) {
      throw new NonRetryableError('CANNOT_CREDIT_UNREPORTED_INVOICE', `Invoice ${original.id} of order ${refund.orderId} is ${original.status}`);
    }
    const credited = await this.creditedTotal(tx, original.id);
    if (credited + refund.amountHalalas > original.total_amount_halalas) {
      throw new NonRetryableError(
        'CREDIT_NOTES_EXCEED_INVOICE',
        `Refund ${refund.refundId} of ${refund.amountHalalas} would credit ${credited + refund.amountHalalas} against invoice ${original.invoice_number} of ${original.total_amount_halalas}`
      );
    }

    // The invoice's own rounding per category bounds how far a pro-rata VAT may sit from net × rate.
    const breakdown = TaxCalculationEngine.breakdownForInvoice(original);
    const tolerance = breakdown.reduce(
      (max, c) => Math.max(max, Math.abs(c.vatAmountHalalas - Math.round((c.taxableAmountHalalas * c.vatRateBps) / 10000)) + 2),
      2
    );
    const lines: InvoiceLineInput[] = p.lines
      .filter((l) => l.netHalalas > 0 || l.vatHalalas > 0)
      .map((l) => {
        const label = CATEGORY_LABEL[l.vatCategory] ?? CATEGORY_LABEL.S;
        const exemption = (l.vatExemptionCode ?? original.lines.find((ol) => ol.vat_category === l.vatCategory)?.vat_exemption_code ?? undefined) as
          | VatExemptionCode
          | undefined;
        return {
          sku: `REFUND-${l.vatCategory}`,
          itemNameEn: `Refund of order ${refund.orderId} - ${label.en}`,
          itemNameAr: `استرداد للطلب ${refund.orderId} - ${label.ar}`,
          quantity: 1,
          unitPriceHalalas: l.netHalalas,
          vatCategory: l.vatCategory,
          ...(l.vatCategory === 'S' ? {} : { vatExemptionCode: exemption }),
          vatAmountHalalas: l.vatHalalas,
        };
      });
    const remaining = original.total_amount_halalas - credited - refund.amountHalalas;
    const draft = await this.invoices.createDraftInvoice(
      {
        taxpayerId,
        egsDeviceId: original.egs_device_id ?? undefined,
        invoiceNumber: `CN-${new Date().getUTCFullYear()}-${crypto.randomUUID().slice(0, 6).toUpperCase()}`,
        invoiceTypeCode: '381',
        invoiceSubtype: original.invoice_subtype,
        paymentMeansCode: original.payment_means_code,
        customerName: original.customer_name,
        customerVatNumber: original.customer_vat_number ?? undefined,
        customerPhone: original.customer_phone ?? undefined,
        customerEmail: original.customer_email ?? undefined,
        billingReferenceId: original.id,
        instructionNote:
          `Customer refund ${refund.refundId}: ${halalasToSar(refund.amountHalalas)} SAR refunded ` +
          `(${remaining === 0 ? 'final' : 'partial'}; ${halalasToSar(refund.refundedTotalHalalas)} of ${halalasToSar(refund.capturedHalalas)} SAR refunded so far)` +
          (refund.reason ? ` - ${refund.reason}` : ''),
        lines,
        sourceRefundId: refund.refundId,
        sourceOrderId: refund.orderId,
        sourceJournalRef: p.journalRef,
        sourceEventId: event.eventId,
        explicitVatToleranceHalalas: tolerance,
      },
      tx
    );
    if (draft.taxable_amount_halalas !== p.netHalalas || draft.vat_amount_halalas !== p.vatHalalas || draft.total_amount_halalas !== refund.amountHalalas) {
      throw new NonRetryableError(
        'LEDGER_TAX_MISMATCH',
        `Refund ${refund.refundId}: the ledger reversed ${p.netHalalas} + VAT ${p.vatHalalas} = ${refund.amountHalalas}, ` +
          `the credit note computes ${draft.taxable_amount_halalas} + ${draft.vat_amount_halalas} = ${draft.total_amount_halalas}`
      );
    }
    await this.invoices.issueInvoiceWithin(tx, draft.id);
    return draft.id;
  }

  /** Σ non-rejected credit notes already issued against an invoice. */
  private async creditedTotal(exec: ZatcaExecutor, invoiceId: string): Promise<number> {
    const res = await exec.query<{ total: string }>(
      `SELECT COALESCE(SUM(total_amount_halalas), 0)::text AS total FROM invoices
       WHERE billing_reference_id = $1 AND invoice_type_code = '381' AND status <> 'REJECTED'`,
      [invoiceId]
    );
    return Number(res.rows[0].total);
  }

  private async withPermanentErrors<T>(work: () => Promise<T>): Promise<T> {
    try {
      return await work();
    } catch (err) {
      if (err instanceof Error && !(err instanceof NonRetryableError) && PERMANENT_ERRORS.test(err.message)) {
        throw new NonRetryableError(err.message.split(/[ :]/)[0], err.message);
      }
      throw err;
    }
  }
}
