import type { PGlite } from '@electric-sql/pglite';
import { InvoiceService, ZatcaExecutor } from '../services/InvoiceService';
import { TaxCalculationEngine } from '../services/TaxCalculationEngine';
import { ReportingService } from '../services/ReportingService';
import type { InvoiceWithLines, TaxpayerRecord } from '../types/zatca';
import {
  CONSUMERS,
  EVENT_TYPES,
  EXCHANGES,
  InvoiceDocument,
  InvoiceReportedPayload,
  LedgerPaymentSettledEvent,
  PRODUCERS,
  ROUTING_KEYS,
  buildIntegrationEvent,
  deterministicEventId,
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

export const ZATCA_SETTLEMENT_CONSUMER = CONSUMERS.zatcaLedgerSettlements;

export type InvoicingOutcome = 'REPORTED' | 'REPORTED_WITH_WARNINGS' | 'DUPLICATE_IGNORED';

export interface InvoicingResult {
  outcome: InvoicingOutcome;
  orderId: string;
  invoiceId?: string;
  icv?: number;
  /** eventId of the zatca.invoice.reported event (deterministic per invoice). */
  eventId?: string;
}

/** Validation failures of the tax engine / invoice service (never fixed by retrying). */
const PERMANENT_ERRORS =
  /^(INVALID_|EMPTY_LINE_ITEMS|TAXPAYER_NOT_FOUND|TAXPAYER_CSID_REVOKED|CANNOT_|CREDIT_NOTE_EXCEEDS)/;

/**
 * ZATCA's part of the order flow: a settled ledger payment becomes a Simplified Tax Invoice (388 /
 * 0200000) with the captured credits applied as a document-level allowance, stamped into the
 * seller's ICV / PIH hash chain and reported to Fatoora.
 *
 * Per-step checkpoints (consumer zatca.ledger-settlements, per event):
 *   'issued'  draft + issue (next ICV, PIH = previous hash, ECDSA stamp, QR, UBL) + checkpoint,
 *             ONE transaction under the taxpayer row lock
 *   'done'    Fatoora accepted it: submission log + invoice status + zatca.invoice.reported outbox
 *             event + checkpoint, ONE transaction
 * A Fatoora outage (5xx) leaves the invoice ISSUED and the message is retried with backoff: the
 * retry finds 'issued' and only reports again, so a retry can never consume a second ICV. The
 * database additionally refuses a second invoice for the same order and any repeated ICV / PIH.
 */
export class OrderInvoicingService {
  constructor(
    private readonly db: PGlite,
    private readonly invoices: InvoiceService,
    private readonly reporting: ReportingService
  ) {}

  async handleLedgerPaymentSettled(event: LedgerPaymentSettledEvent): Promise<InvoicingResult> {
    const p = event.payload;
    const key = { consumer: ZATCA_SETTLEMENT_CONSUMER, eventId: event.eventId };

    // ── step 1: issue exactly once ─────────────────────────────────────────────
    const issued = await this.withPermanentErrors(() =>
      this.db.transaction(async (tx): Promise<{ duplicate: true } | { duplicate: false; invoiceId: string }> => {
        if (await readCheckpoint(tx, { ...key, step: 'done' })) return { duplicate: true };
        const prior = await readCheckpoint(tx, { ...key, step: 'issued' });
        if (prior) return { duplicate: false, invoiceId: String(prior.detail.invoiceId) };

        const taxpayer = await this.invoices.getTaxpayer(p.merchantId, tx);
        const existing = await tx.query<{ id: string; status: string; source_journal_ref: string | null }>(
          `SELECT id, status, source_journal_ref FROM invoices
           WHERE taxpayer_id = $1 AND source_order_id = $2 AND invoice_type_code = '388'`,
          [taxpayer.id, p.orderId]
        );

        let invoiceId: string;
        if (existing.rows.length > 0) {
          const found = existing.rows[0];
          if (found.source_journal_ref && found.source_journal_ref !== p.journalRef) {
            throw new NonRetryableError(
              'ORDER_ALREADY_INVOICED',
              `Order ${p.orderId} is already invoiced (${found.id}) for journal ${found.source_journal_ref}, not ${p.journalRef}`
            );
          }
          invoiceId = found.id;
          if (found.status === 'DRAFT') await this.invoices.issueInvoiceWithin(tx, invoiceId);
        } else {
          invoiceId = await this.issueNewInvoice(tx, event, taxpayer);
        }

        const invoice = await this.invoices.getInvoice(invoiceId, tx);
        await claimCheckpoint(tx, { ...key, step: 'issued', correlationId: p.orderId });
        await completeCheckpoint(tx, {
          ...key,
          step: 'issued',
          outcome: 'ISSUED',
          detail: { invoiceId, icv: invoice.icv, invoiceHash: invoice.invoice_hash, pih: invoice.previous_invoice_hash },
        });
        return { duplicate: false, invoiceId };
      })
    );
    if (issued.duplicate) return { outcome: 'DUPLICATE_IGNORED', orderId: p.orderId };
    const { invoiceId } = issued;

    // ── step 2: report to Fatoora, then announce, exactly once ────────────────
    let current = await this.invoices.getInvoice(invoiceId);
    if (current.status === 'ISSUED') {
      current = await this.reporting.reportSimplifiedInvoice(invoiceId, {
        onRecorded: async (tx, outcome) => {
          if (outcome === 'REPORTED' || outcome === 'REPORTED_WITH_WARNINGS') {
            await this.finalize(tx, event, invoiceId);
          }
        },
      });
    } else if (current.status === 'REPORTED' || current.status === 'REPORTED_WITH_WARNINGS') {
      // Reported by an earlier delivery that died before finalizing (or via the API).
      await this.db.transaction((tx) => this.finalize(tx, event, invoiceId));
    }

    if (current.status === 'REJECTED') {
      throw new NonRetryableError(
        'ZATCA_REJECTED',
        `Fatoora rejected invoice ${invoiceId} (ICV ${current.icv}): ${JSON.stringify(current.zatca_errors)}`
      );
    }
    if (current.status === 'ISSUED') {
      // 5xx / transient: the invoice keeps its ICV; the message is retried with backoff.
      throw new Error(`ZATCA_TRANSIENT_ERROR: Fatoora did not accept invoice ${invoiceId} yet (ICV ${current.icv})`);
    }

    const done = await readCheckpoint(this.db, { ...key, step: 'done' });
    return {
      outcome: current.status as 'REPORTED' | 'REPORTED_WITH_WARNINGS',
      orderId: p.orderId,
      invoiceId,
      icv: current.icv ?? undefined,
      eventId: done ? String(done.detail.eventId) : undefined,
    };
  }

  /** Invoices created by the order flow for an order, with the consumer checkpoints. */
  async getOrderInvoicing(orderId: string): Promise<{ invoices: InvoiceWithLines[]; checkpoints: Checkpoint[] }> {
    const res = await this.db.query<{ id: string }>(
      `SELECT id FROM invoices WHERE source_order_id = $1 ORDER BY created_at`,
      [orderId]
    );
    const invoices: InvoiceWithLines[] = [];
    for (const row of res.rows) invoices.push(await this.invoices.getInvoice(row.id));
    return { invoices, checkpoints: await listCheckpoints(this.db, { correlationId: orderId }) };
  }

  // ── internals ────────────────────────────────────────────────────────────────

  private async issueNewInvoice(tx: ZatcaExecutor, event: LedgerPaymentSettledEvent, taxpayer: TaxpayerRecord): Promise<string> {
    const p = event.payload;
    const egs = await tx.query<{ id: string }>(
      `SELECT id FROM egs_devices WHERE taxpayer_id = $1 AND status = 'ACTIVE' ORDER BY created_at, id LIMIT 1`,
      [taxpayer.id]
    );
    const draft = await this.invoices.createDraftInvoice(
      {
        taxpayerId: taxpayer.id,
        egsDeviceId: egs.rows[0]?.id,
        invoiceTypeCode: '388',
        invoiceSubtype: '0200000',
        // UNCL 4461: 48 bank card; 30 credit transfer (the customer's stored-value wallet balance).
        paymentMeansCode: p.tender === 'WALLET' ? '30' : '48',
        customerName: p.order.customer.name,
        customerEmail: p.order.customer.email,
        customerPhone: p.order.customer.phone,
        // The captured promotional credits, as a document-level allowance (UBL AllowanceCharge,
        // reason code 95, allocated over the VAT categories).
        documentAllowanceHalalas: p.allowanceHalalas,
        documentAllowanceReason: p.allowanceHalalas > 0 ? promotionReason(p.capturedCredits) : undefined,
        lines: p.order.lines.map((line) => ({
          itemNameAr: line.nameAr,
          itemNameEn: line.nameEn,
          sku: line.sku,
          quantity: line.quantity,
          unitPriceHalalas: line.unitPriceHalalas,
          discountHalalas: line.discountHalalas,
          vatCategory: line.vatCategory,
          vatExemptionCode: line.vatExemptionCode,
        })),
        sourceOrderId: p.orderId,
        sourceJournalRef: p.journalRef,
        sourceEventId: event.eventId,
      },
      tx
    );

    // The invoice must state exactly what the ledger booked; otherwise nothing is issued.
    if (
      draft.taxable_amount_halalas !== p.netRevenueHalalas ||
      draft.vat_amount_halalas !== p.vatHalalas ||
      draft.total_amount_halalas !== p.grossHalalas
    ) {
      throw new NonRetryableError(
        'LEDGER_TAX_MISMATCH',
        `Order ${p.orderId}: ledger booked net ${p.netRevenueHalalas} / VAT ${p.vatHalalas} / gross ${p.grossHalalas}, ` +
          `invoice computes ${draft.taxable_amount_halalas} / ${draft.vat_amount_halalas} / ${draft.total_amount_halalas}`
      );
    }

    await this.invoices.issueInvoiceWithin(tx, draft.id);
    return draft.id;
  }

  private async finalize(tx: ZatcaExecutor, event: LedgerPaymentSettledEvent, invoiceId: string): Promise<void> {
    const key = { consumer: ZATCA_SETTLEMENT_CONSUMER, eventId: event.eventId, step: 'done' };
    if (!(await claimCheckpoint(tx, { ...key, correlationId: event.payload.orderId }))) return;

    const invoice = await this.invoices.getInvoice(invoiceId, tx);
    const taxpayer = await this.invoices.getTaxpayer(invoice.taxpayer_id, tx);
    const eventId = deterministicEventId(EVENT_TYPES.invoiceReported, invoiceId);
    await enqueueIntegrationEvent(tx, {
      exchange: EXCHANGES.zatca.name,
      routingKey: ROUTING_KEYS.invoiceReported,
      event: buildIntegrationEvent({
        eventId,
        eventType: EVENT_TYPES.invoiceReported,
        producer: PRODUCERS.zatca,
        correlationId: event.payload.orderId,
        causationId: event.eventId,
        aggregateId: invoiceId,
        payload: invoiceReportedPayload(invoice, taxpayer, event.payload.orderId, event.payload.journalRef),
      }),
    });
    await completeCheckpoint(tx, {
      ...key,
      outcome: invoice.status,
      detail: { invoiceId, icv: invoice.icv, eventId },
    });
  }

  /** Tax-engine validation errors are plain Errors with a code as message: never retry them. */
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

export function invoiceReportedPayload(
  invoice: InvoiceWithLines,
  taxpayer: TaxpayerRecord,
  orderId: string,
  journalRef: string
): InvoiceReportedPayload {
  return {
    ...invoiceDocumentSnapshot(invoice, taxpayer),
    orderId,
    journalRef,
    status: invoice.status as 'REPORTED' | 'REPORTED_WITH_WARNINGS',
  };
}

/**
 * The full snapshot of an issued (and reported / cleared) document that travels in integration
 * events, so that other services (the file cache) can render it without the ZATCA database.
 */
export function invoiceDocumentSnapshot(
  invoice: InvoiceWithLines,
  taxpayer: TaxpayerRecord,
  billingReference: { invoiceId: string; invoiceNumber: string } | null = null
): InvoiceDocument {
  return {
    invoiceId: invoice.id,
    invoiceNumber: invoice.invoice_number,
    uuid: invoice.uuid,
    invoiceTypeCode: invoice.invoice_type_code,
    invoiceSubtype: invoice.invoice_subtype,
    taxpayerId: invoice.taxpayer_id,
    icv: invoice.icv as number,
    invoiceHash: invoice.invoice_hash as string,
    previousInvoiceHash: invoice.previous_invoice_hash as string,
    qrBase64: invoice.tlv_qr_base64 as string,
    status: invoice.status as InvoiceDocument['status'],
    issuedAt: invoice.issued_at as string,
    reportedAt: invoice.zatca_reported_at ?? new Date().toISOString(),
    customerName: invoice.customer_name,
    totals: {
      subtotalHalalas: invoice.subtotal_halalas,
      discountHalalas: invoice.discount_halalas,
      allowanceHalalas: invoice.document_allowance_halalas,
      taxableHalalas: invoice.taxable_amount_halalas,
      vatHalalas: invoice.vat_amount_halalas,
      totalHalalas: invoice.total_amount_halalas,
    },
    lines: invoice.lines.map((l) => ({
      lineNumber: l.line_number,
      sku: l.sku,
      nameEn: l.item_name_en,
      nameAr: l.item_name_ar,
      quantity: l.quantity,
      unitPriceHalalas: l.unit_price_halalas,
      discountHalalas: l.discount_halalas,
      netAmountHalalas: l.net_amount_halalas,
      vatCategory: l.vat_category,
      vatRateBps: l.vat_rate_bps,
      vatAmountHalalas: l.vat_amount_halalas,
      lineTotalHalalas: l.line_total_halalas,
      vatExemptionCode: l.vat_exemption_code,
    })),
    allowanceReason: invoice.document_allowance_reason,
    vatBreakdown: TaxCalculationEngine.breakdownForInvoice(invoice).map((c) => ({
      vatCategory: c.vatCategory,
      vatRateBps: c.vatRateBps,
      exemptionCode: c.exemptionCode,
      lineNetHalalas: c.lineNetHalalas,
      allowanceHalalas: c.allowanceHalalas,
      taxableHalalas: c.taxableAmountHalalas,
      vatHalalas: c.vatAmountHalalas,
    })),
    seller: {
      legalNameEn: taxpayer.legal_name_en,
      legalNameAr: taxpayer.legal_name_ar,
      vatNumber: taxpayer.vat_number,
      crnNumber: taxpayer.crn_number,
      buildingNumber: taxpayer.building_number,
      streetName: taxpayer.street_name,
      city: taxpayer.city,
      postalCode: taxpayer.postal_code,
      countryCode: taxpayer.country_code,
    },
    customerVatNumber: invoice.customer_vat_number,
    paymentMeansCode: invoice.payment_means_code,
    billingReference,
    instructionNote: invoice.instruction_note,
  };
}

/** How the promotional credits applied to an order are named on its invoice. */
export function promotionReason(capturedCredits: number): string {
  return `Promotional credit (${capturedCredits} credit${capturedCredits === 1 ? '' : 's'})`;
}
