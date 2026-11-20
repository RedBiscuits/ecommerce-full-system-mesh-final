import crypto from 'node:crypto';
import type { PGlite } from '@electric-sql/pglite';
import { InvoiceService, ZatcaExecutor } from '../services/InvoiceService';
import { ReportingService } from '../services/ReportingService';
import { TaxCalculationEngine } from '../services/TaxCalculationEngine';
import { PLATFORM_TAXPAYER_ID } from '../db';
import type { InvoiceWithLines, TaxpayerRecord } from '../types/zatca';
import { invoiceDocumentSnapshot } from './OrderInvoicingService';
import {
  CONSUMERS,
  EVENT_TYPES,
  EXCHANGES,
  PRODUCERS,
  ROUTING_KEYS,
  buildIntegrationEvent,
  deterministicEventId,
  type MerchantSettlementPostedEvent,
  type MerchantSettlementPostedPayload,
  type SettlementDocument,
  type SettlementDocumentRole,
  type SettlementDocumentsReportedPayload,
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

export const ZATCA_SETTLEMENT_DOCUMENTS_CONSUMER = CONSUMERS.zatcaMerchantSettlements;

export type SettlementDocumentsOutcome = 'REPORTED' | 'NO_DOCUMENTS' | 'DUPLICATE_IGNORED';

export interface SettlementDocumentsResult {
  outcome: SettlementDocumentsOutcome;
  batchId: string;
  documents?: Array<{ role: SettlementDocumentRole; invoiceId: string; icv: number | null; status: string }>;
  /** eventId of the zatca.settlement-documents.reported event (deterministic per batch). */
  eventId?: string;
}

/** Documents accepted by Fatoora (reported, or cleared for standard B2B documents). */
const ACCEPTED = new Set(['REPORTED', 'REPORTED_WITH_WARNINGS', 'CLEARED']);

/** Validation failures of the tax engine / invoice service (never fixed by retrying). */
const PERMANENT_ERRORS = /^(INVALID_|EMPTY_LINE_ITEMS|TAXPAYER_NOT_FOUND|TAXPAYER_CSID_REVOKED|CANNOT_)/;

const sar = (halalas: number): string => TaxCalculationEngine.formatSar(halalas);

interface IssuedDocs {
  feeInvoiceId: string | null;
  creditNoteId: string | null;
}

/**
 * ZATCA's part of the merchant settlement workflow. The platform (its own taxpayer, its own EGS
 * and ICV / PIH chain) documents a posted settlement towards the merchant:
 *
 *   FEE_INVOICE         tax invoice 388 for the processing fee: taxable = the platform's net fee
 *                       revenue, VAT 15 % — exactly the fee billing priced and the ledger booked
 *                       (verified to the halala, otherwise nothing is issued: dead letter)
 *   PAYOUT_CREDIT_NOTE  credit note 381 referencing the fee invoice for the payout credited to the
 *                       merchant (revenue collected on its behalf, out of the scope of VAT: O /
 *                       VATEX-SA-OOS, so it carries no VAT and changes no VAT return)
 *
 * A merchant that is a registered taxpayer (VAT number on file) gets standard B2B documents
 * (0100000), which are CLEARED by Fatoora; otherwise simplified documents (0200000), REPORTED.
 *
 * Per-step checkpoints (consumer zatca.merchant-settlements, per event):
 *   'issued'  both drafts + issuance (consecutive ICVs, PIH chained) + checkpoint — ONE transaction
 *             under the taxpayer row lock
 *   (submit)  each document once; a Fatoora outage (5xx) retries the submission only — a retry can
 *             never burn another ICV; a rejection dead-letters the message
 *   'done'    zatca.settlement-documents.reported outbox event + checkpoint — ONE transaction
 * The database refuses a second fee invoice / credit note for a batch and any repeated ICV / PIH.
 */
export class SettlementInvoicingService {
  constructor(
    private readonly db: PGlite,
    private readonly invoices: InvoiceService,
    private readonly reporting: ReportingService,
    private readonly platformTaxpayerId: string = PLATFORM_TAXPAYER_ID
  ) {}

  async handleSettlementPosted(event: MerchantSettlementPostedEvent): Promise<SettlementDocumentsResult> {
    const p = event.payload;
    const key = { consumer: ZATCA_SETTLEMENT_DOCUMENTS_CONSUMER, eventId: event.eventId };

    // ── step 1: issue both documents exactly once ─────────────────────────────
    const issued = await this.withPermanentErrors(() =>
      this.db.transaction(async (tx): Promise<{ duplicate: true } | ({ duplicate: false } & IssuedDocs)> => {
        if (await readCheckpoint(tx, { ...key, step: 'done' })) return { duplicate: true };
        const prior = await readCheckpoint(tx, { ...key, step: 'issued' });
        if (prior) {
          return {
            duplicate: false,
            feeInvoiceId: (prior.detail.feeInvoiceId as string | null) ?? null,
            creditNoteId: (prior.detail.creditNoteId as string | null) ?? null,
          };
        }
        const docs = await this.issueDocuments(tx, event);
        const fee = docs.feeInvoiceId ? await this.invoices.getInvoice(docs.feeInvoiceId, tx) : null;
        const note = docs.creditNoteId ? await this.invoices.getInvoice(docs.creditNoteId, tx) : null;
        await claimCheckpoint(tx, { ...key, step: 'issued', correlationId: p.batchId });
        await completeCheckpoint(tx, {
          ...key,
          step: 'issued',
          outcome: 'ISSUED',
          detail: {
            feeInvoiceId: docs.feeInvoiceId,
            creditNoteId: docs.creditNoteId,
            feeIcv: fee?.icv ?? null,
            creditNoteIcv: note?.icv ?? null,
          },
        });
        return { duplicate: false, ...docs };
      })
    );
    if (issued.duplicate) return { outcome: 'DUPLICATE_IGNORED', batchId: p.batchId };

    // ── step 2: submit each document to Fatoora (clearance / reporting), once ──
    const ordered: Array<{ role: SettlementDocumentRole; id: string }> = [];
    if (issued.feeInvoiceId) ordered.push({ role: 'FEE_INVOICE', id: issued.feeInvoiceId });
    if (issued.creditNoteId) ordered.push({ role: 'PAYOUT_CREDIT_NOTE', id: issued.creditNoteId });
    for (const doc of ordered) {
      let current = await this.invoices.getInvoice(doc.id);
      if (current.status === 'ISSUED') {
        current = await this.withPermanentErrors(() =>
          current.invoice_subtype === '0100000' ? this.reporting.clearStandardInvoice(doc.id) : this.reporting.reportSimplifiedInvoice(doc.id)
        );
      }
      if (current.status === 'REJECTED') {
        throw new NonRetryableError(
          'ZATCA_REJECTED',
          `Fatoora rejected the ${doc.role} ${doc.id} of batch ${p.batchId} (ICV ${current.icv}): ${JSON.stringify(current.zatca_errors)}`
        );
      }
      if (!ACCEPTED.has(current.status)) {
        // 5xx / transient: the document keeps its ICV; the message is retried with backoff.
        throw new Error(`ZATCA_TRANSIENT_ERROR: Fatoora did not accept the ${doc.role} ${doc.id} yet (ICV ${current.icv})`);
      }
    }

    // ── step 3: announce, exactly once ─────────────────────────────────────────
    const eventId = deterministicEventId(EVENT_TYPES.settlementDocumentsReported, p.batchId);
    const documents = await this.db.transaction(async (tx) => {
      const claimed = await claimCheckpoint(tx, { ...key, step: 'done', correlationId: p.batchId });
      const taxpayer = await this.invoices.getTaxpayer(this.platformTaxpayerId, tx);
      const snapshots: SettlementDocument[] = [];
      let feeRef: { invoiceId: string; invoiceNumber: string } | null = null;
      for (const doc of ordered) {
        const invoice = await this.invoices.getInvoice(doc.id, tx);
        if (doc.role === 'FEE_INVOICE') feeRef = { invoiceId: invoice.id, invoiceNumber: invoice.invoice_number };
        snapshots.push({
          role: doc.role,
          document: {
            ...invoiceDocumentSnapshot(invoice, taxpayer, doc.role === 'PAYOUT_CREDIT_NOTE' ? feeRef : null),
            xmlUblBase64: invoice.xml_ubl_base64 as string,
          },
        });
      }
      if (!claimed) return snapshots;
      const payload: SettlementDocumentsReportedPayload = {
        batchId: p.batchId,
        merchantId: p.merchantId,
        taxpayerId: taxpayer.id,
        journalRef: p.journalRef,
        settlement: p,
        documents: snapshots,
        reportedAt: new Date().toISOString(),
      };
      await enqueueIntegrationEvent(tx, {
        exchange: EXCHANGES.zatca.name,
        routingKey: ROUTING_KEYS.settlementDocumentsReported,
        event: buildIntegrationEvent({
          eventId,
          eventType: EVENT_TYPES.settlementDocumentsReported,
          producer: PRODUCERS.zatca,
          correlationId: p.batchId,
          causationId: event.eventId,
          aggregateId: p.batchId,
          payload,
        }),
      });
      await completeCheckpoint(tx, {
        ...key,
        step: 'done',
        outcome: snapshots.length > 0 ? 'REPORTED' : 'NO_DOCUMENTS',
        detail: {
          eventId,
          documents: snapshots.map((d) => ({ role: d.role, invoiceId: d.document.invoiceId, icv: d.document.icv, status: d.document.status })),
        },
      });
      return snapshots;
    });

    return {
      outcome: documents.length > 0 ? 'REPORTED' : 'NO_DOCUMENTS',
      batchId: p.batchId,
      documents: documents.map((d) => ({ role: d.role, invoiceId: d.document.invoiceId, icv: d.document.icv, status: d.document.status })),
      eventId,
    };
  }

  /** Documents issued for a settlement batch, with the consumer checkpoints. */
  async getSettlementDocuments(batchId: string): Promise<{ invoices: InvoiceWithLines[]; checkpoints: Checkpoint[] }> {
    const res = await this.db.query<{ id: string }>(
      `SELECT id FROM invoices WHERE source_settlement_batch_id = $1 ORDER BY icv NULLS LAST, created_at`,
      [batchId]
    );
    const invoices: InvoiceWithLines[] = [];
    for (const row of res.rows) invoices.push(await this.invoices.getInvoice(row.id));
    return { invoices, checkpoints: await listCheckpoints(this.db, { correlationId: batchId }) };
  }

  // ── internals ──────────────────────────────────────────────────────────────

  private async issueDocuments(tx: ZatcaExecutor, event: MerchantSettlementPostedEvent): Promise<IssuedDocs> {
    const p = event.payload;
    const platform = await this.invoices.getTaxpayer(this.platformTaxpayerId, tx);

    const existing = await tx.query<{ id: string; invoice_type_code: string; status: string; source_journal_ref: string | null }>(
      `SELECT id, invoice_type_code, status, source_journal_ref FROM invoices
       WHERE taxpayer_id = $1 AND source_settlement_batch_id = $2`,
      [platform.id, p.batchId]
    );
    if (existing.rows.length > 0) {
      // Issued by an earlier delivery whose checkpoint did not commit (or by another event id):
      // reuse them — never a second fee invoice / credit note for a batch.
      for (const row of existing.rows) {
        if (row.source_journal_ref !== p.journalRef) {
          throw new NonRetryableError(
            'SETTLEMENT_ALREADY_DOCUMENTED',
            `Batch ${p.batchId} is documented by ${row.id} for journal ${row.source_journal_ref}, not ${p.journalRef}`
          );
        }
        if (row.status === 'DRAFT') await this.invoices.issueInvoiceWithin(tx, row.id);
      }
      return {
        feeInvoiceId: existing.rows.find((r) => r.invoice_type_code === '388')?.id ?? null,
        creditNoteId: existing.rows.find((r) => r.invoice_type_code === '381')?.id ?? null,
      };
    }

    const buyer = await this.buyerFor(tx, p);
    const subtype = buyer.vatNumber ? '0100000' : '0200000';
    const egs = await tx.query<{ id: string }>(
      `SELECT id FROM egs_devices WHERE taxpayer_id = $1 AND status = 'ACTIVE' ORDER BY created_at, id LIMIT 1`,
      [platform.id]
    );
    const common = {
      taxpayerId: platform.id,
      egsDeviceId: egs.rows[0]?.id,
      invoiceSubtype: subtype as '0100000' | '0200000',
      customerName: buyer.name,
      customerVatNumber: buyer.vatNumber ?? undefined,
      sourceSettlementBatchId: p.batchId,
      sourceJournalRef: p.journalRef,
      sourceEventId: event.eventId,
    };

    if (p.fee.totalHalalas === 0) return { feeInvoiceId: null, creditNoteId: null };

    // The fee invoice: our revenue (net fee) + VAT, exactly as billing priced and the ledger booked.
    const feeDraft = await this.invoices.createDraftInvoice(
      {
        ...common,
        invoiceTypeCode: '388',
        // UNCL 4461 97: clearing between partners — the fee is deducted from the settlement.
        paymentMeansCode: '97',
        lines: [
          {
            itemNameEn: `Payment processing fees - settlement ${p.batchId} (${p.itemCount} transactions)`,
            itemNameAr: `رسوم معالجة المدفوعات - تسوية ${p.batchId} (${p.itemCount} عملية)`,
            sku: 'PLATFORM-PROCESSING-FEE',
            quantity: 1,
            unitPriceHalalas: p.fee.netHalalas,
            vatCategory: 'S',
          },
        ],
      },
      tx
    );
    if (
      feeDraft.taxable_amount_halalas !== p.fee.netHalalas ||
      feeDraft.vat_amount_halalas !== p.fee.vatHalalas ||
      feeDraft.total_amount_halalas !== p.fee.totalHalalas
    ) {
      throw new NonRetryableError(
        'LEDGER_TAX_MISMATCH',
        `Batch ${p.batchId}: the ledger booked a fee of ${p.fee.netHalalas} + VAT ${p.fee.vatHalalas} = ${p.fee.totalHalalas}, ` +
          `the fee invoice computes ${feeDraft.taxable_amount_halalas} + ${feeDraft.vat_amount_halalas} = ${feeDraft.total_amount_halalas}`
      );
    }
    await this.invoices.issueInvoiceWithin(tx, feeDraft.id);

    let creditNoteId: string | null = null;
    if (p.payoutHalalas > 0) {
      const noteDraft = await this.invoices.createDraftInvoice(
        {
          ...common,
          invoiceTypeCode: '381',
          invoiceNumber: `CN-${new Date().getUTCFullYear()}-${crypto.randomUUID().slice(0, 6).toUpperCase()}`,
          paymentMeansCode: '30',
          billingReferenceId: feeDraft.id,
          instructionNote:
            `Settlement payout of batch ${p.batchId}: merchant revenue ${sar(p.grossHalalas)} SAR ` +
            `less platform fee ${sar(p.fee.totalHalalas)} SAR credited to ${p.accounts.payoutWalletId} (journal ${p.journalRef})`,
          lines: [
            {
              itemNameEn: `Settlement payout - ${p.itemCount} settled orders`,
              itemNameAr: `صافي مستحقات التسوية - ${p.itemCount} طلب`,
              sku: 'SETTLEMENT-PAYOUT',
              quantity: 1,
              unitPriceHalalas: p.payoutHalalas,
              vatCategory: 'O',
              vatExemptionCode: 'VATEX-SA-OOS',
            },
          ],
        },
        tx
      );
      if (noteDraft.total_amount_halalas !== p.payoutHalalas || noteDraft.vat_amount_halalas !== 0) {
        throw new NonRetryableError(
          'LEDGER_TAX_MISMATCH',
          `Batch ${p.batchId}: payout ${p.payoutHalalas}, credit note computes ${noteDraft.total_amount_halalas} (VAT ${noteDraft.vat_amount_halalas})`
        );
      }
      await this.invoices.issueInvoiceWithin(tx, noteDraft.id);
      creditNoteId = noteDraft.id;
    }
    return { feeInvoiceId: feeDraft.id, creditNoteId };
  }

  /** The merchant as a buyer: its own ZATCA registration when it has one (B2B), else its display name. */
  private async buyerFor(tx: ZatcaExecutor, p: MerchantSettlementPostedPayload): Promise<{ name: string; vatNumber: string | null }> {
    const res = await tx.query<Pick<TaxpayerRecord, 'legal_name_en' | 'vat_number'>>(
      `SELECT legal_name_en, vat_number FROM taxpayers WHERE id = $1`,
      [p.merchantId]
    );
    if (res.rows.length > 0) return { name: res.rows[0].legal_name_en, vatNumber: res.rows[0].vat_number };
    return { name: p.merchantName, vatNumber: null };
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
