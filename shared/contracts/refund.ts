import { z } from 'zod';
import { integrationEventSchema, type IntegrationEvent } from './envelope';
import { EVENT_TYPES, InvoiceDocumentSchema } from './events';
import { allocateProportionally } from './money';
import { OrderContextSchema, VAT_CATEGORIES, VAT_EXEMPTION_CODES, type OrderVatCategory } from './order';

/**
 * Customer refunds of captured orders (partial or full), as a chain of authenticated events:
 *
 *   hyperpay-gateway  a signed RF (refund) notification for a captured payment: recorded once per
 *                     refund id, refused when it would exceed what is still refundable
 *                     ──► hyperpay / refund.processed                     (REFUND_PROCESSED)
 *   billing-credits   restores the promotional credits pro rata (a new PROMOTIONAL grant)
 *                     ──► billing.events / billing.order-credits.restored
 *   wallet-ledger     ONE balanced 3-leg reversal journal: merchant DEBIT net share,
 *                     VAT payable DEBIT VAT share, funding account CREDIT the refund
 *                     ──► ledger.events / ledger.payment.refunded
 *   zatca-tax         credit note (381) referencing the order's invoice, one line per VAT
 *                     category with the pro-rata VAT, reported to Fatoora
 *                     ──► zatca.events / zatca.credit-note.reported
 *   file-cache        invalidates the invoice's cached PDF and caches the credit note
 *
 * Every refund is split across the VAT categories of the order IN PROPORTION TO WHAT IS STILL
 * REFUNDABLE of each component (taxable amount and VAT per category), with largest-remainder
 * allocation: exact to the halala, never more than what is left, and a final refund reverses
 * exactly what remains.
 */

const halalas = z.number().int().nonnegative().safe();
const positiveHalalas = z.number().int().positive().safe();

// ─── HyperPay: REFUND_PROCESSED (its own flat wire format) ────────────────────

export const HyperPayRefundProcessedSchema = z
  .object({
    eventId: z.string().min(1).max(100),
    eventType: z.literal(EVENT_TYPES.refundProcessed),
    occurredAt: z.string().min(1),
    correlationId: z.string().min(1),
    source: z.string().min(1),
    /** OPPWA id of the refund transaction. */
    transactionId: z.string().min(1).max(100),
    /** The captured payment being refunded. */
    referencedPaymentId: z.string().min(1).max(100),
    amount: z.string().regex(/^\d+(\.\d{1,2})?$/),
    /** Amount of the captured payment (what was originally charged). */
    referencedAmount: z.string().regex(/^\d+(\.\d{1,2})?$/),
    /** Σ successful refunds of the payment, this one included. */
    refundedTotal: z.string().regex(/^\d+(\.\d{1,2})?$/),
    currency: z.string().length(3),
    status: z.enum(['SUCCESS', 'PENDING', 'FAILED']),
    resultCode: z.string().min(1),
    dbRecordId: z.string().min(1),
    orderId: z.string().min(1).max(100),
    merchantId: z.string().min(1).max(64),
    reason: z.string().max(500).optional(),
    order: OrderContextSchema.optional(),
  })
  .passthrough();
export type HyperPayRefundProcessed = z.infer<typeof HyperPayRefundProcessedSchema>;

// ─── shared refund facts ──────────────────────────────────────────────────────

export const RefundFactsSchema = z.object({
  refundId: z.string().min(1).max(100),
  /** eventId of the REFUND_PROCESSED event. */
  refundEventId: z.string().min(1).max(100),
  paymentId: z.string().min(1).max(100),
  orderId: z.string().min(1).max(100),
  merchantId: z.string().min(1).max(64),
  currency: z.literal('SAR'),
  amountHalalas: positiveHalalas,
  /** What the customer originally paid for the order. */
  capturedHalalas: positiveHalalas,
  /** Σ successful refunds, this one included (never more than capturedHalalas). */
  refundedTotalHalalas: positiveHalalas,
  reason: z.string().max(500).nullable(),
  refundedAt: z.string().min(1),
});
export type RefundFacts = z.infer<typeof RefundFactsSchema>;

// ─── Billing: credits restored ────────────────────────────────────────────────

export const OrderCreditsRestoredPayloadSchema = z.object({
  refund: RefundFactsSchema,
  credits: z.object({
    /** RESTORED when credits were given back, NONE when the order had no captured credits. */
    outcome: z.enum(['RESTORED', 'NONE']),
    orgId: z.string().nullable(),
    capturedCredits: z.number().int().nonnegative(),
    /** Credits restored by THIS refund. */
    restoredCredits: z.number().int().nonnegative(),
    /** Σ credits restored for the order so far (never more than capturedCredits). */
    cumulativeRestoredCredits: z.number().int().nonnegative(),
    grantId: z.string().nullable(),
  }),
});
export type OrderCreditsRestoredPayload = z.infer<typeof OrderCreditsRestoredPayloadSchema>;
export const OrderCreditsRestoredSchema = integrationEventSchema(EVENT_TYPES.orderCreditsRestored, OrderCreditsRestoredPayloadSchema);
export type OrderCreditsRestoredEvent = IntegrationEvent<typeof EVENT_TYPES.orderCreditsRestored, OrderCreditsRestoredPayload>;

// ─── Ledger: payment refunded (reversal journal posted) ───────────────────────

export const RefundSplitLineSchema = z.object({
  vatCategory: z.enum(VAT_CATEGORIES),
  vatRateBps: z.number().int().nonnegative(),
  vatExemptionCode: z.enum(VAT_EXEMPTION_CODES).nullable(),
  /** Pro-rata share of the category's taxable amount reversed by this refund. */
  netHalalas: halalas,
  /** Pro-rata share of the category's VAT reversed by this refund. */
  vatHalalas: halalas,
});
export type RefundSplitLine = z.infer<typeof RefundSplitLineSchema>;

export const LedgerPaymentRefundedPayloadSchema = z.object({
  refund: RefundFactsSchema,
  /** The capture this reverses (the order's settlement). */
  captureJournalRef: z.string().min(1),
  journalRef: z.string().min(1),
  /** Merchant revenue reversed (Σ lines' net). */
  netHalalas: halalas,
  /** VAT reversed (Σ lines' VAT). */
  vatHalalas: halalas,
  lines: z.array(RefundSplitLineSchema).min(1),
  /** Which merchant account gave the money back: revenue (not yet settled) or payout (settled). */
  merchantAccount: z.enum(['REVENUE', 'PAYOUT']),
  tender: z.enum(['CARD', 'WALLET']),
  /** The account the refund was credited to (gateway clearing, or the customer's wallet). */
  refundedToWalletId: z.string().min(1),
  /** Order totals after this refund. */
  remaining: z.object({ grossHalalas: halalas, netHalalas: halalas, vatHalalas: halalas }),
  credits: OrderCreditsRestoredPayloadSchema.shape.credits,
  postedAt: z.string().min(1),
});
export type LedgerPaymentRefundedPayload = z.infer<typeof LedgerPaymentRefundedPayloadSchema>;
export const LedgerPaymentRefundedSchema = integrationEventSchema(EVENT_TYPES.ledgerPaymentRefunded, LedgerPaymentRefundedPayloadSchema);
export type LedgerPaymentRefundedEvent = IntegrationEvent<typeof EVENT_TYPES.ledgerPaymentRefunded, LedgerPaymentRefundedPayload>;

// ─── ZATCA: credit note reported ──────────────────────────────────────────────

export const CreditNoteReportedPayloadSchema = z.object({
  refund: RefundFactsSchema,
  journalRef: z.string().min(1),
  /** The invoice the credit note adjusts. */
  originalInvoiceId: z.string().min(1),
  originalInvoiceNumber: z.string().min(1),
  /** Σ credit notes issued against the invoice so far, this one included. */
  creditedTotalHalalas: halalas,
  creditNote: InvoiceDocumentSchema.extend({ xmlUblBase64: z.string().min(1) }),
  /** The adjusted invoice as issued (so its cached PDF can be re-rendered with the credits applied). */
  invoice: InvoiceDocumentSchema,
  /** Every credit note issued against the invoice so far, in ICV order (this one included). */
  creditNotes: z.array(
    z.object({
      invoiceId: z.string().min(1),
      invoiceNumber: z.string().min(1),
      icv: z.number().int().positive(),
      totalHalalas: halalas,
      refundId: z.string().nullable(),
      reportedAt: z.string().nullable(),
    })
  ),
  reportedAt: z.string().min(1),
});
export type CreditNoteReportedPayload = z.infer<typeof CreditNoteReportedPayloadSchema>;
export const CreditNoteReportedSchema = integrationEventSchema(EVENT_TYPES.creditNoteReported, CreditNoteReportedPayloadSchema);
export type CreditNoteReportedEvent = IntegrationEvent<typeof EVENT_TYPES.creditNoteReported, CreditNoteReportedPayload>;

// ─── arithmetic ───────────────────────────────────────────────────────────────

export interface RefundableComponent {
  vatCategory: OrderVatCategory;
  vatRateBps: number;
  vatExemptionCode?: string | null;
  /** Taxable amount of the category still refundable (original − already refunded). */
  netHalalas: number;
  /** VAT of the category still refundable. */
  vatHalalas: number;
}

export class RefundSplitError extends Error {
  public readonly retryable = false;
  constructor(
    public readonly reason: string,
    message: string
  ) {
    super(message);
    this.name = 'RefundSplitError';
  }
}

/**
 * Split a refund over what is still refundable, pro rata of every component (the taxable amount
 * and the VAT of each VAT category), with largest-remainder allocation: the parts sum to the
 * amount exactly, no part exceeds what is left of it, and refunding everything that is left
 * reverses every component exactly. The credit note credits the same VAT the customer paid
 * back, proportionally — not a recomputation from the net.
 */
export function proRataRefundSplit(remaining: readonly RefundableComponent[], amountHalalas: number): RefundSplitLine[] {
  if (!Number.isSafeInteger(amountHalalas) || amountHalalas <= 0) {
    throw new RefundSplitError('INVALID_REFUND_AMOUNT', `Refund amount must be a positive integer of halalas, got ${amountHalalas}`);
  }
  const left = remaining.reduce((s, c) => s + c.netHalalas + c.vatHalalas, 0);
  if (amountHalalas > left) {
    throw new RefundSplitError('REFUND_EXCEEDS_REFUNDABLE', `Refund of ${amountHalalas} exceeds the ${left} still refundable`);
  }
  const weights = remaining.flatMap((c) => [c.netHalalas, c.vatHalalas]);
  const parts = allocateProportionally(amountHalalas, weights);
  return remaining.map((c, i) => ({
    vatCategory: c.vatCategory,
    vatRateBps: c.vatRateBps,
    vatExemptionCode: (c.vatExemptionCode ?? null) as RefundSplitLine['vatExemptionCode'],
    netHalalas: parts[2 * i],
    vatHalalas: parts[2 * i + 1],
  }));
}

/** transaction_ref of a refund's reversal journal (one per refund, ever). */
export function refundJournalRef(refundId: string): string {
  return `rfd_${refundId}`;
}
