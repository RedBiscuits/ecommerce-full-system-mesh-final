import { z } from 'zod';
import { integrationEventSchema, type IntegrationEvent } from './envelope';
import { OrderContextSchema, TENDERS, VAT_CATEGORIES, VAT_EXEMPTION_CODES } from './order';

/**
 * The order-to-invoice flow, as a chain of integration events. Each producer
 * owns its exchange; each consumer owns a queue (plus its retry tiers and DLQ)
 * bound to the producer's exchange:
 *
 *   HyperPay   ──PAYMENT_COMPLETED──────────►  hyperpay        (direct)  payment.result
 *   Billing    ──order-credits.settled──────►  billing.events  (topic)   billing.order-credits.settled
 *   Ledger     ──payment.settled / released─►  ledger.events   (topic)   ledger.payment.settled | .released
 *   ZATCA      ──invoice.reported───────────►  zatca.events    (topic)   zatca.invoice.reported
 *
 *   consumer queue              bound to                                   service
 *   billing.payment-results     hyperpay / payment.result                  billing-credits
 *   ledger.order-settlements    billing.events / billing.order-credits.*   wallet-ledger
 *   zatca.ledger-settlements    ledger.events / ledger.payment.settled     zatca-tax
 *   file-cache.invoice-pdfs     zatca.events / zatca.invoice.reported      file-cache-engine
 */

export const EXCHANGES = {
  hyperpay: { name: 'hyperpay', type: 'direct' },
  billing: { name: 'billing.events', type: 'topic' },
  ledger: { name: 'ledger.events', type: 'topic' },
  zatca: { name: 'zatca.events', type: 'topic' },
  checkout: { name: 'checkout.events', type: 'topic' },
} as const;

export const ROUTING_KEYS = {
  paymentResult: 'payment.result',
  orderCreditsSettled: 'billing.order-credits.settled',
  ledgerPaymentSettled: 'ledger.payment.settled',
  ledgerPaymentReleased: 'ledger.payment.released',
  invoiceReported: 'zatca.invoice.reported',
  checkoutPaymentCompleted: 'checkout.payment.completed',
  // Merchant settlement batch workflow (see ./settlement.ts)
  merchantSettlementCollected: 'ledger.merchant-settlement.collected',
  settlementFeeCalculated: 'billing.settlement-fee.calculated',
  merchantSettlementPosted: 'ledger.merchant-settlement.posted',
  settlementDocumentsReported: 'zatca.settlement-documents.reported',
  // Customer refunds (see ./refund.ts)
  refundProcessed: 'refund.processed',
  orderCreditsRestored: 'billing.order-credits.restored',
  ledgerPaymentRefunded: 'ledger.payment.refunded',
  creditNoteReported: 'zatca.credit-note.reported',
} as const;

export const EVENT_TYPES = {
  paymentCompleted: 'PAYMENT_COMPLETED',
  orderCreditsSettled: 'billing.order-credits.settled',
  ledgerPaymentSettled: 'ledger.payment.settled',
  ledgerPaymentReleased: 'ledger.payment.released',
  invoiceReported: 'zatca.invoice.reported',
  checkoutPaymentCompleted: 'checkout.payment.completed',
  merchantSettlementCollected: 'ledger.merchant-settlement.collected',
  settlementFeeCalculated: 'billing.settlement-fee.calculated',
  merchantSettlementPosted: 'ledger.merchant-settlement.posted',
  settlementDocumentsReported: 'zatca.settlement-documents.reported',
  refundProcessed: 'REFUND_PROCESSED',
  orderCreditsRestored: 'billing.order-credits.restored',
  ledgerPaymentRefunded: 'ledger.payment.refunded',
  creditNoteReported: 'zatca.credit-note.reported',
} as const;

export const CONSUMERS = {
  billingPaymentResults: 'billing.payment-results',
  ledgerOrderSettlements: 'ledger.order-settlements',
  zatcaLedgerSettlements: 'zatca.ledger-settlements',
  fileCacheInvoicePdfs: 'file-cache.invoice-pdfs',
  checkoutLedgerOutcomes: 'checkout.ledger-outcomes',
  // Merchant settlement batch workflow
  billingSettlementFees: 'billing.settlement-fees',
  ledgerSettlementFees: 'ledger.settlement-fees',
  zatcaMerchantSettlements: 'zatca.merchant-settlements',
  fileCacheSettlementStatements: 'file-cache.settlement-statements',
  ledgerSettlementDocuments: 'ledger.settlement-documents',
  // Customer refunds
  billingRefunds: 'billing.refunds',
  ledgerOrderRefunds: 'ledger.order-refunds',
  zatcaLedgerRefunds: 'zatca.ledger-refunds',
  fileCacheCreditNotes: 'file-cache.credit-notes',
} as const;

/** What each consumer queue is bound to (one source of truth for runtimes and the topology bootstrap). */
export const CONSUMER_BINDINGS: Record<
  (typeof CONSUMERS)[keyof typeof CONSUMERS],
  Array<{ exchange: string; exchangeType: 'direct' | 'topic'; routingKey: string }>
> = {
  'billing.payment-results': [
    { exchange: 'hyperpay', exchangeType: 'direct', routingKey: 'payment.result' },
    // Split-tender checkouts whose remainder is paid from the customer's wallet (no card payment).
    { exchange: 'checkout.events', exchangeType: 'topic', routingKey: 'checkout.payment.completed' },
  ],
  'ledger.order-settlements': [
    { exchange: 'billing.events', exchangeType: 'topic', routingKey: 'billing.order-credits.settled' },
  ],
  'zatca.ledger-settlements': [{ exchange: 'ledger.events', exchangeType: 'topic', routingKey: 'ledger.payment.settled' }],
  'file-cache.invoice-pdfs': [{ exchange: 'zatca.events', exchangeType: 'topic', routingKey: 'zatca.invoice.reported' }],
  'checkout.ledger-outcomes': [
    { exchange: 'ledger.events', exchangeType: 'topic', routingKey: 'ledger.payment.settled' },
    { exchange: 'ledger.events', exchangeType: 'topic', routingKey: 'ledger.payment.released' },
  ],
  // Merchant settlement: the ledger collects a batch → billing prices the fee → the ledger posts
  // the settlement journal → ZATCA issues + reports the fee invoice and the payout credit note,
  // the file cache keeps the statement (pending, then final) and the documents → the ledger
  // closes the batch.
  'billing.settlement-fees': [
    { exchange: 'ledger.events', exchangeType: 'topic', routingKey: 'ledger.merchant-settlement.collected' },
  ],
  'ledger.settlement-fees': [
    { exchange: 'billing.events', exchangeType: 'topic', routingKey: 'billing.settlement-fee.calculated' },
  ],
  'zatca.merchant-settlements': [
    { exchange: 'ledger.events', exchangeType: 'topic', routingKey: 'ledger.merchant-settlement.posted' },
  ],
  'file-cache.settlement-statements': [
    { exchange: 'ledger.events', exchangeType: 'topic', routingKey: 'ledger.merchant-settlement.posted' },
    { exchange: 'zatca.events', exchangeType: 'topic', routingKey: 'zatca.settlement-documents.reported' },
  ],
  'ledger.settlement-documents': [
    { exchange: 'zatca.events', exchangeType: 'topic', routingKey: 'zatca.settlement-documents.reported' },
  ],
  // Customer refunds: HyperPay records the refund → billing restores credits → the ledger posts
  // the reversal → ZATCA issues the credit note → the file cache invalidates / caches PDFs.
  'billing.refunds': [{ exchange: 'hyperpay', exchangeType: 'direct', routingKey: 'refund.processed' }],
  'ledger.order-refunds': [{ exchange: 'billing.events', exchangeType: 'topic', routingKey: 'billing.order-credits.restored' }],
  'zatca.ledger-refunds': [{ exchange: 'ledger.events', exchangeType: 'topic', routingKey: 'ledger.payment.refunded' }],
  'file-cache.credit-notes': [{ exchange: 'zatca.events', exchangeType: 'topic', routingKey: 'zatca.credit-note.reported' }],
};

/**
 * Reservation keys of a split-tender checkout, used as idempotency keys by billing (credits) and
 * the ledger (remainder): one hold per key, and a released key can never place a hold again.
 */
export function checkoutHoldKey(checkoutId: string, part: 'credits' | 'remainder'): string {
  return `checkout:${checkoutId}:${part}`;
}

/** Payment id the ledger binds to a wallet-funded checkout remainder (there is no card payment). */
export function walletPaymentId(checkoutId: string): string {
  return `wallet:${checkoutId}`;
}

export const PRODUCERS = {
  hyperpay: 'hyperpay-gateway',
  billing: 'billing-credits',
  ledger: 'wallet-ledger',
  zatca: 'zatca-tax',
  checkout: 'checkout-coordinator',
} as const;

const halalas = z.number().int().nonnegative().safe();
const positiveHalalas = z.number().int().positive().safe();

// ─── HyperPay: PAYMENT_COMPLETED (HyperPay's own flat wire format) ─────────────

/**
 * What HyperPay's outbox publisher puts on the wire for a checkout payment:
 * `{ eventId, eventType, occurredAt, ...PaymentCompletedPayload }`. Only the
 * fields the platform flow relies on are validated; unknown ones pass through.
 */
export const HyperPayPaymentCompletedSchema = z
  .object({
    eventId: z.string().min(1).max(100),
    eventType: z.literal(EVENT_TYPES.paymentCompleted),
    occurredAt: z.string().min(1),
    correlationId: z.string().min(1),
    source: z.string().min(1),
    origin: z.enum(['checkout', 'admin']),
    transactionId: z.string().min(1).max(100),
    status: z.enum(['SUCCESS', 'PENDING', 'FAILED']),
    amount: z.string().regex(/^\d+(\.\d{1,2})?$/),
    currency: z.string().length(3),
    resultCode: z.string().min(1),
    dbRecordId: z.string().min(1),
    paymentBrand: z.string().optional(),
    order: OrderContextSchema.optional(),
  })
  .passthrough();
export type HyperPayPaymentCompleted = z.infer<typeof HyperPayPaymentCompletedSchema>;

// ─── Billing: order credits settled ───────────────────────────────────────────

export const PaymentFactsSchema = z.object({
  paymentId: z.string().min(1).max(100),
  /** eventId of the HyperPay PAYMENT_COMPLETED event. */
  paymentEventId: z.string().min(1).max(100),
  status: z.enum(['SUCCESS', 'FAILED']),
  amountHalalas: halalas,
  currency: z.literal('SAR'),
  resultCode: z.string().min(1),
  paymentBrand: z.string().nullable(),
  tender: z.enum(TENDERS).default('CARD'),
});
export type PaymentFacts = z.infer<typeof PaymentFactsSchema>;

export const OrderCreditsSettledPayloadSchema = z.object({
  payment: PaymentFactsSchema,
  order: OrderContextSchema,
  credits: z.object({
    /** CAPTURED on a successful payment, RELEASED on a failed one, NONE when the order had no hold. */
    outcome: z.enum(['CAPTURED', 'RELEASED', 'NONE']),
    orgId: z.string().nullable(),
    holdIds: z.array(z.string()),
    capturedCredits: z.number().int().nonnegative(),
    releasedCredits: z.number().int().nonnegative(),
    halalasPerCredit: z.number().int().positive(),
    /** capturedCredits × halalasPerCredit: the document-level allowance (discount). */
    allowanceHalalas: halalas,
  }),
});
export type OrderCreditsSettledPayload = z.infer<typeof OrderCreditsSettledPayloadSchema>;
export const OrderCreditsSettledSchema = integrationEventSchema(
  EVENT_TYPES.orderCreditsSettled,
  OrderCreditsSettledPayloadSchema
);
export type OrderCreditsSettledEvent = IntegrationEvent<typeof EVENT_TYPES.orderCreditsSettled, OrderCreditsSettledPayload>;

// ─── Ledger: payment settled / released ───────────────────────────────────────

export const LedgerPaymentSettledPayloadSchema = z.object({
  orderId: z.string().min(1),
  paymentId: z.string().min(1),
  holdId: z.string().min(1),
  merchantId: z.string().min(1),
  currency: z.literal('SAR'),
  journalRef: z.string().min(1),
  /** What the customer paid (debited from the gateway clearing account). */
  grossHalalas: positiveHalalas,
  /** Merchant revenue leg (= taxable amount of the invoice). */
  netRevenueHalalas: halalas,
  /** VAT payable leg (15 % standard rate, after the credit allowance). */
  vatHalalas: halalas,
  allowanceHalalas: halalas,
  capturedCredits: z.number().int().nonnegative(),
  paymentBrand: z.string().nullable(),
  tender: z.enum(TENDERS).default('CARD'),
  order: OrderContextSchema,
  settledAt: z.string().min(1),
});
export type LedgerPaymentSettledPayload = z.infer<typeof LedgerPaymentSettledPayloadSchema>;
export const LedgerPaymentSettledSchema = integrationEventSchema(
  EVENT_TYPES.ledgerPaymentSettled,
  LedgerPaymentSettledPayloadSchema
);
export type LedgerPaymentSettledEvent = IntegrationEvent<
  typeof EVENT_TYPES.ledgerPaymentSettled,
  LedgerPaymentSettledPayload
>;

export const LedgerPaymentReleasedPayloadSchema = z.object({
  orderId: z.string().min(1),
  paymentId: z.string().min(1),
  holdId: z.string().min(1),
  merchantId: z.string().min(1),
  currency: z.literal('SAR'),
  amountHalalas: halalas,
  reason: z.string().min(1),
  releasedAt: z.string().min(1),
  tender: z.enum(TENDERS).default('CARD'),
  checkoutId: z.string().min(1).nullable().default(null),
});
export const LedgerPaymentReleasedSchema = integrationEventSchema(
  EVENT_TYPES.ledgerPaymentReleased,
  LedgerPaymentReleasedPayloadSchema
);
export type LedgerPaymentReleasedPayload = z.infer<typeof LedgerPaymentReleasedPayloadSchema>;
export type LedgerPaymentReleasedEvent = IntegrationEvent<
  typeof EVENT_TYPES.ledgerPaymentReleased,
  LedgerPaymentReleasedPayload
>;

// ─── ZATCA: invoice issued & reported ─────────────────────────────────────────

export const InvoiceDocumentLineSchema = z.object({
  lineNumber: z.number().int().positive(),
  sku: z.string(),
  nameEn: z.string(),
  nameAr: z.string(),
  quantity: z.number().int().positive(),
  unitPriceHalalas: halalas,
  discountHalalas: halalas,
  netAmountHalalas: halalas,
  vatCategory: z.enum(VAT_CATEGORIES),
  vatRateBps: z.number().int().nonnegative(),
  vatAmountHalalas: halalas,
  lineTotalHalalas: halalas,
  vatExemptionCode: z.enum(VAT_EXEMPTION_CODES).nullable().optional(),
});

export const VatBreakdownSchema = z.object({
  vatCategory: z.enum(VAT_CATEGORIES),
  vatRateBps: z.number().int().nonnegative(),
  exemptionCode: z.enum(VAT_EXEMPTION_CODES).nullable(),
  lineNetHalalas: halalas,
  allowanceHalalas: halalas,
  taxableHalalas: halalas,
  vatHalalas: halalas,
});

const InvoiceSellerSchema = z.object({
  legalNameEn: z.string(),
  legalNameAr: z.string(),
  vatNumber: z.string(),
  crnNumber: z.string(),
  buildingNumber: z.string(),
  streetName: z.string(),
  city: z.string(),
  postalCode: z.string(),
  countryCode: z.string(),
});

/**
 * A full snapshot of an issued ZATCA document (invoice, credit or debit note): enough for any
 * service to render it (e.g. the file cache renders and keeps the PDF) without the ZATCA database.
 */
export const InvoiceDocumentSchema = z.object({
  invoiceId: z.string().min(1),
  invoiceNumber: z.string().min(1),
  uuid: z.string().min(1),
  invoiceTypeCode: z.enum(['388', '381', '383']),
  invoiceSubtype: z.string().min(1),
  taxpayerId: z.string().min(1),
  icv: z.number().int().positive(),
  invoiceHash: z.string().min(1),
  previousInvoiceHash: z.string().min(1),
  qrBase64: z.string().min(1),
  status: z.enum(['REPORTED', 'REPORTED_WITH_WARNINGS', 'CLEARED']),
  issuedAt: z.string().min(1),
  reportedAt: z.string().min(1),
  customerName: z.string().min(1),
  totals: z.object({
    subtotalHalalas: halalas,
    discountHalalas: halalas,
    allowanceHalalas: halalas,
    taxableHalalas: halalas,
    vatHalalas: halalas,
    totalHalalas: halalas,
  }),
  lines: z.array(InvoiceDocumentLineSchema).min(1),
  /** Why the document-level allowance was granted (e.g. 'Promotional credit (20 credits)'). */
  allowanceReason: z.string().nullable().default(null),
  /** Per VAT category: allowance share, taxable amount and VAT (sums to the totals exactly). */
  vatBreakdown: z.array(VatBreakdownSchema).default([]),
  seller: InvoiceSellerSchema,
  /** B2B documents (standard subtype 0100000): the buyer's VAT registration number. */
  customerVatNumber: z.string().nullable().optional(),
  /** UNCL 4461 payment means (48 card, 30 credit transfer, 97 clearing between partners, …). */
  paymentMeansCode: z.string().min(1).optional(),
  /** Credit / debit notes: the document they adjust. */
  billingReference: z.object({ invoiceId: z.string().min(1), invoiceNumber: z.string().min(1) }).nullable().optional(),
  /** Credit / debit notes: the reason for issuance (KSA-10). */
  instructionNote: z.string().nullable().optional(),
});
export type InvoiceDocument = z.infer<typeof InvoiceDocumentSchema>;

/** The order flow's reported simplified invoice: the document plus the order it was issued for. */
export const InvoiceReportedPayloadSchema = InvoiceDocumentSchema.extend({
  orderId: z.string().min(1),
  journalRef: z.string().min(1),
  status: z.enum(['REPORTED', 'REPORTED_WITH_WARNINGS']),
});
export type InvoiceReportedPayload = z.infer<typeof InvoiceReportedPayloadSchema>;
export const InvoiceReportedSchema = integrationEventSchema(EVENT_TYPES.invoiceReported, InvoiceReportedPayloadSchema);
export type InvoiceReportedEvent = IntegrationEvent<typeof EVENT_TYPES.invoiceReported, InvoiceReportedPayload>;

// ─── Checkout coordinator: wallet-funded remainder confirmed ───────────────────

/**
 * A split-tender checkout whose remainder is paid from the customer's wallet was confirmed: the
 * funds are already reserved in escrow by the ledger. Enters the pipeline exactly like a
 * successful card payment (billing captures the credits, the ledger captures the escrow).
 */
export const CheckoutPaymentCompletedPayloadSchema = z.object({
  checkoutId: z.string().min(1),
  paymentId: z.string().min(1),
  tender: z.literal('WALLET'),
  status: z.literal('SUCCESS'),
  amountHalalas: positiveHalalas,
  currency: z.literal('SAR'),
  order: OrderContextSchema,
  confirmedAt: z.string().min(1),
});
export type CheckoutPaymentCompletedPayload = z.infer<typeof CheckoutPaymentCompletedPayloadSchema>;
export const CheckoutPaymentCompletedSchema = integrationEventSchema(
  EVENT_TYPES.checkoutPaymentCompleted,
  CheckoutPaymentCompletedPayloadSchema
);
export type CheckoutPaymentCompletedEvent = IntegrationEvent<
  typeof EVENT_TYPES.checkoutPaymentCompleted,
  CheckoutPaymentCompletedPayload
>;
