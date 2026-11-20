import { z } from 'zod';
import { integrationEventSchema, type IntegrationEvent } from './envelope';
import { EVENT_TYPES, InvoiceDocumentSchema } from './events';

/**
 * Merchant settlement batch workflow, as a chain of integration events (every hop is a
 * transactional outbox on the producer side and a checkpointed, idempotent consumer with its own
 * queue, retry tiers and DLQ on the consumer side):
 *
 *   wallet-ledger  sweep: claims the merchant's settled, not-yet-swept order revenue in bounded
 *                  keyset chunks (each entry is claimed by exactly one batch, ever)
 *                  ──► ledger.events / ledger.merchant-settlement.collected
 *   billing        tiered processing fee (integer halalas) + VAT on the fee, frozen per batch
 *                  ──► billing.events / billing.settlement-fee.calculated
 *   wallet-ledger  ONE balanced 3-leg settlement journal, atomically with the batch transition:
 *                    merchant revenue  DEBIT  gross
 *                    platform fees     CREDIT fee (incl. VAT)
 *                    merchant payout   CREDIT gross − fee   (the merchant's next payout)
 *                  ──► ledger.events / ledger.merchant-settlement.posted
 *   file-cache     statement JSON, status PENDING_TAX_REPORTING, kept 72 h
 *   zatca-tax      platform → merchant: tax invoice for the fee (our revenue) + credit note for
 *                  the payout, both in the platform's ICV / PIH chain, cleared / reported
 *                  ──► zatca.events / zatca.settlement-documents.reported
 *   file-cache     document PDFs + UBL XML cached, statement → FINAL (with the document refs)
 *   wallet-ledger  batch → COMPLETED
 */

const halalas = z.number().int().nonnegative().safe();
const positiveHalalas = z.number().int().positive().safe();

/** The time window a batch swept: `from` null = since the beginning of the system. */
export const SettlementWindowSchema = z.object({
  from: z.string().min(1).nullable(),
  to: z.string().min(1),
});
export type SettlementWindow = z.infer<typeof SettlementWindowSchema>;

// ─── ledger: batch collected ──────────────────────────────────────────────────

export const MerchantSettlementCollectedPayloadSchema = z.object({
  batchId: z.string().min(1).max(64),
  merchantId: z.string().min(1).max(64),
  currency: z.literal('SAR'),
  window: SettlementWindowSchema,
  /** Settled orders (revenue entries) claimed by this batch. */
  itemCount: z.number().int().positive().safe(),
  /** Σ merchant revenue of the claimed entries: what the settlement debits from the merchant. */
  grossHalalas: positiveHalalas,
  firstSettledAt: z.string().min(1),
  lastSettledAt: z.string().min(1),
  /** The batch hit its item cap: more unswept entries remain in the window (next batch). */
  hasMore: z.boolean(),
  collectedAt: z.string().min(1),
});
export type MerchantSettlementCollectedPayload = z.infer<typeof MerchantSettlementCollectedPayloadSchema>;
export const MerchantSettlementCollectedSchema = integrationEventSchema(
  EVENT_TYPES.merchantSettlementCollected,
  MerchantSettlementCollectedPayloadSchema
);
export type MerchantSettlementCollectedEvent = IntegrationEvent<
  typeof EVENT_TYPES.merchantSettlementCollected,
  MerchantSettlementCollectedPayload
>;

// ─── billing: fee calculated ──────────────────────────────────────────────────

export const SettlementFeeTierSchema = z.object({
  fromHalalas: halalas,
  /** null = unbounded (the last tier). */
  toHalalas: halalas.nullable(),
  bps: z.number().int().min(0).max(10_000),
  /** The part of the gross that falls into this tier. */
  baseHalalas: halalas,
  /** This tier's share of the percentage fee (the shares sum to it exactly). */
  feeHalalas: halalas,
});

export const SettlementFeeSchema = z.object({
  ruleId: z.string().min(1),
  ruleVersion: z.number().int().positive(),
  tiers: z.array(SettlementFeeTierSchema).min(1),
  /** Σ tier base × bps / 10 000, rounded once (banker's rounding). */
  percentageFeeHalalas: halalas,
  perTransactionHalalas: halalas,
  /** perTransactionHalalas × itemCount. */
  fixedFeeHalalas: halalas,
  minimumFeeHalalas: halalas,
  /** The fee excluding VAT: the platform's revenue (taxable amount of the fee invoice). */
  netHalalas: halalas,
  vatRateBps: z.number().int().min(0).max(10_000),
  /** VAT on the fee, computed exactly as the ZATCA engine computes the fee invoice. */
  vatHalalas: halalas,
  /** netHalalas + vatHalalas: what the platform keeps from the gross. */
  totalHalalas: halalas,
  /** The fee was reduced so that it never exceeds the gross. */
  capped: z.boolean(),
});
export type SettlementFee = z.infer<typeof SettlementFeeSchema>;

export const SettlementFeeCalculatedPayloadSchema = z.object({
  batchId: z.string().min(1).max(64),
  merchantId: z.string().min(1).max(64),
  currency: z.literal('SAR'),
  itemCount: z.number().int().positive().safe(),
  grossHalalas: positiveHalalas,
  fee: SettlementFeeSchema,
  /** grossHalalas − fee.totalHalalas. */
  payoutHalalas: halalas,
  calculatedAt: z.string().min(1),
});
export type SettlementFeeCalculatedPayload = z.infer<typeof SettlementFeeCalculatedPayloadSchema>;
export const SettlementFeeCalculatedSchema = integrationEventSchema(
  EVENT_TYPES.settlementFeeCalculated,
  SettlementFeeCalculatedPayloadSchema
);
export type SettlementFeeCalculatedEvent = IntegrationEvent<
  typeof EVENT_TYPES.settlementFeeCalculated,
  SettlementFeeCalculatedPayload
>;

// ─── ledger: settlement journal posted ────────────────────────────────────────

export const SETTLEMENT_LEG_ROLES = ['MERCHANT_REVENUE', 'PLATFORM_FEES', 'MERCHANT_PAYOUT'] as const;

export const MerchantSettlementPostedPayloadSchema = z.object({
  batchId: z.string().min(1).max(64),
  merchantId: z.string().min(1).max(64),
  merchantName: z.string().min(1),
  currency: z.literal('SAR'),
  window: SettlementWindowSchema,
  itemCount: z.number().int().positive().safe(),
  grossHalalas: positiveHalalas,
  fee: SettlementFeeSchema,
  payoutHalalas: halalas,
  /** transaction_ref of the settlement journal. */
  journalRef: z.string().min(1),
  journal: z
    .array(
      z.object({
        walletId: z.string().min(1),
        role: z.enum(SETTLEMENT_LEG_ROLES),
        direction: z.enum(['DEBIT', 'CREDIT']),
        amountHalalas: positiveHalalas,
      })
    )
    .min(2)
    .max(3),
  accounts: z.object({
    revenueWalletId: z.string().min(1),
    feeWalletId: z.string().min(1),
    payoutWalletId: z.string().min(1),
  }),
  firstSettledAt: z.string().min(1),
  lastSettledAt: z.string().min(1),
  postedAt: z.string().min(1),
});
export type MerchantSettlementPostedPayload = z.infer<typeof MerchantSettlementPostedPayloadSchema>;
export const MerchantSettlementPostedSchema = integrationEventSchema(
  EVENT_TYPES.merchantSettlementPosted,
  MerchantSettlementPostedPayloadSchema
);
export type MerchantSettlementPostedEvent = IntegrationEvent<
  typeof EVENT_TYPES.merchantSettlementPosted,
  MerchantSettlementPostedPayload
>;

// ─── ZATCA: settlement documents issued + reported ─────────────────────────────

export const SETTLEMENT_DOCUMENT_ROLES = ['FEE_INVOICE', 'PAYOUT_CREDIT_NOTE'] as const;
export type SettlementDocumentRole = (typeof SETTLEMENT_DOCUMENT_ROLES)[number];

export const SettlementDocumentSchema = z.object({
  role: z.enum(SETTLEMENT_DOCUMENT_ROLES),
  document: InvoiceDocumentSchema.extend({
    /** The signed UBL 2.1 XML as issued (base64). */
    xmlUblBase64: z.string().min(1),
  }),
});
export type SettlementDocument = z.infer<typeof SettlementDocumentSchema>;

export const SettlementDocumentsReportedPayloadSchema = z.object({
  batchId: z.string().min(1).max(64),
  merchantId: z.string().min(1).max(64),
  /** The platform's taxpayer (seller of the processing service). */
  taxpayerId: z.string().min(1),
  journalRef: z.string().min(1),
  /** The settlement as the ledger posted it (event-carried state: the statement can be built from this alone). */
  settlement: MerchantSettlementPostedPayloadSchema,
  /** Fee invoice, then payout credit note (a zero amount issues no document). */
  documents: z.array(SettlementDocumentSchema).max(2),
  reportedAt: z.string().min(1),
});
export type SettlementDocumentsReportedPayload = z.infer<typeof SettlementDocumentsReportedPayloadSchema>;
export const SettlementDocumentsReportedSchema = integrationEventSchema(
  EVENT_TYPES.settlementDocumentsReported,
  SettlementDocumentsReportedPayloadSchema
);
export type SettlementDocumentsReportedEvent = IntegrationEvent<
  typeof EVENT_TYPES.settlementDocumentsReported,
  SettlementDocumentsReportedPayload
>;

/** transaction_ref of a batch's settlement journal (one per batch, ever). */
export function settlementJournalRef(batchId: string): string {
  return `stl_${batchId}`;
}
