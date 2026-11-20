import crypto from 'crypto';

/**
 * Ledger invariant violation events. Money amounts travel as decimal strings
 * (bigint minor units) so no precision is lost in JSON.
 */
export const LEDGER_EVENT_TYPES = {
  WALLET_DRIFT: 'ledger.invariant.wallet_drift',
  NEGATIVE_BALANCE: 'ledger.invariant.negative_balance',
  GLOBAL_ZERO_SUM: 'ledger.invariant.global_zero_sum',
  UNBALANCED_JOURNAL: 'ledger.invariant.unbalanced_journal',
} as const;

export type LedgerEventType = (typeof LEDGER_EVENT_TYPES)[keyof typeof LEDGER_EVENT_TYPES];

export interface WalletDriftPayload {
  walletId: string;
  currency: string;
  accountType: string;
  balanceCents: string;
  /** Precalculated running total at detection time. */
  ledgerBalanceCents: string;
  ledgerVersion: string;
  /** Full recomputed SUM / COUNT (audit only; the write guard never re-sums). */
  ledgerSumCents?: string;
  ledgerEntryCount?: string;
}

export interface NegativeBalancePayload {
  walletId: string;
  currency: string;
  accountType: string;
  ledgerSumCents: string;
}

export interface GlobalZeroSumPayload {
  currency: string;
  totalCents: string;
}

export interface UnbalancedJournalPayload {
  transactionRef: string;
  currency: string;
  totalCents: string;
}

export interface PayloadByType {
  'ledger.invariant.wallet_drift': WalletDriftPayload;
  'ledger.invariant.negative_balance': NegativeBalancePayload;
  'ledger.invariant.global_zero_sum': GlobalZeroSumPayload;
  'ledger.invariant.unbalanced_journal': UnbalancedJournalPayload;
}

/** AUDIT: found by the periodic ledger audit. WRITE_GUARD: a transfer/withdraw hit a frozen wallet. */
export type DetectedBy = 'AUDIT' | 'WRITE_GUARD';

export interface EventEnvelope<T extends LedgerEventType = LedgerEventType> {
  eventId: string;
  type: T;
  occurredAt: string;
  /** Correlation id: the audit run, or the write-guard detection. */
  auditRunId: string;
  detectedBy: DetectedBy;
  payload: PayloadByType[T];
}

export function createEnvelope<T extends LedgerEventType>(
  type: T,
  auditRunId: string,
  payload: PayloadByType[T],
  detectedBy: DetectedBy = 'AUDIT'
): EventEnvelope<T> {
  return {
    eventId: `evt_${crypto.randomUUID()}`,
    type,
    occurredAt: new Date().toISOString(),
    auditRunId,
    detectedBy,
    payload,
  };
}

const KNOWN_TYPES = new Set<string>(Object.values(LEDGER_EVENT_TYPES));

/** Parse and minimally validate an envelope; throws on anything malformed. */
export function parseEnvelope(content: Buffer): EventEnvelope {
  const parsed: unknown = JSON.parse(content.toString('utf8'));
  if (
    typeof parsed !== 'object' ||
    parsed === null ||
    typeof (parsed as EventEnvelope).eventId !== 'string' ||
    !KNOWN_TYPES.has((parsed as EventEnvelope).type) ||
    typeof (parsed as EventEnvelope).payload !== 'object'
  ) {
    throw new Error('Malformed ledger event envelope');
  }
  return parsed as EventEnvelope;
}
