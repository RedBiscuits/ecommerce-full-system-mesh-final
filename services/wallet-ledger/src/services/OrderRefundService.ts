import type { PoolClient } from 'pg';
import { query } from '../db';
import { JournalLine, systemAccountsFor, toBigInt, vatPayableAccountFor } from '../money';
import { postJournal } from './ledgerWriter';
import { assertWalletNotFrozen, withMoneyTransaction } from './moneyTransaction';
import { WalletVersionConflictError } from '../types/wallet';
import {
  CONSUMERS,
  EVENT_TYPES,
  EXCHANGES,
  PRODUCERS,
  ROUTING_KEYS,
  RefundSplitError,
  buildIntegrationEvent,
  computeOrderTotals,
  deterministicEventId,
  proRataRefundSplit,
  refundJournalRef,
  type LedgerPaymentRefundedPayload,
  type OrderCreditsRestoredEvent,
  type OrderLine,
  type RefundSplitLine,
  type RefundableComponent,
  type Tender,
} from '../../../../shared/contracts';
import {
  NonRetryableError,
  claimCheckpoint,
  completeCheckpoint,
  enqueueIntegrationEvent,
  listCheckpoints,
  type Checkpoint,
} from '../../../../shared/messaging';

export const LEDGER_REFUND_CONSUMER = CONSUMERS.ledgerOrderRefunds;

export type RefundOutcome = 'REFUNDED' | 'DUPLICATE_IGNORED';

export interface RefundResult {
  outcome: RefundOutcome;
  refundId: string;
  orderId: string;
  journalRef?: string;
  netHalalas?: number;
  vatHalalas?: number;
  merchantAccount?: 'REVENUE' | 'PAYOUT';
  /** eventId of the ledger.payment.refunded event (deterministic per refund). */
  eventId?: string;
}

export interface OrderRefundRecord {
  refundId: string;
  orderId: string;
  paymentId: string;
  merchantId: string;
  amountHalalas: number;
  netHalalas: number;
  vatHalalas: number;
  split: RefundSplitLine[];
  merchantAccount: 'REVENUE' | 'PAYOUT';
  refundedToWalletId: string;
  journalRef: string;
  restoredCredits: number;
  refundEventId: string;
  createdAt: string;
}

/** Per VAT category: captured and refunded so far (stored on the settlement as `refund_state`). */
interface CategoryRefundState {
  vatCategory: RefundSplitLine['vatCategory'];
  vatRateBps: number;
  vatExemptionCode: string | null;
  taxableHalalas: number;
  vatHalalas: number;
  refundedNetHalalas: number;
  refundedVatHalalas: number;
}

interface SettlementRow {
  order_id: string;
  payment_id: string;
  hold_id: string;
  merchant_id: string;
  journal_ref: string;
  gross_cents: string | number;
  net_revenue_cents: string | number;
  vat_cents: string | number;
  allowance_cents: string | number;
  settlement_batch_id: string | null;
  order_context: { lines: OrderLine[]; allowanceHalalas: number } | null;
  refunded_gross_cents: string | number;
  refunded_net_cents: string | number;
  refunded_vat_cents: string | number;
  refund_state: CategoryRefundState[] | null;
}

interface RefundRow {
  refund_id: string;
  order_id: string;
  payment_id: string;
  merchant_id: string;
  amount_cents: string | number;
  net_cents: string | number;
  vat_cents: string | number;
  split: RefundSplitLine[];
  merchant_account: 'REVENUE' | 'PAYOUT';
  refunded_to_wallet_id: string;
  journal_ref: string;
  restored_credits: string | number;
  refund_event_id: string;
  created_at: Date | string;
}

interface LockedWallet {
  wallet_id: string;
  version: number;
  balance_cents: unknown;
  ledger_balance_cents: unknown;
}

const toRecord = (r: RefundRow): OrderRefundRecord => ({
  refundId: r.refund_id,
  orderId: r.order_id,
  paymentId: r.payment_id,
  merchantId: r.merchant_id,
  amountHalalas: Number(r.amount_cents),
  netHalalas: Number(r.net_cents),
  vatHalalas: Number(r.vat_cents),
  split: r.split,
  merchantAccount: r.merchant_account,
  refundedToWalletId: r.refunded_to_wallet_id,
  journalRef: r.journal_ref,
  restoredCredits: Number(r.restored_credits),
  refundEventId: r.refund_event_id,
  createdAt: new Date(r.created_at).toISOString(),
});

/** Transient: the order's revenue is inside a settlement batch being priced / posted right now. */
export class SettlementInFlightError extends Error {
  constructor(orderId: string, batchId: string, status: string) {
    super(`SETTLEMENT_IN_FLIGHT: order ${orderId} is in settlement batch ${batchId} (${status}); the refund is retried once it is posted`);
    this.name = 'SettlementInFlightError';
  }
}

/**
 * The ledger's part of a customer refund: reverse the captured order, partially or fully.
 *
 * The refund amount is split over what is still refundable of every component of the capture
 * (taxable amount and VAT per VAT category), pro rata with largest-remainder allocation
 * (`proRataRefundSplit`): exact to the halala, never beyond what is left, and a final refund
 * reverses exactly what remains. ONE balanced 3-leg reversal journal `rfd_<refundId>`:
 *
 *   merchant revenue (unsettled) or merchant payout (settled)   DEBIT   net share
 *   wallet_vat_payable_sar                                      DEBIT   VAT share
 *   gateway clearing (CARD) or the customer's wallet (WALLET)   CREDIT  refund amount
 *
 * Never twice: consumer checkpoint + order_refunds (primary key = refund id) + journal_ref UNIQUE
 * + the running refunded totals on the settlement (CHECKed against the capture), all in one
 * transaction with the ledger.payment.refunded outbox event. An order inside a settlement batch
 * that is still being priced / posted is retried (SETTLEMENT_IN_FLIGHT) until the batch is posted,
 * after which the merchant's payout wallet gives the money back.
 */
export class OrderRefundService {
  constructor(private readonly options: { onOutboxEnqueued?: () => void } = {}) {}

  setOutboxListener(listener: () => void): void {
    this.options.onOutboxEnqueued = listener;
  }

  async handleOrderCreditsRestored(event: OrderCreditsRestoredEvent): Promise<RefundResult> {
    const { refund, credits } = event.payload;
    const result = await withMoneyTransaction(async (client): Promise<RefundResult> => {
      const key = { consumer: LEDGER_REFUND_CONSUMER, eventId: event.eventId, step: 'done' };
      if (!(await claimCheckpoint(client, { ...key, correlationId: refund.orderId }))) {
        return { outcome: 'DUPLICATE_IGNORED', refundId: refund.refundId, orderId: refund.orderId };
      }

      const prior = await client.query<RefundRow>(`SELECT * FROM order_refunds WHERE refund_id = $1`, [refund.refundId]);
      if (prior.rows.length > 0) {
        const record = toRecord(prior.rows[0]);
        await completeCheckpoint(client, { ...key, outcome: 'DUPLICATE_IGNORED', detail: { journalRef: record.journalRef } });
        return { outcome: 'DUPLICATE_IGNORED', refundId: refund.refundId, orderId: refund.orderId, journalRef: record.journalRef, eventId: record.refundEventId };
      }

      const settlementRes = await client.query<SettlementRow>(`SELECT * FROM order_settlements WHERE order_id = $1 FOR UPDATE`, [refund.orderId]);
      if (settlementRes.rows.length === 0) {
        throw new NonRetryableError('ORDER_NOT_SETTLED', `Order ${refund.orderId} has no captured settlement in the ledger; refund ${refund.refundId} cannot be reversed`);
      }
      const settlement = settlementRes.rows[0];
      if (settlement.payment_id !== refund.paymentId) {
        throw new NonRetryableError(
          'REFUND_PAYMENT_MISMATCH',
          `Order ${refund.orderId} was settled by payment ${settlement.payment_id}; refund ${refund.refundId} references ${refund.paymentId}`
        );
      }
      const gross = Number(settlement.gross_cents);
      const refundedGross = Number(settlement.refunded_gross_cents);
      if (gross !== refund.capturedHalalas) {
        throw new NonRetryableError('REFUND_CAPTURE_MISMATCH', `Order ${refund.orderId}: the ledger captured ${gross}, the refund says ${refund.capturedHalalas}`);
      }
      if (refund.amountHalalas > gross - refundedGross) {
        throw new NonRetryableError(
          'REFUND_EXCEEDS_CAPTURE',
          `Refund ${refund.refundId} of ${refund.amountHalalas} exceeds the ${gross - refundedGross} still refundable on order ${refund.orderId}`
        );
      }
      if (!settlement.order_context) {
        throw new NonRetryableError('ORDER_CONTEXT_MISSING', `Order ${refund.orderId} was captured without its lines; the refund cannot be split`);
      }

      // Where the merchant's money is now.
      const merchant = await client.query<{ revenue_wallet_id: string; payout_wallet_id: string | null }>(
        `SELECT revenue_wallet_id, payout_wallet_id FROM merchant_accounts WHERE merchant_id = $1`,
        [settlement.merchant_id]
      );
      if (merchant.rows.length === 0) throw new NonRetryableError('MERCHANT_NOT_FOUND', `No merchant account for ${settlement.merchant_id}`);
      let merchantAccount: 'REVENUE' | 'PAYOUT' = 'REVENUE';
      if (settlement.settlement_batch_id) {
        const batch = await client.query<{ status: string }>(`SELECT status FROM merchant_settlement_batches WHERE batch_id = $1`, [settlement.settlement_batch_id]);
        const status = batch.rows[0]?.status ?? 'POSTED';
        if (status === 'COLLECTING' || status === 'COLLECTED') throw new SettlementInFlightError(refund.orderId, settlement.settlement_batch_id, status);
        merchantAccount = 'PAYOUT';
      }
      const merchantWalletId = merchantAccount === 'REVENUE' ? merchant.rows[0].revenue_wallet_id : merchant.rows[0].payout_wallet_id;
      if (!merchantWalletId) throw new NonRetryableError('MERCHANT_PAYOUT_WALLET_MISSING', `Merchant ${settlement.merchant_id} has no payout wallet`);

      // The funding side: the gateway clearing account (card), or the customer's wallet (wallet tender).
      const hold = await client.query<{ tender: Tender; source_wallet_id: string | null }>(`SELECT tender, source_wallet_id FROM payment_holds WHERE hold_id = $1`, [
        settlement.hold_id,
      ]);
      const tender: Tender = hold.rows[0]?.tender ?? 'CARD';
      const refundedTo = tender === 'WALLET' ? (hold.rows[0].source_wallet_id as string) : systemAccountsFor('SAR').clearing;

      // Pro-rata split over what is still refundable of each component.
      const state = settlement.refund_state ?? this.initialState(settlement);
      const remaining: RefundableComponent[] = state.map((c) => ({
        vatCategory: c.vatCategory,
        vatRateBps: c.vatRateBps,
        vatExemptionCode: c.vatExemptionCode,
        netHalalas: c.taxableHalalas - c.refundedNetHalalas,
        vatHalalas: c.vatHalalas - c.refundedVatHalalas,
      }));
      let lines: RefundSplitLine[];
      try {
        lines = proRataRefundSplit(remaining, refund.amountHalalas);
      } catch (err) {
        if (err instanceof RefundSplitError) throw new NonRetryableError(err.reason, err.message);
        throw err;
      }
      const net = lines.reduce((s, l) => s + l.netHalalas, 0);
      const vat = lines.reduce((s, l) => s + l.vatHalalas, 0);

      // Lock the app-maintained wallets this reversal moves (sorted), check balances, move.
      const appWallets = [merchantWalletId, ...(tender === 'WALLET' ? [refundedTo] : [])];
      const wallets = await this.lockWallets(client, appWallets);
      const merchantWallet = wallets.get(merchantWalletId);
      if (!merchantWallet) throw new NonRetryableError('MERCHANT_WALLET_NOT_FOUND', `Wallet ${merchantWalletId} does not exist`);
      assertWalletNotFrozen(merchantWalletId, merchantWallet);
      if (toBigInt(merchantWallet.balance_cents, 'balance_cents') < BigInt(net)) {
        throw new NonRetryableError(
          'MERCHANT_BALANCE_INSUFFICIENT',
          `Wallet ${merchantWalletId} holds ${String(merchantWallet.balance_cents)} but refund ${refund.refundId} reverses ${net} of revenue`
        );
      }
      if (net > 0) await this.adjustBalance(client, merchantWallet, -BigInt(net));
      if (tender === 'WALLET') {
        const customer = wallets.get(refundedTo);
        if (!customer) throw new NonRetryableError('CUSTOMER_WALLET_NOT_FOUND', `Wallet ${refundedTo} does not exist`);
        assertWalletNotFrozen(refundedTo, customer);
        await this.adjustBalance(client, customer, BigInt(refund.amountHalalas));
      }

      const journalRef = refundJournalRef(refund.refundId);
      const journal: JournalLine[] = [
        { walletId: merchantWalletId, currency: 'SAR', signedAmountMinor: -BigInt(net) },
        { walletId: vatPayableAccountFor('SAR'), currency: 'SAR', signedAmountMinor: -BigInt(vat) },
        { walletId: refundedTo, currency: 'SAR', signedAmountMinor: BigInt(refund.amountHalalas) },
      ].filter((l) => l.signedAmountMinor !== 0n) as JournalLine[];
      await postJournal(client, journalRef, journal);

      // Running totals on the settlement (the CHECKs refuse anything beyond the capture).
      const nextState = state.map((c) => {
        const line = lines.find((l) => l.vatCategory === c.vatCategory) as RefundSplitLine;
        return { ...c, refundedNetHalalas: c.refundedNetHalalas + line.netHalalas, refundedVatHalalas: c.refundedVatHalalas + line.vatHalalas };
      });
      await client.query(
        `UPDATE order_settlements SET
           refunded_gross_cents = refunded_gross_cents + $2,
           refunded_net_cents = refunded_net_cents + $3,
           refunded_vat_cents = refunded_vat_cents + $4,
           refund_state = $5::jsonb
         WHERE order_id = $1`,
        [refund.orderId, refund.amountHalalas, net, vat, JSON.stringify(nextState)]
      );
      const eventId = deterministicEventId(EVENT_TYPES.ledgerPaymentRefunded, refund.refundId);
      await client.query(
        `INSERT INTO order_refunds
           (refund_id, order_id, payment_id, merchant_id, currency, amount_cents, net_cents, vat_cents, split, merchant_account,
            refunded_to_wallet_id, journal_ref, restored_credits, source_event_id, refund_event_id)
         VALUES ($1, $2, $3, $4, 'SAR', $5, $6, $7, $8::jsonb, $9, $10, $11, $12, $13, $14)`,
        [
          refund.refundId,
          refund.orderId,
          refund.paymentId,
          settlement.merchant_id,
          refund.amountHalalas,
          net,
          vat,
          JSON.stringify(lines),
          merchantAccount,
          refundedTo,
          journalRef,
          credits.restoredCredits,
          event.eventId,
          eventId,
        ]
      );

      const payload: LedgerPaymentRefundedPayload = {
        refund,
        captureJournalRef: settlement.journal_ref,
        journalRef,
        netHalalas: net,
        vatHalalas: vat,
        lines,
        merchantAccount,
        tender,
        refundedToWalletId: refundedTo,
        remaining: {
          grossHalalas: gross - refundedGross - refund.amountHalalas,
          netHalalas: Number(settlement.net_revenue_cents) - Number(settlement.refunded_net_cents) - net,
          vatHalalas: Number(settlement.vat_cents) - Number(settlement.refunded_vat_cents) - vat,
        },
        credits,
        postedAt: new Date().toISOString(),
      };
      await enqueueIntegrationEvent(client, {
        exchange: EXCHANGES.ledger.name,
        routingKey: ROUTING_KEYS.ledgerPaymentRefunded,
        event: buildIntegrationEvent({
          eventId,
          eventType: EVENT_TYPES.ledgerPaymentRefunded,
          producer: PRODUCERS.ledger,
          correlationId: refund.orderId,
          causationId: event.eventId,
          aggregateId: refund.orderId,
          payload,
        }),
      });
      await completeCheckpoint(client, {
        ...key,
        outcome: 'REFUNDED',
        detail: { journalRef, netHalalas: net, vatHalalas: vat, merchantAccount, refundedTo, emittedEventId: eventId },
      });
      return { outcome: 'REFUNDED', refundId: refund.refundId, orderId: refund.orderId, journalRef, netHalalas: net, vatHalalas: vat, merchantAccount, eventId };
    });
    if (result.outcome === 'REFUNDED') this.options.onOutboxEnqueued?.();
    return result;
  }

  /** Refunds the ledger reversed for an order, the running totals, and its checkpoints. */
  async getOrderRefunds(orderId: string): Promise<{
    refunds: OrderRefundRecord[];
    totals: { grossHalalas: number; refundedGrossHalalas: number; refundedNetHalalas: number; refundedVatHalalas: number } | null;
    checkpoints: Checkpoint[];
  }> {
    const res = await query<RefundRow>(`SELECT * FROM order_refunds WHERE order_id = $1 ORDER BY created_at, refund_id`, [orderId]);
    const totals = await query<{ gross: string; rg: string; rn: string; rv: string }>(
      `SELECT gross_cents::text AS gross, refunded_gross_cents::text AS rg, refunded_net_cents::text AS rn, refunded_vat_cents::text AS rv
       FROM order_settlements WHERE order_id = $1`,
      [orderId]
    );
    const t = totals.rows[0];
    return {
      refunds: res.rows.map(toRecord),
      totals: t ? { grossHalalas: Number(t.gross), refundedGrossHalalas: Number(t.rg), refundedNetHalalas: Number(t.rn), refundedVatHalalas: Number(t.rv) } : null,
      checkpoints: await listCheckpoints({ query: (text, params) => query(text, params) }, { correlationId: orderId, consumer: LEDGER_REFUND_CONSUMER }),
    };
  }

  // ── internals ──────────────────────────────────────────────────────────────

  /** The capture's per-category components, from the order as booked. */
  private initialState(settlement: SettlementRow): CategoryRefundState[] {
    const context = settlement.order_context as { lines: OrderLine[]; allowanceHalalas: number };
    const totals = computeOrderTotals(context.lines, context.allowanceHalalas);
    if (totals.taxableHalalas !== Number(settlement.net_revenue_cents) || totals.vatHalalas !== Number(settlement.vat_cents)) {
      throw new NonRetryableError(
        'ORDER_CONTEXT_MISMATCH',
        `Order ${settlement.order_id}: its stored lines total ${totals.taxableHalalas} + ${totals.vatHalalas}, the capture booked ${settlement.net_revenue_cents} + ${settlement.vat_cents}`
      );
    }
    return totals.categories.map((c) => ({
      vatCategory: c.vatCategory,
      vatRateBps: c.vatRateBps,
      vatExemptionCode: context.lines.find((l) => (l.vatCategory ?? 'S') === c.vatCategory && l.vatExemptionCode)?.vatExemptionCode ?? null,
      taxableHalalas: c.taxableHalalas,
      vatHalalas: c.vatHalalas,
      refundedNetHalalas: 0,
      refundedVatHalalas: 0,
    }));
  }

  private async lockWallets(client: PoolClient, walletIds: string[]): Promise<Map<string, LockedWallet>> {
    const locked = new Map<string, LockedWallet>();
    for (const walletId of [...new Set(walletIds)].sort()) {
      const res = await client.query<LockedWallet>(
        `SELECT wallet_id, version, balance_cents, ledger_balance_cents FROM wallets WHERE wallet_id = $1 FOR UPDATE`,
        [walletId]
      );
      if (res.rows.length > 0) locked.set(walletId, res.rows[0]);
    }
    return locked;
  }

  private async adjustBalance(client: PoolClient, wallet: LockedWallet, delta: bigint): Promise<void> {
    const res = await client.query<{ version: number }>(
      `UPDATE wallets SET balance_cents = balance_cents + $1, version = version + 1, updated_at = NOW()
       WHERE wallet_id = $2 AND version = $3 AND balance_cents + $1 >= 0
       RETURNING version`,
      [delta.toString(), wallet.wallet_id, wallet.version]
    );
    if (res.rows.length === 0) throw new WalletVersionConflictError(wallet.wallet_id, Number(wallet.version));
    wallet.version = Number(res.rows[0].version);
  }
}

export const orderRefundService = new OrderRefundService();
