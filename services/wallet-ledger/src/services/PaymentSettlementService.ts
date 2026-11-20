import crypto from 'crypto';
import type { PoolClient } from 'pg';
import { query } from '../db';
import { JournalLine, systemAccountsFor, toBigInt, vatPayableAccountFor } from '../money';
import { postJournal } from './ledgerWriter';
import { assertWalletNotFrozen, withMoneyTransaction } from './moneyTransaction';
import {
  CurrencyMismatchError,
  InsufficientFundsError,
  SystemAccountNotAllowedError,
  WalletNotFoundError,
  WalletVersionConflictError,
} from '../types/wallet';
import {
  CONSUMERS,
  EVENT_TYPES,
  EXCHANGES,
  LedgerPaymentReleasedPayload,
  LedgerPaymentSettledPayload,
  OrderCreditsSettledEvent,
  OrderTotalsError,
  PRODUCERS,
  ROUTING_KEYS,
  Tender,
  buildIntegrationEvent,
  checkoutHoldKey,
  computeOrderTotals,
  deterministicEventId,
} from '../../../../shared/contracts';
import {
  NonRetryableError,
  claimCheckpoint,
  completeCheckpoint,
  enqueueIntegrationEvent,
  listCheckpoints,
  type Checkpoint,
} from '../../../../shared/messaging';

export const LEDGER_SETTLEMENT_CONSUMER = CONSUMERS.ledgerOrderSettlements;

export type PaymentHoldStatus = 'HELD' | 'CAPTURED' | 'RELEASED';

export interface PaymentHold {
  holdId: string;
  holdKey: string;
  paymentId: string | null;
  checkoutId: string | null;
  orderId: string;
  merchantId: string;
  currency: string;
  tender: Tender;
  sourceWalletId: string | null;
  amountCents: number;
  status: PaymentHoldStatus;
  reserveJournalRef: string | null;
  journalRef: string | null;
  releaseJournalRef: string | null;
  releaseReason: string | null;
}

/** Legacy card authorization, keyed by the gateway payment id. */
export interface PlaceHoldInput {
  paymentId: string;
  orderId: string;
  merchantId: string;
  amountHalalas: number;
}

export interface TenderHoldInput {
  /** Idempotency key: one hold per key; a released key never places again. */
  holdKey: string;
  orderId: string;
  merchantId: string;
  tender: Tender;
  /** WALLET only: the customer's SAR wallet the amount is reserved from. */
  sourceWalletId?: string;
  amountHalalas: number;
  checkoutId?: string;
  paymentId?: string;
}

export interface ReleaseTenderHoldInput extends TenderHoldInput {
  reason: string;
}

export interface ReleaseTenderHoldResult {
  outcome: 'RELEASED' | 'ALREADY_RELEASED' | 'TOMBSTONED';
  hold: PaymentHold;
}

export type SettlementOutcome = 'CAPTURED' | 'RELEASED' | 'DUPLICATE_IGNORED';

export interface SettlementResult {
  outcome: SettlementOutcome;
  orderId: string;
  paymentId: string;
  holdId?: string;
  tender?: Tender;
  journalRef?: string;
  grossHalalas?: number;
  netRevenueHalalas?: number;
  vatHalalas?: number;
  /** eventId of the ledger.payment.settled / released event this produced. */
  eventId?: string;
}

interface MerchantRow {
  merchant_id: string;
  revenue_wallet_id: string;
  currency: string;
}

interface HoldRow {
  hold_id: string;
  hold_key: string;
  payment_id: string | null;
  checkout_id: string | null;
  order_id: string;
  merchant_id: string;
  currency: string;
  tender: Tender;
  source_wallet_id: string | null;
  amount_cents: string | number;
  status: PaymentHoldStatus;
  reserve_journal_ref: string | null;
  journal_ref: string | null;
  release_journal_ref: string | null;
  release_reason: string | null;
}

interface LockedWallet {
  wallet_id: string;
  owner_id: string;
  currency: string;
  account_type: string;
  version: number;
  balance_cents: unknown;
  ledger_balance_cents: unknown;
}

/** Hold-level business refusals, mapped to HTTP by the payments API (never retried by consumers). */
const HOLD_ERROR_STATUS: Record<string, number> = {
  HOLD_RELEASED: 409,
  HOLD_MISMATCH: 409,
  HOLD_ALREADY_CAPTURED: 409,
  HOLD_ALREADY_RELEASED: 409,
  MERCHANT_NOT_FOUND: 422,
  UNSUPPORTED_CURRENCY: 422,
  INVALID_PAYMENT_AMOUNT: 422,
  SOURCE_WALLET_REQUIRED: 422,
};

export function holdErrorStatus(reason: string): number {
  return HOLD_ERROR_STATUS[reason] ?? 422;
}

/**
 * The ledger's part of the order flow.
 *
 * The payable remainder of an order is a two-phase hold, by tender:
 *   CARD    HELD = authorized at the gateway (nothing booked) → CAPTURED: ONE balanced journal
 *             wallet_treasury_sar  DEBIT gross | merchant revenue CREDIT net | VAT payable CREDIT VAT
 *           → or RELEASED (nothing was ever booked)
 *   WALLET  HELD = the amount moved from the customer's wallet into SAR escrow (reserve journal)
 *           → CAPTURED: escrow DEBIT gross | merchant revenue CREDIT net | VAT payable CREDIT VAT
 *           → or RELEASED: escrow refunds the customer (release journal)
 *
 * The revenue / VAT split is computed from the order lines and the captured-credit allowance with
 * the same arithmetic ZATCA invoices with (per-category allowance allocation, banker's rounding)
 * and must reproduce the amount charged to the halala; otherwise nothing is posted (dead letter).
 *
 * Never twice: hold keys, the hold state machine, the consumer checkpoint, the journal, the
 * order_settlements row (primary key = order id) and the outbox event commit in ONE transaction.
 * Lock order follows the ledger hierarchy: payment hold row → user / escrow wallets (sorted) →
 * sharded accounts.
 */
export class PaymentSettlementService {
  /** Record a card authorization before its outcome is known (idempotent per payment). */
  async placeHold(input: PlaceHoldInput): Promise<PaymentHold> {
    return this.placeTenderHold({
      holdKey: `payment:${input.paymentId}`,
      paymentId: input.paymentId,
      orderId: input.orderId,
      merchantId: input.merchantId,
      tender: 'CARD',
      amountHalalas: input.amountHalalas,
    });
  }

  /**
   * Reserve the remainder of an order (checkout coordinator). Idempotent per hold key: the same
   * key returns the same hold (`created: false`); a released key is refused (HOLD_RELEASED). A
   * WALLET hold moves the amount into escrow in the same transaction, so a refusal (insufficient
   * funds, frozen / unknown wallet) leaves no hold and no money moved.
   */
  async placeTenderHold(input: TenderHoldInput): Promise<PaymentHold & { created: boolean }> {
    return withMoneyTransaction(async (client) => {
      const merchant = await this.merchantFor(client, input.merchantId);
      return this.placeWithin(client, input, merchant);
    });
  }

  /**
   * Release the hold of a key (checkout cancelled / expired / its other tender failed).
   * Idempotent; an unknown key is recorded as a RELEASED tombstone so a placement that is still
   * in flight is refused when it arrives (no orphan hold). A captured hold cannot be released.
   */
  async releaseTenderHold(input: ReleaseTenderHoldInput): Promise<ReleaseTenderHoldResult> {
    return withMoneyTransaction(async (client) => {
      const merchant = await this.merchantFor(client, input.merchantId);
      let hold = await this.lockHoldByKey(client, input.holdKey);
      if (!hold) {
        this.assertTenderShape(input);
        const inserted = await client.query<HoldRow>(
          `INSERT INTO payment_holds (hold_id, hold_key, payment_id, checkout_id, order_id, merchant_id, currency, tender,
             source_wallet_id, amount_cents, status, release_reason)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'RELEASED', $11)
           ON CONFLICT (hold_key) DO NOTHING
           RETURNING *`,
          [
            `ph_${crypto.randomUUID()}`,
            input.holdKey,
            input.paymentId ?? null,
            input.checkoutId ?? null,
            input.orderId,
            merchant.merchant_id,
            merchant.currency,
            input.tender,
            input.sourceWalletId ?? null,
            input.amountHalalas,
            `tombstone: ${input.reason}`,
          ]
        );
        if (inserted.rows.length > 0) return { outcome: 'TOMBSTONED', hold: toHold(inserted.rows[0]) };
        hold = (await this.lockHoldByKey(client, input.holdKey)) as HoldRow; // a concurrent placement won
      }
      if (hold.order_id !== input.orderId) {
        throw new NonRetryableError('HOLD_MISMATCH', `Hold ${hold.hold_key} belongs to order ${hold.order_id}, not ${input.orderId}`);
      }
      if (hold.status === 'RELEASED') return { outcome: 'ALREADY_RELEASED', hold: toHold(hold) };
      if (hold.status === 'CAPTURED') {
        throw new NonRetryableError('HOLD_ALREADY_CAPTURED', `Hold ${hold.hold_key} was already captured (${hold.journal_ref})`);
      }
      const released = await this.releaseWithin(client, hold, input.reason);
      return { outcome: 'RELEASED', hold: toHold(released) };
    });
  }

  async getHold(holdKey: string): Promise<PaymentHold | null> {
    const res = await query<HoldRow>(`SELECT * FROM payment_holds WHERE hold_key = $1`, [holdKey]);
    return res.rows[0] ? toHold(res.rows[0]) : null;
  }

  async handleOrderCreditsSettled(event: OrderCreditsSettledEvent): Promise<SettlementResult> {
    const { payment, order, credits } = event.payload;
    const base = { orderId: order.orderId, paymentId: payment.paymentId };

    return withMoneyTransaction(async (client): Promise<SettlementResult> => {
      const claimed = await claimCheckpoint(client, {
        consumer: LEDGER_SETTLEMENT_CONSUMER,
        eventId: event.eventId,
        step: 'done',
        correlationId: order.orderId,
      });
      if (!claimed) return { ...base, outcome: 'DUPLICATE_IGNORED' };

      const merchant = await this.merchantFor(client, order.merchantId);
      if (payment.amountHalalas <= 0) {
        throw new NonRetryableError('INVALID_PAYMENT_AMOUNT', `Payment ${payment.paymentId} has no amount`);
      }
      const hold = await this.holdForPayment(client, event, merchant);

      const result =
        payment.status === 'FAILED'
          ? await this.release(client, event, hold)
          : await this.capture(client, event, hold, merchant, credits.allowanceHalalas, credits.capturedCredits);

      await completeCheckpoint(client, {
        consumer: LEDGER_SETTLEMENT_CONSUMER,
        eventId: event.eventId,
        step: 'done',
        outcome: result.outcome,
        detail: {
          holdId: result.holdId,
          tender: hold.tender,
          journalRef: result.journalRef ?? null,
          grossHalalas: result.grossHalalas ?? null,
          netRevenueHalalas: result.netRevenueHalalas ?? null,
          vatHalalas: result.vatHalalas ?? null,
          emittedEventId: result.eventId ?? null,
        },
      });
      return result;
    });
  }

  /** Everything the ledger knows about an order: holds, settlement, journal lines, checkpoints. */
  async getOrderSettlement(orderId: string): Promise<{
    holds: PaymentHold[];
    settlement: Record<string, unknown> | null;
    journal: Array<{ walletId: string; currency: string; direction: string; signedAmountCents: number }>;
    checkpoints: Checkpoint[];
  }> {
    const holds = await query<HoldRow>(`SELECT * FROM payment_holds WHERE order_id = $1 ORDER BY created_at, hold_id`, [orderId]);
    const settlementRes = await query(`SELECT * FROM order_settlements WHERE order_id = $1`, [orderId]);
    const settlement = settlementRes.rows[0] ?? null;
    const journal = settlement
      ? await query<{ wallet_id: string; currency: string; direction: string; signed_amount_cents: string }>(
          `SELECT wallet_id, currency, direction, signed_amount_cents::text
           FROM ledger_entries WHERE transaction_ref = $1 ORDER BY wallet_id`,
          [settlement.journal_ref]
        )
      : { rows: [] };
    return {
      holds: holds.rows.map(toHold),
      settlement: settlement
        ? {
            orderId: settlement.order_id,
            paymentId: settlement.payment_id,
            holdId: settlement.hold_id,
            merchantId: settlement.merchant_id,
            currency: settlement.currency,
            journalRef: settlement.journal_ref,
            grossHalalas: Number(settlement.gross_cents),
            netRevenueHalalas: Number(settlement.net_revenue_cents),
            vatHalalas: Number(settlement.vat_cents),
            allowanceHalalas: Number(settlement.allowance_cents),
            capturedCredits: Number(settlement.captured_credits),
            settlementEventId: settlement.settlement_event_id,
          }
        : null,
      journal: journal.rows.map((r) => ({
        walletId: r.wallet_id,
        currency: r.currency,
        direction: r.direction,
        signedAmountCents: Number(r.signed_amount_cents),
      })),
      checkpoints: await listCheckpoints({ query: (t, p) => query(t, p) }, { correlationId: orderId }),
    };
  }

  // ── holds ────────────────────────────────────────────────────────────────────

  private assertTenderShape(input: TenderHoldInput): void {
    if (!Number.isSafeInteger(input.amountHalalas) || input.amountHalalas <= 0) {
      throw new NonRetryableError('INVALID_PAYMENT_AMOUNT', `Invalid hold amount ${input.amountHalalas}`);
    }
    if (input.tender === 'WALLET' && !input.sourceWalletId) {
      throw new NonRetryableError('SOURCE_WALLET_REQUIRED', 'A WALLET hold needs the customer wallet to reserve from');
    }
    if (input.tender === 'CARD' && input.sourceWalletId) {
      throw new NonRetryableError('HOLD_MISMATCH', 'A CARD hold does not reserve from a wallet');
    }
  }

  private async placeWithin(
    client: PoolClient,
    input: TenderHoldInput,
    merchant: MerchantRow
  ): Promise<PaymentHold & { created: boolean }> {
    this.assertTenderShape(input);
    if (input.sourceWalletId) {
      const exists = await client.query(`SELECT 1 FROM wallets WHERE wallet_id = $1`, [input.sourceWalletId]);
      if (exists.rows.length === 0) throw new WalletNotFoundError(input.sourceWalletId);
    }
    const holdId = `ph_${crypto.randomUUID()}`;
    const inserted = await client.query<HoldRow>(
      `INSERT INTO payment_holds (hold_id, hold_key, payment_id, checkout_id, order_id, merchant_id, currency, tender,
         source_wallet_id, amount_cents)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
       ON CONFLICT (hold_key) DO NOTHING
       RETURNING *`,
      [
        holdId,
        input.holdKey,
        input.paymentId ?? null,
        input.checkoutId ?? null,
        input.orderId,
        merchant.merchant_id,
        merchant.currency,
        input.tender,
        input.sourceWalletId ?? null,
        input.amountHalalas,
      ]
    );

    if (inserted.rows.length === 0) {
      const existing = (await this.lockHoldByKey(client, input.holdKey)) as HoldRow;
      if (existing.status === 'RELEASED') {
        throw new NonRetryableError(
          'HOLD_RELEASED',
          `Hold ${input.holdKey} was released and cannot be placed again (${existing.release_reason ?? 'released'})`
        );
      }
      this.assertSameHold(existing, input);
      return { ...toHold(existing), created: false };
    }

    let hold = inserted.rows[0];
    if (hold.tender === 'WALLET') hold = await this.reserveWalletFunds(client, hold);
    return { ...toHold(hold), created: true };
  }

  private assertSameHold(hold: HoldRow, input: TenderHoldInput): void {
    const same =
      hold.order_id === input.orderId &&
      hold.merchant_id === input.merchantId &&
      hold.tender === input.tender &&
      (hold.source_wallet_id ?? undefined) === (input.sourceWalletId ?? undefined) &&
      Number(hold.amount_cents) === input.amountHalalas;
    if (!same) {
      throw new NonRetryableError(
        'HOLD_MISMATCH',
        `Hold ${hold.hold_key} was placed as ${hold.tender} ${hold.amount_cents} for order ${hold.order_id} (${hold.merchant_id}), ` +
          `now requested as ${input.tender} ${input.amountHalalas} for order ${input.orderId} (${input.merchantId})`
      );
    }
  }

  /** The payment's hold: the checkout's remainder hold, or (card payments) one keyed by the payment. */
  private async holdForPayment(client: PoolClient, event: OrderCreditsSettledEvent, merchant: MerchantRow): Promise<HoldRow> {
    const { payment, order } = event.payload;
    const tender: Tender = payment.tender ?? 'CARD';
    if (order.checkoutId) {
      const hold = await this.lockHoldByKey(client, checkoutHoldKey(order.checkoutId, 'remainder'));
      if (!hold) {
        throw new NonRetryableError(
          'CHECKOUT_HOLD_NOT_FOUND',
          `Checkout ${order.checkoutId} of order ${order.orderId} has no remainder hold in the ledger`
        );
      }
      if (
        hold.order_id !== order.orderId ||
        hold.merchant_id !== order.merchantId ||
        hold.tender !== tender ||
        Number(hold.amount_cents) !== payment.amountHalalas ||
        (hold.payment_id !== null && hold.payment_id !== payment.paymentId)
      ) {
        throw new NonRetryableError(
          'HOLD_MISMATCH',
          `Checkout hold ${hold.hold_key} (${hold.tender} ${hold.amount_cents}, order ${hold.order_id}, payment ${hold.payment_id ?? '-'}) ` +
            `does not match ${tender} payment ${payment.paymentId} of ${payment.amountHalalas} for order ${order.orderId}`
        );
      }
      return hold;
    }
    if (tender !== 'CARD') {
      throw new NonRetryableError('CHECKOUT_HOLD_NOT_FOUND', `A ${tender} payment needs its checkout id`);
    }
    try {
      await this.placeWithin(
        client,
        {
          holdKey: `payment:${payment.paymentId}`,
          paymentId: payment.paymentId,
          orderId: order.orderId,
          merchantId: order.merchantId,
          tender: 'CARD',
          amountHalalas: payment.amountHalalas,
        },
        merchant
      );
    } catch (err) {
      // A released authorization is handled by the state machine (e.g. SUCCESS after release).
      if (!(err instanceof NonRetryableError && err.reason === 'HOLD_RELEASED')) throw err;
    }
    return (await this.lockHoldByKey(client, `payment:${payment.paymentId}`)) as HoldRow;
  }

  private async lockHoldByKey(client: PoolClient, holdKey: string): Promise<HoldRow | null> {
    const res = await client.query<HoldRow>(`SELECT * FROM payment_holds WHERE hold_key = $1 FOR UPDATE`, [holdKey]);
    return res.rows[0] ?? null;
  }

  /** Lock user / escrow wallets in wallet_id order (the ledger's deadlock-free hierarchy). */
  private async lockWallets(client: PoolClient, walletIds: string[]): Promise<Map<string, LockedWallet>> {
    const locked = new Map<string, LockedWallet>();
    for (const walletId of [...new Set(walletIds)].sort()) {
      const res = await client.query<LockedWallet>(
        `SELECT wallet_id, owner_id, currency, account_type, version, balance_cents, ledger_balance_cents
         FROM wallets WHERE wallet_id = $1 FOR UPDATE`,
        [walletId]
      );
      if (res.rows.length > 0) locked.set(walletId, res.rows[0]);
    }
    return locked;
  }

  /** Version-guarded compare-and-swap of an app-maintained (user / escrow) balance. */
  private async adjustBalance(client: PoolClient, wallet: LockedWallet, delta: bigint): Promise<void> {
    const res = await client.query(
      `UPDATE wallets SET balance_cents = balance_cents + $1, version = version + 1, updated_at = NOW()
       WHERE wallet_id = $2 AND version = $3 AND balance_cents + $1 >= 0
       RETURNING version`,
      [delta.toString(), wallet.wallet_id, wallet.version]
    );
    if (res.rows.length === 0) throw new WalletVersionConflictError(wallet.wallet_id, Number(wallet.version));
    wallet.version = Number(res.rows[0].version);
  }

  /** WALLET hold placement: customer wallet → SAR escrow, one balanced reserve journal. */
  private async reserveWalletFunds(client: PoolClient, hold: HoldRow): Promise<HoldRow> {
    const escrowId = systemAccountsFor('SAR').escrow;
    const sourceId = hold.source_wallet_id as string;
    const wallets = await this.lockWallets(client, [sourceId, escrowId]);
    const source = wallets.get(sourceId);
    const escrow = wallets.get(escrowId) as LockedWallet;
    if (!source) throw new WalletNotFoundError(sourceId);
    if (source.account_type !== 'USER') throw new SystemAccountNotAllowedError(sourceId, source.account_type);
    if (source.currency !== hold.currency) throw new CurrencyMismatchError(sourceId, hold.currency, source.currency);
    assertWalletNotFrozen(sourceId, source);
    assertWalletNotFrozen(escrowId, escrow);
    const amount = BigInt(hold.amount_cents);
    const balance = toBigInt(source.balance_cents, 'balance_cents');
    if (balance < amount) {
      throw new InsufficientFundsError(sourceId, Number(hold.amount_cents), Number(balance));
    }
    await this.adjustBalance(client, source, -amount);
    await this.adjustBalance(client, escrow, amount);
    const ref = `rsv_${hold.hold_id}`;
    await postJournal(client, ref, [
      { walletId: sourceId, currency: 'SAR', signedAmountMinor: -amount },
      { walletId: escrowId, currency: 'SAR', signedAmountMinor: amount },
    ]);
    const updated = await client.query<HoldRow>(
      `UPDATE payment_holds SET reserve_journal_ref = $2, updated_at = NOW() WHERE hold_id = $1 RETURNING *`,
      [hold.hold_id, ref]
    );
    return updated.rows[0];
  }

  /** HELD → RELEASED. A WALLET hold refunds the customer from escrow; a CARD hold books nothing. */
  private async releaseWithin(client: PoolClient, hold: HoldRow, reason: string, paymentId?: string): Promise<HoldRow> {
    let releaseRef: string | null = null;
    if (hold.tender === 'WALLET') {
      const escrowId = systemAccountsFor('SAR').escrow;
      const sourceId = hold.source_wallet_id as string;
      const wallets = await this.lockWallets(client, [sourceId, escrowId]);
      const source = wallets.get(sourceId) as LockedWallet;
      const escrow = wallets.get(escrowId) as LockedWallet;
      // Refunding into a drifted wallet would fail at COMMIT: refuse cleanly (retryable 409).
      assertWalletNotFrozen(sourceId, source);
      assertWalletNotFrozen(escrowId, escrow);
      const amount = BigInt(hold.amount_cents);
      await this.adjustBalance(client, escrow, -amount);
      await this.adjustBalance(client, source, amount);
      releaseRef = `rls_${hold.hold_id}`;
      await postJournal(client, releaseRef, [
        { walletId: escrowId, currency: 'SAR', signedAmountMinor: -amount },
        { walletId: sourceId, currency: 'SAR', signedAmountMinor: amount },
      ]);
    }
    const updated = await client.query<HoldRow>(
      `UPDATE payment_holds
       SET status = 'RELEASED', release_reason = $2, release_journal_ref = $3, payment_id = COALESCE(payment_id, $4), updated_at = NOW()
       WHERE hold_id = $1 RETURNING *`,
      [hold.hold_id, reason, releaseRef, paymentId ?? null]
    );
    return updated.rows[0];
  }

  // ── transitions driven by the payment outcome ───────────────────────────────────

  private async release(client: PoolClient, event: OrderCreditsSettledEvent, hold: HoldRow): Promise<SettlementResult> {
    const { payment, order } = event.payload;
    if (hold.status === 'CAPTURED') {
      throw new NonRetryableError(
        'HOLD_ALREADY_CAPTURED',
        `Payment ${payment.paymentId} reported FAILED but its hold ${hold.hold_id} was already captured (${hold.journal_ref})`
      );
    }
    const reason = `payment ${payment.paymentId} failed (${payment.resultCode})`;
    let current = hold;
    if (hold.status === 'HELD') current = await this.releaseWithin(client, hold, reason, payment.paymentId);
    const eventId = deterministicEventId(EVENT_TYPES.ledgerPaymentReleased, payment.paymentId);
    const payload: LedgerPaymentReleasedPayload = {
      orderId: order.orderId,
      paymentId: payment.paymentId,
      holdId: hold.hold_id,
      merchantId: order.merchantId,
      currency: 'SAR',
      amountHalalas: Number(hold.amount_cents),
      reason: current.release_reason ?? reason,
      releasedAt: new Date().toISOString(),
      tender: hold.tender,
      checkoutId: order.checkoutId ?? null,
    };
    await enqueueIntegrationEvent(client, {
      exchange: EXCHANGES.ledger.name,
      routingKey: ROUTING_KEYS.ledgerPaymentReleased,
      event: buildIntegrationEvent({
        eventId,
        eventType: EVENT_TYPES.ledgerPaymentReleased,
        producer: PRODUCERS.ledger,
        correlationId: order.orderId,
        causationId: event.eventId,
        aggregateId: order.orderId,
        payload,
      }),
    });
    return { outcome: 'RELEASED', orderId: order.orderId, paymentId: payment.paymentId, holdId: hold.hold_id, tender: hold.tender, eventId };
  }

  private async capture(
    client: PoolClient,
    event: OrderCreditsSettledEvent,
    hold: HoldRow,
    merchant: MerchantRow,
    allowanceHalalas: number,
    capturedCredits: number
  ): Promise<SettlementResult> {
    const { payment, order } = event.payload;
    const gross = payment.amountHalalas;

    if (hold.status === 'RELEASED') {
      throw new NonRetryableError(
        'HOLD_ALREADY_RELEASED',
        `Payment ${payment.paymentId} reported SUCCESS but its hold ${hold.hold_id} was released (${hold.release_reason})`
      );
    }

    const existing = await client.query<{ payment_id: string; journal_ref: string; settlement_event_id: string }>(
      `SELECT payment_id, journal_ref, settlement_event_id FROM order_settlements WHERE order_id = $1`,
      [order.orderId]
    );
    if (existing.rows.length > 0) {
      const prior = existing.rows[0];
      if (prior.payment_id !== payment.paymentId) {
        throw new NonRetryableError(
          'ORDER_ALREADY_SETTLED',
          `Order ${order.orderId} was already settled by payment ${prior.payment_id}; payment ${payment.paymentId} needs a refund`
        );
      }
      // Same payment captured by an earlier event: nothing is posted twice.
      return {
        outcome: 'DUPLICATE_IGNORED',
        orderId: order.orderId,
        paymentId: payment.paymentId,
        holdId: hold.hold_id,
        tender: hold.tender,
        journalRef: prior.journal_ref,
        eventId: prior.settlement_event_id,
      };
    }

    let totals;
    try {
      totals = computeOrderTotals(order.lines, allowanceHalalas);
    } catch (err) {
      if (err instanceof OrderTotalsError) throw new NonRetryableError(err.reason, err.message);
      throw err;
    }
    if (totals.totalHalalas !== gross) {
      throw new NonRetryableError(
        'AMOUNT_MISMATCH',
        `Order ${order.orderId}: paid ${gross} halalas but lines minus a ${allowanceHalalas}-halala credit allowance ` +
          `total ${totals.totalHalalas} (net ${totals.taxableHalalas} + VAT ${totals.vatHalalas})`
      );
    }
    const net = totals.taxableHalalas;
    const vat = totals.vatHalalas;

    // Lock the app-maintained balances this capture moves, in wallet_id order.
    const escrowId = systemAccountsFor('SAR').escrow;
    const funding = hold.tender === 'WALLET' ? escrowId : systemAccountsFor('SAR').clearing;
    const wallets = await this.lockWallets(client, hold.tender === 'WALLET' ? [escrowId, merchant.revenue_wallet_id] : [merchant.revenue_wallet_id]);
    const revenue = wallets.get(merchant.revenue_wallet_id);
    if (!revenue) {
      throw new NonRetryableError('MERCHANT_WALLET_NOT_FOUND', `Wallet ${merchant.revenue_wallet_id} does not exist`);
    }
    assertWalletNotFrozen(merchant.revenue_wallet_id, revenue);
    if (hold.tender === 'WALLET') {
      const escrow = wallets.get(escrowId) as LockedWallet;
      assertWalletNotFrozen(escrowId, escrow);
      await this.adjustBalance(client, escrow, -BigInt(gross));
    }
    if (net > 0) await this.adjustBalance(client, revenue, BigInt(net));

    const journalRef = `cap_${hold.hold_id}`;
    const lines: JournalLine[] = [
      { walletId: funding, currency: 'SAR', signedAmountMinor: -BigInt(gross) },
      { walletId: merchant.revenue_wallet_id, currency: 'SAR', signedAmountMinor: BigInt(net) },
      { walletId: vatPayableAccountFor('SAR'), currency: 'SAR', signedAmountMinor: BigInt(vat) },
    ].filter((line) => line.signedAmountMinor !== 0n) as JournalLine[];
    await postJournal(client, journalRef, lines);

    await client.query(
      `UPDATE payment_holds SET status = 'CAPTURED', journal_ref = $2, payment_id = COALESCE(payment_id, $3), updated_at = NOW()
       WHERE hold_id = $1`,
      [hold.hold_id, journalRef, payment.paymentId]
    );

    const eventId = deterministicEventId(EVENT_TYPES.ledgerPaymentSettled, order.orderId);
    await client.query(
      `INSERT INTO order_settlements
         (order_id, payment_id, hold_id, merchant_id, currency, journal_ref, gross_cents, net_revenue_cents,
          vat_cents, allowance_cents, captured_credits, source_event_id, settlement_event_id, order_context)
       VALUES ($1, $2, $3, $4, 'SAR', $5, $6, $7, $8, $9, $10, $11, $12, $13::jsonb)`,
      [
        order.orderId,
        payment.paymentId,
        hold.hold_id,
        merchant.merchant_id,
        journalRef,
        gross,
        net,
        vat,
        allowanceHalalas,
        capturedCredits,
        event.eventId,
        eventId,
        JSON.stringify({ lines: order.lines, allowanceHalalas, customer: order.customer }),
      ]
    );

    const payload: LedgerPaymentSettledPayload = {
      orderId: order.orderId,
      paymentId: payment.paymentId,
      holdId: hold.hold_id,
      merchantId: merchant.merchant_id,
      currency: 'SAR',
      journalRef,
      grossHalalas: gross,
      netRevenueHalalas: net,
      vatHalalas: vat,
      allowanceHalalas,
      capturedCredits,
      paymentBrand: payment.paymentBrand,
      tender: hold.tender,
      order,
      settledAt: new Date().toISOString(),
    };
    await enqueueIntegrationEvent(client, {
      exchange: EXCHANGES.ledger.name,
      routingKey: ROUTING_KEYS.ledgerPaymentSettled,
      event: buildIntegrationEvent({
        eventId,
        eventType: EVENT_TYPES.ledgerPaymentSettled,
        producer: PRODUCERS.ledger,
        correlationId: order.orderId,
        causationId: event.eventId,
        aggregateId: order.orderId,
        payload,
      }),
    });

    return {
      outcome: 'CAPTURED',
      orderId: order.orderId,
      paymentId: payment.paymentId,
      holdId: hold.hold_id,
      tender: hold.tender,
      journalRef,
      grossHalalas: gross,
      netRevenueHalalas: net,
      vatHalalas: vat,
      eventId,
    };
  }

  // ── rows ─────────────────────────────────────────────────────────────────────

  private async merchantFor(client: PoolClient, merchantId: string): Promise<MerchantRow> {
    const res = await client.query<MerchantRow>(
      `SELECT merchant_id, revenue_wallet_id, currency FROM merchant_accounts WHERE merchant_id = $1`,
      [merchantId]
    );
    if (res.rows.length === 0) {
      throw new NonRetryableError('MERCHANT_NOT_FOUND', `No merchant account for ${merchantId}`);
    }
    if (res.rows[0].currency !== 'SAR') {
      throw new NonRetryableError('UNSUPPORTED_CURRENCY', `Merchant ${merchantId} settles in ${res.rows[0].currency}`);
    }
    return res.rows[0];
  }
}

function toHold(row: HoldRow): PaymentHold {
  return {
    holdId: row.hold_id,
    holdKey: row.hold_key,
    paymentId: row.payment_id,
    checkoutId: row.checkout_id,
    orderId: row.order_id,
    merchantId: row.merchant_id,
    currency: row.currency,
    tender: row.tender,
    sourceWalletId: row.source_wallet_id,
    amountCents: Number(row.amount_cents),
    status: row.status,
    reserveJournalRef: row.reserve_journal_ref,
    journalRef: row.journal_ref,
    releaseJournalRef: row.release_journal_ref,
    releaseReason: row.release_reason,
  };
}

export const paymentSettlementService = new PaymentSettlementService();
