import crypto from 'crypto';
import type { PoolClient } from 'pg';
import { query } from '../db';
import { config } from '../config/env';
import { postJournal } from './ledgerWriter';
import { ActorContext, canReadWallet, ownsWallet } from '../auth/principal';
import { assertWalletNotFrozen, withMoneyTransaction } from './moneyTransaction';
import {
  CurrencyCode,
  FxRateUnavailableError,
  TransferQuote,
  assertSupportedCurrency,
  buildMovementJournal,
  buildSettlementJournal,
  convertMinorUnits,
  quoteTransfer,
  systemAccountsFor,
  toBigInt,
  toSafeNumber,
} from '../money';
import {
  CurrencyMismatchError,
  InsufficientFundsError,
  SystemAccountNotAllowedError,
  TransferFundsInput,
  TransferFundsSchema,
  TransferNotFoundError,
  TransferReviewInput,
  TransferReviewSchema,
  TransferStateError,
  TransferStatus,
  WalletNotFoundError,
  WalletVersionConflictError,
} from '../types/wallet';

/** Legacy export: the USD escrow account. Use systemAccountsFor(ccy).escrow. */
export const ESCROW_WALLET_ID = systemAccountsFor('USD').escrow;

/** JSON-safe view of a quote (all integers, minor units). */
export interface QuoteView {
  sourceCurrency: CurrencyCode;
  destCurrency: CurrencyCode;
  feeBps: number;
  feeCents: number;
  totalDebitCents: number;
  /** Scaled integer rate (x 10^8); 100000000 for same-currency transfers. */
  fxRateScaled: number;
  creditedAmountCents: number;
}

export interface TransferFundsResult extends QuoteView {
  transferId: string;
  fromWalletId: string;
  toWalletId: string;
  /** Principal, in source-currency minor units (fee is charged on top). */
  amountCents: number;
  status: Extract<TransferStatus, 'COMPLETED' | 'PENDING_REVIEW'>;
  /** True when the funds were parked in escrow pending AML review. */
  amlReviewRequired: boolean;
  senderRemainingBalanceCents: number;
  /** Sender wallet version after the debit; usable as `expectedVersion` later. */
  senderVersion: number;
}

export interface TransferReviewResult extends QuoteView {
  transferId: string;
  decision: 'APPROVE' | 'REJECT';
  status: Extract<TransferStatus, 'COMPLETED' | 'REVERSED'>;
  outcome: 'RELEASED_TO_RECIPIENT' | 'COMPENSATED_REFUNDED_TO_SENDER';
  fromWalletId: string;
  toWalletId: string;
  amountCents: number;
}

interface LockedWallet {
  walletId: string;
  balance: bigint;
  version: number;
}

interface TransferRow {
  from_wallet_id: string;
  to_wallet_id: string;
  amount_cents: unknown;
  status: TransferStatus;
  reviewed_by: string | null;
  source_currency: string;
  dest_currency: string;
  fee_bps: number;
  fee_cents: unknown;
  total_debit_cents: unknown;
  fx_rate_scaled: unknown;
  credited_amount_cents: unknown;
}

const TRANSFER_COLUMNS = `from_wallet_id, to_wallet_id, amount_cents, status, reviewed_by,
  source_currency, dest_currency, fee_bps, fee_cents, total_debit_cents,
  fx_rate_scaled, credited_amount_cents`;

/**
 * Orchestrates P2P transfers (same- and cross-currency) as a saga.
 *
 * Pricing: every transfer is priced once by the integer fee/FX engine
 * (src/money) and the resulting quote is persisted on the transfer row. The
 * sender pays principal + fee; the recipient receives the principal converted
 * to their wallet's currency (banker's rounding). Fees go to the fee treasury
 * of the SOURCE currency; cross-currency principal routes through the per-
 * currency FX pools, so every currency's journal balances to exactly zero.
 *
 * Saga layout:
 *   Fast path (USD-equivalent <= AML threshold) — one local transaction:
 *     SENDER_DEBITED -> [FEE_COLLECTED] -> [FX_CONVERTED] -> RECIPIENT_CREDITED -> TRANSFER_COMPLETED
 *   AML escrow path — two local transactions:
 *     T1: SENDER_DEBITED -> ESCROW_HELD -> AML_REVIEW_PENDING   (principal + fee held, source ccy)
 *     T2 APPROVE: ESCROW_RELEASED -> [FEE_COLLECTED] -> [FX_CONVERTED] -> RECIPIENT_CREDITED -> TRANSFER_COMPLETED
 *     T2 REJECT : ESCROW_RELEASED -> COMPENSATION_SENDER_REFUNDED -> TRANSFER_REVERSED  (full refund incl. fee)
 *
 * Concurrency: wallet rows are locked FOR UPDATE in global lexicographic order
 * (AB-BA deadlock-free); every balance change is a version-guarded CAS; the
 * participants' currencies are resolved BEFORE locking so the per-currency
 * escrow joins the same ordered lock set. Ledger-only system accounts (fee
 * treasuries, FX pools) are never row-locked, so they are not hot rows.
 */
export class TransferSagaService {
  async executeTransfer(input: TransferFundsInput, ctx: ActorContext = {}): Promise<TransferFundsResult> {
    const { fromWalletId, toWalletId, amountCents, referenceId, expectedVersion, currency } =
      TransferFundsSchema.parse(input);

    return withMoneyTransaction(async (client) => {
      // 1. Resolve participants and their (immutable) currencies. No locks, no writes.
      const participants = await client.query<{
        wallet_id: string;
        currency: string;
        account_type: string;
        owner_id: string;
      }>(
        `SELECT wallet_id, currency, account_type, owner_id FROM wallets WHERE wallet_id IN ($1, $2)`,
        [fromWalletId, toWalletId]
      );
      const byId = new Map(participants.rows.map((r) => [r.wallet_id, r]));
      const senderMeta = byId.get(fromWalletId);
      const recipientMeta = byId.get(toWalletId);
      // IDOR guard: only the sender's owner may spend from it. Not-yours is
      // reported exactly like not-found (no enumeration oracle). The recipient
      // may be anyone's wallet: that is what a P2P transfer is.
      if (!senderMeta || !ownsWallet(ctx.actor, senderMeta.owner_id)) throw new WalletNotFoundError(fromWalletId);
      if (!recipientMeta) throw new WalletNotFoundError(toWalletId);
      for (const meta of [senderMeta, recipientMeta]) {
        if (meta.account_type !== 'USER') {
          throw new SystemAccountNotAllowedError(meta.wallet_id, meta.account_type);
        }
      }

      const sourceCurrency = assertSupportedCurrency(senderMeta.currency);
      const destCurrency = assertSupportedCurrency(recipientMeta.currency);
      if (currency !== undefined && currency !== sourceCurrency) {
        throw new CurrencyMismatchError(fromWalletId, currency, sourceCurrency);
      }

      // 2. Price the transfer with the integer engine (rates read in this transaction).
      const principal = BigInt(amountCents);
      const quote = quoteTransfer({
        principalMinor: principal,
        sourceCurrency,
        destCurrency,
        feeBps: config.platformFeeBps,
        fxRateScaled:
          sourceCurrency === destCurrency
            ? undefined
            : await this.requireFxRate(client, sourceCurrency, destCurrency),
      });

      // 3. AML decision on the USD-equivalent principal (fails closed to review).
      const amlReviewRequired = await this.requiresAmlReview(client, principal, sourceCurrency);
      const escrowId = systemAccountsFor(sourceCurrency).escrow;

      // 4. Pessimistic locks in global order (AB-BA deadlock prevention).
      const locked = await this.lockWalletsInOrder(
        client,
        amlReviewRequired ? [fromWalletId, toWalletId, escrowId] : [fromWalletId, toWalletId]
      );
      const sender = locked.get(fromWalletId);
      const recipient = locked.get(toWalletId);
      if (!sender) throw new WalletNotFoundError(fromWalletId);
      if (!recipient) throw new WalletNotFoundError(toWalletId);
      const escrow = amlReviewRequired ? locked.get(escrowId) : undefined;
      if (amlReviewRequired && !escrow) throw new WalletNotFoundError(escrowId);

      if (expectedVersion !== undefined && expectedVersion !== sender.version) {
        throw new WalletVersionConflictError(fromWalletId, expectedVersion, sender.version);
      }
      if (sender.balance < quote.totalDebitMinor) {
        throw new InsufficientFundsError(
          fromWalletId,
          toSafeNumber(quote.totalDebitMinor),
          toSafeNumber(sender.balance)
        );
      }

      // 5. Writes.
      const transferId = `tr_${crypto.randomUUID()}`;
      const status: TransferFundsResult['status'] = amlReviewRequired ? 'PENDING_REVIEW' : 'COMPLETED';
      await this.insertTransfer(client, transferId, fromWalletId, toWalletId, referenceId, status, quote);

      const debitedSender = await this.applyBalanceChange(client, sender, -quote.totalDebitMinor);
      await this.recordSagaStep(client, transferId, 'SENDER_DEBITED', `-${quote.totalDebitMinor} ${sourceCurrency} from ${fromWalletId}`);

      if (!amlReviewRequired) {
        await this.applyBalanceChange(client, recipient, quote.creditedMinor);
        await postJournal(client, transferId, buildSettlementJournal(quote, fromWalletId, toWalletId));
        await this.recordSettlementSteps(client, transferId, quote, toWalletId);
      } else {
        await this.applyBalanceChange(client, escrow!, quote.totalDebitMinor);
        await postJournal(
          client,
          transferId,
          buildMovementJournal(sourceCurrency, fromWalletId, escrowId, quote.totalDebitMinor)
        );
        await this.recordSagaStep(client, transferId, 'ESCROW_HELD', `+${quote.totalDebitMinor} ${sourceCurrency} held in ${escrowId}`);
        await this.recordSagaStep(
          client,
          transferId,
          'AML_REVIEW_PENDING',
          `USD-equivalent exceeds AML threshold ${config.amlReviewThresholdCents}`
        );
      }

      return {
        transferId,
        fromWalletId,
        toWalletId,
        amountCents,
        status,
        amlReviewRequired,
        senderRemainingBalanceCents: toSafeNumber(debitedSender.balance),
        senderVersion: debitedSender.version,
        ...this.quoteView(quote),
      };
    });
  }

  /**
   * Complete (APPROVE) or compensate (REJECT) a transfer held for AML review,
   * using exactly the quote locked in at initiation.
   */
  async reviewTransfer(transferId: string, input: TransferReviewInput): Promise<TransferReviewResult> {
    const { decision, reviewedBy, reviewNote } = TransferReviewSchema.parse(input);

    return withMoneyTransaction(async (client) => {
      // Lock the saga instance first so concurrent reviews serialize on it.
      const transferRes = await client.query<TransferRow>(
        `SELECT ${TRANSFER_COLUMNS} FROM transfers WHERE transfer_id = $1 FOR UPDATE`,
        [transferId]
      );
      if (transferRes.rows.length === 0) {
        throw new TransferNotFoundError(transferId);
      }

      const row = transferRes.rows[0];
      if (row.status !== 'PENDING_REVIEW') {
        throw new TransferStateError(
          transferId,
          `cannot review a transfer in status ${row.status}; expected PENDING_REVIEW`
        );
      }

      const quote = this.quoteFromRow(row);
      const fromWalletId = row.from_wallet_id;
      const toWalletId = row.to_wallet_id;
      const escrowId = systemAccountsFor(quote.sourceCurrency).escrow;

      const locked = await this.lockWalletsInOrder(client, [fromWalletId, toWalletId, escrowId]);
      const escrow = locked.get(escrowId);
      if (!escrow) {
        throw new WalletNotFoundError(escrowId);
      }
      if (escrow.balance < quote.totalDebitMinor) {
        // Hard invariant violation: held funds must sit in escrow until reviewed.
        throw new TransferStateError(transferId, 'escrow wallet does not hold the reviewed funds');
      }

      await this.applyBalanceChange(client, escrow, -quote.totalDebitMinor);
      await this.recordSagaStep(client, transferId, 'ESCROW_RELEASED', `-${quote.totalDebitMinor} ${quote.sourceCurrency} from ${escrowId}`);

      let status: TransferReviewResult['status'];
      let outcome: TransferReviewResult['outcome'];

      if (decision === 'APPROVE') {
        const recipient = locked.get(toWalletId);
        if (!recipient) {
          throw new WalletNotFoundError(toWalletId);
        }
        await this.applyBalanceChange(client, recipient, quote.creditedMinor);
        await postJournal(client, `release_${transferId}`, buildSettlementJournal(quote, escrowId, toWalletId));
        await this.recordSettlementSteps(client, transferId, quote, toWalletId, reviewNote ?? null);
        status = 'COMPLETED';
        outcome = 'RELEASED_TO_RECIPIENT';
      } else {
        // Compensation transaction: undo T1 entirely, fee included.
        const sender = locked.get(fromWalletId);
        if (!sender) {
          throw new WalletNotFoundError(fromWalletId);
        }
        await this.applyBalanceChange(client, sender, quote.totalDebitMinor);
        await postJournal(
          client,
          `refund_${transferId}`,
          buildMovementJournal(quote.sourceCurrency, escrowId, fromWalletId, quote.totalDebitMinor)
        );
        await this.recordSagaStep(
          client,
          transferId,
          'COMPENSATION_SENDER_REFUNDED',
          `+${quote.totalDebitMinor} ${quote.sourceCurrency} back to ${fromWalletId}`
        );
        await this.recordSagaStep(client, transferId, 'TRANSFER_REVERSED', reviewNote ?? null);
        status = 'REVERSED';
        outcome = 'COMPENSATED_REFUNDED_TO_SENDER';
      }

      await client.query(
        `UPDATE transfers SET status = $1, reviewed_by = $2, updated_at = NOW() WHERE transfer_id = $3`,
        [status, reviewedBy ?? null, transferId]
      );

      return {
        transferId,
        decision,
        status,
        outcome,
        fromWalletId,
        toWalletId,
        amountCents: toSafeNumber(quote.principalMinor),
        ...this.quoteView(quote),
      };
    });
  }

  /**
   * Fetch a transfer, its locked quote, and its ordered saga step history.
   * With an actor: visible only to the owners of the sending or receiving
   * wallet (and admins); otherwise reported as not found (IDOR guard).
   */
  async getTransfer(transferId: string, ctx: ActorContext = {}): Promise<
    | (QuoteView & {
        transferId: string;
        fromWalletId: string;
        toWalletId: string;
        amountCents: number;
        status: TransferStatus;
        reviewedBy: string | null;
        sagaSteps: Array<{ step: string; detail: string | null }>;
      })
    | null
  > {
    const res = await query<TransferRow & { from_owner: string; to_owner: string }>(
      `SELECT ${TRANSFER_COLUMNS}, fw.owner_id AS from_owner, tw.owner_id AS to_owner
       FROM transfers t
       JOIN wallets fw ON fw.wallet_id = t.from_wallet_id
       JOIN wallets tw ON tw.wallet_id = t.to_wallet_id
       WHERE t.transfer_id = $1`,
      [transferId]
    );
    if (res.rows.length === 0) return null;
    const visible =
      canReadWallet(ctx.actor, res.rows[0].from_owner) || canReadWallet(ctx.actor, res.rows[0].to_owner);
    if (!visible) return null;

    // Bounded: a saga has a small fixed number of steps.
    const steps = await query<{ step: string; detail: string | null }>(
      `SELECT step, detail FROM transfer_saga_steps WHERE transfer_id = $1 ORDER BY seq ASC LIMIT 100`,
      [transferId]
    );

    const row = res.rows[0];
    const quote = this.quoteFromRow(row);
    return {
      transferId,
      fromWalletId: row.from_wallet_id,
      toWalletId: row.to_wallet_id,
      amountCents: toSafeNumber(quote.principalMinor),
      status: row.status,
      reviewedBy: row.reviewed_by,
      sagaSteps: steps.rows.map((s) => ({ step: s.step, detail: s.detail })),
      ...this.quoteView(quote),
    };
  }

  // --------------------------------------------------------------------------

  private async requireFxRate(client: PoolClient, base: CurrencyCode, quote: CurrencyCode): Promise<bigint> {
    const rate = await this.findFxRate(client, base, quote);
    if (rate === null) {
      throw new FxRateUnavailableError(base, quote);
    }
    return rate;
  }

  private async findFxRate(client: PoolClient, base: CurrencyCode, quote: CurrencyCode): Promise<bigint | null> {
    const res = await client.query<{ rate_scaled: unknown }>(
      `SELECT rate_scaled FROM fx_rates WHERE base_currency = $1 AND quote_currency = $2`,
      [base, quote]
    );
    return res.rows.length === 0 ? null : toBigInt(res.rows[0].rate_scaled, 'rate_scaled');
  }

  /** AML threshold is in USD minor units; other currencies are converted first. */
  private async requiresAmlReview(client: PoolClient, principal: bigint, currency: CurrencyCode): Promise<boolean> {
    const threshold = BigInt(config.amlReviewThresholdCents);
    if (currency === 'USD') {
      return principal > threshold;
    }
    const toUsd = await this.findFxRate(client, currency, 'USD');
    if (toUsd === null) {
      return true; // cannot evaluate exposure -> fail closed into manual review
    }
    return convertMinorUnits(principal, currency, 'USD', toUsd) > threshold;
  }

  private async insertTransfer(
    client: PoolClient,
    transferId: string,
    fromWalletId: string,
    toWalletId: string,
    referenceId: string | undefined,
    status: TransferStatus,
    quote: TransferQuote
  ): Promise<void> {
    await client.query(
      `INSERT INTO transfers (
         transfer_id, from_wallet_id, to_wallet_id, amount_cents, reference_id, status,
         source_currency, dest_currency, fee_bps, fee_cents, total_debit_cents,
         fx_rate_scaled, credited_amount_cents)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
      [
        transferId,
        fromWalletId,
        toWalletId,
        quote.principalMinor.toString(),
        referenceId ?? null,
        status,
        quote.sourceCurrency,
        quote.destCurrency,
        quote.feeBps,
        quote.feeMinor.toString(),
        quote.totalDebitMinor.toString(),
        quote.fxRateScaled.toString(),
        quote.creditedMinor.toString(),
      ]
    );
  }

  private quoteFromRow(row: TransferRow): TransferQuote {
    return {
      sourceCurrency: assertSupportedCurrency(row.source_currency),
      destCurrency: assertSupportedCurrency(row.dest_currency),
      principalMinor: toBigInt(row.amount_cents, 'amount_cents'),
      feeBps: Number(row.fee_bps),
      feeMinor: toBigInt(row.fee_cents, 'fee_cents'),
      totalDebitMinor: toBigInt(row.total_debit_cents, 'total_debit_cents'),
      fxRateScaled: toBigInt(row.fx_rate_scaled, 'fx_rate_scaled'),
      creditedMinor: toBigInt(row.credited_amount_cents, 'credited_amount_cents'),
    };
  }

  private quoteView(quote: TransferQuote): QuoteView {
    return {
      sourceCurrency: quote.sourceCurrency,
      destCurrency: quote.destCurrency,
      feeBps: quote.feeBps,
      feeCents: toSafeNumber(quote.feeMinor),
      totalDebitCents: toSafeNumber(quote.totalDebitMinor),
      fxRateScaled: toSafeNumber(quote.fxRateScaled),
      creditedAmountCents: toSafeNumber(quote.creditedMinor),
    };
  }

  private async recordSettlementSteps(
    client: PoolClient,
    transferId: string,
    quote: TransferQuote,
    recipientWalletId: string,
    completionNote: string | null = null
  ): Promise<void> {
    if (quote.feeMinor > 0n) {
      await this.recordSagaStep(
        client,
        transferId,
        'FEE_COLLECTED',
        `${quote.feeMinor} ${quote.sourceCurrency} (${quote.feeBps} bps) to ${systemAccountsFor(quote.sourceCurrency).feeTreasury}`
      );
    }
    if (quote.sourceCurrency !== quote.destCurrency) {
      await this.recordSagaStep(
        client,
        transferId,
        'FX_CONVERTED',
        `${quote.principalMinor} ${quote.sourceCurrency} -> ${quote.creditedMinor} ${quote.destCurrency} @ ${quote.fxRateScaled}/1e8`
      );
    }
    await this.recordSagaStep(
      client,
      transferId,
      'RECIPIENT_CREDITED',
      `+${quote.creditedMinor} ${quote.destCurrency} to ${recipientWalletId}`
    );
    await this.recordSagaStep(client, transferId, 'TRANSFER_COMPLETED', completionNote);
  }

  /**
   * Lock wallet rows with SELECT ... FOR UPDATE in lexicographic wallet_id order.
   * Locking in one global order prevents AB-BA deadlocks between concurrent
   * transfers touching the same wallets in opposite directions. Every locked
   * wallet is checked for drift under its lock (frozen -> 409, race-free).
   */
  private async lockWalletsInOrder(client: PoolClient, walletIds: string[]): Promise<Map<string, LockedWallet>> {
    const ordered = [...new Set(walletIds)].sort();
    const locked = new Map<string, LockedWallet>();

    for (const walletId of ordered) {
      const res = await client.query<{ balance_cents: unknown; ledger_balance_cents: unknown; version: number }>(
        `SELECT balance_cents, ledger_balance_cents, version
         FROM wallets
         WHERE wallet_id = $1
         FOR UPDATE`,
        [walletId]
      );
      if (res.rows.length > 0) {
        assertWalletNotFrozen(walletId, res.rows[0]);
        locked.set(walletId, {
          walletId,
          balance: toBigInt(res.rows[0].balance_cents, 'balance_cents'),
          version: Number(res.rows[0].version),
        });
      }
    }

    return locked;
  }

  /**
   * Version-guarded compare-and-swap balance change. Under the row lock the
   * guard always matches; if any writer bypassed the lock, this affects zero
   * rows and the whole transaction fails closed with a version conflict.
   */
  private async applyBalanceChange(client: PoolClient, wallet: LockedWallet, delta: bigint): Promise<LockedWallet> {
    const res = await client.query<{ balance_cents: unknown; version: number }>(
      `UPDATE wallets
       SET balance_cents = balance_cents + $1,
           version = version + 1,
           updated_at = NOW()
       WHERE wallet_id = $2
         AND version = $3
         AND balance_cents + $1 >= 0
       RETURNING balance_cents, version`,
      [delta.toString(), wallet.walletId, wallet.version]
    );

    if (res.rows.length === 0) {
      throw new WalletVersionConflictError(wallet.walletId, wallet.version);
    }

    return {
      walletId: wallet.walletId,
      balance: toBigInt(res.rows[0].balance_cents, 'balance_cents'),
      version: Number(res.rows[0].version),
    };
  }

  private async recordSagaStep(
    client: PoolClient,
    transferId: string,
    step: string,
    detail: string | null = null
  ): Promise<void> {
    await client.query(
      `INSERT INTO transfer_saga_steps (transfer_id, step, detail) VALUES ($1, $2, $3)`,
      [transferId, step, detail]
    );
  }
}

export const transferSagaService = new TransferSagaService();
