import type { PoolClient } from 'pg';
import { assertWalletNotFrozen, withMoneyTransaction } from './moneyTransaction';
import { postJournal } from './ledgerWriter';
import {
  CurrencyCode,
  assertSupportedCurrency,
  buildMovementJournal,
  systemAccountsFor,
  toBigInt,
} from '../money';
import {
  PayoutNotFoundError,
  PayoutStatus,
  PayoutWebhookInput,
  PayoutWebhookOutcome,
  PayoutWebhookSchema,
  WalletVersionConflictError,
} from '../types/wallet';

export interface PayoutWebhookResult {
  eventId: string;
  payoutId: string;
  payoutStatus: PayoutStatus;
  outcome: PayoutWebhookOutcome;
}

/**
 * Reconciles asynchronous bank payout webhooks. This is the completion half of
 * the withdrawal saga:
 *
 *   withdrawFunds (T1):           debit wallet, payout -> PROCESSING
 *   payout.settled (T2):          PROCESSING -> SETTLED (no money moves)
 *   payout.failed  (T2, comp.):   PROCESSING -> REVERSED, refund = compensation
 *                                 transaction that undoes T1's debit
 *
 * Guarantees:
 *  - Everything runs in one DB transaction; the payout row is locked FOR UPDATE
 *    so concurrent webhook deliveries for the same payout serialize.
 *  - Replays of the SAME eventId are detected via payout_webhook_events (PK on
 *    event_id) and ignored.
 *  - Retries with DIFFERENT eventIds are defused by the payout status state
 *    machine: a refund is only issued on the PROCESSING -> REVERSED transition,
 *    so a wallet can never be refunded twice.
 *  - Out-of-order/conflicting events (settled after reversed, failed after
 *    settled) never move money; they park the payout in RECONCILIATION_REQUIRED
 *    for a human to resolve.
 *  - The refund updates the wallet balance (version-guarded compare-and-swap,
 *    version bump) and writes the balanced ledger pair in the same transaction.
 */
export class PayoutWebhookService {
  async handlePayoutWebhook(input: PayoutWebhookInput): Promise<PayoutWebhookResult> {
    const { eventId, payoutId, eventType, failureReason } = PayoutWebhookSchema.parse(input);

    return withMoneyTransaction(async (client) => {
      const payoutRes = await client.query<{
        wallet_id: string;
        currency: string;
        amount_cents: string | number | bigint;
        status: PayoutStatus;
      }>(
        `SELECT wallet_id, currency, amount_cents, status
         FROM payouts
         WHERE payout_id = $1
         FOR UPDATE`,
        [payoutId]
      );

      if (payoutRes.rows.length === 0) {
        throw new PayoutNotFoundError(payoutId);
      }

      const payout = payoutRes.rows[0];
      const amount = toBigInt(payout.amount_cents, 'amount_cents');
      const currency = assertSupportedCurrency(payout.currency);

      // Exact replay of an already-processed event: acknowledge and do nothing.
      const existingEvent = await client.query(
        `SELECT 1 FROM payout_webhook_events WHERE event_id = $1`,
        [eventId]
      );
      if (existingEvent.rows.length > 0) {
        return {
          eventId,
          payoutId,
          payoutStatus: payout.status,
          outcome: 'DUPLICATE_IGNORED' as const,
        };
      }

      let payoutStatus: PayoutStatus = payout.status;
      let outcome: PayoutWebhookOutcome;

      if (eventType === 'payout.failed') {
        if (payout.status === 'PROCESSING') {
          // Compensation: refund the debited funds, exactly once.
          await this.refundWallet(client, payout.wallet_id, currency, amount, payoutId);
          payoutStatus = 'REVERSED';
          outcome = 'REVERSED_AND_REFUNDED';
          await this.updatePayoutStatus(client, payoutId, payoutStatus, failureReason ?? null);
        } else if (payout.status === 'SETTLED') {
          // Bank claims failure after settling: conflicting truth, no money moves.
          payoutStatus = 'RECONCILIATION_REQUIRED';
          outcome = 'RECONCILIATION_REQUIRED';
          await this.updatePayoutStatus(
            client,
            payoutId,
            payoutStatus,
            `payout.failed (${eventId}) received after payout was SETTLED`
          );
        } else {
          // Already REVERSED (retry storm) or RECONCILIATION_REQUIRED: never refund again.
          outcome = 'DUPLICATE_IGNORED';
        }
      } else {
        // payout.settled
        if (payout.status === 'PROCESSING') {
          payoutStatus = 'SETTLED';
          outcome = 'SETTLED';
          await this.updatePayoutStatus(client, payoutId, payoutStatus, null);
        } else if (payout.status === 'REVERSED') {
          // Out-of-order settlement after we already refunded: the refund must
          // NOT be clawed back automatically; flag for manual reconciliation.
          payoutStatus = 'RECONCILIATION_REQUIRED';
          outcome = 'RECONCILIATION_REQUIRED';
          await this.updatePayoutStatus(
            client,
            payoutId,
            payoutStatus,
            `payout.settled (${eventId}) received after payout was REVERSED and refunded`
          );
        } else {
          // Already SETTLED or RECONCILIATION_REQUIRED.
          outcome = 'DUPLICATE_IGNORED';
        }
      }

      await client.query(
        `INSERT INTO payout_webhook_events (event_id, payout_id, event_type, outcome)
         VALUES ($1, $2, $3, $4)`,
        [eventId, payoutId, eventType, outcome]
      );

      return { eventId, payoutId, payoutStatus, outcome };
    });
  }

  private async refundWallet(
    client: PoolClient,
    walletId: string,
    currency: CurrencyCode,
    amount: bigint,
    payoutId: string
  ): Promise<void> {
    const lockedRes = await client.query<{
      version: number;
      balance_cents: unknown;
      ledger_balance_cents: unknown;
    }>(
      `SELECT version, balance_cents, ledger_balance_cents FROM wallets WHERE wallet_id = $1 FOR UPDATE`,
      [walletId]
    );
    if (lockedRes.rows.length === 0) {
      throw new PayoutNotFoundError(payoutId);
    }
    // Refunding into a drifted wallet would fail at COMMIT; refuse cleanly
    // (409) so the bank retries the webhook after reconciliation.
    assertWalletNotFrozen(walletId, lockedRes.rows[0]);
    const lockedVersion = Number(lockedRes.rows[0].version);

    const creditRes = await client.query(
      `UPDATE wallets
       SET balance_cents = balance_cents + $1,
           version = version + 1,
           updated_at = NOW()
       WHERE wallet_id = $2
         AND version = $3
       RETURNING version`,
      [amount.toString(), walletId, lockedVersion]
    );
    if (creditRes.rows.length === 0) {
      throw new WalletVersionConflictError(walletId, lockedVersion);
    }

    // Reverse the withdrawal journal against the clearing account of the same currency.
    await postJournal(
      client,
      `rev_${payoutId}`,
      buildMovementJournal(currency, systemAccountsFor(currency).clearing, walletId, amount)
    );
  }

  private async updatePayoutStatus(
    client: PoolClient,
    payoutId: string,
    status: PayoutStatus,
    reconciliationNote: string | null
  ): Promise<void> {
    await client.query(
      `UPDATE payouts
       SET status = $1,
           reconciliation_note = COALESCE($2, reconciliation_note),
           updated_at = NOW()
       WHERE payout_id = $3`,
      [status, reconciliationNote, payoutId]
    );
  }
}

export const payoutWebhookService = new PayoutWebhookService();
