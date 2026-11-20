import crypto from 'crypto';
import { transferSagaService, TransferFundsResult } from './TransferSagaService';
import { postJournal } from './ledgerWriter';
import { assertWalletNotFrozen, withMoneyTransaction } from './moneyTransaction';
import { readWalletState } from './walletState';
import { ActorContext, ownsWallet } from '../auth/principal';
import {
  assertSupportedCurrency,
  buildMovementJournal,
  systemAccountsFor,
  toBigInt,
  toSafeNumber,
} from '../money';
import {
  InsufficientFundsError,
  SystemAccountNotAllowedError,
  TransferFundsInput,
  WalletAccount,
  WalletNotFoundError,
  WalletVersionConflictError,
  WithdrawFundsInput,
  WithdrawFundsSchema,
} from '../types/wallet';

export interface WithdrawFundsResult {
  payoutId: string;
  walletId: string;
  currency: string;
  amountCents: number;
  remainingBalanceCents: number;
  /** Wallet version after the debit; pass as `expectedVersion` on the next optimistic call. */
  version: number;
  status: 'PROCESSING';
}

export class WalletService {
  /**
   * Fetch wallet account snapshot by ID.
   */
  async getWallet(walletId: string): Promise<WalletAccount | null> {
    const state = await readWalletState(walletId);
    if (!state) {
      return null;
    }
    return {
      walletId: state.walletId,
      ownerId: state.ownerId,
      currency: state.currency,
      accountType: state.accountType,
      balanceCents: toSafeNumber(state.balance, 'balance_cents'),
      ledgerBalanceCents: toSafeNumber(state.ledgerBalance, 'ledger_balance_cents'),
      ledgerVersion: toSafeNumber(state.ledgerVersion, 'ledger_version'),
      shardCount: state.shardCount,
      version: state.version,
      frozenPendingReconciliation: state.balance !== state.ledgerBalance,
      updatedAt: state.updatedAt,
    };
  }

  /**
   * Withdraw funds from a user's wallet to an external bank account and record a pending payout.
   *
   * Concurrency model (all steps run in ONE database transaction):
   *
   *  1. Pessimistic lock: `SELECT ... FOR UPDATE` takes a row lock on the wallet, so concurrent
   *     withdrawals on the same wallet queue up behind each other instead of all reading the
   *     same stale balance (the double-spend bug).
   *  2. Optimistic check (caller-supplied): if `expectedVersion` is passed, the withdrawal is
   *     rejected with `WalletVersionConflictError` unless the wallet is still at that version.
   *     This lets multi-step callers (e.g. a future saga orchestrator) assert that the wallet
   *     has not changed since they read it.
   *  3. Optimistic compare-and-swap: the debit is applied with
   *     `WHERE version = <locked version> AND balance_cents >= amount` and bumps `version`.
   *     Under the row lock this always matches; it exists as a guard against any writer
   *     that bypasses the lock, and fails closed (rollback + conflict) if it ever does not.
   *  4. Payout row and balanced double-entry ledger rows are written in the same transaction,
   *     so either everything commits or nothing does.
   */
  async withdrawFunds(input: WithdrawFundsInput, ctx: ActorContext = {}): Promise<WithdrawFundsResult> {
    // Validate here as well as at the route so internal callers cannot, for example,
    // pass a negative amount and turn a withdrawal into a credit.
    const { walletId, amountCents, bankAccountRef, idempotencyKey, expectedVersion } =
      WithdrawFundsSchema.parse(input);

    return withMoneyTransaction(async (client) => {
      // 1. Pessimistic lock: read balance + version while holding the row lock.
      const lockedRes = await client.query<{
        balance_cents: unknown;
        ledger_balance_cents: unknown;
        version: number;
        currency: string;
        account_type: string;
        owner_id: string;
      }>(
        `SELECT balance_cents, ledger_balance_cents, version, currency, account_type, owner_id
         FROM wallets
         WHERE wallet_id = $1
         FOR UPDATE`,
        [walletId]
      );

      if (lockedRes.rows.length === 0) {
        throw new WalletNotFoundError(walletId);
      }

      // IDOR guard: only the owner may move a wallet's money. Not-yours is
      // reported exactly like not-found (no existence/enumeration oracle).
      if (!ownsWallet(ctx.actor, lockedRes.rows[0].owner_id)) {
        throw new WalletNotFoundError(walletId);
      }
      if (lockedRes.rows[0].account_type !== 'USER') {
        throw new SystemAccountNotAllowedError(walletId, lockedRes.rows[0].account_type);
      }
      // Drifted wallets are frozen (409), checked under the row lock: race-free.
      assertWalletNotFrozen(walletId, lockedRes.rows[0]);

      const currentBalance = toBigInt(lockedRes.rows[0].balance_cents, 'balance_cents');
      const lockedVersion = Number(lockedRes.rows[0].version);
      const currency = assertSupportedCurrency(lockedRes.rows[0].currency);
      const amount = BigInt(amountCents);

      // 2. Optimistic check against the caller's view of the wallet.
      if (expectedVersion !== undefined && expectedVersion !== lockedVersion) {
        throw new WalletVersionConflictError(walletId, expectedVersion, lockedVersion);
      }

      if (currentBalance < amount) {
        throw new InsufficientFundsError(walletId, amountCents, toSafeNumber(currentBalance));
      }

      // 3. Compare-and-swap debit: relative update, version-guarded, never below zero.
      const debitRes = await client.query<{
        balance_cents: unknown;
        version: number;
      }>(
        `UPDATE wallets
         SET balance_cents = balance_cents - $1,
             version = version + 1,
             updated_at = NOW()
         WHERE wallet_id = $2
           AND version = $3
           AND balance_cents >= $1
         RETURNING balance_cents, version`,
        [amount.toString(), walletId, lockedVersion]
      );

      if (debitRes.rows.length === 0) {
        throw new WalletVersionConflictError(walletId, lockedVersion);
      }

      const remainingBalance = toBigInt(debitRes.rows[0].balance_cents, 'balance_cents');
      const newVersion = Number(debitRes.rows[0].version);
      const payoutId = `po_${crypto.randomUUID()}`;

      // 4. Payout + double-entry ledger rows, atomically with the debit. The
      //    credit leg goes to the clearing account of the wallet's own currency.
      await client.query(
        `INSERT INTO payouts (payout_id, wallet_id, currency, amount_cents, bank_account_ref, status, idempotency_key)
         VALUES ($1, $2, $3, $4, $5, 'PROCESSING', $6)`,
        [payoutId, walletId, currency, amount.toString(), bankAccountRef, idempotencyKey ?? null]
      );

      await postJournal(
        client,
        payoutId,
        buildMovementJournal(currency, walletId, systemAccountsFor(currency).clearing, amount)
      );

      return {
        payoutId,
        walletId,
        currency,
        amountCents,
        remainingBalanceCents: toSafeNumber(remainingBalance, 'remainingBalance'),
        version: newVersion,
        status: 'PROCESSING' as const,
      };
    });
  }

  /**
   * Transfer funds between two wallets. Delegates to the transfer saga
   * orchestrator, which owns deadlock-free locking, AML escrow holds and
   * compensation. See TransferSagaService for the full concurrency model.
   */
  async transferFunds(input: TransferFundsInput, ctx: ActorContext = {}): Promise<TransferFundsResult> {
    return transferSagaService.executeTransfer(input, ctx);
  }
}

export const walletService = new WalletService();
