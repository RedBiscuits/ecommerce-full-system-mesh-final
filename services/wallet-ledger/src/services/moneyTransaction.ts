import crypto from 'crypto';
import type { PoolClient } from 'pg';
import { query, withTransaction } from '../db';
import { readWalletState } from './walletState';
import { config } from '../config/env';
import { redis } from '../redis';
import { LEDGER_EVENT_TYPES, createEnvelope, getEventBus } from '../messaging';
import { WalletFrozenError } from '../types/wallet';

/** Row shape every money path reads when it locks a wallet. */
export interface LockedBalanceRow {
  balance_cents: unknown;
  ledger_balance_cents: unknown;
}

/**
 * Write guard, evaluated right after `SELECT ... FOR UPDATE`. O(1): compares
 * two precalculated columns. Race-free: while we hold the row lock nobody can
 * change the balance or (via the delta trigger) the ledger total.
 */
export function assertWalletNotFrozen(walletId: string, row: LockedBalanceRow): void {
  if (String(row.balance_cents) !== String(row.ledger_balance_cents)) {
    throw new WalletFrozenError(walletId);
  }
}

interface PgLikeError {
  code?: unknown;
  detail?: unknown;
  message?: unknown;
}

function ledgerSyncViolationWalletId(err: unknown): string | null {
  const e = err as PgLikeError | null;
  if (
    e &&
    e.code === '23514' &&
    typeof e.message === 'string' &&
    e.message.startsWith('LEDGER_OUT_OF_SYNC') &&
    typeof e.detail === 'string' &&
    e.detail.length > 0
  ) {
    return e.detail;
  }
  return null;
}

async function isCommittedStateDrifted(walletId: string): Promise<boolean> {
  const res = await query<{ drifted: boolean }>(
    `SELECT balance_cents <> ledger_balance_cents AS drifted FROM wallets WHERE wallet_id = $1`,
    [walletId]
  );
  return res.rows.length > 0 && res.rows[0].drifted === true;
}

/**
 * Run a money-moving transaction with frozen-wallet handling:
 *
 *  - A WalletFrozenError raised by the write guard (under the row lock) is
 *    surfaced as-is (409).
 *  - Ledger-maintained accounts (fee / FX pool / clearing) are deliberately not
 *    row-locked up front (hot rows), so their drift surfaces at COMMIT as the
 *    deferred trigger's LEDGER_OUT_OF_SYNC. That is mapped to the same 409 only
 *    after confirming the COMMITTED state is really drifted; otherwise it was a
 *    genuine bug in this transaction and the original error is rethrown (500).
 *
 * In both cases a reconciliation event is published (deduplicated) before the
 * error propagates. The transaction itself has already rolled back.
 */
export async function withMoneyTransaction<T>(fn: (client: PoolClient) => Promise<T>): Promise<T> {
  try {
    return await withTransaction(fn);
  } catch (err) {
    let frozen: WalletFrozenError | null = err instanceof WalletFrozenError ? err : null;
    if (!frozen) {
      const walletId = ledgerSyncViolationWalletId(err);
      if (walletId && (await isCommittedStateDrifted(walletId))) {
        frozen = new WalletFrozenError(walletId);
      }
    }
    if (frozen) {
      await frozenWalletNotifier.notify(frozen.walletId);
      throw frozen;
    }
    throw err;
  }
}

/**
 * Publishes a WALLET_DRIFT event when a write hits a frozen wallet.
 *
 *  - Deduplicated per wallet with Redis `SET NX EX`, so a client retry storm
 *    against a frozen wallet yields one event per window, not thousands
 *    (bounded broker memory). The key is released if publishing fails.
 *  - Best-effort and time-boxed: the caller still gets its 409 even if the
 *    broker is down; the periodic audit is the safety net.
 */
export class FrozenWalletNotifier {
  async notify(walletId: string): Promise<boolean> {
    const key = `wallet:frozen-event:${walletId}`;
    const acquired = await redis.set(key, '1', 'EX', config.frozenWallet.eventDedupeSeconds, 'NX');
    if (acquired !== 'OK') {
      return false; // already reported within the dedupe window
    }

    try {
      const w = await readWalletState(walletId);
      if (!w) return false;

      const envelope = createEnvelope(
        LEDGER_EVENT_TYPES.WALLET_DRIFT,
        `guard_${crypto.randomUUID()}`,
        {
          walletId,
          currency: w.currency,
          accountType: w.accountType,
          balanceCents: w.balance.toString(),
          ledgerBalanceCents: w.ledgerBalance.toString(),
          ledgerVersion: w.ledgerVersion.toString(),
        },
        'WRITE_GUARD'
      );

      await getEventBus().publish(envelope, {
        signal: AbortSignal.timeout(config.frozenWallet.publishTimeoutMs),
      });
      return true;
    } catch {
      await redis.del(key); // let the next attempt try again
      return false;
    }
  }
}

export const frozenWalletNotifier = new FrozenWalletNotifier();
