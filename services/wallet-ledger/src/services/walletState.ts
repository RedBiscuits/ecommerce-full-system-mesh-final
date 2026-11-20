import { query } from '../db';
import { toBigInt } from '../money';
import type { AccountType } from '../types/wallet';

/** A wallet's effective state: row values, or bucket aggregates when sharded. */
export interface WalletState {
  walletId: string;
  ownerId: string;
  currency: string;
  accountType: AccountType;
  /** 0 = unsharded; > 0 = balance striped over this many buckets. */
  shardCount: number;
  balance: bigint;
  ledgerBalance: bigint;
  ledgerVersion: bigint;
  version: number;
  updatedAt: Date;
}

/**
 * Single source of truth for reading balances. For sharded accounts the live
 * balance is the SUM over the account's buckets: O(shard_count), a small
 * constant, never proportional to ledger history. Sharded accounts keep no
 * separate projection, so their balance and ledger total are the same value.
 */
export const WALLET_STATE_SQL = `
  SELECT w.wallet_id, w.owner_id, w.currency, w.account_type, w.shard_count, w.version, w.updated_at,
         (CASE WHEN w.shard_count > 0 THEN COALESCE(sh.total, 0) ELSE w.balance_cents END)::text AS balance,
         (CASE WHEN w.shard_count > 0 THEN COALESCE(sh.total, 0) ELSE w.ledger_balance_cents END)::text AS ledger_balance,
         (CASE WHEN w.shard_count > 0 THEN COALESCE(sh.entries, 0) ELSE w.ledger_version END)::text AS ledger_version
  FROM wallets w
  LEFT JOIN LATERAL (
    SELECT SUM(s.balance_cents) AS total, SUM(s.entry_count) AS entries
    FROM wallet_balance_shards s
    WHERE s.wallet_id = w.wallet_id
  ) sh ON w.shard_count > 0
  WHERE w.wallet_id = $1`;

interface WalletStateRow {
  wallet_id: string;
  owner_id: string;
  currency: string;
  account_type: AccountType;
  shard_count: number;
  version: number;
  updated_at: Date;
  balance: string;
  ledger_balance: string;
  ledger_version: string;
}

export async function readWalletState(walletId: string): Promise<WalletState | null> {
  const res = await query<WalletStateRow>(WALLET_STATE_SQL, [walletId]);
  if (res.rows.length === 0) return null;
  const r = res.rows[0];
  return {
    walletId: r.wallet_id,
    ownerId: r.owner_id,
    currency: r.currency,
    accountType: r.account_type,
    shardCount: Number(r.shard_count),
    balance: toBigInt(r.balance, 'balance'),
    ledgerBalance: toBigInt(r.ledger_balance, 'ledger_balance'),
    ledgerVersion: toBigInt(r.ledger_version, 'ledger_version'),
    version: Number(r.version),
    updatedAt: r.updated_at,
  };
}
