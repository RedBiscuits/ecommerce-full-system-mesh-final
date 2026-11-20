import type { PoolClient } from 'pg';
import { query, withTransaction } from '../db';
import { toBigInt } from '../money';
import {
  AmqpEventBus,
  EventEnvelope,
  LEDGER_EVENT_TYPES,
  NonRetryableError,
  Subscription,
} from '../messaging';

export type ReconciliationOutcome =
  /** Balance was corrected to the ledger sum. */
  | 'RECONCILED'
  /** Balance already matched the ledger (redelivery, or fixed meanwhile). */
  | 'ALREADY_IN_SYNC'
  /** A global violation reported by the audit no longer reproduces. */
  | 'NO_LONGER_VIOLATED';

export type SourceOfTruthVerdict =
  | { source: 'LEDGER'; ledgerSum: bigint; entryCount: bigint }
  | { source: 'UNDETERMINED'; reason: string };

/** FX pools and clearing accounts may legitimately hold negative positions. */
const NEGATIVE_ALLOWED_ACCOUNT_TYPES = new Set(['FX_POOL', 'CLEARING']);

/**
 * Consumes ledger invariant events and attempts automatic reconciliation.
 *
 * Policy: the append-only double-entry ledger rows are the system of record;
 * balance_cents and the precalculated ledger_balance_cents / ledger_version
 * are caches of it. The ledger is only trusted after it is verified for the
 * wallet in question:
 *   1. every journal touching the wallet balances to zero per currency, and
 *   2. the wallet's ledger sum is non-negative (unless FX_POOL / CLEARING).
 * If verification passes, all three cached columns are rebuilt from the
 * recomputed SUM / COUNT (under a row lock, version-guarded, logged in
 * reconciliation_log). If it fails,
 * neither side can be trusted automatically -> NonRetryableError -> DLQ for a
 * human. Global violations (zero-sum, unbalanced journal, negative balance)
 * cannot be repaired by adjusting one balance, so they are re-verified and
 * escalated to the DLQ if they still reproduce.
 *
 * Idempotent by construction: every handler re-reads current state under a
 * lock and is a no-op if the invariant already holds, so at-least-once
 * redelivery can never double-correct.
 */
export class LedgerReconciliationService {
  async handle(envelope: EventEnvelope): Promise<ReconciliationOutcome> {
    switch (envelope.type) {
      case LEDGER_EVENT_TYPES.WALLET_DRIFT:
        return this.reconcileWalletDrift(envelope as EventEnvelope<typeof LEDGER_EVENT_TYPES.WALLET_DRIFT>);
      case LEDGER_EVENT_TYPES.UNBALANCED_JOURNAL:
        return this.escalateUnbalancedJournal(
          envelope as EventEnvelope<typeof LEDGER_EVENT_TYPES.UNBALANCED_JOURNAL>
        );
      case LEDGER_EVENT_TYPES.GLOBAL_ZERO_SUM:
        return this.escalateGlobalZeroSum(envelope as EventEnvelope<typeof LEDGER_EVENT_TYPES.GLOBAL_ZERO_SUM>);
      case LEDGER_EVENT_TYPES.NEGATIVE_BALANCE:
        return this.escalateNegativeBalance(
          envelope as EventEnvelope<typeof LEDGER_EVENT_TYPES.NEGATIVE_BALANCE>
        );
      default:
        throw new NonRetryableError('UNKNOWN_EVENT_TYPE', `Unhandled event type ${String(envelope.type)}`);
    }
  }

  async reconcileWalletDrift(
    envelope: EventEnvelope<typeof LEDGER_EVENT_TYPES.WALLET_DRIFT>
  ): Promise<ReconciliationOutcome> {
    const { walletId } = envelope.payload;

    return withTransaction(async (client) => {
      // Row lock FIRST. While held, nobody can change this wallet's balance
      // (every writer locks it). For unsharded wallets no ledger entry can be
      // inserted either (the delta trigger must UPDATE this row). For sharded
      // accounts every bucket is locked too, in shard order, which blocks any
      // ledger insert for the account (each must UPDATE one bucket). Either way
      // the ledger we verify and the cache we correct cannot move underneath us.
      const walletRes = await client.query<{
        balance_cents: unknown;
        ledger_balance_cents: unknown;
        ledger_version: unknown;
        shard_count: number;
        version: number;
        currency: string;
        account_type: string;
      }>(
        `SELECT balance_cents, ledger_balance_cents, ledger_version, shard_count, version, currency, account_type
         FROM wallets WHERE wallet_id = $1 FOR UPDATE`,
        [walletId]
      );
      if (walletRes.rows.length === 0) {
        throw new NonRetryableError('WALLET_NOT_FOUND', `Wallet ${walletId} does not exist`);
      }
      const wallet = walletRes.rows[0];
      const sharded = Number(wallet.shard_count) > 0;

      let balance: bigint;
      let ledgerBalance: bigint;
      let ledgerVersion: bigint;
      let bucketCount = 0;
      if (sharded) {
        const buckets = await client.query<{ balance_cents: unknown; entry_count: unknown }>(
          `SELECT balance_cents, entry_count FROM wallet_balance_shards
           WHERE wallet_id = $1 ORDER BY shard_no FOR UPDATE`,
          [walletId]
        );
        bucketCount = buckets.rows.length;
        balance = buckets.rows.reduce((sum, r) => sum + toBigInt(r.balance_cents, 'bucket balance'), 0n);
        ledgerBalance = balance;
        ledgerVersion = buckets.rows.reduce((sum, r) => sum + toBigInt(r.entry_count, 'bucket entries'), 0n);
      } else {
        balance = toBigInt(wallet.balance_cents, 'balance_cents');
        ledgerBalance = toBigInt(wallet.ledger_balance_cents, 'ledger_balance_cents');
        ledgerVersion = toBigInt(wallet.ledger_version, 'ledger_version');
      }

      const verdict = await this.determineSourceOfTruth(client, walletId, wallet.account_type);
      if (verdict.source === 'UNDETERMINED') {
        throw new NonRetryableError('SOURCE_OF_TRUTH_UNDETERMINED', verdict.reason);
      }

      const layoutOk = !sharded || bucketCount === Number(wallet.shard_count);
      if (
        layoutOk &&
        balance === verdict.ledgerSum &&
        ledgerBalance === verdict.ledgerSum &&
        ledgerVersion === verdict.entryCount
      ) {
        return 'ALREADY_IN_SYNC';
      }

      // Ledger verified as the source of truth: rebuild the cache from it and
      // bump the version so optimistic lockers notice.
      if (sharded) {
        await client.query(`DELETE FROM wallet_balance_shards WHERE wallet_id = $1`, [walletId]);
        await client.query(
          `INSERT INTO wallet_balance_shards (wallet_id, shard_no, balance_cents, entry_count)
           SELECT $1, g, CASE WHEN g = 0 THEN $2::bigint ELSE 0 END, CASE WHEN g = 0 THEN $3::bigint ELSE 0 END
           FROM generate_series(0, $4::int - 1) AS g`,
          [walletId, verdict.ledgerSum.toString(), verdict.entryCount.toString(), Number(wallet.shard_count)]
        );
      }
      const updated = await client.query(
        `UPDATE wallets
         SET balance_cents = CASE WHEN shard_count > 0 THEN 0 ELSE $1::bigint END,
             ledger_balance_cents = CASE WHEN shard_count > 0 THEN 0 ELSE $1::bigint END,
             ledger_version = CASE WHEN shard_count > 0 THEN 0 ELSE $2::bigint END,
             version = version + 1,
             updated_at = NOW()
         WHERE wallet_id = $3 AND version = $4
         RETURNING version`,
        [verdict.ledgerSum.toString(), verdict.entryCount.toString(), walletId, wallet.version]
      );
      if (updated.rows.length === 0) {
        throw new Error(`Concurrent modification while reconciling ${walletId}`); // retryable
      }

      await client.query(
        `INSERT INTO reconciliation_log
           (event_id, wallet_id, currency, previous_balance_cents, previous_ledger_balance_cents,
            previous_ledger_version, corrected_balance_cents, corrected_ledger_version, source_of_truth)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'LEDGER')`,
        [
          envelope.eventId,
          walletId,
          wallet.currency,
          balance.toString(),
          ledgerBalance.toString(),
          ledgerVersion.toString(),
          verdict.ledgerSum.toString(),
          verdict.entryCount.toString(),
        ]
      );

      return 'RECONCILED';
    });
  }

  /**
   * Decide whether this wallet's ledger can be trusted. All work is done in
   * PostgreSQL with LIMIT 1 probes; nothing proportional to history is loaded.
   */
  async determineSourceOfTruth(
    client: PoolClient,
    walletId: string,
    accountType: string
  ): Promise<SourceOfTruthVerdict> {
    const unbalanced = await client.query<{ transaction_ref: string; currency: string; total: string }>(
      `SELECT j.transaction_ref, j.currency, SUM(j.signed_amount_cents)::text AS total
       FROM ledger_entries j
       WHERE j.transaction_ref IN (
         SELECT DISTINCT transaction_ref FROM ledger_entries WHERE wallet_id = $1
       )
       GROUP BY j.transaction_ref, j.currency
       HAVING SUM(j.signed_amount_cents) <> 0
       LIMIT 1`,
      [walletId]
    );
    if (unbalanced.rows.length > 0) {
      const j = unbalanced.rows[0];
      return {
        source: 'UNDETERMINED',
        reason: `Ledger untrusted: journal ${j.transaction_ref} touching ${walletId} is unbalanced in ${j.currency} by ${j.total}`,
      };
    }

    const sumRes = await client.query<{ total: string; cnt: string }>(
      `SELECT COALESCE(SUM(signed_amount_cents), 0)::text AS total, COUNT(*)::text AS cnt
       FROM ledger_entries WHERE wallet_id = $1`,
      [walletId]
    );
    const ledgerSum = toBigInt(sumRes.rows[0].total, 'ledger sum');
    const entryCount = toBigInt(sumRes.rows[0].cnt, 'entry count');
    if (ledgerSum < 0n && !NEGATIVE_ALLOWED_ACCOUNT_TYPES.has(accountType)) {
      return {
        source: 'UNDETERMINED',
        reason: `Ledger untrusted: ${walletId} (${accountType}) ledger sum is negative (${ledgerSum})`,
      };
    }

    return { source: 'LEDGER', ledgerSum, entryCount };
  }

  private async escalateUnbalancedJournal(
    envelope: EventEnvelope<typeof LEDGER_EVENT_TYPES.UNBALANCED_JOURNAL>
  ): Promise<ReconciliationOutcome> {
    const { transactionRef, currency } = envelope.payload;
    const res = await query<{ total: string }>(
      `SELECT COALESCE(SUM(signed_amount_cents), 0)::text AS total
       FROM ledger_entries WHERE transaction_ref = $1 AND currency = $2`,
      [transactionRef, currency]
    );
    if (toBigInt(res.rows[0].total) === 0n) return 'NO_LONGER_VIOLATED';
    throw new NonRetryableError(
      'MANUAL_REVIEW_UNBALANCED_JOURNAL',
      `Journal ${transactionRef} is unbalanced in ${currency} by ${res.rows[0].total}; a balancing correction journal must be posted manually`
    );
  }

  private async escalateGlobalZeroSum(
    envelope: EventEnvelope<typeof LEDGER_EVENT_TYPES.GLOBAL_ZERO_SUM>
  ): Promise<ReconciliationOutcome> {
    const { currency } = envelope.payload;
    const res = await query<{ total: string }>(
      `SELECT COALESCE(SUM(signed_amount_cents), 0)::text AS total FROM ledger_entries WHERE currency = $1`,
      [currency]
    );
    if (toBigInt(res.rows[0].total) === 0n) return 'NO_LONGER_VIOLATED';
    throw new NonRetryableError(
      'MANUAL_REVIEW_GLOBAL_ZERO_SUM',
      `${currency} ledger does not sum to zero (off by ${res.rows[0].total})`
    );
  }

  private async escalateNegativeBalance(
    envelope: EventEnvelope<typeof LEDGER_EVENT_TYPES.NEGATIVE_BALANCE>
  ): Promise<ReconciliationOutcome> {
    const { walletId } = envelope.payload;
    const res = await query<{ total: string }>(
      `SELECT COALESCE(SUM(signed_amount_cents), 0)::text AS total FROM ledger_entries WHERE wallet_id = $1`,
      [walletId]
    );
    if (toBigInt(res.rows[0].total) >= 0n) return 'NO_LONGER_VIOLATED';
    throw new NonRetryableError(
      'MANUAL_REVIEW_NEGATIVE_BALANCE',
      `${walletId} ledger sum is negative (${res.rows[0].total})`
    );
  }
}

export const ledgerReconciliationService = new LedgerReconciliationService();

/** Attach the reconciliation handler to the bus's work queue. */
export async function startReconciliationWorker(
  bus: AmqpEventBus,
  service: LedgerReconciliationService = ledgerReconciliationService
): Promise<Subscription> {
  return bus.subscribe(async (envelope) => {
    await service.handle(envelope);
  });
}
