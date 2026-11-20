import crypto from 'crypto';
import type { PoolClient } from 'pg';
import { JournalLine, assertJournalBalanced } from '../money';

/**
 * Post a journal (a set of ledger lines sharing one transaction_ref) inside the
 * caller's transaction. The journal is validated in application code first
 * (balanced per currency, no zero lines, BIGINT range); the deferred DB
 * triggers then re-verify at COMMIT as a second, independent line of defense.
 *
 * Amounts are bound as decimal strings so no bigint ever passes through a JS
 * number on the way to PostgreSQL.
 *
 * Lock ordering: each inserted row's delta trigger UPDATEs its wallet row, which
 * row-locks ledger-maintained accounts (fee treasury / FX pool / clearing) that
 * the caller did not lock up front. PostgreSQL fires row triggers in VALUES
 * order, so lines are sorted by wallet_id: every transaction then acquires
 * those locks in one global order, after all of its user/escrow locks. That
 * lock hierarchy (saga/payout rows -> user+escrow sorted -> system sorted) is
 * what keeps opposite-direction FX transfers (USD->EUR vs EUR->USD, which
 * touch both FX pools) free of AB-BA deadlocks.
 */
export async function postJournal(
  client: PoolClient,
  transactionRef: string,
  lines: readonly JournalLine[]
): Promise<void> {
  assertJournalBalanced(lines);

  const ordered = [...lines].sort((a, b) => (a.walletId < b.walletId ? -1 : a.walletId > b.walletId ? 1 : 0));

  const values: string[] = [];
  const params: unknown[] = [];
  for (const line of ordered) {
    const base = params.length;
    values.push(`($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5}, $${base + 6})`);
    params.push(
      `le_${crypto.randomUUID()}`,
      transactionRef,
      line.walletId,
      line.currency,
      line.signedAmountMinor < 0n ? 'DEBIT' : 'CREDIT',
      line.signedAmountMinor.toString()
    );
  }

  await client.query(
    `INSERT INTO ledger_entries (entry_id, transaction_ref, wallet_id, currency, direction, signed_amount_cents)
     VALUES ${values.join(', ')}`,
    params
  );
}
