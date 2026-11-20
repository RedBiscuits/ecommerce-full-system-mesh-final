import crypto from 'crypto';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import type { FastifyInstance } from 'fastify';
import type { PoolClient } from 'pg';
import { buildApp } from '../src/server';
import { actAs, bearer } from './helpers/auth';
import { closeDb, pool, query, resetAndSeedDatabase, withTransaction } from '../src/db';
import { closeRedis, flushRedis } from '../src/redis';
import { walletService } from '../src/services/WalletService';
import { config } from '../src/config/env';
import { assertSupportedCurrency, systemAccountsFor } from '../src/money';

const SEED_BALANCE = 10000;
const THRESHOLD = config.amlReviewThresholdCents; // 50000 = $500.00

type RawQuery = (sql: string, params?: unknown[]) => Promise<unknown>;

interface RecordedStatement {
  sql: string;
  params?: unknown[];
}

interface QueryHooks {
  before?: (stmt: RecordedStatement, raw: RawQuery) => Promise<void> | void;
  after?: (stmt: RecordedStatement, raw: RawQuery) => Promise<void> | void;
}

/** Wrap pooled clients to observe statements / inject faults into real transactions. */
function instrumentClients(hooks: QueryHooks = {}): { statements: RecordedStatement[] } {
  const statements: RecordedStatement[] = [];
  const originalConnect = pool.connect.bind(pool);

  vi.spyOn(pool, 'connect').mockImplementation(async () => {
    const client = await originalConnect();
    const originalQuery = client.query.bind(client) as unknown as RawQuery;

    (client as unknown as { query: RawQuery }).query = async (sql, params) => {
      const stmt = { sql: String(sql).replace(/\s+/g, ' ').trim(), params };
      statements.push(stmt);
      await hooks.before?.(stmt, originalQuery);
      const result = await originalQuery(sql, params);
      await hooks.after?.(stmt, originalQuery);
      return result;
    };

    return client as PoolClient;
  });

  return { statements };
}

/**
 * Balanced test top-up: credits the wallet against the clearing account of its
 * own currency, so every per-currency ledger stays zero-sum.
 */
async function grantFunds(walletId: string, amountCents: number): Promise<void> {
  await withTransaction(async (client) => {
    const res = await client.query<{ currency: string }>(
      `SELECT currency FROM wallets WHERE wallet_id = $1`,
      [walletId]
    );
    const currency = assertSupportedCurrency(res.rows[0].currency);
    await client.query(
      `UPDATE wallets SET balance_cents = balance_cents + $1, version = version + 1, updated_at = NOW()
       WHERE wallet_id = $2`,
      [amountCents, walletId]
    );
    await client.query(
      `INSERT INTO ledger_entries (entry_id, transaction_ref, wallet_id, currency, direction, signed_amount_cents)
       VALUES ($1, $2, $3, $4, 'DEBIT', $5), ($6, $2, $7, $4, 'CREDIT', $8)`,
      [
        `le_${crypto.randomUUID()}`,
        `test_grant_${crypto.randomUUID()}`,
        systemAccountsFor(currency).clearing,
        currency,
        -amountCents,
        `le_${crypto.randomUUID()}`,
        walletId,
        amountCents,
      ]
    );
  });
}

async function balanceOf(walletId: string): Promise<number> {
  const wallet = await walletService.getWallet(walletId);
  return wallet?.balanceCents ?? NaN;
}

async function ledgerTotal(): Promise<number> {
  const res = await query<{ total: string }>(
    `SELECT COALESCE(SUM(signed_amount_cents), 0)::text AS total FROM ledger_entries`
  );
  return Number(res.rows[0].total);
}

/**
 * The global rule, for EVERY wallet (system accounts included): the effective
 * live balance, running total and entry counter (wallet row, or bucket SUMs
 * for sharded accounts) all match a full recount of the ledger.
 */
async function expectAllWalletsLedgerSynced(): Promise<void> {
  const res = await query<{ wallet_id: string; balance: string; ledger_balance: string; ledger_version: string; sum: string; cnt: string }>(
    `SELECT w.wallet_id,
            (CASE WHEN w.shard_count > 0 THEN sh.total ELSE w.balance_cents END)::text AS balance,
            (CASE WHEN w.shard_count > 0 THEN sh.total ELSE w.ledger_balance_cents END)::text AS ledger_balance,
            (CASE WHEN w.shard_count > 0 THEN sh.entries ELSE w.ledger_version END)::text AS ledger_version,
            l.sum::text AS sum, l.cnt::text AS cnt
     FROM wallets w
     CROSS JOIN LATERAL (
       SELECT COALESCE(SUM(balance_cents), 0) AS total, COALESCE(SUM(entry_count), 0) AS entries
       FROM wallet_balance_shards s WHERE s.wallet_id = w.wallet_id
     ) sh
     CROSS JOIN LATERAL (
       SELECT COALESCE(SUM(signed_amount_cents), 0) AS sum, COUNT(*) AS cnt
       FROM ledger_entries le WHERE le.wallet_id = w.wallet_id
     ) l`
  );
  for (const row of res.rows) {
    expect({ wallet: row.wallet_id, balance: row.balance, ledgerBalance: row.ledger_balance, ledgerVersion: row.ledger_version }).toEqual({
      wallet: row.wallet_id,
      balance: row.sum,
      ledgerBalance: row.sum,
      ledgerVersion: row.cnt,
    });
  }
}

async function sagaStepsOf(transferId: string): Promise<string[]> {
  const res = await query<{ step: string }>(
    `SELECT step FROM transfer_saga_steps WHERE transfer_id = $1 ORDER BY seq ASC`,
    [transferId]
  );
  return res.rows.map((r) => r.step);
}

describe('Transfer saga (P2P transfers, AML escrow, compensation)', () => {
  let app: FastifyInstance;
  let api: ReturnType<typeof actAs>;

  const transfer = (payload: Record<string, unknown>) =>
    api.inject({ method: 'POST', url: '/wallets/transfer', payload });
  const review = (transferId: string, payload: Record<string, unknown>) =>
    api.inject({ method: 'POST', url: `/transfers/${transferId}/review`, payload });

  beforeAll(async () => {
    app = buildApp();
    await app.ready();
    api = actAs(app);
  });

  beforeEach(async () => {
    await resetAndSeedDatabase();
    await flushRedis();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  afterAll(async () => {
    await app.close();
    await closeDb();
    await closeRedis();
  });

  describe('instant transfers (amount <= AML threshold)', () => {
    it('completes atomically: balances, versions, balanced ledger pair and saga steps', async () => {
      const res = await transfer({
        fromWalletId: 'wallet_alice',
        toWalletId: 'wallet_bob',
        amountCents: 2500,
        referenceId: 'ref_happy',
      });

      expect(res.statusCode).toBe(201);
      const body = res.json();
      expect(body).toMatchObject({
        fromWalletId: 'wallet_alice',
        toWalletId: 'wallet_bob',
        amountCents: 2500,
        status: 'COMPLETED',
        amlReviewRequired: false,
        senderRemainingBalanceCents: 7500,
        senderVersion: 2,
      });

      const alice = await walletService.getWallet('wallet_alice');
      const bob = await walletService.getWallet('wallet_bob');
      expect(alice).toMatchObject({ balanceCents: 7500, version: 2 });
      expect(bob).toMatchObject({ balanceCents: 12500, version: 2 });

      const ledger = await query<{ wallet_id: string; amount: string }>(
        `SELECT wallet_id, signed_amount_cents::text AS amount
         FROM ledger_entries WHERE transaction_ref = $1 ORDER BY signed_amount_cents`,
        [body.transferId]
      );
      expect(ledger.rows).toEqual([
        { wallet_id: 'wallet_alice', amount: '-2500' },
        { wallet_id: 'wallet_bob', amount: '2500' },
      ]);

      expect(await sagaStepsOf(body.transferId)).toEqual([
        'SENDER_DEBITED',
        'RECIPIENT_CREDITED',
        'TRANSFER_COMPLETED',
      ]);
      expect(await ledgerTotal()).toBe(0);
      await expectAllWalletsLedgerSynced();
    });

    it('treats an amount exactly at the AML threshold as instant (only "exceeding" is held)', async () => {
      await grantFunds('wallet_alice', THRESHOLD);

      const res = await transfer({
        fromWalletId: 'wallet_alice',
        toWalletId: 'wallet_bob',
        amountCents: THRESHOLD,
      });

      expect(res.statusCode).toBe(201);
      expect(res.json().status).toBe('COMPLETED');
      expect(await balanceOf('wallet_bob')).toBe(SEED_BALANCE + THRESHOLD);
      expect(await balanceOf('wallet_escrow')).toBe(0);
    });

    it('supports optimistic expectedVersion on the sender wallet', async () => {
      const ok = await transfer({
        fromWalletId: 'wallet_alice',
        toWalletId: 'wallet_bob',
        amountCents: 100,
        expectedVersion: 1,
      });
      expect(ok.statusCode).toBe(201);

      const stale = await transfer({
        fromWalletId: 'wallet_alice',
        toWalletId: 'wallet_bob',
        amountCents: 100,
        expectedVersion: 1, // alice is now at version 2
      });
      expect(stale.statusCode).toBe(409);
      expect(stale.json().code).toBe('WALLET_VERSION_CONFLICT');
      expect(await balanceOf('wallet_alice')).toBe(SEED_BALANCE - 100);
    });

    it.each([
      ['self transfer', { toWalletId: 'wallet_alice' }],
      ['zero amount', { amountCents: 0 }],
      ['negative amount', { amountCents: -100 }],
      ['fractional amount', { amountCents: 10.5 }],
      ['missing recipient field', { toWalletId: '' }],
    ])('returns 400 for %s and moves no money', async (_label, override) => {
      const res = await transfer({
        fromWalletId: 'wallet_alice',
        toWalletId: 'wallet_bob',
        amountCents: 100,
        ...override,
      });
      expect(res.statusCode).toBe(400);
      expect(await balanceOf('wallet_alice')).toBe(SEED_BALANCE);
      expect(await balanceOf('wallet_bob')).toBe(SEED_BALANCE);
    });

    it('rejects insufficient funds with 409 and leaves no trace', async () => {
      const res = await transfer({
        fromWalletId: 'wallet_alice',
        toWalletId: 'wallet_bob',
        amountCents: SEED_BALANCE + 1,
      });

      expect(res.statusCode).toBe(409);
      expect(res.json().code).toBe('INSUFFICIENT_FUNDS');
      expect(await balanceOf('wallet_alice')).toBe(SEED_BALANCE);
      expect((await query(`SELECT 1 FROM transfers`)).rows).toHaveLength(0);
    });
  });

  describe('recipient validation happens before any write (negative-ledger bug)', () => {
    it('returns 404 for an unknown recipient with no debit, no ledger rows, no transfer row', async () => {
      const res = await transfer({
        fromWalletId: 'wallet_alice',
        toWalletId: 'wallet_non_existent',
        amountCents: 3000,
      });

      expect(res.statusCode).toBe(404);
      expect(res.json().code).toBe('WALLET_NOT_FOUND');

      const alice = await walletService.getWallet('wallet_alice');
      expect(alice).toMatchObject({ balanceCents: SEED_BALANCE, version: 1 });
      expect((await query(`SELECT 1 FROM ledger_entries WHERE transaction_ref <> 'seed_init'`)).rows).toHaveLength(0);
      expect((await query(`SELECT 1 FROM transfers`)).rows).toHaveLength(0);
      expect(await ledgerTotal()).toBe(0); // previously went negative here
    });

    it('returns 404 for an unknown sender without touching the recipient', async () => {
      const res = await transfer({
        fromWalletId: 'wallet_ghost',
        toWalletId: 'wallet_bob',
        amountCents: 3000,
      });
      expect(res.statusCode).toBe(404);
      expect(await balanceOf('wallet_bob')).toBe(SEED_BALANCE);
    });

    it('never commits a debit when the transfer fails: rollback only, no compensating ledger noise', async () => {
      const { statements } = instrumentClients();

      await transfer({
        fromWalletId: 'wallet_alice',
        toWalletId: 'wallet_non_existent',
        amountCents: 3000,
      });

      const sqls = statements.map((s) => s.sql);
      expect(sqls.some((s) => s.startsWith('UPDATE wallets'))).toBe(false);
      expect(sqls.some((s) => s.startsWith('INSERT INTO'))).toBe(false);
      expect(sqls.at(-1)).toBe('ROLLBACK');
    });
  });

  describe('AB-BA deadlock prevention (ordered locking)', () => {
    const walletLockOrder = (statements: RecordedStatement[]) =>
      statements
        .filter((s) => s.sql.includes('FROM wallets') && s.sql.includes('FOR UPDATE'))
        .map((s) => s.params?.[0]);

    it('locks wallets in lexicographic order for Alice -> Bob', async () => {
      const { statements } = instrumentClients();
      await transfer({ fromWalletId: 'wallet_alice', toWalletId: 'wallet_bob', amountCents: 100 });
      expect(walletLockOrder(statements)).toEqual(['wallet_alice', 'wallet_bob']);
    });

    it('locks wallets in the SAME order for the opposite direction Bob -> Alice', async () => {
      const { statements } = instrumentClients();
      await transfer({ fromWalletId: 'wallet_bob', toWalletId: 'wallet_alice', amountCents: 100 });
      // Sender is bob, but alice's row is still locked first: no AB-BA cycle possible.
      expect(walletLockOrder(statements)).toEqual(['wallet_alice', 'wallet_bob']);
    });

    it('keeps the global order when the escrow wallet joins the lock set', async () => {
      await grantFunds('wallet_bob', THRESHOLD * 2);
      const { statements } = instrumentClients();
      await transfer({
        fromWalletId: 'wallet_bob',
        toWalletId: 'wallet_alice',
        amountCents: THRESHOLD + 1,
      });
      expect(walletLockOrder(statements)).toEqual(['wallet_alice', 'wallet_bob', 'wallet_escrow']);
    });

    it('survives a bidirectional transfer storm with full money conservation', async () => {
      const requests = [
        ...Array.from({ length: 15 }, (_, i) =>
          transfer({
            fromWalletId: 'wallet_alice',
            toWalletId: 'wallet_bob',
            amountCents: 400,
            referenceId: `storm_a_${i}`,
          })
        ),
        ...Array.from({ length: 15 }, (_, i) =>
          transfer({
            fromWalletId: 'wallet_bob',
            toWalletId: 'wallet_alice',
            amountCents: 250,
            referenceId: `storm_b_${i}`,
          })
        ),
      ];

      const results = await Promise.all(requests);
      expect(results.every((r) => r.statusCode === 201)).toBe(true);

      const alice = await walletService.getWallet('wallet_alice');
      const bob = await walletService.getWallet('wallet_bob');
      // Alice: -15*400 +15*250 = -2250; Bob mirrors it.
      expect(alice?.balanceCents).toBe(SEED_BALANCE - 2250);
      expect(bob?.balanceCents).toBe(SEED_BALANCE + 2250);
      expect((alice?.balanceCents ?? 0) + (bob?.balanceCents ?? 0)).toBe(2 * SEED_BALANCE);
      // 15 sends + 15 receives each => 30 version bumps per wallet.
      expect(alice?.version).toBe(31);
      expect(bob?.version).toBe(31);
      expect(await ledgerTotal()).toBe(0);
      await expectAllWalletsLedgerSynced();
    });
  });

  describe('AML escrow hold (amount > threshold)', () => {
    const HELD = THRESHOLD + 2500;

    async function createHeldTransfer(): Promise<string> {
      await grantFunds('wallet_alice', THRESHOLD * 2);
      const res = await transfer({
        fromWalletId: 'wallet_alice',
        toWalletId: 'wallet_bob',
        amountCents: HELD,
      });
      expect(res.statusCode).toBe(202);
      return res.json().transferId;
    }

    it('parks the funds in escrow: sender debited, recipient NOT credited, ledger reflects the hold', async () => {
      await grantFunds('wallet_alice', THRESHOLD * 2);
      const aliceBefore = await balanceOf('wallet_alice');

      const res = await transfer({
        fromWalletId: 'wallet_alice',
        toWalletId: 'wallet_bob',
        amountCents: HELD,
      });

      expect(res.statusCode).toBe(202);
      const body = res.json();
      expect(body).toMatchObject({ status: 'PENDING_REVIEW', amlReviewRequired: true });

      expect(await balanceOf('wallet_alice')).toBe(aliceBefore - HELD);
      expect(await balanceOf('wallet_bob')).toBe(SEED_BALANCE); // not credited yet
      expect(await balanceOf('wallet_escrow')).toBe(HELD);

      const holdLedger = await query<{ wallet_id: string; amount: string }>(
        `SELECT wallet_id, signed_amount_cents::text AS amount
         FROM ledger_entries WHERE transaction_ref = $1 ORDER BY signed_amount_cents`,
        [body.transferId]
      );
      expect(holdLedger.rows).toEqual([
        { wallet_id: 'wallet_alice', amount: `-${HELD}` },
        { wallet_id: 'wallet_escrow', amount: `${HELD}` },
      ]);

      expect(await sagaStepsOf(body.transferId)).toEqual([
        'SENDER_DEBITED',
        'ESCROW_HELD',
        'AML_REVIEW_PENDING',
      ]);

      const statusRes = await api.inject({ method: 'GET', url: `/transfers/${body.transferId}` });
      expect(statusRes.statusCode).toBe(200);
      expect(statusRes.json().status).toBe('PENDING_REVIEW');

      expect(await ledgerTotal()).toBe(0);
      await expectAllWalletsLedgerSynced();
    });

    it('held funds are not spendable by the sender', async () => {
      await createHeldTransfer();
      const remaining = await balanceOf('wallet_alice');

      const res = await transfer({
        fromWalletId: 'wallet_alice',
        toWalletId: 'wallet_bob',
        amountCents: remaining + 1,
      });
      expect(res.statusCode).toBe(409);
      expect(res.json().code).toBe('INSUFFICIENT_FUNDS');
    });

    it('APPROVE releases escrow to the recipient and completes the saga', async () => {
      const transferId = await createHeldTransfer();

      // The reviewer recorded in the audit trail is the VERIFIED token subject;
      // a body-supplied name is ignored (it could be forged).
      const res = await app.inject({
        method: 'POST',
        url: `/transfers/${transferId}/review`,
        headers: await bearer('aml_officer_1', ['admin']),
        payload: { decision: 'APPROVE', reviewedBy: 'someone_else_entirely' },
      });

      expect(res.statusCode).toBe(200);
      expect(res.json()).toMatchObject({
        transferId,
        decision: 'APPROVE',
        status: 'COMPLETED',
        outcome: 'RELEASED_TO_RECIPIENT',
        amountCents: HELD,
      });

      expect(await balanceOf('wallet_bob')).toBe(SEED_BALANCE + HELD);
      expect(await balanceOf('wallet_escrow')).toBe(0);

      const transferRow = await query<{ status: string; reviewed_by: string }>(
        `SELECT status, reviewed_by FROM transfers WHERE transfer_id = $1`,
        [transferId]
      );
      expect(transferRow.rows[0]).toEqual({ status: 'COMPLETED', reviewed_by: 'aml_officer_1' });

      expect(await sagaStepsOf(transferId)).toEqual([
        'SENDER_DEBITED',
        'ESCROW_HELD',
        'AML_REVIEW_PENDING',
        'ESCROW_RELEASED',
        'RECIPIENT_CREDITED',
        'TRANSFER_COMPLETED',
      ]);

      const releaseLedger = await query<{ wallet_id: string; amount: string }>(
        `SELECT wallet_id, signed_amount_cents::text AS amount
         FROM ledger_entries WHERE transaction_ref = $1 ORDER BY signed_amount_cents`,
        [`release_${transferId}`]
      );
      expect(releaseLedger.rows).toEqual([
        { wallet_id: 'wallet_escrow', amount: `-${HELD}` },
        { wallet_id: 'wallet_bob', amount: `${HELD}` },
      ]);

      expect(await ledgerTotal()).toBe(0);
      await expectAllWalletsLedgerSynced();
    });

    it('REJECT runs the compensation transaction: full refund to the sender, transfer REVERSED', async () => {
      const transferId = await createHeldTransfer();
      const aliceAfterHold = await balanceOf('wallet_alice');

      const res = await review(transferId, { decision: 'REJECT', reviewedBy: 'aml_officer_2' });

      expect(res.statusCode).toBe(200);
      expect(res.json()).toMatchObject({
        status: 'REVERSED',
        outcome: 'COMPENSATED_REFUNDED_TO_SENDER',
      });

      expect(await balanceOf('wallet_alice')).toBe(aliceAfterHold + HELD);
      expect(await balanceOf('wallet_bob')).toBe(SEED_BALANCE); // recipient never credited
      expect(await balanceOf('wallet_escrow')).toBe(0);

      expect(await sagaStepsOf(transferId)).toEqual([
        'SENDER_DEBITED',
        'ESCROW_HELD',
        'AML_REVIEW_PENDING',
        'ESCROW_RELEASED',
        'COMPENSATION_SENDER_REFUNDED',
        'TRANSFER_REVERSED',
      ]);

      const refundLedger = await query<{ wallet_id: string; amount: string }>(
        `SELECT wallet_id, signed_amount_cents::text AS amount
         FROM ledger_entries WHERE transaction_ref = $1 ORDER BY signed_amount_cents`,
        [`refund_${transferId}`]
      );
      expect(refundLedger.rows).toEqual([
        { wallet_id: 'wallet_escrow', amount: `-${HELD}` },
        { wallet_id: 'wallet_alice', amount: `${HELD}` },
      ]);

      expect(await ledgerTotal()).toBe(0);
      await expectAllWalletsLedgerSynced();
    });

    it('a review decision is final: second review gets 409 and money moves only once', async () => {
      const transferId = await createHeldTransfer();
      await review(transferId, { decision: 'APPROVE' });

      for (const decision of ['APPROVE', 'REJECT']) {
        const res = await review(transferId, { decision });
        expect(res.statusCode).toBe(409);
        expect(res.json().code).toBe('TRANSFER_STATE_CONFLICT');
      }

      expect(await balanceOf('wallet_bob')).toBe(SEED_BALANCE + HELD);
      expect(await balanceOf('wallet_escrow')).toBe(0);
      await expectAllWalletsLedgerSynced();
    });

    it('a double review cannot raid funds that other transfers hold in escrow', async () => {
      // Two independent holds share the escrow wallet. Re-reviewing the first
      // transfer must not be able to spend the second transfer's held funds.
      await grantFunds('wallet_alice', THRESHOLD * 4);
      const first = await transfer({
        fromWalletId: 'wallet_alice',
        toWalletId: 'wallet_bob',
        amountCents: HELD,
      });
      const second = await transfer({
        fromWalletId: 'wallet_alice',
        toWalletId: 'wallet_bob',
        amountCents: HELD,
      });
      expect(first.statusCode).toBe(202);
      expect(second.statusCode).toBe(202);
      expect(await balanceOf('wallet_escrow')).toBe(2 * HELD);

      const firstReview = await review(first.json().transferId, { decision: 'APPROVE' });
      expect(firstReview.statusCode).toBe(200);

      const doubleReview = await review(first.json().transferId, { decision: 'APPROVE' });
      expect(doubleReview.statusCode).toBe(409);
      expect(doubleReview.json().code).toBe('TRANSFER_STATE_CONFLICT');

      // Bob was credited exactly once; the second transfer's funds are intact.
      expect(await balanceOf('wallet_bob')).toBe(SEED_BALANCE + HELD);
      expect(await balanceOf('wallet_escrow')).toBe(HELD);
      await expectAllWalletsLedgerSynced();
    });

    it('concurrent APPROVE and REJECT: exactly one wins, escrow drains exactly once', async () => {
      const transferId = await createHeldTransfer();

      const [a, b] = await Promise.all([
        review(transferId, { decision: 'APPROVE' }),
        review(transferId, { decision: 'REJECT' }),
      ]);

      const codes = [a.statusCode, b.statusCode].sort();
      expect(codes).toEqual([200, 409]);
      expect(await balanceOf('wallet_escrow')).toBe(0);

      // Whoever won, total money is conserved and the ledger matches every balance.
      const total =
        (await balanceOf('wallet_alice')) +
        (await balanceOf('wallet_bob')) +
        (await balanceOf('wallet_escrow'));
      expect(total).toBe(2 * SEED_BALANCE + THRESHOLD * 2);
      expect(await ledgerTotal()).toBe(0);
      await expectAllWalletsLedgerSynced();
    });

    it('cannot review an instant transfer, an unknown transfer, or send a bogus decision', async () => {
      const instant = await transfer({
        fromWalletId: 'wallet_alice',
        toWalletId: 'wallet_bob',
        amountCents: 100,
      });

      const notPending = await review(instant.json().transferId, { decision: 'APPROVE' });
      expect(notPending.statusCode).toBe(409);
      expect(notPending.json().code).toBe('TRANSFER_STATE_CONFLICT');

      const unknown = await review('tr_missing', { decision: 'REJECT' });
      expect(unknown.statusCode).toBe(404);
      expect(unknown.json().code).toBe('TRANSFER_NOT_FOUND');

      const bogus = await review(instant.json().transferId, { decision: 'MAYBE' });
      expect(bogus.statusCode).toBe(400);
    });

    it('a failed review transaction rolls back completely and stays retryable (saga retry)', async () => {
      const transferId = await createHeldTransfer();
      const aliceAfterHold = await balanceOf('wallet_alice');

      instrumentClients({
        before: (stmt) => {
          if (
            stmt.sql.startsWith('INSERT INTO ledger_entries') &&
            String(stmt.params?.[1]).startsWith('release_')
          ) {
            throw new Error('simulated crash during escrow release');
          }
        },
      });

      const failed = await review(transferId, { decision: 'APPROVE' });
      expect(failed.statusCode).toBe(500);

      vi.restoreAllMocks();

      // Nothing moved; the saga instance is still PENDING_REVIEW.
      expect(await balanceOf('wallet_escrow')).toBe(HELD);
      expect(await balanceOf('wallet_bob')).toBe(SEED_BALANCE);
      expect(await balanceOf('wallet_alice')).toBe(aliceAfterHold);
      const row = await query<{ status: string }>(
        `SELECT status FROM transfers WHERE transfer_id = $1`,
        [transferId]
      );
      expect(row.rows[0].status).toBe('PENDING_REVIEW');
      await expectAllWalletsLedgerSynced();

      // Retry succeeds.
      const retry = await review(transferId, { decision: 'APPROVE' });
      expect(retry.statusCode).toBe(200);
      expect(await balanceOf('wallet_bob')).toBe(SEED_BALANCE + HELD);
      expect(await balanceOf('wallet_escrow')).toBe(0);
    });

    it('escrow can hold multiple concurrent reviews without mixing funds', async () => {
      await grantFunds('wallet_alice', THRESHOLD * 2);
      await grantFunds('wallet_bob', THRESHOLD * 2);

      const [fromAlice, fromBob] = await Promise.all([
        transfer({ fromWalletId: 'wallet_alice', toWalletId: 'wallet_bob', amountCents: THRESHOLD + 1000 }),
        transfer({ fromWalletId: 'wallet_bob', toWalletId: 'wallet_alice', amountCents: THRESHOLD + 2000 }),
      ]);
      expect(fromAlice.statusCode).toBe(202);
      expect(fromBob.statusCode).toBe(202);
      expect(await balanceOf('wallet_escrow')).toBe(2 * THRESHOLD + 3000);

      const [approved, rejected] = await Promise.all([
        review(fromAlice.json().transferId, { decision: 'APPROVE' }),
        review(fromBob.json().transferId, { decision: 'REJECT' }),
      ]);
      expect(approved.statusCode).toBe(200);
      expect(rejected.statusCode).toBe(200);

      expect(await balanceOf('wallet_escrow')).toBe(0);
      // Alice: +2*THRESHOLD grant, sent THRESHOLD+1000 (approved). Bob's send was refunded.
      expect(await balanceOf('wallet_alice')).toBe(SEED_BALANCE + 2 * THRESHOLD - (THRESHOLD + 1000));
      expect(await balanceOf('wallet_bob')).toBe(SEED_BALANCE + 2 * THRESHOLD + (THRESHOLD + 1000));
      expect(await ledgerTotal()).toBe(0);
      await expectAllWalletsLedgerSynced();
    });
  });

  describe('global rule: ledger must stay in sync with wallet balances', () => {
    it('rejects a direct balance mutation that has no matching ledger entry', async () => {
      await expect(
        query(`UPDATE wallets SET balance_cents = balance_cents + 1000 WHERE wallet_id = 'wallet_alice'`)
      ).rejects.toThrow(/LEDGER_OUT_OF_SYNC/);
      expect(await balanceOf('wallet_alice')).toBe(SEED_BALANCE);
    });

    it('rejects a (balanced) journal that has no matching wallet balance change', async () => {
      // The journal itself balances, so only the wallet-sync rule can catch this.
      await expect(
        query(
          `INSERT INTO ledger_entries (entry_id, transaction_ref, wallet_id, currency, direction, signed_amount_cents)
           VALUES ('le_orphan_a', 'rogue_tx', 'wallet_treasury', 'USD', 'DEBIT', -500),
                  ('le_orphan_b', 'rogue_tx', 'wallet_alice', 'USD', 'CREDIT', 500)`
        )
      ).rejects.toThrow(/LEDGER_OUT_OF_SYNC/);
      expect((await query(`SELECT 1 FROM ledger_entries WHERE transaction_ref = 'rogue_tx'`)).rows).toHaveLength(0);
    });

    it('rejects an unbalanced journal even when the wallet balance matches it', async () => {
      // Balance and ledger agree for alice, so only the journal-balance rule can catch this.
      await expect(
        withTransaction(async (client) => {
          await client.query(
            `UPDATE wallets SET balance_cents = balance_cents + 500 WHERE wallet_id = 'wallet_alice'`
          );
          await client.query(
            `INSERT INTO ledger_entries (entry_id, transaction_ref, wallet_id, currency, direction, signed_amount_cents)
             VALUES ('le_unbalanced', 'rogue_mint', 'wallet_alice', 'USD', 'CREDIT', 500)`
          );
        })
      ).rejects.toThrow(/UNBALANCED_JOURNAL/);
      expect(await balanceOf('wallet_alice')).toBe(SEED_BALANCE);
    });

    it('rejects ledger lines booked in a currency different from the account', async () => {
      await expect(
        query(
          `INSERT INTO ledger_entries (entry_id, transaction_ref, wallet_id, currency, direction, signed_amount_cents)
           VALUES ('le_x1', 'ccy_tx', 'wallet_alice', 'EUR', 'CREDIT', 500),
                  ('le_x2', 'ccy_tx', 'wallet_treasury_eur', 'EUR', 'DEBIT', -500)`
        )
      ).rejects.toThrow(/foreign key/);
    });

    it('keeps the ledger append-only: UPDATE and DELETE are rejected', async () => {
      await expect(
        query(`UPDATE ledger_entries SET signed_amount_cents = 1 WHERE entry_id = 'le_seed_alice'`)
      ).rejects.toThrow(/LEDGER_APPEND_ONLY/);
      await expect(query(`DELETE FROM ledger_entries WHERE entry_id = 'le_seed_alice'`)).rejects.toThrow(
        /LEDGER_APPEND_ONLY/
      );
    });

    it('rejects an unbalanced multi-statement transaction at COMMIT and rolls it back entirely', async () => {
      await expect(
        withTransaction(async (client) => {
          await client.query(
            `UPDATE wallets SET balance_cents = balance_cents - 500 WHERE wallet_id = 'wallet_alice'`
          );
          await client.query(
            `UPDATE wallets SET balance_cents = balance_cents + 500 WHERE wallet_id = 'wallet_bob'`
          );
          // no ledger entries written -> must not be able to commit
        })
      ).rejects.toThrow(/LEDGER_OUT_OF_SYNC/);

      expect(await balanceOf('wallet_alice')).toBe(SEED_BALANCE);
      expect(await balanceOf('wallet_bob')).toBe(SEED_BALANCE);
    });

    it('allows balanced transactions regardless of internal statement order', async () => {
      await grantFunds('wallet_alice', 1234); // ledger + balance in one tx
      expect(await balanceOf('wallet_alice')).toBe(SEED_BALANCE + 1234);
      await expectAllWalletsLedgerSynced();
    });

    it('holds across a mixed storm of withdrawals, transfers, escrow holds and reviews', async () => {
      await grantFunds('wallet_alice', THRESHOLD * 2);

      const [w1, t1, held, t2] = await Promise.all([
        api.inject({
          method: 'POST',
          url: '/wallets/withdraw',
          payload: { walletId: 'wallet_bob', amountCents: 1500, bankAccountRef: 'bank_mix' },
        }),
        transfer({ fromWalletId: 'wallet_alice', toWalletId: 'wallet_bob', amountCents: 800 }),
        transfer({ fromWalletId: 'wallet_alice', toWalletId: 'wallet_bob', amountCents: THRESHOLD + 1 }),
        transfer({ fromWalletId: 'wallet_bob', toWalletId: 'wallet_alice', amountCents: 300 }),
      ]);
      expect(w1.statusCode).toBe(201);
      expect(t1.statusCode).toBe(201);
      expect(held.statusCode).toBe(202);
      expect(t2.statusCode).toBe(201);

      await review(held.json().transferId, { decision: 'REJECT' });
      await api.inject({
        method: 'POST',
        url: '/payouts/webhook',
        payload: { eventId: 'evt_mix_fail', payoutId: w1.json().payoutId, eventType: 'payout.failed' },
      });

      await expectAllWalletsLedgerSynced();
      expect(await ledgerTotal()).toBe(0);
      expect(await balanceOf('wallet_escrow')).toBe(0);
    });
  });
});
