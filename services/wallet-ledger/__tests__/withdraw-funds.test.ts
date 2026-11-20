import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import type { FastifyInstance } from 'fastify';
import type { PoolClient } from 'pg';
import { ZodError } from 'zod';
import { buildApp } from '../src/server';
import { actAs } from './helpers/auth';
import { closeDb, pool, query, resetAndSeedDatabase } from '../src/db';
import { closeRedis, flushRedis } from '../src/redis';
import { walletService } from '../src/services/WalletService';
import { WalletVersionConflictError } from '../src/types/wallet';

const SEED_BALANCE = 10000;
const SEED_VERSION = 1;

type RawQuery = (sql: string, params?: unknown[]) => Promise<unknown>;

interface QueryHooks {
  /** Runs before each statement; throw to simulate that statement failing. */
  before?: (sql: string, raw: RawQuery) => Promise<void> | void;
  /** Runs after each statement, on the same connection/transaction. */
  after?: (sql: string, raw: RawQuery) => Promise<void> | void;
}

/**
 * Wrap every pooled client so tests can observe or inject faults into the real
 * transaction (no mocked database). `raw` bypasses the hooks to avoid recursion.
 */
function instrumentClients(hooks: QueryHooks = {}): { statements: string[] } {
  const statements: string[] = [];
  const originalConnect = pool.connect.bind(pool);

  vi.spyOn(pool, 'connect').mockImplementation(async () => {
    const client = await originalConnect();
    const originalQuery = client.query.bind(client) as unknown as RawQuery;

    (client as unknown as { query: RawQuery }).query = async (sql, params) => {
      const normalized = String(sql).replace(/\s+/g, ' ').trim();
      statements.push(normalized);
      await hooks.before?.(normalized, originalQuery);
      const result = await originalQuery(sql, params);
      await hooks.after?.(normalized, originalQuery);
      return result;
    };

    return client as PoolClient;
  });

  return { statements };
}

async function walletSnapshot(walletId: string) {
  const wallet = await walletService.getWallet(walletId);
  const payouts = await query<{ n: string }>(
    `SELECT COUNT(*)::text AS n FROM payouts WHERE wallet_id = $1`,
    [walletId]
  );
  const ledger = await query<{ n: string }>(
    `SELECT COUNT(*)::text AS n FROM ledger_entries WHERE transaction_ref <> 'seed_init'`
  );
  return {
    balanceCents: wallet?.balanceCents,
    version: wallet?.version,
    payoutCount: Number(payouts.rows[0].n),
    nonSeedLedgerCount: Number(ledger.rows[0].n),
  };
}

async function ledgerTotal(): Promise<number> {
  const res = await query<{ total: string }>(
    `SELECT COALESCE(SUM(signed_amount_cents), 0)::text AS total FROM ledger_entries`
  );
  return Number(res.rows[0].total);
}

const UNCHANGED_ALICE = {
  balanceCents: SEED_BALANCE,
  version: SEED_VERSION,
  payoutCount: 0,
  nonSeedLedgerCount: 0,
};

describe('WalletService.withdrawFunds', () => {
  let app: FastifyInstance;
  let api: ReturnType<typeof actAs>;

  const withdraw = (payload: Record<string, unknown>) =>
    api.inject({ method: 'POST', url: '/wallets/withdraw', payload });

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

  describe('happy path', () => {
    it('debits the wallet, bumps the version, and records a PROCESSING payout with a balanced ledger pair', async () => {
      const res = await withdraw({
        walletId: 'wallet_alice',
        amountCents: 2500,
        bankAccountRef: 'bank_happy',
        idempotencyKey: 'idem_happy_1',
      });

      expect(res.statusCode).toBe(201);
      const body = res.json();
      expect(body).toMatchObject({
        walletId: 'wallet_alice',
        amountCents: 2500,
        remainingBalanceCents: 7500,
        version: SEED_VERSION + 1,
        status: 'PROCESSING',
      });
      expect(body.payoutId).toMatch(/^po_/);

      const alice = await walletService.getWallet('wallet_alice');
      expect(alice?.balanceCents).toBe(7500);
      expect(alice?.version).toBe(SEED_VERSION + 1);

      const payout = await query<{
        wallet_id: string;
        amount_cents: string;
        bank_account_ref: string;
        status: string;
        idempotency_key: string;
      }>(
        `SELECT wallet_id, amount_cents::text, bank_account_ref, status, idempotency_key
         FROM payouts WHERE payout_id = $1`,
        [body.payoutId]
      );
      expect(payout.rows).toEqual([
        {
          wallet_id: 'wallet_alice',
          amount_cents: '2500',
          bank_account_ref: 'bank_happy',
          status: 'PROCESSING',
          idempotency_key: 'idem_happy_1',
        },
      ]);

      const ledger = await query<{ wallet_id: string; direction: string; amount: string }>(
        `SELECT wallet_id, direction, signed_amount_cents::text AS amount
         FROM ledger_entries WHERE transaction_ref = $1 ORDER BY direction`,
        [body.payoutId]
      );
      expect(ledger.rows).toEqual([
        { wallet_id: 'wallet_treasury', direction: 'CREDIT', amount: '2500' },
        { wallet_id: 'wallet_alice', direction: 'DEBIT', amount: '-2500' },
      ]);
      expect(await ledgerTotal()).toBe(0);
    });

    it('allows withdrawing the exact full balance down to zero', async () => {
      const res = await withdraw({
        walletId: 'wallet_alice',
        amountCents: SEED_BALANCE,
        bankAccountRef: 'bank_full',
      });

      expect(res.statusCode).toBe(201);
      expect(res.json().remainingBalanceCents).toBe(0);
      expect((await walletService.getWallet('wallet_alice'))?.balanceCents).toBe(0);
    });

    it('bumps the version exactly once per sequential withdrawal', async () => {
      const versions: number[] = [];
      for (const amountCents of [1000, 2000, 3000]) {
        const res = await withdraw({ walletId: 'wallet_alice', amountCents, bankAccountRef: 'b' });
        expect(res.statusCode).toBe(201);
        versions.push(res.json().version);
      }

      expect(versions).toEqual([2, 3, 4]);
      const alice = await walletService.getWallet('wallet_alice');
      expect(alice).toMatchObject({ balanceCents: 4000, version: 4 });
    });

    it('succeeds when expectedVersion matches, and the returned version chains into the next call', async () => {
      const first = await withdraw({
        walletId: 'wallet_alice',
        amountCents: 1000,
        bankAccountRef: 'b',
        expectedVersion: SEED_VERSION,
      });
      expect(first.statusCode).toBe(201);

      const second = await withdraw({
        walletId: 'wallet_alice',
        amountCents: 1000,
        bankAccountRef: 'b',
        expectedVersion: first.json().version,
      });
      expect(second.statusCode).toBe(201);
      expect(second.json()).toMatchObject({ remainingBalanceCents: 8000, version: 3 });
    });
  });

  describe('rejections leave no trace', () => {
    it('rejects a withdrawal 1 cent over the balance with 409 INSUFFICIENT_FUNDS', async () => {
      const res = await withdraw({
        walletId: 'wallet_alice',
        amountCents: SEED_BALANCE + 1,
        bankAccountRef: 'bank_over',
      });

      expect(res.statusCode).toBe(409);
      expect(res.json().code).toBe('INSUFFICIENT_FUNDS');
      expect(await walletSnapshot('wallet_alice')).toEqual(UNCHANGED_ALICE);
    });

    it('rejects any withdrawal from an already-empty wallet', async () => {
      await withdraw({ walletId: 'wallet_alice', amountCents: SEED_BALANCE, bankAccountRef: 'b' });

      const res = await withdraw({ walletId: 'wallet_alice', amountCents: 1, bankAccountRef: 'b' });
      expect(res.statusCode).toBe(409);
      expect(res.json().code).toBe('INSUFFICIENT_FUNDS');

      const alice = await walletService.getWallet('wallet_alice');
      expect(alice).toMatchObject({ balanceCents: 0, version: 2 });
    });

    it('returns 404 WALLET_NOT_FOUND for an unknown wallet and writes nothing', async () => {
      const res = await withdraw({
        walletId: 'wallet_ghost',
        amountCents: 100,
        bankAccountRef: 'b',
      });

      expect(res.statusCode).toBe(404);
      expect(res.json().code).toBe('WALLET_NOT_FOUND');
      const payouts = await query(`SELECT 1 FROM payouts`);
      expect(payouts.rows).toHaveLength(0);
      expect(await ledgerTotal()).toBe(0);
    });

    it.each([
      ['zero amount', { amountCents: 0 }],
      ['negative amount', { amountCents: -500 }],
      ['fractional cents', { amountCents: 10.5 }],
      ['amount as string', { amountCents: '100' }],
      ['empty bankAccountRef', { bankAccountRef: '' }],
      ['zero expectedVersion', { expectedVersion: 0 }],
      ['fractional expectedVersion', { expectedVersion: 1.5 }],
    ])('returns 400 for %s and writes nothing', async (_label, override) => {
      const res = await withdraw({
        walletId: 'wallet_alice',
        amountCents: 100,
        bankAccountRef: 'b',
        ...override,
      });

      expect(res.statusCode).toBe(400);
      expect(await walletSnapshot('wallet_alice')).toEqual(UNCHANGED_ALICE);
    });

    it('validates input at the service layer too, so internal callers cannot credit via a negative amount', async () => {
      await expect(
        walletService.withdrawFunds({
          walletId: 'wallet_alice',
          amountCents: -5000,
          bankAccountRef: 'b',
        })
      ).rejects.toBeInstanceOf(ZodError);

      expect(await walletSnapshot('wallet_alice')).toEqual(UNCHANGED_ALICE);
    });
  });

  describe('optimistic locking (expectedVersion)', () => {
    it('rejects a stale expectedVersion with 409 WALLET_VERSION_CONFLICT and writes nothing', async () => {
      const res = await withdraw({
        walletId: 'wallet_alice',
        amountCents: 100,
        bankAccountRef: 'b',
        expectedVersion: 7,
      });

      expect(res.statusCode).toBe(409);
      expect(res.json().code).toBe('WALLET_VERSION_CONFLICT');
      expect(res.json().error).toContain('expected version 7, found 1');
      expect(await walletSnapshot('wallet_alice')).toEqual(UNCHANGED_ALICE);
    });

    it('reports a version conflict before insufficient funds when both apply', async () => {
      const res = await withdraw({
        walletId: 'wallet_alice',
        amountCents: SEED_BALANCE * 10,
        bankAccountRef: 'b',
        expectedVersion: 99,
      });

      expect(res.statusCode).toBe(409);
      expect(res.json().code).toBe('WALLET_VERSION_CONFLICT');
    });

    it('lets exactly one of N concurrent requests holding the same version win', async () => {
      const responses = await Promise.all(
        Array.from({ length: 5 }, (_, i) =>
          withdraw({
            walletId: 'wallet_alice',
            amountCents: 1000,
            bankAccountRef: `bank_${i}`,
            expectedVersion: SEED_VERSION,
          })
        )
      );

      const codes = responses.map((r) => r.statusCode).sort();
      expect(codes).toEqual([201, 409, 409, 409, 409]);
      expect(
        responses.filter((r) => r.statusCode === 409).every((r) => r.json().code === 'WALLET_VERSION_CONFLICT')
      ).toBe(true);

      expect(await walletSnapshot('wallet_alice')).toEqual({
        balanceCents: 9000,
        version: 2,
        payoutCount: 1,
        nonSeedLedgerCount: 2,
      });
    });

    it('treats a transfer as a version change, invalidating a previously read version', async () => {
      const transfer = await api.inject({
        method: 'POST',
        url: '/wallets/transfer',
        payload: { fromWalletId: 'wallet_bob', toWalletId: 'wallet_alice', amountCents: 500 },
      });
      expect(transfer.statusCode).toBe(201);

      const res = await withdraw({
        walletId: 'wallet_alice',
        amountCents: 100,
        bankAccountRef: 'b',
        expectedVersion: SEED_VERSION,
      });
      expect(res.statusCode).toBe(409);
      expect(res.json().code).toBe('WALLET_VERSION_CONFLICT');
    });

    it('treats a payout.failed refund as a version change, invalidating a previously read version', async () => {
      const first = await withdraw({ walletId: 'wallet_alice', amountCents: 4000, bankAccountRef: 'b' });
      const versionAfterDebit = first.json().version;

      const webhook = await api.inject({
        method: 'POST',
        url: '/payouts/webhook',
        payload: { eventId: 'evt_refund', payoutId: first.json().payoutId, eventType: 'payout.failed' },
      });
      expect(webhook.statusCode).toBe(200);

      const stale = await withdraw({
        walletId: 'wallet_alice',
        amountCents: 100,
        bankAccountRef: 'b',
        expectedVersion: versionAfterDebit,
      });
      expect(stale.statusCode).toBe(409);
      expect(stale.json().code).toBe('WALLET_VERSION_CONFLICT');

      const alice = await walletService.getWallet('wallet_alice');
      expect(alice).toMatchObject({ balanceCents: SEED_BALANCE, version: versionAfterDebit + 1 });
    });
  });

  describe('transactional guarantees', () => {
    it('reads under a row lock and writes in the same transaction (BEGIN → SELECT FOR UPDATE → UPDATE → INSERTs → COMMIT)', async () => {
      const { statements } = instrumentClients();

      const res = await withdraw({ walletId: 'wallet_alice', amountCents: 100, bankAccountRef: 'b' });
      expect(res.statusCode).toBe(201);

      expect(statements).toHaveLength(6);
      expect(statements[0]).toBe('BEGIN');
      expect(statements[1]).toMatch(/^SELECT .* FROM wallets .* FOR UPDATE$/);
      expect(statements[2]).toMatch(/^UPDATE wallets SET .*version = version \+ 1.* WHERE wallet_id = \$2 AND version = \$3 AND balance_cents >= \$1 RETURNING/);
      expect(statements[3]).toMatch(/^INSERT INTO payouts/);
      expect(statements[4]).toMatch(/^INSERT INTO ledger_entries/);
      expect(statements[5]).toBe('COMMIT');
    });

    it('rolls back (never commits) when the balance check fails', async () => {
      const { statements } = instrumentClients();

      const res = await withdraw({
        walletId: 'wallet_alice',
        amountCents: SEED_BALANCE + 1,
        bankAccountRef: 'b',
      });
      expect(res.statusCode).toBe(409);

      expect(statements[0]).toBe('BEGIN');
      expect(statements[1]).toMatch(/FOR UPDATE$/);
      expect(statements.at(-1)).toBe('ROLLBACK');
      expect(statements).not.toContain('COMMIT');
    });

    it('rolls back the debit and payout if the final ledger write fails, then releases the connection', async () => {
      instrumentClients({
        before: (sql) => {
          if (sql.startsWith('INSERT INTO ledger_entries')) {
            throw new Error('simulated ledger write failure');
          }
        },
      });

      await expect(
        walletService.withdrawFunds({ walletId: 'wallet_alice', amountCents: 2500, bankAccountRef: 'b' })
      ).rejects.toThrow('simulated ledger write failure');

      vi.restoreAllMocks();
      expect(await walletSnapshot('wallet_alice')).toEqual(UNCHANGED_ALICE);

      // If the client/transaction slot leaked, this would hang until the test timeout.
      const next = await withdraw({ walletId: 'wallet_alice', amountCents: 2500, bankAccountRef: 'b' });
      expect(next.statusCode).toBe(201);
      expect(next.json()).toMatchObject({ remainingBalanceCents: 7500, version: 2 });
    });

    it('fails closed via the version compare-and-swap if a writer bypasses the row lock', async () => {
      // Simulate a rogue writer that modifies the wallet between the locked read and the
      // debit without honouring FOR UPDATE (e.g. a legacy code path that skips the lock).
      // The balance still covers the withdrawal, so only the version guard can catch this.
      instrumentClients({
        after: async (sql, raw) => {
          if (sql.includes('FOR UPDATE')) {
            await raw(
              `UPDATE wallets SET balance_cents = balance_cents - 100, version = version + 1
               WHERE wallet_id = 'wallet_alice'`
            );
          }
        },
      });

      const err = await walletService
        .withdrawFunds({ walletId: 'wallet_alice', amountCents: 5000, bankAccountRef: 'b' })
        .catch((e: unknown) => e);

      expect(err).toBeInstanceOf(WalletVersionConflictError);
      expect((err as WalletVersionConflictError).expectedVersion).toBe(SEED_VERSION);

      vi.restoreAllMocks();
      // The whole transaction, including the rogue write on the same connection, is rolled back.
      expect(await walletSnapshot('wallet_alice')).toEqual(UNCHANGED_ALICE);
    });
  });

  describe('concurrency / double-spend', () => {
    it('allows exactly 4 of 20 concurrent $25 withdrawals on a $100 wallet', async () => {
      const responses = await Promise.all(
        Array.from({ length: 20 }, (_, i) =>
          withdraw({ walletId: 'wallet_alice', amountCents: 2500, bankAccountRef: `bank_${i}` })
        )
      );

      const ok = responses.filter((r) => r.statusCode === 201);
      const rejected = responses.filter((r) => r.statusCode === 409);
      expect(ok).toHaveLength(4);
      expect(rejected).toHaveLength(16);
      expect(rejected.every((r) => r.json().code === 'INSUFFICIENT_FUNDS')).toBe(true);

      // Each winner observed a distinct, strictly serial post-debit state
      const remaining = ok.map((r) => r.json().remainingBalanceCents).sort((a, b) => b - a);
      expect(remaining).toEqual([7500, 5000, 2500, 0]);
      const versions = ok.map((r) => r.json().version).sort();
      expect(versions).toEqual([2, 3, 4, 5]);

      expect(await walletSnapshot('wallet_alice')).toEqual({
        balanceCents: 0,
        version: 5,
        payoutCount: 4,
        nonSeedLedgerCount: 8,
      });
      expect(await ledgerTotal()).toBe(0);
    });

    it('never overdraws with mixed concurrent amounts, and balance + payouts always equals the opening balance', async () => {
      const amounts = [3000, 4500, 1200, 999, 5000, 2500, 700, 3333, 1, 6000, 1500, 2222];

      const responses = await Promise.all(
        amounts.map((amountCents, i) =>
          withdraw({ walletId: 'wallet_alice', amountCents, bankAccountRef: `bank_${i}` })
        )
      );

      expect(responses.every((r) => r.statusCode === 201 || r.statusCode === 409)).toBe(true);
      const ok = responses.filter((r) => r.statusCode === 201);
      const withdrawn = ok.reduce((sum, r) => sum + r.json().amountCents, 0);

      const alice = await walletService.getWallet('wallet_alice');
      expect(alice!.balanceCents).toBeGreaterThanOrEqual(0);
      expect(alice!.balanceCents + withdrawn).toBe(SEED_BALANCE);
      expect(alice!.version).toBe(SEED_VERSION + ok.length);

      const payoutSum = await query<{ total: string }>(
        `SELECT COALESCE(SUM(amount_cents), 0)::text AS total FROM payouts WHERE wallet_id = 'wallet_alice'`
      );
      expect(Number(payoutSum.rows[0].total)).toBe(withdrawn);
      expect(await ledgerTotal()).toBe(0);

      // No false rejections: balances only decrease here, so each rejected amount must
      // exceed even the final balance (it certainly exceeded the balance when it ran).
      const rejectedAmounts = amounts.filter((_, i) => responses[i].statusCode === 409);
      for (const amount of rejectedAmounts) {
        expect(amount).toBeGreaterThan(alice!.balanceCents);
      }
    });

    it('isolates concurrent withdrawals on different wallets', async () => {
      const responses = await Promise.all(
        Array.from({ length: 12 }, (_, i) =>
          withdraw({
            walletId: i % 2 === 0 ? 'wallet_alice' : 'wallet_bob',
            amountCents: 2000,
            bankAccountRef: `bank_${i}`,
          })
        )
      );

      expect(responses.filter((r) => r.statusCode === 201)).toHaveLength(10);
      expect(responses.filter((r) => r.statusCode === 409)).toHaveLength(2);

      const alice = await walletService.getWallet('wallet_alice');
      const bob = await walletService.getWallet('wallet_bob');
      expect(alice).toMatchObject({ balanceCents: 0, version: 6 });
      expect(bob).toMatchObject({ balanceCents: 0, version: 6 });
      expect(await ledgerTotal()).toBe(0);
    });
  });
});
