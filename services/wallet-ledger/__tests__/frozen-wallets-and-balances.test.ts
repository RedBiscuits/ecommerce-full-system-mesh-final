import crypto from 'crypto';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import type { FastifyInstance } from 'fastify';
import type { PoolClient } from 'pg';
import type { Channel as AmqpChannel } from 'amqplib';
import { buildApp } from '../src/server';
import { actAs } from './helpers/auth';
import { closeDb, pool, query, resetAndSeedDatabase, withTransaction } from '../src/db';
import { closeRedis, flushRedis, redis } from '../src/redis';
import { walletService } from '../src/services/WalletService';
import { withMoneyTransaction } from '../src/services/moneyTransaction';
import {
  ledgerReconciliationService,
  startReconciliationWorker,
} from '../src/services/LedgerReconciliationService';
import {
  AmqpEventBus,
  EventEnvelope,
  LEDGER_EVENT_TYPES,
  Subscription,
  TOPOLOGY,
  DeliverySettled,
  createEnvelope,
  setEventBus,
} from '../src/messaging';
import { BrokerHarness, createBrokerHarness } from './helpers/broker';
import { waitForEvent } from './helpers/events';
import { config } from '../src/config/env';
import { WalletFrozenError } from '../src/types/wallet';


async function withIntegrityTriggersDisabled(fn: () => Promise<void>): Promise<void> {
  await query('ALTER TABLE wallets DISABLE TRIGGER USER');
  await query('ALTER TABLE ledger_entries DISABLE TRIGGER USER');
  try {
    await fn();
  } finally {
    await query('ALTER TABLE wallets ENABLE TRIGGER USER');
    await query('ALTER TABLE ledger_entries ENABLE TRIGGER USER');
  }
}

async function driftWallet(walletId: string, newBalance: number): Promise<void> {
  await withIntegrityTriggersDisabled(async () => {
    await query(`UPDATE wallets SET balance_cents = $1 WHERE wallet_id = $2`, [newBalance, walletId]);
  });
}

async function balanceOf(walletId: string): Promise<number> {
  return (await walletService.getWallet(walletId))?.balanceCents ?? NaN;
}

async function grantFunds(walletId: string, currency: string, clearing: string, amountCents: number): Promise<void> {
  await withTransaction(async (client) => {
    await client.query(`UPDATE wallets SET balance_cents = balance_cents + $1 WHERE wallet_id = $2`, [amountCents, walletId]);
    await client.query(
      `INSERT INTO ledger_entries (entry_id, transaction_ref, wallet_id, currency, direction, signed_amount_cents)
       VALUES ($1, $2, $3, $4, 'DEBIT', $5), ($6, $2, $7, $4, 'CREDIT', $8)`,
      [`le_${crypto.randomUUID()}`, `grant_${crypto.randomUUID()}`, clearing, currency, -amountCents, `le_${crypto.randomUUID()}`, walletId, amountCents]
    );
  });
}

describe('O(1) ledger tracking, live system balances, frozen wallets', () => {
  let app: FastifyInstance;
  let api: ReturnType<typeof actAs>;
  let bus: AmqpEventBus;
  let inspector: AmqpChannel;
  let harness: BrokerHarness;
  let worker: Subscription | undefined;

  const transfer = (payload: Record<string, unknown>) =>
    api.inject({ method: 'POST', url: '/wallets/transfer', payload });
  const withdraw = (payload: Record<string, unknown>) =>
    api.inject({ method: 'POST', url: '/wallets/withdraw', payload });

  async function workQueueMessages(): Promise<EventEnvelope[]> {
    const out: EventEnvelope[] = [];
    for (;;) {
      const msg = await inspector.get(TOPOLOGY.workQueue, { noAck: true });
      if (!msg) return out;
      out.push(JSON.parse(msg.content.toString()) as EventEnvelope);
    }
  }

  const nextAcked = () =>
    waitForEvent<DeliverySettled>(bus, 'delivery-settled', (e) => e.outcome === 'acked');

  /** Record every statement (and params) run through pooled clients. */
  function recordStatements(): Array<{ sql: string; params?: unknown[] }> {
    const statements: Array<{ sql: string; params?: unknown[] }> = [];
    const originalConnect = pool.connect.bind(pool);
    vi.spyOn(pool, 'connect').mockImplementation(async () => {
      const client = await originalConnect();
      const originalQuery = client.query.bind(client) as (sql: string, params?: unknown[]) => Promise<unknown>;
      (client as unknown as { query: typeof originalQuery }).query = async (sql, params) => {
        statements.push({ sql: String(sql).replace(/\s+/g, ' ').trim(), params });
        return originalQuery(sql, params);
      };
      return client;
    });
    return statements;
  }

  beforeAll(async () => {
    app = buildApp();
    await app.ready();
    api = actAs(app);
  });

  beforeEach(async () => {
    await resetAndSeedDatabase();
    await flushRedis();
    harness = await createBrokerHarness({ maxAttempts: 2 });
    bus = harness.bus;
    inspector = harness.inspector;
    setEventBus(bus);
    worker = undefined;
  });

  afterEach(async () => {
    vi.restoreAllMocks();
    config.platformFeeBps = 0;
    await worker?.cancel();
    await harness.close();
    setEventBus(undefined);
  });

  afterAll(async () => {
    await app.close();
    await closeDb();
    await closeRedis();
  });

  describe('O(1) incremental delta tracking', () => {
    it('neither trigger on the write path re-sums ledger history', async () => {
      const res = await query<{ proname: string; src: string }>(
        `SELECT proname, prosrc AS src FROM pg_proc
         WHERE proname IN ('apply_ledger_delta', 'assert_wallet_ledger_sync')`
      );
      expect(res.rows.map((r) => r.proname).sort()).toEqual(['apply_ledger_delta', 'assert_wallet_ledger_sync']);
      for (const fn of res.rows) {
        // No ledger scan at all on the write path...
        expect(fn.src).not.toMatch(/ledger_entries/i);
        // ...and the only aggregate allowed is over an account's fixed set of
        // balance buckets (<= shard_count rows), never over history.
        const sums = fn.src.match(/SUM\s*\([^)]*\)\s*INTO\s+\w+\s+FROM\s+(\w+)/gi) ?? [];
        for (const sum of sums) expect(sum).toMatch(/FROM\s+wallet_balance_shards$/i);
      }
      const sync = res.rows.find((r) => r.proname === 'assert_wallet_ledger_sync')!;
      expect(sync.src).not.toMatch(/SUM\s*\(/i);
    });

    it('advances running total and entry counter by exactly one delta per ledger line', async () => {
      config.platformFeeBps = 100;
      const before = await walletService.getWallet('wallet_alice');
      const feeBefore = await walletService.getWallet('wallet_fee_treasury_usd');

      const res = await transfer({ fromWalletId: 'wallet_alice', toWalletId: 'wallet_bob', amountCents: 1000 });
      expect(res.statusCode).toBe(201);

      const after = await walletService.getWallet('wallet_alice');
      const feeAfter = await walletService.getWallet('wallet_fee_treasury_usd');
      expect(after!.ledgerBalanceCents - before!.ledgerBalanceCents).toBe(-1010);
      expect(after!.ledgerVersion - before!.ledgerVersion).toBe(1); // one ledger line for alice
      expect(feeAfter!.ledgerVersion - feeBefore!.ledgerVersion).toBe(1);
      expect(after!.balanceCents).toBe(after!.ledgerBalanceCents);
    });

    it('posts journal lines in wallet_id order (global lock order for hot system rows)', async () => {
      config.platformFeeBps = 100;
      const statements = recordStatements();

      // USD -> EUR with a fee touches 5 wallets incl. fee treasury and both FX pools.
      const res = await transfer({ fromWalletId: 'wallet_alice', toWalletId: 'wallet_carol', amountCents: 1000 });
      expect(res.statusCode).toBe(201);

      const insert = statements.find((st) => st.sql.startsWith('INSERT INTO ledger_entries'))!;
      // params are 6 per line; wallet_id is the 3rd of each group
      const walletOrder = insert.params!.filter((_, i) => i % 6 === 2);
      expect(walletOrder).toEqual([
        'wallet_alice',
        'wallet_carol',
        'wallet_fee_treasury_usd',
        'wallet_fx_pool_eur',
        'wallet_fx_pool_usd',
      ]);
      expect([...walletOrder].sort()).toEqual(walletOrder);
    });

    it('a new wallet cannot be created with seeded running totals or an unbacked balance', async () => {
      // Precalculated columns are forced to the empty-ledger state on insert.
      await query(
        `INSERT INTO wallets (wallet_id, owner_id, currency, ledger_balance_cents, ledger_version)
         VALUES ('wallet_new', 'user_new', 'USD', 999999, 42)`
      );
      expect(await walletService.getWallet('wallet_new')).toMatchObject({ ledgerBalanceCents: 0, ledgerVersion: 0 });

      // A balance with no ledger behind it cannot commit.
      await expect(
        query(`INSERT INTO wallets (wallet_id, owner_id, currency, balance_cents) VALUES ('wallet_rich', 'u', 'USD', 500)`)
      ).rejects.toThrow(/LEDGER_OUT_OF_SYNC/);
    });
  });

  describe('live balances for ledger-maintained accounts', () => {
    it('GET /wallets/:id shows live clearing, fee-treasury and FX-pool balances, negatives included', async () => {
      config.platformFeeBps = 200;
      const res = await transfer({ fromWalletId: 'wallet_alice', toWalletId: 'wallet_carol', amountCents: 5000 });
      expect(res.statusCode).toBe(201);

      const get = async (id: string) => (await api.inject({ method: 'GET', url: `/wallets/${id}` })).json();

      expect(await get('wallet_treasury')).toMatchObject({
        accountType: 'CLEARING',
        balanceCents: -20000, // opening balances funded from the external world: negative is valid
        frozenPendingReconciliation: false,
      });
      expect(await get('wallet_fee_treasury_usd')).toMatchObject({
        accountType: 'FEE_TREASURY',
        balanceCents: 100,
        ledgerBalanceCents: 100,
        ledgerVersion: 1, // entry counter aggregated over buckets
        shardCount: 16,
        version: 1, // the account row itself is never written by transfers
      });
      expect(await get('wallet_fx_pool_usd')).toMatchObject({ accountType: 'FX_POOL', balanceCents: 5000, shardCount: 16 });
      expect(await get('wallet_fx_pool_eur')).toMatchObject({ accountType: 'FX_POOL', balanceCents: -4600 });
    });

    it.each([
      ['FEE_TREASURY', 'wallet_fee_treasury_usd', 'wallet_fx_pool_usd'],
      ['USER', 'wallet_alice', 'wallet_treasury'],
      ['ESCROW', 'wallet_escrow', 'wallet_treasury'],
    ])('rejects any ledger movement that would take a %s account below zero', async (_type, debited, credited) => {
      await expect(
        withTransaction(async (client) => {
          await client.query(
            `UPDATE wallets SET balance_cents = balance_cents - 20001 WHERE wallet_id = $1 AND account_type IN ('USER', 'ESCROW')`,
            [debited]
          );
          await client.query(
            `INSERT INTO ledger_entries (entry_id, transaction_ref, wallet_id, currency, direction, signed_amount_cents)
             VALUES ('le_neg_1', 'neg_test', $1, 'USD', 'DEBIT', -20001), ('le_neg_2', 'neg_test', $2, 'USD', 'CREDIT', 20001)`,
            [debited, credited]
          );
        })
      ).rejects.toThrow(/wallets_(ledger_)?balance_non_negative|SHARDED_BALANCE_NON_NEGATIVE/);
    });

    it('allows FX pool and clearing accounts to go negative', async () => {
      await transfer({ fromWalletId: 'wallet_carol', toWalletId: 'wallet_alice', amountCents: 1000 });
      expect(await balanceOf('wallet_fx_pool_usd')).toBe(-1087);
      expect(await balanceOf('wallet_treasury_eur')).toBe(-10000);
    });
  });

  describe('sharded balances for hot system accounts', () => {
    it('concurrent same-currency transfers write different buckets and never the account row', async () => {
      config.platformFeeBps = 100;
      const rowVersion = async () =>
        (await query<{ xmin: string }>(`SELECT xmin::text FROM wallets WHERE wallet_id = 'wallet_fee_treasury_usd'`))
          .rows[0].xmin;
      const before = await rowVersion();

      const results = await Promise.all(
        Array.from({ length: 32 }, (_, i) =>
          transfer({
            fromWalletId: i % 2 ? 'wallet_alice' : 'wallet_bob',
            toWalletId: i % 2 ? 'wallet_bob' : 'wallet_alice',
            amountCents: 100,
          })
        )
      );
      expect(results.every((r) => r.statusCode === 201)).toBe(true);

      // The hot account's own row was never written (so never row-locked)...
      expect(await rowVersion()).toBe(before);
      // ...its fee credits were spread across the buckets instead.
      const buckets = await query<{ shard_no: number; entries: string }>(
        `SELECT shard_no, entry_count::text AS entries FROM wallet_balance_shards
         WHERE wallet_id = 'wallet_fee_treasury_usd' AND entry_count > 0 ORDER BY shard_no`
      );
      expect(buckets.rows.length).toBe(16); // every bucket used
      expect(Math.max(...buckets.rows.map((b) => Number(b.entries)))).toBe(2); // evenly: 32 over 16
      expect(await walletService.getWallet('wallet_fee_treasury_usd')).toMatchObject({
        balanceCents: 32, // 32 x 1 cent fee, aggregated over buckets
        ledgerVersion: 32,
      });
    });

    it('creates exactly shard_count buckets per sharded account and none for user wallets', async () => {
      const res = await query<{ account_type: string; shard_count: number; buckets: string }>(
        `SELECT w.account_type, w.shard_count, COUNT(s.shard_no)::text AS buckets
         FROM wallets w LEFT JOIN wallet_balance_shards s ON s.wallet_id = w.wallet_id
         GROUP BY w.wallet_id, w.account_type, w.shard_count`
      );
      for (const row of res.rows) {
        const sharded = ['FEE_TREASURY', 'FX_POOL', 'CLEARING'].includes(row.account_type);
        expect({ type: row.account_type, buckets: Number(row.buckets) }).toEqual({
          type: row.account_type,
          buckets: sharded ? Number(row.shard_count) : 0,
        });
        expect(Number(row.shard_count) > 0).toBe(sharded);
      }
    });

    it('keeps the sharded account row static: its balance columns can never be written', async () => {
      await expect(
        query(`UPDATE wallets SET balance_cents = 5 WHERE wallet_id = 'wallet_fx_pool_usd'`)
      ).rejects.toThrow(/wallets_sharded_row_static/);
    });

    it('debits of a non-negative sharded account check the TRUE total, not one bucket', async () => {
      config.platformFeeBps = 1000; // 10%
      for (let i = 0; i < 4; i++) {
        await transfer({ fromWalletId: 'wallet_alice', toWalletId: 'wallet_bob', amountCents: 100 }); // +10 each
      }
      expect(await balanceOf('wallet_fee_treasury_usd')).toBe(40); // spread over several buckets

      const debitFee = (amount: number, ref: string) =>
        withTransaction(async (client) => {
          await client.query(
            `INSERT INTO ledger_entries (entry_id, transaction_ref, wallet_id, currency, direction, signed_amount_cents)
             VALUES ($1, $2, 'wallet_fee_treasury_usd', 'USD', 'DEBIT', $3), ($4, $2, 'wallet_fx_pool_usd', 'USD', 'CREDIT', $5)`,
            [`le_${ref}_a`, ref, -amount, `le_${ref}_b`, amount]
          );
        });

      // 30 exceeds any single bucket (10-20) but not the true total: allowed.
      await debitFee(30, 'fee_refund_ok');
      expect(await balanceOf('wallet_fee_treasury_usd')).toBe(10);
      const negativeBucket = await query(
        `SELECT 1 FROM wallet_balance_shards WHERE wallet_id = 'wallet_fee_treasury_usd' AND balance_cents < 0`
      );
      expect(negativeBucket.rows.length).toBeGreaterThan(0); // a bucket may dip below zero...

      // ...but the account total may not.
      await expect(debitFee(11, 'fee_refund_too_much')).rejects.toThrow(/SHARDED_BALANCE_NON_NEGATIVE/);
      expect(await balanceOf('wallet_fee_treasury_usd')).toBe(10);
    });
  });

  describe('frozen wallets: clean 409 + reconciliation event', () => {
    it('withdraw from a drifted wallet: 409, nothing written, one WRITE_GUARD event', async () => {
      await driftWallet('wallet_alice', 9000);

      const res = await withdraw({ walletId: 'wallet_alice', amountCents: 100, bankAccountRef: 'b' });

      expect(res.statusCode).toBe(409);
      expect(res.json()).toEqual({
        error: expect.stringContaining('frozen pending reconciliation'),
        code: 'WALLET_FROZEN_PENDING_RECONCILIATION',
        walletId: 'wallet_alice',
      });
      expect((await query(`SELECT 1 FROM payouts`)).rows).toHaveLength(0);
      expect(await walletService.getWallet('wallet_alice')).toMatchObject({
        balanceCents: 9000,
        frozenPendingReconciliation: true,
      });

      const events = await workQueueMessages();
      expect(events).toHaveLength(1);
      expect(events[0]).toMatchObject({
        type: LEDGER_EVENT_TYPES.WALLET_DRIFT,
        detectedBy: 'WRITE_GUARD',
        payload: { walletId: 'wallet_alice', balanceCents: '9000', ledgerBalanceCents: '10000', accountType: 'USER' },
      });
    });

    it('fails fast under the row lock: no balance or ledger write is even attempted', async () => {
      await driftWallet('wallet_bob', 1);
      const statements = recordStatements();

      const res = await transfer({ fromWalletId: 'wallet_alice', toWalletId: 'wallet_bob', amountCents: 100 });

      expect(res.statusCode).toBe(409);
      const txSql = statements.map((st) => st.sql);
      expect(txSql.some((q) => q.startsWith('UPDATE wallets') || q.startsWith('INSERT'))).toBe(false);
      expect(txSql.filter((q) => q.includes('FOR UPDATE'))).toHaveLength(2); // both parties locked, then refused
      expect(txSql.at(-1)).toBe('ROLLBACK'); // the write guard, not the COMMIT-time backstop
    });

    it('deduplicates events for a retry storm against the same frozen wallet', async () => {
      await driftWallet('wallet_alice', 9000);

      const responses = await Promise.all(
        Array.from({ length: 8 }, (_, i) =>
          i % 2
            ? withdraw({ walletId: 'wallet_alice', amountCents: 100, bankAccountRef: 'b' })
            : transfer({ fromWalletId: 'wallet_alice', toWalletId: 'wallet_bob', amountCents: 100 })
        )
      );

      expect(responses.every((r) => r.statusCode === 409)).toBe(true);
      expect(await workQueueMessages()).toHaveLength(1);
    });

    it.each([
      ['sender', 'wallet_alice', { fromWalletId: 'wallet_alice', toWalletId: 'wallet_bob' }],
      ['recipient', 'wallet_bob', { fromWalletId: 'wallet_alice', toWalletId: 'wallet_bob' }],
    ])('blocks a transfer when the %s is drifted, leaving the other side untouched', async (_role, drifted, parties) => {
      await driftWallet(drifted, 1);

      const res = await transfer({ ...parties, amountCents: 100 });

      expect(res.statusCode).toBe(409);
      expect(res.json().walletId).toBe(drifted);
      const other = drifted === 'wallet_alice' ? 'wallet_bob' : 'wallet_alice';
      expect(await balanceOf(other)).toBe(10000);
    });

    it('blocks an AML release from a drifted escrow, keeping the saga retryable', async () => {
      await grantFunds('wallet_alice', 'USD', 'wallet_treasury', 100000);
      const held = await transfer({ fromWalletId: 'wallet_alice', toWalletId: 'wallet_bob', amountCents: 60000 });
      expect(held.statusCode).toBe(202);
      await driftWallet('wallet_escrow', 1);

      const review = () =>
        api.inject({ method: 'POST', url: `/transfers/${held.json().transferId}/review`, payload: { decision: 'APPROVE' } });

      const blocked = await review();
      expect(blocked.statusCode).toBe(409);
      expect(blocked.json().walletId).toBe('wallet_escrow');
      const status = await query<{ status: string }>(`SELECT status FROM transfers WHERE transfer_id = $1`, [
        held.json().transferId,
      ]);
      expect(status.rows[0].status).toBe('PENDING_REVIEW');

      const repaired = nextAcked();
      worker = await startReconciliationWorker(bus);
      await repaired;
      expect((await walletService.getWallet('wallet_escrow'))!.frozenPendingReconciliation).toBe(false);

      const approved = await review();
      expect(approved.statusCode).toBe(200);
      expect(await balanceOf('wallet_bob')).toBe(70000);
    });

    it('refuses a webhook refund into a drifted wallet (409) so the bank retries later', async () => {
      const wd = await withdraw({ walletId: 'wallet_alice', amountCents: 1000, bankAccountRef: 'b' });
      await driftWallet('wallet_alice', 5);

      const hook = await api.inject({
        method: 'POST',
        url: '/payouts/webhook',
        payload: { eventId: 'evt_frozen', payoutId: wd.json().payoutId, eventType: 'payout.failed' },
      });

      expect(hook.statusCode).toBe(409);
      const payout = await query<{ status: string }>(`SELECT status FROM payouts WHERE payout_id = $1`, [wd.json().payoutId]);
      expect(payout.rows[0].status).toBe('PROCESSING');
      // The event id was not consumed, so the bank's retry will be processed.
      expect((await query(`SELECT 1 FROM payout_webhook_events WHERE event_id = 'evt_frozen'`)).rows).toHaveLength(0);
    });

    it('maps a COMMIT-time sync violation on a genuinely drifted wallet to the same 409 + event', async () => {
      // Defense in depth: a write path that forgot the lock-time guard still
      // cannot surface a raw 500 for a drifted wallet.
      await driftWallet('wallet_alice', 9000);

      const err = await withMoneyTransaction(async (client: PoolClient) => {
        await client.query(`UPDATE wallets SET balance_cents = balance_cents + 1 WHERE wallet_id = 'wallet_alice'`);
        await client.query(
          `INSERT INTO ledger_entries (entry_id, transaction_ref, wallet_id, currency, direction, signed_amount_cents)
           VALUES ('le_unguarded_a', 'unguarded', 'wallet_alice', 'USD', 'CREDIT', 1),
                  ('le_unguarded_b', 'unguarded', 'wallet_treasury', 'USD', 'DEBIT', -1)`
        );
      }).catch((e: unknown) => e);

      expect(err).toBeInstanceOf(WalletFrozenError);
      expect((err as WalletFrozenError).walletId).toBe('wallet_alice');
      expect(await balanceOf('wallet_alice')).toBe(9000); // rolled back
      const [event] = await workQueueMessages();
      expect(event.payload).toMatchObject({ walletId: 'wallet_alice', accountType: 'USER' });
    });

    it('corrupted buckets of a sharded account do not block transfers and are rebuilt by reconciliation', async () => {
      // Sharded system accounts have no separate projection, so bucket drift can
      // never let a user overspend: transfers keep flowing, the audit finds it.
      await query(
        `UPDATE wallet_balance_shards SET balance_cents = balance_cents + 999
         WHERE wallet_id = 'wallet_treasury' AND shard_no = 3`
      );
      expect(await balanceOf('wallet_treasury')).toBe(-20000 + 999);

      const res = await withdraw({ walletId: 'wallet_alice', amountCents: 100, bankAccountRef: 'b' });
      expect(res.statusCode).toBe(201);

      const outcome = await ledgerReconciliationService.handle(
        createEnvelope(LEDGER_EVENT_TYPES.WALLET_DRIFT, 'audit_buckets', {
          walletId: 'wallet_treasury',
          currency: 'USD',
          accountType: 'CLEARING',
          balanceCents: String(-20000 + 999 + 100),
          ledgerBalanceCents: String(-20000 + 999 + 100),
          ledgerVersion: '2',
        })
      );
      expect(outcome).toBe('RECONCILED');
      expect(await walletService.getWallet('wallet_treasury')).toMatchObject({
        balanceCents: -20000 + 100,
        ledgerVersion: 2,
        shardCount: 16,
      });
      const buckets = await query(`SELECT 1 FROM wallet_balance_shards WHERE wallet_id = 'wallet_treasury'`);
      expect(buckets.rows).toHaveLength(16); // layout preserved
    });

    it('does NOT disguise a genuine bug as a frozen wallet (committed state is not drifted)', async () => {
      const err = await withMoneyTransaction(async (client: PoolClient) => {
        // A buggy write path: moves a balance without posting its ledger entry.
        await client.query(`UPDATE wallets SET balance_cents = balance_cents + 1 WHERE wallet_id = 'wallet_bob'`);
      }).catch((e: unknown) => e);

      expect(err).not.toBeInstanceOf(WalletFrozenError);
      expect(String((err as Error).message)).toMatch(/LEDGER_OUT_OF_SYNC/);
      expect(await workQueueMessages()).toHaveLength(0);
      expect((await walletService.getWallet('wallet_bob'))!.frozenPendingReconciliation).toBe(false);
    });

    it('still answers 409 when the broker is down, and re-arms the dedupe key', async () => {
      await driftWallet('wallet_alice', 9000);
      const publishSpy = vi.spyOn(bus, 'publish').mockRejectedValueOnce(new Error('broker unreachable'));

      const first = await withdraw({ walletId: 'wallet_alice', amountCents: 100, bankAccountRef: 'b' });
      expect(first.statusCode).toBe(409);
      expect(await redis.get('wallet:frozen-event:wallet_alice')).toBeNull(); // released for retry

      const second = await withdraw({ walletId: 'wallet_alice', amountCents: 100, bankAccountRef: 'b' });
      expect(second.statusCode).toBe(409);
      expect(publishSpy).toHaveBeenCalledTimes(2);
      expect(await workQueueMessages()).toHaveLength(1); // the retry got through
    });

    it('never returns 500 while a frozen wallet is reconciled under concurrent traffic', async () => {
      await driftWallet('wallet_alice', 9000);
      const burst = async () =>
        (
          await Promise.all(
            Array.from({ length: 4 }, () =>
              transfer({ fromWalletId: 'wallet_alice', toWalletId: 'wallet_bob', amountCents: 10 })
            )
          )
        ).map((r) => r.statusCode);

      // 1) Frozen: every request is refused cleanly.
      const frozen = await burst();
      expect(frozen).toEqual([409, 409, 409, 409]);

      // 2) Traffic races the reconciliation: each request is 201 or 409, never 500.
      const repaired = nextAcked();
      worker = await startReconciliationWorker(bus);
      const racing = burst();
      await repaired;
      expect((await racing).every((code) => code === 201 || code === 409)).toBe(true);

      // 3) Repaired: every request succeeds.
      expect(await burst()).toEqual([201, 201, 201, 201]);

      const alice = (await walletService.getWallet('wallet_alice'))!;
      const bob = (await walletService.getWallet('wallet_bob'))!;
      expect(alice.frozenPendingReconciliation).toBe(false);
      expect(alice.balanceCents + bob.balanceCents).toBe(20000); // repaired to the ledger, then conserved
    });
  });

  describe('reconciliation worker', () => {
    it('locks the wallet row FOR UPDATE before reading the ledger and correcting it', async () => {
      await driftWallet('wallet_bob', 1);
      const statements: string[] = [];
      const originalConnect = pool.connect.bind(pool);
      vi.spyOn(pool, 'connect').mockImplementation(async () => {
        const client = await originalConnect();
        const originalQuery = client.query.bind(client) as (sql: string, params?: unknown[]) => Promise<unknown>;
        (client as unknown as { query: typeof originalQuery }).query = async (sql, params) => {
          statements.push(String(sql).replace(/\s+/g, ' ').trim());
          return originalQuery(sql, params);
        };
        return client;
      });

      const outcome = await ledgerReconciliationService.handle(
        createEnvelope(LEDGER_EVENT_TYPES.WALLET_DRIFT, 'audit_lock_test', {
          walletId: 'wallet_bob',
          currency: 'USD',
          accountType: 'USER',
          balanceCents: '1',
          ledgerBalanceCents: '10000',
          ledgerVersion: '1',
        })
      );

      expect(outcome).toBe('RECONCILED');
      expect(statements[0]).toBe('BEGIN');
      expect(statements[1]).toMatch(/^SELECT .* FROM wallets WHERE wallet_id = \$1 FOR UPDATE$/);
      const lockIdx = 1;
      const ledgerReadIdx = statements.findIndex((s) => s.includes('FROM ledger_entries'));
      const updateIdx = statements.findIndex((s) => s.startsWith('UPDATE wallets'));
      expect(lockIdx).toBeLessThan(ledgerReadIdx); // verify the ledger only while holding the lock
      expect(ledgerReadIdx).toBeLessThan(updateIdx);
      expect(statements.at(-1)).toBe('COMMIT');
      expect(await walletService.getWallet('wallet_bob')).toMatchObject({ balanceCents: 10000, frozenPendingReconciliation: false });
    });

    it('repairs a corrupted running total (cache drift), not just the balance', async () => {
      await withIntegrityTriggersDisabled(async () => {
        await query(`UPDATE wallets SET ledger_balance_cents = 123, ledger_version = 99 WHERE wallet_id = 'wallet_carol'`);
      });

      const outcome = await ledgerReconciliationService.handle(
        createEnvelope(LEDGER_EVENT_TYPES.WALLET_DRIFT, 'audit_cache', {
          walletId: 'wallet_carol',
          currency: 'EUR',
          accountType: 'USER',
          balanceCents: '10000',
          ledgerBalanceCents: '123',
          ledgerVersion: '99',
        })
      );

      expect(outcome).toBe('RECONCILED');
      expect(await walletService.getWallet('wallet_carol')).toMatchObject({
        balanceCents: 10000,
        ledgerBalanceCents: 10000,
        ledgerVersion: 1,
      });
      const log = await query<{ prev_lb: string; prev_lv: string }>(
        `SELECT previous_ledger_balance_cents::text AS prev_lb, previous_ledger_version::text AS prev_lv
         FROM reconciliation_log WHERE wallet_id = 'wallet_carol'`
      );
      expect(log.rows[0]).toEqual({ prev_lb: '123', prev_lv: '99' });
    });
  });
});
