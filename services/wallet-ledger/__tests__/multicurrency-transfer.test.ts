import crypto from 'crypto';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import type { FastifyInstance } from 'fastify';
import { buildApp } from '../src/server';
import { actAs } from './helpers/auth';
import { closeDb, pool, query, resetAndSeedDatabase, withTransaction } from '../src/db';
import { closeRedis, flushRedis } from '../src/redis';
import { walletService } from '../src/services/WalletService';
import { config } from '../src/config/env';
import { CURRENCIES, assertSupportedCurrency, systemAccountsFor } from '../src/money';

const DEFAULT_FEE_BPS = config.platformFeeBps;

async function balanceOf(walletId: string): Promise<number> {
  return (await walletService.getWallet(walletId))?.balanceCents ?? NaN;
}

async function ledgerSumFor(walletId: string): Promise<bigint> {
  const res = await query<{ total: string }>(
    `SELECT COALESCE(SUM(signed_amount_cents), 0)::text AS total FROM ledger_entries WHERE wallet_id = $1`,
    [walletId]
  );
  return BigInt(res.rows[0].total);
}

async function perCurrencyTotals(): Promise<Record<string, string>> {
  const res = await query<{ currency: string; total: string }>(
    `SELECT currency, SUM(signed_amount_cents)::text AS total FROM ledger_entries GROUP BY currency ORDER BY currency`
  );
  return Object.fromEntries(res.rows.map((r) => [r.currency, r.total]));
}

async function expectEveryCurrencyZeroSum(): Promise<void> {
  for (const total of Object.values(await perCurrencyTotals())) {
    expect(total).toBe('0');
  }
}

/** Every wallet (sharded system accounts included): effective balance = running total = full recount. */
async function expectAllSyncedWalletsMatchLedger(): Promise<void> {
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

async function journalOf(ref: string): Promise<Array<{ wallet_id: string; currency: string; amount: string }>> {
  const res = await query<{ wallet_id: string; currency: string; amount: string }>(
    `SELECT wallet_id, currency, signed_amount_cents::text AS amount
     FROM ledger_entries WHERE transaction_ref = $1
     ORDER BY currency, signed_amount_cents, wallet_id`,
    [ref]
  );
  return res.rows;
}

async function grantFunds(walletId: string, amountCents: number): Promise<void> {
  await withTransaction(async (client) => {
    const res = await client.query<{ currency: string }>(`SELECT currency FROM wallets WHERE wallet_id = $1`, [walletId]);
    const currency = assertSupportedCurrency(res.rows[0].currency);
    await client.query(
      `UPDATE wallets SET balance_cents = balance_cents + $1, version = version + 1 WHERE wallet_id = $2`,
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

async function setRate(base: string, quote: string, rateScaled: number): Promise<void> {
  await query(
    `UPDATE fx_rates SET rate_scaled = $3, updated_at = NOW() WHERE base_currency = $1 AND quote_currency = $2`,
    [base, quote, rateScaled]
  );
}

describe('Multi-currency transfers with integer fee / FX engine', () => {
  let app: FastifyInstance;
  let api: ReturnType<typeof actAs>;

  const transfer = (payload: Record<string, unknown>) =>
    api.inject({ method: 'POST', url: '/wallets/transfer', payload });

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
    config.platformFeeBps = DEFAULT_FEE_BPS;
    vi.restoreAllMocks();
  });

  afterAll(async () => {
    await app.close();
    await closeDb();
    await closeRedis();
  });

  describe('schema', () => {
    it('keeps the DB currency table identical to the code registry', async () => {
      const res = await query<{ code: string; minor_units: number }>(
        `SELECT code, minor_units FROM currencies ORDER BY code`
      );
      const fromDb = Object.fromEntries(res.rows.map((r) => [r.code, Number(r.minor_units)]));
      const fromCode = Object.fromEntries(Object.values(CURRENCIES).map((c) => [c.code, c.minorUnits]));
      expect(fromDb).toEqual(fromCode);
    });

    it('stores every balance and ledger amount as BIGINT', async () => {
      const res = await query<{ table_name: string; column_name: string; data_type: string }>(
        `SELECT table_name, column_name, data_type FROM information_schema.columns
         WHERE table_schema = 'public' AND (column_name LIKE '%cents%' OR column_name = 'rate_scaled')
         ORDER BY table_name, column_name`
      );
      expect(res.rows.length).toBeGreaterThan(8);
      for (const row of res.rows) {
        expect({ col: `${row.table_name}.${row.column_name}`, type: row.data_type }).toEqual({
          col: `${row.table_name}.${row.column_name}`,
          type: 'bigint',
        });
      }
    });
  });

  describe('platform fee (basis points, charged on top)', () => {
    it('routes the fee to the source-currency fee treasury with a balanced journal', async () => {
      config.platformFeeBps = 150; // 1.5%

      const res = await transfer({ fromWalletId: 'wallet_alice', toWalletId: 'wallet_bob', amountCents: 4000 });

      expect(res.statusCode).toBe(201);
      expect(res.json()).toMatchObject({
        amountCents: 4000,
        feeBps: 150,
        feeCents: 60,
        totalDebitCents: 4060,
        creditedAmountCents: 4000,
        sourceCurrency: 'USD',
        destCurrency: 'USD',
        senderRemainingBalanceCents: 5940,
      });
      expect(await balanceOf('wallet_alice')).toBe(10000 - 4060);
      expect(await balanceOf('wallet_bob')).toBe(10000 + 4000);
      expect(await ledgerSumFor('wallet_fee_treasury_usd')).toBe(60n);

      expect(await journalOf(res.json().transferId)).toEqual([
        { wallet_id: 'wallet_alice', currency: 'USD', amount: '-4060' },
        { wallet_id: 'wallet_fee_treasury_usd', currency: 'USD', amount: '60' },
        { wallet_id: 'wallet_bob', currency: 'USD', amount: '4000' },
      ]);

      const steps = await api.inject({ method: 'GET', url: `/transfers/${res.json().transferId}` });
      expect(steps.json().sagaSteps.map((s: { step: string }) => s.step)).toEqual([
        'SENDER_DEBITED',
        'FEE_COLLECTED',
        'RECIPIENT_CREDITED',
        'TRANSFER_COMPLETED',
      ]);
      await expectEveryCurrencyZeroSum();
      await expectAllSyncedWalletsMatchLedger();
    });

    it.each([
      [250, 2], // 2.5 -> 2 (tie to even)
      [350, 4], // 3.5 -> 4 (tie to even)
      [150, 2], // 1.5 -> 2 (tie to even)
      [50, 0], //  0.5 -> 0: no fee line is written at all
    ])('applies banker\'s rounding end-to-end: 1%% of %s = %s', async (amount, expectedFee) => {
      config.platformFeeBps = 100;

      const res = await transfer({ fromWalletId: 'wallet_alice', toWalletId: 'wallet_bob', amountCents: amount });

      expect(res.json().feeCents).toBe(expectedFee);
      expect(res.json().totalDebitCents).toBe(amount + expectedFee);
      const lines = await journalOf(res.json().transferId);
      expect(lines.some((l) => l.wallet_id === 'wallet_fee_treasury_usd')).toBe(expectedFee > 0);
      await expectEveryCurrencyZeroSum();
    });

    it('checks funds against principal + fee, allowing an exact-to-zero spend', async () => {
      config.platformFeeBps = 150;

      // 9900 + 148.5 -> 148 (tie to even) = 10048 > 10000
      const tooMuch = await transfer({ fromWalletId: 'wallet_alice', toWalletId: 'wallet_bob', amountCents: 9900 });
      expect(tooMuch.statusCode).toBe(409);
      expect(tooMuch.json().code).toBe('INSUFFICIENT_FUNDS');

      // 9852 * 1.5% = 147.78 -> 148; 9852 + 148 = exactly 10000
      const exact = await transfer({ fromWalletId: 'wallet_alice', toWalletId: 'wallet_bob', amountCents: 9852 });
      expect(exact.statusCode).toBe(201);
      expect(exact.json().totalDebitCents).toBe(10000);
      expect(await balanceOf('wallet_alice')).toBe(0);
    });
  });

  describe('FX conversion (scaled integer rates)', () => {
    it('USD -> EUR credits the converted amount and balances each currency through the FX pools', async () => {
      const res = await transfer({ fromWalletId: 'wallet_alice', toWalletId: 'wallet_carol', amountCents: 5000 });

      expect(res.statusCode).toBe(201);
      expect(res.json()).toMatchObject({
        sourceCurrency: 'USD',
        destCurrency: 'EUR',
        fxRateScaled: 92000000,
        creditedAmountCents: 4600, // $50.00 * 0.92
      });
      expect(await balanceOf('wallet_alice')).toBe(5000);
      expect(await balanceOf('wallet_carol')).toBe(10000 + 4600);

      expect(await journalOf(res.json().transferId)).toEqual([
        { wallet_id: 'wallet_fx_pool_eur', currency: 'EUR', amount: '-4600' },
        { wallet_id: 'wallet_carol', currency: 'EUR', amount: '4600' },
        { wallet_id: 'wallet_alice', currency: 'USD', amount: '-5000' },
        { wallet_id: 'wallet_fx_pool_usd', currency: 'USD', amount: '5000' },
      ]);
      await expectEveryCurrencyZeroSum();
      await expectAllSyncedWalletsMatchLedger();
    });

    it('USD -> JPY (2 -> 0 decimals) with a fee rounds half-to-even', async () => {
      config.platformFeeBps = 100;

      const res = await transfer({ fromWalletId: 'wallet_alice', toWalletId: 'wallet_dave', amountCents: 100 });

      // $1.00 @ 151.25 = 151.25 JPY -> 151; fee 1% of 100 cents = 1 cent
      expect(res.json()).toMatchObject({ creditedAmountCents: 151, feeCents: 1, totalDebitCents: 101 });
      expect(await balanceOf('wallet_dave')).toBe(15000 + 151);
      expect(await ledgerSumFor('wallet_fee_treasury_usd')).toBe(1n);
      expect(await ledgerSumFor('wallet_fee_treasury_jpy')).toBe(0n);

      const steps = await api.inject({ method: 'GET', url: `/transfers/${res.json().transferId}` });
      expect(steps.json().sagaSteps.map((s: { step: string }) => s.step)).toEqual([
        'SENDER_DEBITED',
        'FEE_COLLECTED',
        'FX_CONVERTED',
        'RECIPIENT_CREDITED',
        'TRANSFER_COMPLETED',
      ]);
      await expectEveryCurrencyZeroSum();
    });

    it('JPY -> USD (0 -> 2 decimals) scales up exactly', async () => {
      const res = await transfer({ fromWalletId: 'wallet_dave', toWalletId: 'wallet_alice', amountCents: 1000 });
      expect(res.json().creditedAmountCents).toBe(660); // 1000 JPY * 0.0066 = $6.60
      expect(await balanceOf('wallet_dave')).toBe(14000);
      expect(await balanceOf('wallet_alice')).toBe(10660);
      await expectEveryCurrencyZeroSum();
    });

    it.each([
      [1, 2], // 1.5 cents -> 2 (tie to even)
      [3, 4], // 4.5 cents -> 4 (tie to even)
      [5, 8], // 7.5 cents -> 8 (tie to even)
    ])('applies banker\'s rounding to FX ties end-to-end: %s EUR cents @1.5 = %s USD cents', async (amount, expected) => {
      await setRate('EUR', 'USD', 150_000_000);
      const res = await transfer({ fromWalletId: 'wallet_carol', toWalletId: 'wallet_alice', amountCents: amount });
      expect(res.json().creditedAmountCents).toBe(expected);
      await expectEveryCurrencyZeroSum();
    });

    it('is deterministic: identical transfers produce identical quotes', async () => {
      config.platformFeeBps = 37;
      const quotes = [];
      for (let i = 0; i < 3; i++) {
        const res = await transfer({ fromWalletId: 'wallet_alice', toWalletId: 'wallet_dave', amountCents: 1234 });
        const { transferId: _id, senderRemainingBalanceCents: _b, senderVersion: _v, ...quote } = res.json();
        quotes.push(quote);
      }
      expect(quotes[1]).toEqual(quotes[0]);
      expect(quotes[2]).toEqual(quotes[0]);
    });

    it('rejects a currency assertion that does not match the sender wallet (422)', async () => {
      const res = await transfer({
        fromWalletId: 'wallet_alice',
        toWalletId: 'wallet_carol',
        amountCents: 100,
        currency: 'EUR',
      });
      expect(res.statusCode).toBe(422);
      expect(res.json().code).toBe('CURRENCY_MISMATCH');
      expect(await balanceOf('wallet_alice')).toBe(10000);
    });

    it('rejects a pair with no FX rate (422) without writing anything', async () => {
      await query(`DELETE FROM fx_rates WHERE base_currency = 'USD' AND quote_currency = 'EUR'`);
      const res = await transfer({ fromWalletId: 'wallet_alice', toWalletId: 'wallet_carol', amountCents: 100 });
      expect(res.statusCode).toBe(422);
      expect(res.json().code).toBe('FX_RATE_UNAVAILABLE');
      expect((await query(`SELECT 1 FROM transfers`)).rows).toHaveLength(0);
    });

    it('rejects a conversion that would credit zero (422 AMOUNT_TOO_SMALL)', async () => {
      await setRate('JPY', 'USD', 400_000); // 0.004: 1 JPY -> 0.4 cent -> 0
      const res = await transfer({ fromWalletId: 'wallet_dave', toWalletId: 'wallet_alice', amountCents: 1 });
      expect(res.statusCode).toBe(422);
      expect(res.json().code).toBe('AMOUNT_TOO_SMALL');
      expect(await balanceOf('wallet_dave')).toBe(15000);
    });

    it('rejects amounts beyond the safe integer range at validation (400)', async () => {
      const res = await transfer({
        fromWalletId: 'wallet_alice',
        toWalletId: 'wallet_bob',
        amountCents: Number.MAX_SAFE_INTEGER + 2,
      });
      expect(res.statusCode).toBe(400);
    });
  });

  describe('AML escrow across currencies', () => {
    it('evaluates the threshold on the USD equivalent (EUR 460.00 = $500.02 is held)', async () => {
      await grantFunds('wallet_carol', 100000);

      // EUR 459.90 * 1.087 = $499.91 -> instant
      const under = await transfer({ fromWalletId: 'wallet_carol', toWalletId: 'wallet_alice', amountCents: 45990 });
      expect(under.statusCode).toBe(201);

      // EUR 460.00 * 1.087 = $500.02 -> exceeds $500.00
      const over = await transfer({ fromWalletId: 'wallet_carol', toWalletId: 'wallet_alice', amountCents: 46000 });
      expect(over.statusCode).toBe(202);
      expect(await balanceOf('wallet_escrow_eur')).toBe(46000); // held in the SOURCE currency escrow
      expect(await balanceOf('wallet_escrow')).toBe(0);
    });

    it('APPROVE settles using the quote locked at initiation, even if the rate moved', async () => {
      config.platformFeeBps = 200; // 2%
      await grantFunds('wallet_carol', 100000);

      const held = await transfer({ fromWalletId: 'wallet_carol', toWalletId: 'wallet_alice', amountCents: 50000 });
      expect(held.statusCode).toBe(202);
      const quote = held.json();
      expect(quote).toMatchObject({ feeCents: 1000, totalDebitCents: 51000, creditedAmountCents: 54350 });
      expect(await balanceOf('wallet_escrow_eur')).toBe(51000); // principal + fee held

      await setRate('EUR', 'USD', 200_000_000); // rate doubles during review
      config.platformFeeBps = 0; // fee policy changes during review

      const approved = await api.inject({
        method: 'POST',
        url: `/transfers/${quote.transferId}/review`,
        payload: { decision: 'APPROVE' },
      });
      expect(approved.statusCode).toBe(200);
      expect(approved.json()).toMatchObject({ creditedAmountCents: 54350, feeCents: 1000 });

      expect(await balanceOf('wallet_alice')).toBe(10000 + 54350);
      expect(await balanceOf('wallet_escrow_eur')).toBe(0);
      expect(await ledgerSumFor('wallet_fee_treasury_eur')).toBe(1000n);
      expect(await journalOf(`release_${quote.transferId}`)).toEqual([
        { wallet_id: 'wallet_escrow_eur', currency: 'EUR', amount: '-51000' },
        { wallet_id: 'wallet_fee_treasury_eur', currency: 'EUR', amount: '1000' },
        { wallet_id: 'wallet_fx_pool_eur', currency: 'EUR', amount: '50000' },
        { wallet_id: 'wallet_fx_pool_usd', currency: 'USD', amount: '-54350' },
        { wallet_id: 'wallet_alice', currency: 'USD', amount: '54350' },
      ]);
      await expectEveryCurrencyZeroSum();
      await expectAllSyncedWalletsMatchLedger();
    });

    it('REJECT compensates in full, fee included, and collects no fee', async () => {
      config.platformFeeBps = 200;
      await grantFunds('wallet_carol', 100000);
      const carolBefore = await balanceOf('wallet_carol');

      const held = await transfer({ fromWalletId: 'wallet_carol', toWalletId: 'wallet_alice', amountCents: 50000 });
      const rejected = await api.inject({
        method: 'POST',
        url: `/transfers/${held.json().transferId}/review`,
        payload: { decision: 'REJECT' },
      });

      expect(rejected.statusCode).toBe(200);
      expect(await balanceOf('wallet_carol')).toBe(carolBefore);
      expect(await balanceOf('wallet_alice')).toBe(10000);
      expect(await ledgerSumFor('wallet_fee_treasury_eur')).toBe(0n);
      await expectEveryCurrencyZeroSum();
      await expectAllSyncedWalletsMatchLedger();
    });

    it('locks the per-currency escrow inside the global lexicographic lock order', async () => {
      await grantFunds('wallet_carol', 100000);
      const lockedIds: unknown[] = [];
      const originalConnect = pool.connect.bind(pool);
      vi.spyOn(pool, 'connect').mockImplementation(async () => {
        const client = await originalConnect();
        const originalQuery = client.query.bind(client) as (sql: string, params?: unknown[]) => Promise<unknown>;
        (client as unknown as { query: typeof originalQuery }).query = async (sql, params) => {
          if (/FROM wallets/.test(sql) && /FOR UPDATE/.test(sql)) lockedIds.push(params?.[0]);
          return originalQuery(sql, params);
        };
        return client;
      });

      const held = await transfer({ fromWalletId: 'wallet_carol', toWalletId: 'wallet_alice', amountCents: 60000 });
      expect(held.statusCode).toBe(202);
      // Sender is carol, but locks are taken in global order, escrow included.
      expect(lockedIds).toEqual(['wallet_alice', 'wallet_carol', 'wallet_escrow_eur']);
    });

    it('completes opposing held and instant cross-currency transfers concurrently', async () => {
      await grantFunds('wallet_carol', 100000);
      await grantFunds('wallet_alice', 100000);
      const results = await Promise.all([
        transfer({ fromWalletId: 'wallet_carol', toWalletId: 'wallet_alice', amountCents: 60000 }),
        transfer({ fromWalletId: 'wallet_alice', toWalletId: 'wallet_carol', amountCents: 60000 }),
        ...Array.from({ length: 6 }, (_, i) =>
          transfer({
            fromWalletId: i % 2 ? 'wallet_alice' : 'wallet_carol',
            toWalletId: i % 2 ? 'wallet_carol' : 'wallet_alice',
            amountCents: 300,
          })
        ),
      ]);
      expect(results.map((r) => r.statusCode).sort()).toEqual([201, 201, 201, 201, 201, 201, 202, 202]);
      await expectEveryCurrencyZeroSum();
      await expectAllSyncedWalletsMatchLedger();
    });
  });

  describe('system accounts are not user-facing', () => {
    it.each([
      ['withdraw from escrow (would steal AML-held funds)', 'withdraw', 'wallet_escrow'],
      ['withdraw from a fee treasury', 'withdraw', 'wallet_fee_treasury_eur'],
      ['withdraw from clearing', 'withdraw', 'wallet_treasury'],
    ])('rejects %s with 422', async (_label, _kind, walletId) => {
      const res = await api.inject({
        method: 'POST',
        url: '/wallets/withdraw',
        payload: { walletId, amountCents: 1, bankAccountRef: 'attacker' },
      });
      expect(res.statusCode).toBe(422);
      expect(res.json().code).toBe('SYSTEM_ACCOUNT_NOT_ALLOWED');
    });

    it.each([
      ['into escrow', 'wallet_alice', 'wallet_escrow'],
      ['into a fee treasury', 'wallet_alice', 'wallet_fee_treasury_usd'],
      ['out of an FX pool', 'wallet_fx_pool_usd', 'wallet_alice'],
      ['out of clearing', 'wallet_treasury', 'wallet_bob'],
    ])('rejects a transfer %s with 422 and writes nothing', async (_label, from, to) => {
      const res = await transfer({ fromWalletId: from, toWalletId: to, amountCents: 1 });
      expect(res.statusCode).toBe(422);
      expect(res.json().code).toBe('SYSTEM_ACCOUNT_NOT_ALLOWED');
      expect((await query(`SELECT 1 FROM transfers`)).rows).toHaveLength(0);
    });

    it('keeps AML-held escrow funds safe until review', async () => {
      await grantFunds('wallet_alice', 100000);
      const held = await transfer({ fromWalletId: 'wallet_alice', toWalletId: 'wallet_bob', amountCents: 60000 });
      expect(held.statusCode).toBe(202);

      const steal = await api.inject({
        method: 'POST',
        url: '/wallets/withdraw',
        payload: { walletId: 'wallet_escrow', amountCents: 60000, bankAccountRef: 'attacker' },
      });
      expect(steal.statusCode).toBe(422);
      expect(await balanceOf('wallet_escrow')).toBe(60000);

      const approved = await api.inject({
        method: 'POST',
        url: `/transfers/${held.json().transferId}/review`,
        payload: { decision: 'APPROVE' },
      });
      expect(approved.statusCode).toBe(200);
      expect(await balanceOf('wallet_bob')).toBe(10000 + 60000);
    });
  });

  describe('withdrawals and refunds per currency', () => {
    it('routes a EUR withdrawal and its failure refund through the EUR clearing account', async () => {
      const withdraw = await api.inject({
        method: 'POST',
        url: '/wallets/withdraw',
        payload: { walletId: 'wallet_carol', amountCents: 2500, bankAccountRef: 'iban_de_1' },
      });
      expect(withdraw.statusCode).toBe(201);
      expect(withdraw.json().currency).toBe('EUR');
      const { payoutId } = withdraw.json();

      expect(await journalOf(payoutId)).toEqual([
        { wallet_id: 'wallet_carol', currency: 'EUR', amount: '-2500' },
        { wallet_id: 'wallet_treasury_eur', currency: 'EUR', amount: '2500' },
      ]);

      await api.inject({
        method: 'POST',
        url: '/payouts/webhook',
        payload: { eventId: 'evt_eur_fail', payoutId, eventType: 'payout.failed' },
      });
      expect(await journalOf(`rev_${payoutId}`)).toEqual([
        { wallet_id: 'wallet_treasury_eur', currency: 'EUR', amount: '-2500' },
        { wallet_id: 'wallet_carol', currency: 'EUR', amount: '2500' },
      ]);
      expect(await balanceOf('wallet_carol')).toBe(10000);
      await expectEveryCurrencyZeroSum();
    });
  });

  describe('invariants under concurrent mixed-currency load', () => {
    it('keeps every currency zero-sum and every wallet in sync after a concurrent storm', async () => {
      config.platformFeeBps = 75;
      const pairs: Array<[string, string]> = [
        ['wallet_alice', 'wallet_carol'],
        ['wallet_carol', 'wallet_alice'],
        ['wallet_alice', 'wallet_dave'],
        ['wallet_dave', 'wallet_alice'],
        ['wallet_carol', 'wallet_dave'],
        ['wallet_dave', 'wallet_carol'],
        ['wallet_alice', 'wallet_bob'],
        ['wallet_bob', 'wallet_alice'],
      ];
      const results = await Promise.all(
        Array.from({ length: 32 }, (_, i) => {
          const [from, to] = pairs[i % pairs.length];
          return transfer({ fromWalletId: from, toWalletId: to, amountCents: 97 + i * 13 });
        })
      );
      expect(results.every((r) => r.statusCode === 201)).toBe(true);

      await expectEveryCurrencyZeroSum();
      await expectAllSyncedWalletsMatchLedger();

      // Fees collected == sum of fees quoted, per source currency (no remainder lost).
      const quotedFees = new Map<string, number>();
      for (const r of results) {
        const body = r.json();
        quotedFees.set(body.sourceCurrency, (quotedFees.get(body.sourceCurrency) ?? 0) + body.feeCents);
      }
      for (const [ccy, fees] of quotedFees) {
        const code = assertSupportedCurrency(ccy);
        expect(await ledgerSumFor(systemAccountsFor(code).feeTreasury)).toBe(BigInt(fees));
      }
    });
  });
});
