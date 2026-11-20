import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { FastifyInstance } from 'fastify';
import { buildApp } from '../src/server';
import { actAs } from './helpers/auth';
import { closeDb, query, resetAndSeedDatabase } from '../src/db';
import { closeRedis, flushRedis } from '../src/redis';
import { walletService } from '../src/services/WalletService';

describe('Multi-Currency Wallet & Double-Entry Ledger Engine', () => {
  let app: FastifyInstance;
  let api: ReturnType<typeof actAs>;

  beforeAll(async () => {
    app = buildApp();
    await app.ready();
    api = actAs(app);
  });

  beforeEach(async () => {
    await resetAndSeedDatabase();
    await flushRedis();
  });

  afterAll(async () => {
    await app.close();
    await closeDb();
    await closeRedis();
  });

  it('processes a single sequential transfer between Alice and Bob', async () => {
    const res = await api.inject({
      method: 'POST',
      url: '/wallets/transfer',
      payload: {
        fromWalletId: 'wallet_alice',
        toWalletId: 'wallet_bob',
        amountCents: 2500,
      },
    });

    expect(res.statusCode).toBe(201);
    const alice = await walletService.getWallet('wallet_alice');
    const bob = await walletService.getWallet('wallet_bob');

    expect(alice?.balanceCents).toBe(7500);
    expect(bob?.balanceCents).toBe(12500);
  });

  it('prevents double-spending when 20 concurrent $25.00 withdrawals hit a $100.00 wallet', async () => {
    const attempts = 20;

    const responses = await Promise.all(
      Array.from({ length: attempts }, (_, idx) =>
        api.inject({
          method: 'POST',
          url: '/wallets/withdraw',
          payload: {
            walletId: 'wallet_alice',
            amountCents: 2500,
            bankAccountRef: `bank_acct_${idx + 1}`,
          },
        })
      )
    );

    const succeeded = responses.filter((r) => r.statusCode === 201);
    const rejected = responses.filter((r) => r.statusCode === 409);

    expect(succeeded.length).toBe(4);
    expect(rejected.length).toBe(16);
    expect(rejected.every((r) => r.json().code === 'INSUFFICIENT_FUNDS')).toBe(true);

    const alice = await walletService.getWallet('wallet_alice');
    expect(alice?.balanceCents).toBe(0);
    // Exactly one version bump per successful debit (seed version is 1)
    expect(alice?.version).toBe(5);

    // Only the successful withdrawals left a payout and a balanced ledger pair behind
    const payouts = await query<{ n: string }>(
      `SELECT COUNT(*)::text AS n FROM payouts WHERE wallet_id = 'wallet_alice'`
    );
    expect(Number(payouts.rows[0].n)).toBe(4);

    const ledgerSum = await query<{ total: string }>(
      `SELECT COALESCE(SUM(signed_amount_cents), 0)::text AS total FROM ledger_entries`
    );
    expect(Number(ledgerSum.rows[0].total)).toBe(0);
  });

  it('preserves money conservation and atomicity across concurrent bidirectional P2P transfers and failed recipients', async () => {
    // 1. Transfer to a non-existent recipient wallet MUST roll back sender debit and ledger entry
    const invalidTransfer = await api.inject({
      method: 'POST',
      url: '/wallets/transfer',
      payload: {
        fromWalletId: 'wallet_alice',
        toWalletId: 'wallet_non_existent',
        amountCents: 3000,
      },
    });
    expect(invalidTransfer.statusCode).toBe(404);

    const aliceAfterFailed = await walletService.getWallet('wallet_alice');
    expect(aliceAfterFailed?.balanceCents).toBe(10000);

    // 2. 10 concurrent transfers Alice -> Bob ($10) and 10 concurrent transfers Bob -> Alice ($15)
    const requests = [
      ...Array.from({ length: 10 }, (_, i) =>
        api.inject({
          method: 'POST',
          url: '/wallets/transfer',
          payload: {
            fromWalletId: 'wallet_alice',
            toWalletId: 'wallet_bob',
            amountCents: 500,
            referenceId: `a_to_b_${i}`,
          },
        })
      ),
      ...Array.from({ length: 10 }, (_, i) =>
        api.inject({
          method: 'POST',
          url: '/wallets/transfer',
          payload: {
            fromWalletId: 'wallet_bob',
            toWalletId: 'wallet_alice',
            amountCents: 300,
            referenceId: `b_to_a_${i}`,
          },
        })
      ),
    ];

    const results = await Promise.all(requests);
    expect(results.every((r) => r.statusCode === 201)).toBe(true);

    const alice = await walletService.getWallet('wallet_alice');
    const bob = await walletService.getWallet('wallet_bob');

    // Alice sent 10 * 500 (-5000) and received 10 * 300 (+3000) => 10000 - 2000 = 8000
    // Bob sent 10 * 300 (-3000) and received 10 * 500 (+5000) => 10000 + 2000 = 12000
    expect(alice?.balanceCents).toBe(8000);
    expect(bob?.balanceCents).toBe(12000);
    expect((alice?.balanceCents ?? 0) + (bob?.balanceCents ?? 0)).toBe(20000);

    // Global double-entry ledger invariant: sum of all signed_amount_cents MUST equal 0
    const ledgerSum = await query<{ total: string }>(
      `SELECT COALESCE(SUM(signed_amount_cents), 0)::text AS total FROM ledger_entries`
    );
    expect(Number(ledgerSum.rows[0].total)).toBe(0);
  });

  it('prevents duplicate payout.failed webhooks from double-refunding a user wallet', async () => {
    const withdrawRes = await api.inject({
      method: 'POST',
      url: '/wallets/withdraw',
      payload: {
        walletId: 'wallet_alice',
        amountCents: 4000,
        bankAccountRef: 'bank_dup_fail',
      },
    });
    expect(withdrawRes.statusCode).toBe(201);
    const { payoutId } = withdrawRes.json();

    // Bank webhook delivers payout.failed 5 times concurrently during a retry storm
    const webhookCalls = await Promise.all(
      Array.from({ length: 5 }, (_, idx) =>
        api.inject({
          method: 'POST',
          url: '/payouts/webhook',
          payload: {
            eventId: idx === 0 ? 'evt_fail_1' : `evt_fail_retry_${idx}`,
            payoutId,
            eventType: 'payout.failed',
            failureReason: 'AC01_INCORRECT_ACCOUNT_NUMBER',
          },
        })
      )
    );

    expect(webhookCalls.every((r) => r.statusCode === 200)).toBe(true);

    // Wallet MUST be refunded only once (back to 10000, NOT 10000 + 4 * 4000 = 26000!)
    const alice = await walletService.getWallet('wallet_alice');
    expect(alice?.balanceCents).toBe(10000);
  });

  it('flags out-of-order payout.settled webhook arriving after payout.failed as RECONCILIATION_REQUIRED', async () => {
    const withdrawRes = await api.inject({
      method: 'POST',
      url: '/wallets/withdraw',
      payload: {
        walletId: 'wallet_bob',
        amountCents: 6000,
        bankAccountRef: 'bank_late_settle',
      },
    });
    const { payoutId } = withdrawRes.json();

    // 1. Bank sends payout.failed first; Bob's $60.00 is refunded back to $100.00
    const failWebhook = await api.inject({
      method: 'POST',
      url: '/payouts/webhook',
      payload: {
        eventId: 'evt_bob_fail_first',
        payoutId,
        eventType: 'payout.failed',
      },
    });
    expect(failWebhook.statusCode).toBe(200);
    expect(failWebhook.json().payoutStatus).toBe('REVERSED');

    // 2. Bob spends $80.00 of his refunded balance (leaving $20.00)
    await api.inject({
      method: 'POST',
      url: '/wallets/transfer',
      payload: {
        fromWalletId: 'wallet_bob',
        toWalletId: 'wallet_alice',
        amountCents: 8000,
      },
    });

    // 3. Delayed/out-of-order payout.settled webhook arrives from bank for the already-reversed payout!
    const lateSettledWebhook = await api.inject({
      method: 'POST',
      url: '/payouts/webhook',
      payload: {
        eventId: 'evt_bob_settled_late',
        payoutId,
        eventType: 'payout.settled',
      },
    });

    expect(lateSettledWebhook.statusCode).toBe(200);
    expect(lateSettledWebhook.json().outcome).toBe('RECONCILIATION_REQUIRED');
    expect(lateSettledWebhook.json().payoutStatus).toBe('RECONCILIATION_REQUIRED');

    const payoutDb = await query<{ status: string }>(
      `SELECT status FROM payouts WHERE payout_id = $1`,
      [payoutId]
    );
    expect(payoutDb.rows[0].status).toBe('RECONCILIATION_REQUIRED');
  });
});
