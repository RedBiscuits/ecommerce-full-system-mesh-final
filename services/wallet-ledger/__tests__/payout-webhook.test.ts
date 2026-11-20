import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import type { FastifyInstance } from 'fastify';
import { buildApp } from '../src/server';
import { actAs } from './helpers/auth';
import { closeDb, query, resetAndSeedDatabase } from '../src/db';
import { closeRedis, flushRedis } from '../src/redis';
import { walletService } from '../src/services/WalletService';

const SEED_BALANCE = 10000;

describe('Payout webhook reconciliation (withdrawal saga completion)', () => {
  let app: FastifyInstance;
  let api: ReturnType<typeof actAs>;

  const webhook = (payload: Record<string, unknown>) =>
    api.inject({ method: 'POST', url: '/payouts/webhook', payload });

  async function createPayout(amountCents: number, walletId = 'wallet_alice'): Promise<string> {
    const res = await api.inject({
      method: 'POST',
      url: '/wallets/withdraw',
      payload: { walletId, amountCents, bankAccountRef: 'bank_webhook_tests' },
    });
    expect(res.statusCode).toBe(201);
    return res.json().payoutId;
  }

  async function payoutStatus(payoutId: string): Promise<string> {
    const res = await query<{ status: string }>(`SELECT status FROM payouts WHERE payout_id = $1`, [
      payoutId,
    ]);
    return res.rows[0].status;
  }

  async function balanceOf(walletId: string): Promise<number> {
    return (await walletService.getWallet(walletId))?.balanceCents ?? NaN;
  }

  async function ledgerSyncHolds(walletId: string): Promise<void> {
    const sum = await query<{ total: string }>(
      `SELECT COALESCE(SUM(signed_amount_cents), 0)::text AS total FROM ledger_entries WHERE wallet_id = $1`,
      [walletId]
    );
    expect(await balanceOf(walletId)).toBe(Number(sum.rows[0].total));
  }

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

  it('payout.settled completes the withdrawal: PROCESSING -> SETTLED, no money moves', async () => {
    const payoutId = await createPayout(3000);

    const res = await webhook({ eventId: 'evt_settle', payoutId, eventType: 'payout.settled' });

    expect(res.statusCode).toBe(200);
    expect(res.json()).toMatchObject({ payoutStatus: 'SETTLED', outcome: 'SETTLED' });
    expect(await balanceOf('wallet_alice')).toBe(SEED_BALANCE - 3000);
    await ledgerSyncHolds('wallet_alice');
  });

  it('payout.failed compensates the withdrawal: refund + REVERSED + balanced ledger pair', async () => {
    const payoutId = await createPayout(3000);

    const res = await webhook({
      eventId: 'evt_fail',
      payoutId,
      eventType: 'payout.failed',
      failureReason: 'AC01',
    });

    expect(res.json()).toMatchObject({
      payoutStatus: 'REVERSED',
      outcome: 'REVERSED_AND_REFUNDED',
    });
    expect(await balanceOf('wallet_alice')).toBe(SEED_BALANCE);

    const reversal = await query<{ wallet_id: string; amount: string }>(
      `SELECT wallet_id, signed_amount_cents::text AS amount
       FROM ledger_entries WHERE transaction_ref = $1 ORDER BY signed_amount_cents`,
      [`rev_${payoutId}`]
    );
    expect(reversal.rows).toEqual([
      { wallet_id: 'wallet_treasury', amount: '-3000' },
      { wallet_id: 'wallet_alice', amount: '3000' },
    ]);
    await ledgerSyncHolds('wallet_alice');
  });

  it('replaying the SAME eventId is acknowledged but ignored', async () => {
    const payoutId = await createPayout(2000);
    await webhook({ eventId: 'evt_once', payoutId, eventType: 'payout.failed' });

    const replay = await webhook({ eventId: 'evt_once', payoutId, eventType: 'payout.failed' });

    expect(replay.statusCode).toBe(200);
    expect(replay.json()).toMatchObject({ payoutStatus: 'REVERSED', outcome: 'DUPLICATE_IGNORED' });
    expect(await balanceOf('wallet_alice')).toBe(SEED_BALANCE); // refunded exactly once
  });

  it('retries with DIFFERENT eventIds never refund twice (state machine, not event dedupe)', async () => {
    const payoutId = await createPayout(2000);
    await webhook({ eventId: 'evt_f1', payoutId, eventType: 'payout.failed' });

    const retry = await webhook({ eventId: 'evt_f2', payoutId, eventType: 'payout.failed' });

    expect(retry.json().outcome).toBe('DUPLICATE_IGNORED');
    expect(await balanceOf('wallet_alice')).toBe(SEED_BALANCE);
    expect(await payoutStatus(payoutId)).toBe('REVERSED');
  });

  it('duplicate payout.settled with a new eventId is ignored', async () => {
    const payoutId = await createPayout(2000);
    await webhook({ eventId: 'evt_s1', payoutId, eventType: 'payout.settled' });

    const dup = await webhook({ eventId: 'evt_s2', payoutId, eventType: 'payout.settled' });
    expect(dup.json().outcome).toBe('DUPLICATE_IGNORED');
    expect(await payoutStatus(payoutId)).toBe('SETTLED');
  });

  it('payout.failed AFTER settled flags reconciliation and does NOT refund', async () => {
    const payoutId = await createPayout(2000);
    await webhook({ eventId: 'evt_s', payoutId, eventType: 'payout.settled' });

    const conflicting = await webhook({ eventId: 'evt_late_f', payoutId, eventType: 'payout.failed' });

    expect(conflicting.json()).toMatchObject({
      payoutStatus: 'RECONCILIATION_REQUIRED',
      outcome: 'RECONCILIATION_REQUIRED',
    });
    expect(await balanceOf('wallet_alice')).toBe(SEED_BALANCE - 2000); // money stayed out
    expect(await payoutStatus(payoutId)).toBe('RECONCILIATION_REQUIRED');
  });

  it('payout.settled AFTER failed flags reconciliation and does NOT claw back the refund', async () => {
    const payoutId = await createPayout(2000);
    await webhook({ eventId: 'evt_f', payoutId, eventType: 'payout.failed' });

    const late = await webhook({ eventId: 'evt_late_s', payoutId, eventType: 'payout.settled' });

    expect(late.json()).toMatchObject({
      payoutStatus: 'RECONCILIATION_REQUIRED',
      outcome: 'RECONCILIATION_REQUIRED',
    });
    expect(await balanceOf('wallet_alice')).toBe(SEED_BALANCE); // refund untouched

    const note = await query<{ reconciliation_note: string }>(
      `SELECT reconciliation_note FROM payouts WHERE payout_id = $1`,
      [payoutId]
    );
    expect(note.rows[0].reconciliation_note).toContain('REVERSED');
  });

  it('a payout parked in RECONCILIATION_REQUIRED ignores all further events', async () => {
    const payoutId = await createPayout(2000);
    await webhook({ eventId: 'evt_a', payoutId, eventType: 'payout.failed' });
    await webhook({ eventId: 'evt_b', payoutId, eventType: 'payout.settled' }); // -> RECONCILIATION_REQUIRED

    for (const [eventId, eventType] of [
      ['evt_c', 'payout.settled'],
      ['evt_d', 'payout.failed'],
    ] as const) {
      const res = await webhook({ eventId, payoutId, eventType });
      expect(res.json().outcome).toBe('DUPLICATE_IGNORED');
    }
    expect(await payoutStatus(payoutId)).toBe('RECONCILIATION_REQUIRED');
    expect(await balanceOf('wallet_alice')).toBe(SEED_BALANCE);
  });

  it('returns 404 for an unknown payout', async () => {
    const res = await webhook({ eventId: 'evt_x', payoutId: 'po_missing', eventType: 'payout.settled' });
    expect(res.statusCode).toBe(404);
    expect(res.json().code).toBe('PAYOUT_NOT_FOUND');
  });

  it('returns 400 for an unknown event type', async () => {
    const payoutId = await createPayout(1000);
    const res = await webhook({ eventId: 'evt_bad', payoutId, eventType: 'payout.exploded' });
    expect(res.statusCode).toBe(400);
  });

  it('records every processed event with its outcome for audit', async () => {
    const payoutId = await createPayout(2000);
    await webhook({ eventId: 'evt_audit_1', payoutId, eventType: 'payout.failed' });
    await webhook({ eventId: 'evt_audit_2', payoutId, eventType: 'payout.failed' });

    const events = await query<{ event_id: string; outcome: string }>(
      `SELECT event_id, outcome FROM payout_webhook_events WHERE payout_id = $1 ORDER BY event_id`,
      [payoutId]
    );
    expect(events.rows).toEqual([
      { event_id: 'evt_audit_1', outcome: 'REVERSED_AND_REFUNDED' },
      { event_id: 'evt_audit_2', outcome: 'DUPLICATE_IGNORED' },
    ]);
  });

  it('survives a concurrent storm of conflicting settled/failed events with money moving at most once', async () => {
    const payoutId = await createPayout(4000);

    const responses = await Promise.all(
      Array.from({ length: 6 }, (_, i) =>
        webhook({
          eventId: `evt_storm_${i}`,
          payoutId,
          eventType: i % 2 === 0 ? 'payout.failed' : 'payout.settled',
        })
      )
    );
    expect(responses.every((r) => r.statusCode === 200)).toBe(true);

    const balance = await balanceOf('wallet_alice');
    const refundPairs = await query(
      `SELECT 1 FROM ledger_entries WHERE transaction_ref = $1`,
      [`rev_${payoutId}`]
    );
    const status = await payoutStatus(payoutId);

    if (refundPairs.rows.length > 0) {
      // A failed event won the PROCESSING transition: refunded exactly once.
      expect(refundPairs.rows).toHaveLength(2);
      expect(balance).toBe(SEED_BALANCE);
      expect(['REVERSED', 'RECONCILIATION_REQUIRED']).toContain(status);
    } else {
      // A settled event won: no refund ever happened.
      expect(balance).toBe(SEED_BALANCE - 4000);
      expect(['SETTLED', 'RECONCILIATION_REQUIRED']).toContain(status);
    }
    await ledgerSyncHolds('wallet_alice');
  });
});
