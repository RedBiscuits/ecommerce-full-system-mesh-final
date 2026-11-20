import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { FastifyInstance } from 'fastify';
import { buildApp } from '../src/server';
import { query, resetAndSeedDatabase } from '../src/db';
import { closeRedis, flushRedis } from '../src/redis';
import { autoRechargeService } from '../src/services/AutoRechargeService';
import { walletLockMetrics } from '../src/services/OrgWallet';
import { expectWalletInvariants, getAvailableTotal, setAvailableTotal } from './helpers/wallet';

async function getOrgState(orgId: string) {
  const cachedAvailable = await getAvailableTotal(orgId);
  const lots = await query<{ total: string }>(
    `SELECT COALESCE(SUM(remaining_credits), 0)::text AS total
     FROM credit_grant_balances WHERE org_id = $1 AND lifecycle_status = 'ACTIVE'`,
    [orgId]
  );
  const events = await query<{ count: string; charged: string }>(
    `SELECT COUNT(*)::text AS count, COALESCE(SUM(credits_charged), 0)::text AS charged
     FROM usage_events WHERE org_id = $1`,
    [orgId]
  );
  const recharges = await query<{ count: string; added: string }>(
    `SELECT COUNT(*)::text AS count, COALESCE(SUM(credits_added), 0)::text AS added
     FROM auto_recharge_orders WHERE org_id = $1`,
    [orgId]
  );
  const autoGrants = await query<{ count: string }>(
    `SELECT COUNT(*)::text AS count FROM credit_grants
     WHERE org_id = $1 AND source = 'AUTO_RECHARGE'`,
    [orgId]
  );
  return {
    cachedAvailable,
    activeLotTotal: Number(lots.rows[0].total),
    usageEventCount: Number(events.rows[0].count),
    totalCharged: Number(events.rows[0].charged),
    rechargeOrderCount: Number(recharges.rows[0].count),
    totalRecharged: Number(recharges.rows[0].added),
    autoRechargeGrantCount: Number(autoGrants.rows[0].count),
  };
}

describe('Transactional concurrency safety', () => {
  let app: FastifyInstance;

  const deduct = (payload: Record<string, unknown>, headers?: Record<string, string>) =>
    app.inject({ method: 'POST', url: '/credits/deduct', payload, headers });

  beforeAll(async () => {
    app = buildApp();
    await app.ready();
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
    await closeRedis();
  });

  describe('deductUsage', () => {
    it('applies parallel deductions without lost updates and keeps lots in sync with the balance', async () => {
      const responses = await Promise.all(
        Array.from({ length: 10 }, (_, i) =>
          deduct({ orgId: 'org_acme', requestId: `req_par_${i}`, modelName: 'gpt-4o', credits: 100 })
        )
      );

      expect(responses.every((r) => r.statusCode === 200)).toBe(true);

      const state = await getOrgState('org_acme');
      expect(state.cachedAvailable).toBe(500);
      expect(state.activeLotTotal).toBe(500);
      expect(state.usageEventCount).toBe(10);
      expect(state.totalCharged).toBe(1000);

      // FIFO: the promo lot (earliest expiry) was fully drained before the invoice lot was touched.
      const lots = await query<{ grant_id: string; remaining_credits: string; status: string }>(
        `SELECT grant_id, remaining_credits, status FROM credit_grant_balances
         WHERE org_id = 'org_acme' ORDER BY grant_id`
      );
      expect(lots.rows.map((r) => [r.grant_id, Number(r.remaining_credits), r.status])).toEqual([
        ['grant_inv_01', 500, 'ACTIVE'],
        ['grant_promo_01', 0, 'DEPLETED'],
      ]);

      const allocated = await query<{ total: string }>(
        `SELECT SUM(credits_deducted)::text AS total FROM credit_lot_allocations`
      );
      expect(Number(allocated.rows[0].total)).toBe(1000);
    });

    it('never overdraws when parallel deductions exceed the spendable balance', async () => {
      const responses = await Promise.all(
        Array.from({ length: 5 }, (_, i) =>
          deduct({ orgId: 'org_acme', requestId: `req_over_${i}`, modelName: 'gpt-4o', credits: 400 })
        )
      );

      const ok = responses.filter((r) => r.statusCode === 200);
      const rejected = responses.filter((r) => r.statusCode === 402);
      expect(ok).toHaveLength(3);
      expect(rejected).toHaveLength(2);
      expect(rejected.every((r) => r.json().code === 'INSUFFICIENT_CREDITS')).toBe(true);

      const state = await getOrgState('org_acme');
      expect(state.cachedAvailable).toBe(300);
      expect(state.activeLotTotal).toBe(300);
      expect(state.usageEventCount).toBe(3);
    });

    it('charges once for parallel requests sharing an idempotency key and replays the original result', async () => {
      const payload = {
        orgId: 'org_acme',
        requestId: 'req_idem_01',
        modelName: 'gpt-4o',
        credits: 700,
      };
      const responses = await Promise.all(
        Array.from({ length: 5 }, () => deduct(payload, { 'idempotency-key': 'idem_parallel_01' }))
      );

      expect(responses.every((r) => r.statusCode === 200)).toBe(true);
      const bodies = responses.map((r) => r.json());
      expect(new Set(bodies.map((b) => b.usageId)).size).toBe(1);
      expect(bodies.filter((b) => b.idempotentReplay === false)).toHaveLength(1);
      expect(responses.filter((r) => r.headers['idempotent-replayed'] === 'true')).toHaveLength(4);

      // Every replay is identical to the original response apart from the replay flag.
      for (const body of bodies) {
        expect({ ...body, idempotentReplay: undefined }).toEqual({
          ...bodies[0],
          idempotentReplay: undefined,
        });
      }
      expect(bodies[0].remainingSpendableCredits).toBe(800);
      expect(bodies[0].allocations).toEqual([
        { grantId: 'grant_promo_01', creditsDeducted: 500 },
        { grantId: 'grant_inv_01', creditsDeducted: 200 },
      ]);

      const state = await getOrgState('org_acme');
      expect(state.cachedAvailable).toBe(800);
      expect(state.usageEventCount).toBe(1);
    });

    it('replays the original balance snapshot even after later balance changes', async () => {
      const payload = {
        orgId: 'org_acme',
        requestId: 'req_idem_02',
        modelName: 'gpt-4o',
        credits: 200,
        idempotencyKey: 'idem_snapshot_01',
      };
      const first = await deduct(payload);
      await deduct({ orgId: 'org_acme', requestId: 'req_other', modelName: 'gpt-4o', credits: 300 });
      const replay = await deduct(payload);

      expect(replay.statusCode).toBe(200);
      expect(replay.json()).toEqual({ ...first.json(), idempotentReplay: true });
      expect(replay.json().remainingSpendableCredits).toBe(1300);
      expect((await getOrgState('org_acme')).cachedAvailable).toBe(1000);
    });

    it('rejects reuse of an idempotency key with a different payload (409) without charging', async () => {
      const base = {
        orgId: 'org_acme',
        requestId: 'req_idem_03',
        modelName: 'gpt-4o',
        idempotencyKey: 'idem_conflict_01',
      };
      expect((await deduct({ ...base, credits: 100 })).statusCode).toBe(200);

      const conflict = await deduct({ ...base, credits: 999 });
      expect(conflict.statusCode).toBe(409);
      expect(conflict.json().code).toBe('IDEMPOTENCY_KEY_CONFLICT');

      const state = await getOrgState('org_acme');
      expect(state.cachedAvailable).toBe(1400);
      expect(state.usageEventCount).toBe(1);
    });

    it('scopes idempotency keys per organization', async () => {
      const key = { idempotencyKey: 'idem_shared_key' };
      const acme = await deduct({ orgId: 'org_acme', requestId: 'r1', modelName: 'gpt-4o', credits: 10, ...key });
      const globex = await deduct({ orgId: 'org_globex', requestId: 'r1', modelName: 'gpt-4o', credits: 10, ...key });

      expect(acme.json().idempotentReplay).toBe(false);
      expect(globex.json().idempotentReplay).toBe(false);
      expect(acme.json().usageId).not.toBe(globex.json().usageId);
    });

    it('rolls back partial FIFO lot deductions when the lots cannot cover the charge', async () => {
      // Drift the cached balance above the real lot total so the balance check passes
      // but the FIFO walk runs out of lots after partially draining them.
      await setAvailableTotal('org_acme', 5000);

      const res = await deduct({ orgId: 'org_acme', requestId: 'req_drift', modelName: 'gpt-4o', credits: 2000 });
      expect(res.statusCode).toBe(402);

      const lots = await query<{ grant_id: string; remaining_credits: string; status: string }>(
        `SELECT grant_id, remaining_credits, status FROM credit_grant_balances
         WHERE org_id = 'org_acme' ORDER BY grant_id`
      );
      expect(lots.rows.map((r) => [r.grant_id, Number(r.remaining_credits), r.status])).toEqual([
        ['grant_inv_01', 1000, 'ACTIVE'],
        ['grant_promo_01', 500, 'ACTIVE'],
      ]);
      const state = await getOrgState('org_acme');
      expect(state.cachedAvailable).toBe(5000);
      expect(state.usageEventCount).toBe(0);
    });

    it('never fails or rolls back a deduction when the asynchronous top-up fails; the intent is retried', async () => {
      // org_globex: threshold 500. 5000 - 4600 = 400 -> an intent is committed with the deduction.
      const evaluate = vi
        .spyOn(autoRechargeService, 'evaluateAndTriggerIfNeeded')
        .mockRejectedValueOnce(new Error('payment processor unavailable'));
      const spy = vi.spyOn(console, 'error').mockImplementation(() => undefined);

      const res = await deduct({ orgId: 'org_globex', requestId: 'req_ar_fail', modelName: 'gpt-4o', credits: 4600 });
      expect(res.statusCode).toBe(200);
      expect(res.json()).toMatchObject({ autoRechargeQueued: true, remainingSpendableCredits: 400 });
      await autoRechargeService.whenOutboxIdle();

      const pending = await query<{ status: string; attempts: number }>(
        `SELECT status, attempts FROM auto_recharge_outbox WHERE org_id = 'org_globex'`
      );
      expect(pending.rows).toEqual([{ status: 'PENDING', attempts: 1 }]);
      expect(await getOrgState('org_globex')).toMatchObject({ cachedAvailable: 400, rechargeOrderCount: 0 });

      // Next relay run (e.g. the maintenance worker) succeeds.
      evaluate.mockRestore();
      spy.mockRestore();
      expect((await autoRechargeService.processOutbox()).ordersCreated).toBe(1);
      expect(await getOrgState('org_globex')).toMatchObject({ cachedAvailable: 3400, rechargeOrderCount: 1 });
      await expectWalletInvariants('org_globex');
    });
  });

  describe('auto-recharge', () => {
    it('creates a single recharge order and grant for parallel evaluations at the threshold', async () => {
      // org_globex: threshold 500, amount 3000, cap 15000
      await setAvailableTotal('org_globex', 400);

      const results = await Promise.all(
        Array.from({ length: 5 }, () => autoRechargeService.evaluateAndTriggerIfNeeded('org_globex'))
      );

      expect(results.filter((r) => r.triggered)).toHaveLength(1);
      expect(results.filter((r) => r.reason === 'ABOVE_THRESHOLD')).toHaveLength(4);

      const state = await getOrgState('org_globex');
      expect(state.rechargeOrderCount).toBe(1);
      expect(state.autoRechargeGrantCount).toBe(1);
      expect(state.cachedAvailable).toBe(3400);
    });

    it('commits recharge intents with the deductions and creates exactly one order when several cross the threshold', async () => {
      autoRechargeService.autoKick = false; // relay runs explicitly below
      try {
        // 5000 -> 600, just above the 500 threshold.
        const setup = await deduct({ orgId: 'org_globex', requestId: 'req_setup', modelName: 'gpt-4o', credits: 4400 });
        expect(setup.json().autoRechargeQueued).toBe(false);

        const responses = await Promise.all(
          Array.from({ length: 4 }, (_, i) =>
            deduct({ orgId: 'org_globex', requestId: `req_cross_${i}`, modelName: 'gpt-4o', credits: 150 })
          )
        );
        expect(responses.every((r) => r.statusCode === 200)).toBe(true);
        // The deductions never waited for a top-up: the intent is committed, nothing is credited yet.
        expect(responses.filter((r) => r.json().autoRechargeQueued).length).toBeGreaterThanOrEqual(1);
        expect(await getOrgState('org_globex')).toMatchObject({ cachedAvailable: 0, rechargeOrderCount: 0 });
        const linked = await query<{ count: string }>(
          `SELECT COUNT(*)::text AS count FROM usage_events WHERE auto_recharge_intent_id IS NOT NULL`
        );
        expect(Number(linked.rows[0].count)).toBeGreaterThanOrEqual(1);

        // Concurrent relay runs coalesce every pending intent into a single order.
        await Promise.all([autoRechargeService.processOutbox(), autoRechargeService.processOutbox()]);
        const state = await getOrgState('org_globex');
        expect(state.rechargeOrderCount).toBe(1);
        expect(state.autoRechargeGrantCount).toBe(1);
        expect(state.totalRecharged).toBe(3000);
        expect(state.cachedAvailable).toBe(3000);
        const open = await query(`SELECT 1 FROM auto_recharge_outbox WHERE status = 'PENDING'`);
        expect(open.rowCount).toBe(0);
        await expectWalletInvariants('org_globex');
      } finally {
        autoRechargeService.autoKick = true;
      }
    });

    it('never exceeds the monthly recharge cap under parallel evaluations', async () => {
      await query(
        `UPDATE org_balances
         SET auto_recharge_threshold_credits = 1000,
             auto_recharge_amount_credits = 100,
             monthly_recharge_cap_credits = 250
         WHERE org_id = 'org_globex'`
      );
      await setAvailableTotal('org_globex', 400);

      const results = await Promise.all(
        Array.from({ length: 5 }, () => autoRechargeService.evaluateAndTriggerIfNeeded('org_globex'))
      );

      expect(results.filter((r) => r.triggered)).toHaveLength(2);
      expect(results.filter((r) => r.reason === 'MONTHLY_CAP_REACHED')).toHaveLength(3);

      const state = await getOrgState('org_globex');
      expect(state.rechargeOrderCount).toBe(2);
      expect(state.totalRecharged).toBe(200);
      expect(state.cachedAvailable).toBe(600);
    });
  });
  describe('striped wallet (parallel writers per org)', () => {
    const stripesTouched = async (usageId: string) =>
      (
        await query<{ stripe_no: number }>(
          `SELECT DISTINCT stripe_no FROM credit_lot_allocations WHERE usage_id = $1 ORDER BY stripe_no`,
          [usageId]
        )
      ).rows.map((r) => Number(r.stripe_no));

    it('serves a charge that fits one stripe from that stripe alone, and escalates larger ones', async () => {
      const before = { ...walletLockMetrics };

      // org_globex: 5000 credits over 4 stripes of 1250.
      const small = (await deduct({ orgId: 'org_globex', requestId: 's1', modelName: 'gpt-4o', credits: 100 })).json();
      expect(await stripesTouched(small.usageId)).toHaveLength(1);
      expect(walletLockMetrics.stripeFastPath - before.stripeFastPath).toBe(1);

      // 2000 > any stripe's 1250: FULL lock, drawn across stripes in global FIFO order.
      const large = (await deduct({ orgId: 'org_globex', requestId: 's2', modelName: 'gpt-4o', credits: 2000 })).json();
      expect((await stripesTouched(large.usageId)).length).toBeGreaterThan(1);
      expect(large.allocations).toEqual([{ grantId: 'grant_ent_01', creditsDeducted: 2000 }]);
      expect(walletLockMetrics.escalatedToFull - before.escalatedToFull).toBe(1);
      await expectWalletInvariants('org_globex');
    });

    it('keeps exact global FIFO when the head grant is split across stripes', async () => {
      // org_acme: the 500-credit promo lot is split 125 per stripe. Single-stripe draws that would
      // spill into the invoice lot while promo credits remain on other stripes must escalate.
      const allocations = [];
      for (let i = 0; i < 6; i++) {
        const res = await deduct({ orgId: 'org_acme', requestId: `fifo_${i}`, modelName: 'gpt-4o', credits: 100 });
        allocations.push(res.json().allocations);
      }
      expect(allocations.slice(0, 5)).toEqual(
        Array.from({ length: 5 }, () => [{ grantId: 'grant_promo_01', creditsDeducted: 100 }])
      );
      expect(allocations[5]).toEqual([{ grantId: 'grant_inv_01', creditsDeducted: 100 }]);
      await expectWalletInvariants('org_acme');
    });

    it('places a hold larger than any stripe by gathering free credits onto one stripe', async () => {
      // org_acme stripes hold 375 each; a 1000-credit hold must live on a single stripe.
      const hold = await app.inject({
        method: 'POST',
        url: '/credits/holds',
        payload: { orgId: 'org_acme', requestId: 'big_hold', modelName: 'gpt-4o', estimatedCredits: 1000 },
      });
      expect(hold.statusCode).toBe(201);

      const stripe = await query<{ available_credits: string; reserved_credits: string }>(
        `SELECT s.available_credits, s.reserved_credits
         FROM credit_holds h JOIN org_wallet_stripes s ON s.org_id = h.org_id AND s.stripe_no = h.home_stripe_no
         WHERE h.hold_id = $1`,
        [hold.json().holdId]
      );
      expect(Number(stripe.rows[0].reserved_credits)).toBe(1000);
      expect(Number(stripe.rows[0].available_credits)).toBeGreaterThanOrEqual(1000);
      expect(await expectWalletInvariants('org_acme')).toMatchObject({ cached: 1500, reserved: 1000 });

      // The remaining 500 free credits are still spendable; nothing beyond that.
      expect((await deduct({ orgId: 'org_acme', requestId: 'after_big', modelName: 'gpt-4o', credits: 501 })).statusCode).toBe(402);
      expect((await deduct({ orgId: 'org_acme', requestId: 'after_big2', modelName: 'gpt-4o', credits: 500 })).statusCode).toBe(200);
      expect(await expectWalletInvariants('org_acme')).toMatchObject({ cached: 1000, reserved: 1000 });
    });
  });
});
