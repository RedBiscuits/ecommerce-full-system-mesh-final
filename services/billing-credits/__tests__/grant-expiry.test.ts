import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { FastifyInstance } from 'fastify';
import { buildApp } from '../src/server';
import { query, resetAndSeedDatabase } from '../src/db';
import { closeRedis, flushRedis } from '../src/redis';
import { creditLedgerService } from '../src/services/CreditLedgerService';
import { billingWebhookService } from '../src/services/BillingWebhookService';
import { autoRechargeService } from '../src/services/AutoRechargeService';
import { MaintenanceWorker } from '../src/workers/MaintenanceWorker';
import { expireLot, getGrant, getWalletState, setAvailableTotal } from './helpers/wallet';

// Seed: org_acme has grant_promo_01 (500, expires in 7d, no invoice) and
// grant_inv_01 (1000, expires in 90d, invoice inv_initial_1000); cached balance 1500.


async function getOrg(orgId: string) {
  const state = await getWalletState(orgId);
  return { available: state.cached, reserved: state.reserved, activeLotTotal: state.activeLotCredits };
}

const getLot = getGrant;

async function getDiscrepancies() {
  const res = await query<{
    org_id: string;
    discrepancy_type: string;
    invoice_id: string | null;
    grant_id: string;
    unrecovered_credits: string;
    reason: string;
  }>(
    `SELECT org_id, discrepancy_type, invoice_id, grant_id, unrecovered_credits, reason
     FROM billing_discrepancies ORDER BY created_at ASC, grant_id ASC`
  );
  return res.rows.map((r) => ({ ...r, unrecovered_credits: Number(r.unrecovered_credits) }));
}

describe('Credit lot expiry with active holds', () => {
  let app: FastifyInstance;

  const createHold = async (estimatedCredits: number, orgId = 'org_acme') => {
    const res = await app.inject({
      method: 'POST',
      url: '/credits/holds',
      payload: { orgId, requestId: `req_hold_${estimatedCredits}`, modelName: 'gpt-4o', estimatedCredits },
    });
    expect(res.statusCode).toBe(201);
    return res.json().holdId as string;
  };

  const deduct = (payload: Record<string, unknown>) =>
    app.inject({ method: 'POST', url: '/credits/deduct', payload });

  beforeAll(async () => {
    app = buildApp();
    await app.ready();
  });

  beforeEach(async () => {
    await resetAndSeedDatabase();
    await flushRedis();
  });

  afterAll(async () => {
    await app.close();
    await closeRedis();
  });

  describe('expireStaleGrants', () => {
    it('expires and zeroes lapsed lots, reconciles the balance, and is a no-op on re-run', async () => {
      await expireLot('grant_promo_01');

      const result = await creditLedgerService.expireStaleGrants();
      expect(result).toEqual({
        expiredLotsCount: 1,
        expiredCreditsTotal: 500,
        discrepanciesLogged: 0,
        unbackedHoldCredits: 0,
      });

      expect(await getLot('grant_promo_01')).toEqual({ status: 'EXPIRED', remaining: 0 });
      expect(await getOrg('org_acme')).toMatchObject({ available: 1000, activeLotTotal: 1000 });
      expect(await getDiscrepancies()).toEqual([]);

      const balance = (await app.inject({ method: 'GET', url: '/credits/organizations/org_acme/balance' })).json();
      expect(balance.availableCredits).toBe(1000);
      expect(balance.activeLots.map((l: { grantId: string }) => l.grantId)).toEqual(['grant_inv_01']);

      expect(await creditLedgerService.expireStaleGrants()).toMatchObject({ expiredLotsCount: 0 });
      expect((await getOrg('org_acme')).available).toBe(1000);
    });

    it('logs an EXPIRED_DURING_ACTIVE_HOLD discrepancy when holds exceed the remaining lots', async () => {
      const holdId = await createHold(1200);
      await expireLot('grant_promo_01');

      const result = await creditLedgerService.expireStaleGrants();
      expect(result).toMatchObject({ expiredLotsCount: 1, discrepanciesLogged: 1, unbackedHoldCredits: 200 });

      // Available reconciles to the non-expired lots; the reservation still mirrors the live hold.
      expect(await getOrg('org_acme')).toEqual({ available: 1000, reserved: 1200, activeLotTotal: 1000 });

      const discrepancies = await getDiscrepancies();
      expect(discrepancies).toHaveLength(1);
      expect(discrepancies[0]).toMatchObject({
        org_id: 'org_acme',
        discrepancy_type: 'EXPIRED_DURING_ACTIVE_HOLD',
        grant_id: 'grant_promo_01',
        invoice_id: null,
        unrecovered_credits: 200,
      });
      expect(discrepancies[0].reason).toContain(holdId);

      const balance = (await app.inject({ method: 'GET', url: '/credits/organizations/org_acme/balance' })).json();
      expect(balance.netSpendableCredits).toBe(0);
    });

    it('logs nothing when the remaining lots still cover every active hold', async () => {
      await createHold(600);
      await expireLot('grant_promo_01');

      const result = await creditLedgerService.expireStaleGrants();
      expect(result).toMatchObject({ discrepanciesLogged: 0, unbackedHoldCredits: 0 });
      expect(await getOrg('org_acme')).toEqual({ available: 1000, reserved: 600, activeLotTotal: 1000 });
      expect(await getDiscrepancies()).toEqual([]);
    });

    it('never drives the balance negative when the cached balance had drifted below the lots', async () => {
      // The old subtractive sweep would compute 300 - 500 = -200 here.
      await setAvailableTotal('org_acme', 300);
      await expireLot('grant_promo_01');
      await creditLedgerService.expireStaleGrants();
      expect((await getOrg('org_acme')).available).toBe(1000);

      await setAvailableTotal('org_acme', 100);
      await expireLot('grant_inv_01');
      await creditLedgerService.expireStaleGrants();
      expect(await getOrg('org_acme')).toMatchObject({ available: 0, activeLotTotal: 0 });
    });

    it('attributes only newly unbacked hold credits across successive sweeps (no double counting)', async () => {
      await createHold(1200);

      await expireLot('grant_promo_01');
      await creditLedgerService.expireStaleGrants();
      await expireLot('grant_inv_01');
      const second = await creditLedgerService.expireStaleGrants();
      expect(second.unbackedHoldCredits).toBe(1000);

      const discrepancies = await getDiscrepancies();
      expect(discrepancies.map((d) => [d.grant_id, d.invoice_id, d.unrecovered_credits])).toEqual([
        ['grant_promo_01', null, 200],
        ['grant_inv_01', 'inv_initial_1000', 1000],
      ]);
      // Total unbacked equals the full reservation once every lot has expired.
      expect(discrepancies.reduce((s, d) => s + d.unrecovered_credits, 0)).toBe(1200);
      expect(await getOrg('org_acme')).toEqual({ available: 0, reserved: 1200, activeLotTotal: 0 });
    });

    it('stays consistent when sweeps race each other and concurrent deductions', async () => {
      await expireLot('grant_promo_01');

      const [sweepA, sweepB, ...deductions] = await Promise.all([
        creditLedgerService.expireStaleGrants(),
        creditLedgerService.expireStaleGrants(),
        ...Array.from({ length: 4 }, (_, i) =>
          deduct({ orgId: 'org_acme', requestId: `req_race_${i}`, modelName: 'gpt-4o', credits: 100 })
        ),
      ]);

      // The lot is expired exactly once, no matter which sweep won.
      expect(sweepA.expiredLotsCount + sweepB.expiredLotsCount).toBe(1);
      expect(deductions.every((d) => d.statusCode === 200)).toBe(true);
      for (const d of deductions) {
        expect(d.json().allocations).toEqual([{ grantId: 'grant_inv_01', creditsDeducted: 100 }]);
      }
      expect(await getLot('grant_promo_01')).toEqual({ status: 'EXPIRED', remaining: 0 });
      expect(await getOrg('org_acme')).toMatchObject({ available: 600, activeLotTotal: 600 });
    });

    it('keeps the balance equal to the lots when grants and refunds race the sweep', async () => {
      await expireLot('grant_promo_01');

      await Promise.all([
        creditLedgerService.createGrant({
          orgId: 'org_acme',
          source: 'PROMOTIONAL',
          credits: 100,
          expiresAt: new Date(Date.now() + 86_400_000).toISOString(),
        }),
        billingWebhookService.handleStripeWebhook({
          eventId: 'evt_refund_race',
          eventType: 'charge.refunded',
          orgId: 'org_acme',
          invoiceId: 'inv_initial_1000',
          credits: 1000,
        }),
        creditLedgerService.expireStaleGrants(),
      ]);

      expect(await getOrg('org_acme')).toMatchObject({ available: 100, activeLotTotal: 100 });
    });

    it('still settles a hold whose original lot expired, as long as live lots cover the actual usage', async () => {
      const holdId = await createHold(600);
      await expireLot('grant_promo_01');
      await creditLedgerService.expireStaleGrants();

      const settle = await app.inject({
        method: 'POST',
        url: '/credits/holds/settle',
        payload: { holdId, actualCredits: 500 },
      });
      expect(settle.statusCode).toBe(200);
      expect(await getOrg('org_acme')).toEqual({ available: 500, reserved: 0, activeLotTotal: 500 });
    });
  });

  describe('deductUsage before the sweep runs', () => {
    it('skips lapsed-but-unswept lots and excludes them from the spendable balance', async () => {
      await expireLot('grant_promo_01');

      const res = await deduct({ orgId: 'org_acme', requestId: 'req_skip', modelName: 'gpt-4o', credits: 300 });
      expect(res.statusCode).toBe(200);
      expect(res.json().allocations).toEqual([{ grantId: 'grant_inv_01', creditsDeducted: 300 }]);
      expect(res.json().remainingSpendableCredits).toBe(700);
      // Untouched: the sweep, not the deduction, retires the lot.
      expect(await getLot('grant_promo_01')).toEqual({ status: 'ACTIVE', remaining: 500 });

      await creditLedgerService.expireStaleGrants();
      expect(await getOrg('org_acme')).toMatchObject({ available: 700, activeLotTotal: 700 });
    });

    it('rejects with 402 when only expired credits could cover the charge', async () => {
      await expireLot('grant_promo_01');

      const res = await deduct({ orgId: 'org_acme', requestId: 'req_402', modelName: 'gpt-4o', credits: 1200 });
      expect(res.statusCode).toBe(402);
      expect(res.json().error).toContain('available 1000');
      expect(await getLot('grant_inv_01')).toEqual({ status: 'ACTIVE', remaining: 1000 });
    });

    it('triggers auto-recharge when the balance is only above threshold because of expired credits', async () => {
      // org_globex: 5000 enterprise lot + a lapsed 1000 lot still counted in the cache; threshold 500.
      await query(
        `INSERT INTO credit_grants (grant_id, org_id, source, initial_credits, status, expires_at)
         VALUES ('grant_globex_lapsed', 'org_globex', 'PROMOTIONAL', 1000, 'ACTIVE', NOW() - INTERVAL '1 day')`
      );
      await query(
        `INSERT INTO credit_lots (grant_id, stripe_no, org_id, remaining_credits)
         SELECT 'grant_globex_lapsed', stripe_no, org_id, 250 FROM org_wallet_stripes WHERE org_id = 'org_globex'`
      );
      await query(
        `UPDATE org_wallet_stripes SET available_credits = available_credits + 250 WHERE org_id = 'org_globex'`
      );

      const res = await deduct({ orgId: 'org_globex', requestId: 'req_ar', modelName: 'gpt-4o', credits: 4600 });
      expect(res.statusCode).toBe(200);
      // 5000 live - 4600 = 400 <= 500: the lapsed 1000 credits do not hide the threshold crossing.
      expect(res.json()).toMatchObject({ autoRechargeQueued: true, remainingSpendableCredits: 400 });
      await autoRechargeService.whenOutboxIdle();

      await creditLedgerService.expireStaleGrants();
      expect(await getOrg('org_globex')).toMatchObject({ available: 3400, activeLotTotal: 3400 });
    });
  });

  describe('MaintenanceWorker', () => {
    it('releases expired holds before sweeping grants so abandoned holds are not flagged', async () => {
      await createHold(1200);
      await query(`UPDATE credit_holds SET expires_at = NOW() - INTERVAL '1 minute'`);
      await expireLot('grant_promo_01');

      const cycle = await new MaintenanceWorker().runCycle();
      expect(cycle).toEqual({
        expiredLotsCount: 1,
        expiredCreditsTotal: 500,
        releasedHoldsCount: 1,
        releasedHoldCredits: 1200,
        discrepanciesLogged: 0,
        unbackedHoldCredits: 0,
        rechargeOrdersCreated: 0,
        rollupDeltasCompacted: 0,
      });
      expect(await getOrg('org_acme')).toEqual({ available: 1000, reserved: 0, activeLotTotal: 1000 });
      expect(await getDiscrepancies()).toEqual([]);
    });
  });
});
