import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { FastifyInstance } from 'fastify';
import RedisMock from 'ioredis-mock';
import type Redis from 'ioredis';
import { buildApp } from '../src/server';
import { query, resetAndSeedDatabase, withTransaction } from '../src/db';
import { closeRedis, flushRedis, redis } from '../src/redis';
import { autoRechargeService } from '../src/services/AutoRechargeService';
import { burstKey, CreditBurstRateLimiter } from '../src/services/CreditBurstRateLimiter';
import { creditLedgerService } from '../src/services/CreditLedgerService';
import {
  clientQuery,
  loadOrgChain,
  LockLedger,
  LockOrderViolationError,
  lockStripes,
  stripeRotation,
} from '../src/services/OrgWallet';
import { expectWalletInvariants, getAvailableTotal } from './helpers/wallet';

// Seed: org_globex (ENTERPRISE, 5000 credits, auto-recharge at <= 500 by 3000), org_acme (1500).

describe('Hierarchical pooling, recharge outbox, rollups and stripe acquisition', () => {
  let app: FastifyInstance;
  let childGrantId: string;

  const post = (url: string, payload: Record<string, unknown>) => app.inject({ method: 'POST', url, payload });
  const put = (url: string, payload: Record<string, unknown>) => app.inject({ method: 'PUT', url, payload });
  const deduct = (orgId: string, credits: number, requestId = `req_${Math.random()}`) =>
    post('/credits/deduct', { orgId, requestId, modelName: 'gpt-4o', credits });
  const createHold = (orgId: string, estimatedCredits: number) =>
    post('/credits/holds', { orgId, requestId: `req_${Math.random()}`, modelName: 'gpt-4o', estimatedCredits });

  const reserved = async (orgId: string) =>
    Number(
      (await query<{ r: string }>(`SELECT SUM(reserved_credits)::text AS r FROM org_wallet_stripes WHERE org_id = $1`, [orgId]))
        .rows[0].r
    );

  /** org_child (300 own credits) under org_globex. */
  const createChild = async (orgId = 'org_child', credits = 300, parent = 'org_globex') => {
    const grant = await creditLedgerService.createGrant({
      orgId,
      source: 'PREPAID_INVOICE',
      credits,
      expiresAt: new Date(Date.now() + 30 * 86_400_000).toISOString(),
    });
    const res = await put(`/credits/organizations/${orgId}/parent`, { parentOrgId: parent });
    expect(res.statusCode).toBe(200);
    return grant.grantId;
  };

  beforeAll(async () => {
    app = buildApp();
    await app.ready();
  });

  beforeEach(async () => {
    await resetAndSeedDatabase();
    await flushRedis();
    childGrantId = await createChild();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    autoRechargeService.autoKick = true;
  });

  afterAll(async () => {
    await app.close();
    await closeRedis();
  });

  describe('hierarchical credit pooling', () => {
    it('draws a sub-org\'s own grants first and cascades the remaining deduction to the parent pool', async () => {
      const res = await deduct('org_child', 500, 'req_cascade');
      expect(res.statusCode).toBe(200);
      expect(res.json().allocations).toEqual([
        { grantId: childGrantId, creditsDeducted: 300 },
        { grantId: 'grant_ent_01', creditsDeducted: 200 },
      ]);
      expect(await getAvailableTotal('org_child')).toBe(0);
      expect(await getAvailableTotal('org_globex')).toBe(4800);

      const report = (await app.inject({ method: 'GET', url: '/credits/organizations/org_child/usage/daily' })).json();
      expect(report.pooledCredits).toBe(200);
      expect(report.days[0].byFundingOrg).toEqual({ org_child: 300, org_globex: 200 });
      // The parent's own usage report only contains its own consumption.
      expect((await app.inject({ method: 'GET', url: '/credits/organizations/org_globex/usage/daily' })).json().totalCredits).toBe(0);

      const balance = (await app.inject({ method: 'GET', url: '/credits/organizations/org_child/balance' })).json();
      expect(balance).toMatchObject({ parentOrgId: 'org_globex', netSpendableCredits: 0, poolSpendableCredits: 4800 });
      await expectWalletInvariants('org_child');
      await expectWalletInvariants('org_globex');
    });

    it('cascades a hold\'s reservation deficit, protects it from parent spending and settles own-first (LIFO release)', async () => {
      const hold = await createHold('org_child', 800);
      expect(hold.statusCode).toBe(201);
      const portions = await query<{ seq: number; org_id: string; reserved_credits: string }>(
        `SELECT seq, org_id, reserved_credits FROM credit_hold_portions WHERE hold_id = $1 ORDER BY seq`,
        [hold.json().holdId]
      );
      expect(portions.rows.map((p) => [p.seq, p.org_id, Number(p.reserved_credits)])).toEqual([
        [0, 'org_child', 300],
        [1, 'org_globex', 500],
      ]);

      // The parent cannot spend credits reserved for its sub-org's hold.
      expect((await deduct('org_globex', 4600)).statusCode).toBe(402);
      expect((await deduct('org_globex', 4500)).statusCode).toBe(200);
      expect(await reserved('org_globex')).toBe(500);

      // Reset the parent's spending for the settlement check.
      await resetAndSeedDatabase();
      childGrantId = await createChild();
      const h2 = (await createHold('org_child', 800)).json();
      const settled = await post('/credits/holds/settle', { holdId: h2.holdId, actualCredits: 400 });
      expect(settled.json()).toMatchObject({ chargedCredits: 400, pooledCredits: 100, shortfallCredits: 0 });
      // Own 300 first, only the excess from the parent; the parent portion is fully released.
      expect(await reserved('org_globex')).toBe(0);
      expect(await getAvailableTotal('org_globex')).toBe(4900);
      expect(await getAvailableTotal('org_child')).toBe(0);
      await expectWalletInvariants('org_child');
      await expectWalletInvariants('org_globex');
    });

    it('returns the whole parent portion when usage fits the sub-org\'s own reservation', async () => {
      const hold = (await createHold('org_child', 500)).json(); // 300 own + 200 cascaded
      expect(await reserved('org_globex')).toBe(200);
      const settled = await post('/credits/holds/settle', { holdId: hold.holdId, actualCredits: 250 });
      expect(settled.json()).toMatchObject({ chargedCredits: 250, pooledCredits: 0 });
      expect(await reserved('org_globex')).toBe(0);
      expect(await getAvailableTotal('org_globex')).toBe(5000);
      expect(await getAvailableTotal('org_child')).toBe(50);
      await expectWalletInvariants('org_child');
    });

    it('cascades extensions and releases of a hold across the pools', async () => {
      const hold = (await createHold('org_child', 100)).json(); // fits the sub-org's own pool
      const ext = await post('/credits/holds/extend', { holdId: hold.holdId, additionalTtlSeconds: 30, additionalCredits: 400 });
      expect(ext.statusCode).toBe(200);
      expect(ext.json().estimatedCredits).toBe(500);
      expect(await reserved('org_child')).toBe(300);
      expect(await reserved('org_globex')).toBe(200);

      expect((await post('/credits/holds/release', { holdId: hold.holdId })).statusCode).toBe(200);
      expect(await reserved('org_child')).toBe(0);
      expect(await reserved('org_globex')).toBe(0);
      await expectWalletInvariants('org_child');
      await expectWalletInvariants('org_globex');
    });

    it('rejects what the sub-org and its ancestors together cannot cover, leaving every pool untouched', async () => {
      expect((await deduct('org_child', 5301)).statusCode).toBe(402);
      expect((await createHold('org_child', 5301)).statusCode).toBe(402);
      expect(await getAvailableTotal('org_child')).toBe(300);
      expect(await getAvailableTotal('org_globex')).toBe(5000);
      expect(await reserved('org_globex')).toBe(0);
    });

    it('keeps pools consistent under concurrent parent deductions, cascading holds and settlements', async () => {
      const holds = await Promise.all([createHold('org_child', 600), createHold('org_child', 400)]);
      const [h1, h2] = holds.map((h) => h.json().holdId as string);

      const results = await Promise.allSettled([
        deduct('org_globex', 1000),
        deduct('org_globex', 700),
        deduct('org_child', 250),
        createHold('org_child', 900),
        post('/credits/holds/settle', { holdId: h1, actualCredits: 650 }),
        post('/credits/holds/release', { holdId: h2 }),
        post('/credits/holds/settle', { holdId: h1, actualCredits: 10 }),
        createHold('org_globex', 1200),
      ]);
      for (const r of results) {
        expect(r.status).toBe('fulfilled');
        if (r.status === 'fulfilled') expect([200, 201, 402, 409]).toContain(r.value.statusCode);
      }
      await expectWalletInvariants('org_child');
      await expectWalletInvariants('org_globex');
    });

    it('validates hierarchy changes: cycles, self-parenting, depth, unknown parents, active cascades', async () => {
      expect((await put('/credits/organizations/org_globex/parent', { parentOrgId: 'org_child' })).statusCode).toBe(409);
      expect((await put('/credits/organizations/org_child/parent', { parentOrgId: 'org_child' })).statusCode).toBe(409);
      expect((await put('/credits/organizations/org_child/parent', { parentOrgId: 'org_nope' })).statusCode).toBe(404);

      // org_globex <- org_child <- org_l2 <- org_l3 is the deepest allowed (4 levels).
      await createChild('org_l2', 10, 'org_child');
      await createChild('org_l3', 10, 'org_l2');
      await creditLedgerService.createGrant({
        orgId: 'org_l4',
        source: 'PROMOTIONAL',
        credits: 10,
        expiresAt: new Date(Date.now() + 86_400_000).toISOString(),
      });
      const tooDeep = await put('/credits/organizations/org_l4/parent', { parentOrgId: 'org_l3' });
      expect(tooDeep.statusCode).toBe(409);
      expect(tooDeep.json().code).toBe('POOL_CONFIGURATION_CONFLICT');

      // A hold of a sub-org reserved in the parent pool pins the hierarchy until it is released.
      const hold = (await createHold('org_child', 800)).json();
      expect((await put('/credits/organizations/org_child/parent', { parentOrgId: 'org_acme' })).statusCode).toBe(409);
      await post('/credits/holds/release', { holdId: hold.holdId });
      expect((await put('/credits/organizations/org_child/parent', { parentOrgId: 'org_acme' })).statusCode).toBe(200);
      expect((await put('/credits/organizations/org_child/parent', { parentOrgId: null })).json().chain).toEqual(['org_child']);
    });

    it('enforces the canonical lock order (descendants before ancestors, ascending stripes)', async () => {
      const ledger = new LockLedger();
      ledger.acquire([{ depth: 1, orgId: 'org_child', stripeNo: 2 }]);
      ledger.acquire([{ depth: 0, orgId: 'org_globex', stripeNo: 0 }]);
      expect(() => ledger.acquire([{ depth: 1, orgId: 'org_child', stripeNo: 3 }])).toThrow(LockOrderViolationError);
      const mark = ledger.mark();
      ledger.acquire([{ depth: 0, orgId: 'org_globex', stripeNo: 3 }]);
      ledger.rollbackTo(mark);
      expect(ledger.holds('org_globex', 3)).toBe(false);
      expect(() => ledger.acquire([{ depth: 0, orgId: 'org_globex', stripeNo: 1 }])).not.toThrow();

      // A transaction locking a parent stripe and then a child stripe is refused before it can wait.
      await expect(
        withTransaction(async (client) => {
          const [child, parent] = await loadOrgChain(clientQuery(client), 'org_child');
          await lockStripes(client, parent, [0]);
          await lockStripes(client, child, [0]);
        })
      ).rejects.toThrow(LockOrderViolationError);
    });
  });

  describe('auto-recharge transactional outbox', () => {
    it('queues a recharge for the parent pool when a sub-org\'s cascade drains it, without waiting for it', async () => {
      autoRechargeService.autoKick = false;
      const res = await deduct('org_child', 4800, 'req_drain'); // 300 own + 4500 parent -> parent at 500
      expect(res.json()).toMatchObject({ autoRechargeQueued: true });
      const intents = await query<{ org_id: string; status: string }>(`SELECT org_id, status FROM auto_recharge_outbox`);
      expect(intents.rows).toEqual([{ org_id: 'org_globex', status: 'PENDING' }]);
      expect(await getAvailableTotal('org_globex')).toBe(500);

      const run = await autoRechargeService.processOutbox();
      expect(run).toMatchObject({ ordersCreated: 1, intentsCompleted: 1 });
      expect(await getAvailableTotal('org_globex')).toBe(3500);
      await expectWalletInvariants('org_globex');
    });

    it('never turns an intent into two orders, even when processed repeatedly or concurrently', async () => {
      autoRechargeService.autoKick = false;
      await Promise.all([deduct('org_globex', 4600), deduct('org_child', 100)]);
      await Promise.all([
        autoRechargeService.processOutbox(),
        autoRechargeService.processOutbox(['org_globex']),
        autoRechargeService.processOutbox(['org_globex']),
      ]);
      await autoRechargeService.processOutbox();
      const orders = await query<{ n: string; intents: string }>(
        `SELECT COUNT(*)::text AS n, COUNT(DISTINCT intent_id)::text AS intents FROM auto_recharge_orders`
      );
      expect(orders.rows[0]).toEqual({ n: '1', intents: '1' });
    });

    it('skips intents whose org was topped up in the meantime', async () => {
      autoRechargeService.autoKick = false;
      await deduct('org_globex', 4600);
      await creditLedgerService.createGrant({
        orgId: 'org_globex',
        source: 'PREPAID_INVOICE',
        credits: 5000,
        expiresAt: new Date(Date.now() + 86_400_000).toISOString(),
      });
      expect(await autoRechargeService.processOutbox()).toMatchObject({ ordersCreated: 0, intentsSkipped: 1 });
      const intent = await query<{ status: string; reason: string }>(`SELECT status, reason FROM auto_recharge_outbox`);
      expect(intent.rows).toEqual([{ status: 'SKIPPED', reason: 'ABOVE_THRESHOLD' }]);
    });
  });

  describe('daily usage rollups without hot rows', () => {
    it('appends immutable deltas on the write path and compacts them without changing the report', async () => {
      await deduct('org_child', 500);
      await deduct('org_child', 40);
      await deduct('org_acme', 700);

      // Write path: only inserts into the delta log, never an upsert of a shared totals row.
      expect((await query(`SELECT 1 FROM usage_daily_rollups`)).rowCount).toBe(0);
      expect((await query(`SELECT 1 FROM usage_rollup_deltas`)).rowCount).toBe(5);

      const before = (await app.inject({ method: 'GET', url: '/credits/organizations/org_child/usage/daily' })).json();
      expect((await creditLedgerService.compactUsageRollups()).deltasCompacted).toBe(5);
      expect((await creditLedgerService.compactUsageRollups()).deltasCompacted).toBe(0);
      const after = (await app.inject({ method: 'GET', url: '/credits/organizations/org_child/usage/daily' })).json();
      expect(after).toEqual(before);
      expect(after.days[0].models[0].eventsBySource.PURCHASE).toBe(3); // 2 own + 1 pooled delta
      await expectWalletInvariants('org_child');
      await expectWalletInvariants('org_acme');
    });

    it('never loses or double counts usage when compaction races deductions', async () => {
      await Promise.all([
        ...Array.from({ length: 6 }, () => deduct('org_acme', 50)),
        creditLedgerService.compactUsageRollups(2),
        creditLedgerService.compactUsageRollups(3),
        ...Array.from({ length: 4 }, () => deduct('org_child', 90)),
      ]);
      await expectWalletInvariants('org_acme');
      await expectWalletInvariants('org_child');
    });
  });

  describe('deterministic stripe acquisition and the sliding burst window', () => {
    it('chooses stripes deterministically per operation key and spreads keys evenly', async () => {
      expect(stripeRotation([0, 1, 2, 3], 'req_a')).toEqual(stripeRotation([3, 2, 1, 0], 'req_a'));
      const counts = [0, 0, 0, 0];
      for (let i = 0; i < 2000; i++) counts[stripeRotation([0, 1, 2, 3], `req_${i}`)[0]] += 1;
      for (const c of counts) expect(c).toBeGreaterThan(400);

      // With every stripe idle, a deduction lands on the first stripe of its rotation.
      const res = (await deduct('org_globex', 10, 'req_deterministic')).json();
      const stripe = await query<{ stripe_no: number }>(
        `SELECT stripe_no FROM credit_lot_allocations WHERE usage_id = $1`,
        [res.usageId]
      );
      expect(Number(stripe.rows[0].stripe_no)).toBe(stripeRotation([0, 1, 2, 3], 'req_deterministic')[0]);
    });

    it('slides the window key expiry with its contents instead of a fixed TTL', async () => {
      let now = 1_700_000_000_000;
      const client = new RedisMock() as unknown as Redis;
      const limiter = new CreditBurstRateLimiter(client, { now: () => now, enabled: true });

      await limiter.admit('org_x', 300, 500);
      now += 40_000;
      await limiter.admit('org_x', 100, 500);
      expect(await client.pttl(burstKey('org_x'))).toBeGreaterThan(59_000); // newest entry + 60s

      now += 30_000; // the first entry slid out; a denied request does not extend the key
      expect((await limiter.admit('org_x', 450, 500)).allowed).toBe(false);
      const ttl = await client.pttl(burstKey('org_x'));
      expect(ttl).toBeGreaterThan(29_000);
      expect(ttl).toBeLessThanOrEqual(30_000);

      now += 30_001; // everything slid out: the key is gone
      expect((await limiter.admit('org_x', 600, 500)).allowed).toBe(false);
      expect(await client.exists(burstKey('org_x'))).toBe(0);
    });

    it('measures the window on the Redis server clock by default', async () => {
      const limiter = new CreditBurstRateLimiter(redis, { enabled: true });
      await limiter.admit('org_clock', 10, 100, 'probe');
      const [, score] = await redis.zrange(burstKey('org_clock'), 0, -1, 'WITHSCORES');
      expect(Math.abs(Number(score) - Date.now())).toBeLessThan(5_000);
    });

    it('enforces the burst quota on hold extensions and gives back failed ones', async () => {
      await query(`UPDATE org_balances SET burst_limit_credits_per_minute = 1000 WHERE org_id = 'org_acme'`);
      const hold = (await createHold('org_acme', 600)).json();
      const extend = (additionalCredits: number) =>
        post('/credits/holds/extend', { holdId: hold.holdId, additionalTtlSeconds: 30, additionalCredits });

      expect((await extend(300)).statusCode).toBe(200);
      const limited = await extend(200);
      expect(limited.statusCode).toBe(429);
      expect(Number(limited.headers['retry-after'])).toBeGreaterThan(0);
      expect(await reserved('org_acme')).toBe(900);

      // A failed extension (insufficient credits) does not consume burst budget.
      await query(`UPDATE org_balances SET burst_limit_credits_per_minute = 100000 WHERE org_id = 'org_acme'`);
      expect((await extend(5000)).statusCode).toBe(402);
      const used = (await redis.zrange(burstKey('org_acme'), 0, -1)).reduce((s, m) => s + Number(m.split('|').pop()), 0);
      expect(used).toBe(900);
      await expectWalletInvariants('org_acme');
    });
  });
});
