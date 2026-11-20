import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { FastifyInstance } from 'fastify';
import RedisMock from 'ioredis-mock';
import type Redis from 'ioredis';
import { buildApp } from '../src/server';
import { pool, query, resetAndSeedDatabase } from '../src/db';
import { closeRedis, flushRedis, redis } from '../src/redis';
import { burstKey, CreditBurstRateLimiter } from '../src/services/CreditBurstRateLimiter';
import { autoRechargeService } from '../src/services/AutoRechargeService';
import { expectWalletInvariants } from './helpers/wallet';

const today = () => new Date().toISOString().slice(0, 10);

describe('Credit burst rate limiter and daily usage rollups', () => {
  let app: FastifyInstance;

  const deduct = (payload: Record<string, unknown>, headers?: Record<string, string>) =>
    app.inject({ method: 'POST', url: '/credits/deduct', payload, headers });

  const setBurstLimit = (orgId: string, limit: number) =>
    query(`UPDATE org_balances SET burst_limit_credits_per_minute = $2 WHERE org_id = $1`, [orgId, limit]);

  const burstUsed = async (orgId: string) => {
    const members = await redis.zrange(burstKey(orgId), 0, -1);
    return members.reduce((sum, m) => sum + Number(m.split('|').pop()), 0);
  };

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

  describe('rolling 60s credit burst limiter', () => {
    it('rejects spend beyond the rolling limit with 429 and Retry-After, without touching the wallet', async () => {
      await setBurstLimit('org_acme', 500);

      expect((await deduct({ orgId: 'org_acme', requestId: 'r1', modelName: 'gpt-4o', credits: 300 })).statusCode).toBe(200);
      const limited = await deduct({ orgId: 'org_acme', requestId: 'r2', modelName: 'gpt-4o', credits: 300 });

      expect(limited.statusCode).toBe(429);
      expect(limited.json().code).toBe('CREDIT_BURST_LIMIT_EXCEEDED');
      const retryAfter = Number(limited.headers['retry-after']);
      expect(retryAfter).toBeGreaterThanOrEqual(59);
      expect(retryAfter).toBeLessThanOrEqual(60);
      expect(await expectWalletInvariants('org_acme')).toMatchObject({ cached: 1200, usageCharged: 300 });
      // The key expires with the window.
      const ttl = await redis.pttl(burstKey('org_acme'));
      expect(ttl).toBeGreaterThan(0);
      expect(ttl).toBeLessThanOrEqual(60_000);
    });

    it('admits concurrent spends atomically up to the limit', async () => {
      await setBurstLimit('org_acme', 1000);
      const responses = await Promise.all(
        Array.from({ length: 5 }, (_, i) => deduct({ orgId: 'org_acme', requestId: `c${i}`, modelName: 'gpt-4o', credits: 300 }))
      );
      expect(responses.filter((r) => r.statusCode === 200)).toHaveLength(3);
      expect(responses.filter((r) => r.statusCode === 429)).toHaveLength(2);
      expect(await burstUsed('org_acme')).toBe(900);
    });

    it('replays idempotent requests at zero cost, even when the org is at its limit', async () => {
      await setBurstLimit('org_acme', 500);
      const original = { orgId: 'org_acme', requestId: 'r_idem', modelName: 'gpt-4o', credits: 300 };
      const first = await deduct(original, { 'idempotency-key': 'idem_burst' });
      expect((await deduct({ orgId: 'org_acme', requestId: 'r_other', modelName: 'gpt-4o', credits: 200 })).statusCode).toBe(200);
      expect(await burstUsed('org_acme')).toBe(500);

      const replay = await deduct(original, { 'idempotency-key': 'idem_burst' });
      expect(replay.statusCode).toBe(200);
      expect(replay.json()).toEqual({ ...first.json(), idempotentReplay: true });
      expect(await burstUsed('org_acme')).toBe(500);

      // Even once the original admission has left the window and the budget is full again,
      // a replay is served from the ledger without consulting the limiter.
      await redis.zrem(burstKey('org_acme'), 'idem:idem_burst|300');
      await redis.zadd(burstKey('org_acme'), Date.now(), 'filler|300');
      const lateReplay = await deduct(original, { 'idempotency-key': 'idem_burst' });
      expect(lateReplay.statusCode).toBe(200);
      expect(lateReplay.json().idempotentReplay).toBe(true);
      expect(await burstUsed('org_acme')).toBe(500);
    });

    it('charges concurrent duplicates of one idempotency key to the burst budget once', async () => {
      await setBurstLimit('org_acme', 1000);
      const payload = { orgId: 'org_acme', requestId: 'r_dup', modelName: 'gpt-4o', credits: 400 };
      const responses = await Promise.all(
        Array.from({ length: 4 }, () => deduct(payload, { 'idempotency-key': 'idem_dup_burst' }))
      );
      expect(responses.every((r) => r.statusCode === 200)).toBe(true);
      expect(await burstUsed('org_acme')).toBe(400);
    });

    it('gives the burst budget back when the deduction or hold fails', async () => {
      await setBurstLimit('org_acme', 2000);
      // 1600 > 1500 balance -> 402, and the 1600 must not stay counted.
      expect((await deduct({ orgId: 'org_acme', requestId: 'r_big', modelName: 'gpt-4o', credits: 1600 })).statusCode).toBe(402);
      const hold = await app.inject({
        method: 'POST',
        url: '/credits/holds',
        payload: { orgId: 'org_acme', requestId: 'r_hold_big', modelName: 'gpt-4o', estimatedCredits: 1600 },
      });
      expect(hold.statusCode).toBe(402);
      expect(await burstUsed('org_acme')).toBe(0);
      expect((await deduct({ orgId: 'org_acme', requestId: 'r_fit', modelName: 'gpt-4o', credits: 1500 })).statusCode).toBe(200);
    });

    it('counts hold reservations toward the burst limit', async () => {
      await setBurstLimit('org_acme', 1000);
      const hold = await app.inject({
        method: 'POST',
        url: '/credits/holds',
        payload: { orgId: 'org_acme', requestId: 'r_hold', modelName: 'gpt-4o', estimatedCredits: 600 },
      });
      expect(hold.statusCode).toBe(201);
      expect((await deduct({ orgId: 'org_acme', requestId: 'r_after_hold', modelName: 'gpt-4o', credits: 500 })).statusCode).toBe(429);
    });

    it('fails open when Redis errors or is too slow', async () => {
      await setBurstLimit('org_acme', 100);
      vi.spyOn(console, 'warn').mockImplementation(() => undefined);

      vi.spyOn(redis, 'eval').mockRejectedValueOnce(new Error('ECONNREFUSED'));
      expect((await deduct({ orgId: 'org_acme', requestId: 'fo1', modelName: 'gpt-4o', credits: 300 })).statusCode).toBe(200);

      vi.spyOn(redis, 'eval').mockImplementationOnce(() => new Promise(() => undefined) as never);
      const started = Date.now();
      expect((await deduct({ orgId: 'org_acme', requestId: 'fo2', modelName: 'gpt-4o', credits: 300 })).statusCode).toBe(200);
      expect(Date.now() - started).toBeLessThan(2_000);
    });

    it('evicts spends older than 60s from the rolling window (unit, injected clock)', async () => {
      let now = 1_700_000_000_000;
      const client = new RedisMock() as unknown as Redis;
      const limiter = new CreditBurstRateLimiter(client, { now: () => now, enabled: true });

      expect((await limiter.admit('org_x', 300, 500)).allowed).toBe(true);
      now += 10_000;
      const denied = await limiter.admit('org_x', 300, 500);
      expect(denied).toMatchObject({ allowed: false, usedCredits: 300, retryAfterMs: 50_000 });
      now += 50_001;
      expect(await limiter.admit('org_x', 300, 500)).toMatchObject({ allowed: true, usedCredits: 300 });
    });
  });

  describe('GET /credits/organizations/:orgId/usage/daily', () => {
    const daily = (orgId: string, qs = '') =>
      app.inject({ method: 'GET', url: `/credits/organizations/${orgId}/usage/daily${qs}` });

    it('rolls usage up per UTC day by model and funding source (promo, purchase, auto-recharge)', async () => {
      // acme: 700 on gpt-4o = 500 promo + 200 purchased; 100 on claude = purchased.
      await deduct({ orgId: 'org_acme', requestId: 'a1', modelName: 'gpt-4o', credits: 700 });
      await deduct({ orgId: 'org_acme', requestId: 'a2', modelName: 'claude-3-5-haiku', credits: 100 });
      // globex: 4600 purchased triggers a 3000 auto-recharge; 500 more drains 400 purchased + 100 auto-recharge.
      await deduct({ orgId: 'org_globex', requestId: 'g1', modelName: 'gpt-4o', credits: 4600 });
      await autoRechargeService.whenOutboxIdle(); // the asynchronous top-up lands
      await deduct({ orgId: 'org_globex', requestId: 'g2', modelName: 'gpt-4o', credits: 500 });

      const acme = (await daily('org_acme')).json();
      expect(acme.totalCredits).toBe(800);
      expect(acme.bySource).toEqual({ PROMO: 500, PURCHASE: 300, AUTO_RECHARGE: 0 });
      expect(acme.pooledCredits).toBe(0);
      expect(acme.days).toEqual([
        {
          date: today(),
          totalCredits: 800,
          bySource: { PROMO: 500, PURCHASE: 300, AUTO_RECHARGE: 0 },
          byFundingOrg: { org_acme: 800 },
          models: [
            {
              modelName: 'claude-3-5-haiku',
              totalCredits: 100,
              pooledCredits: 0,
              bySource: { PROMO: 0, PURCHASE: 100, AUTO_RECHARGE: 0 },
              eventsBySource: { PROMO: 0, PURCHASE: 1, AUTO_RECHARGE: 0 },
            },
            {
              modelName: 'gpt-4o',
              totalCredits: 700,
              pooledCredits: 0,
              bySource: { PROMO: 500, PURCHASE: 200, AUTO_RECHARGE: 0 },
              eventsBySource: { PROMO: 1, PURCHASE: 1, AUTO_RECHARGE: 0 },
            },
          ],
        },
      ]);

      const globex = (await daily('org_globex')).json();
      expect(globex.bySource).toEqual({ PROMO: 0, PURCHASE: 5000, AUTO_RECHARGE: 100 });
      expect(globex.days[0].models[0].eventsBySource).toEqual({ PROMO: 0, PURCHASE: 2, AUTO_RECHARGE: 1 });
      await expectWalletInvariants('org_acme');
      await expectWalletInvariants('org_globex');
    });

    it('includes settled stream holds and serves the report with a single query', async () => {
      const hold = await app.inject({
        method: 'POST',
        url: '/credits/holds',
        payload: { orgId: 'org_acme', requestId: 's1', modelName: 'gemini-2.5-pro', estimatedCredits: 300 },
      });
      await app.inject({
        method: 'POST',
        url: '/credits/holds/settle',
        payload: { holdId: hold.json().holdId, actualCredits: 250 },
      });

      const querySpy = vi.spyOn(pool, 'query');
      const report = (await daily('org_acme')).json();
      expect(querySpy).toHaveBeenCalledTimes(1);
      expect(report.days[0].models).toEqual([
        expect.objectContaining({ modelName: 'gemini-2.5-pro', totalCredits: 250 }),
      ]);
    });

    it('filters by an inclusive UTC date range and sums compacted totals with pending deltas', async () => {
      await query(
        `INSERT INTO usage_daily_rollups (org_id, usage_date, model_name, funding_source, funding_org_id, credits, event_count)
         VALUES
           ('org_acme', '2025-01-31', 'gpt-4o', 'PROMO', 'org_acme', 10, 1),
           ('org_acme', '2025-02-01', 'gpt-4o', 'PROMO', 'org_acme', 20, 1),
           ('org_acme', '2025-02-02', 'gpt-4o', 'PURCHASE', 'org_acme', 40, 1),
           ('org_acme', '2025-02-03', 'gpt-4o', 'PURCHASE', 'org_acme', 99, 1)`
      );
      await query(
        `INSERT INTO usage_rollup_deltas (delta_id, org_id, usage_date, model_name, funding_source, funding_org_id, credits)
         VALUES
           ('d1', 'org_acme', '2025-02-01', 'gpt-4o', 'PROMO', 'org_acme', 5),
           ('d2', 'org_acme', '2025-02-01', 'gpt-4o', 'PURCHASE', 'org_parent', 7),
           ('d3', 'org_acme', '2025-02-04', 'gpt-4o', 'PURCHASE', 'org_acme', 1)`
      );

      const res = await daily('org_acme', '?from=2025-02-01&to=2025-02-02');
      expect(res.statusCode).toBe(200);
      const body = res.json();
      expect(body.days.map((d: { date: string; totalCredits: number }) => [d.date, d.totalCredits])).toEqual([
        ['2025-02-01', 32],
        ['2025-02-02', 40],
      ]);
      expect(body.days[0].bySource).toEqual({ PROMO: 25, PURCHASE: 7, AUTO_RECHARGE: 0 });
      expect(body.days[0].byFundingOrg).toEqual({ org_acme: 25, org_parent: 7 });
      expect(body.pooledCredits).toBe(7);
      expect(body.totalCredits).toBe(72);
    });

    it('returns an empty report for an org without usage and 404 for an unknown org', async () => {
      const empty = await daily('org_globex');
      expect(empty.statusCode).toBe(200);
      expect(empty.json()).toMatchObject({ totalCredits: 0, days: [] });
      expect((await daily('org_missing')).statusCode).toBe(404);
    });

    it.each([
      ['?from=2025-13-01', 'invalid date'],
      ['?from=2025-03-01&to=2025-02-01', 'from after to'],
      ['?from=2023-01-01&to=2025-01-01', 'range too long'],
    ])('rejects %s (%s) with 400', async (qs) => {
      const res = await daily('org_acme', qs);
      expect(res.statusCode).toBe(400);
      expect(res.json().code).toBe('VALIDATION_ERROR');
    });
  });
});
