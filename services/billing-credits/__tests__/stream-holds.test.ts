import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { FastifyInstance } from 'fastify';
import { buildApp } from '../src/server';
import { pool, query, resetAndSeedDatabase } from '../src/db';
import { closeRedis, flushRedis } from '../src/redis';
import { creditLedgerService } from '../src/services/CreditLedgerService';
import { streamHoldService } from '../src/services/StreamHoldService';
import { autoRechargeService } from '../src/services/AutoRechargeService';
import { expectWalletInvariants, expireLot } from './helpers/wallet';

describe('Streaming Pre-Authorization Credit Holds', () => {
  let app: FastifyInstance;

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

  it('places a streaming pre-auth hold and settles actual usage upon completion', async () => {
    const holdRes = await app.inject({
      method: 'POST',
      url: '/credits/holds',
      payload: {
        orgId: 'org_acme',
        requestId: 'req_stream_202',
        modelName: 'gpt-4o',
        estimatedCredits: 600,
      },
    });

    expect(holdRes.statusCode).toBe(201);
    const hold = holdRes.json();
    expect(hold.status).toBe('ACTIVE');

    const midBalanceRes = await app.inject({
      method: 'GET',
      url: '/credits/organizations/org_acme/balance',
    });
    expect(midBalanceRes.json().reservedCredits).toBe(600);
    expect(midBalanceRes.json().netSpendableCredits).toBe(900);

    const settleRes = await app.inject({
      method: 'POST',
      url: '/credits/holds/settle',
      payload: {
        holdId: hold.holdId,
        actualCredits: 250,
      },
    });

    expect(settleRes.statusCode).toBe(200);
    expect(settleRes.json().status).toBe('SETTLED');

    const finalBalanceRes = await app.inject({
      method: 'GET',
      url: '/credits/organizations/org_acme/balance',
    });
    expect(finalBalanceRes.json().reservedCredits).toBe(0);
    expect(finalBalanceRes.json().availableCredits).toBe(1250);
    expect(finalBalanceRes.json().netSpendableCredits).toBe(1250);
  });

  it('extends an active stream hold TTL and increases reserved credits', async () => {
    const holdRes = await app.inject({
      method: 'POST',
      url: '/credits/holds',
      payload: {
        orgId: 'org_acme',
        requestId: 'req_stream_ext_01',
        modelName: 'claude-3-7-sonnet',
        estimatedCredits: 400,
        ttlSeconds: 120,
      },
    });
    const hold = holdRes.json();

    const extendRes = await app.inject({
      method: 'POST',
      url: '/credits/holds/extend',
      payload: {
        holdId: hold.holdId,
        additionalTtlSeconds: 180,
        additionalCredits: 300,
      },
    });

    expect(extendRes.statusCode).toBe(200);
    expect(extendRes.json().estimatedCredits).toBe(700);

    const balanceRes = await app.inject({
      method: 'GET',
      url: '/credits/organizations/org_acme/balance',
    });
    expect(balanceRes.json().reservedCredits).toBe(700);
    expect(balanceRes.json().netSpendableCredits).toBe(800);
  });

  it('releases an active stream hold without charging credits when client aborts early', async () => {
    const holdRes = await app.inject({
      method: 'POST',
      url: '/credits/holds',
      payload: {
        orgId: 'org_acme',
        requestId: 'req_stream_abort_01',
        modelName: 'gemini-2.5-pro',
        estimatedCredits: 500,
      },
    });
    const hold = holdRes.json();

    const releaseRes = await app.inject({
      method: 'POST',
      url: '/credits/holds/release',
      payload: {
        holdId: hold.holdId,
        reason: 'client_disconnected',
      },
    });

    expect(releaseRes.statusCode).toBe(200);
    expect(releaseRes.json().status).toBe('RELEASED');
    expect(releaseRes.json().releasedCredits).toBe(500);

    const balanceRes = await app.inject({
      method: 'GET',
      url: '/credits/organizations/org_acme/balance',
    });
    expect(balanceRes.json().reservedCredits).toBe(0);
    expect(balanceRes.json().availableCredits).toBe(1500);
  });

  it('settles a stream hold using token counts when explicit actualCredits is omitted', async () => {
    const holdRes = await app.inject({
      method: 'POST',
      url: '/credits/holds',
      payload: {
        orgId: 'org_acme',
        requestId: 'req_stream_tokens_01',
        modelName: 'gpt-4o',
        estimatedCredits: 200,
      },
    });
    const hold = holdRes.json();

    // 2000 input * 3/1k + 3000 output * 10/1k = 6 + 30 = 36 credits
    const settleRes = await app.inject({
      method: 'POST',
      url: '/credits/holds/settle',
      payload: {
        holdId: hold.holdId,
        inputTokens: 2000,
        outputTokens: 3000,
      },
    });

    expect(settleRes.statusCode).toBe(200);
    expect(settleRes.json().actualCredits).toBe(36);

    const balanceRes = await app.inject({
      method: 'GET',
      url: '/credits/organizations/org_acme/balance',
    });
    expect(balanceRes.json().availableCredits).toBe(1464);
    expect(balanceRes.json().reservedCredits).toBe(0);
  });

  it('rejects stream hold creation with 400 when model name is not in pricing catalog', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/credits/holds',
      payload: {
        orgId: 'org_acme',
        requestId: 'req_bad_model',
        modelName: 'unknown-model-v99',
        estimatedCredits: 100,
      },
    });

    expect(res.statusCode).toBe(400);
    expect(res.json().code).toBe('UNSUPPORTED_MODEL');
  });
  describe('transactional hold lifecycle', () => {
    type HoldOp = 'settle' | 'release' | 'extend';

    const post = (url: string, payload: Record<string, unknown>) =>
      app.inject({ method: 'POST', url, payload });

    const createHold = (estimatedCredits: number, orgId = 'org_acme') =>
      post('/credits/holds', {
        orgId,
        requestId: `req_${estimatedCredits}_${Math.random()}`,
        modelName: 'gpt-4o',
        estimatedCredits,
      });

    const runOp = (op: HoldOp, holdId: string) => {
      if (op === 'settle') return post('/credits/holds/settle', { holdId, actualCredits: 100 });
      if (op === 'release') return post('/credits/holds/release', { holdId });
      return post('/credits/holds/extend', { holdId, additionalTtlSeconds: 60, additionalCredits: 50 });
    };

    const holdStatus = async (holdId: string) =>
      (await query<{ status: string }>(`SELECT status FROM credit_holds WHERE hold_id = $1`, [holdId]))
        .rows[0].status;

    const usageEventCount = async () =>
      Number((await query<{ n: string }>(`SELECT COUNT(*)::text AS n FROM usage_events`)).rows[0].n);

    afterEach(() => {
      vi.restoreAllMocks();
    });

    it('excludes lots past expiry from hold creation and the balance endpoint before the sweep runs', async () => {
      await expireLot('grant_promo_01');

      const balance = (await app.inject({ method: 'GET', url: '/credits/organizations/org_acme/balance' })).json();
      expect(balance.availableCredits).toBe(1000);
      expect(balance.netSpendableCredits).toBe(1000);
      expect(balance.totalGrantedCredits).toBe(1000);
      expect(balance.activeLots.map((l: { grantId: string }) => l.grantId)).toEqual(['grant_inv_01']);

      const tooBig = await createHold(1200);
      expect(tooBig.statusCode).toBe(402);
      expect(tooBig.json().error).toContain('available 1000');
      expect((await createHold(1000)).statusCode).toBe(201);
    });

    it('excludes lots past expiry when extending a hold with additional credits', async () => {
      const hold = (await createHold(900)).json();
      await expireLot('grant_promo_01');

      const extend = await post('/credits/holds/extend', {
        holdId: hold.holdId,
        additionalTtlSeconds: 60,
        additionalCredits: 200,
      });
      expect(extend.statusCode).toBe(402);
      expect(extend.json().error).toContain('available 100');
      expect((await expectWalletInvariants('org_acme')).reserved).toBe(900);
    });

    it('never over-reserves under parallel hold creation', async () => {
      const responses = await Promise.all(Array.from({ length: 5 }, () => createHold(400)));

      expect(responses.filter((r) => r.statusCode === 201)).toHaveLength(3);
      expect(responses.filter((r) => r.statusCode === 402)).toHaveLength(2);
      expect((await expectWalletInvariants('org_acme')).reserved).toBe(1200);
    });

    it('lets exactly one of several concurrent settle/release calls on a hold win', async () => {
      const hold = (await createHold(300)).json();

      const responses = await Promise.all([
        runOp('settle', hold.holdId),
        runOp('settle', hold.holdId),
        runOp('release', hold.holdId),
        runOp('settle', hold.holdId),
      ]);

      expect(responses.filter((r) => r.statusCode === 200)).toHaveLength(1);
      const conflicts = responses.filter((r) => r.statusCode === 409);
      expect(conflicts).toHaveLength(3);
      expect(conflicts.every((r) => r.json().code === 'HOLD_STATE_CONFLICT')).toBe(true);
      expect(await usageEventCount()).toBeLessThanOrEqual(1);
      const state = await expectWalletInvariants('org_acme');
      expect(state.reserved).toBe(0);
    });

    it.each<[HoldOp, HoldOp]>([
      ['settle', 'settle'],
      ['settle', 'release'],
      ['settle', 'extend'],
      ['release', 'release'],
      ['release', 'settle'],
    ])('rejects %s followed by %s with 409 and leaves the wallet untouched', async (first, second) => {
      const hold = (await createHold(300)).json();
      expect((await runOp(first, hold.holdId)).statusCode).toBe(200);
      const before = await expectWalletInvariants('org_acme');

      const repeat = await runOp(second, hold.holdId);
      expect(repeat.statusCode).toBe(409);
      expect(repeat.json().code).toBe('HOLD_STATE_CONFLICT');
      expect(await expectWalletInvariants('org_acme')).toEqual(before);
    });

    it.each<HoldOp>(['settle', 'extend', 'release'])(
      'expires a hold past its TTL on %s, returns its reservation and responds 409',
      async (op) => {
        const hold = (await createHold(600)).json();
        await query(`UPDATE credit_holds SET expires_at = NOW() - INTERVAL '1 second' WHERE hold_id = $1`, [
          hold.holdId,
        ]);

        const res = await runOp(op, hold.holdId);
        expect(res.statusCode).toBe(409);
        expect(res.json().error).toContain('expired');
        expect(await holdStatus(hold.holdId)).toBe('EXPIRED');
        expect(await usageEventCount()).toBe(0);
        expect(await expectWalletInvariants('org_acme')).toMatchObject({ reserved: 0, cached: 1500 });

        // The sweep has nothing left to do for this hold.
        expect(await streamHoldService.releaseExpiredHolds()).toEqual({ releasedCount: 0, releasedCredits: 0 });
      }
    );

    const shortfalls = async () =>
      (
        await query<{ hold_id: string; usage_id: string | null; unrecovered_credits: string; reason: string }>(
          `SELECT hold_id, usage_id, unrecovered_credits, reason FROM billing_discrepancies
           WHERE discrepancy_type = 'SETTLEMENT_SHORTFALL'`
        )
      ).rows.map((r) => ({ ...r, unrecovered_credits: Number(r.unrecovered_credits) }));

    it('settles an over-budget stream without consuming a sibling hold\'s reservation', async () => {
      const a = (await createHold(600)).json();
      const b = (await createHold(800)).json();
      // 1500 credits, 1400 reserved: hold A may use its 600 plus the 100 free credits.

      const settled = await post('/credits/holds/settle', { holdId: a.holdId, actualCredits: 1000 });
      expect(settled.statusCode).toBe(200);
      expect(settled.json()).toMatchObject({ actualCredits: 1000, chargedCredits: 700, shortfallCredits: 300 });
      expect(await shortfalls()).toEqual([
        expect.objectContaining({ hold_id: a.holdId, usage_id: settled.json().usageId, unrecovered_credits: 300 }),
      ]);
      expect((await shortfalls())[0].reason).toContain('over-budget 300');

      // Sibling B is still fully backed and settles in full.
      expect(await expectWalletInvariants('org_acme')).toMatchObject({ cached: 800, reserved: 800 });
      const siblingSettle = await post('/credits/holds/settle', { holdId: b.holdId, actualCredits: 800 });
      expect(siblingSettle.json()).toMatchObject({ chargedCredits: 800, shortfallCredits: 0 });
      expect(await expectWalletInvariants('org_acme')).toMatchObject({ cached: 0, reserved: 0 });
      expect(await shortfalls()).toHaveLength(1);
    });

    it('settles a starved hold (its backing expired) from free credits only, logging the unbacked rest', async () => {
      const a = (await createHold(1000)).json();
      const b = (await createHold(200)).json();
      await expireLot('grant_promo_01');
      await creditLedgerService.expireStaleGrants(); // 1000 live credits now back 1200 reserved

      const settled = await post('/credits/holds/settle', { holdId: a.holdId, actualCredits: 1000 });
      expect(settled.statusCode).toBe(200);
      // Everything except sibling B's 200 reservation.
      expect(settled.json()).toMatchObject({ chargedCredits: 800, shortfallCredits: 200 });
      expect((await shortfalls())[0].reason).toContain('unbacked reservation 200');

      const siblingSettle = await post('/credits/holds/settle', { holdId: b.holdId, actualCredits: 200 });
      expect(siblingSettle.json()).toMatchObject({ chargedCredits: 200, shortfallCredits: 0 });
      expect(await expectWalletInvariants('org_acme')).toMatchObject({ cached: 0, reserved: 0 });
    });

    it('settles, deducts and commits the auto-recharge intent on one transaction handle (no nested transactions)', async () => {
      const hold = (await createHold(200, 'org_globex')).json();
      const kick = vi.spyOn(autoRechargeService, 'kickOutbox').mockImplementation(() => undefined);
      const connectSpy = vi.spyOn(pool, 'connect');
      const autocommitSpy = vi.spyOn(pool, 'query');

      // org_globex: 5000 credits, auto-recharge below 500 by 3000.
      const settled = await streamHoldService.settleStreamHold({ holdId: hold.holdId, actualCredits: 4600 });

      // One client/transaction for the whole settlement (reservation release, FIFO draw, usage
      // event, rollup delta, outbox intent), no statement outside of it, no synchronous top-up.
      expect(connectSpy).toHaveBeenCalledTimes(1);
      expect(autocommitSpy).not.toHaveBeenCalled();
      expect(settled.usageId).toBeTruthy();
      expect(kick).toHaveBeenCalledWith(['org_globex']);
      const intents = await query<{ status: string; triggered_by_usage_id: string }>(
        `SELECT status, triggered_by_usage_id FROM auto_recharge_outbox`
      );
      expect(intents.rows).toEqual([{ status: 'PENDING', triggered_by_usage_id: settled.usageId }]);
      expect(await expectWalletInvariants('org_globex')).toMatchObject({ reserved: 0, cached: 400 });

      kick.mockRestore();
      expect((await autoRechargeService.processOutbox()).ordersCreated).toBe(1);
      expect(await expectWalletInvariants('org_globex')).toMatchObject({ reserved: 0, cached: 3400 });
    });

    it('preserves wallet invariants under concurrent holds, settlements, deductions, sweeps and refunds', async () => {
      const holds = await Promise.all([createHold(200), createHold(150), createHold(100), createHold(50)]);
      const [h1, h2, h3, h4] = holds.map((r) => r.json().holdId as string);
      await query(`UPDATE credit_holds SET expires_at = NOW() - INTERVAL '1 second' WHERE hold_id = $1`, [h4]);
      await expireLot('grant_promo_01');

      const results = await Promise.allSettled([
        runOp('settle', h1),
        runOp('settle', h1),
        runOp('release', h2),
        runOp('extend', h3),
        runOp('settle', h4),
        createHold(120),
        createHold(5000),
        post('/credits/deduct', { orgId: 'org_acme', requestId: 'chaos_1', modelName: 'gpt-4o', credits: 90 }),
        post('/credits/deduct', { orgId: 'org_acme', requestId: 'chaos_2', modelName: 'gpt-4o', credits: 60 }),
        post('/webhooks/stripe', {
          eventId: 'evt_chaos_refund',
          eventType: 'charge.refunded',
          orgId: 'org_acme',
          invoiceId: 'inv_initial_1000',
          credits: 1000,
        }),
        creditLedgerService.expireStaleGrants(),
        streamHoldService.releaseExpiredHolds(),
      ]);

      // Only business-rule rejections are acceptable; never 500s (deadlocks, constraint violations...).
      for (const r of results) {
        expect(r.status).toBe('fulfilled');
        if (r.status === 'fulfilled' && r.value && typeof r.value === 'object' && 'statusCode' in r.value) {
          expect([200, 201, 402, 409]).toContain(r.value.statusCode);
        }
      }
      await expectWalletInvariants('org_acme');
    });
  });
});
