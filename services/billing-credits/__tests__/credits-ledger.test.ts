import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { FastifyInstance } from 'fastify';
import { buildApp } from '../src/server';
import { resetAndSeedDatabase } from '../src/db';
import { closeRedis, flushRedis } from '../src/redis';

describe('Credit Ledger & Token Pricing', () => {
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

  it('returns organization balance summary and active credit lots ordered by expiration', async () => {
    const res = await app.inject({
      method: 'GET',
      url: '/credits/organizations/org_acme/balance',
    });

    expect(res.statusCode).toBe(200);
    const body = res.json();
    expect(body.orgId).toBe('org_acme');
    expect(body.planTier).toBe('GROWTH');
    expect(body.availableCredits).toBe(1500);
    expect(body.reservedCredits).toBe(0);
    expect(body.netSpendableCredits).toBe(1500);
    expect(body.activeLots).toHaveLength(2);
    expect(body.activeLots[0].grantId).toBe('grant_promo_01');
    expect(body.activeLots[1].grantId).toBe('grant_inv_01');
  });

  it('deducts usage across earliest-expiring credit lots first (FIFO)', async () => {
    const deductRes = await app.inject({
      method: 'POST',
      url: '/credits/deduct',
      payload: {
        orgId: 'org_acme',
        requestId: 'req_chat_101',
        modelName: 'claude-3-7-sonnet',
        credits: 700,
      },
    });

    expect(deductRes.statusCode).toBe(200);
    const deduction = deductRes.json();
    expect(deduction.creditsCharged).toBe(700);
    expect(deduction.remainingSpendableCredits).toBe(800);
    expect(deduction.allocations).toEqual([
      { grantId: 'grant_promo_01', creditsDeducted: 500 },
      { grantId: 'grant_inv_01', creditsDeducted: 200 },
    ]);

    const balanceRes = await app.inject({
      method: 'GET',
      url: '/credits/organizations/org_acme/balance',
    });
    const balance = balanceRes.json();
    expect(balance.availableCredits).toBe(800);
    expect(balance.activeLots).toHaveLength(1);
    expect(balance.activeLots[0].grantId).toBe('grant_inv_01');
    expect(balance.activeLots[0].remainingCredits).toBe(800);
  });

  it('calculates token-based usage cost and applies enterprise plan tier discount', async () => {
    const quoteRes = await app.inject({
      method: 'POST',
      url: '/credits/pricing/quote',
      payload: {
        modelName: 'claude-3-7-sonnet',
        inputTokens: 10000,
        outputTokens: 4000,
        cachedInputTokens: 10000,
        planTier: 'ENTERPRISE',
      },
    });

    expect(quoteRes.statusCode).toBe(200);
    const quote = quoteRes.json();
    // 10k * 3 + 4k * 15 + 10k * 1 = 30 + 60 + 10 = 100 raw credits; 10% enterprise discount = 90
    expect(quote.rawCredits).toBe(100);
    expect(quote.discountBps).toBe(1000);
    expect(quote.billableCredits).toBe(90);

    const deductRes = await app.inject({
      method: 'POST',
      url: '/credits/deduct',
      payload: {
        orgId: 'org_globex',
        requestId: 'req_ent_token_01',
        modelName: 'claude-3-7-sonnet',
        inputTokens: 10000,
        outputTokens: 4000,
        cachedInputTokens: 10000,
      },
    });

    expect(deductRes.statusCode).toBe(200);
    expect(deductRes.json().creditsCharged).toBe(90);
    expect(deductRes.json().remainingSpendableCredits).toBe(4910);
  });

  it('records usage history items with per-lot allocations', async () => {
    await app.inject({
      method: 'POST',
      url: '/credits/deduct',
      payload: {
        orgId: 'org_acme',
        requestId: 'req_hist_01',
        modelName: 'gpt-4o',
        inputTokens: 2000,
        outputTokens: 1000,
        credits: 600,
      },
    });

    const historyRes = await app.inject({
      method: 'GET',
      url: '/credits/organizations/org_acme/history?limit=10',
    });

    expect(historyRes.statusCode).toBe(200);
    const body = historyRes.json();
    expect(body.count).toBe(1);
    expect(body.items[0].requestId).toBe('req_hist_01');
    expect(body.items[0].creditsCharged).toBe(600);
    expect(body.items[0].allocations).toEqual([
      { grantId: 'grant_promo_01', creditsDeducted: 500 },
      { grantId: 'grant_inv_01', creditsDeducted: 100 },
    ]);
  });

  it('rejects usage deduction with 402 when requested credits exceed spendable balance', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/credits/deduct',
      payload: {
        orgId: 'org_acme',
        requestId: 'req_overlimit_303',
        modelName: 'claude-3-7-sonnet',
        credits: 2000,
      },
    });

    expect(res.statusCode).toBe(402);
    expect(res.json().code).toBe('INSUFFICIENT_CREDITS');
  });
});
