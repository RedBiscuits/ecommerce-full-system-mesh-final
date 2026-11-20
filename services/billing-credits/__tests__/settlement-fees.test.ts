import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import type { FastifyInstance } from 'fastify';
import { buildApp } from '../src/server';
import { DEFAULT_SETTLEMENT_FEE_RULE, query, resetAndSeedDatabase } from '../src/db';
import { closeRedis, flushRedis } from '../src/redis';
import {
  SettlementFeeError,
  calculateSettlementFee,
  feeVatHalalas,
  maxNetFeeWithin,
  type SettlementFeeRule,
} from '../src/services/SettlementFeeCalculator';
import { BILLING_SETTLEMENT_FEE_CONSUMER, settlementFeeService } from '../src/integration/SettlementFeeService';
import {
  EVENT_TYPES,
  PRODUCERS,
  buildIntegrationEvent,
  computeOrderTotals,
  type MerchantSettlementCollectedEvent,
} from '../../../shared/contracts';
import { NonRetryableError, readCheckpoint } from '../../../shared/messaging';

const RULE: SettlementFeeRule = {
  ruleId: DEFAULT_SETTLEMENT_FEE_RULE.ruleId,
  version: 1,
  merchantId: null,
  tiers: DEFAULT_SETTLEMENT_FEE_RULE.tiers.map((t) => ({ ...t })),
  perTransactionHalalas: 50,
  minimumFeeHalalas: 0,
  vatRateBps: 1500,
};

let seq = 0;
function collected(overrides: Partial<{ batchId: string; eventId: string; grossHalalas: number; itemCount: number; merchantId: string }> = {}): MerchantSettlementCollectedEvent {
  const n = ++seq;
  const batchId = overrides.batchId ?? `stb_fee_${n}`;
  return buildIntegrationEvent({
    eventId: overrides.eventId ?? `evt_collected_${n}`,
    eventType: EVENT_TYPES.merchantSettlementCollected,
    producer: PRODUCERS.ledger,
    correlationId: batchId,
    causationId: null,
    aggregateId: batchId,
    payload: {
      batchId,
      merchantId: overrides.merchantId ?? 'txp_riyadh_retail',
      currency: 'SAR' as const,
      window: { from: null, to: new Date().toISOString() },
      itemCount: overrides.itemCount ?? 4,
      grossHalalas: overrides.grossHalalas ?? 1_234_567,
      firstSettledAt: new Date(Date.now() - 60_000).toISOString(),
      lastSettledAt: new Date().toISOString(),
      hasMore: false,
      collectedAt: new Date().toISOString(),
    },
  });
}

describe('settlement fee calculator (tiered, integer halalas)', () => {
  it('applies marginal tiers, rounds the percentage once (half-even) and adds the per-transaction fee', () => {
    // 80,000.00 SAR over 10 transactions: 50,000 @ 2.50 % + 30,000 @ 2.00 % = 1,250 + 600 = 1,850.00 SAR
    const { fee, payoutHalalas } = calculateSettlementFee(RULE, 8_000_000, 10);
    expect(fee.tiers.map((t) => [t.baseHalalas, t.feeHalalas])).toEqual([
      [5_000_000, 125_000],
      [3_000_000, 60_000],
      [0, 0],
    ]);
    expect(fee.percentageFeeHalalas).toBe(185_000);
    expect(fee.fixedFeeHalalas).toBe(500);
    expect(fee.netHalalas).toBe(185_500);
    expect(fee.vatHalalas).toBe(27_825);
    expect(fee.totalHalalas).toBe(213_325);
    expect(payoutHalalas).toBe(8_000_000 - 213_325);
    expect(fee.capped).toBe(false);
  });

  it('reaches the last, unbounded tier', () => {
    const { fee } = calculateSettlementFee(RULE, 30_000_000, 1);
    // 50k @ 2.5 % = 1,250 ; 200k @ 2 % = 4,000 ; 50k @ 1.5 % = 750  → 6,000.00 SAR
    expect(fee.tiers.map((t) => t.baseHalalas)).toEqual([5_000_000, 20_000_000, 5_000_000]);
    expect(fee.percentageFeeHalalas).toBe(600_000);
  });

  it('rounds the exact percentage once with banker\'s rounding and the tier shares sum to it exactly', () => {
    // 2.50 % of 1 halala = 0.025 → 0 ; of 20 = 0.5 → 0 (half to even) ; of 60 = 1.5 → 2
    expect(calculateSettlementFee({ ...RULE, perTransactionHalalas: 0 }, 20, 1).fee.percentageFeeHalalas).toBe(0);
    expect(calculateSettlementFee({ ...RULE, perTransactionHalalas: 0 }, 60, 1).fee.percentageFeeHalalas).toBe(2);
    for (const gross of [1, 99, 4_999_999, 5_000_001, 24_999_999, 25_000_001, 987_654_321]) {
      const { fee } = calculateSettlementFee(RULE, gross, 3);
      expect(fee.tiers.reduce((s, t) => s + t.feeHalalas, 0)).toBe(fee.percentageFeeHalalas);
      expect(fee.tiers.reduce((s, t) => s + t.baseHalalas, 0)).toBe(gross);
    }
  });

  it('computes the VAT on the fee exactly as the ZATCA engine invoices it', () => {
    for (let net = 0; net < 3_000; net += 7) {
      const expected = net === 0 ? 0 : computeOrderTotals([{ sku: 'F', nameEn: 'f', nameAr: 'f', quantity: 1, unitPriceHalalas: net }], 0).vatHalalas;
      expect(feeVatHalalas(net)).toBe(expected);
    }
  });

  it('never charges more than the gross (fee + VAT capped), and the cap is the largest possible fee', () => {
    const { fee, payoutHalalas } = calculateSettlementFee({ ...RULE, perTransactionHalalas: 5_000 }, 10_000, 5);
    expect(fee.capped).toBe(true);
    expect(fee.totalHalalas).toBeLessThanOrEqual(10_000);
    expect(payoutHalalas).toBe(10_000 - fee.totalHalalas);
    const net = maxNetFeeWithin(10_000);
    expect(net + feeVatHalalas(net)).toBeLessThanOrEqual(10_000);
    expect(net + 1 + feeVatHalalas(net + 1)).toBeGreaterThan(10_000);
  });

  it('applies the minimum fee', () => {
    const { fee } = calculateSettlementFee({ ...RULE, perTransactionHalalas: 0, minimumFeeHalalas: 1_000 }, 10_000, 1);
    expect(fee.netHalalas).toBe(1_000);
  });

  it('conserves every halala: fee + payout == gross over many amounts', () => {
    let x = 12345;
    for (let i = 0; i < 2_000; i++) {
      x = (x * 1103515245 + 12345) % 2 ** 31;
      const gross = 1 + (x % 40_000_000);
      const items = 1 + (x % 500);
      const { fee, payoutHalalas } = calculateSettlementFee(RULE, gross, items);
      expect(fee.totalHalalas + payoutHalalas).toBe(gross);
      expect(fee.netHalalas + fee.vatHalalas).toBe(fee.totalHalalas);
      expect(payoutHalalas).toBeGreaterThanOrEqual(0);
    }
  });

  it('refuses invalid rules and amounts', () => {
    expect(() => calculateSettlementFee({ ...RULE, tiers: [{ upToHalalas: 100, bps: 10 }] }, 1_000, 1)).toThrow(SettlementFeeError);
    expect(() => calculateSettlementFee({ ...RULE, tiers: [{ upToHalalas: 500, bps: 10 }, { upToHalalas: 100, bps: 5 }, { upToHalalas: null, bps: 1 }] }, 1_000, 1)).toThrow(/ascending/);
    expect(() => calculateSettlementFee(RULE, 0, 1)).toThrow(/gross/);
    expect(() => calculateSettlementFee(RULE, 100, 0)).toThrow(/itemCount/);
  });

  it('handles gross amounts beyond 2^53 / 10 000 without losing precision', () => {
    const gross = 900_000_000_000_000; // 9 trillion SAR
    const { fee, payoutHalalas } = calculateSettlementFee(RULE, gross, 1);
    expect(fee.totalHalalas + payoutHalalas).toBe(gross);
    expect(fee.tiers[2].baseHalalas).toBe(gross - 25_000_000);
  });
});

describe('SettlementFeeService (billing.settlement-fees consumer)', () => {
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

  it('prices a collected batch once: quote row, checkpoint and outbox event in one transaction', async () => {
    const event = collected({ grossHalalas: 8_000_000, itemCount: 10 });
    const result = await settlementFeeService.handleSettlementCollected(event);
    expect(result.outcome).toBe('PRICED');
    expect(result.fee?.totalHalalas).toBe(213_325);

    const quote = await settlementFeeService.getBatchFee(event.payload.batchId);
    expect(quote.quote).toMatchObject({ grossHalalas: 8_000_000, itemCount: 10, payoutHalalas: 8_000_000 - 213_325 });
    const outbox = await query<{ event_id: string; routing_key: string; body: { payload: { payoutHalalas: number } } }>(
      `SELECT event_id, routing_key, body FROM integration_outbox`
    );
    expect(outbox.rows).toHaveLength(1);
    expect(outbox.rows[0].routing_key).toBe('billing.settlement-fee.calculated');
    expect(outbox.rows[0].event_id).toBe(result.feeEventId);
    expect(outbox.rows[0].body.payload.payoutHalalas).toBe(8_000_000 - 213_325);
    const cp = await readCheckpoint({ query: (t, p) => query(t, p) }, { consumer: BILLING_SETTLEMENT_FEE_CONSUMER, eventId: event.eventId, step: 'done' });
    expect(cp?.outcome).toBe('PRICED');
  });

  it('is idempotent: a redelivery is ignored, a re-announcement under a new id keeps the frozen fee', async () => {
    const event = collected();
    const first = await settlementFeeService.handleSettlementCollected(event);
    expect((await settlementFeeService.handleSettlementCollected(event)).outcome).toBe('DUPLICATE_IGNORED');

    // The pricing changes; a re-announcement of the same batch (new event id) keeps the original fee.
    await settlementFeeService.publishRule({ ruleId: 'std-tiered', merchantId: null, tiers: [{ upToHalalas: null, bps: 900 }], perTransactionHalalas: 0, minimumFeeHalalas: 0 });
    const again = await settlementFeeService.handleSettlementCollected({ ...event, eventId: `${event.eventId}_again` });
    expect(again.outcome).toBe('ALREADY_PRICED');
    expect(again.fee).toEqual(first.fee);
    expect(Number((await query(`SELECT COUNT(*)::int AS n FROM settlement_fee_quotes`)).rows[0].n)).toBe(1);
    expect(Number((await query(`SELECT COUNT(*)::int AS n FROM integration_outbox`)).rows[0].n)).toBe(1);
  });

  it('refuses a batch re-announced with different totals (dead letter, nothing stored)', async () => {
    const event = collected();
    await settlementFeeService.handleSettlementCollected(event);
    await expect(
      settlementFeeService.handleSettlementCollected({ ...event, eventId: 'evt_other', payload: { ...event.payload, grossHalalas: 1 } })
    ).rejects.toMatchObject({ reason: 'BATCH_MISMATCH' });
    expect(await readCheckpoint({ query: (t, p) => query(t, p) }, { consumer: BILLING_SETTLEMENT_FEE_CONSUMER, eventId: 'evt_other', step: 'done' })).toBeNull();
  });

  it('uses the merchant\'s own active rule over the default, and a new version retires the previous one', async () => {
    await settlementFeeService.publishRule({
      ruleId: 'riyadh-retail-vip',
      merchantId: 'txp_riyadh_retail',
      tiers: [{ upToHalalas: null, bps: 100 }],
      perTransactionHalalas: 0,
      minimumFeeHalalas: 0,
    });
    const vip = await settlementFeeService.handleSettlementCollected(collected({ grossHalalas: 1_000_000 }));
    expect(vip.fee).toMatchObject({ ruleId: 'riyadh-retail-vip', ruleVersion: 1, netHalalas: 10_000 });
    const other = await settlementFeeService.handleSettlementCollected(collected({ grossHalalas: 1_000_000, merchantId: 'txp_other' }));
    expect(other.fee?.ruleId).toBe('std-tiered');

    await settlementFeeService.publishRule({
      ruleId: 'riyadh-retail-vip',
      merchantId: 'txp_riyadh_retail',
      tiers: [{ upToHalalas: null, bps: 50 }],
      perTransactionHalalas: 0,
      minimumFeeHalalas: 0,
    });
    const rules = await settlementFeeService.listRules();
    expect(rules.filter((r) => r.ruleId === 'riyadh-retail-vip').map((r) => [r.version, r.active])).toEqual([
      [1, false],
      [2, true],
    ]);
  });

  it('dead-letters when no rule applies', async () => {
    await query(`UPDATE settlement_fee_rules SET active = FALSE`);
    await expect(settlementFeeService.handleSettlementCollected(collected())).rejects.toBeInstanceOf(NonRetryableError);
  });

  it('serves quotes, rules and frozen batch fees over HTTP', async () => {
    const quote = await app.inject({ method: 'POST', url: '/billing/settlement-fees/quote', payload: { merchantId: 'txp_riyadh_retail', grossHalalas: 8_000_000, itemCount: 10 } });
    expect(quote.statusCode).toBe(200);
    expect(quote.json().fee.totalHalalas).toBe(213_325);
    expect((await app.inject({ method: 'POST', url: '/billing/settlement-fees/quote', payload: { merchantId: 'x', grossHalalas: -1, itemCount: 1 } })).statusCode).toBe(400);

    const rules = await app.inject({ method: 'GET', url: '/billing/settlement-fees/rules' });
    expect(rules.json().rules[0]).toMatchObject({ ruleId: 'std-tiered', active: true });

    const published = await app.inject({
      method: 'POST',
      url: '/billing/settlement-fees/rules',
      payload: { ruleId: 'std-tiered', merchantId: null, tiers: [{ upToHalalas: null, bps: 175 }] },
    });
    expect(published.statusCode).toBe(201);
    expect(published.json().version).toBe(2);

    const event = collected();
    await settlementFeeService.handleSettlementCollected(event);
    const batch = await app.inject({ method: 'GET', url: `/billing/settlement-fees/batches/${event.payload.batchId}` });
    expect(batch.statusCode).toBe(200);
    expect(batch.json().quote.fee.ruleVersion).toBe(2);
    expect(batch.json().checkpoints.map((c: { consumer: string }) => c.consumer)).toEqual([BILLING_SETTLEMENT_FEE_CONSUMER]);
    expect((await app.inject({ method: 'GET', url: '/billing/settlement-fees/batches/nope' })).statusCode).toBe(404);
  });
});
