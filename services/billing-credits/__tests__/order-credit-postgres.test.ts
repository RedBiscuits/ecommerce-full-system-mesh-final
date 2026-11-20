/**
 * Real-PostgreSQL concurrency of the order-credit consumer (opt-in; provisioned automatically by
 * the root test run when PostgreSQL is installed, see __tests__/setup/postgres.global.ts).
 *
 * PGlite serializes whole transactions; here deliveries truly interleave, so this checks what the
 * broker can really do to the consumer: the same payment event delivered to several consumer
 * instances at once, re-announced under different event ids, and many orders capturing against one
 * org concurrently. Exactly one capture per payment, no deadlocks, wallet invariants intact.
 */
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { closeDb, query, resetAndSeedDatabase } from '../src/db';
import { closeRedis, flushRedis } from '../src/redis';
import { streamHoldService } from '../src/services/StreamHoldService';
import { orderCreditSettlementService } from '../src/integration/OrderCreditSettlementService';
import { expectWalletInvariants } from './helpers/wallet';
import { computeOrderTotals, halalasToSar, type HyperPayPaymentCompleted, type OrderLine } from '../../../shared/contracts';

const PG_URL = process.env.TEST_DATABASE_URL;
const LINES: OrderLine[] = [{ sku: 'RTR', nameEn: 'Router', nameAr: 'موجه', quantity: 1, unitPriceHalalas: 10000 }];

function paymentEvent(orderId: string, paymentId: string, eventId: string, credits: number): HyperPayPaymentCompleted {
  return {
    eventId,
    eventType: 'PAYMENT_COMPLETED',
    occurredAt: new Date().toISOString(),
    correlationId: orderId,
    source: 'webhook',
    origin: 'checkout',
    transactionId: paymentId,
    status: 'SUCCESS',
    amount: halalasToSar(computeOrderTotals(LINES, credits * 100).totalHalalas),
    currency: 'SAR',
    resultCode: '000.100.110',
    dbRecordId: `rec_${paymentId}`,
    order: { orderId, merchantId: 'txp_riyadh_retail', creditOrgId: 'org_acme', customer: { name: 'C' }, lines: LINES },
  };
}

async function count(sql: string, params: unknown[] = []): Promise<number> {
  return Number((await query<{ n: string }>(sql, params)).rows[0].n);
}

describe.skipIf(!PG_URL)('Order credit capture on real PostgreSQL (concurrent deliveries)', () => {
  beforeAll(async () => {
    await closeDb();
    process.env.DATABASE_URL = PG_URL;
  });

  beforeEach(async () => {
    await resetAndSeedDatabase();
    await flushRedis();
  });

  afterAll(async () => {
    await closeDb();
    await closeRedis();
    delete process.env.DATABASE_URL;
  });

  it('the same event delivered to 12 consumer instances at once captures exactly once', async () => {
    await streamHoldService.createOrderCreditHold({ orgId: 'org_acme', orderId: 'ord_pg_1', credits: 30 });
    const event = paymentEvent('ord_pg_1', 'pay_pg_1', 'evt_pg_1', 30);
    const results = await Promise.all(Array.from({ length: 12 }, () => orderCreditSettlementService.handlePaymentCompleted(event)));
    expect(results.filter((r) => r.outcome === 'CAPTURED')).toHaveLength(1);
    expect(results.filter((r) => r.outcome === 'DUPLICATE_IGNORED')).toHaveLength(11);
    expect(await count(`SELECT COUNT(*)::text AS n FROM usage_events`)).toBe(1);
    expect(await count(`SELECT COUNT(*)::text AS n FROM integration_outbox`)).toBe(1);
    await expectWalletInvariants('org_acme');
  });

  it('a payment re-announced under 12 different event ids at once captures exactly once (no false DLQ)', async () => {
    await streamHoldService.createOrderCreditHold({ orgId: 'org_acme', orderId: 'ord_pg_2', credits: 25 });
    const results = await Promise.allSettled(
      Array.from({ length: 12 }, (_, i) =>
        orderCreditSettlementService.handlePaymentCompleted(paymentEvent('ord_pg_2', 'pay_pg_2', `evt_pg_2_${i}`, 25))
      )
    );
    const outcomes = results.map((r) => (r.status === 'fulfilled' ? r.value.outcome : `ERR:${(r.reason as Error).message}`));
    expect(outcomes.filter((o) => o === 'CAPTURED')).toHaveLength(1);
    // Losers are duplicates; a loser that raced the settlement insert itself gets a retryable
    // unique violation and becomes a duplicate on its retry. None is dead-lettered as a conflict.
    expect(outcomes.filter((o) => o !== 'CAPTURED' && o !== 'DUPLICATE_IGNORED' && !/duplicate key/.test(o))).toEqual([]);
    for (const [i, r] of results.entries()) {
      if (r.status === 'rejected') {
        expect((await orderCreditSettlementService.handlePaymentCompleted(paymentEvent('ord_pg_2', 'pay_pg_2', `evt_pg_2_${i}`, 25))).outcome).toBe(
          'DUPLICATE_IGNORED'
        );
      }
    }
    expect(await count(`SELECT COUNT(*)::text AS n FROM usage_events`)).toBe(1);
    expect(await count(`SELECT COUNT(*)::text AS n FROM integration_outbox`)).toBe(1);
    await expectWalletInvariants('org_acme');
  });

  it('captures 24 orders of one org concurrently without deadlocks or lost credits', async () => {
    for (let i = 0; i < 24; i++) {
      await streamHoldService.createOrderCreditHold({ orgId: 'org_acme', orderId: `ord_pg_many_${i}`, credits: 10 });
    }
    const results = await Promise.all(
      Array.from({ length: 24 }, (_, i) =>
        orderCreditSettlementService.handlePaymentCompleted(paymentEvent(`ord_pg_many_${i}`, `pay_pg_many_${i}`, `evt_pg_many_${i}`, 10))
      )
    );
    expect(results.every((r) => r.outcome === 'CAPTURED')).toBe(true);
    const state = await expectWalletInvariants('org_acme');
    expect(state.usageCharged).toBe(240);
    expect(await count(`SELECT COUNT(*)::text AS n FROM integration_outbox`)).toBe(24);
  });
});
