import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { FastifyInstance } from 'fastify';
import { buildApp } from '../src/server';
import { query, resetAndSeedDatabase } from '../src/db';
import { closeRedis, flushRedis } from '../src/redis';
import { ORDER_CREDIT_MODEL, streamHoldService } from '../src/services/StreamHoldService';
import { orderCreditSettlementService } from '../src/integration/OrderCreditSettlementService';
import { HoldStateConflictError } from '../src/types/credits';
import { expectWalletInvariants } from './helpers/wallet';
import { HyperPayPaymentCompleted, computeOrderTotals, halalasToSar, type OrderLine } from '../../../shared/contracts';
import { NonRetryableError, readCheckpoint } from '../../../shared/messaging';

const LINES: OrderLine[] = [{ sku: 'RTR', nameEn: 'Router', nameAr: 'موجه', quantity: 2, unitPriceHalalas: 10000 }];

let seq = 0;
function paymentEvent(
  overrides: Partial<{ orderId: string; paymentId: string; eventId: string; status: 'SUCCESS' | 'FAILED' | 'PENDING'; credits: number; currency: string; withOrder: boolean; creditOrgId: string | null }> = {}
): HyperPayPaymentCompleted {
  const n = ++seq;
  const orderId = overrides.orderId ?? `ord_b_${n}`;
  const allowance = (overrides.credits ?? 0) * 100;
  return {
    eventId: overrides.eventId ?? `evt_pay_${n}`,
    eventType: 'PAYMENT_COMPLETED',
    occurredAt: new Date().toISOString(),
    correlationId: orderId,
    source: 'webhook',
    origin: 'checkout',
    transactionId: overrides.paymentId ?? `pay_b_${n}`,
    status: overrides.status ?? 'SUCCESS',
    amount: halalasToSar(computeOrderTotals(LINES, allowance).totalHalalas),
    currency: overrides.currency ?? 'SAR',
    resultCode: overrides.status === 'FAILED' ? '800.100.151' : '000.100.110',
    dbRecordId: `rec_${n}`,
    paymentBrand: 'VISA',
    ...(overrides.withOrder === false
      ? {}
      : {
          order: {
            orderId,
            merchantId: 'txp_riyadh_retail',
            creditOrgId: overrides.creditOrgId === undefined ? 'org_acme' : overrides.creditOrgId,
            customer: { name: 'Faisal' },
            lines: LINES,
          },
        }),
  };
}

async function count(sql: string, params: unknown[] = []): Promise<number> {
  return Number((await query<{ n: string }>(sql, params)).rows[0].n);
}

async function spendable(orgId: string): Promise<number> {
  const res = await query<{ s: string }>(
    `SELECT (SUM(available_credits) - SUM(reserved_credits))::text AS s FROM org_wallet_stripes WHERE org_id = $1`,
    [orgId]
  );
  return Number(res.rows[0].s);
}

describe('Order credit holds: captured on payment success, released on failure (exactly once)', () => {
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

  describe('placing the hold at checkout', () => {
    it('reserves credits for an order, idempotently, through the API', async () => {
      const first = await app.inject({ method: 'POST', url: '/credits/orders/ord_api/hold', payload: { orgId: 'org_acme', credits: 25 } });
      expect(first.statusCode).toBe(201);
      expect(first.json()).toMatchObject({ orderId: 'ord_api', estimatedCredits: 25, status: 'ACTIVE', created: true });
      const again = await app.inject({ method: 'POST', url: '/credits/orders/ord_api/hold', payload: { orgId: 'org_acme', credits: 25 } });
      expect(again.statusCode).toBe(200);
      expect(again.json()).toMatchObject({ holdId: first.json().holdId, created: false });
      expect(await spendable('org_acme')).toBe(1500 - 25);

      const conflicting = await app.inject({ method: 'POST', url: '/credits/orders/ord_api/hold', payload: { orgId: 'org_acme', credits: 30 } });
      expect(conflicting.statusCode).toBe(409);
      const invalid = await app.inject({ method: 'POST', url: '/credits/orders/ord_api/hold', payload: { orgId: 'org_acme', credits: 0 } });
      expect(invalid.statusCode).toBe(400);
    });

    it('never reserves twice for concurrent placements of the same order hold', async () => {
      const results = await Promise.allSettled(
        Array.from({ length: 6 }, () => streamHoldService.createOrderCreditHold({ orgId: 'org_acme', orderId: 'ord_race', credits: 10 }))
      );
      const holds = results.filter((r) => r.status === 'fulfilled').map((r) => (r as PromiseFulfilledResult<{ holdId: string }>).value.holdId);
      expect(new Set(holds).size).toBe(1);
      expect(await count(`SELECT COUNT(*)::text AS n FROM credit_holds WHERE request_id = 'order:ord_race' AND status = 'ACTIVE'`)).toBe(1);
      expect(await spendable('org_acme')).toBe(1500 - 10);
      await expectWalletInvariants('org_acme');
    });

    it('rejects an order hold the org cannot fund', async () => {
      await expect(streamHoldService.createOrderCreditHold({ orgId: 'org_acme', orderId: 'ord_big', credits: 5000 })).rejects.toThrow(
        /insufficient/i
      );
    });
  });

  describe('settling on the payment outcome', () => {
    it('SUCCESS captures the hold: credits charged (FIFO, rolled up), allowance in halalas, event in the outbox', async () => {
      const event = paymentEvent({ credits: 20 });
      const hold = await streamHoldService.createOrderCreditHold({ orgId: 'org_acme', orderId: event.order!.orderId, credits: 20 });

      const result = await orderCreditSettlementService.handlePaymentCompleted(event);
      expect(result).toMatchObject({ outcome: 'CAPTURED', capturedCredits: 20, allowanceHalalas: 2000 });

      const holdRow = await query(`SELECT status, charged_credits FROM credit_holds WHERE hold_id = $1`, [hold.holdId]);
      expect(holdRow.rows[0]).toMatchObject({ status: 'SETTLED' });
      expect(Number(holdRow.rows[0].charged_credits)).toBe(20);
      const usage = await query(`SELECT model_name, credits_charged, request_id FROM usage_events WHERE hold_id = $1`, [hold.holdId]);
      expect(usage.rows).toMatchObject([{ model_name: ORDER_CREDIT_MODEL, request_id: `order:${event.order!.orderId}` }]);
      expect(await spendable('org_acme')).toBe(1500 - 20);
      await expectWalletInvariants('org_acme');

      const outbox = await query(`SELECT event_id, exchange, routing_key, body FROM integration_outbox`);
      expect(outbox.rows).toHaveLength(1);
      expect(outbox.rows[0]).toMatchObject({
        event_id: result.settlementEventId,
        exchange: 'billing.events',
        routing_key: 'billing.order-credits.settled',
        body: {
          causationId: event.eventId,
          correlationId: event.order!.orderId,
          payload: {
            payment: { paymentId: event.transactionId, status: 'SUCCESS', amountHalalas: 20700, currency: 'SAR' },
            credits: { outcome: 'CAPTURED', capturedCredits: 20, allowanceHalalas: 2000, holdIds: [hold.holdId] },
          },
        },
      });
      expect(await readCheckpoint({ query: (t, p) => query(t, p) }, { consumer: 'billing.payment-results', eventId: event.eventId, step: 'done' }))
        .toMatchObject({ outcome: 'CAPTURED', correlationId: event.order!.orderId });
    });

    it('FAILED releases the hold and returns the reservation to the pool', async () => {
      const event = paymentEvent({ status: 'FAILED', credits: 40 });
      await streamHoldService.createOrderCreditHold({ orgId: 'org_acme', orderId: event.order!.orderId, credits: 40 });
      expect(await spendable('org_acme')).toBe(1460);

      const result = await orderCreditSettlementService.handlePaymentCompleted(event);
      expect(result).toMatchObject({ outcome: 'RELEASED', releasedCredits: 40, capturedCredits: 0, allowanceHalalas: 0 });
      expect(await spendable('org_acme')).toBe(1500);
      expect(await count(`SELECT COUNT(*)::text AS n FROM usage_events`)).toBe(0);
      await expectWalletInvariants('org_acme');
    });

    it('an order without a credit hold settles as NONE; PENDING and non-order payments are ignored', async () => {
      expect((await orderCreditSettlementService.handlePaymentCompleted(paymentEvent())).outcome).toBe('NONE');
      expect((await orderCreditSettlementService.handlePaymentCompleted(paymentEvent({ status: 'PENDING' }))).outcome).toBe('IGNORED_PENDING');
      expect((await orderCreditSettlementService.handlePaymentCompleted(paymentEvent({ withOrder: false }))).outcome).toBe('IGNORED_NO_ORDER');
      expect(await count(`SELECT COUNT(*)::text AS n FROM integration_outbox`)).toBe(1);
    });

    it('a redelivered event (same id) or a re-announced payment (new id) is a no-op', async () => {
      const event = paymentEvent({ credits: 5 });
      await streamHoldService.createOrderCreditHold({ orgId: 'org_acme', orderId: event.order!.orderId, credits: 5 });
      await orderCreditSettlementService.handlePaymentCompleted(event);
      expect((await orderCreditSettlementService.handlePaymentCompleted(event)).outcome).toBe('DUPLICATE_IGNORED');
      expect((await orderCreditSettlementService.handlePaymentCompleted({ ...event, eventId: 'evt_new_id' })).outcome).toBe('DUPLICATE_IGNORED');
      expect(await count(`SELECT COUNT(*)::text AS n FROM usage_events`)).toBe(1);
      expect(await count(`SELECT COUNT(*)::text AS n FROM integration_outbox`)).toBe(1);
      expect(await spendable('org_acme')).toBe(1495);
    });

    it('a second successful payment for a captured order is refused (needs a refund, not a second capture)', async () => {
      const first = paymentEvent({ orderId: 'ord_twice', paymentId: 'pay_1' });
      await orderCreditSettlementService.handlePaymentCompleted(first);
      const err = await orderCreditSettlementService
        .handlePaymentCompleted(paymentEvent({ orderId: 'ord_twice', paymentId: 'pay_2' }))
        .catch((e: unknown) => e);
      expect(err).toBeInstanceOf(NonRetryableError);
      expect(err).toMatchObject({ reason: 'ORDER_ALREADY_CAPTURED' });
    });

    it('refuses to capture an expired hold and rolls everything back (the sweep expires it later)', async () => {
      const event = paymentEvent({ credits: 15 });
      const hold = await streamHoldService.createOrderCreditHold({ orgId: 'org_acme', orderId: event.order!.orderId, credits: 15 });
      await query(`UPDATE credit_holds SET expires_at = NOW() - INTERVAL '1 second' WHERE hold_id = $1`, [hold.holdId]);

      await expect(orderCreditSettlementService.handlePaymentCompleted(event)).rejects.toMatchObject({
        reason: 'ORDER_CREDIT_HOLD_NOT_CAPTURABLE',
      });
      expect(await count(`SELECT COUNT(*)::text AS n FROM integration_checkpoints`)).toBe(0);
      expect(await count(`SELECT COUNT(*)::text AS n FROM order_credit_settlements`)).toBe(0);
      expect((await query(`SELECT status FROM credit_holds WHERE hold_id = $1`, [hold.holdId])).rows[0].status).toBe('ACTIVE');
    });

    it('a failed payment whose hold already expired settles as RELEASED (the credits are back either way)', async () => {
      const event = paymentEvent({ status: 'FAILED', credits: 15 });
      const hold = await streamHoldService.createOrderCreditHold({ orgId: 'org_acme', orderId: event.order!.orderId, credits: 15 });
      await query(`UPDATE credit_holds SET expires_at = NOW() - INTERVAL '1 second' WHERE hold_id = $1`, [hold.holdId]);
      const result = await orderCreditSettlementService.handlePaymentCompleted(event);
      expect(result).toMatchObject({ outcome: 'RELEASED', releasedCredits: 0 });
      expect((await query(`SELECT status FROM credit_holds WHERE hold_id = $1`, [hold.holdId])).rows[0].status).toBe('EXPIRED');
      expect(await spendable('org_acme')).toBe(1500);
      await expectWalletInvariants('org_acme');
    });

    it('refuses a hold of another org and a non-SAR payment', async () => {
      const event = paymentEvent({ credits: 5, creditOrgId: 'org_globex' });
      await streamHoldService.createOrderCreditHold({ orgId: 'org_acme', orderId: event.order!.orderId, credits: 5 });
      await expect(orderCreditSettlementService.handlePaymentCompleted(event)).rejects.toMatchObject({ reason: 'ORDER_CREDIT_ORG_MISMATCH' });
      await expect(orderCreditSettlementService.handlePaymentCompleted(paymentEvent({ currency: 'USD' }))).rejects.toMatchObject({
        reason: 'UNSUPPORTED_CURRENCY',
      });
    });

    it('is atomic: a failure while recording the outbox event undoes the capture and the checkpoint', async () => {
      const event = paymentEvent({ credits: 10 });
      const hold = await streamHoldService.createOrderCreditHold({ orgId: 'org_acme', orderId: event.order!.orderId, credits: 10 });
      await query(`CREATE OR REPLACE FUNCTION fail_outbox() RETURNS TRIGGER AS $$ BEGIN RAISE EXCEPTION 'outbox unavailable'; END; $$ LANGUAGE plpgsql`);
      await query(`CREATE TRIGGER trg_fail_outbox BEFORE INSERT ON integration_outbox FOR EACH ROW EXECUTE FUNCTION fail_outbox()`);
      try {
        await expect(orderCreditSettlementService.handlePaymentCompleted(event)).rejects.toThrow(/outbox unavailable/);
        expect((await query(`SELECT status FROM credit_holds WHERE hold_id = $1`, [hold.holdId])).rows[0].status).toBe('ACTIVE');
        expect(await count(`SELECT COUNT(*)::text AS n FROM usage_events`)).toBe(0);
        expect(await count(`SELECT COUNT(*)::text AS n FROM integration_checkpoints`)).toBe(0);
        expect(await count(`SELECT COUNT(*)::text AS n FROM order_credit_settlements`)).toBe(0);
        await expectWalletInvariants('org_acme');
      } finally {
        await query(`DROP TRIGGER IF EXISTS trg_fail_outbox ON integration_outbox`);
      }
      // The retry succeeds exactly once.
      expect((await orderCreditSettlementService.handlePaymentCompleted(event)).outcome).toBe('CAPTURED');
      expect(await count(`SELECT COUNT(*)::text AS n FROM usage_events`)).toBe(1);
    });

    it('exposes an order settlement with its checkpoints', async () => {
      const event = paymentEvent({ credits: 2 });
      await streamHoldService.createOrderCreditHold({ orgId: 'org_acme', orderId: event.order!.orderId, credits: 2 });
      await orderCreditSettlementService.handlePaymentCompleted(event);
      const res = await app.inject({ method: 'GET', url: `/credits/orders/${event.order!.orderId}/settlement` });
      expect(res.statusCode).toBe(200);
      expect(res.json()).toMatchObject({
        settlements: [{ outcome: 'CAPTURED', capturedCredits: 2, allowanceHalalas: 200 }],
        checkpoints: [{ consumer: 'billing.payment-results', step: 'done', outcome: 'CAPTURED' }],
      });
    });

    it('stream holds keep validating their model against the rate cards', async () => {
      await expect(
        streamHoldService.createStreamHold({ orgId: 'org_acme', requestId: 'r', modelName: ORDER_CREDIT_MODEL, estimatedCredits: 5 })
      ).rejects.toThrow(/Unsupported model/);
      expect(HoldStateConflictError).toBeDefined();
    });
  });
});
