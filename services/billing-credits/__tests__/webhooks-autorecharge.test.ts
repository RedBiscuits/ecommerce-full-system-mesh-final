import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { FastifyInstance } from 'fastify';
import { buildApp } from '../src/server';
import { query, resetAndSeedDatabase } from '../src/db';
import { closeRedis, flushRedis } from '../src/redis';
import { creditLedgerService } from '../src/services/CreditLedgerService';
import { autoRechargeService } from '../src/services/AutoRechargeService';
import { expectWalletInvariants, expireLot } from './helpers/wallet';

describe('Stripe Webhooks & Auto-Recharge Policies', () => {
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

  it('grants prepaid invoice credits when invoice.paid webhook is received', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/webhooks/stripe',
      payload: {
        eventId: 'evt_stripe_paid_01',
        eventType: 'invoice.paid',
        orgId: 'org_acme',
        invoiceId: 'inv_topup_2500',
        credits: 2500,
        expiresInDays: 180,
      },
    });

    expect(res.statusCode).toBe(200);
    expect(res.json().outcome).toBe('GRANTED');
    expect(res.json().grantId).toBeDefined();

    const balanceRes = await app.inject({
      method: 'GET',
      url: '/credits/organizations/org_acme/balance',
    });
    expect(balanceRes.json().availableCredits).toBe(4000);
    expect(balanceRes.json().activeLots).toHaveLength(3);
  });

  it('revokes untouched invoice credit grant when charge.refunded webhook is received', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/webhooks/stripe',
      payload: {
        eventId: 'evt_stripe_refund_01',
        eventType: 'charge.refunded',
        orgId: 'org_acme',
        invoiceId: 'inv_initial_1000',
        credits: 1000,
      },
    });

    expect(res.statusCode).toBe(200);
    expect(res.json().outcome).toBe('REVOKED');

    const balanceRes = await app.inject({
      method: 'GET',
      url: '/credits/organizations/org_acme/balance',
    });
    expect(balanceRes.json().availableCredits).toBe(500);
    expect(balanceRes.json().activeLots).toHaveLength(1);
    expect(balanceRes.json().activeLots[0].grantId).toBe('grant_promo_01');
  });

  it('updates organization auto-recharge settings via PUT /credits/organizations/:orgId/auto-recharge', async () => {
    const updateRes = await app.inject({
      method: 'PUT',
      url: '/credits/organizations/org_acme/auto-recharge',
      payload: {
        enabled: true,
        thresholdCredits: 300,
        amountCredits: 1500,
        monthlyCapCredits: 6000,
      },
    });

    expect(updateRes.statusCode).toBe(200);
    expect(updateRes.json().enabled).toBe(true);
    expect(updateRes.json().thresholdCredits).toBe(300);

    const balanceRes = await app.inject({
      method: 'GET',
      url: '/credits/organizations/org_acme/balance',
    });
    expect(balanceRes.json().autoRecharge).toEqual({
      enabled: true,
      thresholdCredits: 300,
      amountCredits: 1500,
      monthlyCapCredits: 6000,
    });
  });

  it('automatically triggers an auto-recharge grant when usage drops balance below threshold', async () => {
    // org_globex starts with 5,000 credits, auto_recharge_enabled=true, threshold=500, amount=3000
    const deductRes = await app.inject({
      method: 'POST',
      url: '/credits/deduct',
      payload: {
        orgId: 'org_globex',
        requestId: 'req_globex_heavy_01',
        modelName: 'claude-3-7-sonnet',
        credits: 4600,
      },
    });

    expect(deductRes.statusCode).toBe(200);
    const body = deductRes.json();
    // 5000 - 4600 = 400 (<= 500 threshold): an intent is committed with the deduction; the top-up
    // itself runs asynchronously (outbox relay), so the response does not wait for it.
    expect(body.autoRechargeQueued).toBe(true);
    expect(body.remainingSpendableCredits).toBe(400);

    await autoRechargeService.whenOutboxIdle();
    // +3000 auto-recharge = 3400
    const balanceRes = await app.inject({
      method: 'GET',
      url: '/credits/organizations/org_globex/balance',
    });
    expect(balanceRes.json().availableCredits).toBe(3400);
  });
  describe('Stripe event idempotency and refund clawbacks', () => {
    const webhook = (payload: Record<string, unknown>) =>
      app.inject({ method: 'POST', url: '/webhooks/stripe', payload });

    const paid = (eventId: string, invoiceId = 'inv_topup_500', credits = 500) =>
      webhook({ eventId, eventType: 'invoice.paid', orgId: 'org_acme', invoiceId, credits });

    const refund = (eventId: string, invoiceId = 'inv_initial_1000', extra: Record<string, unknown> = {}) =>
      webhook({ eventId, eventType: 'charge.refunded', orgId: 'org_acme', invoiceId, credits: 1000, ...extra });

    const count = async (sql: string, params: unknown[] = []) =>
      Number((await query<{ n: string }>(sql, params)).rows[0].n);

    const discrepancies = () =>
      query<{ discrepancy_type: string; grant_id: string; invoice_id: string; unrecovered_credits: string }>(
        `SELECT discrepancy_type, grant_id, invoice_id, unrecovered_credits FROM billing_discrepancies`
      ).then((r) => r.rows.map((d) => ({ ...d, unrecovered_credits: Number(d.unrecovered_credits) })));

    it('grants exactly once when the same invoice.paid event is delivered concurrently', async () => {
      const responses = await Promise.all(Array.from({ length: 5 }, () => paid('evt_paid_dup')));

      expect(responses.every((r) => r.statusCode === 200)).toBe(true);
      const bodies = responses.map((r) => r.json());
      expect(bodies.filter((b) => b.outcome === 'GRANTED')).toHaveLength(1);
      expect(bodies.filter((b) => b.outcome === 'DUPLICATE_IGNORED')).toHaveLength(4);
      expect(new Set(bodies.map((b) => b.grantId)).size).toBe(1);

      expect(await count(`SELECT COUNT(*)::text AS n FROM credit_grants WHERE invoice_id = 'inv_topup_500'`)).toBe(1);
      const events = await query<{ outcome: string }>(`SELECT outcome FROM stripe_webhook_events`);
      expect(events.rows).toEqual([{ outcome: 'GRANTED' }]);
      expect((await expectWalletInvariants('org_acme')).cached).toBe(2000);
    });

    it('ignores sequential redeliveries and new events for an already granted invoice', async () => {
      const first = (await paid('evt_paid_a')).json();
      const redelivery = (await paid('evt_paid_a')).json();
      const sameInvoiceNewEvent = (await paid('evt_paid_b')).json();

      expect(first.outcome).toBe('GRANTED');
      expect(redelivery).toMatchObject({ outcome: 'DUPLICATE_IGNORED', grantId: first.grantId });
      expect(sameInvoiceNewEvent).toMatchObject({ outcome: 'DUPLICATE_IGNORED', grantId: first.grantId });
      expect((await expectWalletInvariants('org_acme')).cached).toBe(2000);
    });

    it('revokes an untouched grant without logging a discrepancy', async () => {
      const res = (await refund('evt_refund_clean')).json();

      expect(res).toMatchObject({ outcome: 'REVOKED', grantId: 'grant_inv_01', revokedCredits: 1000 });
      expect(await discrepancies()).toEqual([]);
      expect((await expectWalletInvariants('org_acme')).cached).toBe(500);
    });

    it('claws back the unspent part of a partially consumed grant and logs the consumed part', async () => {
      // 700 credits: 500 from the promo lot, 200 from grant_inv_01 -> 800 left on grant_inv_01.
      await creditLedgerService.deductUsage({
        orgId: 'org_acme',
        requestId: 'req_before_refund',
        modelName: 'gpt-4o',
        credits: 700,
      });

      const res = (await refund('evt_refund_partial')).json();
      expect(res).toMatchObject({
        outcome: 'DISCREPANCY_LOGGED',
        grantId: 'grant_inv_01',
        revokedCredits: 800,
        unrecoveredCredits: 200,
      });

      const grant = await query<{ status: string; remaining_credits: string }>(
        `SELECT status, remaining_credits FROM credit_grant_balances WHERE grant_id = 'grant_inv_01'`
      );
      expect(grant.rows[0].status).toBe('REVOKED');
      expect(Number(grant.rows[0].remaining_credits)).toBe(0);
      expect(await discrepancies()).toEqual([
        {
          discrepancy_type: 'REFUND_UNRECOVERABLE',
          grant_id: 'grant_inv_01',
          invoice_id: 'inv_initial_1000',
          unrecovered_credits: 200,
        },
      ]);
      expect((await expectWalletInvariants('org_acme')).cached).toBe(0);
    });

    it('never claws back twice: redelivered and second refund events are ignored', async () => {
      await creditLedgerService.deductUsage({ orgId: 'org_acme', requestId: 'r', modelName: 'gpt-4o', credits: 700 });
      const responses = await Promise.all([
        refund('evt_refund_once'),
        refund('evt_refund_once'),
        refund('evt_refund_other'),
      ]);

      const outcomes = responses.map((r) => r.json().outcome).sort();
      expect(outcomes).toEqual(['DISCREPANCY_LOGGED', 'DUPLICATE_IGNORED', 'DUPLICATE_IGNORED']);
      expect(await discrepancies()).toHaveLength(1);
      expect((await expectWalletInvariants('org_acme')).cached).toBe(0);
    });

    it('targets the grant issued by the original event id and rejects mismatched references', async () => {
      const issued = (await paid('evt_paid_src', 'inv_src_1')).json();

      const wrongOrg = await webhook({
        eventId: 'evt_refund_wrong_org',
        eventType: 'charge.refunded',
        orgId: 'org_globex',
        invoiceId: 'inv_src_1',
        credits: 500,
        originalEventId: 'evt_paid_src',
      });
      expect(wrongOrg.statusCode).toBe(404);
      // A failed delivery is not recorded, so Stripe's retry will be processed.
      expect(await count(`SELECT COUNT(*)::text AS n FROM stripe_webhook_events WHERE event_id = 'evt_refund_wrong_org'`)).toBe(0);

      const res = (await refund('evt_refund_src', 'inv_src_1', { originalEventId: 'evt_paid_src' })).json();
      expect(res).toMatchObject({ outcome: 'REVOKED', grantId: issued.grantId, revokedCredits: 500 });
      expect((await expectWalletInvariants('org_acme')).cached).toBe(1500);
    });

    it('returns 404 for a refund of an unknown invoice without recording the event', async () => {
      const res = await refund('evt_refund_unknown', 'inv_does_not_exist');
      expect(res.statusCode).toBe(404);
      expect(await count(`SELECT COUNT(*)::text AS n FROM stripe_webhook_events`)).toBe(0);
      await expectWalletInvariants('org_acme');
    });

    const refundOf = (eventId: string, credits: number) =>
      webhook({ eventId, eventType: 'charge.refunded', orgId: 'org_acme', invoiceId: 'inv_initial_1000', credits });

    const grantState = async () =>
      (
        await query<{ status: string; remaining_credits: string; refunded_credits: string }>(
          `SELECT status, remaining_credits, refunded_credits FROM credit_grant_balances WHERE grant_id = 'grant_inv_01'`
        )
      ).rows.map((r) => ({
        status: r.status,
        remaining: Number(r.remaining_credits),
        refunded: Number(r.refunded_credits),
      }))[0];

    it('applies partial refunds cumulatively and revokes the grant once fully refunded', async () => {
      const partial = (await refundOf('evt_partial_1', 400)).json();
      expect(partial).toMatchObject({ outcome: 'PARTIALLY_REVOKED', refundedCredits: 400, revokedCredits: 400 });
      expect(await grantState()).toEqual({ status: 'ACTIVE', remaining: 600, refunded: 400 });
      expect((await expectWalletInvariants('org_acme')).cached).toBe(1100);

      // Refunding more than is left is capped at the refundable remainder.
      const rest = (await refundOf('evt_partial_2', 900)).json();
      expect(rest).toMatchObject({ outcome: 'REVOKED', refundedCredits: 600, revokedCredits: 600 });
      expect(await grantState()).toEqual({ status: 'REVOKED', remaining: 0, refunded: 1000 });

      expect((await refundOf('evt_partial_3', 100)).json().outcome).toBe('DUPLICATE_IGNORED');
      expect((await expectWalletInvariants('org_acme')).cached).toBe(500);
      expect(await discrepancies()).toEqual([]);
    });

    it('logs only consumed credits as unrecoverable across successive partial refunds', async () => {
      // 1300 = 500 promo + 800 of grant_inv_01 -> 200 left on the invoice grant.
      await creditLedgerService.deductUsage({ orgId: 'org_acme', requestId: 'r_big', modelName: 'gpt-4o', credits: 1300 });

      const first = (await refundOf('evt_consumed_1', 500)).json();
      expect(first).toMatchObject({ outcome: 'DISCREPANCY_LOGGED', revokedCredits: 200, unrecoveredCredits: 300 });
      const second = (await refundOf('evt_consumed_2', 500)).json();
      expect(second).toMatchObject({ outcome: 'DISCREPANCY_LOGGED', revokedCredits: 0, unrecoveredCredits: 500 });

      const logged = await discrepancies();
      expect(logged.map((d) => [d.discrepancy_type, d.unrecovered_credits])).toEqual([
        ['REFUND_UNRECOVERABLE', 300],
        ['REFUND_UNRECOVERABLE', 500],
      ]);
      // Exactly the 800 consumed credits, never more.
      expect(logged.reduce((sum, d) => sum + d.unrecovered_credits, 0)).toBe(800);
      expect(await grantState()).toMatchObject({ status: 'REVOKED', remaining: 0 });
      expect((await expectWalletInvariants('org_acme')).cached).toBe(0);
    });

    it('logs REVOKED_DURING_ACTIVE_HOLD when a clawback leaves active holds unbacked', async () => {
      const hold = await app.inject({
        method: 'POST',
        url: '/credits/holds',
        payload: { orgId: 'org_acme', requestId: 'r_hold_refund', modelName: 'gpt-4o', estimatedCredits: 1200 },
      });
      expect(hold.statusCode).toBe(201);

      const res = (await refund('evt_refund_under_hold')).json();
      // 1500 -> 500 effective credits against 1200 reserved: 700 held credits lost their backing.
      expect(res).toMatchObject({ outcome: 'REVOKED', revokedCredits: 1000, unbackedHoldCredits: 700 });
      expect(await discrepancies()).toEqual([
        {
          discrepancy_type: 'REVOKED_DURING_ACTIVE_HOLD',
          grant_id: 'grant_inv_01',
          invoice_id: 'inv_initial_1000',
          unrecovered_credits: 700,
        },
      ]);
      expect(await expectWalletInvariants('org_acme')).toMatchObject({ cached: 500, reserved: 1200 });
    });

    it('revokes a grant past expiry that the sweep has not retired yet, without double subtraction', async () => {
      await expireLot('grant_inv_01');

      const res = (await refund('evt_refund_expired')).json();
      expect(res).toMatchObject({ outcome: 'REVOKED', revokedCredits: 1000 });
      expect((await expectWalletInvariants('org_acme')).cached).toBe(500);

      expect((await creditLedgerService.expireStaleGrants()).expiredLotsCount).toBe(0);
      expect((await expectWalletInvariants('org_acme')).cached).toBe(500);
    });
  });
});
