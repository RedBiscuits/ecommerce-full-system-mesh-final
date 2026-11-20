import { beforeEach, describe, expect, it } from 'vitest';
import type { PGlite } from '@electric-sql/pglite';
import { resetDatabase } from '../src/db';
import { buildApp } from '../src/server';
import { CheckoutCoordinator, CheckoutError, type CoordinatorOptions } from '../src/services/CheckoutCoordinator';
import { FakeBilling, FakeWallet } from './helpers/fakes';
import {
  EVENT_TYPES,
  PRODUCERS,
  buildIntegrationEvent,
  checkoutHoldKey,
  computeOrderTotals,
  type OrderLine,
} from '../../../shared/contracts';

const LINES: OrderLine[] = [
  { sku: 'RTR', nameEn: 'Router', nameAr: 'موجه', quantity: 1, unitPriceHalalas: 10000, vatCategory: 'S' },
  { sku: 'EXP', nameEn: 'Export', nameAr: 'تصدير', quantity: 1, unitPriceHalalas: 10000, vatCategory: 'Z', vatExemptionCode: 'VATEX-SA-32' },
  { sku: 'FIN', nameEn: 'Financing fee', nameAr: 'رسوم تمويل', quantity: 1, unitPriceHalalas: 5000, vatCategory: 'E', vatExemptionCode: 'VATEX-SA-29' },
];

const OPTIONS: CoordinatorOptions = {
  halalasPerCredit: 100,
  reservationTtlSeconds: 900,
  creditHoldTtlMarginSeconds: 300,
  retry: { maxAttempts: 3, baseDelayMs: 1, maxDelayMs: 2, minDelayMs: 0 },
  leaseMs: 30_000,
  defaultMerchantId: 'txp_riyadh_retail',
  sleep: () => Promise.resolve(),
};

let seq = 0;
function request(overrides: Record<string, unknown> = {}) {
  const n = ++seq;
  return {
    checkoutId: `co_${n}`,
    orderId: `ord_${n}`,
    customer: { name: 'Noura Al-Qahtani', email: 'noura@example.sa' },
    lines: LINES,
    promotion: { orgId: 'org_acme', credits: 25 },
    remainder: { tender: 'WALLET', walletId: 'wallet_noura_sar' },
    ...overrides,
  };
}

describe('CheckoutCoordinator (split-tender reservations, compensation, idempotency)', () => {
  let db: PGlite;
  let billing: FakeBilling;
  let wallet: FakeWallet;
  let coordinator: CheckoutCoordinator;
  let wakes: number;

  const make = (overrides: Partial<CoordinatorOptions> = {}) =>
    new CheckoutCoordinator(db, billing, wallet, { ...OPTIONS, onOutboxEnqueued: () => void wakes++, ...overrides });

  beforeEach(async () => {
    db = await resetDatabase();
    billing = new FakeBilling();
    wallet = new FakeWallet();
    wakes = 0;
    coordinator = make();
  });

  it('reserves the promotional credits and the remainder, and prices the remainder exactly', async () => {
    const { checkout, created } = await coordinator.startCheckout(request());
    expect(created).toBe(true);
    const totals = computeOrderTotals(LINES, 2500);
    expect(checkout).toMatchObject({
      status: 'RESERVED',
      promotion: { orgId: 'org_acme', credits: 25, allowanceHalalas: 2500 },
      payment: { tender: 'WALLET', walletId: 'wallet_noura_sar', amountHalalas: 23850, amountSar: '238.50' },
      holds: { credits: { status: 'PLACED', holdId: 'hold_1' }, remainder: { status: 'PLACED', holdId: 'ph_1' } },
      failure: null,
    });
    expect(checkout.amounts).toEqual(totals);
    expect(billing.calls).toEqual(['place']);
    expect(wallet.calls).toEqual(['place']);
    expect(billing.reservedCredits).toBe(25);
    expect(wallet.balances.get('wallet_noura_sar')).toBe(50000 - 23850);
    expect((await coordinator.listSteps(checkout.checkoutId)).map((s) => `${s.step}:${s.outcome}`)).toEqual([
      'CREATED:RESERVING',
      'RESERVE_CREDITS:PLACED',
      'RESERVE_REMAINDER:PLACED',
      'RESERVED:RESERVED',
    ]);
  });

  it('is idempotent per checkout id: a retried request returns the same checkout without reserving again', async () => {
    const body = request();
    const first = await coordinator.startCheckout(body);
    const again = await coordinator.startCheckout(body);
    const burst = await Promise.all([1, 2, 3].map(() => coordinator.startCheckout(body)));
    for (const r of [again, ...burst]) expect(r).toMatchObject({ created: false, checkout: { status: 'RESERVED', checkoutId: first.checkout.checkoutId } });
    expect(billing.calls).toEqual(['place']);
    expect(wallet.calls).toEqual(['place']);
    await expect(coordinator.startCheckout({ ...body, promotion: { orgId: 'org_acme', credits: 26 } })).rejects.toMatchObject({
      statusCode: 409,
      code: 'IDEMPOTENCY_KEY_REUSED',
    });
  });

  it('keeps one open checkout per order', async () => {
    const body = request();
    await coordinator.startCheckout(body);
    await expect(coordinator.startCheckout({ ...body, checkoutId: 'co_other' })).rejects.toMatchObject({ code: 'ORDER_HAS_OPEN_CHECKOUT' });
  });

  describe('atomicity: both reservations or none', () => {
    it('a refused wallet hold fails the credit hold for the same reason (credits released, nothing left held)', async () => {
      const { checkout } = await coordinator.startCheckout(request({ remainder: { tender: 'WALLET', walletId: 'wallet_noura_sar' }, lines: [{ ...LINES[0], unitPriceHalalas: 9_999_999 }] }));
      expect(checkout).toMatchObject({
        status: 'FAILED',
        failure: { code: 'INSUFFICIENT_FUNDS' },
        holds: { credits: { status: 'RELEASED' }, remainder: { status: 'RELEASED' } },
      });
      expect(billing.active()).toBe(0);
      expect(billing.reservedCredits).toBe(0);
      expect(wallet.held()).toBe(0);
      expect(wallet.balances.get('wallet_noura_sar')).toBe(50000);
      const steps = await coordinator.listSteps(checkout.checkoutId);
      expect(steps.find((s) => s.step === 'RESERVE_REMAINDER')).toMatchObject({ outcome: 'REJECTED', detail: { code: 'INSUFFICIENT_FUNDS' } });
      expect(steps.find((s) => s.step === 'COMPENSATE')).toMatchObject({ outcome: 'FAILED', detail: { code: 'INSUFFICIENT_FUNDS' } });
      expect(steps.find((s) => s.step === 'RELEASE_CREDITS')).toMatchObject({ outcome: 'RELEASED' });
    });

    it('any wallet refusal (unknown / frozen wallet) releases the credits with that code', async () => {
      wallet.fail('place', 'REJECTED', 'WALLET_FROZEN_PENDING_RECONCILIATION');
      const frozen = await coordinator.startCheckout(request());
      expect(frozen.checkout).toMatchObject({ status: 'FAILED', failure: { code: 'WALLET_FROZEN_PENDING_RECONCILIATION' } });
      const unknown = await coordinator.startCheckout(request({ remainder: { tender: 'WALLET', walletId: 'wallet_ghost' } }));
      expect(unknown.checkout).toMatchObject({ status: 'FAILED', failure: { code: 'WALLET_NOT_FOUND' } });
      expect(billing.active()).toBe(0);
    });

    it('refused credits never reach the wallet, and both keys are tombstoned', async () => {
      billing.fail('place', 'REJECTED', 'INSUFFICIENT_CREDITS', 1, 402);
      const { checkout } = await coordinator.startCheckout(request());
      expect(checkout).toMatchObject({ status: 'FAILED', failure: { code: 'INSUFFICIENT_CREDITS' } });
      expect(wallet.calls).toEqual(['release']); // tombstone only
      expect(wallet.holds.get(checkout.holds.remainder.key)?.status).toBe('RELEASED');
      expect(billing.holds.get(checkout.holds.credits.key)?.status).toBe('CANCELLED');
    });

    it('retries an unavailable service with the same key: one reservation, no double hold', async () => {
      wallet.fail('place', 'UNAVAILABLE', 'HTTP_503', 2);
      billing.fail('place', 'UNAVAILABLE', 'UNREACHABLE', 1);
      const { checkout } = await coordinator.startCheckout(request());
      expect(checkout.status).toBe('RESERVED');
      expect(billing.calls).toEqual(['place', 'place']);
      expect(wallet.calls).toEqual(['place', 'place', 'place']);
      expect(billing.reservedCredits).toBe(25);
      expect(wallet.holds.size).toBe(1);
    });

    it('gives up on a service that stays unavailable and releases what was reserved (tombstones block late placements)', async () => {
      wallet.fail('place', 'UNAVAILABLE', 'HTTP_503', 3);
      const { checkout } = await coordinator.startCheckout(request());
      expect(checkout).toMatchObject({ status: 'FAILED', failure: { code: 'WALLET_UNAVAILABLE' } });
      expect(billing.active()).toBe(0);
      // A placement that was still in flight lands afterwards: refused, no orphan hold.
      await expect(
        wallet.placeTenderHold({
          holdKey: checkout.holds.remainder.key,
          orderId: checkout.orderId,
          merchantId: checkout.merchantId,
          tender: 'WALLET',
          sourceWalletId: 'wallet_noura_sar',
          amountHalalas: checkout.payment.amountHalalas,
          checkoutId: checkout.checkoutId,
        })
      ).rejects.toMatchObject({ code: 'HOLD_RELEASED' });
      expect(wallet.held()).toBe(0);
    });

    it('resumes a checkout whose coordinator crashed after reserving the credits: nothing is reserved twice', async () => {
      billing.after.place = () => {
        billing.after.place = undefined;
        throw new Error('process crashed'); // the hold exists downstream, the coordinator never recorded it
      };
      const crashed = make({ leaseMs: 1, instanceId: 'instance-a' });
      await expect(crashed.startCheckout(request({ checkoutId: 'co_crash', orderId: 'ord_crash' }))).rejects.toThrow('process crashed');
      const stuck = await coordinator.getCheckout('co_crash');
      expect(stuck).toMatchObject({ status: 'RESERVING', holds: { credits: { status: 'PENDING' } } });

      await new Promise((r) => setTimeout(r, 5)); // the dead instance's lease expires
      const survivor = make({ instanceId: 'instance-b' });
      expect(await survivor.recoverStuck()).toEqual(['co_crash']);
      const resumed = await survivor.getCheckout('co_crash');
      expect(resumed).toMatchObject({ status: 'RESERVED', holds: { credits: { status: 'PLACED', holdId: 'hold_1' } } });
      expect(billing.reservedCredits).toBe(25); // the replayed placement returned the existing hold
      expect(billing.holds.size).toBe(1);
    });

    it('keeps compensating until both releases went through (recovery sweep), then fails the checkout', async () => {
      wallet.fail('place', 'REJECTED', 'INSUFFICIENT_FUNDS');
      billing.fail('release', 'UNAVAILABLE', 'HTTP_503', 3);
      const { checkout } = await coordinator.startCheckout(request());
      expect(checkout).toMatchObject({ status: 'COMPENSATING', holds: { credits: { status: 'PLACED' }, remainder: { status: 'RELEASED' } } });
      expect(billing.active()).toBe(1);

      expect(await coordinator.recoverStuck()).toEqual([checkout.checkoutId]);
      expect(await coordinator.getCheckout(checkout.checkoutId)).toMatchObject({
        status: 'FAILED',
        failure: { code: 'INSUFFICIENT_FUNDS' },
        holds: { credits: { status: 'RELEASED' } },
      });
      expect(billing.active()).toBe(0);
    });

    it('a checkout without a promotion reserves the remainder only', async () => {
      const { checkout } = await coordinator.startCheckout(request({ promotion: null }));
      expect(checkout).toMatchObject({ status: 'RESERVED', promotion: null, holds: { credits: { status: 'NOT_REQUIRED' } } });
      expect(checkout.payment.amountHalalas).toBe(computeOrderTotals(LINES, 0).totalHalalas);
      expect(billing.calls).toEqual([]);
    });

    it('refuses when billing values the credits differently (configuration drift) and releases both', async () => {
      billing.halalasPerCredit = 99;
      const { checkout } = await coordinator.startCheckout(request());
      expect(checkout).toMatchObject({ status: 'FAILED', failure: { code: 'ALLOWANCE_MISMATCH' } });
      expect(billing.active()).toBe(0);
      expect(wallet.held()).toBe(0);
    });
  });

  describe('lifecycle', () => {
    it('confirming a WALLET checkout hands it to the pipeline exactly once (transactional outbox)', async () => {
      const { checkout } = await coordinator.startCheckout(request());
      const confirmed = await coordinator.confirmCheckout(checkout.checkoutId);
      expect(confirmed).toMatchObject({ status: 'CONFIRMED', payment: { paymentId: `wallet:${checkout.checkoutId}` } });
      await coordinator.confirmCheckout(checkout.checkoutId);
      const outbox = await db.query<{ event_type: string; routing_key: string; body: any }>(`SELECT event_type, routing_key, body FROM integration_outbox`);
      expect(outbox.rows).toHaveLength(1);
      expect(outbox.rows[0]).toMatchObject({
        event_type: 'checkout.payment.completed',
        routing_key: 'checkout.payment.completed',
        body: {
          payload: {
            checkoutId: checkout.checkoutId,
            tender: 'WALLET',
            amountHalalas: 23850,
            order: { orderId: checkout.orderId, checkoutId: checkout.checkoutId, creditOrgId: 'org_acme', lines: LINES },
          },
        },
      });
      expect(wakes).toBe(1);
    });

    it('a CARD remainder is paid at the gateway (confirm is refused with the instructions)', async () => {
      const { checkout } = await coordinator.startCheckout(request({ remainder: { tender: 'CARD' } }));
      expect(checkout).toMatchObject({ status: 'RESERVED', payment: { tender: 'CARD', walletId: null } });
      expect(wallet.balances.get('wallet_noura_sar')).toBe(50000);
      await expect(coordinator.confirmCheckout(checkout.checkoutId)).rejects.toMatchObject({ code: 'PAY_REMAINDER_AT_GATEWAY' });
    });

    it('cancelling releases both reservations once; a cancelled checkout cannot be confirmed', async () => {
      const { checkout } = await coordinator.startCheckout(request());
      const cancelled = await coordinator.cancelCheckout(checkout.checkoutId);
      expect(cancelled).toMatchObject({ status: 'CANCELLED', holds: { credits: { status: 'RELEASED' }, remainder: { status: 'RELEASED' } } });
      await coordinator.cancelCheckout(checkout.checkoutId);
      expect(billing.calls.filter((c) => c === 'release')).toHaveLength(1);
      expect(wallet.balances.get('wallet_noura_sar')).toBe(50000);
      await expect(coordinator.confirmCheckout(checkout.checkoutId)).rejects.toMatchObject({ code: 'CHECKOUT_NOT_RESERVED' });
      // The order may be checked out again.
      expect((await coordinator.startCheckout({ ...request(), orderId: checkout.orderId })).checkout.status).toBe('RESERVED');
    });

    it('expires unpaid checkouts and releases their reservations', async () => {
      const shortLived = make({ reservationTtlSeconds: 0 });
      const { checkout } = await shortLived.startCheckout(request());
      expect(checkout.status).toBe('RESERVED');
      expect(await shortLived.expireDue()).toEqual([checkout.checkoutId]);
      expect(await shortLived.getCheckout(checkout.checkoutId)).toMatchObject({ status: 'EXPIRED', failure: { code: 'CHECKOUT_EXPIRED' } });
      expect(billing.active()).toBe(0);
      expect(wallet.held()).toBe(0);
    });

    it('a cancellation that loses the race against the capture leaves the checkout to complete', async () => {
      const { checkout } = await coordinator.startCheckout(request({ remainder: { tender: 'CARD' } }));
      wallet.holds.get(checkout.holds.remainder.key)!.status = 'CAPTURED'; // the card payment was captured meanwhile
      const view = await coordinator.cancelCheckout(checkout.checkoutId);
      expect(view).toMatchObject({ status: 'CONFIRMED', failure: { code: 'CANCELLED_TOO_LATE' } });
      expect(billing.active()).toBe(1); // the credits are captured by the pipeline, not released
    });

    it('closes checkouts from the ledger outcome, exactly once', async () => {
      const { checkout } = await coordinator.startCheckout(request());
      await coordinator.confirmCheckout(checkout.checkoutId);
      const settled = buildIntegrationEvent({
        eventId: 'evt_settled',
        eventType: EVENT_TYPES.ledgerPaymentSettled,
        producer: PRODUCERS.ledger,
        correlationId: checkout.orderId,
        causationId: 'x',
        aggregateId: checkout.orderId,
        payload: {
          orderId: checkout.orderId,
          paymentId: `wallet:${checkout.checkoutId}`,
          holdId: 'ph_1',
          merchantId: 'txp_riyadh_retail',
          currency: 'SAR' as const,
          journalRef: 'cap_ph_1',
          grossHalalas: 23850,
          netRevenueHalalas: 22500,
          vatHalalas: 1350,
          allowanceHalalas: 2500,
          capturedCredits: 25,
          paymentBrand: 'WALLET',
          tender: 'WALLET' as const,
          order: { orderId: checkout.orderId, merchantId: 'txp_riyadh_retail', creditOrgId: 'org_acme', customer: { name: 'N' }, lines: LINES, checkoutId: checkout.checkoutId },
          settledAt: new Date().toISOString(),
        },
      });
      expect(await coordinator.handleLedgerOutcome(settled)).toBe('COMPLETED');
      expect(await coordinator.handleLedgerOutcome(settled)).toBe('DUPLICATE_IGNORED');
      expect(await coordinator.getCheckout(checkout.checkoutId)).toMatchObject({
        status: 'COMPLETED',
        holds: { credits: { status: 'CAPTURED' }, remainder: { status: 'CAPTURED' } },
      });
    });

    it('marks a checkout whose card payment failed (the pipeline already released both holds)', async () => {
      const { checkout } = await coordinator.startCheckout(request({ remainder: { tender: 'CARD' } }));
      const released = buildIntegrationEvent({
        eventId: 'evt_released',
        eventType: EVENT_TYPES.ledgerPaymentReleased,
        producer: PRODUCERS.ledger,
        correlationId: checkout.orderId,
        causationId: 'x',
        aggregateId: checkout.orderId,
        payload: {
          orderId: checkout.orderId,
          paymentId: 'pay_1',
          holdId: 'ph_1',
          merchantId: 'txp_riyadh_retail',
          currency: 'SAR' as const,
          amountHalalas: checkout.payment.amountHalalas,
          reason: 'payment pay_1 failed (800.100.151)',
          releasedAt: new Date().toISOString(),
          tender: 'CARD' as const,
          checkoutId: checkout.checkoutId,
        },
      });
      expect(await coordinator.handleLedgerOutcome(released)).toBe('PAYMENT_FAILED');
      expect(await coordinator.getCheckout(checkout.checkoutId)).toMatchObject({
        status: 'PAYMENT_FAILED',
        failure: { code: 'PAYMENT_FAILED' },
        holds: { credits: { status: 'RELEASED' }, remainder: { status: 'RELEASED' } },
      });
    });
  });

  describe('validation', () => {
    it('refuses requests that cannot be priced', async () => {
      await expect(coordinator.startCheckout(request({ promotion: { orgId: 'org_acme', credits: 1000 } }))).rejects.toMatchObject({
        code: 'ALLOWANCE_EXCEEDS_NET',
        statusCode: 422,
      });
      const single: OrderLine[] = [{ sku: 'Z', nameEn: 'z', nameAr: 'z', quantity: 1, unitPriceHalalas: 500, vatCategory: 'Z', vatExemptionCode: 'VATEX-SA-33' }];
      await expect(coordinator.startCheckout(request({ lines: single, promotion: { orgId: 'org_acme', credits: 5 } }))).rejects.toMatchObject({
        code: 'REMAINDER_REQUIRED',
      });
      await expect(coordinator.startCheckout({ ...request(), remainder: { tender: 'WALLET' } })).rejects.toThrow();
      expect(CheckoutError).toBeDefined();
      expect(billing.calls).toEqual([]);
    });
  });

  describe('HTTP API', () => {
    it('maps the outcome of a checkout to its status code', async () => {
      const { app } = await buildApp({ billing, wallet, coordinator: OPTIONS });
      const ok = await app.inject({ method: 'POST', url: '/checkouts', payload: request() });
      expect(ok.statusCode).toBe(201);
      expect((await app.inject({ method: 'POST', url: '/checkouts', payload: { ...request(), checkoutId: undefined }, headers: { 'idempotency-key': 'co_hdr' } })).statusCode).toBe(201);
      const replay = await app.inject({ method: 'GET', url: `/checkouts/${ok.json().checkoutId}` });
      expect(replay.json().steps.length).toBeGreaterThan(0);
      wallet.fail('place', 'REJECTED', 'INSUFFICIENT_FUNDS');
      const refused = await app.inject({ method: 'POST', url: '/checkouts', payload: request() });
      expect(refused.statusCode).toBe(422);
      expect(refused.json()).toMatchObject({ status: 'FAILED', failure: { code: 'INSUFFICIENT_FUNDS' } });
      wallet.fail('place', 'UNAVAILABLE', 'HTTP_503', 3);
      expect((await app.inject({ method: 'POST', url: '/checkouts', payload: request() })).statusCode).toBe(503);
      expect((await app.inject({ method: 'POST', url: '/checkouts', payload: { orderId: 'x' } })).statusCode).toBe(400);
      expect((await app.inject({ method: 'POST', url: `/checkouts/${ok.json().checkoutId}/confirm` })).statusCode).toBe(202);
      expect((await app.inject({ method: 'POST', url: `/checkouts/${ok.json().checkoutId}/cancel` })).statusCode).toBe(409);
      expect((await app.inject({ method: 'GET', url: '/checkouts/nope' })).statusCode).toBe(404);
      expect(checkoutHoldKey('a', 'credits')).toBe('checkout:a:credits');
      await app.close();
    });
  });
});
