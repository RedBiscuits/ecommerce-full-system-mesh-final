import crypto from 'crypto';
import os from 'os';
import type { PGlite, Transaction } from '@electric-sql/pglite';
import { z } from 'zod';
import {
  CONSUMERS,
  CheckoutPaymentCompletedPayload,
  EVENT_TYPES,
  EXCHANGES,
  LedgerPaymentReleasedPayload,
  LedgerPaymentSettledPayload,
  OrderContext,
  OrderCustomerSchema,
  OrderLineSchema,
  OrderTotals,
  OrderTotalsError,
  PRODUCERS,
  ROUTING_KEYS,
  Tender,
  buildIntegrationEvent,
  checkoutHoldKey,
  computeOrderTotals,
  deterministicEventId,
  halalasToSar,
  walletPaymentId,
  type IntegrationEvent,
} from '../../../../shared/contracts';
import { claimCheckpoint, completeCheckpoint, enqueueIntegrationEvent, type RetryPolicy } from '../../../../shared/messaging';
import type { BillingPort, WalletPort } from '../clients/ports';
import { DownstreamError, withRetries } from '../clients/http';

export const CHECKOUT_OUTCOME_CONSUMER = CONSUMERS.checkoutLedgerOutcomes;

export const CheckoutRequestSchema = z.object({
  /** Idempotency key of the checkout: retries with the same id and request return the same checkout. */
  checkoutId: z.string().min(1).max(100),
  orderId: z.string().min(1).max(100),
  merchantId: z.string().min(1).max(64).optional(),
  customer: OrderCustomerSchema,
  lines: z.array(OrderLineSchema).min(1).max(500),
  /** Promotional credits of a billing org, applied as a document-level allowance. */
  promotion: z
    .object({ orgId: z.string().min(1).max(64), credits: z.number().int().positive().safe() })
    .nullable()
    .optional(),
  /** How the remainder (lines − promotion + VAT) is paid. */
  remainder: z.discriminatedUnion('tender', [
    z.object({ tender: z.literal('CARD') }),
    z.object({ tender: z.literal('WALLET'), walletId: z.string().min(1).max(64) }),
  ]),
});
export type CheckoutRequest = z.infer<typeof CheckoutRequestSchema>;

export type CheckoutStatus =
  | 'RESERVING'
  | 'RESERVED'
  | 'CONFIRMED'
  | 'COMPLETED'
  | 'COMPENSATING'
  | 'FAILED'
  | 'CANCELLED'
  | 'EXPIRED'
  | 'PAYMENT_FAILED';

type HoldStatus = 'PENDING' | 'NOT_REQUIRED' | 'PLACED' | 'RELEASED' | 'CAPTURED';
type CompensationTarget = 'FAILED' | 'CANCELLED' | 'EXPIRED';

interface CheckoutRow {
  checkout_id: string;
  request_fingerprint: string;
  order_id: string;
  merchant_id: string;
  request: CheckoutRequest;
  promotion_org_id: string | null;
  promotion_credits: string | number;
  tender: Tender;
  source_wallet_id: string | null;
  allowance_halalas: string | number;
  total_halalas: string | number;
  totals: OrderTotals;
  credit_hold_key: string;
  credit_hold_id: string | null;
  credit_hold_status: HoldStatus;
  remainder_hold_key: string;
  remainder_hold_id: string | null;
  remainder_hold_status: HoldStatus;
  status: CheckoutStatus;
  compensation_target: CompensationTarget | null;
  failure_code: string | null;
  failure_message: string | null;
  payment_id: string | null;
  lease_owner: string | null;
  lease_until: Date | null;
  expires_at: Date;
  created_at: Date;
  updated_at: Date;
}

export interface CheckoutView {
  checkoutId: string;
  orderId: string;
  merchantId: string;
  status: CheckoutStatus;
  amounts: OrderTotals;
  promotion: { orgId: string; credits: number; allowanceHalalas: number } | null;
  /** What the remainder tender pays, and how (for CARD: through HyperPay with customParameters.checkoutId). */
  payment: { tender: Tender; walletId: string | null; amountHalalas: number; amountSar: string; paymentId: string | null };
  holds: {
    credits: { key: string; holdId: string | null; status: HoldStatus };
    remainder: { key: string; holdId: string | null; status: HoldStatus };
  };
  failure: { code: string; message: string | null } | null;
  expiresAt: string;
  createdAt: string;
  updatedAt: string;
}

export class CheckoutError extends Error {
  constructor(
    public readonly statusCode: number,
    public readonly code: string,
    message: string,
    public readonly checkout?: CheckoutView
  ) {
    super(message);
    this.name = 'CheckoutError';
  }
}

export interface CoordinatorOptions {
  halalasPerCredit: number;
  reservationTtlSeconds: number;
  creditHoldTtlMarginSeconds: number;
  retry: RetryPolicy;
  leaseMs: number;
  defaultMerchantId: string;
  instanceId?: string;
  sleep?: (ms: number) => Promise<void>;
  /** Called after a transaction that wrote an outbox event commits (wakes the relay). */
  onOutboxEnqueued?: () => void;
}

/**
 * Split-tender checkout coordinator (saga orchestrator).
 *
 * A checkout reserves BOTH tenders before the customer pays:
 *   1. promotional credits at billing-credits  (order credit hold, key checkout:<id>:credits)
 *   2. the remainder at wallet-ledger          (CARD authorization or WALLET funds into escrow,
 *                                               key checkout:<id>:remainder)
 * and is only RESERVED when both are. If the remainder hold is refused (insufficient wallet
 * balance, frozen / unknown wallet, …) the credit hold is released with that same failure code;
 * if any reservation cannot be made, everything already reserved is released (compensation).
 *
 * No double reservation, no orphan hold:
 *   - every downstream call carries a deterministic idempotency key, so a retried call (timeout,
 *     5xx, crash and recovery) returns the same hold instead of reserving twice;
 *   - releasing a key that has no hold yet tombstones it downstream, so a placement still in
 *     flight is refused when it lands;
 *   - each step is persisted before the next one starts; a crashed instance's checkout is resumed
 *     (or compensated) by the recovery sweep once its lease expired; abandoned checkouts expire.
 *
 * Completion: WALLET → confirm() emits checkout.payment.completed (transactional outbox) into the
 * order pipeline; CARD → the customer pays the remainder through HyperPay with
 * customParameters.checkoutId. The ledger's settled / released event closes the checkout.
 */
export class CheckoutCoordinator {
  private readonly instanceId: string;
  private readonly chains = new Map<string, Promise<unknown>>();

  constructor(
    private readonly db: PGlite,
    private readonly billing: BillingPort,
    private readonly wallet: WalletPort,
    private readonly options: CoordinatorOptions
  ) {
    this.instanceId = options.instanceId ?? `${os.hostname()}:${process.pid}:${crypto.randomBytes(3).toString('hex')}`;
  }

  // ── API ───────────────────────────────────────────────────────────────────────

  async startCheckout(raw: unknown): Promise<{ checkout: CheckoutView; created: boolean }> {
    const request = CheckoutRequestSchema.parse(raw);
    const normalized: CheckoutRequest = { ...request, merchantId: request.merchantId ?? this.options.defaultMerchantId };
    const fingerprint = fingerprintOf(normalized);
    const credits = normalized.promotion?.credits ?? 0;
    const allowance = credits * this.options.halalasPerCredit;

    let totals: OrderTotals;
    try {
      totals = computeOrderTotals(normalized.lines, allowance);
    } catch (err) {
      if (err instanceof OrderTotalsError) throw new CheckoutError(422, err.reason, err.message);
      throw err;
    }
    if (totals.totalHalalas <= 0) {
      throw new CheckoutError(422, 'REMAINDER_REQUIRED', 'The promotion covers the whole order: nothing is left for the remainder tender');
    }

    const id = normalized.checkoutId;
    const inserted = await this.serialize(id, async () => {
      const existing = await this.load(id);
      if (existing) {
        if (existing.request_fingerprint !== fingerprint) {
          throw new CheckoutError(409, 'IDEMPOTENCY_KEY_REUSED', `Checkout ${id} exists with a different request`, toView(existing));
        }
        return false;
      }
      try {
        await this.db.query(
          `INSERT INTO checkouts (checkout_id, request_fingerprint, order_id, merchant_id, request, promotion_org_id, promotion_credits,
             tender, source_wallet_id, allowance_halalas, total_halalas, totals, credit_hold_key, credit_hold_status,
             remainder_hold_key, status, expires_at)
           VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8, $9, $10, $11, $12::jsonb, $13, $14, $15, 'RESERVING',
             NOW() + ($16::int * INTERVAL '1 second'))`,
          [
            id,
            fingerprint,
            normalized.orderId,
            normalized.merchantId,
            JSON.stringify(normalized),
            normalized.promotion?.orgId ?? null,
            credits,
            normalized.remainder.tender,
            normalized.remainder.tender === 'WALLET' ? normalized.remainder.walletId : null,
            allowance,
            totals.totalHalalas,
            JSON.stringify(totals),
            checkoutHoldKey(id, 'credits'),
            credits > 0 ? 'PENDING' : 'NOT_REQUIRED',
            checkoutHoldKey(id, 'remainder'),
            this.options.reservationTtlSeconds,
          ]
        );
      } catch (err) {
        if ((err as { code?: string }).code === '23505') {
          throw new CheckoutError(409, 'ORDER_HAS_OPEN_CHECKOUT', `Order ${normalized.orderId} already has an open checkout`);
        }
        throw err;
      }
      await this.step(this.db, id, 'CREATED', 'RESERVING', { totalHalalas: totals.totalHalalas, allowanceHalalas: allowance });
      return true;
    });

    const row = await this.advance(id);
    return { checkout: toView(row), created: inserted };
  }

  async getCheckout(checkoutId: string): Promise<CheckoutView | null> {
    const row = await this.load(checkoutId);
    return row ? toView(row) : null;
  }

  /** Every checkout of an order (a failed / cancelled one may have been retried), oldest first. */
  async findByOrder(orderId: string): Promise<CheckoutView[]> {
    const res = await this.db.query<CheckoutRow>(`SELECT * FROM checkouts WHERE order_id = $1 ORDER BY created_at, checkout_id`, [orderId]);
    return res.rows.map(toView);
  }

  async listSteps(checkoutId: string): Promise<Array<{ step: string; outcome: string; detail: Record<string, unknown>; at: string }>> {
    const res = await this.db.query<{ step: string; outcome: string; detail: Record<string, unknown>; at: Date }>(
      `SELECT step, outcome, detail, at FROM checkout_steps WHERE checkout_id = $1 ORDER BY seq`,
      [checkoutId]
    );
    return res.rows.map((r) => ({ ...r, at: new Date(r.at).toISOString() }));
  }

  /** WALLET remainder: hand the reserved checkout to the order pipeline (idempotent). */
  async confirmCheckout(checkoutId: string): Promise<CheckoutView> {
    return this.serialize(checkoutId, async () => {
      const row = await this.mustLoad(checkoutId);
      if (row.status === 'CONFIRMED' || row.status === 'COMPLETED') return toView(row);
      if (row.status !== 'RESERVED') {
        throw new CheckoutError(409, 'CHECKOUT_NOT_RESERVED', `Checkout ${checkoutId} is ${row.status}`, toView(row));
      }
      if (row.tender !== 'WALLET') {
        throw new CheckoutError(
          409,
          'PAY_REMAINDER_AT_GATEWAY',
          `Checkout ${checkoutId} is paid by card: pay ${halalasToSar(Number(row.total_halalas))} SAR through HyperPay with customParameters.checkoutId`,
          toView(row)
        );
      }
      if (new Date(row.expires_at).getTime() <= Date.now()) {
        const expired = await this.beginCompensation(row, 'EXPIRED', 'CHECKOUT_EXPIRED', 'The checkout expired before it was confirmed');
        const final = await this.advanceLocked(expired.checkout_id);
        throw new CheckoutError(409, 'CHECKOUT_EXPIRED', `Checkout ${checkoutId} expired`, toView(final));
      }

      const confirmed = await this.db.transaction(async (tx) => {
        const res = await tx.query<CheckoutRow>(
          `UPDATE checkouts SET status = 'CONFIRMED', payment_id = $2, updated_at = NOW()
           WHERE checkout_id = $1 AND status = 'RESERVED' RETURNING *`,
          [checkoutId, walletPaymentId(checkoutId)]
        );
        if (res.rows.length === 0) return null;
        const updated = res.rows[0];
        const payload: CheckoutPaymentCompletedPayload = {
          checkoutId,
          paymentId: walletPaymentId(checkoutId),
          tender: 'WALLET',
          status: 'SUCCESS',
          amountHalalas: Number(updated.total_halalas),
          currency: 'SAR',
          order: orderContextOf(updated),
          confirmedAt: new Date().toISOString(),
        };
        await enqueueIntegrationEvent(tx, {
          exchange: EXCHANGES.checkout.name,
          routingKey: ROUTING_KEYS.checkoutPaymentCompleted,
          event: buildIntegrationEvent({
            eventId: deterministicEventId(EVENT_TYPES.checkoutPaymentCompleted, checkoutId),
            eventType: EVENT_TYPES.checkoutPaymentCompleted,
            producer: PRODUCERS.checkout,
            correlationId: updated.order_id,
            causationId: null,
            aggregateId: checkoutId,
            payload,
          }) as IntegrationEvent,
        });
        await this.step(tx, checkoutId, 'CONFIRMED', 'CONFIRMED', { amountHalalas: payload.amountHalalas });
        return updated;
      });
      if (confirmed) this.options.onOutboxEnqueued?.();
      return toView(confirmed ?? (await this.mustLoad(checkoutId)));
    });
  }

  /** Release both reservations (idempotent). Refused once the checkout was confirmed / paid. */
  async cancelCheckout(checkoutId: string, reason = 'CANCELLED_BY_CUSTOMER'): Promise<CheckoutView> {
    return this.serialize(checkoutId, async () => {
      const row = await this.mustLoad(checkoutId);
      if (['CANCELLED', 'EXPIRED', 'FAILED', 'PAYMENT_FAILED'].includes(row.status)) return toView(row);
      if (row.status === 'CONFIRMED' || row.status === 'COMPLETED') {
        throw new CheckoutError(409, 'CHECKOUT_ALREADY_CONFIRMED', `Checkout ${checkoutId} is ${row.status}`, toView(row));
      }
      if (row.status === 'RESERVED' || row.status === 'RESERVING') {
        if (row.status === 'RESERVING' && this.leasedByOther(row)) {
          throw new CheckoutError(409, 'CHECKOUT_BUSY', `Checkout ${checkoutId} is being reserved by another instance`, toView(row));
        }
        await this.beginCompensation(row, 'CANCELLED', reason, 'Cancelled before payment');
      }
      return toView(await this.advanceLocked(checkoutId));
    });
  }

  /** Release the reservations of checkouts that were not paid in time. */
  async expireDue(): Promise<string[]> {
    const res = await this.db.query<{ checkout_id: string }>(
      `SELECT checkout_id FROM checkouts WHERE status = 'RESERVED' AND expires_at <= NOW() ORDER BY expires_at LIMIT 100`
    );
    const expired: string[] = [];
    for (const { checkout_id: id } of res.rows) {
      await this.serialize(id, async () => {
        const row = await this.mustLoad(id);
        if (row.status !== 'RESERVED' || new Date(row.expires_at).getTime() > Date.now()) return;
        await this.beginCompensation(row, 'EXPIRED', 'CHECKOUT_EXPIRED', 'The checkout was not paid before its reservations expired');
        await this.advanceLocked(id);
        expired.push(id);
      });
    }
    return expired;
  }

  /** Resume sagas left half-way by a crash (their lease expired): finish or compensate them. */
  async recoverStuck(graceMs = 0): Promise<string[]> {
    const res = await this.db.query<{ checkout_id: string }>(
      `SELECT checkout_id FROM checkouts
       WHERE status IN ('RESERVING', 'COMPENSATING')
         AND (lease_until IS NULL OR lease_until < NOW())
         AND updated_at <= NOW() - ($1::int * INTERVAL '1 millisecond')
       ORDER BY updated_at LIMIT 100`,
      [graceMs]
    );
    const resumed: string[] = [];
    for (const { checkout_id: id } of res.rows) {
      await this.advance(id);
      resumed.push(id);
    }
    return resumed;
  }

  /** The ledger settled (captured) or released the checkout's payment: close the checkout. */
  async handleLedgerOutcome(
    event: IntegrationEvent<string, LedgerPaymentSettledPayload | LedgerPaymentReleasedPayload>
  ): Promise<string> {
    const settled = event.eventType === EVENT_TYPES.ledgerPaymentSettled;
    const checkoutId = settled
      ? (event.payload as LedgerPaymentSettledPayload).order.checkoutId
      : (event.payload as LedgerPaymentReleasedPayload).checkoutId;
    if (!checkoutId) return 'IGNORED_NO_CHECKOUT';

    return this.serialize(checkoutId, () =>
      this.db.transaction(async (tx) => {
        const claimed = await claimCheckpoint(tx, {
          consumer: CHECKOUT_OUTCOME_CONSUMER,
          eventId: event.eventId,
          step: 'done',
          correlationId: event.correlationId,
        });
        if (!claimed) return 'DUPLICATE_IGNORED';
        const res = await tx.query<CheckoutRow>(`SELECT * FROM checkouts WHERE checkout_id = $1 FOR UPDATE`, [checkoutId]);
        const row = res.rows[0];
        let outcome = 'IGNORED_UNKNOWN_CHECKOUT';
        if (row) {
          if (settled) {
            await tx.query(
              `UPDATE checkouts SET status = 'COMPLETED', compensation_target = NULL, payment_id = $2,
                 remainder_hold_status = 'CAPTURED',
                 credit_hold_status = CASE WHEN credit_hold_status = 'NOT_REQUIRED' THEN 'NOT_REQUIRED' ELSE 'CAPTURED' END,
                 updated_at = NOW()
               WHERE checkout_id = $1`,
              [checkoutId, event.payload.paymentId]
            );
            outcome = 'COMPLETED';
          } else {
            await tx.query(
              `UPDATE checkouts SET status = CASE WHEN status IN ('RESERVED', 'CONFIRMED') THEN 'PAYMENT_FAILED' ELSE status END,
                 payment_id = COALESCE(payment_id, $2), remainder_hold_status = 'RELEASED',
                 credit_hold_status = CASE WHEN credit_hold_status = 'NOT_REQUIRED' THEN 'NOT_REQUIRED' ELSE 'RELEASED' END,
                 failure_code = COALESCE(failure_code, 'PAYMENT_FAILED'), failure_message = COALESCE(failure_message, $3),
                 updated_at = NOW()
               WHERE checkout_id = $1`,
              [checkoutId, event.payload.paymentId, (event.payload as LedgerPaymentReleasedPayload).reason]
            );
            outcome = 'PAYMENT_FAILED';
          }
          await this.step(tx, checkoutId, settled ? 'PAYMENT_SETTLED' : 'PAYMENT_RELEASED', outcome, { eventId: event.eventId });
        }
        await completeCheckpoint(tx, { consumer: CHECKOUT_OUTCOME_CONSUMER, eventId: event.eventId, step: 'done', outcome });
        return outcome;
      })
    );
  }

  // ── saga ──────────────────────────────────────────────────────────────────────

  /** Drive a checkout as far as it can go now (reserve or compensate). */
  advance(checkoutId: string): Promise<CheckoutRow> {
    return this.serialize(checkoutId, () => this.advanceLocked(checkoutId));
  }

  private async advanceLocked(checkoutId: string): Promise<CheckoutRow> {
    const leased = await this.acquireLease(checkoutId);
    if (!leased) return this.mustLoad(checkoutId); // another instance is working on it
    let row = leased;
    try {
      for (let guard = 0; guard < 4; guard++) {
        if (row.status === 'RESERVING') {
          row = await this.reserve(row);
        } else if (row.status === 'COMPENSATING') {
          row = await this.compensate(row);
          // Still compensating: a release is deferred (service unavailable); the recovery sweep retries.
          if (row.status === 'COMPENSATING') break;
        } else {
          break;
        }
      }
      return row;
    } finally {
      await this.db.query(
        `UPDATE checkouts SET lease_owner = NULL, lease_until = NULL WHERE checkout_id = $1 AND lease_owner = $2`,
        [checkoutId, this.instanceId]
      );
    }
  }

  private async reserve(row: CheckoutRow): Promise<CheckoutRow> {
    if (row.credit_hold_status === 'PENDING') {
      let placed;
      try {
        placed = await withRetries(
          this.options.retry,
          () =>
            this.billing.placeCreditHold({
              orderId: row.order_id,
              orgId: row.promotion_org_id as string,
              credits: Number(row.promotion_credits),
              ttlSeconds: this.options.reservationTtlSeconds + this.options.creditHoldTtlMarginSeconds,
              holdKey: row.credit_hold_key,
            }),
          this.options.sleep
        );
      } catch (err) {
        return this.failDownstream(row, err, 'BILLING_UNAVAILABLE', 'RESERVE_CREDITS');
      }
      row = await this.update(row, { credit_hold_status: 'PLACED', credit_hold_id: placed.holdId }, 'RESERVE_CREDITS', 'PLACED', {
        holdId: placed.holdId,
        created: placed.created,
      });
      if (placed.allowanceHalalas !== Number(row.allowance_halalas)) {
        return this.beginCompensation(
          row,
          'FAILED',
          'ALLOWANCE_MISMATCH',
          `billing values the credits at ${placed.allowanceHalalas} halalas, the checkout at ${row.allowance_halalas}`
        );
      }
    }

    if (row.remainder_hold_status === 'PENDING') {
      let placed;
      try {
        placed = await withRetries(
          this.options.retry,
          () =>
            this.wallet.placeTenderHold({
              holdKey: row.remainder_hold_key,
              orderId: row.order_id,
              merchantId: row.merchant_id,
              tender: row.tender,
              ...(row.source_wallet_id ? { sourceWalletId: row.source_wallet_id } : {}),
              amountHalalas: Number(row.total_halalas),
              checkoutId: row.checkout_id,
            }),
          this.options.sleep
        );
      } catch (err) {
        // The remainder could not be reserved: the credit hold fails for the same reason.
        return this.failDownstream(row, err, 'WALLET_UNAVAILABLE', 'RESERVE_REMAINDER');
      }
      row = await this.update(row, { remainder_hold_status: 'PLACED', remainder_hold_id: placed.holdId }, 'RESERVE_REMAINDER', 'PLACED', {
        holdId: placed.holdId,
        created: placed.created,
        tender: row.tender,
        amountHalalas: Number(row.total_halalas),
      });
    }
    return this.update(row, { status: 'RESERVED' }, 'RESERVED', 'RESERVED', {});
  }

  private async failDownstream(row: CheckoutRow, err: unknown, unavailableCode: string, step: string): Promise<CheckoutRow> {
    if (!(err instanceof DownstreamError)) throw err;
    const code = err.kind === 'UNAVAILABLE' ? unavailableCode : err.code;
    await this.step(this.db, row.checkout_id, step, 'REJECTED', { service: err.service, kind: err.kind, status: err.status, code: err.code, message: err.message });
    return this.beginCompensation(row, 'FAILED', code, err.message);
  }

  private async beginCompensation(row: CheckoutRow, target: CompensationTarget, code: string, message: string): Promise<CheckoutRow> {
    return this.update(
      row,
      { status: 'COMPENSATING', compensation_target: target, failure_code: code, failure_message: message },
      'COMPENSATE',
      target,
      { code, message }
    );
  }

  /**
   * Release both reservations by key (tombstoning keys that never got a hold), remainder first.
   * An unavailable service leaves the checkout COMPENSATING: the recovery sweep retries it.
   */
  private async compensate(row: CheckoutRow): Promise<CheckoutRow> {
    const reason = `${row.compensation_target}: ${row.failure_code ?? ''}`.trim();
    try {
      if (row.remainder_hold_status === 'PENDING' || row.remainder_hold_status === 'PLACED') {
        const released = await withRetries(
          this.options.retry,
          () =>
            this.wallet.releaseTenderHold({
              holdKey: row.remainder_hold_key,
              orderId: row.order_id,
              merchantId: row.merchant_id,
              tender: row.tender,
              ...(row.source_wallet_id ? { sourceWalletId: row.source_wallet_id } : {}),
              amountHalalas: Number(row.total_halalas),
              checkoutId: row.checkout_id,
              reason,
            }),
          this.options.sleep
        );
        row = await this.update(row, { remainder_hold_status: 'RELEASED' }, 'RELEASE_REMAINDER', released.outcome, {});
      }
      if (row.credit_hold_status === 'PENDING' || row.credit_hold_status === 'PLACED') {
        const released = await withRetries(
          this.options.retry,
          () => this.billing.releaseCreditHold({ orderId: row.order_id, holdKey: row.credit_hold_key, reason }),
          this.options.sleep
        );
        row = await this.update(row, { credit_hold_status: 'RELEASED' }, 'RELEASE_CREDITS', released.outcome, {});
      }
    } catch (err) {
      if (err instanceof DownstreamError && err.kind === 'REJECTED' && isAlreadyCaptured(err)) {
        // The payment won the race (captured before the cancellation / expiry got there): the
        // order is being settled; the ledger's settled event completes the checkout.
        return this.update(
          row,
          { status: 'CONFIRMED', compensation_target: null, failure_code: 'CANCELLED_TOO_LATE', failure_message: err.message },
          'COMPENSATE',
          'TOO_LATE_CAPTURED',
          { service: err.service, code: err.code }
        );
      }
      if (err instanceof DownstreamError) {
        await this.step(this.db, row.checkout_id, 'COMPENSATE', 'RETRY_LATER', { service: err.service, kind: err.kind, code: err.code, message: err.message });
        return row; // stays COMPENSATING
      }
      throw err;
    }
    return this.update(row, { status: row.compensation_target as CheckoutStatus, compensation_target: null }, 'COMPENSATED', row.compensation_target as string, {});
  }

  // ── persistence ─────────────────────────────────────────────────────────────────

  private async load(checkoutId: string): Promise<CheckoutRow | null> {
    const res = await this.db.query<CheckoutRow>(`SELECT * FROM checkouts WHERE checkout_id = $1`, [checkoutId]);
    return res.rows[0] ?? null;
  }

  private async mustLoad(checkoutId: string): Promise<CheckoutRow> {
    const row = await this.load(checkoutId);
    if (!row) throw new CheckoutError(404, 'CHECKOUT_NOT_FOUND', `Checkout ${checkoutId} not found`);
    return row;
  }

  private leasedByOther(row: CheckoutRow): boolean {
    return !!row.lease_owner && row.lease_owner !== this.instanceId && !!row.lease_until && new Date(row.lease_until).getTime() > Date.now();
  }

  private async acquireLease(checkoutId: string): Promise<CheckoutRow | null> {
    const res = await this.db.query<CheckoutRow>(
      `UPDATE checkouts SET lease_owner = $2, lease_until = NOW() + ($3::int * INTERVAL '1 millisecond')
       WHERE checkout_id = $1 AND (lease_owner IS NULL OR lease_owner = $2 OR lease_until < NOW())
       RETURNING *`,
      [checkoutId, this.instanceId, this.options.leaseMs]
    );
    return res.rows[0] ?? null;
  }

  private async update(
    row: CheckoutRow,
    patch: Partial<Pick<CheckoutRow, 'status' | 'compensation_target' | 'failure_code' | 'failure_message' | 'credit_hold_status' | 'credit_hold_id' | 'remainder_hold_status' | 'remainder_hold_id'>>,
    step: string,
    outcome: string,
    detail: Record<string, unknown>
  ): Promise<CheckoutRow> {
    const columns = Object.keys(patch);
    const values = Object.values(patch);
    const set = columns.map((c, i) => `${c} = $${i + 2}`).join(', ');
    const updated = await this.db.transaction(async (tx) => {
      const res = await tx.query<CheckoutRow>(
        `UPDATE checkouts SET ${set}, updated_at = NOW() WHERE checkout_id = $1 RETURNING *`,
        [row.checkout_id, ...values]
      );
      await this.step(tx, row.checkout_id, step, outcome, detail);
      return res.rows[0];
    });
    return updated;
  }

  private async step(
    exec: Pick<PGlite, 'query'> | Pick<Transaction, 'query'>,
    checkoutId: string,
    step: string,
    outcome: string,
    detail: Record<string, unknown>
  ): Promise<void> {
    await exec.query(`INSERT INTO checkout_steps (checkout_id, step, outcome, detail) VALUES ($1, $2, $3, $4::jsonb)`, [
      checkoutId,
      step,
      outcome,
      JSON.stringify(detail),
    ]);
  }

  /** One saga transition at a time per checkout in this process (the lease covers other instances). */
  private serialize<T>(checkoutId: string, work: () => Promise<T>): Promise<T> {
    const previous = this.chains.get(checkoutId) ?? Promise.resolve();
    const run = previous.then(work, work);
    const settled = run.catch(() => undefined);
    this.chains.set(checkoutId, settled);
    void settled.then(() => {
      if (this.chains.get(checkoutId) === settled) this.chains.delete(checkoutId);
    });
    return run;
  }
}

function isAlreadyCaptured(err: DownstreamError): boolean {
  return err.code === 'HOLD_ALREADY_CAPTURED' || (err.service === 'billing-credits' && err.code === 'HOLD_STATE_CONFLICT' && /captured/i.test(err.message));
}

function orderContextOf(row: CheckoutRow): OrderContext {
  return {
    orderId: row.order_id,
    merchantId: row.merchant_id,
    creditOrgId: row.promotion_org_id,
    customer: row.request.customer,
    lines: row.request.lines,
    checkoutId: row.checkout_id,
  };
}

function canonical(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === 'object') {
    return Object.fromEntries(
      Object.keys(value as Record<string, unknown>)
        .sort()
        .filter((k) => (value as Record<string, unknown>)[k] !== undefined)
        .map((k) => [k, canonical((value as Record<string, unknown>)[k])])
    );
  }
  return value;
}

function fingerprintOf(request: CheckoutRequest): string {
  return crypto.createHash('sha256').update(JSON.stringify(canonical(request))).digest('hex');
}

function toView(row: CheckoutRow): CheckoutView {
  const total = Number(row.total_halalas);
  return {
    checkoutId: row.checkout_id,
    orderId: row.order_id,
    merchantId: row.merchant_id,
    status: row.status,
    amounts: row.totals,
    promotion: row.promotion_org_id
      ? { orgId: row.promotion_org_id, credits: Number(row.promotion_credits), allowanceHalalas: Number(row.allowance_halalas) }
      : null,
    payment: {
      tender: row.tender,
      walletId: row.source_wallet_id,
      amountHalalas: total,
      amountSar: halalasToSar(total),
      paymentId: row.payment_id,
    },
    holds: {
      credits: { key: row.credit_hold_key, holdId: row.credit_hold_id, status: row.credit_hold_status },
      remainder: { key: row.remainder_hold_key, holdId: row.remainder_hold_id, status: row.remainder_hold_status },
    },
    failure: row.failure_code ? { code: row.failure_code, message: row.failure_message } : null,
    expiresAt: new Date(row.expires_at).toISOString(),
    createdAt: new Date(row.created_at).toISOString(),
    updatedAt: new Date(row.updated_at).toISOString(),
  };
}
