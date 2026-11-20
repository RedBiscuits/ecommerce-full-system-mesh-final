import type { PoolClient } from 'pg';
import { withTransaction, query } from '../db';
import { config } from '../config/env';
import { balanceCacheKey, redis } from '../redis';
import { CreditLedgerService, creditLedgerService } from '../services/CreditLedgerService';
import { ensureOrgWallet, lockFullWallet } from '../services/OrgWallet';
import {
  CONSUMERS,
  EVENT_TYPES,
  EXCHANGES,
  PRODUCERS,
  ROUTING_KEYS,
  buildIntegrationEvent,
  deterministicEventId,
  sarToHalalas,
  type HyperPayRefundProcessed,
  type OrderCreditsRestoredPayload,
  type RefundFacts,
} from '../../../../shared/contracts';
import {
  NonRetryableError,
  claimCheckpoint,
  completeCheckpoint,
  enqueueIntegrationEvent,
  listCheckpoints,
  type Checkpoint,
} from '../../../../shared/messaging';

export const BILLING_REFUND_CONSUMER = CONSUMERS.billingRefunds;

export type OrderCreditRefundOutcome = 'RESTORED' | 'NONE' | 'DUPLICATE_IGNORED' | 'IGNORED_NOT_SUCCESSFUL';

export interface OrderCreditRefundResult {
  outcome: OrderCreditRefundOutcome;
  refundId: string;
  orderId: string;
  restoredCredits?: number;
  cumulativeRestoredCredits?: number;
  grantId?: string;
  /** eventId of the billing.order-credits.restored event (deterministic per refund). */
  restoredEventId?: string;
}

export interface OrderCreditRefundRecord {
  refundId: string;
  orderId: string;
  paymentId: string;
  orgId: string | null;
  amountHalalas: number;
  capturedHalalas: number;
  refundedTotalHalalas: number;
  capturedCredits: number;
  restoredCredits: number;
  cumulativeRestoredCredits: number;
  grantId: string | null;
  sourceEventId: string;
  restoredEventId: string;
  createdAt: string;
}

interface RefundRow {
  refund_id: string;
  order_id: string;
  payment_id: string;
  org_id: string | null;
  amount_halalas: string | number;
  captured_halalas: string | number;
  refunded_total_halalas: string | number;
  captured_credits: string | number;
  restored_credits: string | number;
  cumulative_restored_credits: string | number;
  restore_grant_id: string | null;
  source_event_id: string;
  restored_event_id: string;
  created_at: Date | string;
}

const toRecord = (r: RefundRow): OrderCreditRefundRecord => ({
  refundId: r.refund_id,
  orderId: r.order_id,
  paymentId: r.payment_id,
  orgId: r.org_id,
  amountHalalas: Number(r.amount_halalas),
  capturedHalalas: Number(r.captured_halalas),
  refundedTotalHalalas: Number(r.refunded_total_halalas),
  capturedCredits: Number(r.captured_credits),
  restoredCredits: Number(r.restored_credits),
  cumulativeRestoredCredits: Number(r.cumulative_restored_credits),
  grantId: r.restore_grant_id,
  sourceEventId: r.source_event_id,
  restoredEventId: r.restored_event_id,
  createdAt: new Date(r.created_at).toISOString(),
});

/**
 * Billing's part of a customer refund: give the promotional credits back, pro rata.
 *
 * For an order that captured C credits on a payment of P halalas, after a refund that brings the
 * refunded total to R the org must hold back ⌊C × R / P⌋ of those credits (so a full refund
 * restores all of them, partial refunds restore a monotone share, and Σ restored ≤ C whatever
 * the order of deliveries). The delta is issued as a PROMOTIONAL grant (priority 1, expiring
 * when the consumed grants would have, or after ORDER_CREDIT_RESTORE_TTL_DAYS) — the original
 * lots are not rewritten, so the FIFO history of the capture stays intact.
 *
 * Exactly once, in ONE transaction: checkpoint + order_credit_refunds row (primary key = refund
 * id) + grant + outbox event (deterministic id per refund). Unsuccessful refunds (PENDING /
 * FAILED) are acknowledged and change nothing.
 */
export class OrderCreditRefundService {
  constructor(private readonly ledger: CreditLedgerService = creditLedgerService) {}

  async handleRefundProcessed(event: HyperPayRefundProcessed): Promise<OrderCreditRefundResult> {
    const refundId = event.transactionId;
    const orderId = event.orderId;
    if (event.currency !== 'SAR') throw new NonRetryableError('UNSUPPORTED_CURRENCY', `Order refunds must be in SAR, got ${event.currency}`);

    const touchedOrgs: string[] = [];
    const result = await withTransaction(async (client): Promise<OrderCreditRefundResult> => {
      const key = { consumer: BILLING_REFUND_CONSUMER, eventId: event.eventId, step: 'done' };
      if (!(await claimCheckpoint(client, { ...key, correlationId: orderId }))) {
        return { outcome: 'DUPLICATE_IGNORED', refundId, orderId };
      }
      if (event.status !== 'SUCCESS') {
        await completeCheckpoint(client, { ...key, outcome: 'IGNORED_NOT_SUCCESSFUL', detail: { status: event.status } });
        return { outcome: 'IGNORED_NOT_SUCCESSFUL', refundId, orderId };
      }

      const existing = await client.query<RefundRow>(`SELECT * FROM order_credit_refunds WHERE refund_id = $1`, [refundId]);
      if (existing.rows.length > 0) {
        // Re-announced under another event id: the same restoration, announced with the same id.
        const prior = toRecord(existing.rows[0]);
        await this.announce(client, event, prior);
        await completeCheckpoint(client, { ...key, outcome: 'DUPLICATE_IGNORED', detail: { restoredByEventId: prior.sourceEventId } });
        return { outcome: 'DUPLICATE_IGNORED', refundId, orderId, restoredEventId: prior.restoredEventId };
      }

      const amountHalalas = sarToHalalas(event.amount);
      const capturedHalalas = sarToHalalas(event.referencedAmount);
      const refundedTotalHalalas = sarToHalalas(event.refundedTotal);
      if (amountHalalas <= 0 || refundedTotalHalalas > capturedHalalas || amountHalalas > refundedTotalHalalas) {
        throw new NonRetryableError(
          'REFUND_AMOUNT_INVALID',
          `Refund ${refundId}: amount ${amountHalalas}, refunded total ${refundedTotalHalalas}, captured ${capturedHalalas}`
        );
      }

      // What the order captured (the successful payment's settlement) and what was restored so far.
      const capture = await client.query<{ payment_id: string; org_id: string | null; captured_credits: string | number; hold_id: string | null }>(
        `SELECT payment_id, org_id, captured_credits, hold_id FROM order_credit_settlements
         WHERE order_id = $1 AND payment_status = 'SUCCESS'`,
        [orderId]
      );
      if (capture.rows.length === 0) {
        throw new NonRetryableError('ORDER_NOT_CAPTURED', `Order ${orderId} has no captured payment in billing; refund ${refundId} cannot be applied`);
      }
      const captured = capture.rows[0];
      if (captured.payment_id !== event.referencedPaymentId) {
        throw new NonRetryableError(
          'REFUND_PAYMENT_MISMATCH',
          `Order ${orderId} was captured by payment ${captured.payment_id}, refund ${refundId} references ${event.referencedPaymentId}`
        );
      }
      const capturedCredits = Number(captured.captured_credits);
      const restoredSoFar = Number(
        (await client.query<{ s: string }>(`SELECT COALESCE(SUM(restored_credits), 0)::text AS s FROM order_credit_refunds WHERE order_id = $1`, [orderId]))
          .rows[0].s
      );
      // Keep back ⌊captured × refundedTotal / captured amount⌋ in total; restore the difference now.
      const target = Number((BigInt(capturedCredits) * BigInt(refundedTotalHalalas)) / BigInt(capturedHalalas));
      const restore = Math.max(0, Math.min(capturedCredits - restoredSoFar, target - restoredSoFar));

      let grantId: string | null = null;
      if (restore > 0 && captured.org_id) {
        await ensureOrgWallet(client, captured.org_id);
        const lock = await lockFullWallet(client, captured.org_id);
        const grant = await this.ledger.createGrant(
          {
            orgId: captured.org_id,
            source: 'PROMOTIONAL',
            credits: restore,
            expiresAt: await this.restoredExpiry(client, captured.hold_id),
            invoiceId: `refund:${refundId}`,
          },
          { wallet: lock }
        );
        grantId = grant.grantId;
        touchedOrgs.push(captured.org_id);
      }

      const restoredEventId = deterministicEventId(EVENT_TYPES.orderCreditsRestored, refundId);
      const inserted = await client.query<RefundRow>(
        `INSERT INTO order_credit_refunds
           (refund_id, order_id, payment_id, org_id, amount_halalas, captured_halalas, refunded_total_halalas,
            captured_credits, restored_credits, cumulative_restored_credits, restore_grant_id, source_event_id, restored_event_id)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
         RETURNING *`,
        [
          refundId,
          orderId,
          event.referencedPaymentId,
          captured.org_id,
          amountHalalas,
          capturedHalalas,
          refundedTotalHalalas,
          capturedCredits,
          restore,
          restoredSoFar + restore,
          grantId,
          event.eventId,
          restoredEventId,
        ]
      );
      const record = toRecord(inserted.rows[0]);
      await this.announce(client, event, record);
      const outcome = restore > 0 ? 'RESTORED' : 'NONE';
      await completeCheckpoint(client, {
        ...key,
        outcome,
        detail: { restoredCredits: restore, cumulativeRestoredCredits: restoredSoFar + restore, grantId, restoredEventId },
      });
      return { outcome, refundId, orderId, restoredCredits: restore, cumulativeRestoredCredits: restoredSoFar + restore, grantId: grantId ?? undefined, restoredEventId };
    });
    for (const orgId of touchedOrgs) await redis.del(balanceCacheKey(orgId));
    return result;
  }

  /** Refunds billing applied for an order, with its checkpoints. */
  async getOrderRefunds(orderId: string): Promise<{ refunds: OrderCreditRefundRecord[]; checkpoints: Checkpoint[] }> {
    const res = await query<RefundRow>(`SELECT * FROM order_credit_refunds WHERE order_id = $1 ORDER BY created_at, refund_id`, [orderId]);
    const checkpoints = await listCheckpoints({ query: (t, p) => query(t, p) }, { correlationId: orderId, consumer: BILLING_REFUND_CONSUMER });
    return { refunds: res.rows.map(toRecord), checkpoints };
  }

  private async announce(client: PoolClient, event: HyperPayRefundProcessed, record: OrderCreditRefundRecord): Promise<void> {
    const refund: RefundFacts = {
      refundId: record.refundId,
      refundEventId: event.eventId,
      paymentId: record.paymentId,
      orderId: record.orderId,
      merchantId: event.merchantId,
      currency: 'SAR',
      amountHalalas: record.amountHalalas,
      capturedHalalas: record.capturedHalalas,
      refundedTotalHalalas: record.refundedTotalHalalas,
      reason: event.reason ?? null,
      refundedAt: event.occurredAt,
    };
    const payload: OrderCreditsRestoredPayload = {
      refund,
      credits: {
        outcome: record.restoredCredits > 0 ? 'RESTORED' : 'NONE',
        orgId: record.orgId,
        capturedCredits: record.capturedCredits,
        restoredCredits: record.restoredCredits,
        cumulativeRestoredCredits: record.cumulativeRestoredCredits,
        grantId: record.grantId,
      },
    };
    await enqueueIntegrationEvent(client, {
      exchange: EXCHANGES.billing.name,
      routingKey: ROUTING_KEYS.orderCreditsRestored,
      event: buildIntegrationEvent({
        eventId: record.restoredEventId,
        eventType: EVENT_TYPES.orderCreditsRestored,
        producer: PRODUCERS.billing,
        correlationId: record.orderId,
        causationId: event.eventId,
        aggregateId: record.orderId,
        payload,
      }),
    });
  }

  /** Restored credits live as long as the grants the order consumed would have (else the default TTL). */
  private async restoredExpiry(client: PoolClient, holdId: string | null): Promise<string> {
    const fallback = new Date(Date.now() + config.orderCredits.restoreTtlDays * 24 * 3600_000).toISOString();
    if (!holdId) return fallback;
    const res = await client.query<{ expires_at: Date | string | null }>(
      `SELECT MAX(g.expires_at) AS expires_at
       FROM usage_events u
       JOIN credit_lot_allocations a ON a.usage_id = u.usage_id
       JOIN credit_grants g ON g.grant_id = a.grant_id
       WHERE u.hold_id = $1 AND g.expires_at > NOW()`,
      [holdId]
    );
    const latest = res.rows[0]?.expires_at;
    return latest ? new Date(latest).toISOString() : fallback;
  }
}

export const orderCreditRefundService = new OrderCreditRefundService();
