import type { PoolClient } from 'pg';
import { withTransaction } from '../db';
import { config } from '../config/env';
import { HoldStateConflictError } from '../types/credits';
import {
  JoinedTransaction,
  ORDER_CREDIT_MODEL,
  StreamHoldService,
  orderCreditRequestId,
  streamHoldService,
} from '../services/StreamHoldService';
import {
  CONSUMERS,
  CheckoutPaymentCompletedEvent,
  EVENT_TYPES,
  EXCHANGES,
  HyperPayPaymentCompleted,
  OrderContext,
  Tender,
  OrderCreditsSettledPayload,
  PRODUCERS,
  ROUTING_KEYS,
  buildIntegrationEvent,
  deterministicEventId,
  sarToHalalas,
} from '../../../../shared/contracts';
import {
  NonRetryableError,
  claimCheckpoint,
  completeCheckpoint,
  enqueueIntegrationEvent,
  listCheckpoints,
  type Checkpoint,
} from '../../../../shared/messaging';

export const BILLING_PAYMENT_CONSUMER = CONSUMERS.billingPaymentResults;

export type OrderCreditOutcome =
  | 'CAPTURED'
  | 'RELEASED'
  | 'NONE'
  | 'DUPLICATE_IGNORED'
  | 'IGNORED_NO_ORDER'
  | 'IGNORED_PENDING';

export interface OrderCreditSettlementResult {
  outcome: OrderCreditOutcome;
  orderId?: string;
  paymentId: string;
  capturedCredits?: number;
  releasedCredits?: number;
  allowanceHalalas?: number;
  /** eventId of the billing.order-credits.settled event (deterministic per payment outcome). */
  settlementEventId?: string;
}

/**
 * A payment outcome, whichever way the remainder of the order was paid: a card payment announced
 * by HyperPay, or a split-tender checkout whose remainder was paid from the customer's wallet.
 */
export interface PaymentOutcome {
  eventId: string;
  paymentId: string;
  status: 'SUCCESS' | 'FAILED' | 'PENDING';
  amountHalalas: number;
  currency: string;
  resultCode: string;
  paymentBrand: string | null;
  tender: Tender;
  order?: OrderContext;
}

export function paymentOutcomeFromHyperPay(event: HyperPayPaymentCompleted): PaymentOutcome {
  return {
    eventId: event.eventId,
    paymentId: event.transactionId,
    status: event.status,
    amountHalalas: sarToHalalas(event.amount),
    currency: event.currency,
    resultCode: event.resultCode,
    paymentBrand: event.paymentBrand ?? null,
    tender: 'CARD',
    order: event.order,
  };
}

export function paymentOutcomeFromCheckout(event: CheckoutPaymentCompletedEvent): PaymentOutcome {
  return {
    eventId: event.eventId,
    paymentId: event.payload.paymentId,
    status: event.payload.status,
    amountHalalas: event.payload.amountHalalas,
    currency: event.payload.currency,
    resultCode: 'WALLET_FUNDS_RESERVED',
    paymentBrand: 'WALLET',
    tender: 'WALLET',
    order: event.payload.order,
  };
}

interface ActiveOrderHold {
  hold_id: string;
  org_id: string;
  estimated_credits: string | number;
  status: string;
}

export interface OrderCreditSettlementRecord {
  paymentId: string;
  orderId: string;
  orgId: string | null;
  paymentStatus: 'SUCCESS' | 'FAILED';
  outcome: 'CAPTURED' | 'RELEASED' | 'NONE';
  holdId: string | null;
  capturedCredits: number;
  releasedCredits: number;
  allowanceHalalas: number;
  sourceEventId: string;
  settlementEventId: string;
  createdAt: string;
}

/**
 * Billing's part of the order flow: when HyperPay announces a checkout payment outcome, the
 * order's credit hold is CAPTURED (payment succeeded: the credits are charged and become the
 * order's allowance in SAR) or RELEASED (payment failed: the reservation returns to the pool).
 *
 * Exactly-once effects, in ONE transaction:
 *   consumer checkpoint (billing.payment-results, eventId, 'done')
 *   + hold settle / release (joined into the same transaction)
 *   + order_credit_settlements row (one per payment; one successful capture per order)
 *   + billing.order-credits.settled outbox row (deterministic event id)
 * A redelivered or replayed event finds the checkpoint and changes nothing.
 */
export class OrderCreditSettlementService {
  constructor(private readonly holds: StreamHoldService = streamHoldService) {}

  async handlePaymentCompleted(event: HyperPayPaymentCompleted): Promise<OrderCreditSettlementResult> {
    return this.handlePaymentOutcome(paymentOutcomeFromHyperPay(event));
  }

  async handlePaymentOutcome(event: PaymentOutcome): Promise<OrderCreditSettlementResult> {
    const paymentId = event.paymentId;
    if (!event.order) return { outcome: 'IGNORED_NO_ORDER', paymentId };
    if (event.status === 'PENDING') return { outcome: 'IGNORED_PENDING', paymentId, orderId: event.order.orderId };
    if (event.currency !== 'SAR') {
      throw new NonRetryableError('UNSUPPORTED_CURRENCY', `Order payments must be in SAR, got ${event.currency}`);
    }

    const order = event.order;
    const status = event.status;
    const afterCommit: Array<() => Promise<void> | void> = [];

    const result = await withTransaction(async (client): Promise<OrderCreditSettlementResult> => {
      const claimed = await claimCheckpoint(client, {
        consumer: BILLING_PAYMENT_CONSUMER,
        eventId: event.eventId,
        step: 'done',
        correlationId: order.orderId,
      });
      if (!claimed) return { outcome: 'DUPLICATE_IGNORED', paymentId, orderId: order.orderId };

      // Second line of defence: the same payment outcome announced under another event id.
      const prior = await this.settlementFor(client, paymentId);
      if (prior) {
        await completeCheckpoint(client, {
          consumer: BILLING_PAYMENT_CONSUMER,
          eventId: event.eventId,
          step: 'done',
          outcome: 'DUPLICATE_IGNORED',
          detail: { settledByEventId: prior.sourceEventId },
        });
        return {
          outcome: 'DUPLICATE_IGNORED',
          paymentId,
          orderId: order.orderId,
          settlementEventId: prior.settlementEventId,
        };
      }

      if (status === 'SUCCESS') {
        const captured = await client.query<{ payment_id: string }>(
          `SELECT payment_id FROM order_credit_settlements WHERE order_id = $1 AND payment_status = 'SUCCESS'`,
          [order.orderId]
        );
        if (captured.rows.length > 0) {
          throw new NonRetryableError(
            'ORDER_ALREADY_CAPTURED',
            `Order ${order.orderId} was already paid by payment ${captured.rows[0].payment_id}; payment ${paymentId} needs a refund`
          );
        }
      }

      const hold = await this.activeHold(client, order.orderId);
      if (hold && order.creditOrgId && hold.org_id !== order.creditOrgId) {
        throw new NonRetryableError(
          'ORDER_CREDIT_ORG_MISMATCH',
          `Order ${order.orderId} names credit org ${order.creditOrgId} but its hold ${hold.hold_id} belongs to ${hold.org_id}`
        );
      }
      const joined: JoinedTransaction = { client, afterCommit: (fn) => afterCommit.push(fn) };

      let outcome: 'CAPTURED' | 'RELEASED' | 'NONE' = 'NONE';
      let capturedCredits = 0;
      let releasedCredits = 0;

      if (status === 'SUCCESS') {
        if (hold) {
          const estimated = Number(hold.estimated_credits);
          let settled;
          try {
            settled = await this.holds.settleStreamHold({ holdId: hold.hold_id, actualCredits: estimated }, joined);
          } catch (err) {
            if (!(err instanceof HoldStateConflictError)) throw err;
            // On a real multi-connection PostgreSQL a concurrent delivery of the same payment (under
            // another event id) may have captured the hold while we waited for its lock: READ
            // COMMITTED lets this statement see that committed settlement. Then it is a duplicate.
            const winner = await this.settlementFor(client, paymentId);
            if (winner) {
              await completeCheckpoint(client, {
                consumer: BILLING_PAYMENT_CONSUMER,
                eventId: event.eventId,
                step: 'done',
                outcome: 'DUPLICATE_IGNORED',
                detail: { settledByEventId: winner.sourceEventId },
              });
              return {
                outcome: 'DUPLICATE_IGNORED',
                paymentId,
                orderId: order.orderId,
                settlementEventId: winner.settlementEventId,
              };
            }
            throw new NonRetryableError('ORDER_CREDIT_HOLD_NOT_CAPTURABLE', err.message);
          }
          if (settled.chargedCredits !== estimated) {
            // The order was priced with this discount; an unbacked discount needs a human.
            throw new NonRetryableError(
              'ORDER_CREDIT_CAPTURE_SHORTFALL',
              `Hold ${hold.hold_id} of order ${order.orderId} reserved ${estimated} credits but only ` +
                `${settled.chargedCredits} could be captured`
            );
          }
          outcome = 'CAPTURED';
          capturedCredits = settled.chargedCredits;
        }
        // No active hold: nothing to capture (allowance 0). If the checkout had priced the order
        // with a discount that lapsed, the ledger's amount check rejects the settlement.
      } else if (hold) {
        outcome = 'RELEASED';
        try {
          const released = await this.holds.releaseStreamHold(
            { holdId: hold.hold_id, reason: `payment ${paymentId} failed (${event.resultCode})` },
            joined
          );
          releasedCredits = released.releasedCredits;
        } catch (err) {
          // Already past its TTL: the expiry just returned the credits to the pool.
          if (!(err instanceof HoldStateConflictError)) throw err;
        }
      }

      const halalasPerCredit = config.orderCredits.halalasPerCredit;
      const allowanceHalalas = capturedCredits * halalasPerCredit;
      const settlementEventId = deterministicEventId(EVENT_TYPES.orderCreditsSettled, paymentId, status);
      const payload: OrderCreditsSettledPayload = {
        payment: {
          paymentId,
          paymentEventId: event.eventId,
          status,
          amountHalalas: event.amountHalalas,
          currency: 'SAR',
          resultCode: event.resultCode,
          paymentBrand: event.paymentBrand,
          tender: event.tender,
        },
        order,
        credits: {
          outcome,
          orgId: hold?.org_id ?? order.creditOrgId ?? null,
          holdIds: hold ? [hold.hold_id] : [],
          capturedCredits,
          releasedCredits,
          halalasPerCredit,
          allowanceHalalas,
        },
      };

      await client.query(
        `INSERT INTO order_credit_settlements
           (payment_id, order_id, org_id, payment_status, outcome, hold_id, captured_credits, released_credits,
            allowance_halalas, source_event_id, settlement_event_id)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
        [
          paymentId,
          order.orderId,
          payload.credits.orgId,
          status,
          outcome,
          hold?.hold_id ?? null,
          capturedCredits,
          releasedCredits,
          allowanceHalalas,
          event.eventId,
          settlementEventId,
        ]
      );

      await enqueueIntegrationEvent(client, {
        exchange: EXCHANGES.billing.name,
        routingKey: ROUTING_KEYS.orderCreditsSettled,
        event: buildIntegrationEvent({
          eventId: settlementEventId,
          eventType: EVENT_TYPES.orderCreditsSettled,
          producer: PRODUCERS.billing,
          correlationId: order.orderId,
          causationId: event.eventId,
          aggregateId: order.orderId,
          payload,
        }),
      });

      await completeCheckpoint(client, {
        consumer: BILLING_PAYMENT_CONSUMER,
        eventId: event.eventId,
        step: 'done',
        outcome,
        detail: { paymentId, capturedCredits, releasedCredits, allowanceHalalas, settlementEventId },
      });

      return {
        outcome,
        paymentId,
        orderId: order.orderId,
        capturedCredits,
        releasedCredits,
        allowanceHalalas,
        settlementEventId,
      };
    });

    for (const effect of afterCommit) {
      try {
        await effect();
      } catch (err) {
        console.error('[OrderCreditSettlement] post-commit effect failed:', err);
      }
    }
    return result;
  }

  async getOrderSettlements(orderId: string): Promise<{ settlements: OrderCreditSettlementRecord[]; checkpoints: Checkpoint[] }> {
    return withTransaction(async (client) => {
      const res = await client.query(
        `SELECT * FROM order_credit_settlements WHERE order_id = $1 ORDER BY created_at`,
        [orderId]
      );
      return {
        settlements: res.rows.map(toRecord),
        checkpoints: await listCheckpoints(client, { correlationId: orderId }),
      };
    });
  }

  private async settlementFor(client: PoolClient, paymentId: string): Promise<OrderCreditSettlementRecord | null> {
    const res = await client.query(`SELECT * FROM order_credit_settlements WHERE payment_id = $1`, [paymentId]);
    return res.rows.length > 0 ? toRecord(res.rows[0]) : null;
  }

  private async activeHold(client: PoolClient, orderId: string): Promise<ActiveOrderHold | null> {
    const res = await client.query<ActiveOrderHold>(
      `SELECT hold_id, org_id, estimated_credits, status FROM credit_holds
       WHERE request_id = $1 AND model_name = $2 AND status = 'ACTIVE'`,
      [orderCreditRequestId(orderId), ORDER_CREDIT_MODEL]
    );
    return res.rows[0] ?? null;
  }
}

function toRecord(row: any): OrderCreditSettlementRecord {
  return {
    paymentId: row.payment_id,
    orderId: row.order_id,
    orgId: row.org_id ?? null,
    paymentStatus: row.payment_status,
    outcome: row.outcome,
    holdId: row.hold_id ?? null,
    capturedCredits: Number(row.captured_credits),
    releasedCredits: Number(row.released_credits),
    allowanceHalalas: Number(row.allowance_halalas),
    sourceEventId: row.source_event_id,
    settlementEventId: row.settlement_event_id,
    createdAt: new Date(row.created_at).toISOString(),
  };
}

export const orderCreditSettlementService = new OrderCreditSettlementService();
