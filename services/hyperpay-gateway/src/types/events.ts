/**
 * src/types/events.ts
 *
 * Domain events this service announces, and their wire bodies.
 *
 * Every event is first written to the `outbox_events` collection in the same
 * DB transaction as the state change it describes, then published to
 * RabbitMQ by the outbox publisher (messaging/outboxPublisher.ts). Consumers
 * therefore get every event at least once; `eventId` (also the AMQP
 * `messageId`) is the de-duplication key.
 *
 *   PAYMENT_COMPLETED   a payment verification / charge / capture reached an
 *                       outcome (SUCCESS, FAILED or still PENDING)
 *   TOKEN_REGISTERED    a new registration token was sealed in the vault
 *   REFUND_PROCESSED    the gateway processed a refund request
 */

import type { OrderContext } from '../../../../shared/contracts/order';
import type { PaymentResultEvent, PaymentResultSource } from './rabbitmq';
import type { TransactionStatus } from './hyperpay';

export type DomainEventType = 'PAYMENT_COMPLETED' | 'TOKEN_REGISTERED' | 'REFUND_PROCESSED';

export const DOMAIN_EVENT_TYPES: readonly DomainEventType[] = ['PAYMENT_COMPLETED', 'TOKEN_REGISTERED', 'REFUND_PROCESSED'];

/** Fields the outbox publisher stamps on every event body at publish time. */
export interface DomainEventEnvelope {
  /** Unique per event (the outbox row id). Use it to de-duplicate redeliveries. */
  eventId: string;
  eventType: DomainEventType;
  /** When the state change was committed (ISO-8601). */
  occurredAt: string;
}

// ─── PAYMENT_COMPLETED ────────────────────────────────────────────────────────

/**
 * Superset of the legacy `PaymentResultEvent`: existing `hyperpay.results`
 * consumers keep working, new consumers get the envelope plus the operation
 * details for admin-originated payments.
 */
export interface PaymentCompletedPayload extends PaymentResultEvent {
  source: PaymentResultSource;
  /**
   * Origin of the money movement:
   *   checkout  a hosted-checkout payment verified via queue / webhook / reconciliation
   *   admin     a DB / PA / CP operation on a stored card or earlier payment
   */
  origin: 'checkout' | 'admin';
  /** For admin-originated payments: which operation produced the transaction. */
  operation?: 'DB' | 'PA' | 'CP';
  /** For captures: the pre-authorisation that was captured. */
  referencedPaymentId?: string;
  /** Card / wallet brand reported by the gateway (transaction metadata, not card data). */
  paymentBrand?: string;
  /**
   * Commerce context of a checkout payment (order id, merchant, credit org,
   * customer and cart lines), when the checkout carried one. Downstream
   * services (billing credits, ledger, ZATCA invoicing) act on it; consumers
   * of the legacy result stream can ignore it.
   */
  order?: OrderContext;
}

// ─── TOKEN_REGISTERED ─────────────────────────────────────────────────────────

/**
 * References only. The registration token never travels over the bus
 * (charge the stored card through POST /api/admin/vault/:vaultId/charge),
 * and — because outbox rows sit in clear in the DB file — neither do the
 * masked card details (last4, expiry) or the customer e-mail: the vault's
 * rule is that card data exists on disk only as ciphertext. Consumers that
 * need the masked view fetch it via GET /api/admin/vault/:vaultId.
 * `paymentBrand` is transaction metadata (already in clear on the payment
 * record) and is included for routing / display.
 */
export interface TokenRegisteredPayload {
  correlationId: string;
  source: PaymentResultSource;
  vaultId: string;
  paymentBrand: string;
  /** OPPWA transaction that produced the token. */
  transactionId: string;
  /** `_id` of the payment record that references the vault entry. */
  dbRecordId: string;
}

// ─── REFUND_PROCESSED ─────────────────────────────────────────────────────────

/**
 * Raised when an RF operation is recorded (any outcome) and again whenever
 * reconciliation moves a PENDING refund to its final status — so a consumer
 * always learns the last word the gateway had on a refund.
 */
export interface RefundProcessedPayload {
  correlationId: string;
  source: PaymentResultSource;
  /** OPPWA id of the refund transaction. */
  transactionId: string;
  /** The debit or capture that was refunded. */
  referencedPaymentId: string;
  amount: string;
  currency: string;
  status: TransactionStatus;
  resultCode: string;
  /** `_id` of the operation record. */
  dbRecordId: string;
  /** Operator-supplied Idempotency-Key of the refund request. */
  idempotencyKey: string;
  /** Amount of the captured payment being refunded. */
  referencedAmount?: string;
  /** Σ successful / pending refunds of that payment, this one included. */
  refundedTotal?: string;
  /** The order the captured payment paid for, and its merchant. */
  orderId?: string;
  merchantId?: string;
  reason?: string;
  order?: OrderContext;
}

// ─── Union ────────────────────────────────────────────────────────────────────

export type DomainEventPayload = PaymentCompletedPayload | TokenRegisteredPayload | RefundProcessedPayload;

export type PaymentCompletedEvent = DomainEventEnvelope & PaymentCompletedPayload & { eventType: 'PAYMENT_COMPLETED' };
export type TokenRegisteredEvent = DomainEventEnvelope & TokenRegisteredPayload & { eventType: 'TOKEN_REGISTERED' };
export type RefundProcessedEvent = DomainEventEnvelope & RefundProcessedPayload & { eventType: 'REFUND_PROCESSED' };

/** What actually goes on the wire. */
export type DomainEvent = PaymentCompletedEvent | TokenRegisteredEvent | RefundProcessedEvent;
