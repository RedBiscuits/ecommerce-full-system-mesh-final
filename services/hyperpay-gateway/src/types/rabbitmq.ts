/**
 * src/types/rabbitmq.ts
 *
 * Typed envelopes for events published to / consumed from RabbitMQ.
 */

import type { TransactionStatus } from './hyperpay';

// ─── Inbound (consumed by this service) ──────────────────────────────────────

/**
 * Event published by any upstream service (e.g. a payment gateway adapter or
 * the HTTP ingress) to request verification of a HyperPay transaction.
 *
 * Routing key: `payment.verify`
 * Exchange:    `hyperpay` (direct)
 * Queue:       `hyperpay.payments`
 */
export interface PaymentVerifyEvent {
  /** Opaque ID for correlating the inbound event with its result. */
  correlationId: string;
  /** OPPWA resource path, e.g. "/v1/checkouts/ABC123/payment" */
  resourcePath?: string;
  /** Checkout / payment ID used when resourcePath is absent. */
  checkoutId?: string;
}

// ─── Outbound (published by this service) ────────────────────────────────────

/**
 * Event published after a payment has been verified and persisted.
 *
 * Routing key: `payment.result`
 * Exchange:    `hyperpay` (direct)
 * Queue:       `hyperpay.results` (and `hyperpay.events`)
 *
 * This is the wire body of the PAYMENT_COMPLETED domain event (see
 * types/events.ts). It is written to the transactional outbox in the same
 * DB transaction as the payment record and published from there — never
 * directly from the request path.
 *
 * What triggered the verification:
 *   verify          an inbound PaymentVerifyEvent from the queue
 *   webhook         HyperPay's direct webhook delivery
 *   admin           an operator money-movement call (charge / capture)
 *   reconciliation  the background / on-demand re-verification of PENDING records
 *   remediation     the operator command correcting FAILED records
 */
export type PaymentResultSource = 'verify' | 'webhook' | 'admin' | 'reconciliation' | 'remediation';

export interface PaymentResultEvent {
  /**
   * Echoed from the inbound PaymentVerifyEvent, or the operator's
   * Idempotency-Key for admin operations. Events without a caller-supplied id
   * (webhook, reconciliation, remediation) use `<source>:<dbRecordId>`.
   */
  correlationId: string;
  /** What produced this event. Absent means `verify` (the normal flow). */
  source?: PaymentResultSource;
  /** OPPWA transaction ID. */
  transactionId: string;
  /** Normalised status determined by this service. */
  status: TransactionStatus;
  amount: string;
  currency: string;
  resultCode: string;
  /** _id of the record persisted in the JSON DB. */
  dbRecordId: string;
  /**
   * Reference to the vaulted payment method, when the payment was tokenised.
   * Upstream can charge the stored card later via the vault — the token
   * itself never travels over the bus.
   */
  vaultId?: string;
}
