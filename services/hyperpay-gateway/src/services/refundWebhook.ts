/**
 * src/services/refundWebhook.ts
 *
 * Ingestion of a (signature-verified) HyperPay REFUND notification: an OPPWA `RF` transaction
 * that references the captured payment it refunds.
 *
 * ONE DB transaction per delivery: refund record + REFUND_PROCESSED outbox row, committed
 * together; the outbox publisher signs and moves the event to `hyperpay / refund.processed`
 * (stream hyperpay.refund.processed, audience: billing), from where the refund flows through
 * billing (credits restored) → ledger (reversal journal) → ZATCA (credit note) → file cache.
 *
 * Refusals (nothing recorded, HTTP 409 / 422):
 *   ORIGINAL_PAYMENT_UNKNOWN     the referenced payment was never recorded here
 *   ORIGINAL_PAYMENT_NOT_CAPTURED the referenced payment is not a successful checkout payment
 *   CURRENCY_MISMATCH
 *   REFUND_EXCEEDS_REFUNDABLE    amount > captured amount − refunds already recorded (successful
 *                                or still pending) for that payment: the total refunded can never
 *                                exceed what was charged
 *
 * Idempotency — one record per OPPWA refund id, one event per outcome:
 *   - same refund, same status (webhook retry)      → DUPLICATE, no new event
 *   - PENDING → SUCCESS/FAILED                      → record updated, new event
 *   - contradicting final outcome                   → CONFLICT recorded, no event
 */
import { z } from 'zod';
import { config } from '../config/env';
import { PAYMENTS_COLLECTION, REFUNDS_COLLECTION } from './collections';
import { db } from './jsonDb';
import type { DbRecord, DbTransaction } from './jsonDb';
import { outbox } from './outbox';
import { orderContextFrom, parsePaymentWebhook, type PaymentWebhookBody } from './paymentWebhook';
import { statusFromResultCode } from './resultCodes';
import { fromMinor, toMinor } from '../utils/money';
import type { PaymentRecord, TransactionStatus } from '../types/hyperpay';
import type { RefundProcessedPayload } from '../types/events';
import type { OrderContext } from '../../../../shared/contracts/order';

/** Shape stored in the JSON DB under the "refunds" collection: one record per OPPWA refund id. */
export interface RefundRecord {
  idempotencyKey: string;
  /** OPPWA id of the RF transaction. */
  refundId: string;
  referencedPaymentId: string;
  orderId: string;
  merchantId: string;
  status: TransactionStatus;
  amount: string;
  amountHalalas: number;
  currency: string;
  resultCode: string;
  resultDescription: string;
  reason?: string;
  rawTimestamp: string;
  lastEventId?: string;
  statusConflicts?: Array<{ reportedStatus: TransactionStatus; resultCode: string; receivedAt: string }>;
}

type StoredRefund = RefundRecord & DbRecord;
type StoredPayment = PaymentRecord & DbRecord;

export class RefundRefusedError extends Error {
  constructor(
    public readonly statusCode: number,
    public readonly code: string,
    message: string
  ) {
    super(message);
    this.name = 'RefundRefusedError';
  }
}

export type RefundOutcome = 'RECORDED' | 'STATUS_UPDATED' | 'DUPLICATE' | 'CONFLICT';

export interface RefundIngestionResult {
  outcome: RefundOutcome;
  refundId: string;
  paymentId: string;
  orderId: string;
  dbRecordId: string;
  status: TransactionStatus;
  amountHalalas: number;
  /** Σ successful / pending refunds of the payment after this one. */
  refundedTotalHalalas: number;
  refundableHalalas: number;
  eventId?: string;
}

const RefundExtrasSchema = z.object({
  referencedId: z.string().min(1).max(100),
  paymentType: z.literal('RF'),
  /** Why the customer was refunded (free text from the back office / OPPWA `descriptor`). */
  reason: z.string().max(500).optional(),
});

/** Whether a signed notification is a refund (RF) rather than a payment. */
export function isRefundNotification(body: unknown): boolean {
  return typeof body === 'object' && body !== null && (body as { paymentType?: unknown }).paymentType === 'RF';
}

export async function ingestRefundWebhook(rawBody: unknown): Promise<RefundIngestionResult> {
  const body = parsePaymentWebhook(rawBody);
  const extras = RefundExtrasSchema.safeParse(rawBody);
  if (!extras.success) {
    throw new RefundRefusedError(400, 'INVALID_REFUND_NOTIFICATION', `A refund (RF) notification needs referencedId: ${extras.error.issues.map((i) => i.message).join('; ')}`);
  }
  const { referencedId, reason } = extras.data;
  const status = statusFromResultCode(body.result.code);
  const amountHalalas = toMinor(body.amount);
  const idempotencyKey = body.resourcePath ?? `/v1/payments/${referencedId}/refunds/${body.id}`;

  return db.transaction(async (tx) => {
    const original = await tx.findOne<StoredPayment>(PAYMENTS_COLLECTION, (p) => p.transactionId === referencedId);
    if (!original) {
      throw new RefundRefusedError(409, 'ORIGINAL_PAYMENT_UNKNOWN', `Refund ${body.id} references payment ${referencedId}, which is not recorded`);
    }
    if (original.status !== 'SUCCESS' || !original.orderId) {
      throw new RefundRefusedError(
        422,
        'ORIGINAL_PAYMENT_NOT_CAPTURED',
        `Refund ${body.id} references payment ${referencedId}, which is ${original.status}${original.orderId ? '' : ' and not an order payment'}`
      );
    }
    if (body.currency !== original.currency) {
      throw new RefundRefusedError(422, 'CURRENCY_MISMATCH', `Refund ${body.id} is in ${body.currency}, the payment in ${original.currency}`);
    }
    const capturedHalalas = toMinor(original.amount);
    const order = orderContextFrom(parsePaymentWebhook(original.raw) as PaymentWebhookBody);
    const merchantId = order?.merchantId ?? config.commerce.defaultMerchantId;

    // Everything already counted against the payment (successful or still pending), other refunds only.
    const others = await tx.findWhere<StoredRefund>(
      REFUNDS_COLLECTION,
      (r) => r.referencedPaymentId === referencedId && r.refundId !== body.id && r.status !== 'FAILED'
    );
    const refundedByOthers = others.reduce((s, r) => s + r.amountHalalas, 0);
    const existing = await tx.findOne<StoredRefund>(REFUNDS_COLLECTION, (r) => r.refundId === body.id || r.idempotencyKey === idempotencyKey);

    const summary = (record: StoredRefund, outcome: RefundOutcome, eventId?: string): RefundIngestionResult => {
      const counted = record.status === 'FAILED' ? 0 : record.amountHalalas;
      return {
        outcome,
        refundId: body.id,
        paymentId: referencedId,
        orderId: original.orderId as string,
        dbRecordId: record._id,
        status: record.status,
        amountHalalas: record.amountHalalas,
        refundedTotalHalalas: refundedByOthers + counted,
        refundableHalalas: capturedHalalas - refundedByOthers - counted,
        eventId,
      };
    };

    if (existing) {
      if (existing.status === status) return summary(existing, 'DUPLICATE', existing.lastEventId);
      if (existing.status !== 'PENDING') {
        await tx.update<StoredRefund>(REFUNDS_COLLECTION, existing._id, {
          statusConflicts: [
            ...(existing.statusConflicts ?? []),
            { reportedStatus: status, resultCode: body.result.code, receivedAt: new Date().toISOString() },
          ],
        });
        console.warn(`[Webhook] refund ${body.id}: ${status} reported after final ${existing.status} — recorded as a conflict, no event`);
        return summary(existing, 'CONFLICT', existing.lastEventId);
      }
      const updated =
        (await tx.update<StoredRefund>(REFUNDS_COLLECTION, existing._id, {
          status,
          resultCode: body.result.code,
          resultDescription: body.result.description,
          rawTimestamp: body.timestamp ?? new Date().toISOString(),
        })) ?? existing;
      const eventId = await announce(tx, updated, capturedHalalas, refundedByOthers, order);
      await tx.update<StoredRefund>(REFUNDS_COLLECTION, existing._id, { lastEventId: eventId });
      return summary({ ...updated, status }, 'STATUS_UPDATED', eventId);
    }

    if (amountHalalas <= 0) throw new RefundRefusedError(422, 'INVALID_REFUND_AMOUNT', `Refund ${body.id} has no amount`);
    if (status !== 'FAILED' && amountHalalas > capturedHalalas - refundedByOthers) {
      throw new RefundRefusedError(
        422,
        'REFUND_EXCEEDS_REFUNDABLE',
        `Refund ${body.id} of ${fromMinor(amountHalalas)} ${body.currency} exceeds the ${fromMinor(capturedHalalas - refundedByOthers)} still refundable ` +
          `on payment ${referencedId} (captured ${original.amount}, already refunded ${fromMinor(refundedByOthers)})`
      );
    }

    const record: RefundRecord = {
      idempotencyKey,
      refundId: body.id,
      referencedPaymentId: referencedId,
      orderId: original.orderId,
      merchantId,
      status,
      amount: fromMinor(amountHalalas),
      amountHalalas,
      currency: body.currency,
      resultCode: body.result.code,
      resultDescription: body.result.description,
      ...(reason ? { reason } : {}),
      rawTimestamp: body.timestamp ?? new Date().toISOString(),
    };
    const saved = await tx.insert<RefundRecord>(REFUNDS_COLLECTION, record);
    const eventId = await announce(tx, saved, capturedHalalas, refundedByOthers, order);
    await tx.update<StoredRefund>(REFUNDS_COLLECTION, saved._id, { lastEventId: eventId });
    return summary(saved, 'RECORDED', eventId);
  });
}

async function announce(
  tx: DbTransaction,
  refund: StoredRefund,
  capturedHalalas: number,
  refundedByOthers: number,
  order: OrderContext | undefined
): Promise<string> {
  const counted = refund.status === 'FAILED' ? 0 : refund.amountHalalas;
  const payload: RefundProcessedPayload = {
    correlationId: refund.orderId,
    source: 'webhook',
    transactionId: refund.refundId,
    referencedPaymentId: refund.referencedPaymentId,
    amount: refund.amount,
    currency: refund.currency,
    status: refund.status,
    resultCode: refund.resultCode,
    dbRecordId: refund._id,
    idempotencyKey: refund.idempotencyKey,
    referencedAmount: fromMinor(capturedHalalas),
    refundedTotal: fromMinor(refundedByOthers + counted),
    orderId: refund.orderId,
    merchantId: refund.merchantId,
    ...(refund.reason ? { reason: refund.reason } : {}),
    ...(order ? { order } : {}),
  };
  const row = await outbox.enqueue(tx, {
    eventType: 'REFUND_PROCESSED',
    aggregateType: 'refund',
    aggregateId: refund._id,
    correlationId: refund.orderId,
    exchange: config.rabbitmq.exchange,
    routingKey: config.rabbitmq.refundProcessedRoutingKey,
    payload,
  });
  return row._id;
}

/** Refunds recorded against a payment (for traces and the admin API). */
export async function refundsOfPayment(paymentId: string): Promise<StoredRefund[]> {
  return db.findWhere<StoredRefund>(REFUNDS_COLLECTION, (r) => r.referencedPaymentId === paymentId);
}
