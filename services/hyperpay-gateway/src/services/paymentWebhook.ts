/**
 * src/services/paymentWebhook.ts
 *
 * Ingestion of a (signature-verified) HyperPay payment notification.
 *
 * ONE DB transaction per delivery:
 *   payment record (card data redacted)  +  vault entry (if tokenised)
 *   +  PAYMENT_COMPLETED outbox row
 * so the payment and the event announcing it are committed together or not at
 * all; the outbox publisher moves the event to the broker afterwards (no dual
 * write on the request path).
 *
 * Idempotency — one record per OPPWA payment id, one event per outcome:
 *   - same payment, same status (webhook retry)      → DUPLICATE, no new event
 *   - PENDING → SUCCESS/FAILED (status progressed)   → record updated, new event
 *   - SUCCESS ↔ FAILED (final outcome contradicted)  → CONFLICT recorded, no event
 *                                                      (a human must reconcile; money
 *                                                      is never moved on a contradiction)
 */

import { z } from 'zod';
import { config } from '../config/env';
import { OrderContext, OrderContextSchema, VAT_CATEGORIES, VAT_EXEMPTION_CODES } from '../../../../shared/contracts/order';
import { PAYMENTS_COLLECTION } from './collections';
import { db } from './jsonDb';
import type { DbRecord, DbTransaction } from './jsonDb';
import { outbox } from './outbox';
import { paymentMethodFrom, redactPaymentResource, vault } from './vault';
import { statusFromResultCode } from './resultCodes';
import { toMinor, fromMinor } from '../utils/money';
import type { OppwaPaymentResource, PaymentRecord, TransactionStatus } from '../types/hyperpay';
import type { PaymentCompletedPayload } from '../types/events';

// ─── Payload ──────────────────────────────────────────────────────────────────

const amountString = z.union([z.string(), z.number()]).transform((v, ctx) => {
  const text = typeof v === 'number' ? v.toFixed(2) : v.trim();
  if (!/^\d+(\.\d{1,2})?$/.test(text)) {
    ctx.addIssue({ code: z.ZodIssueCode.custom, message: `invalid amount "${text}"` });
    return z.NEVER;
  }
  return text;
});
const intString = z.union([z.string(), z.number()]).transform((v, ctx) => {
  const n = typeof v === 'number' ? v : Number(v);
  if (!Number.isInteger(n) || n <= 0) {
    ctx.addIssue({ code: z.ZodIssueCode.custom, message: `invalid quantity "${v}"` });
    return z.NEVER;
  }
  return n;
});

/** Cart item as OPPWA sends it (`cart.items[n]`), plus optional KSA VAT metadata. */
const CartItemSchema = z.object({
  merchantItemId: z.string().min(1).optional(),
  sku: z.string().min(1).optional(),
  name: z.string().min(1),
  nameAr: z.string().min(1).optional(),
  quantity: intString,
  /** Unit price excluding VAT, SAR. */
  price: amountString,
  discount: amountString.optional(),
  vatCategory: z.enum(VAT_CATEGORIES).optional(),
  vatExemptionCode: z.enum(VAT_EXEMPTION_CODES).optional(),
});

/** The payment resource HyperPay delivers (OPPWA shape), with the commerce extras. */
export const PaymentWebhookSchema = z
  .object({
    id: z.string().min(1).max(100),
    resourcePath: z.string().min(1).optional(),
    paymentType: z.string().optional(),
    paymentBrand: z.string().optional(),
    amount: amountString,
    currency: z.string().length(3),
    merchantTransactionId: z.string().min(1).max(100).optional(),
    descriptor: z.string().optional(),
    result: z.object({ code: z.string().min(1), description: z.string().default('') }),
    timestamp: z.string().optional(),
    ndc: z.string().optional(),
    registrationId: z.string().optional(),
    card: z
      .object({
        bin: z.string().optional(),
        last4Digits: z.string().optional(),
        holder: z.string().optional(),
        expiryMonth: z.string().optional(),
        expiryYear: z.string().optional(),
        type: z.string().optional(),
      })
      .partial()
      .optional(),
    customer: z
      .object({
        givenName: z.string().optional(),
        surname: z.string().optional(),
        email: z.string().optional(),
        mobile: z.string().optional(),
        phone: z.string().optional(),
        ip: z.string().optional(),
      })
      .partial()
      .optional(),
    customParameters: z.record(z.string()).optional(),
    cart: z.object({ items: z.array(CartItemSchema).min(1) }).optional(),
  })
  .passthrough();

export type PaymentWebhookBody = z.infer<typeof PaymentWebhookSchema>;

export class WebhookPayloadError extends Error {
  public readonly statusCode = 400;
  constructor(public readonly details: string[]) {
    super(`Invalid payment notification: ${details.join('; ')}`);
    this.name = 'WebhookPayloadError';
  }
}

export type WebhookOutcome = 'RECORDED' | 'STATUS_UPDATED' | 'DUPLICATE' | 'CONFLICT';

export interface WebhookIngestionResult {
  outcome: WebhookOutcome;
  paymentId: string;
  dbRecordId: string;
  status: TransactionStatus;
  orderId?: string;
  /** eventId of the PAYMENT_COMPLETED event this delivery produced (or the earlier one, for a duplicate). */
  eventId?: string;
}

type StoredPayment = PaymentRecord & DbRecord;

// ─── Mapping ──────────────────────────────────────────────────────────────────

/** Commerce context of a checkout; `undefined` for payments without an order / cart. */
export function orderContextFrom(body: PaymentWebhookBody): OrderContext | undefined {
  if (!body.merchantTransactionId || !body.cart) return undefined;
  const params = body.customParameters ?? {};
  const customerName =
    [body.customer?.givenName, body.customer?.surname].filter((p) => p && p.trim()).join(' ').trim() ||
    params.customerName ||
    'Walk-in Customer';

  const candidate = {
    orderId: body.merchantTransactionId,
    merchantId: params.merchantId || config.commerce.defaultMerchantId,
    creditOrgId: params.creditOrgId || null,
    // Split-tender checkout (coordinator) whose card remainder this payment pays.
    ...(params.checkoutId ? { checkoutId: params.checkoutId } : {}),
    customer: {
      name: customerName,
      ...(body.customer?.email ? { email: body.customer.email } : {}),
      ...(body.customer?.mobile || body.customer?.phone ? { phone: body.customer.mobile ?? body.customer.phone } : {}),
    },
    lines: body.cart.items.map((item, index) => ({
      sku: item.merchantItemId ?? item.sku ?? `LINE-${index + 1}`,
      nameEn: item.name,
      nameAr: item.nameAr ?? item.name,
      quantity: item.quantity,
      unitPriceHalalas: toMinor(item.price),
      ...(item.discount ? { discountHalalas: toMinor(item.discount) } : {}),
      ...(item.vatCategory ? { vatCategory: item.vatCategory } : {}),
      ...(item.vatExemptionCode ? { vatExemptionCode: item.vatExemptionCode } : {}),
    })),
  };
  const parsed = OrderContextSchema.safeParse(candidate);
  if (!parsed.success) {
    throw new WebhookPayloadError(parsed.error.issues.map((i) => `order.${i.path.join('.')}: ${i.message}`));
  }
  return parsed.data;
}

export function parsePaymentWebhook(body: unknown): PaymentWebhookBody {
  const parsed = PaymentWebhookSchema.safeParse(body);
  if (!parsed.success) {
    throw new WebhookPayloadError(parsed.error.issues.map((i) => `${i.path.join('.') || '(body)'}: ${i.message}`));
  }
  return parsed.data;
}

function toResource(body: PaymentWebhookBody): OppwaPaymentResource {
  return {
    ...(body as unknown as OppwaPaymentResource),
    amount: fromMinor(toMinor(body.amount)),
    timestamp: body.timestamp ?? new Date().toISOString(),
    ndc: body.ndc ?? '',
    paymentType: body.paymentType ?? 'DB',
    result: { code: body.result.code, description: body.result.description },
  } as OppwaPaymentResource;
}

// ─── Ingestion ────────────────────────────────────────────────────────────────

export async function ingestPaymentWebhook(rawBody: unknown): Promise<WebhookIngestionResult> {
  const body = parsePaymentWebhook(rawBody);
  const order = orderContextFrom(body);
  const status = statusFromResultCode(body.result.code);
  const resource = toResource(body);
  const idempotencyKey = body.resourcePath ?? `/v1/payments/${body.id}`;

  return db.transaction(async (tx) => {
    const existing = await tx.findOne<StoredPayment>(
      PAYMENTS_COLLECTION,
      (p) => p.transactionId === body.id || p.idempotencyKey === idempotencyKey
    );

    if (existing) {
      const base = { paymentId: body.id, dbRecordId: existing._id, orderId: existing.orderId };
      if (existing.status === status) {
        return { ...base, outcome: 'DUPLICATE', status, eventId: existing.lastEventId };
      }
      if (existing.status !== 'PENDING') {
        // A final outcome is never overwritten by a contradicting one.
        await tx.update<StoredPayment>(PAYMENTS_COLLECTION, existing._id, {
          statusConflicts: [
            ...(existing.statusConflicts ?? []),
            { reportedStatus: status, resultCode: body.result.code, receivedAt: new Date().toISOString() },
          ],
        });
        console.warn(
          `[Webhook] payment ${body.id}: ${status} reported after final ${existing.status} — recorded as a conflict, no event`
        );
        return { ...base, outcome: 'CONFLICT', status: existing.status, eventId: existing.lastEventId };
      }
      const updated = await tx.update<StoredPayment>(PAYMENTS_COLLECTION, existing._id, {
        status,
        resultCode: body.result.code,
        resultDescription: body.result.description,
        raw: redactPaymentResource(resource),
        rawTimestamp: resource.timestamp,
      });
      const eventId = await announce(tx, updated ?? existing, status, body, order);
      await tx.update<StoredPayment>(PAYMENTS_COLLECTION, existing._id, { lastEventId: eventId });
      return { ...base, outcome: 'STATUS_UPDATED', status, eventId };
    }

    // Tokenised payment: the registration token is sealed in the vault in this
    // same transaction; the payment record only ever holds the redacted resource.
    const method = paymentMethodFrom(resource);
    const vaulted = method
      ? await vault.store(method, { transactionId: body.id, customerEmail: body.customer?.email }, tx)
      : null;

    const record: PaymentRecord = {
      idempotencyKey,
      transactionId: body.id,
      status,
      amount: resource.amount,
      currency: body.currency,
      ...(body.paymentBrand ? { paymentBrand: body.paymentBrand } : {}),
      resultCode: body.result.code,
      resultDescription: body.result.description,
      ...(body.customer?.email ? { customerEmail: body.customer.email } : {}),
      rawTimestamp: resource.timestamp,
      ...(vaulted ? { vaultId: vaulted.vaultId } : {}),
      raw: redactPaymentResource(resource),
      ...(order ? { orderId: order.orderId } : {}),
    };
    const saved = await tx.insert<PaymentRecord>(PAYMENTS_COLLECTION, record);
    const eventId = await announce(tx, saved, status, body, order);
    await tx.update<StoredPayment>(PAYMENTS_COLLECTION, saved._id, { lastEventId: eventId });
    return { outcome: 'RECORDED', paymentId: body.id, dbRecordId: saved._id, status, orderId: order?.orderId, eventId };
  });
}

async function announce(
  tx: DbTransaction,
  payment: StoredPayment,
  status: TransactionStatus,
  body: PaymentWebhookBody,
  order: OrderContext | undefined
): Promise<string> {
  const correlationId = order?.orderId ?? `webhook:${payment._id}`;
  const payload: PaymentCompletedPayload = {
    correlationId,
    source: 'webhook',
    origin: 'checkout',
    transactionId: payment.transactionId,
    status,
    amount: payment.amount,
    currency: payment.currency,
    resultCode: body.result.code,
    dbRecordId: payment._id,
    ...(payment.vaultId ? { vaultId: payment.vaultId } : {}),
    ...(body.paymentBrand ? { paymentBrand: body.paymentBrand } : {}),
    ...(order ? { order } : {}),
  };
  const row = await outbox.enqueue(tx, {
    eventType: 'PAYMENT_COMPLETED',
    aggregateType: 'payment',
    aggregateId: payment._id,
    correlationId,
    exchange: config.rabbitmq.exchange,
    routingKey: config.rabbitmq.resultsRoutingKey,
    payload,
  });
  return row._id;
}
