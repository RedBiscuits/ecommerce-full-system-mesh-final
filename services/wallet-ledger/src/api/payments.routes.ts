import { FastifyInstance, FastifyReply } from 'fastify';
import { z, ZodError } from 'zod';
import { holdErrorStatus, paymentSettlementService } from '../services/PaymentSettlementService';
import { orderRefundService } from '../services/OrderRefundService';
import { NonRetryableError } from '../../../../shared/messaging';
import { TENDERS } from '../../../../shared/contracts';
import {
  CurrencyMismatchError,
  InsufficientFundsError,
  SystemAccountNotAllowedError,
  WalletFrozenError,
  WalletNotFoundError,
  WalletVersionConflictError,
} from '../types/wallet';

const amount = z.number().int().positive().safe();

/** Legacy card authorization keyed by the gateway payment id. */
const PlaceCardHoldSchema = z.object({
  paymentId: z.string().min(1).max(100),
  orderId: z.string().min(1).max(100),
  merchantId: z.string().min(1).max(64),
  amountHalalas: amount,
});

/** Keyed tender hold (checkout coordinator): CARD authorization or WALLET funds into escrow. */
const TenderHoldSchema = z.object({
  holdKey: z.string().min(1).max(160),
  orderId: z.string().min(1).max(100),
  merchantId: z.string().min(1).max(64),
  tender: z.enum(TENDERS),
  sourceWalletId: z.string().min(1).max(64).optional(),
  amountHalalas: amount,
  checkoutId: z.string().min(1).max(100).optional(),
});

const ReleaseTenderHoldSchema = TenderHoldSchema.extend({ reason: z.string().min(1).max(500) });

function sendError(reply: FastifyReply, err: unknown): FastifyReply | undefined {
  if (err instanceof ZodError) return reply.status(400).send({ error: 'Validation failed', details: err.errors });
  if (err instanceof NonRetryableError) return reply.status(holdErrorStatus(err.reason)).send({ error: err.message, code: err.reason });
  if (err instanceof InsufficientFundsError) return reply.status(409).send({ error: err.message, code: 'INSUFFICIENT_FUNDS' });
  if (err instanceof WalletNotFoundError) return reply.status(404).send({ error: err.message, code: 'WALLET_NOT_FOUND' });
  if (err instanceof WalletFrozenError) return reply.status(409).send({ error: err.message, code: err.code, walletId: err.walletId });
  if (err instanceof WalletVersionConflictError) return reply.status(409).send({ error: err.message, code: 'WALLET_VERSION_CONFLICT' });
  if (err instanceof SystemAccountNotAllowedError || err instanceof CurrencyMismatchError) {
    return reply.status(422).send({ error: err.message, code: err.code });
  }
  return undefined;
}

export async function paymentRoutes(app: FastifyInstance): Promise<void> {
  /**
   * Place a payment hold (idempotent per key). Either a card authorization
   * `{ paymentId, orderId, merchantId, amountHalalas }`, or a keyed tender hold
   * `{ holdKey, orderId, merchantId, tender: CARD | WALLET, sourceWalletId?, amountHalalas, checkoutId? }`.
   * 201 created, 200 replay of the same key, 409 insufficient funds / released key / mismatch.
   */
  app.post('/payments/holds', { config: { access: 'payment_holds' } }, async (request, reply) => {
    try {
      const body = request.body as Record<string, unknown> | null;
      if (body && typeof body === 'object' && 'holdKey' in body) {
        const hold = await paymentSettlementService.placeTenderHold(TenderHoldSchema.parse(body));
        return reply.status(hold.created ? 201 : 200).send(hold);
      }
      const hold = await paymentSettlementService.placeHold(PlaceCardHoldSchema.parse(body));
      return reply.status(201).send(hold);
    } catch (err) {
      const sent = sendError(reply, err);
      if (sent) return sent;
      throw err;
    }
  });

  /** Release the hold of a key (idempotent; an unknown key is tombstoned). 409 if captured. */
  app.post('/payments/holds/release', { config: { access: 'payment_holds' } }, async (request, reply) => {
    try {
      const result = await paymentSettlementService.releaseTenderHold(ReleaseTenderHoldSchema.parse(request.body));
      return reply.status(200).send(result);
    } catch (err) {
      const sent = sendError(reply, err);
      if (sent) return sent;
      throw err;
    }
  });

  app.get<{ Params: { holdKey: string } }>('/payments/holds/:holdKey', { config: { access: 'payment_holds' } }, async (request, reply) => {
    const hold = await paymentSettlementService.getHold(request.params.holdKey);
    return hold ? reply.status(200).send(hold) : reply.status(404).send({ error: 'Hold not found', code: 'HOLD_NOT_FOUND' });
  });

  /** Holds, settlement, the posted journal and the consumer checkpoints of one order. */
  app.get<{ Params: { orderId: string } }>(
    '/payments/orders/:orderId/settlement',
    { config: { access: 'admin' } },
    async (request, reply) => {
      return reply.status(200).send(await paymentSettlementService.getOrderSettlement(request.params.orderId));
    }
  );

  /** Refunds reversed for an order: each refund's split and journal, the running totals, the checkpoints. */
  app.get<{ Params: { orderId: string } }>('/payments/orders/:orderId/refunds', { config: { access: 'admin' } }, async (request, reply) => {
    return reply.send(await orderRefundService.getOrderRefunds(request.params.orderId));
  });
}
