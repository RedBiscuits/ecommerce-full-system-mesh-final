import { FastifyInstance } from 'fastify';
import { z } from 'zod';
import { streamHoldService } from '../services/StreamHoldService';
import { config } from '../config/env';
import { orderCreditSettlementService } from '../integration/OrderCreditSettlementService';
import { orderCreditRefundService } from '../integration/OrderCreditRefundService';

const PlaceOrderCreditHoldSchema = z.object({
  orgId: z.string().min(1),
  credits: z.number().int().positive(),
  ttlSeconds: z.number().int().positive().optional(),
  holdKey: z.string().min(1).max(160).optional(),
});

const ReleaseOrderCreditHoldSchema = z.object({
  holdKey: z.string().min(1).max(160),
  reason: z.string().min(1).max(500),
});

export async function ordersRoutes(app: FastifyInstance): Promise<void> {
  /**
   * Reserve credits for an order at checkout. The payment outcome captures (SUCCESS) or releases
   * (FAILED) the hold. Idempotent: re-placing the same hold returns it (200).
   */
  app.post<{ Params: { orderId: string } }>('/credits/orders/:orderId/hold', async (request, reply) => {
    const input = PlaceOrderCreditHoldSchema.parse(request.body);
    const hold = await streamHoldService.createOrderCreditHold({ ...input, orderId: request.params.orderId });
    const halalasPerCredit = config.orderCredits.halalasPerCredit;
    // The value the credits will have as the order's document-level allowance, once captured.
    return reply
      .status(hold.created ? 201 : 200)
      .send({ ...hold, halalasPerCredit, allowanceHalalas: hold.estimatedCredits * halalasPerCredit });
  });

  /**
   * Release the credit hold of a reservation key (idempotent; a key without a hold is cancelled
   * so a late placement can never create one). 409 when the credits were already captured.
   */
  app.post<{ Params: { orderId: string } }>('/credits/orders/:orderId/hold/release', async (request, reply) => {
    const input = ReleaseOrderCreditHoldSchema.parse(request.body);
    const result = await streamHoldService.releaseOrderCreditHold({ ...input, orderId: request.params.orderId });
    return reply.status(200).send(result);
  });

  /** What billing did for an order's payments, with its consumer checkpoints. */
  app.get<{ Params: { orderId: string } }>('/credits/orders/:orderId/settlement', async (request, reply) => {
    return reply.status(200).send(await orderCreditSettlementService.getOrderSettlements(request.params.orderId));
  });

  /** Refunds applied to an order: the credits restored per refund, with billing's checkpoints. */
  app.get<{ Params: { orderId: string } }>('/credits/orders/:orderId/refunds', async (request, reply) => {
    return reply.send(await orderCreditRefundService.getOrderRefunds(request.params.orderId));
  });
}
