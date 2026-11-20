import { FastifyInstance, FastifyReply } from 'fastify';
import { ZodError } from 'zod';
import { CheckoutCoordinator, CheckoutError, CheckoutView } from '../services/CheckoutCoordinator';

/** HTTP status of a checkout as returned by POST /checkouts. */
function statusFor(view: CheckoutView, created: boolean): number {
  switch (view.status) {
    case 'RESERVED':
      return created ? 201 : 200;
    case 'FAILED':
      return view.failure?.code.endsWith('_UNAVAILABLE') ? 503 : 422;
    case 'COMPENSATING':
      return 503; // releases pending on an unavailable service; the recovery sweep finishes them
    default:
      return 200;
  }
}

function sendError(reply: FastifyReply, err: unknown): FastifyReply {
  if (err instanceof ZodError) return reply.status(400).send({ error: 'Validation failed', code: 'VALIDATION_ERROR', details: err.errors });
  if (err instanceof CheckoutError) {
    return reply.status(err.statusCode).send({ error: err.message, code: err.code, ...(err.checkout ? { checkout: err.checkout } : {}) });
  }
  throw err;
}

export function registerCheckoutRoutes(app: FastifyInstance, coordinator: CheckoutCoordinator): void {
  /**
   * Reserve a split-tender checkout: promotional credits at billing + the remainder on the
   * customer's card (authorization) or wallet (escrow). Idempotent per checkoutId (also accepted
   * as the Idempotency-Key header). 201 reserved · 200 replay · 422 refused (both reservations
   * released, failure code of the refusal) · 503 a service was unavailable · 409 conflicts.
   */
  app.post('/checkouts', async (request, reply) => {
    try {
      const headerKey = request.headers['idempotency-key'];
      const body = { ...(request.body as Record<string, unknown>) };
      if (body.checkoutId === undefined && typeof headerKey === 'string') body.checkoutId = headerKey;
      const { checkout, created } = await coordinator.startCheckout(body);
      return reply.status(statusFor(checkout, created)).send(checkout);
    } catch (err) {
      return sendError(reply, err);
    }
  });

  app.get<{ Params: { checkoutId: string } }>('/checkouts/:checkoutId', async (request, reply) => {
    const checkout = await coordinator.getCheckout(request.params.checkoutId);
    if (!checkout) return reply.status(404).send({ error: 'Checkout not found', code: 'CHECKOUT_NOT_FOUND' });
    return reply.send({ ...checkout, steps: await coordinator.listSteps(request.params.checkoutId) });
  });

  /** WALLET remainder: confirm the purchase; the order pipeline captures both reservations. */
  app.post<{ Params: { checkoutId: string } }>('/checkouts/:checkoutId/confirm', async (request, reply) => {
    try {
      return reply.status(202).send(await coordinator.confirmCheckout(request.params.checkoutId));
    } catch (err) {
      return sendError(reply, err);
    }
  });

  /** Release both reservations (idempotent). */
  app.post<{ Params: { checkoutId: string } }>('/checkouts/:checkoutId/cancel', async (request, reply) => {
    try {
      return reply.send(await coordinator.cancelCheckout(request.params.checkoutId));
    } catch (err) {
      return sendError(reply, err);
    }
  });

  /** Run the expiry and crash-recovery sweeps now (they also run on a timer). */
  app.post('/checkouts/maintenance', async (_request, reply) => {
    const expired = await coordinator.expireDue();
    const recovered = await coordinator.recoverStuck();
    return reply.send({ expired, recovered });
  });
}
