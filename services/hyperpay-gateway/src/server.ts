import Fastify, { type FastifyInstance } from 'fastify';
import { config } from './config/env';
import { SIGNATURE_HEADER, verify } from './security/webhookSignature';
import { gatewayBreaker } from './services/gatewayBreaker';
import { outbox } from './services/outbox';
import { vault } from './services/vault';
import { WebhookPayloadError, ingestPaymentWebhook } from './services/paymentWebhook';
import { RefundRefusedError, ingestRefundWebhook, isRefundNotification, refundsOfPayment } from './services/refundWebhook';
import type { VaultedPaymentMethod } from './types/hyperpay';
import { startHyperPayMessaging } from './messaging/runtime';
import { brokerDriverFromEnv, resolveAmqpConnect } from '../../../shared/messaging';

declare module 'fastify' {
  interface FastifyRequest {
    /** Exact bytes of a JSON request body, as received (what webhook signatures cover). */
    rawBody?: Buffer;
  }
}

export interface HyperPayServerOptions {
  /** HMAC secret for webhook signatures; defaults to WEBHOOK_SECRET (dev fallback outside production). */
  webhookSecret?: string;
  webhookToleranceSec?: number;
}

/** Dev-only fallback; config/env.ts refuses to start in production without WEBHOOK_SECRET. */
const DEV_WEBHOOK_SECRET = 'default-webhook-key';

export function buildHyperPayServer(options: HyperPayServerOptions = {}): FastifyInstance {
  const app = Fastify({ logger: false });
  const webhookSecret = options.webhookSecret ?? config.webhook.secret ?? DEV_WEBHOOK_SECRET;
  const toleranceSec = options.webhookToleranceSec ?? config.webhook.signatureToleranceSec;

  // Keep the raw bytes: the webhook MAC is computed over exactly what the
  // sender transmitted, never over a re-serialised object (key order and
  // whitespace would silently break verification).
  app.addContentTypeParser('application/json', { parseAs: 'buffer' }, (request, body, done) => {
    const raw = body as Buffer;
    request.rawBody = raw;
    if (raw.length === 0) {
      done(null, {});
      return;
    }
    try {
      done(null, JSON.parse(raw.toString('utf8')));
    } catch {
      const err = new Error('Request body is not valid JSON') as Error & { statusCode: number };
      err.statusCode = 400;
      done(err, undefined);
    }
  });

  app.get('/health', async () => {
    const outboxStats = await outbox.stats();
    const circuit = gatewayBreaker.getMetrics();
    return {
      status: 'ok',
      service: 'hyperpay-gateway',
      vaultEnabled: vault.enabled,
      circuitState: circuit.state,
      outboxPending: outboxStats.pending,
    };
  });

  app.get('/api/circuit', async () => {
    return gatewayBreaker.getMetrics();
  });

  app.get('/api/outbox/stats', async () => {
    return outbox.stats();
  });

  app.post<{
    Body: {
      transactionId: string;
      customerId?: string;
      method: VaultedPaymentMethod;
    };
  }>('/api/vault/tokenize', async (request, reply) => {
    const { transactionId, customerId, method } = request.body;
    const stored = await vault.store(method, {
      transactionId,
      customerEmail: customerId,
    });
    if (!stored) {
      return reply.code(503).send({ error: 'VAULT_DISABLED' });
    }
    return reply.code(stored.created ? 201 : 200).send(stored);
  });

  /**
   * Signed HyperPay payment notification.
   *   401  missing / malformed / stale / forged signature (nothing recorded)
   *   400  signed but not a valid payment notification (nothing recorded)
   *   202  recorded: payment + PAYMENT_COMPLETED event committed together
   *   200  acknowledged replay (duplicate) or contradicting outcome (conflict): no new event
   */
  app.post('/api/webhook/hyperpay', async (request, reply) => {
    const sigHeader = request.headers[SIGNATURE_HEADER] as string | undefined;
    const rawBody = request.rawBody ?? Buffer.from(JSON.stringify(request.body ?? {}));
    const check = verify(sigHeader, rawBody, { secret: webhookSecret, toleranceSec });
    if (!check.ok) {
      return reply.code(401).send({ error: 'INVALID_WEBHOOK_SIGNATURE', reason: check.reason });
    }

    try {
      if (isRefundNotification(request.body)) {
        // RF: a customer refund of a captured payment (→ refund.processed).
        const result = await ingestRefundWebhook(request.body);
        const fresh = result.outcome === 'RECORDED' || result.outcome === 'STATUS_UPDATED';
        const { status: refundStatus, ...rest } = result;
        return reply.code(fresh ? 202 : 200).send({ status: fresh ? 'ACCEPTED' : result.outcome, kind: 'REFUND', refundStatus, ...rest });
      }
      const result = await ingestPaymentWebhook(request.body);
      const fresh = result.outcome === 'RECORDED' || result.outcome === 'STATUS_UPDATED';
      const { status: paymentStatus, ...rest } = result;
      return reply.code(fresh ? 202 : 200).send({ status: fresh ? 'ACCEPTED' : result.outcome, paymentStatus, ...rest });
    } catch (err) {
      if (err instanceof WebhookPayloadError) {
        return reply.code(400).send({ error: 'INVALID_PAYMENT_NOTIFICATION', details: err.details });
      }
      if (err instanceof RefundRefusedError) {
        return reply.code(err.statusCode).send({ error: err.code, message: err.message });
      }
      throw err;
    }
  });

  /** Refunds recorded against a captured payment (what was refunded, and what is still refundable). */
  app.get<{ Params: { paymentId: string } }>('/api/payments/:paymentId/refunds', async (request, reply) => {
    const refunds = await refundsOfPayment(request.params.paymentId);
    return reply.send({
      paymentId: request.params.paymentId,
      refunds: refunds.map((r) => ({
        refundId: r.refundId,
        status: r.status,
        amount: r.amount,
        amountHalalas: r.amountHalalas,
        currency: r.currency,
        reason: r.reason ?? null,
        eventId: r.lastEventId ?? null,
        recordedAt: r._createdAt,
      })),
      refundedTotalHalalas: refunds.filter((r) => r.status !== 'FAILED').reduce((s, r) => s + r.amountHalalas, 0),
    });
  });

  return app;
}

async function start(): Promise<void> {
  const app = buildHyperPayServer();
  await app.listen({ port: config.port, host: '0.0.0.0' });
  // Outbox publisher: moves committed PAYMENT_COMPLETED events to the `hyperpay` exchange.
  const messaging = await startHyperPayMessaging({ connect: resolveAmqpConnect(brokerDriverFromEnv()) });
  for (const signal of ['SIGTERM', 'SIGINT'] as const) {
    process.once(signal, () => {
      void app
        .close()
        .then(() => messaging.stop())
        .then(() => process.exit(0));
    });
  }
  console.log(`hyperpay-gateway listening on port ${config.port}`);
}

if (require.main === module) {
  start().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
