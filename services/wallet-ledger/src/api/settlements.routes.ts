import { FastifyInstance, FastifyReply } from 'fastify';
import { z, ZodError } from 'zod';
import { MerchantSettlementService, SettlementRequestError, merchantSettlementService } from '../services/MerchantSettlementService';

const timestamp = z.string().min(1).max(64);

const OpenBatchSchema = z.object({
  /** Start of the window (inclusive); omit or null = since the beginning of the system. */
  from: timestamp.nullable().optional(),
  /** End of the window (exclusive); default now. */
  to: timestamp.optional(),
  idempotencyKey: z.string().min(1).max(160).optional(),
  maxItems: z.number().int().positive().max(10_000_000).optional(),
  /** Sweep inside the request (201 with the collected batch) instead of in the background (202). */
  wait: z.boolean().optional(),
});

function sendError(reply: FastifyReply, err: unknown): FastifyReply | undefined {
  if (err instanceof ZodError) return reply.status(400).send({ error: 'Validation failed', details: err.errors });
  if (err instanceof SettlementRequestError) {
    return reply.status(err.statusCode).send({ error: err.message, code: err.code, ...(err.batch ? { batch: err.batch } : {}) });
  }
  return undefined;
}

/**
 * Merchant settlement batches (finance operations; admin role).
 *
 *   POST /settlements/merchants/:merchantId/batches   open a batch and sweep the merchant's settled revenue
 *   GET  /settlements/merchants/:merchantId/batches   the merchant's batches, newest first
 *   GET  /settlements/batches/:batchId                a batch, its settlement journal and its checkpoints
 *   GET  /settlements/batches/:batchId/items          the entries it settled (keyset pages)
 *   POST /settlements/maintenance                     resume sweeps left COLLECTING (crash / abort)
 */
export function settlementRoutes(service: MerchantSettlementService = merchantSettlementService) {
  return async (app: FastifyInstance): Promise<void> => {
    app.post<{ Params: { merchantId: string } }>(
      '/settlements/merchants/:merchantId/batches',
      { config: { access: 'admin' } },
      async (request, reply) => {
        try {
          const input = OpenBatchSchema.parse(request.body ?? {});
          const headerKey = request.headers['idempotency-key'];
          const { batch, created } = await service.openBatch({
            merchantId: request.params.merchantId,
            from: input.from ?? null,
            to: input.to,
            idempotencyKey: input.idempotencyKey ?? (typeof headerKey === 'string' ? headerKey : undefined),
            maxItems: input.maxItems,
          });
          if (batch.status !== 'COLLECTING') return reply.status(200).send(batch);
          if (input.wait) return reply.status(created ? 201 : 200).send(await service.collect(batch.batchId));
          // Sweep in the background (chunked, yielding between chunks); poll GET /settlements/batches/:id.
          void service.collect(batch.batchId).catch((err: unknown) => {
            request.log.error({ err, batchId: batch.batchId }, 'settlement sweep failed; the batch stays COLLECTING (resume via /settlements/maintenance)');
          });
          return reply.status(202).send(batch);
        } catch (err) {
          const sent = sendError(reply, err);
          if (sent) return sent;
          throw err;
        }
      }
    );

    app.get<{ Params: { merchantId: string }; Querystring: { limit?: string } }>(
      '/settlements/merchants/:merchantId/batches',
      { config: { access: 'admin' } },
      async (request, reply) => {
        const limit = request.query.limit ? Number(request.query.limit) : 50;
        return reply.send({ batches: await service.listBatches(request.params.merchantId, Number.isFinite(limit) ? limit : 50) });
      }
    );

    app.get<{ Params: { batchId: string } }>('/settlements/batches/:batchId', { config: { access: 'admin' } }, async (request, reply) => {
      const trace = await service.traceBatch(request.params.batchId);
      if (!trace.batch) return reply.status(404).send({ error: 'Settlement batch not found', code: 'BATCH_NOT_FOUND' });
      return reply.send(trace);
    });

    app.get<{ Params: { batchId: string }; Querystring: { after?: string; limit?: string } }>(
      '/settlements/batches/:batchId/items',
      { config: { access: 'admin' } },
      async (request, reply) => {
        const limit = request.query.limit ? Number(request.query.limit) : undefined;
        return reply.send(await service.listBatchItems(request.params.batchId, { after: request.query.after, limit }));
      }
    );

    app.post('/settlements/maintenance', { config: { access: 'admin' } }, async (_request, reply) => {
      const resumed = await service.resumeCollecting();
      return reply.send({ resumed: resumed.map((b) => ({ batchId: b.batchId, status: b.status })) });
    });
  };
}
