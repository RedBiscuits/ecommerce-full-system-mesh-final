import { FastifyInstance } from 'fastify';
import { z } from 'zod';
import { settlementFeeService } from '../integration/SettlementFeeService';
import { FeeTierSchema } from '../services/SettlementFeeCalculator';
import { NonRetryableError } from '../../../../shared/messaging';

const QuoteSchema = z.object({
  merchantId: z.string().min(1).max(64),
  grossHalalas: z.number().int().positive().safe(),
  itemCount: z.number().int().positive().safe(),
});

const RuleSchema = z.object({
  ruleId: z.string().min(1).max(64),
  merchantId: z.string().min(1).max(64).nullable(),
  tiers: z.array(FeeTierSchema).min(1).max(20),
  perTransactionHalalas: z.number().int().nonnegative().safe().default(0),
  minimumFeeHalalas: z.number().int().nonnegative().safe().default(0),
});

/** Merchant settlement processing fees (tiered pricing). */
export async function settlementFeesRoutes(app: FastifyInstance): Promise<void> {
  app.get('/billing/settlement-fees/rules', async (_request, reply) => {
    return reply.send({ rules: await settlementFeeService.listRules() });
  });

  /** Publish a new rule version (the merchant's, or the default with merchantId null). */
  app.post('/billing/settlement-fees/rules', async (request, reply) => {
    const input = RuleSchema.parse(request.body);
    return reply.status(201).send(await settlementFeeService.publishRule(input));
  });

  /** Preview: what a settlement of this gross / transaction count would be charged now. */
  app.post('/billing/settlement-fees/quote', async (request, reply) => {
    const input = QuoteSchema.parse(request.body);
    try {
      const quote = await settlementFeeService.quote(input.merchantId, input.grossHalalas, input.itemCount);
      return reply.send({ merchantId: input.merchantId, grossHalalas: input.grossHalalas, itemCount: input.itemCount, ...quote });
    } catch (err) {
      if (err instanceof NonRetryableError) return reply.status(422).send({ error: err.message, code: err.reason });
      throw err;
    }
  });

  /** The fee frozen for a settlement batch, with billing's checkpoints. */
  app.get<{ Params: { batchId: string } }>('/billing/settlement-fees/batches/:batchId', async (request, reply) => {
    const result = await settlementFeeService.getBatchFee(request.params.batchId);
    if (!result.quote) return reply.status(404).send({ error: 'Batch not priced', code: 'NOT_FOUND', checkpoints: result.checkpoints });
    return reply.send(result);
  });
}
