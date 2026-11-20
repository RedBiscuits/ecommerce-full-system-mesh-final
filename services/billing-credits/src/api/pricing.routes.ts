import { FastifyInstance } from 'fastify';
import { pricingCalculator } from '../services/PricingCalculator';
import { PriceQuoteSchema } from '../types/credits';

export async function pricingRoutes(app: FastifyInstance): Promise<void> {
  app.get('/credits/pricing/models', async (_request, reply) => {
    const models = pricingCalculator.listRateCards();
    return reply.status(200).send({ models });
  });

  app.post('/credits/pricing/quote', async (request, reply) => {
    const input = PriceQuoteSchema.parse(request.body);
    const quote = pricingCalculator.calculateQuote(input);
    return reply.status(200).send(quote);
  });
}
