import { FastifyInstance } from 'fastify';
import { billingWebhookService } from '../services/BillingWebhookService';
import { StripeWebhookSchema } from '../types/credits';

export async function webhooksRoutes(app: FastifyInstance): Promise<void> {
  app.post('/webhooks/stripe', async (request, reply) => {
    const input = StripeWebhookSchema.parse(request.body);
    const result = await billingWebhookService.handleStripeWebhook(input);
    return reply.status(200).send(result);
  });
}
