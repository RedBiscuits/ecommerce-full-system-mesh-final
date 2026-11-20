import { FastifyInstance } from 'fastify';
import { streamHoldService } from '../services/StreamHoldService';
import {
  CreateStreamHoldSchema,
  ExtendStreamHoldSchema,
  ReleaseStreamHoldSchema,
  SettleStreamHoldSchema,
} from '../types/credits';

export async function holdsRoutes(app: FastifyInstance): Promise<void> {
  app.post('/credits/holds', async (request, reply) => {
    const input = CreateStreamHoldSchema.parse(request.body);
    const hold = await streamHoldService.createStreamHold(input);
    return reply.status(201).send(hold);
  });

  app.post('/credits/holds/extend', async (request, reply) => {
    const input = ExtendStreamHoldSchema.parse(request.body);
    const extended = await streamHoldService.extendStreamHold(input);
    return reply.status(200).send(extended);
  });

  app.post('/credits/holds/release', async (request, reply) => {
    const input = ReleaseStreamHoldSchema.parse(request.body);
    const released = await streamHoldService.releaseStreamHold(input);
    return reply.status(200).send(released);
  });

  app.post('/credits/holds/settle', async (request, reply) => {
    const input = SettleStreamHoldSchema.parse(request.body);
    const settled = await streamHoldService.settleStreamHold(input);
    return reply.status(200).send(settled);
  });
}
