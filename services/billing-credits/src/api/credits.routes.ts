import { FastifyInstance } from 'fastify';
import { creditLedgerService } from '../services/CreditLedgerService';
import { autoRechargeService } from '../services/AutoRechargeService';
import {
  ConfigureAutoRechargeSchema,
  CreateGrantSchema,
  DailyUsageQuerySchema,
  DeductUsageSchema,
  SetParentOrgSchema,
} from '../types/credits';
import { orgPoolService } from '../services/OrgPoolService';

export async function creditsRoutes(app: FastifyInstance): Promise<void> {
  app.get<{ Params: { orgId: string } }>(
    '/credits/organizations/:orgId/balance',
    async (request, reply) => {
      const summary = await creditLedgerService.getBalanceSummary(request.params.orgId);
      return reply.status(200).send(summary);
    }
  );

  app.get<{ Params: { orgId: string }; Querystring: { limit?: string } }>(
    '/credits/organizations/:orgId/history',
    async (request, reply) => {
      const limit = request.query.limit ? parseInt(request.query.limit, 10) : 20;
      const items = await creditLedgerService.listUsageHistory(
        request.params.orgId,
        Number.isNaN(limit) ? 20 : limit
      );
      return reply.status(200).send({
        orgId: request.params.orgId,
        count: items.length,
        items,
      });
    }
  );

  app.get<{ Params: { orgId: string }; Querystring: { from?: string; to?: string } }>(
    '/credits/organizations/:orgId/usage/daily',
    async (request, reply) => {
      const { from, to } = DailyUsageQuerySchema.parse(request.query ?? {});
      const report = await creditLedgerService.getDailyUsage(request.params.orgId, from, to);
      return reply.status(200).send(report);
    }
  );

  app.put<{ Params: { orgId: string } }>(
    '/credits/organizations/:orgId/parent',
    async (request, reply) => {
      const { parentOrgId } = SetParentOrgSchema.parse(request.body);
      const updated = await orgPoolService.setParent(request.params.orgId, parentOrgId);
      return reply.status(200).send(updated);
    }
  );

  app.put<{ Params: { orgId: string } }>(
    '/credits/organizations/:orgId/auto-recharge',
    async (request, reply) => {
      const input = ConfigureAutoRechargeSchema.parse(request.body);
      const updated = await autoRechargeService.configureAutoRecharge(
        request.params.orgId,
        input
      );
      return reply.status(200).send(updated);
    }
  );

  app.post('/credits/grants', async (request, reply) => {
    const input = CreateGrantSchema.parse(request.body);
    const grant = await creditLedgerService.createGrant(input);
    return reply.status(201).send(grant);
  });

  app.post('/credits/deduct', async (request, reply) => {
    const headerKey = request.headers['idempotency-key'] as string | undefined;
    const bodyObj = (request.body as Record<string, unknown>) || {};
    const input = DeductUsageSchema.parse({
      ...bodyObj,
      idempotencyKey: bodyObj.idempotencyKey ?? headerKey,
    });

    const result = await creditLedgerService.deductUsage(input);
    if (result.idempotentReplay) {
      reply.header('Idempotent-Replayed', 'true');
    }
    return reply.status(200).send(result);
  });
}
