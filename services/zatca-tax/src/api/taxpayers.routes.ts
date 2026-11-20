import { FastifyInstance } from 'fastify';
import { z } from 'zod';
import { InvoiceService } from '../services/InvoiceService';

const createTaxpayerSchema = z.object({
  id: z.string().min(1).optional(),
  legalNameAr: z.string().min(2),
  legalNameEn: z.string().min(2),
  vatNumber: z.string().length(15),
  crnNumber: z.string().min(5),
  egsSerialNumber: z.string().min(3).optional(),
  businessCategory: z.string().min(2).optional(),
  streetName: z.string().min(2),
  buildingNumber: z.string().min(1),
  districtName: z.string().min(2).optional(),
  city: z.string().min(2),
  postalCode: z.string().min(4),
});

const registerEgsSchema = z.object({
  egsUnitId: z.string().min(3),
  serialNumber: z.string().min(3),
  branchName: z.string().min(2),
  city: z.string().min(2),
  modelVersion: z.string().optional(),
});

export function registerTaxpayerRoutes(
  app: FastifyInstance,
  invoiceService: InvoiceService
): void {
  app.post('/api/v1/taxpayers', async (request, reply) => {
    const body = createTaxpayerSchema.parse(request.body);
    const created = await invoiceService.createTaxpayer(body);
    return reply.status(201).send(created);
  });

  /** Verify the taxpayer's ICV / PIH hash chain end to end (gaps, forks, tampering, stamps). */
  app.get<{ Params: { id: string } }>('/api/v1/taxpayers/:id/hash-chain', async (request) => {
    return invoiceService.verifyHashChain(request.params.id);
  });

  app.get('/api/v1/taxpayers', async () => {
    const list = await invoiceService.listTaxpayers();
    return { taxpayers: list };
  });

  app.get('/api/v1/taxpayers/:id', async (request) => {
    const { id } = request.params as { id: string };
    return invoiceService.getTaxpayer(id);
  });

  app.post('/api/v1/taxpayers/:id/egs-devices', async (request, reply) => {
    const { id } = request.params as { id: string };
    const body = registerEgsSchema.parse(request.body);
    const device = await invoiceService.registerEgsDevice({
      taxpayerId: id,
      ...body,
    });
    return reply.status(201).send(device);
  });

  app.get('/api/v1/taxpayers/:id/egs-devices', async (request) => {
    const { id } = request.params as { id: string };
    const devices = await invoiceService.listEgsDevices(id);
    return { devices };
  });

  app.get('/api/v1/taxpayers/:id/compliance-summary', async (request) => {
    const { id } = request.params as { id: string };
    return invoiceService.getTaxpayerComplianceSummary(id);
  });
}
