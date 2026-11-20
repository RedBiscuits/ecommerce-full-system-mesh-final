import { FastifyInstance } from 'fastify';
import { z } from 'zod';
import { InvoiceService } from '../services/InvoiceService';
import { PdfInvoiceGenerator } from '../services/PdfInvoiceGenerator';
import { ReportingService } from '../services/ReportingService';
import { UblXmlBuilder } from '../services/UblXmlBuilder';
import { InvoiceStatus, InvoiceTypeCode } from '../types/zatca';

const vatExemptionEnum = z.enum([
  'VATEX-SA-29',
  'VATEX-SA-29-7',
  'VATEX-SA-30',
  'VATEX-SA-32',
  'VATEX-SA-33',
  'VATEX-SA-34-1',
  'VATEX-SA-EDU',
  'VATEX-SA-HEA',
  'VATEX-SA-OOS',
]);

const lineItemSchema = z.object({
  itemNameAr: z.string().min(1),
  itemNameEn: z.string().min(1),
  sku: z.string().min(1),
  quantity: z.number().int().positive(),
  unitPriceHalalas: z.number().int().nonnegative(),
  discountHalalas: z.number().int().nonnegative().optional(),
  vatCategory: z.enum(['S', 'Z', 'E', 'O']).optional(),
  vatExemptionCode: vatExemptionEnum.optional(),
  vatExemptionReason: z.string().optional(),
});

const createInvoiceSchema = z.object({
  taxpayerId: z.string().min(1),
  egsDeviceId: z.string().optional(),
  invoiceNumber: z.string().min(1).optional(),
  invoiceTypeCode: z.enum(['388', '381', '383']).optional(),
  invoiceSubtype: z.enum(['0200000', '0100000']).optional(),
  paymentMeansCode: z.enum(['10', '30', '42', '48', '97']).optional(),
  customerName: z.string().min(1),
  customerVatNumber: z.string().optional(),
  customerPhone: z.string().optional(),
  customerEmail: z.string().email().optional(),
  documentAllowanceHalalas: z.number().int().nonnegative().optional(),
  documentAllowanceReason: z.string().min(1).max(200).optional(),
  supplyDate: z.string().optional(),
  lines: z.array(lineItemSchema).min(1),
});

const updateInvoiceSchema = z.object({
  customerName: z.string().min(1).optional(),
  customerPhone: z.string().optional(),
  customerEmail: z.string().email().optional(),
  paymentMeansCode: z.enum(['10', '30', '42', '48', '97']).optional(),
  documentAllowanceHalalas: z.number().int().nonnegative().optional(),
  lines: z.array(lineItemSchema).min(1).optional(),
});

const createAdjustmentNoteSchema = z.object({
  instructionNote: z.string().min(3),
  paymentMeansCode: z.enum(['10', '30', '42', '48', '97']).optional(),
  lines: z.array(lineItemSchema).min(1),
});

export function registerInvoiceRoutes(
  app: FastifyInstance,
  invoiceService: InvoiceService,
  pdfGenerator: PdfInvoiceGenerator,
  reportingService: ReportingService
): void {
  app.post('/api/v1/invoices', async (request, reply) => {
    const body = createInvoiceSchema.parse(request.body);
    const created = await invoiceService.createDraftInvoice(body);
    return reply.status(201).send(created);
  });

  app.get('/api/v1/invoices', async (request) => {
    const query = request.query as {
      taxpayerId?: string;
      status?: InvoiceStatus;
      invoiceTypeCode?: InvoiceTypeCode;
    };
    const invoices = await invoiceService.listInvoices(query);
    return { invoices };
  });

  app.get('/api/v1/invoices/:id', async (request) => {
    const { id } = request.params as { id: string };
    return invoiceService.getInvoice(id);
  });

  app.patch('/api/v1/invoices/:id', async (request) => {
    const { id } = request.params as { id: string };
    const body = updateInvoiceSchema.parse(request.body);
    return invoiceService.updateDraftInvoice(id, body);
  });

  app.delete('/api/v1/invoices/:id', async (request, reply) => {
    const { id } = request.params as { id: string };
    await invoiceService.deleteDraftInvoice(id);
    return reply.status(204).send();
  });

  app.post('/api/v1/invoices/:id/issue', async (request) => {
    const { id } = request.params as { id: string };
    return invoiceService.issueInvoice(id);
  });

  app.post('/api/v1/invoices/:id/credit-notes', async (request, reply) => {
    const { id } = request.params as { id: string };
    const body = createAdjustmentNoteSchema.parse(request.body);
    const creditNote = await invoiceService.createCreditNote(id, body);
    return reply.status(201).send(creditNote);
  });

  app.post('/api/v1/invoices/:id/debit-notes', async (request, reply) => {
    const { id } = request.params as { id: string };
    const body = createAdjustmentNoteSchema.parse(request.body);
    const debitNote = await invoiceService.createDebitNote(id, body);
    return reply.status(201).send(debitNote);
  });

  app.get('/api/v1/invoices/:id/xml', async (request, reply) => {
    const { id } = request.params as { id: string };
    const invoice = await invoiceService.getInvoice(id);
    const taxpayer = await invoiceService.getTaxpayer(invoice.taxpayer_id);

    const xml = invoice.xml_ubl_base64
      ? Buffer.from(invoice.xml_ubl_base64, 'base64').toString('utf8')
      : UblXmlBuilder.buildUblInvoiceXml(invoice, taxpayer);

    reply.header('Content-Type', 'application/xml; charset=utf-8').send(xml);
  });

  app.get('/api/v1/invoices/:id/pdf', async (request, reply) => {
    const { id } = request.params as { id: string };
    const invoice = await invoiceService.getInvoice(id);
    const taxpayer = await invoiceService.getTaxpayer(invoice.taxpayer_id);
    const artifact = await pdfGenerator.generateAndStorePdf(invoice, taxpayer);

    reply
      .header('Content-Type', 'application/pdf')
      .header('Content-Disposition', `inline; filename="${invoice.invoice_number}.pdf"`)
      .header('X-Content-SHA256', artifact.contentSha256)
      .send(artifact.buffer);
  });

  app.post('/api/v1/invoices/:id/report', async (request) => {
    const { id } = request.params as { id: string };
    return reportingService.reportSimplifiedInvoice(id);
  });

  /** Standard (B2B) invoices: submit to the Fatoora clearance API. */
  app.post('/api/v1/invoices/:id/clearance', async (request) => {
    const { id } = request.params as { id: string };
    return reportingService.clearStandardInvoice(id);
  });

  app.get('/api/v1/invoices/:id/submissions', async (request) => {
    const { id } = request.params as { id: string };
    const submissions = await reportingService.listSubmissionLogs(id);
    return { submissions };
  });
}
