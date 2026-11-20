import Fastify, { FastifyInstance } from 'fastify';
import { getDb, resetDatabaseAndSeed } from './db';
import { config } from './config/env';
import { registerErrorHandler } from './middleware/errorHandler';
import { InvoiceService } from './services/InvoiceService';
import { PdfInvoiceGenerator } from './services/PdfInvoiceGenerator';
import { ReportingService } from './services/ReportingService';
import { registerTaxpayerRoutes } from './api/taxpayers.routes';
import { registerInvoiceRoutes } from './api/invoices.routes';
import { OrderInvoicingService } from './integration/OrderInvoicingService';
import { SettlementInvoicingService } from './integration/SettlementInvoicingService';
import { RefundInvoicingService } from './integration/RefundInvoicingService';
import { startZatcaIntegration } from './integration/runtime';
import { brokerDriverFromEnv, defaultTtlPumpIntervalMs, platformLogFromEnv, resolveAmqpConnect } from '../../../shared/messaging';

export interface AppContext {
  app: FastifyInstance;
  invoiceService: InvoiceService;
  pdfGenerator: PdfInvoiceGenerator;
  reportingService: ReportingService;
  /** The order flow's invoicing (driven by ledger settlement events; see integration/). */
  orderInvoicingService: OrderInvoicingService;
  /** Merchant settlement documents (driven by ledger settlement-posted events). */
  settlementInvoicingService: SettlementInvoicingService;
  /** Customer refunds: credit notes (driven by ledger refund events). */
  refundInvoicingService: RefundInvoicingService;
}

export async function buildApp(): Promise<AppContext> {
  const app = Fastify({ logger: false });
  const db = await getDb();

  const invoiceService = new InvoiceService(db);
  const pdfGenerator = new PdfInvoiceGenerator(db);
  const reportingService = new ReportingService(db, invoiceService);
  const orderInvoicingService = new OrderInvoicingService(db, invoiceService, reportingService);
  const settlementInvoicingService = new SettlementInvoicingService(db, invoiceService, reportingService);
  const refundInvoicingService = new RefundInvoicingService(db, invoiceService, reportingService);

  app.setErrorHandler(registerErrorHandler);

  app.get('/health', async () => ({
    status: 'ok',
    service: 'zatca-tax-service',
    timestamp: new Date().toISOString(),
  }));

  registerTaxpayerRoutes(app, invoiceService);
  registerInvoiceRoutes(app, invoiceService, pdfGenerator, reportingService);

  /** Invoices the order flow issued for an order, with the consumer checkpoints. */
  app.get<{ Params: { orderId: string } }>('/api/v1/orders/:orderId/invoicing', async (request) => {
    return orderInvoicingService.getOrderInvoicing(request.params.orderId);
  });

  /** The fee invoice and payout credit note issued for a merchant settlement batch, with the checkpoints. */
  app.get<{ Params: { batchId: string } }>('/api/v1/settlements/:batchId/documents', async (request) => {
    return settlementInvoicingService.getSettlementDocuments(request.params.batchId);
  });

  /** Credit notes issued for an order's refunds, with the checkpoints. */
  app.get<{ Params: { orderId: string } }>('/api/v1/orders/:orderId/credit-notes', async (request) => {
    return refundInvoicingService.getOrderCreditNotes(request.params.orderId);
  });

  return {
    app,
    invoiceService,
    pdfGenerator,
    reportingService,
    orderInvoicingService,
    settlementInvoicingService,
    refundInvoicingService,
  };
}

if (require.main === module) {
  (async () => {
    await resetDatabaseAndSeed();
    const { app, invoiceService, reportingService } = await buildApp();
    // Platform order flow: consume ledger settlements, publish reported invoices.
    if (process.env.INTEGRATION_ENABLED !== 'false') {
      const driver = brokerDriverFromEnv();
      const integration = await startZatcaIntegration({
        url: process.env.RABBITMQ_URL || 'amqp://localhost:5672',
        connect: resolveAmqpConnect(driver),
        ttlPumpIntervalMs: defaultTtlPumpIntervalMs(driver),
        context: { invoiceService, reportingService },
        log: platformLogFromEnv(),
      });
      app.addHook('onClose', () => integration.stop());
    }
    await app.listen({ port: config.port, host: '0.0.0.0' });
    for (const signal of ['SIGTERM', 'SIGINT'] as const) {
      process.once(signal, () => void app.close().then(() => process.exit(0)));
    }
    console.log(`zatca-tax-service listening on port ${config.port}`);
  })().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
