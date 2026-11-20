/**
 * Standalone invoice-PDF worker for multi-process deployments (real RabbitMQ):
 * the file-cache engine consumes `zatca.invoice.reported` and caches each
 * invoice's PDF, rendered with the ZATCA renderer (from the event alone).
 *
 *   PLATFORM_BROKER_DRIVER=amqp RABBITMQ_URL=amqp://… FILE_CACHE_DIR=./cache npm run worker:invoice-pdfs
 */
import { createFileCache } from '../services/file-cache-engine/src/main';
import { startInvoicePdfWorker } from '../services/file-cache-engine/src/integration/invoice-pdf-worker';
import { renderReportedInvoicePdf } from '../services/zatca-tax/src/integration/invoicePdf';
import { brokerDriverFromEnv, defaultTtlPumpIntervalMs, platformLogFromEnv, resolveAmqpConnect } from '../shared/messaging';

async function main(): Promise<void> {
  const fileCache = await createFileCache({ directory: process.env.FILE_CACHE_DIR ?? './.file-cache' });
  const worker = await startInvoicePdfWorker({
    cache: fileCache.cache,
    render: renderReportedInvoicePdf,
    url: process.env.RABBITMQ_URL ?? 'amqp://localhost:5672',
    connect: resolveAmqpConnect(brokerDriverFromEnv()),
    ttlPumpIntervalMs: defaultTtlPumpIntervalMs(brokerDriverFromEnv()),
    log: platformLogFromEnv(),
  });
  console.log(`invoice PDF worker consuming ${worker.consumer.queues.workQueue}`);
  for (const signal of ['SIGTERM', 'SIGINT'] as const) {
    process.once(signal, () => {
      void worker
        .stop()
        .then(() => fileCache.close())
        .then(() => process.exit(0));
    });
  }
}

main().catch((err) => {
  console.error('invoice PDF worker failed:', err);
  process.exit(1);
});
