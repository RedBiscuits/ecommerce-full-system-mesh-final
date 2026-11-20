/**
 * Invoice PDF rendering + caching, driven by the platform's order flow.
 *
 * Consumes `zatca.invoice.reported` (queue `file-cache.invoice-pdfs`) and keeps
 * the rendered PDF of every reported invoice in the file cache under a
 * deterministic key, with its SHA-256, the invoice hash and the ICV as entry
 * attributes.
 *
 * Idempotent by construction: the cache entry is the checkpoint. A redelivered
 * or replayed event finds a fresh entry for the same invoice hash and is
 * acknowledged without rendering again; a missing or corrupt entry (evicted,
 * swept, disk lost) is simply rendered again, which is always safe for a
 * derived artifact. A failed render or a failed disk write throws, so the
 * message is retried with jittered backoff and finally dead-lettered.
 */
import { createHash } from 'node:crypto';

import type { CacheManager } from '../cache/cache-manager';
import type { EntryMetadata } from '../cache/types';
import {
  CONSUMERS,
  CONSUMER_BINDINGS,
  InvoiceReportedSchema,
  type InvoiceReportedEvent,
  type InvoiceReportedPayload,
} from '../../../../shared/contracts';
import { EventAuthenticator, SERVICES, deliveryVerifier, eventAuthFor } from '../../../../shared/security';
import {
  ReliableConsumer,
  type AmqpConnect,
  type LogFn,
  type ReconnectPolicy,
  type RetryPolicy,
} from '../../../../shared/messaging';

export const INVOICE_PDF_CONSUMER = CONSUMERS.fileCacheInvoicePdfs;

/** Renders the PDF of a reported invoice from the event's document snapshot. */
export type InvoicePdfRenderer = (invoice: InvoiceReportedPayload) => Promise<Uint8Array>;

export type InvoicePdfOutcome = 'CACHED' | 'ALREADY_CACHED';

export interface CachedInvoicePdf {
  key: string;
  pdf: Buffer;
  metadata: EntryMetadata;
}

/** Cache key of an invoice PDF (hashed by the store; any string is safe). */
export function invoicePdfKey(taxpayerId: string, invoiceId: string): string {
  return `zatca/invoices/${taxpayerId}/${invoiceId}.pdf`;
}

const ONE_YEAR_MS = 365 * 24 * 60 * 60 * 1000;

export class InvoicePdfCacheWorker {
  constructor(
    private readonly cache: CacheManager,
    private readonly render: InvoicePdfRenderer,
    private readonly ttlMs: number = ONE_YEAR_MS,
  ) {}

  async handle(event: InvoiceReportedEvent): Promise<InvoicePdfOutcome> {
    const invoice = event.payload;
    const key = invoicePdfKey(invoice.taxpayerId, invoice.invoiceId);

    const existing = await this.cache.head(key);
    if (
      existing !== undefined &&
      existing.freshness === 'fresh' &&
      existing.attributes['x-invoice-hash'] === invoice.invoiceHash
    ) {
      return 'ALREADY_CACHED';
    }

    const pdf = await this.render(invoice);
    if (pdf.byteLength === 0) {
      throw new Error(`Renderer produced an empty PDF for invoice ${invoice.invoiceId}`);
    }
    await this.cache.put(key, pdf, {
      ttlMs: this.ttlMs,
      staleWhileRevalidateMs: 0,
      attributes: {
        'content-type': 'application/pdf',
        'x-content-sha256': createHash('sha256').update(pdf).digest('hex'),
        'x-invoice-id': invoice.invoiceId,
        'x-invoice-number': invoice.invoiceNumber,
        'x-invoice-hash': invoice.invoiceHash,
        'x-icv': String(invoice.icv),
        'x-order-id': invoice.orderId,
        'x-event-id': event.eventId,
      },
    });
    return 'CACHED';
  }

  /** The cached PDF of an invoice, if present and fresh. */
  async get(taxpayerId: string, invoiceId: string): Promise<CachedInvoicePdf | undefined> {
    const key = invoicePdfKey(taxpayerId, invoiceId);
    const hit = await this.cache.get(key);
    if (hit === undefined) {
      return undefined;
    }
    return { key, pdf: hit.value, metadata: hit.metadata };
  }
}

export interface InvoicePdfWorkerOptions {
  cache: CacheManager;
  render: InvoicePdfRenderer;
  url: string;
  connect: AmqpConnect;
  ttlMs?: number;
  retry?: Partial<RetryPolicy>;
  prefetch?: number;
  ttlPumpIntervalMs?: number;
  reconnect?: Partial<ReconnectPolicy>;
  /** Service-to-service event authentication; default: the file cache's own. */
  auth?: EventAuthenticator;
  log?: LogFn;
}

export interface InvoicePdfWorkerRuntime {
  worker: InvoicePdfCacheWorker;
  consumer: ReliableConsumer<InvoiceReportedEvent>;
  stop(): Promise<void>;
}

/** Consume `zatca.events / zatca.invoice.reported` and cache every reported invoice's PDF. */
export async function startInvoicePdfWorker(
  options: InvoicePdfWorkerOptions,
): Promise<InvoicePdfWorkerRuntime> {
  const worker = new InvoicePdfCacheWorker(
    options.cache,
    options.render,
    options.ttlMs,
  );
  const consumer = new ReliableConsumer<InvoiceReportedEvent>({
    name: INVOICE_PDF_CONSUMER,
    bindings: CONSUMER_BINDINGS[CONSUMERS.fileCacheInvoicePdfs],
    decode: (raw) => InvoiceReportedSchema.parse(raw) as InvoiceReportedEvent,
    verify: deliveryVerifier(options.auth ?? eventAuthFor(SERVICES.fileCache)),
    handler: (event) => worker.handle(event),
    url: options.url,
    connect: options.connect,
    prefetch: options.prefetch,
    retry: options.retry,
    ttlPumpIntervalMs: options.ttlPumpIntervalMs,
    reconnect: options.reconnect,
    log: options.log,
  });
  await consumer.start();
  return {
    worker,
    consumer,
    stop: () => consumer.close(),
  };
}
