/**
 * Customer refunds in the file cache: consumes `zatca.credit-note.reported` (queue
 * `file-cache.credit-notes`) and
 *   1. caches the credit note's PDF (rendered from the event's snapshot) and UBL XML under
 *      `zatca/invoices/<taxpayer>/<creditNote>.pdf|xml`;
 *   2. invalidates the stale PDF of the credited invoice: the entry is replaced by a fresh render
 *      that lists every credit note applied and the amount still standing (the `x-credited-halalas`
 *      attribute says how much). A reader therefore never gets the pre-refund PDF again.
 *
 * Idempotent: a redelivery finds the credit note already cached for the same hash and the invoice
 * PDF already rendered for the same credited total, and changes nothing.
 */
import { createHash } from 'node:crypto';

import type { CacheManager } from '../cache/cache-manager';
import {
  CONSUMERS,
  CONSUMER_BINDINGS,
  CreditNoteReportedSchema,
  type CreditNoteReportedEvent,
  type InvoiceDocument,
} from '../../../../shared/contracts';
import {
  ReliableConsumer,
  type AmqpConnect,
  type LogFn,
  type ReconnectPolicy,
  type RetryPolicy,
} from '../../../../shared/messaging';
import { EventAuthenticator, SERVICES, deliveryVerifier, eventAuthFor } from '../../../../shared/security';
import { invoicePdfKey } from './invoice-pdf-worker';
import { invoiceXmlKey } from './settlement-statements';

export const CREDIT_NOTE_CONSUMER = CONSUMERS.fileCacheCreditNotes;

export type CreditNoteRenderer = (
  document: InvoiceDocument,
  options?: { creditNotes?: Array<{ invoiceNumber: string; icv: number; totalHalalas: number; refundId?: string | null }> }
) => Promise<Uint8Array>;

export type CreditNoteOutcome = 'CACHED_AND_INVALIDATED' | 'ALREADY_CACHED';

const ONE_YEAR_MS = 365 * 24 * 60 * 60 * 1000;
const sha256 = (bytes: Uint8Array): string => createHash('sha256').update(bytes).digest('hex');

export class CreditNoteCacheWorker {
  constructor(
    private readonly cache: CacheManager,
    private readonly render: CreditNoteRenderer,
    private readonly ttlMs: number = ONE_YEAR_MS
  ) {}

  async handle(event: CreditNoteReportedEvent): Promise<CreditNoteOutcome> {
    const p = event.payload;
    const note = p.creditNote;
    let changed = false;

    // 1. the credit note itself (PDF + signed XML), once per hash
    const pdfKey = invoicePdfKey(note.taxpayerId, note.invoiceId);
    const pdfMeta = await this.cache.head(pdfKey);
    if (pdfMeta === undefined || pdfMeta.freshness !== 'fresh' || pdfMeta.attributes['x-invoice-hash'] !== note.invoiceHash) {
      const pdf = await this.render(note);
      if (pdf.byteLength === 0) throw new Error(`Renderer produced an empty PDF for credit note ${note.invoiceId}`);
      await this.cache.put(pdfKey, pdf, {
        ttlMs: this.ttlMs,
        staleWhileRevalidateMs: 0,
        attributes: {
          'content-type': 'application/pdf',
          'x-content-sha256': sha256(pdf),
          'x-invoice-id': note.invoiceId,
          'x-invoice-number': note.invoiceNumber,
          'x-invoice-hash': note.invoiceHash,
          'x-icv': String(note.icv),
          'x-order-id': p.refund.orderId,
          'x-refund-id': p.refund.refundId,
          'x-billing-reference': p.originalInvoiceId,
          'x-event-id': event.eventId,
        },
      });
      changed = true;
    }
    const xmlKey = invoiceXmlKey(note.taxpayerId, note.invoiceId);
    const xmlMeta = await this.cache.head(xmlKey);
    if (xmlMeta === undefined || xmlMeta.freshness !== 'fresh' || xmlMeta.attributes['x-invoice-hash'] !== note.invoiceHash) {
      const xml = Buffer.from(note.xmlUblBase64, 'base64');
      await this.cache.put(xmlKey, xml, {
        ttlMs: this.ttlMs,
        staleWhileRevalidateMs: 0,
        attributes: {
          'content-type': 'application/xml; charset=utf-8',
          'x-content-sha256': sha256(xml),
          'x-invoice-id': note.invoiceId,
          'x-invoice-hash': note.invoiceHash,
          'x-icv': String(note.icv),
          'x-refund-id': p.refund.refundId,
        },
      });
      changed = true;
    }

    // 2. the credited invoice: its cached PDF is stale now; replace it with one that shows the credits.
    const invoiceKey = invoicePdfKey(p.invoice.taxpayerId, p.invoice.invoiceId);
    const current = await this.cache.head(invoiceKey);
    const creditedNow = String(p.creditedTotalHalalas);
    const alreadyCurrent =
      current !== undefined &&
      current.freshness === 'fresh' &&
      current.attributes['x-invoice-hash'] === p.invoice.invoiceHash &&
      current.attributes['x-credited-halalas'] === creditedNow &&
      Number(current.attributes['x-credit-notes'] ?? 0) >= p.creditNotes.length;
    if (!alreadyCurrent) {
      // Invalidate first: between the delete and the put nobody can be served the stale copy.
      await this.cache.delete(invoiceKey);
      const pdf = await this.render(p.invoice, { creditNotes: p.creditNotes });
      if (pdf.byteLength === 0) throw new Error(`Renderer produced an empty PDF for invoice ${p.invoice.invoiceId}`);
      await this.cache.put(invoiceKey, pdf, {
        ttlMs: this.ttlMs,
        staleWhileRevalidateMs: 0,
        attributes: {
          'content-type': 'application/pdf',
          'x-content-sha256': sha256(pdf),
          'x-invoice-id': p.invoice.invoiceId,
          'x-invoice-number': p.invoice.invoiceNumber,
          'x-invoice-hash': p.invoice.invoiceHash,
          'x-icv': String(p.invoice.icv),
          'x-order-id': p.refund.orderId,
          'x-credited-halalas': creditedNow,
          'x-credit-notes': String(p.creditNotes.length),
          'x-event-id': event.eventId,
        },
      });
      changed = true;
    }
    return changed ? 'CACHED_AND_INVALIDATED' : 'ALREADY_CACHED';
  }
}

export interface CreditNoteWorkerOptions {
  cache: CacheManager;
  render: CreditNoteRenderer;
  url: string;
  connect: AmqpConnect;
  ttlMs?: number;
  retry?: Partial<RetryPolicy>;
  prefetch?: number;
  ttlPumpIntervalMs?: number;
  reconnect?: Partial<ReconnectPolicy>;
  auth?: EventAuthenticator;
  log?: LogFn;
}

export interface CreditNoteWorkerRuntime {
  worker: CreditNoteCacheWorker;
  consumer: ReliableConsumer<CreditNoteReportedEvent>;
  stop(): Promise<void>;
}

export async function startCreditNoteWorker(options: CreditNoteWorkerOptions): Promise<CreditNoteWorkerRuntime> {
  const worker = new CreditNoteCacheWorker(options.cache, options.render, options.ttlMs);
  const consumer = new ReliableConsumer<CreditNoteReportedEvent>({
    name: CREDIT_NOTE_CONSUMER,
    bindings: CONSUMER_BINDINGS[CONSUMERS.fileCacheCreditNotes],
    verify: deliveryVerifier(options.auth ?? eventAuthFor(SERVICES.fileCache)),
    decode: (raw) => CreditNoteReportedSchema.parse(raw) as CreditNoteReportedEvent,
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
  return { worker, consumer, stop: () => consumer.close() };
}
