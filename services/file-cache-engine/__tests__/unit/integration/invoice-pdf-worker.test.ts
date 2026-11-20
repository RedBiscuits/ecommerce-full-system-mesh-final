import { createHash } from 'node:crypto';

import type { Channel, ChannelModel } from 'amqplib';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { createFileCache, type FileCache } from '../../../src/main';
import {
  INVOICE_PDF_CONSUMER,
  InvoicePdfCacheWorker,
  invoicePdfKey,
  startInvoicePdfWorker,
  type InvoicePdfRenderer,
  type InvoicePdfWorkerRuntime,
} from '../../../src/integration/invoice-pdf-worker';
import {
  EVENT_TYPES,
  PRODUCERS,
  buildIntegrationEvent,
  type InvoiceReportedEvent,
} from '../../../../../shared/contracts';
import { resolveAmqpConnect, type DeliverySettled } from '../../../../../shared/messaging';
import { SERVICES, eventAuthFor, signedPublishProperties } from '../../../../../shared/security';
import { makeTempDir, type TempDir } from '../../helpers/temp-dir';

function reported(overrides: { invoiceId?: string; invoiceHash?: string; eventId?: string } = {}): InvoiceReportedEvent {
  const invoiceId = overrides.invoiceId ?? 'inv_1';
  return buildIntegrationEvent({
    eventId: overrides.eventId ?? `evt_${invoiceId}`,
    eventType: EVENT_TYPES.invoiceReported,
    producer: PRODUCERS.zatca,
    correlationId: 'ord_1',
    causationId: 'evt_ledger_1',
    aggregateId: invoiceId,
    payload: {
      invoiceId,
      invoiceNumber: 'INV-2026-0001',
      uuid: '7c6a2d3e-0000-4000-8000-000000000001',
      invoiceTypeCode: '388',
      invoiceSubtype: '0200000',
      taxpayerId: 'txp_riyadh_retail',
      orderId: 'ord_1',
      journalRef: 'cap_ph_1',
      icv: 7,
      invoiceHash: overrides.invoiceHash ?? 'hash-A',
      previousInvoiceHash: 'hash-prev',
      qrBase64: 'AQ==',
      status: 'REPORTED',
      issuedAt: '2026-11-15T10:00:00.000Z',
      reportedAt: '2026-11-15T10:00:01.000Z',
      customerName: 'Noura',
      totals: { subtotalHalalas: 20000, discountHalalas: 0, allowanceHalalas: 2000, taxableHalalas: 18000, vatHalalas: 2700, totalHalalas: 20700 },
      lines: [
        {
          lineNumber: 1,
          sku: 'RTR',
          nameEn: 'Router',
          nameAr: 'موجه',
          quantity: 2,
          unitPriceHalalas: 10000,
          discountHalalas: 0,
          netAmountHalalas: 20000,
          vatCategory: 'S',
          vatRateBps: 1500,
          vatAmountHalalas: 3000,
          lineTotalHalalas: 23000,
        },
      ],
      seller: {
        legalNameEn: 'Riyadh Orbit E-Commerce Co.',
        legalNameAr: 'شركة مدار الرياض',
        vatNumber: '310123456700003',
        crnNumber: '1010987654',
        buildingNumber: '4210',
        streetName: 'King Fahd Road',
        city: 'Riyadh',
        postalCode: '12214',
        countryCode: 'SA',
      },
    },
  }) as InvoiceReportedEvent;
}

const fakePdf = (tag: string): Buffer => Buffer.from(`%PDF-1.3 invoice ${tag}`);

describe('InvoicePdfCacheWorker (render + cache reported invoices)', () => {
  let dir: TempDir;
  let fileCache: FileCache;

  beforeEach(async () => {
    dir = await makeTempDir();
    fileCache = await createFileCache({ directory: dir.path, sweeper: false });
  });

  afterEach(async () => {
    await fileCache.close();
    await dir.cleanup();
  });

  it('renders once and stores the PDF with its integrity attributes', async () => {
    const render = vi.fn<InvoicePdfRenderer>(async (inv) => fakePdf(inv.invoiceHash));
    const worker = new InvoicePdfCacheWorker(fileCache.cache, render);
    expect(await worker.handle(reported())).toBe('CACHED');

    const cached = await worker.get('txp_riyadh_retail', 'inv_1');
    expect(cached?.key).toBe(invoicePdfKey('txp_riyadh_retail', 'inv_1'));
    expect(cached?.pdf.toString()).toBe('%PDF-1.3 invoice hash-A');
    expect(cached?.metadata.attributes).toEqual({
      'content-type': 'application/pdf',
      'x-content-sha256': createHash('sha256').update(fakePdf('hash-A')).digest('hex'),
      'x-invoice-id': 'inv_1',
      'x-invoice-number': 'INV-2026-0001',
      'x-invoice-hash': 'hash-A',
      'x-icv': '7',
      'x-order-id': 'ord_1',
      'x-event-id': 'evt_inv_1',
    });
    expect(render).toHaveBeenCalledTimes(1);
  });

  it('is idempotent: a redelivered event is acknowledged without rendering again', async () => {
    const render = vi.fn<InvoicePdfRenderer>(async () => fakePdf('x'));
    const worker = new InvoicePdfCacheWorker(fileCache.cache, render);
    await worker.handle(reported());
    expect(await worker.handle(reported())).toBe('ALREADY_CACHED');
    expect(await worker.handle(reported({ eventId: 'evt_other_delivery' }))).toBe('ALREADY_CACHED');
    expect(render).toHaveBeenCalledTimes(1);
  });

  it('re-renders when the cached entry is for another invoice hash, or was lost', async () => {
    const render = vi.fn<InvoicePdfRenderer>(async (inv) => fakePdf(inv.invoiceHash));
    const worker = new InvoicePdfCacheWorker(fileCache.cache, render);
    await worker.handle(reported());
    expect(await worker.handle(reported({ invoiceHash: 'hash-B' }))).toBe('CACHED');
    expect((await worker.get('txp_riyadh_retail', 'inv_1'))?.pdf.toString()).toBe('%PDF-1.3 invoice hash-B');
    await fileCache.cache.delete(invoicePdfKey('txp_riyadh_retail', 'inv_1'));
    expect(await worker.handle(reported({ invoiceHash: 'hash-B' }))).toBe('CACHED');
    expect(render).toHaveBeenCalledTimes(3);
  });

  it('fails (so the message is retried) when rendering fails or produces nothing', async () => {
    const broken = new InvoicePdfCacheWorker(fileCache.cache, async () => {
      throw new Error('font missing');
    });
    await expect(broken.handle(reported())).rejects.toThrow('font missing');
    const empty = new InvoicePdfCacheWorker(fileCache.cache, async () => Buffer.alloc(0));
    await expect(empty.handle(reported())).rejects.toThrow(/empty PDF/);
    expect(await fileCache.cache.head(invoicePdfKey('txp_riyadh_retail', 'inv_1'))).toBeUndefined();
  });

  describe('over the broker', () => {
    let url: string;
    let keepAlive: ChannelModel;
    let publisher: Channel;
    let runtime: InvoicePdfWorkerRuntime | undefined;
    let seq = 0;

    beforeEach(async () => {
      url = `amqp://file-cache-worker-${process.pid}-${++seq}`;
      keepAlive = await resolveAmqpConnect('fake')(url);
      publisher = await keepAlive.createChannel();
      publisher.on('error', () => undefined);
    });

    afterEach(async () => {
      await runtime?.stop();
      runtime = undefined;
      await keepAlive.close().catch(() => undefined);
    });

    function settled(): Promise<DeliverySettled> {
      return new Promise((resolve) => runtime!.consumer.once('settled', resolve));
    }

    it('consumes zatca.invoice.reported from its own queue and dead-letters a malformed event', async () => {
      runtime = await startInvoicePdfWorker({
        cache: fileCache.cache,
        render: async (inv) => fakePdf(inv.invoiceHash),
        url,
        connect: resolveAmqpConnect('fake'),
        retry: { maxAttempts: 2, baseDelayMs: 5, maxDelayMs: 10, minDelayMs: 1 },
        ttlPumpIntervalMs: 5,
      });
      expect(runtime.consumer.queues.workQueue).toBe(INVOICE_PDF_CONSUMER);

      // Signed by ZATCA with the key of the zatca.invoice.reported stream (the file cache holds it).
      const zatca = eventAuthFor(SERVICES.zatca);
      const sign = (event: InvoiceReportedEvent | { eventType: string }, body: Buffer) =>
        signedPublishProperties(zatca, {
          eventType: event.eventType,
          eventId: (event as InvoiceReportedEvent).eventId ?? 'evt_unknown',
          body,
        });

      const ok = settled();
      const body = Buffer.from(JSON.stringify(reported()));
      publisher.publish('zatca.events', 'zatca.invoice.reported', body, sign(reported(), body));
      expect(await ok).toMatchObject({ outcome: 'acked', result: 'CACHED' });
      expect(await runtime.worker.get('txp_riyadh_retail', 'inv_1')).toBeDefined();

      const bad = settled();
      const badBody = Buffer.from(JSON.stringify({ eventType: 'zatca.invoice.reported' }));
      publisher.publish('zatca.events', 'zatca.invoice.reported', badBody, sign({ eventType: 'zatca.invoice.reported' }, badBody));
      const e = await bad;
      expect(e.outcome).toBe('dead-lettered');
      expect(e.reason).toMatch(/^SCHEMA_INVALID/);

      // Unsigned (or forged): rejected before the body is even parsed.
      const forged = settled();
      publisher.publish('zatca.events', 'zatca.invoice.reported', Buffer.from(JSON.stringify(reported({ invoiceId: 'inv_forged' }))), {
        messageId: 'evt_forged',
      });
      const rejected = await forged;
      expect(rejected.outcome).toBe('dead-lettered');
      expect(rejected.reason).toMatch(/^UNTRUSTED_EVENT: UNSIGNED_EVENT/);
      expect(await runtime.worker.get('txp_riyadh_retail', 'inv_forged')).toBeUndefined();
    });
  });
});
