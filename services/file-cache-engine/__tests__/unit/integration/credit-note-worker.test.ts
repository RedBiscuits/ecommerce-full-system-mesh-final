import type { Channel, ChannelModel } from 'amqplib';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { createFileCache, type FileCache } from '../../../src/main';
import { CREDIT_NOTE_CONSUMER, CreditNoteCacheWorker, startCreditNoteWorker, type CreditNoteRenderer, type CreditNoteWorkerRuntime } from '../../../src/integration/credit-note-worker';
import { InvoicePdfCacheWorker, invoicePdfKey } from '../../../src/integration/invoice-pdf-worker';
import { invoiceXmlKey } from '../../../src/integration/settlement-statements';
import { EVENT_TYPES, PRODUCERS, buildIntegrationEvent, type CreditNoteReportedEvent, type InvoiceDocument, type InvoiceReportedEvent } from '../../../../../shared/contracts';
import { resolveAmqpConnect, type DeliverySettled } from '../../../../../shared/messaging';
import { SERVICES, eventAuthFor, signedPublishProperties } from '../../../../../shared/security';
import { makeTempDir, type TempDir } from '../../helpers/temp-dir';

const TAXPAYER = 'txp_riyadh_retail';

function document(invoiceId: string, typeCode: '388' | '381', hash: string, total: number, icv: number): InvoiceDocument {
  return {
    invoiceId,
    invoiceNumber: `${typeCode === '388' ? 'INV' : 'CN'}-${invoiceId}`,
    uuid: `uuid-${invoiceId}`,
    invoiceTypeCode: typeCode,
    invoiceSubtype: '0200000',
    taxpayerId: TAXPAYER,
    icv,
    invoiceHash: hash,
    previousInvoiceHash: 'prev',
    qrBase64: 'AQ==',
    status: 'REPORTED',
    issuedAt: '2026-11-15T10:00:00.000Z',
    reportedAt: '2026-11-15T10:00:01.000Z',
    customerName: 'Noura',
    totals: { subtotalHalalas: total, discountHalalas: 0, allowanceHalalas: 0, taxableHalalas: total, vatHalalas: 0, totalHalalas: total },
    lines: [{ lineNumber: 1, sku: 'X', nameEn: 'x', nameAr: 'x', quantity: 1, unitPriceHalalas: total, discountHalalas: 0, netAmountHalalas: total, vatCategory: 'S', vatRateBps: 1500, vatAmountHalalas: 0, lineTotalHalalas: total }],
    allowanceReason: null,
    vatBreakdown: [],
    seller: { legalNameEn: 'Riyadh Orbit', legalNameAr: 'مدار', vatNumber: '310123456700003', crnNumber: '1010987654', buildingNumber: '4210', streetName: 'King Fahd Road', city: 'Riyadh', postalCode: '12214', countryCode: 'SA' },
  };
}

function reportedInvoice(): InvoiceReportedEvent {
  return buildIntegrationEvent({
    eventId: 'evt_inv',
    eventType: EVENT_TYPES.invoiceReported,
    producer: PRODUCERS.zatca,
    correlationId: 'ord_1',
    causationId: null,
    aggregateId: 'inv_1',
    payload: { ...document('inv_1', '388', 'hash-inv', 20700, 1), orderId: 'ord_1', journalRef: 'cap_1', status: 'REPORTED' },
  }) as InvoiceReportedEvent;
}

function creditNoteEvent(refundId: string, amount: number, creditedTotal: number, eventId = `evt_cn_${refundId}`): CreditNoteReportedEvent {
  return buildIntegrationEvent({
    eventId,
    eventType: EVENT_TYPES.creditNoteReported,
    producer: PRODUCERS.zatca,
    correlationId: 'ord_1',
    causationId: null,
    aggregateId: 'inv_1',
    payload: {
      refund: { refundId, refundEventId: `evt_rf_${refundId}`, paymentId: 'pay_1', orderId: 'ord_1', merchantId: TAXPAYER, currency: 'SAR', amountHalalas: amount, capturedHalalas: 20700, refundedTotalHalalas: creditedTotal, reason: null, refundedAt: '2026-11-16T00:00:00.000Z' },
      journalRef: `rfd_${refundId}`,
      originalInvoiceId: 'inv_1',
      originalInvoiceNumber: 'INV-inv_1',
      creditedTotalHalalas: creditedTotal,
      creditNote: { ...document(`cn_${refundId}`, '381', `hash-${refundId}`, amount, 2), xmlUblBase64: Buffer.from(`<Invoice>${refundId}</Invoice>`).toString('base64') },
      invoice: document('inv_1', '388', 'hash-inv', 20700, 1),
      creditNotes: [{ invoiceId: `cn_${refundId}`, invoiceNumber: `CN-cn_${refundId}`, icv: 2, totalHalalas: amount, refundId, reportedAt: null }],
      reportedAt: '2026-11-16T00:00:02.000Z',
    },
  }) as CreditNoteReportedEvent;
}

const render: CreditNoteRenderer = async (doc, options) => Buffer.from(`%PDF-1.3 ${doc.invoiceTypeCode} ${doc.invoiceHash} credits=${(options?.creditNotes ?? []).map((n) => n.totalHalalas).join('+') || 'none'}`);

describe('CreditNoteCacheWorker (credit note PDFs + invalidation of the credited invoice PDF)', () => {
  let dir: TempDir;
  let fileCache: FileCache;

  beforeEach(async () => {
    dir = await makeTempDir();
    fileCache = await createFileCache({ directory: dir.path, sweeper: false });
    // The invoice PDF as cached by the order flow, before any refund.
    await new InvoicePdfCacheWorker(fileCache.cache, async (inv) => Buffer.from(`%PDF-1.3 388 ${inv.invoiceHash} credits=none`)).handle(reportedInvoice());
  });

  afterEach(async () => {
    await fileCache.close();
    await dir.cleanup();
  });

  it('caches the credit note (PDF + XML) and replaces the stale invoice PDF with one that shows the credits', async () => {
    const spy = vi.fn(render);
    const worker = new CreditNoteCacheWorker(fileCache.cache, spy);
    const before = await fileCache.cache.get(invoicePdfKey(TAXPAYER, 'inv_1'));
    expect(before?.value.toString()).toBe('%PDF-1.3 388 hash-inv credits=none');

    expect(await worker.handle(creditNoteEvent('rf_1', 5000, 5000))).toBe('CACHED_AND_INVALIDATED');
    const note = await fileCache.cache.get(invoicePdfKey(TAXPAYER, 'cn_rf_1'));
    expect(note?.value.toString()).toBe('%PDF-1.3 381 hash-rf_1 credits=none');
    expect(note?.metadata.attributes).toMatchObject({ 'x-refund-id': 'rf_1', 'x-billing-reference': 'inv_1', 'x-invoice-hash': 'hash-rf_1' });
    expect((await fileCache.cache.get(invoiceXmlKey(TAXPAYER, 'cn_rf_1')))?.value.toString()).toBe('<Invoice>rf_1</Invoice>');

    const after = await fileCache.cache.get(invoicePdfKey(TAXPAYER, 'inv_1'));
    expect(after?.value.toString()).toBe('%PDF-1.3 388 hash-inv credits=5000');
    expect(after?.metadata.attributes).toMatchObject({ 'x-credited-halalas': '5000', 'x-credit-notes': '1', 'x-invoice-hash': 'hash-inv' });
    expect(after?.metadata.checksum).not.toBe(before?.metadata.checksum);
    expect(spy).toHaveBeenCalledTimes(2);
  });

  it('is idempotent: a redelivery renders nothing again; a second refund refreshes the invoice PDF once more', async () => {
    const spy = vi.fn(render);
    const worker = new CreditNoteCacheWorker(fileCache.cache, spy);
    await worker.handle(creditNoteEvent('rf_1', 5000, 5000));
    expect(await worker.handle(creditNoteEvent('rf_1', 5000, 5000, 'evt_cn_replay'))).toBe('ALREADY_CACHED');
    expect(spy).toHaveBeenCalledTimes(2);

    const second = creditNoteEvent('rf_2', 700, 5700);
    second.payload.creditNotes.unshift({ invoiceId: 'cn_rf_1', invoiceNumber: 'CN-cn_rf_1', icv: 2, totalHalalas: 5000, refundId: 'rf_1', reportedAt: null });
    expect(await worker.handle(second)).toBe('CACHED_AND_INVALIDATED');
    const invoice = await fileCache.cache.get(invoicePdfKey(TAXPAYER, 'inv_1'));
    expect(invoice?.value.toString()).toBe('%PDF-1.3 388 hash-inv credits=5000+700');
    expect(invoice?.metadata.attributes['x-credited-halalas']).toBe('5700');
    expect(spy).toHaveBeenCalledTimes(4);
  });

  it('fails (retry) when rendering fails, leaving the previous invoice PDF in place', async () => {
    const worker = new CreditNoteCacheWorker(fileCache.cache, async () => {
      throw new Error('font missing');
    });
    await expect(worker.handle(creditNoteEvent('rf_1', 5000, 5000))).rejects.toThrow('font missing');
    expect((await fileCache.cache.get(invoicePdfKey(TAXPAYER, 'inv_1')))?.value.toString()).toBe('%PDF-1.3 388 hash-inv credits=none');
  });

  describe('over the broker', () => {
    let url: string;
    let keepAlive: ChannelModel;
    let publisher: Channel;
    let runtime: CreditNoteWorkerRuntime | undefined;
    let seq = 0;

    beforeEach(async () => {
      url = `amqp://file-cache-credit-notes-${process.pid}-${++seq}`;
      keepAlive = await resolveAmqpConnect('fake')(url);
      publisher = await keepAlive.createChannel();
      publisher.on('error', () => undefined);
    });

    afterEach(async () => {
      await runtime?.stop();
      runtime = undefined;
      await keepAlive.close().catch(() => undefined);
    });

    it('consumes zatca.credit-note.reported from its own queue, signed by ZATCA', async () => {
      runtime = await startCreditNoteWorker({ cache: fileCache.cache, render, url, connect: resolveAmqpConnect('fake'), retry: { maxAttempts: 2, baseDelayMs: 5, maxDelayMs: 10, minDelayMs: 1 }, ttlPumpIntervalMs: 5 });
      expect(runtime.consumer.queues.workQueue).toBe(CREDIT_NOTE_CONSUMER);
      const settled = new Promise<DeliverySettled>((resolve) => runtime!.consumer.once('settled', resolve));
      const event = creditNoteEvent('rf_1', 5000, 5000);
      const body = Buffer.from(JSON.stringify(event));
      publisher.publish('zatca.events', 'zatca.credit-note.reported', body, signedPublishProperties(eventAuthFor(SERVICES.zatca), { eventType: event.eventType, eventId: event.eventId, body }));
      expect(await settled).toMatchObject({ outcome: 'acked', result: 'CACHED_AND_INVALIDATED' });
      expect((await fileCache.cache.get(invoicePdfKey(TAXPAYER, 'inv_1')))?.metadata.attributes['x-credited-halalas']).toBe('5000');
    });
  });
});
