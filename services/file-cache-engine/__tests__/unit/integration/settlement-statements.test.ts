import { createHash } from 'node:crypto';
import { createServer, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';

import type { Channel, ChannelModel } from 'amqplib';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { CacheManager } from '../../../src/cache/cache-manager';
import { createFileCache, type FileCache } from '../../../src/main';
import {
  SETTLEMENT_STATEMENT_CONSUMER,
  SETTLEMENT_STATEMENT_TTL_MS,
  SettlementStatementStore,
  SettlementStatementWorker,
  invoiceXmlKey,
  settlementStatementKey,
  startSettlementStatementWorker,
  type InvoiceDocumentRenderer,
  type SettlementStatementRuntime,
} from '../../../src/integration/settlement-statements';
import { createSettlementHttpHandler } from '../../../src/integration/settlement-http';
import { invoicePdfKey } from '../../../src/integration/invoice-pdf-worker';
import {
  EVENT_TYPES,
  PRODUCERS,
  buildIntegrationEvent,
  type InvoiceDocument,
  type MerchantSettlementPostedEvent,
  type MerchantSettlementPostedPayload,
  type SettlementDocumentsReportedEvent,
} from '../../../../../shared/contracts';
import { resolveAmqpConnect, type DeliverySettled } from '../../../../../shared/messaging';
import { SERVICES, eventAuthFor, signedPublishProperties } from '../../../../../shared/security';
import { FakeClock } from '../../helpers/fake-clock';
import { FaultyStore } from '../../helpers/faulty-store';
import { makeTempDir, type TempDir } from '../../helpers/temp-dir';

const MERCHANT = 'txp_riyadh_retail';
const HOUR = 3600_000;

function settlement(batchId = 'stb_1'): MerchantSettlementPostedPayload {
  return {
    batchId,
    merchantId: MERCHANT,
    merchantName: 'Riyadh Orbit E-Commerce Co.',
    currency: 'SAR',
    window: { from: null, to: '2026-11-15T00:00:00.000Z' },
    itemCount: 3,
    grossHalalas: 60000,
    fee: {
      ruleId: 'std-tiered',
      ruleVersion: 1,
      tiers: [{ fromHalalas: 0, toHalalas: null, bps: 250, baseHalalas: 60000, feeHalalas: 1500 }],
      percentageFeeHalalas: 1500,
      perTransactionHalalas: 50,
      fixedFeeHalalas: 150,
      minimumFeeHalalas: 0,
      netHalalas: 1650,
      vatRateBps: 1500,
      vatHalalas: 248,
      totalHalalas: 1898,
      capped: false,
    },
    payoutHalalas: 58102,
    journalRef: `stl_${batchId}`,
    journal: [
      { walletId: 'wallet_merchant_riyadh_retail', role: 'MERCHANT_REVENUE', direction: 'DEBIT', amountHalalas: 60000 },
      { walletId: 'wallet_fee_treasury_sar', role: 'PLATFORM_FEES', direction: 'CREDIT', amountHalalas: 1898 },
      { walletId: 'wallet_payout_riyadh_retail', role: 'MERCHANT_PAYOUT', direction: 'CREDIT', amountHalalas: 58102 },
    ],
    accounts: { revenueWalletId: 'wallet_merchant_riyadh_retail', feeWalletId: 'wallet_fee_treasury_sar', payoutWalletId: 'wallet_payout_riyadh_retail' },
    firstSettledAt: '2026-11-14T08:00:00.000Z',
    lastSettledAt: '2026-11-14T20:00:00.000Z',
    postedAt: '2026-11-15T00:00:01.000Z',
  };
}

function postedEvent(batchId = 'stb_1'): MerchantSettlementPostedEvent {
  return buildIntegrationEvent({
    eventId: `evt_posted_${batchId}`,
    eventType: EVENT_TYPES.merchantSettlementPosted,
    producer: PRODUCERS.ledger,
    correlationId: batchId,
    causationId: null,
    aggregateId: batchId,
    payload: settlement(batchId),
  }) as MerchantSettlementPostedEvent;
}

function document(invoiceId: string, typeCode: '388' | '381', hash: string, total: number): InvoiceDocument & { xmlUblBase64: string } {
  return {
    invoiceId,
    invoiceNumber: `${typeCode === '388' ? 'INV' : 'CN'}-${invoiceId}`,
    uuid: `uuid-${invoiceId}`,
    invoiceTypeCode: typeCode,
    invoiceSubtype: '0100000',
    taxpayerId: 'txp_ksa_platform',
    icv: typeCode === '388' ? 1 : 2,
    invoiceHash: hash,
    previousInvoiceHash: 'prev',
    qrBase64: 'AQ==',
    status: 'CLEARED',
    issuedAt: '2026-11-15T00:00:02.000Z',
    reportedAt: '2026-11-15T00:00:03.000Z',
    customerName: 'Riyadh Orbit E-Commerce Co.',
    totals: { subtotalHalalas: total, discountHalalas: 0, allowanceHalalas: 0, taxableHalalas: total, vatHalalas: 0, totalHalalas: total },
    lines: [
      {
        lineNumber: 1,
        sku: 'X',
        nameEn: 'x',
        nameAr: 'x',
        quantity: 1,
        unitPriceHalalas: total,
        discountHalalas: 0,
        netAmountHalalas: total,
        vatCategory: 'O',
        vatRateBps: 0,
        vatAmountHalalas: 0,
        lineTotalHalalas: total,
      },
    ],
    allowanceReason: null,
    vatBreakdown: [],
    seller: {
      legalNameEn: 'KSA Commerce Payments Platform Co.',
      legalNameAr: 'منصة',
      vatNumber: '311234567800003',
      crnNumber: '1010765432',
      buildingNumber: '7788',
      streetName: 'Prince Turki Road',
      city: 'Riyadh',
      postalCode: '13515',
      countryCode: 'SA',
    },
    xmlUblBase64: Buffer.from(`<Invoice>${invoiceId}</Invoice>`).toString('base64'),
  };
}

function reportedEvent(batchId = 'stb_1', eventId = `evt_docs_${batchId}`): SettlementDocumentsReportedEvent {
  return buildIntegrationEvent({
    eventId,
    eventType: EVENT_TYPES.settlementDocumentsReported,
    producer: PRODUCERS.zatca,
    correlationId: batchId,
    causationId: `evt_posted_${batchId}`,
    aggregateId: batchId,
    payload: {
      batchId,
      merchantId: MERCHANT,
      taxpayerId: 'txp_ksa_platform',
      journalRef: `stl_${batchId}`,
      settlement: settlement(batchId),
      documents: [
        { role: 'FEE_INVOICE' as const, document: document('inv_fee', '388', 'hash-fee', 1898) },
        { role: 'PAYOUT_CREDIT_NOTE' as const, document: document('inv_cn', '381', 'hash-cn', 58102) },
      ],
      reportedAt: '2026-11-15T00:00:04.000Z',
    },
  }) as SettlementDocumentsReportedEvent;
}

const fakePdf: InvoiceDocumentRenderer = async (doc) => Buffer.from(`%PDF-1.3 ${doc.invoiceTypeCode} ${doc.invoiceHash}`);

describe('Settlement statements (72 h JSON, pending → final, stampede-free reads)', () => {
  let clock: FakeClock;
  let store: FaultyStore;
  let cache: CacheManager;
  let statements: SettlementStatementStore;

  beforeEach(() => {
    clock = new FakeClock(Date.parse('2026-11-15T00:00:05.000Z'));
    store = new FaultyStore();
    cache = new CacheManager(store, { clock, ttlJitterRatio: 0, staleWhileRevalidateMs: 0 });
    statements = new SettlementStatementStore(cache, { clock, hotThreshold: 3, hotTtlMs: 1_000 });
  });

  it('writes the statement PENDING_TAX_REPORTING with everything the merchant needs, for 72 hours', async () => {
    expect(await statements.applyPosted(settlement())).toBe('STATEMENT_PENDING');
    const read = await statements.fetch(MERCHANT, 'stb_1');
    expect(read?.statement).toMatchObject({
      schema: 'ksa.merchant-settlement-statement/v1',
      status: 'PENDING_TAX_REPORTING',
      batchId: 'stb_1',
      itemCount: 3,
      amounts: { grossHalalas: 60000, feeNetHalalas: 1650, feeVatHalalas: 248, feeTotalHalalas: 1898, netPayoutHalalas: 58102, gross: '600.00', feeTotal: '18.98', netPayout: '581.02' },
      journal: { transactionId: 'stl_stb_1' },
      tax: null,
    });
    expect(read?.statement.journal.legs).toHaveLength(3);
    expect(read?.expiresAt).toBe(clock.now() + SETTLEMENT_STATEMENT_TTL_MS);
    expect(read?.etag).toBe(createHash('sha256').update(read!.bytes).digest('hex'));
    const entry = store.entries.get(settlementStatementKey(MERCHANT, 'stb_1'));
    expect(entry?.metadata.attributes).toMatchObject({ 'x-settlement-status': 'PENDING_TAX_REPORTING', 'x-journal-ref': 'stl_stb_1' });

    clock.advance(72 * HOUR - 1);
    expect(await statements.fetch(MERCHANT, 'stb_1')).toBeDefined();
    clock.advance(1);
    expect(await statements.fetch(MERCHANT, 'stb_1')).toBeUndefined();
  });

  it('only moves forward: FINAL keeps the original 72 h, a late posted event never downgrades it', async () => {
    await statements.applyPosted(settlement());
    const createdAt = clock.now();
    clock.advance(10 * HOUR);
    const tax = { taxpayerId: 'txp_ksa_platform', reportedAt: new Date(clock.now()).toISOString(), documents: [] };
    expect(await statements.applyFinal(settlement(), tax)).toBe('STATEMENT_FINAL');
    const final = await statements.fetch(MERCHANT, 'stb_1');
    expect(final?.statement.status).toBe('FINAL');
    expect(final?.expiresAt).toBe(createdAt + SETTLEMENT_STATEMENT_TTL_MS);
    expect(final?.statement.firstWrittenAt).toBe(new Date(createdAt).toISOString());

    expect(await statements.applyPosted(settlement())).toBe('ALREADY_PRESENT');
    expect(await statements.applyFinal(settlement(), tax)).toBe('ALREADY_FINAL');
    expect((await statements.fetch(MERCHANT, 'stb_1'))?.statement.status).toBe('FINAL');
  });

  it('accepts the events in any order (documents reported before the posted event arrives)', async () => {
    const tax = { taxpayerId: 'txp_ksa_platform', reportedAt: '2026-11-15T00:00:04.000Z', documents: [] };
    expect(await statements.applyFinal(settlement('stb_2'), tax)).toBe('STATEMENT_FINAL');
    expect(await statements.applyPosted(settlement('stb_2'))).toBe('ALREADY_PRESENT');
    const read = await statements.fetch(MERCHANT, 'stb_2');
    expect(read?.statement.status).toBe('FINAL');
    expect(read?.expiresAt).toBe(clock.now() + SETTLEMENT_STATEMENT_TTL_MS);
  });

  it('coalesces a stampede: 500 concurrent fetches cost ONE store read', async () => {
    await statements.applyPosted(settlement());
    const readsBefore = store.calls.read;
    const results = await Promise.all(Array.from({ length: 500 }, () => statements.fetch(MERCHANT, 'stb_1')));
    expect(new Set(results.map((r) => r?.etag)).size).toBe(1);
    expect(store.calls.read - readsBefore).toBe(1);
    expect(statements.stats()).toMatchObject({ fetches: 500, storeReads: 1, coalesced: 499 });
    // …and the tracker now knows the statement is hot.
    expect(statements.hotness(MERCHANT, 'stb_1')).toBe(15);
  });

  it('pins hot statements: once fetched often, repeated fetches are served from memory until the pin expires', async () => {
    await statements.applyPosted(settlement());
    const readsBefore = store.calls.read;
    for (let i = 0; i < 10; i++) await statements.fetch(MERCHANT, 'stb_1');
    // Fetches 1–3 read the store (the 3rd makes it hot and pins it); 4–10 are hot hits.
    expect(store.calls.read - readsBefore).toBe(3);
    expect(statements.stats()).toMatchObject({ hotHits: 7, pinned: 1 });

    clock.advance(1_001); // the pin expired: one read refreshes it
    for (let i = 0; i < 5; i++) await statements.fetch(MERCHANT, 'stb_1');
    expect(store.calls.read - readsBefore).toBe(4);

    // A cold statement is not pinned.
    await statements.applyPosted(settlement('stb_cold'));
    await statements.fetch(MERCHANT, 'stb_cold');
    expect(statements.stats().pinned).toBe(1);
  });

  it('a write drops the pinned copy: nobody is served the PENDING statement after it became FINAL', async () => {
    await statements.applyPosted(settlement());
    for (let i = 0; i < 5; i++) await statements.fetch(MERCHANT, 'stb_1');
    expect((await statements.fetch(MERCHANT, 'stb_1'))?.statement.status).toBe('PENDING_TAX_REPORTING');
    await statements.applyFinal(settlement(), { taxpayerId: 'txp_ksa_platform', reportedAt: 'x', documents: [] });
    expect((await statements.fetch(MERCHANT, 'stb_1'))?.statement.status).toBe('FINAL');
  });

  it('a read racing a write is never pinned with the older version', async () => {
    await statements.applyPosted(settlement());
    for (let i = 0; i < 3; i++) await statements.fetch(MERCHANT, 'stb_1'); // hot
    clock.advance(1_001);
    const racing = statements.fetch(MERCHANT, 'stb_1'); // reads PENDING …
    await statements.applyFinal(settlement(), { taxpayerId: 'txp_ksa_platform', reportedAt: 'x', documents: [] }); // … while FINAL lands
    await racing;
    expect((await statements.fetch(MERCHANT, 'stb_1'))?.statement.status).toBe('FINAL');
  });

  it('never serves a pinned statement past its 72 h', async () => {
    const shortLived = new SettlementStatementStore(cache, { clock, ttlMs: 2_000, hotThreshold: 1, hotTtlMs: 60_000 });
    await shortLived.applyPosted(settlement('stb_short'));
    expect(await shortLived.fetch(MERCHANT, 'stb_short')).toBeDefined();
    clock.advance(2_000);
    expect(await shortLived.fetch(MERCHANT, 'stb_short')).toBeUndefined();
  });

  it('bounds the pinned set, dropping the coldest statement first', async () => {
    const small = new SettlementStatementStore(cache, { clock, hotThreshold: 1, maxHotEntries: 2 });
    for (const id of ['a', 'b', 'c']) await small.applyPosted(settlement(id));
    for (let i = 0; i < 5; i++) await small.fetch(MERCHANT, 'a');
    await small.fetch(MERCHANT, 'b');
    await small.fetch(MERCHANT, 'c'); // evicts b (colder than a)
    expect(small.stats().pinned).toBe(2);
    const reads = store.calls.read;
    await small.fetch(MERCHANT, 'a');
    expect(store.calls.read).toBe(reads); // a stayed pinned
  });
});

describe('SettlementStatementWorker (documents + statement, idempotent)', () => {
  let dir: TempDir;
  let fileCache: FileCache;
  let statementCache: CacheManager;

  beforeEach(async () => {
    dir = await makeTempDir();
    fileCache = await createFileCache({ directory: dir.path, sweeper: false });
    statementCache = new CacheManager(fileCache.store, { ttlJitterRatio: 0, staleWhileRevalidateMs: 0 });
  });

  afterEach(async () => {
    await statementCache.close();
    await fileCache.close();
    await dir.cleanup();
  });

  it('caches PDF + UBL XML of both documents, then finalises the statement with their references', async () => {
    const render = vi.fn<InvoiceDocumentRenderer>(fakePdf);
    const statements = new SettlementStatementStore(statementCache, { lockDirectory: `${dir.path}/locks` });
    const worker = new SettlementStatementWorker(fileCache.cache, statements, render);

    expect(await worker.handle(postedEvent())).toBe('STATEMENT_PENDING');
    expect(await worker.handle(reportedEvent())).toBe('STATEMENT_FINAL');
    expect(render).toHaveBeenCalledTimes(2);

    const pdf = await worker.getDocument('txp_ksa_platform', 'inv_fee', 'pdf');
    expect(pdf?.key).toBe(invoicePdfKey('txp_ksa_platform', 'inv_fee'));
    expect(pdf?.bytes.toString()).toBe('%PDF-1.3 388 hash-fee');
    expect(pdf?.attributes).toMatchObject({ 'content-type': 'application/pdf', 'x-document-role': 'FEE_INVOICE', 'x-invoice-hash': 'hash-fee', 'x-batch-id': 'stb_1' });
    const xml = await worker.getDocument('txp_ksa_platform', 'inv_cn', 'xml');
    expect(xml?.key).toBe(invoiceXmlKey('txp_ksa_platform', 'inv_cn'));
    expect(xml?.bytes.toString()).toBe('<Invoice>inv_cn</Invoice>');

    const final = (await statements.fetch(MERCHANT, 'stb_1'))!.statement;
    expect(final.status).toBe('FINAL');
    expect(final.tax?.documents.map((d) => [d.role, d.invoiceNumber, d.icv, d.pdf.key, d.xml.key])).toEqual([
      ['FEE_INVOICE', 'INV-inv_fee', 1, invoicePdfKey('txp_ksa_platform', 'inv_fee'), invoiceXmlKey('txp_ksa_platform', 'inv_fee')],
      ['PAYOUT_CREDIT_NOTE', 'CN-inv_cn', 2, invoicePdfKey('txp_ksa_platform', 'inv_cn'), invoiceXmlKey('txp_ksa_platform', 'inv_cn')],
    ]);
    expect(final.tax?.documents[0].pdf.sha256).toBe(createHash('sha256').update(pdf!.bytes).digest('hex'));

    // Redeliveries (any order, any count) change nothing and render nothing again.
    expect(await worker.handle(reportedEvent('stb_1', 'evt_docs_replay'))).toBe('ALREADY_FINAL');
    expect(await worker.handle(postedEvent())).toBe('ALREADY_PRESENT');
    expect(render).toHaveBeenCalledTimes(2);
  });

  it('fails (so the message is retried) when a document cannot be rendered; the statement stays pending', async () => {
    const statements = new SettlementStatementStore(statementCache);
    const worker = new SettlementStatementWorker(fileCache.cache, statements, async () => {
      throw new Error('font missing');
    });
    await worker.handle(postedEvent());
    await expect(worker.handle(reportedEvent())).rejects.toThrow('font missing');
    expect((await statements.fetch(MERCHANT, 'stb_1'))?.statement.status).toBe('PENDING_TAX_REPORTING');
  });

  it('serves statements and documents over HTTP with ETags', async () => {
    const statements = new SettlementStatementStore(statementCache);
    const worker = new SettlementStatementWorker(fileCache.cache, statements, fakePdf);
    await worker.handle(postedEvent());
    const server: Server = createServer(createSettlementHttpHandler({ statements, worker }));
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    const base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
    try {
      const pending = await fetch(`${base}/settlements/${MERCHANT}/stb_1`);
      expect(pending.status).toBe(200);
      expect(pending.headers.get('x-settlement-status')).toBe('PENDING_TAX_REPORTING');
      const etag = pending.headers.get('etag')!;
      expect((await pending.json()).amounts.netPayout).toBe('581.02');
      expect((await fetch(`${base}/settlements/${MERCHANT}/stb_1`, { headers: { 'if-none-match': etag } })).status).toBe(304);

      await worker.handle(reportedEvent());
      const final = await fetch(`${base}/settlements/${MERCHANT}/stb_1.json`, { headers: { 'if-none-match': etag } });
      expect(final.status).toBe(200); // changed: the old ETag no longer matches
      expect(final.headers.get('x-settlement-status')).toBe('FINAL');

      const pdf = await fetch(`${base}/documents/txp_ksa_platform/inv_fee.pdf`);
      expect(pdf.headers.get('content-type')).toBe('application/pdf');
      expect(Buffer.from(await pdf.arrayBuffer()).toString()).toBe('%PDF-1.3 388 hash-fee');
      const xml = await fetch(`${base}/documents/txp_ksa_platform/inv_cn.xml`);
      expect(xml.headers.get('content-type')).toContain('application/xml');
      expect((await fetch(`${base}/settlements/${MERCHANT}/nope`)).status).toBe(404);
      expect((await fetch(`${base}/documents/txp_ksa_platform/nope.pdf`)).status).toBe(404);

      // A burst of concurrent HTTP fetches of one statement is one store read.
      const before = statements.stats().storeReads;
      await Promise.all(Array.from({ length: 50 }, () => fetch(`${base}/settlements/${MERCHANT}/stb_1`).then((r) => r.arrayBuffer())));
      expect(statements.stats().storeReads - before).toBeLessThanOrEqual(2);
    } finally {
      await new Promise((resolve) => server.close(resolve));
    }
  });

  describe('over the broker', () => {
    let url: string;
    let keepAlive: ChannelModel;
    let publisher: Channel;
    let runtime: SettlementStatementRuntime | undefined;
    let seq = 0;

    beforeEach(async () => {
      url = `amqp://file-cache-settlements-${process.pid}-${++seq}`;
      keepAlive = await resolveAmqpConnect('fake')(url);
      publisher = await keepAlive.createChannel();
      publisher.on('error', () => undefined);
    });

    afterEach(async () => {
      await runtime?.stop();
      runtime = undefined;
      await keepAlive.close().catch(() => undefined);
    });

    it('consumes both settlement events from its own queue and dead-letters a malformed one', async () => {
      runtime = await startSettlementStatementWorker({
        statementCache,
        documentCache: fileCache.cache,
        render: fakePdf,
        url,
        connect: resolveAmqpConnect('fake'),
        retry: { maxAttempts: 2, baseDelayMs: 5, maxDelayMs: 10, minDelayMs: 1 },
        ttlPumpIntervalMs: 5,
      });
      expect(runtime.consumer.queues.workQueue).toBe(SETTLEMENT_STATEMENT_CONSUMER);
      const settled = (): Promise<DeliverySettled> => new Promise((resolve) => runtime!.consumer.once('settled', resolve));

      const ledger = eventAuthFor(SERVICES.ledger);
      const zatca = eventAuthFor(SERVICES.zatca);

      let next = settled();
      let body = Buffer.from(JSON.stringify(postedEvent()));
      publisher.publish(
        'ledger.events',
        'ledger.merchant-settlement.posted',
        body,
        signedPublishProperties(ledger, { eventType: postedEvent().eventType, eventId: postedEvent().eventId, body })
      );
      expect(await next).toMatchObject({ outcome: 'acked', result: 'STATEMENT_PENDING' });

      next = settled();
      body = Buffer.from(JSON.stringify(reportedEvent()));
      publisher.publish(
        'zatca.events',
        'zatca.settlement-documents.reported',
        body,
        signedPublishProperties(zatca, { eventType: reportedEvent().eventType, eventId: reportedEvent().eventId, body })
      );
      expect(await next).toMatchObject({ outcome: 'acked', result: 'STATEMENT_FINAL' });
      expect((await runtime.statements.fetch(MERCHANT, 'stb_1'))?.statement.status).toBe('FINAL');

      next = settled();
      body = Buffer.from(JSON.stringify({ eventType: 'zatca.settlement-documents.reported' }));
      publisher.publish(
        'zatca.events',
        'zatca.settlement-documents.reported',
        body,
        signedPublishProperties(zatca, { eventType: 'zatca.settlement-documents.reported', eventId: 'evt_bad', body })
      );
      const bad = await next;
      expect(bad.outcome).toBe('dead-lettered');
      expect(bad.reason).toMatch(/^SCHEMA_INVALID/);

      // An event signed with a key the file cache does not hold (here: the ledger's own invariant
      // stream key) is refused before it is parsed.
      next = settled();
      body = Buffer.from(JSON.stringify(postedEvent('stb_forged')));
      publisher.publish('ledger.events', 'ledger.merchant-settlement.posted', body, {
        persistent: true,
        messageId: 'evt_forged',
        type: 'ledger.merchant-settlement.posted',
        headers: {
          'x-signature-alg': 'HMAC-SHA256',
          'x-key-id': 'ledger.invariant@v1',
          'x-event-stream': 'ledger.invariant',
          'x-event-issuer': 'wallet-ledger',
          'x-event-audience': 'file-cache-engine',
          'x-signed-at': new Date().toISOString(),
          'x-signature': 'AAAA',
        },
      });
      const forged = await next;
      expect(forged.outcome).toBe('dead-lettered');
      expect(forged.reason).toMatch(/^UNTRUSTED_EVENT: UNKNOWN_KEY_ID/);
      expect(await runtime.statements.fetch(MERCHANT, 'stb_forged')).toBeUndefined();
    });
  });
});
