/**
 * Merchant settlement statements and documents, driven by the platform's settlement workflow.
 *
 * Consumes (queue `file-cache.settlement-statements`):
 *   ledger.events / ledger.merchant-settlement.posted      → the statement JSON (gross, fee, VAT,
 *        net payout, journal transaction id, legs, window, …) with status PENDING_TAX_REPORTING,
 *        kept for 72 hours
 *   zatca.events  / zatca.settlement-documents.reported    → the fee invoice and the payout credit
 *        note cached as PDF (rendered from the event's document snapshot) and UBL XML, then the
 *        statement becomes FINAL and lists them (keys, hashes, ICVs)
 *
 * The statement only ever moves forward (absent → PENDING → FINAL): the two events may arrive in
 * any order (different queues, retries, DLQ replays) and any number of times. Writes of one
 * statement are serialised in-process (KeyedMutex) and across processes sharing the cache
 * directory (a lock file), and a FINAL statement keeps the expiry of the first write (72 h from
 * the settlement, not from the last event).
 *
 * Reads go through the hotness tracker (a TinyLFU frequency sketch): concurrent fetches of one
 * statement share ONE store read and parse (single flight), and a statement that is hot (fetched
 * at least `hotThreshold` times recently) is pinned in memory for `hotTtlMs`, so a burst of
 * fetches — e.g. every merchant dashboard refreshing when the settlement lands — costs one read
 * instead of thousands (no cache stampede). A write drops the pinned copy and starts a new read
 * generation, so nobody is served a PENDING statement after it became FINAL in this process.
 */
import { createHash } from 'node:crypto';
import { mkdir } from 'node:fs/promises';
import { join } from 'node:path';

import type { CacheManager } from '../cache/cache-manager';
import { FrequencySketch } from '../cache/tiny-lfu';
import { FileLock } from '../resilience/file-lock';
import type { Clock } from '../shared/clock';
import { systemClock } from '../shared/clock';
import { KeyedMutex } from '../shared/keyed-mutex';
import { SingleFlight } from '../shared/single-flight';
import {
  CONSUMERS,
  CONSUMER_BINDINGS,
  EVENT_TYPES,
  MerchantSettlementPostedSchema,
  SettlementDocumentsReportedSchema,
  type InvoiceDocument,
  type MerchantSettlementPostedEvent,
  type MerchantSettlementPostedPayload,
  type SettlementDocumentRole,
  type SettlementDocumentsReportedEvent,
  type SettlementFee,
} from '../../../../shared/contracts';
import { EventAuthenticator, SERVICES, deliveryVerifier, eventAuthFor } from '../../../../shared/security';
import {
  ReliableConsumer,
  type AmqpConnect,
  type LogFn,
  type ReconnectPolicy,
  type RetryPolicy,
} from '../../../../shared/messaging';
import { invoicePdfKey } from './invoice-pdf-worker';

export const SETTLEMENT_STATEMENT_CONSUMER = CONSUMERS.fileCacheSettlementStatements;

/** How long a merchant's settlement statement is kept: 72 hours. */
export const SETTLEMENT_STATEMENT_TTL_MS = 72 * 60 * 60 * 1000;
const ONE_YEAR_MS = 365 * 24 * 60 * 60 * 1000;

export function settlementStatementKey(merchantId: string, batchId: string): string {
  return `settlements/${merchantId}/${batchId}.json`;
}

export function invoiceXmlKey(taxpayerId: string, invoiceId: string): string {
  return `zatca/invoices/${taxpayerId}/${invoiceId}.xml`;
}

export type SettlementStatementStatus = 'PENDING_TAX_REPORTING' | 'FINAL';

export interface StatementDocumentRef {
  role: SettlementDocumentRole;
  invoiceId: string;
  invoiceNumber: string;
  invoiceTypeCode: string;
  invoiceSubtype: string;
  icv: number;
  invoiceHash: string;
  previousInvoiceHash: string;
  status: string;
  totals: { taxableHalalas: number; vatHalalas: number; totalHalalas: number };
  pdf: { key: string; sha256: string; bytes: number };
  xml: { key: string; sha256: string; bytes: number };
}

export interface SettlementStatement {
  schema: 'ksa.merchant-settlement-statement/v1';
  status: SettlementStatementStatus;
  batchId: string;
  merchantId: string;
  merchantName: string;
  currency: 'SAR';
  window: { from: string | null; to: string };
  period: { firstSettledAt: string; lastSettledAt: string };
  itemCount: number;
  amounts: {
    grossHalalas: number;
    feeNetHalalas: number;
    feeVatHalalas: number;
    feeTotalHalalas: number;
    netPayoutHalalas: number;
    gross: string;
    feeNet: string;
    feeVat: string;
    feeTotal: string;
    netPayout: string;
  };
  fee: SettlementFee;
  journal: {
    transactionId: string;
    postedAt: string;
    legs: MerchantSettlementPostedPayload['journal'];
    accounts: MerchantSettlementPostedPayload['accounts'];
  };
  /** null while PENDING_TAX_REPORTING; the reported / cleared ZATCA documents once FINAL. */
  tax: null | { taxpayerId: string; reportedAt: string; documents: StatementDocumentRef[] };
  firstWrittenAt: string;
  updatedAt: string;
  expiresAt: string;
}

export interface StatementRead {
  key: string;
  statement: SettlementStatement;
  /** The JSON exactly as stored (served as-is over HTTP). */
  bytes: Buffer;
  /** Strong validator: SHA-256 of the bytes. */
  etag: string;
  expiresAt: number;
}

export interface StatementStoreStats {
  /** Every fetch call. */
  fetches: number;
  /** Served from the pinned copy of a hot statement (no store access at all). */
  hotHits: number;
  /** Joined a read already in flight for the same statement. */
  coalesced: number;
  /** Actual reads of the cache store. */
  storeReads: number;
  misses: number;
  writes: number;
  /** Statements currently pinned as hot. */
  pinned: number;
}

export interface SettlementStatementStoreOptions {
  /** Statement lifetime. Default 72 h. */
  ttlMs?: number;
  /** Fetches (as estimated by the frequency sketch) after which a statement counts as hot. Default 3. */
  hotThreshold?: number;
  /** How long a hot statement is served from memory before it is read again. Default 1 s. */
  hotTtlMs?: number;
  /** Upper bound on pinned statements (the coldest is dropped first). Default 256. */
  maxHotEntries?: number;
  /** Expected number of distinct statements fetched (sizes the sketch). Default 10 000. */
  sketchCapacity?: number;
  /** Directory for cross-process write locks (processes sharing the cache directory). */
  lockDirectory?: string;
  lockTimeoutMs?: number;
  clock?: Clock;
}

const formatSar = (halalas: number): string => {
  const sign = halalas < 0 ? '-' : '';
  const abs = Math.abs(halalas);
  return `${sign}${Math.floor(abs / 100)}.${String(abs % 100).padStart(2, '0')}`;
};

const sha256 = (bytes: Uint8Array): string => createHash('sha256').update(bytes).digest('hex');

export type StatementWriteOutcome = 'STATEMENT_PENDING' | 'STATEMENT_FINAL' | 'ALREADY_PRESENT' | 'ALREADY_FINAL';

/**
 * The statements in the file cache: monotonic writes, stampede-free reads. `cache` must be a
 * CacheManager without TTL jitter (the 72 h are a retention promise, not a refresh hint).
 */
export class SettlementStatementStore {
  readonly #cache: CacheManager;
  readonly #ttlMs: number;
  readonly #hotThreshold: number;
  readonly #hotTtlMs: number;
  readonly #maxHot: number;
  readonly #clock: Clock;
  readonly #lockDirectory: string | undefined;
  readonly #lockTimeoutMs: number;
  /** The hotness tracker: approximate, aging fetch frequency per statement (fixed memory). */
  readonly #sketch: FrequencySketch;
  readonly #flights = new SingleFlight<StatementRead | undefined>();
  readonly #hot = new Map<string, { read: StatementRead; until: number }>();
  /** Bumped by every write of a key: reads of an older generation are never pinned or joined. */
  readonly #generation = new Map<string, number>();
  readonly #writes = new KeyedMutex();
  #lockDirReady: Promise<void> | undefined;
  readonly #stats = { fetches: 0, hotHits: 0, coalesced: 0, storeReads: 0, misses: 0, writes: 0 };

  constructor(cache: CacheManager, options: SettlementStatementStoreOptions = {}) {
    this.#cache = cache;
    this.#ttlMs = options.ttlMs ?? SETTLEMENT_STATEMENT_TTL_MS;
    this.#hotThreshold = options.hotThreshold ?? 3;
    this.#hotTtlMs = options.hotTtlMs ?? 1_000;
    this.#maxHot = options.maxHotEntries ?? 256;
    this.#clock = options.clock ?? systemClock;
    this.#lockDirectory = options.lockDirectory;
    this.#lockTimeoutMs = options.lockTimeoutMs ?? 5_000;
    this.#sketch = new FrequencySketch({ capacity: options.sketchCapacity ?? 10_000 });
  }

  /** Fetch a statement (undefined when absent or expired). Stampede-protected; see the module doc. */
  async fetch(merchantId: string, batchId: string): Promise<StatementRead | undefined> {
    const key = settlementStatementKey(merchantId, batchId);
    this.#stats.fetches += 1;
    this.#sketch.increment(key);
    const now = this.#clock.now();
    const pinned = this.#hot.get(key);
    if (pinned !== undefined) {
      if (pinned.until > now && pinned.read.expiresAt > now) {
        this.#stats.hotHits += 1;
        return pinned.read;
      }
      this.#hot.delete(key);
    }
    const generation = this.#generation.get(key) ?? 0;
    const flightKey = `${key}#${generation}`;
    if (this.#flights.isInFlight(flightKey)) this.#stats.coalesced += 1;
    return this.#flights.run(flightKey, async () => {
      this.#stats.storeReads += 1;
      const read = await this.#readStore(key);
      if (read === undefined) {
        this.#stats.misses += 1;
        return undefined;
      }
      if ((this.#generation.get(key) ?? 0) === generation && this.#sketch.frequency(key) >= this.#hotThreshold) {
        this.#pin(key, read);
      }
      return read;
    });
  }

  /** The hotness tracker's estimate of how often a statement was fetched recently. */
  hotness(merchantId: string, batchId: string): number {
    return this.#sketch.frequency(settlementStatementKey(merchantId, batchId));
  }

  stats(): StatementStoreStats {
    return { ...this.#stats, pinned: this.#hot.size };
  }

  /** The settlement was posted: write the PENDING statement unless one already exists. */
  async applyPosted(settlement: MerchantSettlementPostedPayload): Promise<StatementWriteOutcome> {
    const key = settlementStatementKey(settlement.merchantId, settlement.batchId);
    return this.#exclusive(key, async () => {
      const current = await this.#readStore(key);
      if (current !== undefined) return 'ALREADY_PRESENT';
      const now = this.#clock.now();
      const statement = buildStatement(settlement, null, now, now + this.#ttlMs);
      await this.#write(key, statement, this.#ttlMs);
      return 'STATEMENT_PENDING';
    });
  }

  /** ZATCA accepted the documents: the statement becomes FINAL (keeping its original expiry). */
  async applyFinal(
    settlement: MerchantSettlementPostedPayload,
    tax: NonNullable<SettlementStatement['tax']>
  ): Promise<StatementWriteOutcome> {
    const key = settlementStatementKey(settlement.merchantId, settlement.batchId);
    return this.#exclusive(key, async () => {
      const current = await this.#readStore(key);
      if (current?.statement.status === 'FINAL') return 'ALREADY_FINAL';
      const now = this.#clock.now();
      // Keep the retention the merchant was promised at the first write (72 h from then).
      const expiresAt = current !== undefined ? Date.parse(current.statement.expiresAt) : now + this.#ttlMs;
      const firstWrittenAt = current !== undefined ? Date.parse(current.statement.firstWrittenAt) : now;
      const statement = buildStatement(settlement, tax, firstWrittenAt, expiresAt, now);
      await this.#write(key, statement, Math.max(1, expiresAt - now));
      return 'STATEMENT_FINAL';
    });
  }

  async #readStore(key: string): Promise<StatementRead | undefined> {
    const hit = await this.#cache.get(key);
    if (hit === undefined) return undefined;
    return {
      key,
      statement: JSON.parse(hit.value.toString('utf8')) as SettlementStatement,
      bytes: hit.value,
      etag: sha256(hit.value),
      expiresAt: hit.metadata.expiresAt,
    };
  }

  async #write(key: string, statement: SettlementStatement, ttlMs: number): Promise<void> {
    const bytes = Buffer.from(JSON.stringify(statement, null, 2), 'utf8');
    await this.#cache.put(key, bytes, {
      ttlMs,
      staleWhileRevalidateMs: 0,
      attributes: {
        'content-type': 'application/json',
        'x-batch-id': statement.batchId,
        'x-merchant-id': statement.merchantId,
        'x-settlement-status': statement.status,
        'x-journal-ref': statement.journal.transactionId,
        'x-content-sha256': sha256(bytes),
      },
    });
    this.#stats.writes += 1;
    this.#generation.set(key, (this.#generation.get(key) ?? 0) + 1);
    this.#hot.delete(key);
  }

  #pin(key: string, read: StatementRead): void {
    if (!this.#hot.has(key) && this.#hot.size >= this.#maxHot) {
      // Drop the coldest pinned statement (by the sketch's estimate).
      let coldest: string | undefined;
      let lowest = Number.POSITIVE_INFINITY;
      for (const candidate of this.#hot.keys()) {
        const f = this.#sketch.frequency(candidate);
        if (f < lowest) {
          lowest = f;
          coldest = candidate;
        }
      }
      if (coldest !== undefined) this.#hot.delete(coldest);
    }
    this.#hot.set(key, { read, until: Math.min(this.#clock.now() + this.#hotTtlMs, read.expiresAt) });
  }

  async #exclusive<T>(key: string, task: () => Promise<T>): Promise<T> {
    return this.#writes.runExclusive(key, async () => {
      if (this.#lockDirectory === undefined) return task();
      this.#lockDirReady ??= mkdir(this.#lockDirectory, { recursive: true }).then(() => undefined);
      await this.#lockDirReady;
      const lockPath = join(this.#lockDirectory, `${sha256(Buffer.from(key)).slice(0, 32)}.lock`);
      return FileLock.withLock(lockPath, { timeoutMs: this.#lockTimeoutMs, staleAfterMs: 30_000, retryDelayMs: 20 }, () => task());
    });
  }
}

function buildStatement(
  p: MerchantSettlementPostedPayload,
  tax: SettlementStatement['tax'],
  firstWrittenAt: number,
  expiresAt: number,
  updatedAt: number = firstWrittenAt
): SettlementStatement {
  return {
    schema: 'ksa.merchant-settlement-statement/v1',
    status: tax === null ? 'PENDING_TAX_REPORTING' : 'FINAL',
    batchId: p.batchId,
    merchantId: p.merchantId,
    merchantName: p.merchantName,
    currency: 'SAR',
    window: p.window,
    period: { firstSettledAt: p.firstSettledAt, lastSettledAt: p.lastSettledAt },
    itemCount: p.itemCount,
    amounts: {
      grossHalalas: p.grossHalalas,
      feeNetHalalas: p.fee.netHalalas,
      feeVatHalalas: p.fee.vatHalalas,
      feeTotalHalalas: p.fee.totalHalalas,
      netPayoutHalalas: p.payoutHalalas,
      gross: formatSar(p.grossHalalas),
      feeNet: formatSar(p.fee.netHalalas),
      feeVat: formatSar(p.fee.vatHalalas),
      feeTotal: formatSar(p.fee.totalHalalas),
      netPayout: formatSar(p.payoutHalalas),
    },
    fee: p.fee,
    journal: { transactionId: p.journalRef, postedAt: p.postedAt, legs: p.journal, accounts: p.accounts },
    tax,
    firstWrittenAt: new Date(firstWrittenAt).toISOString(),
    updatedAt: new Date(updatedAt).toISOString(),
    expiresAt: new Date(expiresAt).toISOString(),
  };
}

/** Renders the PDF of an issued ZATCA document from its event snapshot. */
export type InvoiceDocumentRenderer = (document: InvoiceDocument) => Promise<Uint8Array>;

export type SettlementStatementOutcome =
  | StatementWriteOutcome
  | 'IGNORED_UNKNOWN_EVENT';

export type SettlementWorkerEvent = MerchantSettlementPostedEvent | SettlementDocumentsReportedEvent;

export interface SettlementStatementWorkerOptions {
  /** Lifetime of cached document PDFs / XML. Default one year. */
  documentTtlMs?: number;
}

/** Handles both events of the statement lifecycle (idempotent, order-independent). */
export class SettlementStatementWorker {
  constructor(
    private readonly cache: CacheManager,
    readonly statements: SettlementStatementStore,
    private readonly render: InvoiceDocumentRenderer,
    private readonly options: SettlementStatementWorkerOptions = {}
  ) {}

  async handle(event: SettlementWorkerEvent): Promise<SettlementStatementOutcome> {
    if (event.eventType === EVENT_TYPES.merchantSettlementPosted) {
      return this.statements.applyPosted(event.payload);
    }
    if (event.eventType === EVENT_TYPES.settlementDocumentsReported) {
      const p = event.payload;
      const documents: StatementDocumentRef[] = [];
      for (const { role, document } of p.documents) {
        documents.push(await this.cacheDocument(role, document, p.batchId, event.eventId));
      }
      return this.statements.applyFinal(p.settlement, { taxpayerId: p.taxpayerId, reportedAt: p.reportedAt, documents });
    }
    return 'IGNORED_UNKNOWN_EVENT';
  }

  /** The cached PDF / XML of a settlement document (undefined when absent). */
  async getDocument(taxpayerId: string, invoiceId: string, format: 'pdf' | 'xml'): Promise<{ key: string; bytes: Buffer; attributes: Record<string, string> } | undefined> {
    const key = format === 'pdf' ? invoicePdfKey(taxpayerId, invoiceId) : invoiceXmlKey(taxpayerId, invoiceId);
    const hit = await this.cache.get(key);
    return hit === undefined ? undefined : { key, bytes: hit.value, attributes: { ...hit.metadata.attributes } };
  }

  /** PDF (rendered) + UBL XML (as issued) of one document; a fresh entry for the same hash is kept. */
  private async cacheDocument(
    role: SettlementDocumentRole,
    document: InvoiceDocument & { xmlUblBase64: string },
    batchId: string,
    eventId: string
  ): Promise<StatementDocumentRef> {
    const ttlMs = this.options.documentTtlMs ?? ONE_YEAR_MS;
    const attributes = {
      'x-invoice-id': document.invoiceId,
      'x-invoice-number': document.invoiceNumber,
      'x-invoice-hash': document.invoiceHash,
      'x-icv': String(document.icv),
      'x-document-role': role,
      'x-batch-id': batchId,
      'x-event-id': eventId,
    };

    const pdfKey = invoicePdfKey(document.taxpayerId, document.invoiceId);
    let pdfMeta = await this.cache.head(pdfKey);
    if (pdfMeta === undefined || pdfMeta.freshness !== 'fresh' || pdfMeta.attributes['x-invoice-hash'] !== document.invoiceHash) {
      const pdf = await this.render(document);
      if (pdf.byteLength === 0) throw new Error(`Renderer produced an empty PDF for ${role} ${document.invoiceId}`);
      const meta = await this.cache.put(pdfKey, pdf, {
        ttlMs,
        staleWhileRevalidateMs: 0,
        attributes: { ...attributes, 'content-type': 'application/pdf', 'x-content-sha256': sha256(pdf) },
      });
      pdfMeta = { ...meta, freshness: 'fresh' };
    }

    const xmlKey = invoiceXmlKey(document.taxpayerId, document.invoiceId);
    let xmlMeta = await this.cache.head(xmlKey);
    if (xmlMeta === undefined || xmlMeta.freshness !== 'fresh' || xmlMeta.attributes['x-invoice-hash'] !== document.invoiceHash) {
      const xml = Buffer.from(document.xmlUblBase64, 'base64');
      const meta = await this.cache.put(xmlKey, xml, {
        ttlMs,
        staleWhileRevalidateMs: 0,
        attributes: { ...attributes, 'content-type': 'application/xml; charset=utf-8', 'x-content-sha256': sha256(xml) },
      });
      xmlMeta = { ...meta, freshness: 'fresh' };
    }

    return {
      role,
      invoiceId: document.invoiceId,
      invoiceNumber: document.invoiceNumber,
      invoiceTypeCode: document.invoiceTypeCode,
      invoiceSubtype: document.invoiceSubtype,
      icv: document.icv,
      invoiceHash: document.invoiceHash,
      previousInvoiceHash: document.previousInvoiceHash,
      status: document.status,
      totals: {
        taxableHalalas: document.totals.taxableHalalas,
        vatHalalas: document.totals.vatHalalas,
        totalHalalas: document.totals.totalHalalas,
      },
      pdf: { key: pdfKey, sha256: pdfMeta.attributes['x-content-sha256'] ?? '', bytes: pdfMeta.size },
      xml: { key: xmlKey, sha256: xmlMeta.attributes['x-content-sha256'] ?? '', bytes: xmlMeta.size },
    };
  }
}

export interface SettlementStatementRuntimeOptions extends SettlementStatementStoreOptions, SettlementStatementWorkerOptions {
  /** For the statements: a CacheManager without TTL jitter. */
  statementCache: CacheManager;
  /** For the document PDFs / XML (the service's regular cache). */
  documentCache: CacheManager;
  render: InvoiceDocumentRenderer;
  url: string;
  connect: AmqpConnect;
  retry?: Partial<RetryPolicy>;
  prefetch?: number;
  ttlPumpIntervalMs?: number;
  reconnect?: Partial<ReconnectPolicy>;
  /** Service-to-service event authentication; default: the file cache's own. */
  auth?: EventAuthenticator;
  log?: LogFn;
}

export interface SettlementStatementRuntime {
  worker: SettlementStatementWorker;
  statements: SettlementStatementStore;
  consumer: ReliableConsumer<SettlementWorkerEvent>;
  stop(): Promise<void>;
}

/** Consume both settlement events into one queue (`file-cache.settlement-statements`). */
export async function startSettlementStatementWorker(options: SettlementStatementRuntimeOptions): Promise<SettlementStatementRuntime> {
  const statements = new SettlementStatementStore(options.statementCache, options);
  const worker = new SettlementStatementWorker(options.documentCache, statements, options.render, options);
  const consumer = new ReliableConsumer<SettlementWorkerEvent>({
    name: SETTLEMENT_STATEMENT_CONSUMER,
    bindings: CONSUMER_BINDINGS[CONSUMERS.fileCacheSettlementStatements],
    decode: (raw) => {
      const type = (raw as { eventType?: unknown } | null)?.eventType;
      return (
        type === EVENT_TYPES.settlementDocumentsReported
          ? SettlementDocumentsReportedSchema.parse(raw)
          : MerchantSettlementPostedSchema.parse(raw)
      ) as SettlementWorkerEvent;
    },
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
  return { worker, statements, consumer, stop: () => consumer.close() };
}
