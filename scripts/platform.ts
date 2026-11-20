/**
 * Platform composition root: boots every service in one process on ONE broker
 * and wires the order-to-invoice flow, split-tender checkouts (checkout
 * coordinator) and the merchant settlement batch workflow.
 *
 *   signed webhook ─► hyperpay-gateway ──outbox──► hyperpay / payment.result
 *                                                    │  billing.payment-results
 *                     billing-credits  ◄─────────────┘  capture (SUCCESS) / release (FAILED) the order's credit hold
 *                                      ──outbox──► billing.events / billing.order-credits.settled
 *                                                    │  ledger.order-settlements
 *                     wallet-ledger    ◄─────────────┘  capture → 3-leg journal (gateway / revenue / VAT), or release
 *                                      ──outbox──► ledger.events / ledger.payment.settled
 *                                                    │  zatca.ledger-settlements
 *                     zatca-tax        ◄─────────────┘  simplified invoice (credit allowance) → ICV/PIH → Fatoora
 *                                      ──outbox──► zatca.events / zatca.invoice.reported
 *                                                    │  file-cache.invoice-pdfs
 *                     file-cache-engine◄─────────────┘  render + cache the invoice PDF
 *
 * Merchant settlement (see shared/contracts/settlement.ts):
 *   wallet-ledger sweep ──► ledger.merchant-settlement.collected ──► billing (tiered fee)
 *   ──► billing.settlement-fee.calculated ──► wallet-ledger (3-leg settlement journal)
 *   ──► ledger.merchant-settlement.posted ──► zatca-tax (fee invoice + payout credit note)
 *                                         └─► file-cache (statement JSON, PENDING, 72 h)
 *   ──► zatca.settlement-documents.reported ──► file-cache (PDF/XML, statement FINAL)
 *                                           └─► wallet-ledger (batch COMPLETED)
 *
 * Driver: 'fake' (default) = the in-process @onify/fake-amqplib broker; every
 * service connects to the same broker URL. 'amqp' = a real RabbitMQ at
 * RABBITMQ_URL (PLATFORM_BROKER_DRIVER=amqp).
 */
import crypto from 'node:crypto';
import os from 'node:os';
import path from 'node:path';
import fs from 'node:fs/promises';
import type { FastifyInstance } from 'fastify';
import type { ChannelModel } from 'amqplib';

import {
  AmqpConnect,
  BrokerDriver,
  LogFn,
  ReconnectPolicy,
  ReliableConsumer,
  RetryPolicy,
  brokerDriverFromEnv,
  defaultTtlPumpIntervalMs,
  platformLogFromEnv,
  resolveAmqpConnect,
  listCheckpoints,
  type Checkpoint,
} from '../shared/messaging';
import { CONSUMERS, sarToHalalas } from '../shared/contracts';

import { buildHyperPayServer } from '../services/hyperpay-gateway/src/server';
import { startHyperPayMessaging, type HyperPayMessaging } from '../services/hyperpay-gateway/src/messaging/runtime';
import { db as hyperpayDb } from '../services/hyperpay-gateway/src/services/jsonDb';
import { outbox as hyperpayOutbox } from '../services/hyperpay-gateway/src/services/outbox';
import { OUTBOX_COLLECTION, PAYMENTS_COLLECTION, REFUNDS_COLLECTION, VAULT_COLLECTION } from '../services/hyperpay-gateway/src/services/collections';
import type { PaymentRecord } from '../services/hyperpay-gateway/src/types/hyperpay';

import { buildApp as buildBillingApp } from '../services/billing-credits/src/server';
import { query as billingQuery, resetAndSeedDatabase as resetBillingDb } from '../services/billing-credits/src/db';
import { flushRedis as flushBillingRedis } from '../services/billing-credits/src/redis';
import { startBillingIntegration, type BillingIntegration } from '../services/billing-credits/src/integration/runtime';
import { orderCreditSettlementService } from '../services/billing-credits/src/integration/OrderCreditSettlementService';

import { buildApp as buildWalletApp } from '../services/wallet-ledger/src/server';
import { resetAndSeedDatabase as resetWalletDb } from '../services/wallet-ledger/src/db';
import { flushRedis as flushWalletRedis } from '../services/wallet-ledger/src/redis';
import { createEventBus, setEventBus, type AmqpEventBus, type Subscription } from '../services/wallet-ledger/src/messaging';
import { startReconciliationWorker } from '../services/wallet-ledger/src/services/LedgerReconciliationService';
import { startWalletIntegration, type WalletIntegration } from '../services/wallet-ledger/src/integration/runtime';
import { paymentSettlementService } from '../services/wallet-ledger/src/services/PaymentSettlementService';

import { buildApp as buildZatcaApp, type AppContext as ZatcaContext } from '../services/zatca-tax/src/server';
import { resetDatabaseAndSeed as resetZatcaDb } from '../services/zatca-tax/src/db';
import { startZatcaIntegration, type ZatcaIntegration } from '../services/zatca-tax/src/integration/runtime';
import { renderReportedInvoicePdf } from '../services/zatca-tax/src/integration/invoicePdf';
import type { ZatcaPortalTransport } from '../services/zatca-tax/src/services/ReportingService';

import { createFileCache, type FileCache } from '../services/file-cache-engine/src/main';
import { buildApp as buildCoordinatorApp, type CoordinatorContext } from '../services/checkout-coordinator/src/server';
import { resetDatabase as resetCoordinatorDb } from '../services/checkout-coordinator/src/db';
import { billingHttpPort, walletHttpPort } from '../services/checkout-coordinator/src/clients/ports';
import { injectTransport, type HttpTransport } from '../services/checkout-coordinator/src/clients/http';
import {
  startCoordinatorIntegration,
  type CoordinatorIntegration,
} from '../services/checkout-coordinator/src/integration/runtime';
import type { CheckoutView, CoordinatorOptions } from '../services/checkout-coordinator/src/services/CheckoutCoordinator';
import { config as walletConfig } from '../services/wallet-ledger/src/config/env';
import { SignJWT } from 'jose';
import {
  startInvoicePdfWorker,
  type InvoicePdfWorkerRuntime,
} from '../services/file-cache-engine/src/integration/invoice-pdf-worker';
import { CacheManager } from '../services/file-cache-engine/src/cache/cache-manager';
import {
  startSettlementStatementWorker,
  type SettlementStatementRuntime,
} from '../services/file-cache-engine/src/integration/settlement-statements';
import { createSettlementHttpHandler } from '../services/file-cache-engine/src/integration/settlement-http';
import { renderInvoiceDocumentPdf } from '../services/zatca-tax/src/integration/invoicePdf';
import { settlementFeeService } from '../services/billing-credits/src/integration/SettlementFeeService';
import type { MerchantSettlementService } from '../services/wallet-ledger/src/services/MerchantSettlementService';
import { checkSettlementConsistency, traceSettlementBatch, type SettlementTrace } from './settlement-consistency';
import {
  DEFAULT_MASTER_SECRET,
  EventAuthenticator,
  Keyring,
  SERVICES,
  derivedKeyring,
  eventAuthFor,
  type EventSignatureMode,
  type ServiceId,
} from '../shared/security';
import { publishOutboxEvent } from '../services/hyperpay-gateway/src/messaging/publisher';
import { startCreditNoteWorker, type CreditNoteWorkerRuntime } from '../services/file-cache-engine/src/integration/credit-note-worker';
import { checkRefundConsistency, traceOrderRefunds, type OrderRefundTrace } from './refund-consistency';
import type { RequestListener } from 'node:http';

export interface PlatformOptions {
  driver?: BrokerDriver;
  /** Fastify request logs of the billing / wallet APIs (default: the services' own default — on outside tests). */
  httpLogs?: boolean;
  /** Broker URL. Fake driver: a fresh in-process broker per boot unless given. */
  url?: string;
  /** Retry policy of every integration consumer (attempts, jittered backoff). */
  retry?: Partial<RetryPolicy>;
  prefetch?: number;
  /** Fake broker: how often expired retry messages are moved back (0 = never). */
  ttlPumpIntervalMs?: number;
  relayPollIntervalMs?: number;
  reconnect?: Partial<ReconnectPolicy>;
  /** Reset and seed every service's database first (default true). */
  resetData?: boolean;
  cacheDirectory?: string;
  webhookSecret?: string;
  /** Fatoora gateway transport (tests inject outages / rejections). */
  zatcaTransport?: ZatcaPortalTransport;
  /** Start the wallet's invariant reconciliation worker on the platform broker (default true). */
  ledgerReconciliationWorker?: boolean;
  /**
   * Service-to-service event authentication (see shared/security). Omitted: every service uses
   * its own keyring and mode from the environment. Tests use it to run a service with the wrong
   * keys, or to watch the platform in `permissive` mode.
   */
  eventAuth?: {
    mode?: EventSignatureMode;
    /** Derive every service's keys from this master secret instead of the environment's. */
    masterSecret?: string;
    /** Replace one service's authenticator entirely (e.g. a service holding no keys). */
    overrides?: Partial<Record<ServiceId, EventAuthenticator>>;
  };
  /** Checkout coordinator tuning (reservation TTL, downstream retries, …). */
  checkout?: Partial<CoordinatorOptions>;
  /** Wrap the coordinator's HTTP transports (tests inject timeouts / outages). */
  checkoutTransports?: { billing?: (base: HttpTransport) => HttpTransport; wallet?: (base: HttpTransport) => HttpTransport };
  log?: LogFn;
}

export interface PlatformRuntime {
  driver: BrokerDriver;
  url: string;
  webhookSecret: string;
  hyperpay: { app: FastifyInstance; messaging: HyperPayMessaging };
  billing: { app: FastifyInstance; integration: BillingIntegration };
  wallet: { app: FastifyInstance; integration: WalletIntegration; eventBus: AmqpEventBus };
  zatca: { context: ZatcaContext; integration: ZatcaIntegration };
  fileCache: {
    cache: FileCache;
    pdfs: InvoicePdfWorkerRuntime;
    /** Merchant settlement statements (72 h) and settlement document PDFs / XML. */
    settlements: SettlementStatementRuntime;
    /** Customer refunds: credit note PDFs, and the credited invoice's PDF re-rendered. */
    creditNotes: CreditNoteWorkerRuntime;
    /** GET /settlements/:merchantId/:batchId, GET /documents/:taxpayerId/:invoiceId.(pdf|xml). */
    http: RequestListener;
    directory: string;
  };
  checkout: { context: CoordinatorContext; integration: CoordinatorIntegration };
  /** Merchant settlement batches (the ledger's sweep + posting service). */
  settlements: MerchantSettlementService;
  /** Every integration consumer, by queue name. */
  consumers: Record<string, ReliableConsumer<any>>;
  /** Resolves once every outbox is published, every consumer idle and no retry is pending. */
  settle(options?: { timeoutMs?: number }): Promise<void>;
  traceOrder(orderId: string): Promise<OrderTrace>;
  verifyOrderConsistency(orderId: string): Promise<ConsistencyReport>;
  /** Everything every service recorded for a merchant settlement batch. */
  traceSettlement(batchId: string): Promise<SettlementTrace>;
  /** Cross-service agreement on a settlement batch (ledger, billing, ZATCA, file cache). */
  verifySettlementConsistency(batchId: string): Promise<SettlementConsistencyReport>;
  shutdown(): Promise<void>;
}

export interface SettlementConsistencyReport {
  batchId: string;
  ok: boolean;
  status: string;
  checks: ConsistencyCheck[];
}

export interface OrderTrace {
  orderId: string;
  hyperpay: { payments: Array<PaymentRecord & { _id: string }> };
  billing: Awaited<ReturnType<typeof orderCreditSettlementService.getOrderSettlements>> & {
    /** The order's credit holds (any status). */
    holds: Array<{ holdId: string; orgId: string; status: string; estimatedCredits: number; chargedCredits: number | null }>;
  };
  wallet: Awaited<ReturnType<typeof paymentSettlementService.getOrderSettlement>>;
  zatca: Awaited<ReturnType<ZatcaContext['orderInvoicingService']['getOrderInvoicing']>>;
  /** Split-tender checkouts of the order (coordinator). */
  checkouts: CheckoutView[];
  fileCache: Array<{ invoiceId: string; key: string; byteSize: number; attributes: Record<string, string> }>;
  /** Customer refunds of the order, across services. */
  refunds: OrderRefundTrace;
  /** Every consumer checkpoint recorded for this order, across services. */
  checkpoints: Checkpoint[];
}

export interface ConsistencyCheck {
  name: string;
  ok: boolean;
  detail: string;
}

export interface ConsistencyReport {
  orderId: string;
  ok: boolean;
  outcome: 'CAPTURED' | 'RELEASED' | 'INCOMPLETE';
  checks: ConsistencyCheck[];
}

let urlSeq = 0;

/** Logger honouring PLATFORM_LOG_LEVEL (silent | error | warn | info; default warn). */
export const platformLogger = platformLogFromEnv;

/**
 * Initialise every service's embedded database once (the first PGlite start costs seconds each).
 * Test files call it in a beforeAll with a generous timeout, so no single test pays the cold start.
 */
export async function warmUpPlatform(): Promise<void> {
  const { getDb: getZatcaDb } = await import('../services/zatca-tax/src/db');
  const { getDb: getCoordinatorDb } = await import('../services/checkout-coordinator/src/db');
  const { query: walletQuery } = await import('../services/wallet-ledger/src/db');
  await Promise.all([getZatcaDb(), getCoordinatorDb(), walletQuery('SELECT 1'), billingQuery('SELECT 1')]);
}

export async function bootPlatform(options: PlatformOptions = {}): Promise<PlatformRuntime> {
  const driver = options.driver ?? brokerDriverFromEnv();
  const url =
    options.url ??
    (driver === 'fake' ? `amqp://ksa-platform-${process.pid}-${++urlSeq}` : process.env.RABBITMQ_URL ?? 'amqp://localhost:5672');
  const rawConnect = resolveAmqpConnect(driver);
  const log = options.log ?? platformLogger();
  const webhookSecret = options.webhookSecret ?? process.env.WEBHOOK_SECRET ?? 'default-webhook-key';
  const common = {
    url,
    retry: options.retry,
    prefetch: options.prefetch,
    ttlPumpIntervalMs: options.ttlPumpIntervalMs ?? defaultTtlPumpIntervalMs(driver),
    reconnect: options.reconnect,
    log,
  };
  const relay = { pollIntervalMs: options.relayPollIntervalMs ?? 1_000 };
  /** Each service's view of the event authorization matrix (its keys + the mode it enforces). */
  const authFor = (service: ServiceId): EventAuthenticator => {
    const override = options.eventAuth?.overrides?.[service];
    if (override) return override;
    if (!options.eventAuth) return eventAuthFor(service);
    return new EventAuthenticator(service, {
      mode: options.eventAuth.mode,
      keyring: new Keyring(derivedKeyring(service, options.eventAuth.masterSecret ?? DEFAULT_MASTER_SECRET)),
    });
  };

  // The fake broker forgets everything when its last connection closes (RabbitMQ keeps durable
  // queues): hold one connection for the platform's lifetime.
  let keepAlive: ChannelModel | null = null;
  if (driver === 'fake') keepAlive = await rawConnect(url);
  const connect: AmqpConnect = (u) => rawConnect(u);

  if (options.resetData ?? true) {
    await resetWalletDb();
    await flushWalletRedis();
    await resetBillingDb();
    await flushBillingRedis();
    await resetZatcaDb();
    await resetCoordinatorDb();
    for (const collection of [PAYMENTS_COLLECTION, REFUNDS_COLLECTION, OUTBOX_COLLECTION, VAULT_COLLECTION]) {
      await hyperpayDb.clear(collection);
    }
  }

  const cacheDirectory =
    options.cacheDirectory ?? (await fs.mkdtemp(path.join(os.tmpdir(), 'ksa-platform-file-cache-')));
  const ownsCacheDirectory = options.cacheDirectory === undefined;

  const stoppers: Array<() => Promise<void>> = [];
  try {
    // ── wallet-ledger ────────────────────────────────────────────────────────
    const eventBus = createEventBus({ url, connect, auth: authFor(SERVICES.ledger) });
    setEventBus(eventBus);
    await eventBus.start();
    stoppers.push(() => eventBus.close());
    let reconciliation: Subscription | null = null;
    if (options.ledgerReconciliationWorker ?? true) {
      reconciliation = await startReconciliationWorker(eventBus);
      stoppers.push(async () => {
        await reconciliation?.cancel();
      });
    }
    const walletApp = buildWalletApp({ logger: options.httpLogs });
    stoppers.push(() => walletApp.close());
    const walletIntegration = await startWalletIntegration({ ...common, connect, relay, auth: authFor(SERVICES.ledger) });
    stoppers.push(() => walletIntegration.stop());

    // ── billing-credits ──────────────────────────────────────────────────────
    const billingApp = buildBillingApp({ logger: options.httpLogs });
    stoppers.push(() => billingApp.close());
    const billingIntegration = await startBillingIntegration({ ...common, connect, relay, auth: authFor(SERVICES.billing) });
    stoppers.push(() => billingIntegration.stop());

    // ── zatca-tax ────────────────────────────────────────────────────────────
    const zatcaContext = await buildZatcaApp();
    stoppers.push(() => zatcaContext.app.close());
    if (options.zatcaTransport) zatcaContext.reportingService.setTransport(options.zatcaTransport);
    const zatcaIntegration = await startZatcaIntegration({
      ...common,
      connect,
      relay,
      auth: authFor(SERVICES.zatca),
      context: {
        invoiceService: zatcaContext.invoiceService,
        reportingService: zatcaContext.reportingService,
      },
    });
    stoppers.push(() => zatcaIntegration.stop());

    // ── file-cache-engine ────────────────────────────────────────────────────
    const fileCache = await createFileCache({
      directory: cacheDirectory,
      sweeper: false,
      tiering: { memoryBytes: 8 * 1024 * 1024 },
    });
    stoppers.push(() => fileCache.close());
    const pdfs = await startInvoicePdfWorker({
      ...common,
      connect,
      cache: fileCache.cache,
      render: renderReportedInvoicePdf,
      auth: authFor(SERVICES.fileCache),
    });
    stoppers.push(() => pdfs.stop());
    // Settlement statements: no TTL jitter — the 72 h are a retention promise.
    const statementCache = new CacheManager(fileCache.store, {
      ttlJitterRatio: 0,
      staleWhileRevalidateMs: 0,
      telemetry: fileCache.telemetry,
    });
    stoppers.push(() => statementCache.close());
    const settlementStatements = await startSettlementStatementWorker({
      ...common,
      connect,
      statementCache,
      documentCache: fileCache.cache,
      render: renderInvoiceDocumentPdf,
      lockDirectory: path.join(cacheDirectory, 'settlement-locks'),
      auth: authFor(SERVICES.fileCache),
    });
    stoppers.push(() => settlementStatements.stop());
    const creditNotes = await startCreditNoteWorker({
      ...common,
      connect,
      cache: fileCache.cache,
      render: renderInvoiceDocumentPdf,
      auth: authFor(SERVICES.fileCache),
    });
    stoppers.push(() => creditNotes.stop());
    const fileCacheHttp = createSettlementHttpHandler({
      statements: settlementStatements.statements,
      worker: settlementStatements.worker,
    });

    // ── checkout-coordinator: split-tender reservations at billing + the wallet ledger ──
    // In-process HTTP to both services (same routing, validation and auth as over the wire); the
    // ledger authenticates the coordinator as the `checkout_coordinator` service principal.
    const serviceToken = await mintWalletServiceToken();
    let wakeCheckoutRelay: () => void = () => undefined;
    const checkoutContext = await buildCoordinatorApp({
      billing: billingHttpPort((options.checkoutTransports?.billing ?? ((t) => t))(injectTransport(billingApp))),
      wallet: walletHttpPort((options.checkoutTransports?.wallet ?? ((t) => t))(injectTransport(walletApp)), () => serviceToken),
      coordinator: { ...options.checkout, onOutboxEnqueued: () => wakeCheckoutRelay() },
    });
    stoppers.push(() => checkoutContext.app.close());
    const checkoutIntegration = await startCoordinatorIntegration({
      ...common,
      connect,
      relay,
      db: checkoutContext.db,
      coordinator: checkoutContext.coordinator,
      maintenanceIntervalMs: 0,
      auth: authFor(SERVICES.checkout),
    });
    wakeCheckoutRelay = () => checkoutIntegration.relay.wake();
    stoppers.push(() => checkoutIntegration.stop());

    // ── hyperpay-gateway (last: it is the entry point) ─────────────────────────
    const hyperpayApp = buildHyperPayServer({ webhookSecret });
    stoppers.push(() => hyperpayApp.close());
    const hyperpayAuth = authFor(SERVICES.hyperpay);
    const hyperpayMessaging = await startHyperPayMessaging({
      url,
      connect,
      retryDelayMs: 50,
      maxRetryDelayMs: 2_000,
      outbox: {
        pollIntervalMs: relay.pollIntervalMs,
        retryDelayMs: 100,
        maxRetryDelayMs: 5_000,
        // Every PAYMENT_COMPLETED leaves the gateway signed with the hyperpay.payment.result key.
        publish: (channel, row, timeoutMs) => publishOutboxEvent(channel, row, timeoutMs, hyperpayAuth),
      },
    });
    stoppers.push(() => hyperpayMessaging.stop());

    const consumers: Record<string, ReliableConsumer<any>> = {
      [CONSUMERS.billingPaymentResults]: billingIntegration.consumer,
      [CONSUMERS.ledgerOrderSettlements]: walletIntegration.consumer,
      [CONSUMERS.zatcaLedgerSettlements]: zatcaIntegration.consumer,
      [CONSUMERS.fileCacheInvoicePdfs]: pdfs.consumer,
      [CONSUMERS.checkoutLedgerOutcomes]: checkoutIntegration.consumer,
      [CONSUMERS.billingSettlementFees]: billingIntegration.settlementFees,
      [CONSUMERS.ledgerSettlementFees]: walletIntegration.settlementFees,
      [CONSUMERS.zatcaMerchantSettlements]: zatcaIntegration.settlements,
      [CONSUMERS.fileCacheSettlementStatements]: settlementStatements.consumer,
      [CONSUMERS.ledgerSettlementDocuments]: walletIntegration.settlementDocuments,
      [CONSUMERS.billingRefunds]: billingIntegration.refunds,
      [CONSUMERS.ledgerOrderRefunds]: walletIntegration.orderRefunds,
      [CONSUMERS.zatcaLedgerRefunds]: zatcaIntegration.refunds,
      [CONSUMERS.fileCacheCreditNotes]: creditNotes.consumer,
    };
    const relays = [billingIntegration.relay, walletIntegration.relay, zatcaIntegration.relay, checkoutIntegration.relay];

    const settle = async ({ timeoutMs = 20_000 } = {}): Promise<void> => {
      const deadline = Date.now() + timeoutMs;
      let quietRounds = 0;
      for (;;) {
        await hyperpayMessaging.flush();
        for (const r of relays) await r.drain();
        for (const c of Object.values(consumers)) await c.waitForIdle();
        const pendingHyperpay = (await hyperpayOutbox.stats()).pending;
        let pendingRelays = 0;
        for (const r of relays) pendingRelays += (await r.stats()).pending;
        let queued = 0;
        for (const c of Object.values(consumers)) {
          const depths = await c.queueDepths();
          queued += depths.work + Object.values(depths.retry).reduce((a, b) => a + b, 0) + c.stats().inFlight;
        }
        quietRounds = pendingHyperpay + pendingRelays + queued === 0 ? quietRounds + 1 : 0;
        // Two quiet rounds in a row: nothing was in transit between two observations.
        if (quietRounds >= 2) return;
        if (Date.now() > deadline) {
          throw new Error(
            `platform did not settle within ${timeoutMs}ms (hyperpay outbox ${pendingHyperpay}, relays ${pendingRelays}, queued ${queued})`
          );
        }
        await new Promise((r) => setTimeout(r, 20));
      }
    };

    const traceOrder = async (orderId: string): Promise<OrderTrace> => {
      const payments = await hyperpayDb.findWhere<PaymentRecord & { _id: string; _createdAt: string }>(
        PAYMENTS_COLLECTION,
        (p) => p.orderId === orderId
      );
      const holdRows = await billingQuery<{ hold_id: string; org_id: string; status: string; estimated_credits: string; charged_credits: string | null }>(
        `SELECT hold_id, org_id, status, estimated_credits, charged_credits FROM credit_holds WHERE request_id = $1 ORDER BY created_at, hold_id`,
        [`order:${orderId}`]
      );
      const billing = {
        ...(await orderCreditSettlementService.getOrderSettlements(orderId)),
        holds: holdRows.rows.map((h) => ({
          holdId: h.hold_id,
          orgId: h.org_id,
          status: h.status,
          estimatedCredits: Number(h.estimated_credits),
          chargedCredits: h.charged_credits === null ? null : Number(h.charged_credits),
        })),
      };
      const wallet = await paymentSettlementService.getOrderSettlement(orderId);
      const zatca = await zatcaContext.orderInvoicingService.getOrderInvoicing(orderId);
      const cached: OrderTrace['fileCache'] = [];
      for (const invoice of zatca.invoices) {
        const hit = await pdfs.worker.get(invoice.taxpayer_id, invoice.id);
        if (hit) {
          cached.push({
            invoiceId: invoice.id,
            key: hit.key,
            byteSize: hit.pdf.byteLength,
            attributes: { ...hit.metadata.attributes },
          });
        }
      }
      return {
        orderId,
        hyperpay: { payments },
        billing,
        wallet,
        zatca,
        checkouts: await checkoutContext.coordinator.findByOrder(orderId),
        fileCache: cached,
        refunds: await traceOrderRefunds(orderId, zatca.invoices, { zatca: zatcaContext, pdfs, creditNotes }),
        checkpoints: [
          ...billing.checkpoints,
          ...wallet.checkpoints,
          ...zatca.checkpoints,
          ...(await listCheckpoints(checkoutContext.db, { correlationId: orderId })),
        ],
      };
    };

    const verifyOrderConsistency = async (orderId: string): Promise<ConsistencyReport> =>
      checkConsistency(await traceOrder(orderId), zatcaContext, pdfs);

    const traceSettlement = (batchId: string): Promise<SettlementTrace> =>
      traceSettlementBatch(batchId, {
        settlements: walletIntegration.settlements,
        fees: settlementFeeService,
        zatca: zatcaContext,
        statements: settlementStatements,
      });
    const verifySettlementConsistency = async (batchId: string): Promise<SettlementConsistencyReport> =>
      checkSettlementConsistency(await traceSettlement(batchId), zatcaContext);

    const runtime: PlatformRuntime = {
      driver,
      url,
      webhookSecret,
      hyperpay: { app: hyperpayApp, messaging: hyperpayMessaging },
      billing: { app: billingApp, integration: billingIntegration },
      wallet: { app: walletApp, integration: walletIntegration, eventBus },
      zatca: { context: zatcaContext, integration: zatcaIntegration },
      fileCache: { cache: fileCache, pdfs, settlements: settlementStatements, creditNotes, http: fileCacheHttp, directory: cacheDirectory },
      checkout: { context: checkoutContext, integration: checkoutIntegration },
      settlements: walletIntegration.settlements,
      consumers,
      settle,
      traceOrder,
      verifyOrderConsistency,
      traceSettlement,
      verifySettlementConsistency,
      shutdown: async () => {
        for (const stop of stoppers.reverse()) {
          try {
            await stop();
          } catch (err) {
            log('warn', `[platform] shutdown step failed: ${(err as Error).message}`);
          }
        }
        setEventBus(undefined);
        if (keepAlive) await keepAlive.close().catch(() => undefined);
        if (ownsCacheDirectory) await fs.rm(cacheDirectory, { recursive: true, force: true });
      },
    };
    return runtime;
  } catch (err) {
    for (const stop of stoppers.reverse()) await stop().catch(() => undefined);
    setEventBus(undefined);
    if (keepAlive) await keepAlive.close().catch(() => undefined);
    if (ownsCacheDirectory) await fs.rm(cacheDirectory, { recursive: true, force: true });
    throw err;
  }
}

/** A short-lived bearer token of the coordinator's service principal at the wallet ledger. */
async function mintWalletServiceToken(): Promise<string> {
  return new SignJWT({ roles: ['checkout_coordinator'] })
    .setProtectedHeader({ alg: 'HS256', typ: 'JWT' })
    .setIssuedAt()
    .setIssuer(walletConfig.auth.issuer)
    .setAudience(walletConfig.auth.audience)
    .setSubject('svc_checkout_coordinator')
    .setExpirationTime('12h')
    .sign(new TextEncoder().encode(walletConfig.auth.jwtSecret));
}

/**
 * Cross-service agreement for one order (eventual consensus, once settled):
 * what the customer paid (card through HyperPay, or wallet through a checkout), what billing
 * captured, what the ledger booked, what ZATCA invoiced and what the file cache holds must all
 * describe the same sale — and no reservation may be left open (no orphan hold).
 */
async function checkConsistency(
  trace: OrderTrace,
  zatca: ZatcaContext,
  pdfs: InvoicePdfWorkerRuntime
): Promise<ConsistencyReport> {
  const checks: ConsistencyCheck[] = [];
  const add = (name: string, ok: boolean, detail: string) => checks.push({ name, ok, detail });

  const openCreditHolds = trace.billing.holds.filter((h) => h.status === 'ACTIVE');
  const openLedgerHolds = trace.wallet.holds.filter((h) => h.status === 'HELD');
  const noOrphans = () =>
    add(
      'no orphan hold (credits or remainder)',
      openCreditHolds.length === 0 && openLedgerHolds.length === 0,
      `${openCreditHolds.length} active credit hold(s), ${openLedgerHolds.length} held ledger hold(s)`
    );
  const walletReleasesRefunded = () => {
    const unrefunded = trace.wallet.holds.filter((h) => h.tender === 'WALLET' && h.status === 'RELEASED' && h.reserveJournalRef && !h.releaseJournalRef);
    add('every released wallet reservation was refunded', unrefunded.length === 0, `${unrefunded.length} unrefunded`);
  };

  const cardPayment = trace.hyperpay.payments.find((p) => p.status === 'SUCCESS');
  const walletCheckout = trace.checkouts.find(
    (c) => c.payment.tender === 'WALLET' && (c.status === 'CONFIRMED' || c.status === 'COMPLETED')
  );
  const paymentId = cardPayment?.transactionId ?? walletCheckout?.payment.paymentId ?? null;

  if (!paymentId) {
    // Nothing was paid: every reservation must be released and nothing booked or invoiced.
    const failedPayments = trace.hyperpay.payments.filter((p) => p.status === 'FAILED');
    const closedCheckouts = trace.checkouts.filter((c) => ['FAILED', 'CANCELLED', 'EXPIRED', 'PAYMENT_FAILED'].includes(c.status));
    add(
      'the order was declined, refused or abandoned',
      failedPayments.length > 0 || (trace.checkouts.length > 0 && closedCheckouts.length === trace.checkouts.length),
      `${failedPayments.length} failed payment(s); checkouts ${trace.checkouts.map((c) => c.status).join(', ') || 'none'}`
    );
    add('billing captured nothing', trace.billing.settlements.every((s) => s.outcome !== 'CAPTURED') && trace.billing.holds.every((h) => h.status !== 'SETTLED'), JSON.stringify(trace.billing.settlements.map((s) => s.outcome)));
    add('ledger booked nothing', trace.wallet.settlement === null && trace.wallet.journal.length === 0, `${trace.wallet.journal.length} journal lines`);
    noOrphans();
    walletReleasesRefunded();
    add('no invoice issued', trace.zatca.invoices.length === 0, `${trace.zatca.invoices.length} invoice(s)`);
    add('no PDF cached', trace.fileCache.length === 0, `${trace.fileCache.length} PDF(s)`);
    const ok = checks.every((c) => c.ok);
    return { orderId: trace.orderId, ok, outcome: ok ? 'RELEASED' : 'INCOMPLETE', checks };
  }

  const paid = cardPayment ? sarToHalalas(cardPayment.amount) : (walletCheckout as CheckoutView).payment.amountHalalas;
  const captured = trace.billing.settlements.find((s) => s.paymentId === paymentId);
  const settlement = trace.wallet.settlement as null | {
    grossHalalas: number;
    netRevenueHalalas: number;
    vatHalalas: number;
    allowanceHalalas: number;
    journalRef: string;
  };
  const invoice = trace.zatca.invoices.find((i) => i.invoice_type_code === '388');

  add('billing settled the payment', !!captured && captured.paymentStatus === 'SUCCESS', captured ? captured.outcome : 'missing');
  add('ledger settlement exists', !!settlement, settlement ? settlement.journalRef : 'missing');
  if (settlement) {
    add('ledger gross == amount paid', settlement.grossHalalas === paid, `${settlement.grossHalalas} vs ${paid}`);
    add(
      'ledger allowance == captured credits value',
      settlement.allowanceHalalas === (captured?.allowanceHalalas ?? 0),
      `${settlement.allowanceHalalas} vs ${captured?.allowanceHalalas}`
    );
    const sum = trace.wallet.journal.reduce((s, l) => s + l.signedAmountCents, 0);
    add('journal balanced', sum === 0, `sum ${sum}`);
    add('journal has 3 legs (funding, revenue, VAT)', trace.wallet.journal.length === (settlement.vatHalalas > 0 ? 3 : 2), `${trace.wallet.journal.length} legs`);
    const captures = trace.wallet.holds.filter((h) => h.status === 'CAPTURED');
    add('exactly one capture (journal) per order', captures.length === 1, captures.map((h) => h.journalRef).join(', '));
    const credited = trace.wallet.journal.filter((l) => l.signedAmountCents > 0).reduce((s, l) => s + l.signedAmountCents, 0);
    add('revenue + VAT == amount paid (no halala gained or lost)', credited === paid && settlement.netRevenueHalalas + settlement.vatHalalas === paid, `${credited} vs ${paid}`);
  }

  const checkout = trace.checkouts.find((c) => c.status === 'COMPLETED' || c.status === 'CONFIRMED');
  if (checkout && settlement) {
    add('checkout completed', checkout.status === 'COMPLETED', checkout.status);
    add('checkout remainder == ledger gross', checkout.payment.amountHalalas === settlement.grossHalalas, `${checkout.payment.amountHalalas} vs ${settlement.grossHalalas}`);
    add('checkout promotion == ledger allowance', (checkout.promotion?.allowanceHalalas ?? 0) === settlement.allowanceHalalas, `${checkout.promotion?.allowanceHalalas ?? 0} vs ${settlement.allowanceHalalas}`);
    const hold = trace.wallet.holds.find((h) => h.holdKey === checkout.holds.remainder.key);
    add('the checkout\'s own remainder hold was captured', hold?.status === 'CAPTURED' && hold.tender === checkout.payment.tender, `${hold?.tender} ${hold?.status}`);
    if (checkout.payment.tender === 'WALLET') {
      const funding = trace.wallet.journal.find((l) => l.signedAmountCents < 0);
      add('wallet remainder paid from escrow (reserved at checkout)', funding?.walletId === 'wallet_escrow_sar' && !!hold?.reserveJournalRef, `${funding?.walletId} / ${hold?.reserveJournalRef}`);
    }
    if (checkout.promotion) {
      const settled = trace.billing.holds.find((h) => h.holdId === checkout.holds.credits.holdId);
      add('the checkout\'s credit hold was captured in full', settled?.status === 'SETTLED' && settled.chargedCredits === checkout.promotion.credits, `${settled?.status} ${settled?.chargedCredits}`);
    }
  }

  noOrphans();
  walletReleasesRefunded();
  add('one invoice issued', trace.zatca.invoices.filter((i) => i.invoice_type_code === '388').length === 1, `${trace.zatca.invoices.length} invoice(s)`);
  if (invoice && settlement) {
    add('invoice reported to Fatoora', invoice.status === 'REPORTED' || invoice.status === 'REPORTED_WITH_WARNINGS', invoice.status);
    add('invoice total == ledger gross', invoice.total_amount_halalas === settlement.grossHalalas, `${invoice.total_amount_halalas} vs ${settlement.grossHalalas}`);
    add('invoice taxable == ledger revenue', invoice.taxable_amount_halalas === settlement.netRevenueHalalas, `${invoice.taxable_amount_halalas} vs ${settlement.netRevenueHalalas}`);
    add('invoice VAT == ledger VAT payable', invoice.vat_amount_halalas === settlement.vatHalalas, `${invoice.vat_amount_halalas} vs ${settlement.vatHalalas}`);
    add('invoice allowance == credit discount', invoice.document_allowance_halalas === settlement.allowanceHalalas, `${invoice.document_allowance_halalas} vs ${settlement.allowanceHalalas}`);
    const chain = await zatca.invoiceService.verifyHashChain(invoice.taxpayer_id);
    add('ICV / PIH hash chain intact', chain.ok, chain.ok ? `${chain.length} documents` : chain.breaks.join('; '));
    const pdf = await pdfs.worker.get(invoice.taxpayer_id, invoice.id);
    const sha = pdf ? crypto.createHash('sha256').update(pdf.pdf).digest('hex') : null;
    add(
      'invoice PDF cached and intact',
      !!pdf && pdf.pdf.subarray(0, 5).toString('ascii') === '%PDF-' && pdf.metadata.attributes['x-content-sha256'] === sha,
      pdf ? `${pdf.pdf.byteLength} bytes` : 'missing'
    );
    add('cached PDF is for this invoice hash', pdf?.metadata.attributes['x-invoice-hash'] === invoice.invoice_hash, pdf?.metadata.attributes['x-invoice-hash'] ?? 'missing');
  }
  if (trace.refunds.hyperpay.length > 0 || trace.refunds.wallet.refunds.length > 0) {
    const capture = settlement && captured
      ? { grossHalalas: settlement.grossHalalas, netRevenueHalalas: settlement.netRevenueHalalas, vatHalalas: settlement.vatHalalas, capturedCredits: captured.capturedCredits }
      : null;
    checks.push(
      ...(await checkRefundConsistency(trace.refunds, capture, async (ref) => {
        const { query } = await import('../services/wallet-ledger/src/db');
        const res = await query<{ wallet_id: string; signed: string }>(
          `SELECT wallet_id, signed_amount_cents::text AS signed FROM ledger_entries WHERE transaction_ref = $1`,
          [ref]
        );
        return res.rows.map((r) => ({ walletId: r.wallet_id, signedAmountCents: Number(r.signed) }));
      }))
    );
  }
  const done = (consumer: string) => trace.checkpoints.some((c) => c.consumer === consumer && c.step === 'done');
  add(
    'every consumer checkpointed the order',
    done(CONSUMERS.billingPaymentResults) && done(CONSUMERS.ledgerOrderSettlements) && done(CONSUMERS.zatcaLedgerSettlements),
    trace.checkpoints.map((c) => `${c.consumer}:${c.step}`).join(', ')
  );
  const ok = checks.every((c) => c.ok);
  return { orderId: trace.orderId, ok, outcome: ok ? 'CAPTURED' : 'INCOMPLETE', checks };
}
