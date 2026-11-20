import type { PGlite } from '@electric-sql/pglite';
import { getDb } from '../db';
import { InvoiceService } from '../services/InvoiceService';
import { ReportingService } from '../services/ReportingService';
import { OrderInvoicingService, ZATCA_SETTLEMENT_CONSUMER } from './OrderInvoicingService';
import { SettlementInvoicingService, ZATCA_SETTLEMENT_DOCUMENTS_CONSUMER } from './SettlementInvoicingService';
import { RefundInvoicingService, ZATCA_REFUND_CONSUMER } from './RefundInvoicingService';
import {
  CONSUMERS,
  CONSUMER_BINDINGS,
  EXCHANGES,
  LedgerPaymentRefundedSchema,
  LedgerPaymentSettledSchema,
  MerchantSettlementPostedSchema,
  type LedgerPaymentRefundedEvent,
  type LedgerPaymentSettledEvent,
  type MerchantSettlementPostedEvent,
} from '../../../../shared/contracts';
import { EventAuthenticator, SERVICES, deliveryVerifier, eventAuthFor, outboxSigner } from '../../../../shared/security';
import {
  AmqpConnect,
  LogFn,
  OutboxRelay,
  OutboxRelayOptions,
  ReconnectPolicy,
  ReliableConsumer,
  RetryPolicy,
  SqlOutboxStore,
} from '../../../../shared/messaging';

export interface ZatcaIntegrationOptions {
  url: string;
  connect: AmqpConnect;
  /** Reuse the app's services (and e.g. its Fatoora transport); built from the shared DB otherwise. */
  context?: { db?: PGlite; invoiceService?: InvoiceService; reportingService?: ReportingService };
  retry?: Partial<RetryPolicy>;
  prefetch?: number;
  ttlPumpIntervalMs?: number;
  reconnect?: Partial<ReconnectPolicy>;
  relay?: Partial<Pick<OutboxRelayOptions, 'pollIntervalMs' | 'batchSize' | 'leaseMs' | 'retry' | 'publishTimeoutMs'>>;
  /** Service-to-service event authentication; default: ZATCA's own. */
  auth?: EventAuthenticator;
  log?: LogFn;
}

export interface ZatcaIntegration {
  consumer: ReliableConsumer<LedgerPaymentSettledEvent>;
  /** Merchant settlements: fee invoice + payout credit note (queue zatca.merchant-settlements). */
  settlements: ReliableConsumer<MerchantSettlementPostedEvent>;
  /** Customer refunds: credit notes against the order's invoice (queue zatca.ledger-refunds). */
  refunds: ReliableConsumer<LedgerPaymentRefundedEvent>;
  relay: OutboxRelay;
  service: OrderInvoicingService;
  settlementService: SettlementInvoicingService;
  refundService: RefundInvoicingService;
  stop(): Promise<void>;
}

/**
 * ZATCA's broker side:
 *   consumes  ledger.events / ledger.payment.settled  → queue zatca.ledger-settlements
 *             ledger.events / ledger.merchant-settlement.posted → queue zatca.merchant-settlements
 *             ledger.events / ledger.payment.refunded → queue zatca.ledger-refunds
 *   publishes zatca.events / zatca.invoice.reported | zatca.settlement-documents.reported
 *             (transactional outbox)
 */
export async function startZatcaIntegration(options: ZatcaIntegrationOptions): Promise<ZatcaIntegration> {
  const db = options.context?.db ?? (await getDb());
  const invoiceService = options.context?.invoiceService ?? new InvoiceService(db);
  const reportingService = options.context?.reportingService ?? new ReportingService(db, invoiceService);
  const service = new OrderInvoicingService(db, invoiceService, reportingService);
  const settlementService = new SettlementInvoicingService(db, invoiceService, reportingService);
  const refundService = new RefundInvoicingService(db, invoiceService, reportingService);

  const auth = options.auth ?? eventAuthFor(SERVICES.zatca);
  const relay = new OutboxRelay({
    name: 'zatca-tax',
    store: new SqlOutboxStore(db),
    url: options.url,
    connect: options.connect,
    exchanges: [EXCHANGES.zatca],
    signHeaders: outboxSigner(auth),
    reconnect: options.reconnect,
    log: options.log,
    ...options.relay,
  });

  // Ledger events share the `ledger.events` exchange with the ledger's own invariant events;
  // this queue binds the settlement key only.
  const consumer = new ReliableConsumer<LedgerPaymentSettledEvent>({
    name: ZATCA_SETTLEMENT_CONSUMER,
    bindings: CONSUMER_BINDINGS[CONSUMERS.zatcaLedgerSettlements],
    verify: deliveryVerifier(auth),
    decode: (raw) => LedgerPaymentSettledSchema.parse(raw) as LedgerPaymentSettledEvent,
    handler: async (event) => {
      const result = await service.handleLedgerPaymentSettled(event);
      relay.wake();
      return result.outcome;
    },
    url: options.url,
    connect: options.connect,
    prefetch: options.prefetch,
    retry: options.retry,
    ttlPumpIntervalMs: options.ttlPumpIntervalMs,
    reconnect: options.reconnect,
    log: options.log,
  });

  const settlements = new ReliableConsumer<MerchantSettlementPostedEvent>({
    name: ZATCA_SETTLEMENT_DOCUMENTS_CONSUMER,
    bindings: CONSUMER_BINDINGS[CONSUMERS.zatcaMerchantSettlements],
    verify: deliveryVerifier(auth),
    decode: (raw) => MerchantSettlementPostedSchema.parse(raw) as MerchantSettlementPostedEvent,
    handler: async (event) => {
      const result = await settlementService.handleSettlementPosted(event);
      relay.wake();
      return result.outcome;
    },
    url: options.url,
    connect: options.connect,
    prefetch: options.prefetch,
    retry: options.retry,
    ttlPumpIntervalMs: options.ttlPumpIntervalMs,
    reconnect: options.reconnect,
    log: options.log,
  });

  const refunds = new ReliableConsumer<LedgerPaymentRefundedEvent>({
    name: ZATCA_REFUND_CONSUMER,
    bindings: CONSUMER_BINDINGS[CONSUMERS.zatcaLedgerRefunds],
    verify: deliveryVerifier(auth),
    decode: (raw) => LedgerPaymentRefundedSchema.parse(raw) as LedgerPaymentRefundedEvent,
    handler: async (event) => {
      const result = await refundService.handleLedgerPaymentRefunded(event);
      relay.wake();
      return result.outcome;
    },
    url: options.url,
    connect: options.connect,
    prefetch: options.prefetch,
    retry: options.retry,
    ttlPumpIntervalMs: options.ttlPumpIntervalMs,
    reconnect: options.reconnect,
    log: options.log,
  });

  await relay.start();
  await consumer.start();
  await settlements.start();
  await refunds.start();

  return {
    consumer,
    settlements,
    refunds,
    relay,
    service,
    settlementService,
    refundService,
    stop: async () => {
      await refunds.close();
      await settlements.close();
      await consumer.close();
      await relay.close();
    },
  };
}
