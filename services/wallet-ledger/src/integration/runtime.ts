import { query } from '../db';
import {
  LEDGER_SETTLEMENT_CONSUMER,
  PaymentSettlementService,
  paymentSettlementService,
} from '../services/PaymentSettlementService';
import {
  LEDGER_SETTLEMENT_DOCUMENTS_CONSUMER,
  LEDGER_SETTLEMENT_FEE_CONSUMER,
  MerchantSettlementService,
  merchantSettlementService,
} from '../services/MerchantSettlementService';
import { LEDGER_REFUND_CONSUMER, OrderRefundService, orderRefundService } from '../services/OrderRefundService';
import {
  CONSUMERS,
  CONSUMER_BINDINGS,
  EXCHANGES,
  OrderCreditsRestoredSchema,
  OrderCreditsSettledSchema,
  type OrderCreditsRestoredEvent,
  SettlementDocumentsReportedSchema,
  SettlementFeeCalculatedSchema,
  type OrderCreditsSettledEvent,
  type SettlementDocumentsReportedEvent,
  type SettlementFeeCalculatedEvent,
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

export interface WalletIntegrationOptions {
  url: string;
  connect: AmqpConnect;
  retry?: Partial<RetryPolicy>;
  prefetch?: number;
  ttlPumpIntervalMs?: number;
  reconnect?: Partial<ReconnectPolicy>;
  relay?: Partial<Pick<OutboxRelayOptions, 'pollIntervalMs' | 'batchSize' | 'leaseMs' | 'retry' | 'publishTimeoutMs'>>;
  service?: PaymentSettlementService;
  settlements?: MerchantSettlementService;
  refunds?: OrderRefundService;
  /** Service-to-service event authentication; default: the ledger's own. */
  auth?: EventAuthenticator;
  log?: LogFn;
}

export interface WalletIntegration {
  consumer: ReliableConsumer<OrderCreditsSettledEvent>;
  /** Merchant settlement: posts the settlement journal once billing priced the fee. */
  settlementFees: ReliableConsumer<SettlementFeeCalculatedEvent>;
  /** Merchant settlement: closes the batch once ZATCA reported its documents. */
  settlementDocuments: ReliableConsumer<SettlementDocumentsReportedEvent>;
  settlements: MerchantSettlementService;
  /** Customer refunds: posts the reversal journal (queue ledger.order-refunds). */
  orderRefunds: ReliableConsumer<OrderCreditsRestoredEvent>;
  refundService: OrderRefundService;
  relay: OutboxRelay;
  stop(): Promise<void>;
}

/**
 * The ledger's broker side for card payments (independent of the invariant / reconciliation
 * event bus, which keeps its own queues on the same `ledger.events` exchange):
 *   consumes  billing.events / billing.order-credits.settled  → queue ledger.order-settlements
 *             billing.events / billing.settlement-fee.calculated → queue ledger.settlement-fees
 *             zatca.events / zatca.settlement-documents.reported → queue ledger.settlement-documents
 *             billing.events / billing.order-credits.restored → queue ledger.order-refunds
 *   publishes ledger.events / ledger.payment.settled | ledger.payment.released
 *             ledger.events / ledger.merchant-settlement.collected | .posted  (transactional outbox)
 */
export async function startWalletIntegration(options: WalletIntegrationOptions): Promise<WalletIntegration> {
  const service = options.service ?? paymentSettlementService;
  const auth = options.auth ?? eventAuthFor(SERVICES.ledger);
  const relay = new OutboxRelay({
    name: 'wallet-ledger',
    store: new SqlOutboxStore({ query: (text, params) => query(text, params) }),
    url: options.url,
    connect: options.connect,
    exchanges: [EXCHANGES.ledger],
    signHeaders: outboxSigner(auth),
    reconnect: options.reconnect,
    log: options.log,
    ...options.relay,
  });

  const consumer = new ReliableConsumer<OrderCreditsSettledEvent>({
    name: LEDGER_SETTLEMENT_CONSUMER,
    bindings: CONSUMER_BINDINGS[CONSUMERS.ledgerOrderSettlements],
    verify: deliveryVerifier(auth),
    decode: (raw) => OrderCreditsSettledSchema.parse(raw) as OrderCreditsSettledEvent,
    handler: async (event) => {
      const result = await service.handleOrderCreditsSettled(event);
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

  const settlements = options.settlements ?? merchantSettlementService;
  settlements.setOutboxListener(() => relay.wake());
  const common = {
    url: options.url,
    connect: options.connect,
    verify: deliveryVerifier(auth),
    prefetch: options.prefetch,
    retry: options.retry,
    ttlPumpIntervalMs: options.ttlPumpIntervalMs,
    reconnect: options.reconnect,
    log: options.log,
  };
  const settlementFees = new ReliableConsumer<SettlementFeeCalculatedEvent>({
    ...common,
    name: LEDGER_SETTLEMENT_FEE_CONSUMER,
    bindings: CONSUMER_BINDINGS[CONSUMERS.ledgerSettlementFees],
    decode: (raw) => SettlementFeeCalculatedSchema.parse(raw) as SettlementFeeCalculatedEvent,
    handler: async (event) => (await settlements.handleFeeCalculated(event)).outcome,
  });
  const settlementDocuments = new ReliableConsumer<SettlementDocumentsReportedEvent>({
    ...common,
    name: LEDGER_SETTLEMENT_DOCUMENTS_CONSUMER,
    bindings: CONSUMER_BINDINGS[CONSUMERS.ledgerSettlementDocuments],
    decode: (raw) => SettlementDocumentsReportedSchema.parse(raw) as SettlementDocumentsReportedEvent,
    handler: async (event) => (await settlements.handleDocumentsReported(event)).outcome,
  });

  const refundService = options.refunds ?? orderRefundService;
  refundService.setOutboxListener(() => relay.wake());
  const orderRefunds = new ReliableConsumer<OrderCreditsRestoredEvent>({
    ...common,
    name: LEDGER_REFUND_CONSUMER,
    bindings: CONSUMER_BINDINGS[CONSUMERS.ledgerOrderRefunds],
    decode: (raw) => OrderCreditsRestoredSchema.parse(raw) as OrderCreditsRestoredEvent,
    handler: async (event) => (await refundService.handleOrderCreditsRestored(event)).outcome,
  });

  await relay.start();
  await consumer.start();
  await settlementFees.start();
  await settlementDocuments.start();
  await orderRefunds.start();

  return {
    consumer,
    settlementFees,
    settlementDocuments,
    settlements,
    orderRefunds,
    refundService,
    relay,
    stop: async () => {
      await orderRefunds.close();
      await settlementDocuments.close();
      await settlementFees.close();
      await consumer.close();
      await relay.close();
    },
  };
}
