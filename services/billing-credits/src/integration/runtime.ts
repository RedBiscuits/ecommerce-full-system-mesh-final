import { query } from '../db';
import {
  OrderCreditSettlementService,
  PaymentOutcome,
  orderCreditSettlementService,
  BILLING_PAYMENT_CONSUMER,
  paymentOutcomeFromCheckout,
  paymentOutcomeFromHyperPay,
} from './OrderCreditSettlementService';
import { BILLING_SETTLEMENT_FEE_CONSUMER, SettlementFeeService, settlementFeeService } from './SettlementFeeService';
import { BILLING_REFUND_CONSUMER, OrderCreditRefundService, orderCreditRefundService } from './OrderCreditRefundService';
import {
  CONSUMERS,
  CONSUMER_BINDINGS,
  CheckoutPaymentCompletedSchema,
  MerchantSettlementCollectedSchema,
  type MerchantSettlementCollectedEvent,
  EVENT_TYPES,
  EXCHANGES,
  HyperPayPaymentCompletedSchema,
  HyperPayRefundProcessedSchema,
  type CheckoutPaymentCompletedEvent,
  type HyperPayRefundProcessed,
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

export interface BillingIntegrationOptions {
  url: string;
  connect: AmqpConnect;
  retry?: Partial<RetryPolicy>;
  prefetch?: number;
  ttlPumpIntervalMs?: number;
  reconnect?: Partial<ReconnectPolicy>;
  relay?: Partial<Pick<OutboxRelayOptions, 'pollIntervalMs' | 'batchSize' | 'leaseMs' | 'retry' | 'publishTimeoutMs'>>;
  service?: OrderCreditSettlementService;
  settlementFees?: SettlementFeeService;
  refunds?: OrderCreditRefundService;
  /** Service-to-service event authentication (signing + audience checks); default: billing's own. */
  auth?: EventAuthenticator;
  log?: LogFn;
}

export interface BillingIntegration {
  consumer: ReliableConsumer<PaymentOutcome>;
  /** Merchant settlement batches: prices the processing fee (queue billing.settlement-fees). */
  settlementFees: ReliableConsumer<MerchantSettlementCollectedEvent>;
  /** Customer refunds: restores the promotional credits (queue billing.refunds). */
  refunds: ReliableConsumer<HyperPayRefundProcessed>;
  relay: OutboxRelay;
  stop(): Promise<void>;
}

/**
 * Billing's broker side:
 *   consumes  hyperpay / payment.result        → queue billing.payment-results
 *             ledger.events / ledger.merchant-settlement.collected → queue billing.settlement-fees
 *             hyperpay / refund.processed                → queue billing.refunds
 *   publishes billing.events / billing.order-credits.settled | billing.settlement-fee.calculated
 *             (transactional outbox)
 */
export async function startBillingIntegration(options: BillingIntegrationOptions): Promise<BillingIntegration> {
  const service = options.service ?? orderCreditSettlementService;
  const auth = options.auth ?? eventAuthFor(SERVICES.billing);
  const relay = new OutboxRelay({
    name: 'billing-credits',
    store: new SqlOutboxStore({ query: (text, params) => query(text, params) }),
    url: options.url,
    connect: options.connect,
    exchanges: [EXCHANGES.billing],
    signHeaders: outboxSigner(auth),
    reconnect: options.reconnect,
    log: options.log,
    ...options.relay,
  });

  const consumer = new ReliableConsumer<PaymentOutcome>({
    name: BILLING_PAYMENT_CONSUMER,
    bindings: CONSUMER_BINDINGS[CONSUMERS.billingPaymentResults],
    verify: deliveryVerifier(auth),
    // Card payments (HyperPay) and wallet-funded split-tender checkouts, one normalized outcome.
    decode: (raw) =>
      (raw as { eventType?: unknown } | null)?.eventType === EVENT_TYPES.checkoutPaymentCompleted
        ? paymentOutcomeFromCheckout(CheckoutPaymentCompletedSchema.parse(raw) as CheckoutPaymentCompletedEvent)
        : paymentOutcomeFromHyperPay(HyperPayPaymentCompletedSchema.parse(raw)),
    handler: async (event) => {
      const result = await service.handlePaymentOutcome(event);
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

  const fees = options.settlementFees ?? settlementFeeService;
  const settlementFees = new ReliableConsumer<MerchantSettlementCollectedEvent>({
    name: BILLING_SETTLEMENT_FEE_CONSUMER,
    bindings: CONSUMER_BINDINGS[CONSUMERS.billingSettlementFees],
    verify: deliveryVerifier(auth),
    decode: (raw) => MerchantSettlementCollectedSchema.parse(raw) as MerchantSettlementCollectedEvent,
    handler: async (event) => {
      const result = await fees.handleSettlementCollected(event);
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

  const refundService = options.refunds ?? orderCreditRefundService;
  const refunds = new ReliableConsumer<HyperPayRefundProcessed>({
    name: BILLING_REFUND_CONSUMER,
    bindings: CONSUMER_BINDINGS[CONSUMERS.billingRefunds],
    verify: deliveryVerifier(auth),
    decode: (raw) => HyperPayRefundProcessedSchema.parse(raw),
    handler: async (event) => {
      const result = await refundService.handleRefundProcessed(event);
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
  await settlementFees.start();
  await refunds.start();

  return {
    consumer,
    settlementFees,
    refunds,
    relay,
    stop: async () => {
      await refunds.close();
      await settlementFees.close();
      await consumer.close();
      await relay.close();
    },
  };
}
