import type { PGlite } from '@electric-sql/pglite';
import {
  CONSUMERS,
  CONSUMER_BINDINGS,
  EVENT_TYPES,
  EXCHANGES,
  LedgerPaymentReleasedSchema,
  LedgerPaymentSettledSchema,
  type IntegrationEvent,
  type LedgerPaymentReleasedPayload,
  type LedgerPaymentSettledPayload,
} from '../../../../shared/contracts';
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
import { EventAuthenticator, SERVICES, deliveryVerifier, eventAuthFor, outboxSigner } from '../../../../shared/security';
import { CheckoutCoordinator } from '../services/CheckoutCoordinator';

type LedgerOutcome = IntegrationEvent<string, LedgerPaymentSettledPayload | LedgerPaymentReleasedPayload>;

export interface CoordinatorIntegrationOptions {
  url: string;
  connect: AmqpConnect;
  db: PGlite;
  coordinator: CheckoutCoordinator;
  retry?: Partial<RetryPolicy>;
  prefetch?: number;
  ttlPumpIntervalMs?: number;
  reconnect?: Partial<ReconnectPolicy>;
  relay?: Partial<Pick<OutboxRelayOptions, 'pollIntervalMs' | 'batchSize' | 'leaseMs' | 'retry' | 'publishTimeoutMs'>>;
  /** Expiry + crash-recovery sweep interval; 0 disables the timer. */
  maintenanceIntervalMs?: number;
  /** Service-to-service event authentication; default: the coordinator's own. */
  auth?: EventAuthenticator;
  log?: LogFn;
}

export interface CoordinatorIntegration {
  relay: OutboxRelay;
  consumer: ReliableConsumer<LedgerOutcome>;
  stop(): Promise<void>;
}

/**
 * The coordinator's broker side:
 *   publishes checkout.events / checkout.payment.completed   (transactional outbox)
 *   consumes  ledger.events / ledger.payment.settled | released  → queue checkout.ledger-outcomes
 * plus the expiry / recovery sweep.
 */
export async function startCoordinatorIntegration(options: CoordinatorIntegrationOptions): Promise<CoordinatorIntegration> {
  const auth = options.auth ?? eventAuthFor(SERVICES.checkout);
  const relay = new OutboxRelay({
    name: 'checkout-coordinator',
    store: new SqlOutboxStore(options.db),
    url: options.url,
    connect: options.connect,
    exchanges: [EXCHANGES.checkout],
    signHeaders: outboxSigner(auth),
    reconnect: options.reconnect,
    log: options.log,
    ...options.relay,
  });
  const consumer = new ReliableConsumer<LedgerOutcome>({
    name: CONSUMERS.checkoutLedgerOutcomes,
    bindings: CONSUMER_BINDINGS[CONSUMERS.checkoutLedgerOutcomes],
    verify: deliveryVerifier(auth),
    decode: (raw) =>
      ((raw as { eventType?: unknown } | null)?.eventType === EVENT_TYPES.ledgerPaymentReleased
        ? LedgerPaymentReleasedSchema.parse(raw)
        : LedgerPaymentSettledSchema.parse(raw)) as LedgerOutcome,
    handler: (event) => options.coordinator.handleLedgerOutcome(event),
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

  let timer: NodeJS.Timeout | null = null;
  let sweeping = false;
  if ((options.maintenanceIntervalMs ?? 0) > 0) {
    timer = setInterval(() => {
      if (sweeping) return;
      sweeping = true;
      void (async () => {
        try {
          await options.coordinator.expireDue();
          await options.coordinator.recoverStuck(1_000);
        } catch (err) {
          options.log?.('error', `[checkout-coordinator] maintenance sweep failed: ${(err as Error).message}`);
        } finally {
          sweeping = false;
        }
      })();
    }, options.maintenanceIntervalMs);
    timer.unref();
  }

  return {
    relay,
    consumer,
    stop: async () => {
      if (timer) clearInterval(timer);
      await consumer.close();
      await relay.close();
    },
  };
}
