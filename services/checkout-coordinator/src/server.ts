import Fastify, { FastifyInstance } from 'fastify';
import type { PGlite } from '@electric-sql/pglite';
import { config } from './config/env';
import { getDb } from './db';
import { CheckoutCoordinator, CoordinatorOptions } from './services/CheckoutCoordinator';
import { BillingPort, WalletPort, billingHttpPort, walletHttpPort } from './clients/ports';
import { fetchTransport } from './clients/http';
import { registerCheckoutRoutes } from './api/checkouts.routes';
import { startCoordinatorIntegration } from './integration/runtime';
import {
  brokerDriverFromEnv,
  defaultTtlPumpIntervalMs,
  platformLogFromEnv,
  resolveAmqpConnect,
} from '../../../shared/messaging';

export interface CoordinatorContext {
  app: FastifyInstance;
  db: PGlite;
  coordinator: CheckoutCoordinator;
}

export interface BuildCoordinatorOptions {
  billing?: BillingPort;
  wallet?: WalletPort;
  coordinator?: Partial<CoordinatorOptions>;
}

export function coordinatorOptionsFromConfig(overrides: Partial<CoordinatorOptions> = {}): CoordinatorOptions {
  return {
    halalasPerCredit: config.halalasPerCredit,
    reservationTtlSeconds: config.reservationTtlSeconds,
    creditHoldTtlMarginSeconds: config.creditHoldTtlMarginSeconds,
    retry: {
      maxAttempts: config.downstream.maxAttempts,
      baseDelayMs: config.downstream.baseDelayMs,
      maxDelayMs: config.downstream.maxDelayMs,
      minDelayMs: 10,
    },
    leaseMs: config.leaseMs,
    defaultMerchantId: config.defaultMerchantId,
    ...overrides,
  };
}

export async function buildApp(options: BuildCoordinatorOptions = {}): Promise<CoordinatorContext> {
  const db = await getDb();
  const billing = options.billing ?? billingHttpPort(fetchTransport(config.billingUrl, config.downstream.timeoutMs));
  const wallet = options.wallet ?? walletHttpPort(fetchTransport(config.walletUrl, config.downstream.timeoutMs), () => config.walletServiceToken);
  const coordinator = new CheckoutCoordinator(db, billing, wallet, coordinatorOptionsFromConfig(options.coordinator));
  const app = Fastify({ logger: false });
  app.get('/health', async () => ({ status: 'ok', service: 'checkout-coordinator' }));
  registerCheckoutRoutes(app, coordinator);
  return { app, db, coordinator };
}

if (require.main === module) {
  (async () => {
    const driver = brokerDriverFromEnv();
    let wake: () => void = () => undefined;
    const ctx = await buildApp({ coordinator: { onOutboxEnqueued: () => wake() } });
    if (process.env.INTEGRATION_ENABLED !== 'false') {
      const integration = await startCoordinatorIntegration({
        url: process.env.RABBITMQ_URL || 'amqp://localhost:5672',
        connect: resolveAmqpConnect(driver),
        db: ctx.db,
        coordinator: ctx.coordinator,
        ttlPumpIntervalMs: defaultTtlPumpIntervalMs(driver),
        maintenanceIntervalMs: config.maintenanceIntervalMs,
        log: platformLogFromEnv(),
      });
      wake = () => integration.relay.wake();
      ctx.app.addHook('onClose', () => integration.stop());
    }
    await ctx.app.listen({ port: config.port, host: '0.0.0.0' });
    for (const signal of ['SIGTERM', 'SIGINT'] as const) {
      process.once(signal, () => void ctx.app.close().then(() => process.exit(0)));
    }
    console.log(`checkout-coordinator listening on port ${config.port}`);
  })().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
