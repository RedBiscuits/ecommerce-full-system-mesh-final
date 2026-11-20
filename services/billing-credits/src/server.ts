import Fastify, { FastifyInstance } from 'fastify';
import { creditsRoutes } from './api/credits.routes';
import { holdsRoutes } from './api/holds.routes';
import { ordersRoutes } from './api/orders.routes';
import { pricingRoutes } from './api/pricing.routes';
import { webhooksRoutes } from './api/webhooks.routes';
import { settlementFeesRoutes } from './api/settlement-fees.routes';
import { config } from './config/env';
import { resetAndSeedDatabase } from './db';
import { errorHandler } from './middleware/errorHandler';
import { maintenanceWorker } from './workers/MaintenanceWorker';
import { startBillingIntegration } from './integration/runtime';
import { brokerDriverFromEnv, defaultTtlPumpIntervalMs, platformLogFromEnv, resolveAmqpConnect } from '../../../shared/messaging';

export function buildApp(options: { logger?: boolean } = {}): FastifyInstance {
  const app = Fastify({
    logger: options.logger ?? config.nodeEnv !== 'test',
  });

  app.setErrorHandler(errorHandler);

  app.get('/health', async () => ({
    status: 'ok',
    service: 'billing-credits-service',
  }));

  app.register(creditsRoutes);
  app.register(holdsRoutes);
  app.register(ordersRoutes);
  app.register(pricingRoutes);
  app.register(webhooksRoutes);
  app.register(settlementFeesRoutes);

  return app;
}

async function start() {
  const app = buildApp();
  try {
    // The embedded PGlite sandbox is in-memory and per process (like wallet-ledger and zatca-tax,
    // the server installs and seeds its own schema); a real DATABASE_URL is left untouched.
    if (!process.env.DATABASE_URL && (process.env.DB_SEED_ON_START ?? 'true') !== 'false') {
      await resetAndSeedDatabase();
    }
    // Platform order flow: consume payment results, publish order-credit settlements.
    if (process.env.INTEGRATION_ENABLED !== 'false') {
      const driver = brokerDriverFromEnv();
      const integration = await startBillingIntegration({
        url: process.env.RABBITMQ_URL || 'amqp://localhost:5672',
        connect: resolveAmqpConnect(driver),
        ttlPumpIntervalMs: defaultTtlPumpIntervalMs(driver),
        log: platformLogFromEnv(),
      });
      app.addHook('onClose', () => integration.stop());
    }
    await app.listen({ port: config.port, host: '0.0.0.0' });
    maintenanceWorker.start();
    for (const signal of ['SIGTERM', 'SIGINT'] as const) {
      process.once(signal, () => {
        maintenanceWorker.stop();
        void app.close().then(() => process.exit(0));
      });
    }
    console.log(`Billing Credits Service listening on port ${config.port}`);
  } catch (err) {
    app.log.error(err);
    console.error(err);
    maintenanceWorker.stop();
    process.exit(1);
  }
}

if (require.main === module) {
  start();
}
