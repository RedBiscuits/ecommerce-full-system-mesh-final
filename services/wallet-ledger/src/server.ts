import Fastify, { FastifyInstance } from 'fastify';
import { walletRoutes } from './api/wallet.routes';
import { paymentRoutes } from './api/payments.routes';
import { settlementRoutes } from './api/settlements.routes';
import { installAuth } from './auth/plugin';
import { config } from './config/env';
import { getEventBus } from './messaging';
import { resetAndSeedDatabase } from './db';
import { startReconciliationWorker } from './services/LedgerReconciliationService';
import { startWalletIntegration } from './integration/runtime';
import { brokerDriverFromEnv, defaultTtlPumpIntervalMs, platformLogFromEnv, resolveAmqpConnect } from '../../../shared/messaging';

export function buildApp(options: { logger?: boolean } = {}): FastifyInstance {
  const app = Fastify({
    logger: options.logger ?? config.nodeEnv !== 'test',
  });

  // Authentication/authorization first: every route registered afterwards
  // must declare an access policy (startup error otherwise).
  installAuth(app);

  app.get('/health', { config: { access: 'public' } }, async () => ({
    status: 'ok',
    service: 'wallet-ledger-engine',
  }));
  app.register(walletRoutes);
  app.register(paymentRoutes);
  app.register(settlementRoutes());

  return app;
}

async function start() {
  let app: FastifyInstance | undefined;
  try {
    // Throws on unsafe auth configuration (missing/weak secret): fail fast.
    app = buildApp();
    const server = app;

    // The sandbox database (embedded PGlite) is in-memory and per process, so
    // the server must create and seed its own schema at boot; a separate
    // `npm run db:seed` process cannot populate it.
    if (config.db.seedOnStart) {
      await resetAndSeedDatabase();
      server.log.info('Sandbox database initialized and seeded (DB_SEED_ON_START=true)');
    }

    // Ledger reconciliation worker: consumes invariant-violation events.
    const bus = getEventBus();
    await bus.start();
    await startReconciliationWorker(bus);
    server.addHook('onClose', async () => {
      await bus.close(); // cancels consumers, drains in-flight handlers, fails pending publishes
    });

    // Platform order flow: consume order-credit settlements, publish payment settlements.
    if (process.env.INTEGRATION_ENABLED !== 'false') {
      const driver = brokerDriverFromEnv();
      const integration = await startWalletIntegration({
        url: config.eventBus.url,
        connect: resolveAmqpConnect(driver),
        ttlPumpIntervalMs: defaultTtlPumpIntervalMs(driver),
        log: platformLogFromEnv(),
      });
      server.addHook('onClose', () => integration.stop());
    }

    for (const signal of ['SIGTERM', 'SIGINT'] as const) {
      process.once(signal, () => {
        server.log.info(`${signal} received: shutting down gracefully`);
        server.close().then(
          () => process.exit(0),
          (err: unknown) => {
            server.log.error(err);
            process.exit(1);
          }
        );
      });
    }

    await server.listen({ port: config.port, host: '0.0.0.0' });
    console.log(`Wallet & Ledger Engine listening on port ${config.port}`);
  } catch (err) {
    if (app) app.log.error(err);
    else console.error(err instanceof Error ? err.message : err);
    process.exit(1);
  }
}

if (require.main === module) {
  void start();
}
