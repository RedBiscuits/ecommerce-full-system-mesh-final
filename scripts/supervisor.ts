/**
 * Supervisor: boots every service in-process on one broker with the order
 * flow wired (see scripts/platform.ts), and optionally serves their HTTP APIs.
 *
 *   npm run cluster:supervise              boot, report health + topology, shut down
 *   npm run cluster:supervise -- --serve   keep running; HTTP on the ports of .env.example
 *   npm run platform:demo                  boot and push one signed checkout through the whole flow
 */
import http from 'node:http';
import type { FastifyInstance } from 'fastify';
import { bootPlatform, type PlatformOptions, type PlatformRuntime } from './platform';
import { bootstrapPlatformTopology, type PlatformTopologyReport } from './topology-bootstrap';
import type { FileCache } from '../services/file-cache-engine/src/main';
import type { AppContext as ZatcaContext } from '../services/zatca-tax/src/server';

export interface PlatformClusterRuntime {
  topology: PlatformTopologyReport;
  platform: PlatformRuntime;
  hyperpayApp: FastifyInstance;
  walletApp: FastifyInstance;
  billingApp: FastifyInstance;
  fileCache: FileCache;
  zatcaContext: ZatcaContext;
  shutdown: () => Promise<void>;
}

export async function bootSupervisorCluster(options: PlatformOptions = {}): Promise<PlatformClusterRuntime> {
  const platform = await bootPlatform(options);
  try {
    // Same broker: the report reflects what the running services declared.
    const topology = await bootstrapPlatformTopology({ driver: platform.driver, url: platform.url, maxAttempts: options.retry?.maxAttempts });
    return {
      topology,
      platform,
      hyperpayApp: platform.hyperpay.app,
      walletApp: platform.wallet.app,
      billingApp: platform.billing.app,
      fileCache: platform.fileCache.cache,
      zatcaContext: platform.zatca.context,
      shutdown: () => platform.shutdown(),
    };
  } catch (err) {
    await platform.shutdown();
    throw err;
  }
}

const PORTS = {
  hyperpay: Number(process.env.PAYMENT_GATEWAY_PORT ?? 3101),
  wallet: Number(process.env.WALLET_LEDGER_PORT ?? 3102),
  billing: Number(process.env.BILLING_CREDITS_PORT ?? 3103),
  zatca: Number(process.env.ZATCA_TAX_PORT ?? 3104),
  checkout: Number(process.env.CHECKOUT_COORDINATOR_PORT ?? 3105),
  fileCache: Number(process.env.FILE_CACHE_PORT ?? 3106),
};

async function serve(cluster: PlatformClusterRuntime): Promise<void> {
  await cluster.hyperpayApp.listen({ port: PORTS.hyperpay, host: '0.0.0.0' });
  await cluster.walletApp.listen({ port: PORTS.wallet, host: '0.0.0.0' });
  await cluster.billingApp.listen({ port: PORTS.billing, host: '0.0.0.0' });
  await cluster.zatcaContext.app.listen({ port: PORTS.zatca, host: '0.0.0.0' });
  await cluster.platform.checkout.context.app.listen({ port: PORTS.checkout, host: '0.0.0.0' });
  const fileCacheServer = http.createServer(cluster.platform.fileCache.http);
  await new Promise<void>((resolve) => fileCacheServer.listen(PORTS.fileCache, '0.0.0.0', resolve));
  console.log(`hyperpay-gateway  http://localhost:${PORTS.hyperpay}  (POST /api/webhook/hyperpay)`);
  console.log(`wallet-ledger     http://localhost:${PORTS.wallet}`);
  console.log(`billing-credits   http://localhost:${PORTS.billing}  (POST /credits/orders/:orderId/hold)`);
  console.log(`zatca-tax         http://localhost:${PORTS.zatca}`);
  console.log(`checkout-coord.   http://localhost:${PORTS.checkout}  (POST /checkouts — split tender)`);
  console.log(`file-cache        http://localhost:${PORTS.fileCache}  (GET /settlements/:merchantId/:batchId, /documents/:taxpayerId/:invoiceId.pdf)`);
  for (const signal of ['SIGINT', 'SIGTERM'] as const) {
    process.once(signal, () => {
      console.log(`${signal}: shutting down`);
      fileCacheServer.close();
      void cluster.shutdown().then(() => process.exit(0));
    });
  }
}

if (require.main === module) {
  bootSupervisorCluster()
    .then(async (cluster) => {
      console.log(`KSA Fintech & ZATCA cluster (6 services) booted in-process on ${cluster.platform.driver} broker ${cluster.topology.broker.url}`);
      console.log('Exchanges:', cluster.topology.exchanges.join(', '));
      console.log('Integration consumers:');
      for (const [name, bindings] of Object.entries(cluster.topology.consumers)) console.log(`  ${name}  ←  ${bindings.join(', ')}`);
      console.log('Event authorization matrix (HMAC-SHA256, one shared key per stream):');
      for (const entry of cluster.topology.access) {
        console.log(`  ${entry.stream}  ${entry.issuer} ──[${entry.keyId}]──► ${entry.audience.join(', ')}`);
      }
      if (process.argv.includes('--serve')) {
        await serve(cluster);
      } else {
        await cluster.shutdown();
      }
    })
    .catch((err) => {
      console.error('Supervisor boot failed:', err);
      process.exit(1);
    });
}
