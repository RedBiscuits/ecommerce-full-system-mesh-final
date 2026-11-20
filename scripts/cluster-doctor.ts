import os from 'node:os';
import path from 'node:path';
import fs from 'node:fs/promises';
import { outbox } from '../services/hyperpay-gateway/src/services/outbox';
import { gatewayBreaker } from '../services/hyperpay-gateway/src/services/gatewayBreaker';
import { buildApp as buildWalletApp } from '../services/wallet-ledger/src/server';
import { buildApp as buildBillingApp } from '../services/billing-credits/src/server';
import { createFileCache } from '../services/file-cache-engine/src/main';
import { resetDatabaseAndSeed } from '../services/zatca-tax/src/db';
import { buildApp as buildZatcaApp } from '../services/zatca-tax/src/server';
import { EVENT_STREAMS, SERVICES, eventAuthFor, signatureModeFromEnv } from '../shared/security';
import { refundInvariants } from './refund-consistency';
import type { PlatformRuntime } from './platform';
import type { QueueDepths } from '../shared/messaging';

export interface IntegrationDiagnostic {
  broker: { driver: string; url: string };
  /** Service-to-service event authentication: mode, streams, and anything refused as untrusted. */
  eventSecurity: {
    mode: string;
    streams: number;
    /** Per service: the key ids it holds (its own streams + the ones addressed to it). */
    keyIds: Record<string, string[]>;
    /** Parked deliveries that failed authentication (forged, unsigned, wrong audience, …). */
    untrustedParked: Array<{ consumer: string; messageId?: string; reason?: string }>;
  };
  /** Per integration consumer: queue depths (work / retry tiers / DLQ) and delivery outcomes. */
  consumers: Record<string, { depths: QueueDepths; stats: Record<string, number | boolean> }>;
  /** Per producing service: transactional outbox backlog and parked rows. */
  outboxes: Record<string, { pending: number; published: number; failed: number }>;
  zatcaHashChain: { ok: boolean; length: number; breaks: string[] };
  /**
   * Customer refund invariants across the ledger, billing and ZATCA: refunded totals never beyond
   * the capture and equal to the refund records, every reversal balanced and recorded, restored
   * credits never beyond the captured ones, one credit note per refund of the same amount, and
   * Σ credit notes never beyond the invoice. `awaitingCreditNotes` refunds are still in flight.
   */
  refunds: { refunds: number; orders: number; violations: string[]; awaitingCreditNotes: number };
  /** Anything that needs an operator: parked (DLQ) messages, FAILED outbox rows, a broken chain. */
  problems: string[];
}

export interface ClusterDoctorDiagnostic {
  overallStatus: 'HEALTHY' | 'DEGRADED';
  /** Present when the doctor examines a running platform (`runClusterDoctor({ platform })`). */
  integration?: IntegrationDiagnostic;
  services: {
    hyperpayGateway: { circuitState: string; outboxPending: number; outboxFailed: number };
    walletLedger: { healthStatus: string };
    billingCredits: { healthStatus: string };
    fileCacheEngine: { sweeperRunning: boolean; l1Hits: number };
    zatcaTax: { complianceLastIcv: number; issuedSalesHalalas: number };
  };
}

export async function diagnoseIntegration(platform: PlatformRuntime): Promise<IntegrationDiagnostic> {
  const problems: string[] = [];
  const consumers: IntegrationDiagnostic['consumers'] = {};
  const untrustedParked: IntegrationDiagnostic['eventSecurity']['untrustedParked'] = [];
  for (const [name, consumer] of Object.entries(platform.consumers)) {
    const depths = await consumer.queueDepths();
    consumers[name] = { depths, stats: consumer.stats() };
    if (depths.deadLetter > 0) {
      problems.push(`${name}: ${depths.deadLetter} message(s) parked in ${name}.dlq`);
      for (const parked of await consumer.inspectDeadLetters({ limit: 50 })) {
        if (parked.reason?.startsWith('UNTRUSTED_EVENT')) {
          untrustedParked.push({ consumer: name, messageId: parked.messageId, reason: parked.reason });
        }
      }
    }
  }
  if (untrustedParked.length > 0) {
    problems.push(`event authentication: ${untrustedParked.length} delivery(ies) refused (forged, unsigned or wrong audience)`);
  }
  const hyperpay = await outbox.stats();
  const outboxes: IntegrationDiagnostic['outboxes'] = {
    'hyperpay-gateway': { pending: hyperpay.pending, published: hyperpay.published, failed: hyperpay.failed },
    'billing-credits': await platform.billing.integration.relay.stats(),
    'wallet-ledger': await platform.wallet.integration.relay.stats(),
    'zatca-tax': await platform.zatca.integration.relay.stats(),
    'checkout-coordinator': await platform.checkout.integration.relay.stats(),
  };
  for (const [service, stats] of Object.entries(outboxes)) {
    if (stats.failed > 0) problems.push(`${service}: ${stats.failed} outbox event(s) parked as FAILED`);
  }
  const taxpayers = await platform.zatca.context.invoiceService.listTaxpayers();
  const breaks: string[] = [];
  let length = 0;
  for (const taxpayer of taxpayers) {
    const chain = await platform.zatca.context.invoiceService.verifyHashChain(taxpayer.id);
    length += chain.length;
    breaks.push(...chain.breaks.map((b) => `${taxpayer.id}: ${b}`));
  }
  if (breaks.length > 0) problems.push(`ZATCA hash chain broken (${breaks.length} break(s))`);
  const refunds = await refundInvariants(platform.zatca.context);
  for (const violation of refunds.violations) problems.push(`refund invariant violated — ${violation}`);
  const stuck = await platform.checkout.context.db.query<{ n: number }>(
    `SELECT COUNT(*)::int AS n FROM checkouts WHERE status IN ('RESERVING', 'COMPENSATING') AND updated_at < NOW() - INTERVAL '1 minute'`
  );
  if (stuck.rows[0].n > 0) problems.push(`checkout-coordinator: ${stuck.rows[0].n} checkout(s) stuck reserving / compensating`);
  const { query: walletQuery } = await import('../services/wallet-ledger/src/db');
  const sweeps = await walletQuery<{ n: number }>(
    `SELECT COUNT(*)::int AS n FROM merchant_settlement_batches WHERE status = 'COLLECTING' AND updated_at < NOW() - INTERVAL '1 minute'`
  );
  if (sweeps.rows[0].n > 0) {
    problems.push(`wallet-ledger: ${sweeps.rows[0].n} settlement batch(es) stuck collecting (resume: POST /settlements/maintenance)`);
  }
  return {
    broker: { driver: platform.driver, url: platform.url },
    eventSecurity: {
      mode: signatureModeFromEnv(),
      streams: Object.keys(EVENT_STREAMS).length,
      keyIds: Object.fromEntries(Object.values(SERVICES).map((service) => [service, eventAuthFor(service).keyring.keyIds])),
      untrustedParked,
    },
    consumers,
    outboxes: Object.fromEntries(
      Object.entries(outboxes).map(([k, v]) => [k, { pending: v.pending, published: v.published, failed: v.failed }])
    ),
    zatcaHashChain: { ok: breaks.length === 0, length, breaks },
    refunds,
    problems,
  };
}

export async function runClusterDoctor(options: { platform?: PlatformRuntime } = {}): Promise<ClusterDoctorDiagnostic> {
  const outboxStats = await outbox.stats();
  const breakerMetrics = gatewayBreaker.getMetrics();

  const walletApp = buildWalletApp();
  const walletHealth = await walletApp.inject({ method: 'GET', url: '/health' });
  const walletBody = JSON.parse(walletHealth.body) as { status: string };
  await walletApp.close();

  const billingApp = buildBillingApp();
  const billingHealth = await billingApp.inject({ method: 'GET', url: '/health' });
  const billingBody = JSON.parse(billingHealth.body) as { status: string };
  await billingApp.close();

  const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), 'ksa-file-cache-'));
  const fileCache = await createFileCache({
    directory: tmpDir,
    sweeper: false,
    tiering: { memoryBytes: 1024 * 1024 },
  });
  const cacheHealth = fileCache.health();
  await fileCache.close();
  await fs.rm(tmpDir, { recursive: true, force: true });

  const zatcaCtx = await buildZatcaApp();
  const taxpayers = await zatcaCtx.invoiceService.listTaxpayers();
  if (taxpayers.length === 0) {
    await resetDatabaseAndSeed();
  }
  const zatcaSummary = await zatcaCtx.invoiceService.getTaxpayerComplianceSummary('txp_riyadh_retail');
  await zatcaCtx.app.close();

  const integration = options.platform ? await diagnoseIntegration(options.platform) : undefined;

  const overallStatus =
    outboxStats.failed === 0 &&
    breakerMetrics.state === 'CLOSED' &&
    walletBody.status === 'ok' &&
    billingBody.status === 'ok' &&
    (integration?.problems.length ?? 0) === 0
      ? 'HEALTHY'
      : 'DEGRADED';

  return {
    overallStatus,
    ...(integration ? { integration } : {}),
    services: {
      hyperpayGateway: {
        circuitState: breakerMetrics.state,
        outboxPending: outboxStats.pending,
        outboxFailed: outboxStats.failed,
      },
      walletLedger: {
        healthStatus: walletBody.status,
      },
      billingCredits: {
        healthStatus: billingBody.status,
      },
      fileCacheEngine: {
        sweeperRunning: cacheHealth.sweeperRunning,
        l1Hits: cacheHealth.tiering?.l1Hits ?? 0,
      },
      zatcaTax: {
        complianceLastIcv: zatcaSummary.lastIcv,
        issuedSalesHalalas: zatcaSummary.totalsHalalas.issuedSalesHalalas,
      },
    },
  };
}

if (require.main === module) {
  runClusterDoctor()
    .then((diag) => {
      console.log('=== KSA FINTECH & ZATCA CLUSTER DOCTOR ===');
      console.log(JSON.stringify(diag, null, 2));
    })
    .catch((err) => {
      console.error('Cluster Doctor failed:', err);
      process.exit(1);
    });
}
