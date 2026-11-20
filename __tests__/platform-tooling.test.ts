import { afterEach, beforeAll, describe, expect, it } from 'vitest';
import type { ConfirmChannel, ChannelModel } from 'amqplib';
import { bootstrapPlatformTopology } from '../scripts/topology-bootstrap';
import { warmUpPlatform } from '../scripts/platform';
import { bootSupervisorCluster, type PlatformClusterRuntime } from '../scripts/supervisor';
import { runClusterDoctor } from '../scripts/cluster-doctor';
import { buildCheckout, postWebhook } from './helpers/checkout';
import { resolveAmqpConnect } from '../shared/messaging';
import { SERVICES, eventAuthFor, signedPublishProperties } from '../shared/security';
import { EVENT_TYPES } from '../shared/contracts';

const FAST = {
  retry: { maxAttempts: 2, baseDelayMs: 10, maxDelayMs: 20, minDelayMs: 1 },
  ttlPumpIntervalMs: 10,
  relayPollIntervalMs: 20,
  log: () => undefined,
};

describe('platform tooling: topology bootstrap, supervisor, doctor', () => {
  // One-off cold start of the embedded databases (seconds each), outside any single test's budget.
  beforeAll(() => warmUpPlatform(), 180_000);

  let cluster: PlatformClusterRuntime | undefined;

  afterEach(async () => {
    await cluster?.shutdown();
    cluster = undefined;
  });

  it('declares every exchange, consumer queue, retry tier and DLQ without any service running', async () => {
    const report = await bootstrapPlatformTopology({ driver: 'fake', maxAttempts: 3 });
    expect(report.exchanges).toEqual(
      expect.arrayContaining(['hyperpay', 'hyperpay.dlx', 'ledger.events', 'billing.events', 'zatca.events', 'platform.requeue', 'platform.dlx'])
    );
    expect(report.queues).toEqual(
      expect.arrayContaining([
        'hyperpay.payments',
        'hyperpay.results',
        'ledger.reconciliation',
        'billing.payment-results',
        'ledger.order-settlements',
        'zatca.ledger-settlements',
        'file-cache.invoice-pdfs',
      ])
    );
    expect(report.retryQueues).toEqual(expect.arrayContaining(['zatca.ledger-settlements.retry.1', 'zatca.ledger-settlements.retry.2']));
    expect(report.retryQueues).not.toContain('zatca.ledger-settlements.retry.3');
    expect(report.deadLetterQueues).toEqual(
      expect.arrayContaining(['hyperpay.dlq', 'ledger.reconciliation.dlq', 'billing.payment-results.dlq', 'file-cache.invoice-pdfs.dlq'])
    );
    expect(report.consumers['billing.payment-results']).toEqual([
      'hyperpay (direct) / payment.result',
      'checkout.events (topic) / checkout.payment.completed',
    ]);
    expect(report.consumers['checkout.ledger-outcomes']).toEqual([
      'ledger.events (topic) / ledger.payment.settled',
      'ledger.events (topic) / ledger.payment.released',
    ]);
    expect(report.exchanges).toContain('checkout.events');
  });

  it('the supervisor boots the wired platform and the doctor reports its integration health', async () => {
    cluster = await bootSupervisorCluster(FAST);
    expect(cluster.topology.consumers['zatca.ledger-settlements']).toEqual(['ledger.events (topic) / ledger.payment.settled']);

    const checkout = buildCheckout();
    expect((await postWebhook(cluster.platform, checkout.body)).statusCode).toBe(202);
    await cluster.platform.settle();

    const healthy = await runClusterDoctor({ platform: cluster.platform });
    expect(healthy.overallStatus).toBe('HEALTHY');
    expect(healthy.integration).toMatchObject({
      problems: [],
      zatcaHashChain: { ok: true, length: 1 },
      outboxes: { 'billing-credits': { failed: 0, published: 1 }, 'wallet-ledger': { failed: 0 }, 'zatca-tax': { failed: 0 } },
    });
    expect(healthy.integration!.consumers['file-cache.invoice-pdfs'].stats.acked).toBe(1);

    // Service-to-service authentication: the doctor reports the mode and who holds which keys.
    expect(healthy.integration!.eventSecurity).toMatchObject({ mode: 'enforce', streams: 15, untrustedParked: [] });
    expect(healthy.integration!.eventSecurity.keyIds['file-cache-engine']).toEqual([
      'ledger.merchant-settlement.posted@v1',
      'zatca.credit-note.reported@v1',
      'zatca.invoice.reported@v1',
      'zatca.settlement-documents.reported@v1',
    ]);
    expect(healthy.integration!.refunds).toEqual({ refunds: 0, orders: 0, violations: [], awaitingCreditNotes: 0 });
    expect(cluster.topology.access.find((a) => a.stream === 'hyperpay.payment.result')).toEqual({
      stream: 'hyperpay.payment.result',
      issuer: 'hyperpay-gateway',
      audience: ['billing-credits'],
      keyId: 'hyperpay.payment.result@v1',
      routingKeys: ['payment.result'],
    });

    // A poison message parks in a DLQ: the doctor turns DEGRADED and says where.
    const conn: ChannelModel = await resolveAmqpConnect('fake')(cluster.platform.url);
    const ch: ConfirmChannel = await conn.createConfirmChannel();
    const poison = Buffer.from('not json');
    ch.publish(
      'zatca.events',
      'zatca.invoice.reported',
      poison,
      // Authentic (signed by ZATCA) but unusable: it parks for its body, not for its signature.
      signedPublishProperties(eventAuthFor(SERVICES.zatca), { eventType: EVENT_TYPES.invoiceReported, eventId: 'evt-poison', body: poison })
    );
    await ch.waitForConfirms();
    await conn.close();
    await cluster.platform.settle();

    const degraded = await runClusterDoctor({ platform: cluster.platform });
    expect(degraded.overallStatus).toBe('DEGRADED');
    expect(degraded.integration!.problems).toEqual(['file-cache.invoice-pdfs: 1 message(s) parked in file-cache.invoice-pdfs.dlq']);
    expect(degraded.integration!.eventSecurity.untrustedParked).toEqual([]);
  });
});
