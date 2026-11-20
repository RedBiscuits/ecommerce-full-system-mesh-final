import { describe, it, expect } from 'vitest';
import { bootSupervisorCluster } from '../scripts/supervisor';
import { runClusterDoctor } from '../scripts/cluster-doctor';

describe('KSA Fintech & ZATCA Multi-Service Platform Supervisor & Doctor', () => {
  it('boots all 5 real services in-process and verifies health across the cluster', async () => {
    const cluster = await bootSupervisorCluster();

    expect(cluster.topology.exchanges).toContain('hyperpay');
    expect(cluster.topology.exchanges).toContain('ledger.events');
    expect(cluster.topology.queues).toContain('hyperpay.payments');
    expect(cluster.topology.queues).toContain('ledger.reconciliation');

    const hyperpayHealth = await cluster.hyperpayApp.inject({
      method: 'GET',
      url: '/health',
    });
    expect(hyperpayHealth.statusCode).toBe(200);

    const walletHealth = await cluster.walletApp.inject({
      method: 'GET',
      url: '/health',
    });
    expect(walletHealth.statusCode).toBe(200);

    const billingHealth = await cluster.billingApp.inject({
      method: 'GET',
      url: '/health',
    });
    expect(billingHealth.statusCode).toBe(200);

    const cacheHealth = cluster.fileCache.health();
    expect(cacheHealth.tiering).toBeDefined();

    const zatcaHealth = await cluster.zatcaContext.app.inject({
      method: 'GET',
      url: '/health',
    });
    expect(zatcaHealth.statusCode).toBe(200);

    await cluster.shutdown();

    const doctor = await runClusterDoctor();
    expect(doctor.overallStatus).toBe('HEALTHY');
    expect(doctor.services.hyperpayGateway.circuitState).toBe('CLOSED');
    expect(doctor.services.walletLedger.healthStatus).toBe('ok');
    expect(doctor.services.billingCredits.healthStatus).toBe('ok');
  });
});
