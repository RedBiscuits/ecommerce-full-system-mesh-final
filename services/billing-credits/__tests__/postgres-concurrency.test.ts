/**
 * Real-PostgreSQL concurrency stress test (opt-in).
 *
 * The default suite runs on embedded PGlite, which is a single connection and therefore serializes
 * whole transactions; it cannot exhibit real row-lock contention or deadlocks. This file runs the
 * services against a real PostgreSQL server through a connection pool so that transactions truly
 * interleave, then checks that:
 *   - no operation fails with anything but a business-rule rejection (a deadlock would surface as
 *     SQLSTATE 40P01, a broken invariant as a CHECK violation 23514),
 *   - every wallet invariant holds afterwards,
 *   - exactly-once effects (idempotency keys, webhook event ids, hold transitions) stay exactly-once.
 *
 * The workload includes a 3-level pooling chain (org_squad -> org_team -> org_globex), so sub-org
 * cascades, parent deductions and LIFO settlements of cascaded holds contend on the same parent
 * stripes, together with auto-recharge outbox relays and rollup compaction.
 *
 * Run with:  TEST_DATABASE_URL=postgres://localhost:5432/db npm run test:pg
 */
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { closeDb, pool, query, resetAndSeedDatabase } from '../src/db';
import { closeRedis, flushRedis } from '../src/redis';
import { autoRechargeService } from '../src/services/AutoRechargeService';
import { billingWebhookService } from '../src/services/BillingWebhookService';
import { creditLedgerService } from '../src/services/CreditLedgerService';
import { streamHoldService } from '../src/services/StreamHoldService';
import { walletLockMetrics } from '../src/services/OrgWallet';
import { orgPoolService } from '../src/services/OrgPoolService';
import { expectWalletInvariants, expireLot } from './helpers/wallet';

const PG_URL = process.env.TEST_DATABASE_URL;

const EXPECTED_REJECTIONS = new Set([
  'InsufficientCreditsError',
  'HoldStateConflictError',
  'IdempotencyKeyConflictError',
]);

/** Small deterministic PRNG so a failing interleaving mix can be reproduced by seed. */
function mulberry32(seed: number): () => number {
  let a = seed;
  return () => {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

describe.skipIf(!PG_URL)('Real PostgreSQL concurrency (row locks, no deadlocks)', () => {
  beforeAll(async () => {
    // Test files share modules (one worker): drop whatever backend is open, then the next query
    // lazily connects to real Postgres because DATABASE_URL is set.
    await closeDb();
    process.env.DATABASE_URL = PG_URL;
  });

  beforeEach(async () => {
    await resetAndSeedDatabase();
    await flushRedis();
  });

  afterAll(async () => {
    await closeDb();
    await closeRedis();
    delete process.env.DATABASE_URL;
  });

  it.each([1, 2])('survives a randomized mixed workload on shared orgs and pools (seed %i)', async (seed) => {
    const rand = mulberry32(seed);
    const pick = <T>(items: T[]): T => items[Math.floor(rand() * items.length)];
    // org_squad -> org_team -> org_globex: a 3-level pooling chain next to the standalone org_acme.
    const orgs = ['org_acme', 'org_globex', 'org_team', 'org_squad'];
    const pooled = ['org_team', 'org_squad'];

    // org_globex gets a large balance (mostly single-stripe fast paths); org_acme stays tight so
    // stripes run dry and operations escalate to FULL wallet locks mid-transaction.
    await creditLedgerService.createGrant({
      orgId: 'org_globex',
      source: 'PREPAID_INVOICE',
      credits: 20_000,
      invoiceId: 'inv_stress_org_globex',
      expiresAt: new Date(Date.now() + 86_400_000).toISOString(),
    });
    await creditLedgerService.createGrant({
      orgId: 'org_acme',
      source: 'PREPAID_INVOICE',
      credits: 3_000,
      invoiceId: 'inv_stress_org_acme',
      expiresAt: new Date(Date.now() + 86_400_000).toISOString(),
    });
    for (const [orgId, parent, credits] of [
      ['org_team', 'org_globex', 800],
      ['org_squad', 'org_team', 200],
    ] as const) {
      await creditLedgerService.createGrant({
        orgId,
        source: 'PREPAID_INVOICE',
        credits,
        expiresAt: new Date(Date.now() + 86_400_000).toISOString(),
      });
      await orgPoolService.setParent(orgId, parent);
    }
    // Keep the parent pool near its auto-recharge threshold so outbox intents and relay runs
    // interleave with everything else.
    await query(
      `UPDATE org_balances SET auto_recharge_threshold_credits = 22000, monthly_recharge_cap_credits = 12000
       WHERE org_id = 'org_globex'`
    );
    const holdIds: string[] = (
      await Promise.all(
        Array.from({ length: 30 }, (_, i) =>
          streamHoldService.createStreamHold({
            orgId: orgs[i % orgs.length],
            requestId: `req_pre_${i}`,
            modelName: 'gpt-4o',
            estimatedCredits: 50 + (i % 5) * 40,
          })
        )
      )
    ).map((h) => h.holdId);
    // Some holds and one lot lapse so sweeps/inline expiry race with everything else.
    await query(`UPDATE credit_holds SET expires_at = NOW() - INTERVAL '1 second' WHERE hold_id = ANY($1)`, [
      holdIds.slice(0, 10),
    ]);
    await expireLot('grant_promo_01');

    const ops: Array<() => Promise<unknown>> = [];
    for (let i = 0; i < 300; i++) {
      const orgId = pick(orgs);
      const roll = rand();
      if (roll < 0.08) {
        // Sub-org cascades that reach deep into the parent pools.
        const child = pick(pooled);
        ops.push(() =>
          rand() < 0.5
            ? creditLedgerService.deductUsage({
                orgId: child,
                requestId: `req_c_${i}`,
                modelName: 'gpt-4o',
                credits: 100 + Math.floor(rand() * 900),
              })
            : streamHoldService
                .createStreamHold({
                  orgId: child,
                  requestId: `req_ch_${i}`,
                  modelName: 'gpt-4o',
                  estimatedCredits: 100 + Math.floor(rand() * 900),
                })
                .then((h) => holdIds.push(h.holdId))
        );
      } else if (roll < 0.25) {
        ops.push(() =>
          creditLedgerService.deductUsage({
            orgId,
            requestId: `req_d_${i}`,
            modelName: 'gpt-4o',
            credits: 1 + Math.floor(rand() * 400),
            // A handful of shared keys -> concurrent idempotent replays.
            idempotencyKey: rand() < 0.3 ? `idem_${orgId}_${i % 5}` : undefined,
          })
        );
      } else if (roll < 0.4) {
        ops.push(() =>
          streamHoldService.createStreamHold({
            orgId,
            requestId: `req_h_${i}`,
            modelName: 'gpt-4o',
            estimatedCredits: 20 + Math.floor(rand() * 300),
          })
        );
      } else if (roll < 0.55) {
        const holdId = pick(holdIds);
        ops.push(() => streamHoldService.settleStreamHold({ holdId, actualCredits: Math.floor(rand() * 250) }));
      } else if (roll < 0.62) {
        const holdId = pick(holdIds);
        ops.push(() => streamHoldService.releaseStreamHold({ holdId }));
      } else if (roll < 0.69) {
        const holdId = pick(holdIds);
        ops.push(() =>
          streamHoldService.extendStreamHold({ holdId, additionalTtlSeconds: 30, additionalCredits: 25 })
        );
      } else if (roll < 0.76) {
        // Few distinct event ids -> heavy redelivery contention on the same event.
        ops.push(() =>
          billingWebhookService.handleStripeWebhook({
            eventId: `evt_paid_${orgId}_${i % 3}`,
            eventType: 'invoice.paid',
            orgId,
            invoiceId: `inv_paid_${orgId}_${i % 3}`,
            credits: 1_000,
          })
        );
      } else if (roll < 0.8) {
        ops.push(() =>
          billingWebhookService.handleStripeWebhook({
            eventId: `evt_refund_${i % 2}`,
            eventType: 'charge.refunded',
            orgId: 'org_acme',
            invoiceId: 'inv_initial_1000',
            credits: 1_000,
          })
        );
      } else if (roll < 0.86) {
        ops.push(() => creditLedgerService.expireStaleGrants());
      } else if (roll < 0.92) {
        ops.push(() => streamHoldService.releaseExpiredHolds());
      } else if (roll < 0.94) {
        ops.push(() =>
          rand() < 0.5 ? autoRechargeService.processOutbox() : autoRechargeService.evaluateAndTriggerIfNeeded(orgId)
        );
      } else if (roll < 0.96) {
        ops.push(() => creditLedgerService.compactUsageRollups(7));
      } else {
        ops.push(() =>
          creditLedgerService.createGrant({
            orgId,
            source: 'PROMOTIONAL',
            credits: 1_500,
            expiresAt: new Date(Date.now() + 86_400_000).toISOString(),
          })
        );
      }
    }

    const metricsBefore = { ...walletLockMetrics };
    const results = await Promise.allSettled(ops.map((op) => op()));
    await autoRechargeService.whenOutboxIdle();
    // The workload must exercise both parallel single-stripe paths and mid-transaction escalation.
    expect(walletLockMetrics.stripeFastPath - metricsBefore.stripeFastPath).toBeGreaterThan(50);
    expect(walletLockMetrics.escalatedToFull - metricsBefore.escalatedToFull).toBeGreaterThan(5);

    const unexpected = results
      .filter((r): r is PromiseRejectedResult => r.status === 'rejected')
      .map((r) => r.reason as Error & { code?: string })
      .filter((err) => !EXPECTED_REJECTIONS.has(err.name))
      .map((err) => `${err.name}${err.code ? ` [${err.code}]` : ''}: ${err.message}`);
    expect(unexpected).toEqual([]);
    // Sanity check that the workload really exercised the happy paths.
    expect(results.filter((r) => r.status === 'fulfilled').length).toBeGreaterThan(120);

    for (const orgId of orgs) {
      await expectWalletInvariants(orgId);
    }

    // Exactly-once effects.
    const dupGrants = await query<{ invoice_id: string; n: string }>(
      `SELECT invoice_id, COUNT(*)::text AS n FROM credit_grants
       WHERE invoice_id IS NOT NULL GROUP BY invoice_id HAVING COUNT(*) > 1`
    );
    expect(dupGrants.rows).toEqual([]);
    const dupKeys = await query(
      `SELECT org_id, idempotency_key FROM usage_events
       WHERE idempotency_key IS NOT NULL GROUP BY org_id, idempotency_key HAVING COUNT(*) > 1`
    );
    expect(dupKeys.rows).toEqual([]);
    const settledWithoutSingleUsage = await query(
      `SELECT h.hold_id FROM credit_holds h
       LEFT JOIN usage_events u ON u.hold_id = h.hold_id
       WHERE h.status = 'SETTLED' AND h.actual_credits > 0
       GROUP BY h.hold_id HAVING COUNT(u.usage_id) <> 1`
    );
    expect(settledWithoutSingleUsage.rows).toEqual([]);
    const refundDiscrepancies = await query<{ n: string }>(
      `SELECT COUNT(*)::text AS n FROM billing_discrepancies WHERE discrepancy_type = 'REFUND_UNRECOVERABLE'`
    );
    expect(Number(refundDiscrepancies.rows[0].n)).toBeLessThanOrEqual(1);
    const inFlightEvents = await query(`SELECT event_id FROM stripe_webhook_events WHERE outcome = 'PROCESSING'`);
    expect(inFlightEvents.rows).toEqual([]);
    // Outbox: every order came from a distinct intent (or a direct evaluation), cascades reached
    // the parent pools, and the monthly cap held.
    const orders = await query<{ n: string; intents: string; with_intent: string; added: string }>(
      `SELECT COUNT(*)::text AS n, COUNT(DISTINCT intent_id)::text AS intents,
              COUNT(intent_id)::text AS with_intent, COALESCE(SUM(credits_added), 0)::text AS added
       FROM auto_recharge_orders WHERE org_id = 'org_globex'`
    );
    expect(orders.rows[0].intents).toBe(orders.rows[0].with_intent);
    expect(Number(orders.rows[0].added)).toBeLessThanOrEqual(12000);
    const pooledUsage = await query<{ n: string }>(
      `SELECT COUNT(*)::text AS n FROM (
         SELECT funding_org_id FROM usage_rollup_deltas WHERE org_id <> funding_org_id
         UNION ALL SELECT funding_org_id FROM usage_daily_rollups WHERE org_id <> funding_org_id) x`
    );
    expect(Number(pooledUsage.rows[0].n)).toBeGreaterThan(0);
  });

  it('never deadlocks when parent deductions, child cascading holds and LIFO settlements contend on the parent pool', async () => {
    await creditLedgerService.createGrant({
      orgId: 'org_team',
      source: 'PREPAID_INVOICE',
      credits: 200,
      expiresAt: new Date(Date.now() + 86_400_000).toISOString(),
    });
    await orgPoolService.setParent('org_team', 'org_globex');
    const cascadingHold = () =>
      streamHoldService.createStreamHold({
        orgId: 'org_team',
        requestId: `req_team_${Math.random()}`,
        modelName: 'gpt-4o',
        estimatedCredits: 150,
      });

    for (let round = 0; round < 6; round++) {
      // Holds whose reservations live (partly or fully) in the parent pool.
      const holds = await Promise.all(Array.from({ length: 4 }, cascadingHold));
      const results = await Promise.allSettled([
        // LIFO settlements / releases unwinding cascaded portions on parent stripes...
        ...holds.slice(0, 3).map((h, i) => streamHoldService.settleStreamHold({ holdId: h.holdId, actualCredits: 60 + i * 50 })),
        streamHoldService.releaseStreamHold({ holdId: holds[3].holdId }),
        // ...while the parent spends from the same stripes...
        ...Array.from({ length: 4 }, (_, i) =>
          creditLedgerService.deductUsage({ orgId: 'org_globex', requestId: `req_parent_${round}_${i}`, modelName: 'gpt-4o', credits: 40 })
        ),
        // ...new children cascade into it, and org-wide parent maintenance takes FULL locks.
        cascadingHold(),
        cascadingHold(),
        creditLedgerService.deductUsage({ orgId: 'org_team', requestId: `req_team_d_${round}`, modelName: 'gpt-4o', credits: 120 }),
        creditLedgerService.createGrant({
          orgId: 'org_globex',
          source: 'PROMOTIONAL',
          credits: 50,
          expiresAt: new Date(Date.now() + 86_400_000).toISOString(),
        }),
        creditLedgerService.expireStaleGrants(),
      ]);
      const unexpected = results
        .filter((r): r is PromiseRejectedResult => r.status === 'rejected')
        .map((r) => r.reason as Error & { code?: string })
        .filter((err) => !EXPECTED_REJECTIONS.has(err.name))
        .map((err) => `${err.name}${err.code ? ` [${err.code}]` : ''}: ${err.message}`);
      expect(unexpected).toEqual([]);
    }
    await expectWalletInvariants('org_team');
    await expectWalletInvariants('org_globex');
  });

  it('never deadlocks when hold transitions race the expiry sweeps on the same hold and org', async () => {
    for (let round = 0; round < 8; round++) {
      const hold = await streamHoldService.createStreamHold({
        orgId: 'org_acme',
        requestId: `req_race_${round}`,
        modelName: 'gpt-4o',
        estimatedCredits: 100,
      });
      await query(`UPDATE credit_holds SET expires_at = NOW() - INTERVAL '1 second' WHERE hold_id = $1`, [
        hold.holdId,
      ]);
      await expireLot('grant_promo_01');

      const results = await Promise.allSettled([
        streamHoldService.settleStreamHold({ holdId: hold.holdId, actualCredits: 10 }),
        streamHoldService.releaseStreamHold({ holdId: hold.holdId }),
        streamHoldService.extendStreamHold({ holdId: hold.holdId, additionalTtlSeconds: 10 }),
        streamHoldService.releaseExpiredHolds(),
        streamHoldService.releaseExpiredHolds(),
        creditLedgerService.expireStaleGrants(),
        creditLedgerService.deductUsage({ orgId: 'org_acme', requestId: `req_race_d_${round}`, modelName: 'gpt-4o', credits: 5 }),
      ]);

      const unexpected = results
        .filter((r): r is PromiseRejectedResult => r.status === 'rejected')
        .map((r) => r.reason as Error & { code?: string })
        .filter((err) => !EXPECTED_REJECTIONS.has(err.name))
        .map((err) => `${err.name}${err.code ? ` [${err.code}]` : ''}: ${err.message}`);
      expect(unexpected).toEqual([]);
      await expectWalletInvariants('org_acme');
    }
  });

  it('keeps deductions and holds of an org flowing while another transaction holds one of its stripes', async () => {
    // Simulate a slow in-flight operation pinning stripe 0 of org_globex.
    const blocker = await pool.connect();
    try {
      await blocker.query('BEGIN');
      await blocker.query(
        `SELECT 1 FROM org_wallet_stripes WHERE org_id = 'org_globex' AND stripe_no = 0 FOR UPDATE`
      );

      const withinSeconds = <T>(promise: Promise<T>, ms: number) =>
        Promise.race([
          promise,
          new Promise<never>((_, reject) => setTimeout(() => reject(new Error(`still blocked after ${ms}ms`)), ms)),
        ]);

      // Same-org deductions and holds route around the busy stripe: one SKIP LOCKED pass finds an
      // idle stripe for every operation as long as no more operations are in flight than there
      // are idle stripes (3 here), so nothing queues behind the pinned stripe - and nothing polls.
      for (let wave = 0; wave < 4; wave++) {
        const work = await withinSeconds(
          Promise.all([
            creditLedgerService.deductUsage({ orgId: 'org_globex', requestId: `req_p_${wave}_a`, modelName: 'gpt-4o', credits: 50 }),
            creditLedgerService.deductUsage({ orgId: 'org_globex', requestId: `req_p_${wave}_b`, modelName: 'gpt-4o', credits: 50 }),
            streamHoldService.createStreamHold({ orgId: 'org_globex', requestId: `req_ph_${wave}`, modelName: 'gpt-4o', estimatedCredits: 100 }),
          ]),
          5_000
        );
        expect(work).toHaveLength(3);
      }
      const usedStripes = await query<{ stripe_no: number }>(
        `SELECT DISTINCT stripe_no FROM credit_lot_allocations
         UNION SELECT DISTINCT stripe_no FROM credit_hold_portions`
      );
      expect(usedStripes.rows.map((r) => Number(r.stripe_no))).not.toContain(0);

      // An org-wide (FULL lock) operation does wait for the pinned stripe...
      let grantDone = false;
      const grant = creditLedgerService
        .createGrant({
          orgId: 'org_globex',
          source: 'PROMOTIONAL',
          credits: 400,
          expiresAt: new Date(Date.now() + 86_400_000).toISOString(),
        })
        .then(() => {
          grantDone = true;
        });
      await new Promise((r) => setTimeout(r, 300));
      expect(grantDone).toBe(false);

      // ...and proceeds as soon as it is released.
      await blocker.query('COMMIT');
      await withinSeconds(grant, 5_000);
      expect(grantDone).toBe(true);
    } finally {
      await blocker.query('ROLLBACK').catch(() => undefined);
      blocker.release();
    }
    await expectWalletInvariants('org_globex');
  });

  it('serializes 25 concurrent settlements of the same hold into exactly one charge', async () => {
    const hold = await streamHoldService.createStreamHold({
      orgId: 'org_acme',
      requestId: 'req_same_hold',
      modelName: 'gpt-4o',
      estimatedCredits: 300,
    });

    const results = await Promise.allSettled(
      Array.from({ length: 25 }, () => streamHoldService.settleStreamHold({ holdId: hold.holdId, actualCredits: 120 }))
    );

    expect(results.filter((r) => r.status === 'fulfilled')).toHaveLength(1);
    const rejected = results.filter((r): r is PromiseRejectedResult => r.status === 'rejected');
    expect(rejected.every((r) => (r.reason as Error).name === 'HoldStateConflictError')).toBe(true);
    expect(await expectWalletInvariants('org_acme')).toMatchObject({ reserved: 0, cached: 1380 });
  });
});
