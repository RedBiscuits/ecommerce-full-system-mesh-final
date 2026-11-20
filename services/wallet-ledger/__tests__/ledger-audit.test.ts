import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import type { FastifyInstance } from 'fastify';
import { buildApp } from '../src/server';
import { closeDb, pool, query, resetAndSeedDatabase } from '../src/db';
import { closeRedis, flushRedis } from '../src/redis';
import { walletService } from '../src/services/WalletService';
import { ledgerAuditService } from '../src/services/LedgerAuditService';
import {
  ledgerReconciliationService,
  startReconciliationWorker,
} from '../src/services/LedgerReconciliationService';
import {
  DeliverySettled,
  EventBusOptions,
  LEDGER_EVENT_TYPES,
  PublishRejectedError,
  Subscription,
  TOPOLOGY,
  createEnvelope,
  setEventBus,
} from '../src/messaging';
import {
  BrokerHarness,
  createBrokerHarness,
  drainDeadLetters,
  queueDepth,
  simulateBrokerTtlExpiry,
} from './helpers/broker';
import { collectEvents, deferred, waitForEvent } from './helpers/events';
import { actAs } from './helpers/auth';
import { SERVICES, eventAuthFor, signedPublishProperties } from '../../../shared/security';

const MAX_ATTEMPTS = 3;

/** Simulate an out-of-band corruption (e.g. an ops script run with triggers off). */
async function withIntegrityTriggersDisabled(fn: () => Promise<void>): Promise<void> {
  await query('ALTER TABLE wallets DISABLE TRIGGER USER');
  await query('ALTER TABLE ledger_entries DISABLE TRIGGER USER');
  try {
    await fn();
  } finally {
    await query('ALTER TABLE wallets ENABLE TRIGGER USER');
    await query('ALTER TABLE ledger_entries ENABLE TRIGGER USER');
  }
}

async function driftWallet(walletId: string, newBalance: number): Promise<void> {
  await withIntegrityTriggersDisabled(async () => {
    await query(`UPDATE wallets SET balance_cents = $1 WHERE wallet_id = $2`, [newBalance, walletId]);
  });
}

async function ledgerSumFor(walletId: string): Promise<number> {
  const res = await query<{ total: string }>(
    `SELECT COALESCE(SUM(signed_amount_cents), 0)::text AS total FROM ledger_entries WHERE wallet_id = $1`,
    [walletId]
  );
  return Number(res.rows[0].total);
}

async function balanceOf(walletId: string): Promise<number> {
  return (await walletService.getWallet(walletId))?.balanceCents ?? NaN;
}

async function reconciliationLogCount(walletId?: string): Promise<number> {
  const res = walletId
    ? await query(`SELECT 1 FROM reconciliation_log WHERE wallet_id = $1`, [walletId])
    : await query(`SELECT 1 FROM reconciliation_log`);
  return res.rows.length;
}

const settled = (h: BrokerHarness, count: number, predicate: (e: DeliverySettled) => boolean = () => true) =>
  collectEvents<DeliverySettled>(h.bus, 'delivery-settled', count, predicate);

describe('Ledger integrity audit + RabbitMQ reconciliation pipeline', () => {
  let app: FastifyInstance;
  let h: BrokerHarness;
  let worker: Subscription | undefined;

  async function recreateHarness(overrides: Partial<EventBusOptions>): Promise<void> {
    await h.close();
    h = await createBrokerHarness({ maxAttempts: MAX_ATTEMPTS, ...overrides });
  }

  beforeAll(async () => {
    app = buildApp();
    await app.ready();
  });

  beforeEach(async () => {
    await resetAndSeedDatabase();
    await flushRedis();
    h = await createBrokerHarness({ maxAttempts: MAX_ATTEMPTS });
    worker = undefined;
  });

  afterEach(async () => {
    vi.restoreAllMocks();
    await worker?.cancel();
    await h.close();
    setEventBus(undefined);
  });

  afterAll(async () => {
    await app.close();
    await closeDb();
    await closeRedis();
  });

  describe('audit verification', () => {
    it('reports a clean ledger after real multi-currency activity and publishes nothing', async () => {
      const api = actAs(app);
      const transfers = [
        { fromWalletId: 'wallet_alice', toWalletId: 'wallet_carol', amountCents: 1500 },
        { fromWalletId: 'wallet_dave', toWalletId: 'wallet_bob', amountCents: 2000 },
        { fromWalletId: 'wallet_bob', toWalletId: 'wallet_alice', amountCents: 700 },
      ];
      for (const payload of transfers) {
        const res = await api.inject({ method: 'POST', url: '/wallets/transfer', payload });
        expect(res.statusCode).toBe(201);
      }
      await api.inject({
        method: 'POST',
        url: '/wallets/withdraw',
        payload: { walletId: 'wallet_carol', amountCents: 900, bankAccountRef: 'iban' },
      });

      const report = await ledgerAuditService.runAudit({ bus: h.bus });

      expect(report.ok).toBe(true);
      expect(report.checks.globalZeroSum.violations).toBe(0);
      expect(report.checks.unbalancedJournals.violations).toBe(0);
      expect(report.checks.walletDrift.violations).toBe(0);
      expect(report.checks.negativeBalances.violations).toBe(0);
      expect(report.events).toEqual({ published: 0, unpublished: 0, backpressureWaits: 0 });
      expect(report.completed).toBe(true);
      expect(await queueDepth(h, TOPOLOGY.workQueue)).toBe(0);
    });

    it('detects per-wallet drift and publishes one event per violation', async () => {
      await driftWallet('wallet_alice', 7777);

      const report = await ledgerAuditService.runAudit({ bus: h.bus });

      expect(report.ok).toBe(false);
      expect(report.checks.walletDrift).toEqual({
        violations: 1,
        sample: [
          {
            walletId: 'wallet_alice',
            currency: 'USD',
            accountType: 'USER',
            balanceCents: '7777',
            ledgerBalanceCents: '10000', // precalculated total is intact...
            ledgerVersion: '1',
            ledgerSumCents: '10000', // ...and agrees with the full recount
            ledgerEntryCount: '1',
          },
        ],
      });
      expect(report.checks.globalZeroSum.violations).toBe(0);
      expect(report.checks.unbalancedJournals.violations).toBe(0);
      expect(report.events).toEqual({ published: 1, unpublished: 0, backpressureWaits: 0 });
      expect(await queueDepth(h, TOPOLOGY.workQueue)).toBe(1);
    });

    it('detects global zero-sum, unbalanced journal and negative-balance violations', async () => {
      await withIntegrityTriggersDisabled(async () => {
        // Minted money: an unbalanced USD credit to alice (balance moved with it).
        await query(
          `INSERT INTO ledger_entries (entry_id, transaction_ref, wallet_id, currency, direction, signed_amount_cents)
           VALUES ('le_mint', 'rogue_mint', 'wallet_alice', 'USD', 'CREDIT', 500)`
        );
        await query(`UPDATE wallets SET balance_cents = balance_cents + 500 WHERE wallet_id = 'wallet_alice'`);
        // A balanced journal that drives the EUR fee treasury negative.
        await query(
          `INSERT INTO ledger_entries (entry_id, transaction_ref, wallet_id, currency, direction, signed_amount_cents)
           VALUES ('le_neg_a', 'rogue_fee_refund', 'wallet_fee_treasury_eur', 'EUR', 'DEBIT', -300),
                  ('le_neg_b', 'rogue_fee_refund', 'wallet_fx_pool_eur', 'EUR', 'CREDIT', 300)`
        );
      });

      const report = await ledgerAuditService.runAudit({ publish: false });

      expect(report.checks.globalZeroSum.sample).toEqual([{ currency: 'USD', totalCents: '500' }]);
      expect(report.checks.unbalancedJournals.sample).toEqual([
        { transactionRef: 'rogue_mint', currency: 'USD', totalCents: '500' },
      ]);
      expect(report.checks.negativeBalances.sample).toEqual([
        { walletId: 'wallet_fee_treasury_eur', currency: 'EUR', accountType: 'FEE_TREASURY', ledgerSumCents: '-300' },
      ]);
      // The rogue rows bypassed the triggers, so the O(1) caches (alice's running
      // total; the sharded accounts' buckets) were never updated: caught too.
      expect(
        report.checks.walletDrift.sample.map((d) => [d.walletId, d.ledgerBalanceCents, d.ledgerSumCents])
      ).toEqual([
        ['wallet_alice', '10000', '10500'],
        ['wallet_fee_treasury_eur', '0', '-300'],
        ['wallet_fx_pool_eur', '0', '300'],
      ]);
      expect(report.events).toEqual({ published: 0, unpublished: 0, backpressureWaits: 0 });
    });

    it('pages with keyset LIMIT queries and caps the report sample (bounded memory)', async () => {
      const count = 25;
      await query(
        `INSERT INTO wallets (wallet_id, owner_id, currency)
         SELECT 'wallet_bulk_' || lpad(i::text, 3, '0'), 'user_bulk', 'USD'
         FROM generate_series(1, ${count}) AS i`
      );
      await withIntegrityTriggersDisabled(async () => {
        await query(`UPDATE wallets SET balance_cents = 42 WHERE wallet_id LIKE 'wallet_bulk_%'`);
      });

      const seen: string[] = [];
      const original = pool.query.bind(pool);
      vi.spyOn(pool, 'query').mockImplementation((text, params) => {
        seen.push(String(text).replace(/\s+/g, ' '));
        return original(text, params);
      });

      const report = await ledgerAuditService.runAudit({ bus: h.bus, pageSize: 4, sampleSize: 5 });

      expect(report.checks.walletDrift.violations).toBe(count);
      expect(report.checks.walletDrift.sample).toHaveLength(5);
      expect(report.events.published).toBe(count);

      const driftQueries = seen.filter((s) => s.includes('w.ledger_version <> l.cnt'));
      expect(driftQueries).toHaveLength(Math.ceil(count / 4)); // 7 pages of <= 4 rows
      for (const sql of seen.filter((s) => !s.includes('GROUP BY currency HAVING'))) {
        expect(sql).toMatch(/LIMIT \$\d/);
      }
    });

    it('waits for the worker to drain a full queue between pages instead of dropping events', async () => {
      await recreateHarness({ maxQueueLength: 2, prefetch: 1 });

      const count = 9;
      await query(
        `INSERT INTO wallets (wallet_id, owner_id, currency)
         SELECT 'wallet_bp_' || lpad(i::text, 2, '0'), 'user_bp', 'USD' FROM generate_series(1, ${count}) AS i`
      );
      await withIntegrityTriggersDisabled(async () => {
        await query(`UPDATE wallets SET balance_cents = 3 WHERE wallet_id LIKE 'wallet_bp_%'`);
      });
      const done = settled(h, count);
      worker = await startReconciliationWorker(h.bus);

      const report = await ledgerAuditService.runAudit({ bus: h.bus, pageSize: 4 });

      expect(report.completed).toBe(true);
      expect(report.checks.walletDrift.violations).toBe(count);
      expect(report.events.published).toBe(count); // every event delivered...
      expect(report.events.unpublished).toBe(0); // ...none dropped
      expect(report.events.backpressureWaits).toBeGreaterThan(0); // it really had to wait

      expect((await done).every((e) => e.outcome === 'acked')).toBe(true);
      expect(await reconciliationLogCount()).toBe(count);
      expect(await drainDeadLetters(h)).toEqual([]); // nothing evicted or lost
      expect((await ledgerAuditService.runAudit({ publish: false })).ok).toBe(true);
    });

    it('re-publishes events the broker NACKs when another publisher races it to the last slot', async () => {
      const cap = 2;
      const prefetch = 1;
      await recreateHarness({ maxQueueLength: cap, prefetch });

      const count = 8;
      await query(
        `INSERT INTO wallets (wallet_id, owner_id, currency)
         SELECT 'wallet_race_' || lpad(i::text, 2, '0'), 'user_race', 'USD' FROM generate_series(1, ${count}) AS i`
      );
      await withIntegrityTriggersDisabled(async () => {
        await query(`UPDATE wallets SET balance_cents = 3 WHERE wallet_id LIKE 'wallet_race_%'`);
      });

      // Stale headroom: the audit always believes the queue has room, as it
      // would if a concurrent publisher filled the queue right after the check.
      vi.spyOn(h.bus, 'workQueueHeadroom').mockResolvedValue(cap);

      // Faithful x-overflow=reject-publish (RabbitMQ): a publish into a full
      // queue is NACKed and NOT enqueued. The fake instead evicts the oldest
      // message (and counts unacked deliveries toward the cap), so the refusal
      // is modelled here at the fake's effective capacity (cap - prefetch).
      const firstNack = deferred();
      const realPublish = h.bus.publish.bind(h.bus);
      let brokerNacks = 0;
      vi.spyOn(h.bus, 'publish').mockImplementation(async (envelope, opts) => {
        if ((await queueDepth(h, TOPOLOGY.workQueue)) >= cap - prefetch) {
          brokerNacks++;
          firstNack.resolve();
          throw new PublishRejectedError(`${TOPOLOGY.exchange}/${envelope.type}`, new Error('message nacked'));
        }
        return realPublish(envelope, opts);
      });

      // Gate the consumer until the broker has really NACKed a publish, so the
      // queue is provably full at that moment (deterministic; no sleeping).
      const original = ledgerReconciliationService.reconcileWalletDrift.bind(ledgerReconciliationService);
      vi.spyOn(ledgerReconciliationService, 'reconcileWalletDrift').mockImplementation(async (env) => {
        await firstNack.promise;
        return original(env);
      });
      const done = settled(h, count);
      worker = await startReconciliationWorker(h.bus);

      const report = await ledgerAuditService.runAudit({ bus: h.bus, pageSize: 4 });

      expect(brokerNacks).toBeGreaterThan(0); // the broker really refused publishes...
      expect(report.completed).toBe(true);
      expect(report.events).toMatchObject({ published: count, unpublished: 0 }); // ...and none were lost
      await done;
      expect(await reconciliationLogCount()).toBe(count);
      expect(await drainDeadLetters(h)).toEqual([]);
    });

    it('stops and reports INCOMPLETE (never silently drops) when nothing drains the queue', async () => {
      await recreateHarness({ maxQueueLength: 2, prefetch: 1 }); // and NO worker

      for (const id of ['wallet_alice', 'wallet_bob', 'wallet_carol', 'wallet_dave']) {
        await driftWallet(id, 1);
      }

      const startedAt = Date.now();
      const report = await ledgerAuditService.runAudit({ bus: h.bus, drainTimeoutMs: 150 });

      expect(Date.now() - startedAt).toBeLessThan(3000); // bounded wait, no hang
      expect(report.completed).toBe(false);
      expect(report.ok).toBe(false);
      expect(report.incompleteReason).toMatch(/^PUBLISH_STALLED/);
      expect(report.events.published).toBe(2); // exactly the queue cap
      expect(report.events.unpublished).toBe(2); // accounted for, not lost
      expect(await queueDepth(h, TOPOLOGY.workQueue)).toBe(2);
    });

    it('exposes the audit over HTTP to admins', async () => {
      setEventBus(h.bus);
      const api = actAs(app);
      await driftWallet('wallet_bob', 1);

      const dryRun = await api.inject({ method: 'POST', url: '/admin/ledger/audit', payload: { publish: false } });
      expect(dryRun.statusCode).toBe(200);
      expect(dryRun.json().checks.walletDrift.violations).toBe(1);
      expect(dryRun.json().events.published).toBe(0);

      const real = await api.inject({ method: 'POST', url: '/admin/ledger/audit', payload: {} });
      expect(real.json().events.published).toBe(1);

      const bad = await api.inject({ method: 'POST', url: '/admin/ledger/audit', payload: { publish: 'yes' } });
      expect(bad.statusCode).toBe(400);
    });
  });

  describe('reconciliation worker', () => {
    it('corrects a drifted balance to the verified ledger, logs it, and re-audits clean', async () => {
      await driftWallet('wallet_alice', 7777);
      const versionBefore = (await walletService.getWallet('wallet_alice'))!.version;
      const done = settled(h, 1);
      worker = await startReconciliationWorker(h.bus);

      const report = await ledgerAuditService.runAudit({ bus: h.bus });
      expect(report.events.published).toBe(1);
      expect((await done)[0].outcome).toBe('acked');

      const alice = await walletService.getWallet('wallet_alice');
      expect(alice?.balanceCents).toBe(10000);
      expect(alice?.balanceCents).toBe(await ledgerSumFor('wallet_alice'));
      expect(alice?.version).toBe(versionBefore + 1); // optimistic lockers see the correction

      const log = await query<{ previous: string; corrected: string; source_of_truth: string }>(
        `SELECT previous_balance_cents::text AS previous, corrected_balance_cents::text AS corrected, source_of_truth
         FROM reconciliation_log WHERE wallet_id = 'wallet_alice'`
      );
      expect(log.rows[0]).toEqual({ previous: '7777', corrected: '10000', source_of_truth: 'LEDGER' });

      expect((await ledgerAuditService.runAudit({ publish: false })).ok).toBe(true);
      expect(await drainDeadLetters(h)).toEqual([]);
    });

    it('is idempotent: a redelivered event never double-corrects', async () => {
      await driftWallet('wallet_bob', 1);
      const handleSpy = vi.spyOn(ledgerReconciliationService, 'handle');
      const done = settled(h, 2);
      worker = await startReconciliationWorker(h.bus);

      const envelope = createEnvelope(LEDGER_EVENT_TYPES.WALLET_DRIFT, 'audit_manual', {
        walletId: 'wallet_bob',
        currency: 'USD',
        accountType: 'USER',
        balanceCents: '1',
        ledgerBalanceCents: '10000',
        ledgerVersion: '1',
      });
      await h.bus.publish(envelope);
      await h.bus.publish(envelope); // at-least-once redelivery of the same event

      await done;
      const outcomes = await Promise.all(handleSpy.mock.results.map((r) => r.value as Promise<string>));
      expect(outcomes.sort()).toEqual(['ALREADY_IN_SYNC', 'RECONCILED']);
      expect(await balanceOf('wallet_bob')).toBe(10000);
      expect(await reconciliationLogCount('wallet_bob')).toBe(1);
    });

    it('refuses to trust an unbalanced ledger: dead-letters for manual review and changes nothing', async () => {
      await withIntegrityTriggersDisabled(async () => {
        // Orphan ledger line: alice's ledger now says 10500 but its journal is unbalanced.
        await query(
          `INSERT INTO ledger_entries (entry_id, transaction_ref, wallet_id, currency, direction, signed_amount_cents)
           VALUES ('le_orphan', 'rogue_orphan', 'wallet_alice', 'USD', 'CREDIT', 500)`
        );
      });
      const done = settled(h, 3);
      worker = await startReconciliationWorker(h.bus);

      const report = await ledgerAuditService.runAudit({ bus: h.bus });
      expect(report.checks.walletDrift.violations).toBe(1);
      expect(report.checks.unbalancedJournals.violations).toBe(1);
      expect(report.checks.globalZeroSum.violations).toBe(1);

      expect((await done).every((e) => e.outcome === 'dead-lettered')).toBe(true);
      const dead = await drainDeadLetters(h);
      expect(dead.map((d) => [d.type, d.reason]).sort()).toEqual([
        [LEDGER_EVENT_TYPES.GLOBAL_ZERO_SUM, 'MANUAL_REVIEW_GLOBAL_ZERO_SUM'],
        [LEDGER_EVENT_TYPES.UNBALANCED_JOURNAL, 'MANUAL_REVIEW_UNBALANCED_JOURNAL'],
        [LEDGER_EVENT_TYPES.WALLET_DRIFT, 'SOURCE_OF_TRUTH_UNDETERMINED'],
      ]);
      expect(dead.every((d) => d.attempt === 1)).toBe(true); // non-retryable: no retry churn
      expect(await balanceOf('wallet_alice')).toBe(10000);
      expect(await reconciliationLogCount()).toBe(0);
    });

    it('dead-letters a negative fee treasury but reconciles the FX pool on the same journal', async () => {
      await withIntegrityTriggersDisabled(async () => {
        await query(
          `INSERT INTO ledger_entries (entry_id, transaction_ref, wallet_id, currency, direction, signed_amount_cents)
           VALUES ('le_n1', 'rogue_neg', 'wallet_fee_treasury_usd', 'USD', 'DEBIT', -100),
                  ('le_n2', 'rogue_neg', 'wallet_fx_pool_usd', 'USD', 'CREDIT', 100)`
        );
      });
      const done = settled(h, 3);
      worker = await startReconciliationWorker(h.bus);

      const report = await ledgerAuditService.runAudit({ bus: h.bus });
      expect(report.checks.negativeBalances.violations).toBe(1);
      expect(report.checks.walletDrift.violations).toBe(2);

      const outcomes = (await done).map((e) => e.outcome).sort();
      expect(outcomes).toEqual(['acked', 'dead-lettered', 'dead-lettered']);
      const dead = await drainDeadLetters(h);
      expect(dead.map((d) => d.reason).sort()).toEqual([
        'MANUAL_REVIEW_NEGATIVE_BALANCE',
        'SOURCE_OF_TRUTH_UNDETERMINED', // fee treasury drift: -100 is not a valid fee balance
      ]);
      const fxPool = await walletService.getWallet('wallet_fx_pool_usd');
      expect(fxPool).toMatchObject({ balanceCents: 100, ledgerBalanceCents: 100, ledgerVersion: 1 });
      expect(await balanceOf('wallet_fee_treasury_usd')).toBe(0); // untouched pending manual review
    });

    it('acks a global violation that no longer reproduces instead of escalating it', async () => {
      const handleSpy = vi.spyOn(ledgerReconciliationService, 'handle');
      const done = settled(h, 1);
      worker = await startReconciliationWorker(h.bus);

      await h.bus.publish(
        createEnvelope(LEDGER_EVENT_TYPES.GLOBAL_ZERO_SUM, 'audit_stale', { currency: 'USD', totalCents: '999' })
      );

      expect((await done)[0].outcome).toBe('acked');
      await expect(handleSpy.mock.results[0].value).resolves.toBe('NO_LONGER_VIOLATED');
      expect(await drainDeadLetters(h)).toEqual([]);
    });

    it('retries transient failures through the TTL retry tiers and then succeeds', async () => {
      await driftWallet('wallet_carol', 5);
      const spy = vi
        .spyOn(ledgerReconciliationService, 'reconcileWalletDrift')
        .mockRejectedValueOnce(new Error('connection reset by peer'))
        .mockRejectedValueOnce(new Error('connection reset by peer'));
      const stopExpiry = simulateBrokerTtlExpiry(h);

      // The attempt header only increments when a message is re-published
      // through a retry tier (a plain requeue would redeliver it as attempt 1).
      const attemptsSeen: number[] = [];
      const done = settled(h, 3);
      worker = await h.bus.subscribe(async (envelope, ctx) => {
        attemptsSeen.push(ctx.attempt);
        await ledgerReconciliationService.handle(envelope);
      });

      await ledgerAuditService.runAudit({ bus: h.bus });
      const outcomes = (await done).map((e) => [e.outcome, e.retryQueue ?? null]);
      stopExpiry();

      expect(outcomes).toEqual([
        ['retry-scheduled', TOPOLOGY.retryQueue(1)],
        ['retry-scheduled', TOPOLOGY.retryQueue(2)], // backoff tier escalates
        ['acked', null],
      ]);
      expect(attemptsSeen).toEqual([1, 2, 3]);
      expect(spy).toHaveBeenCalledTimes(MAX_ATTEMPTS);
      expect(await balanceOf('wallet_carol')).toBe(10000);
      expect(await drainDeadLetters(h)).toEqual([]);
    });

    it('dead-letters after exhausting retries, preserving attempt count and reason', async () => {
      await driftWallet('wallet_dave', 5);
      const spy = vi
        .spyOn(ledgerReconciliationService, 'reconcileWalletDrift')
        .mockRejectedValue(new Error('database unavailable'));
      const stopExpiry = simulateBrokerTtlExpiry(h);
      const deadLettered = waitForEvent<DeliverySettled>(h.bus, 'delivery-settled', (e) => e.outcome === 'dead-lettered');
      worker = await startReconciliationWorker(h.bus);

      await ledgerAuditService.runAudit({ bus: h.bus });
      await deadLettered;
      stopExpiry();

      const [dead] = await drainDeadLetters(h);
      expect(dead).toMatchObject({
        type: LEDGER_EVENT_TYPES.WALLET_DRIFT,
        reason: 'database unavailable',
        attempt: MAX_ATTEMPTS,
      });
      expect(JSON.parse(dead.body).payload.walletId).toBe('wallet_dave');
      expect(spy).toHaveBeenCalledTimes(MAX_ATTEMPTS);
      expect(await balanceOf('wallet_dave')).toBe(5); // untouched; awaiting manual action
    });

    it('dead-letters malformed messages without invoking the handler', async () => {
      const handleSpy = vi.spyOn(ledgerReconciliationService, 'handle');
      const done = settled(h, 1);
      worker = await startReconciliationWorker(h.bus);

      // Authentic (signed by the ledger's own key) but not a valid envelope.
      const body = Buffer.from('{not json');
      h.inspector.publish(
        TOPOLOGY.exchange,
        LEDGER_EVENT_TYPES.WALLET_DRIFT,
        body,
        signedPublishProperties(eventAuthFor(SERVICES.ledger), { eventType: LEDGER_EVENT_TYPES.WALLET_DRIFT, eventId: 'evt_malformed', body })
      );
      expect((await done)[0]).toMatchObject({ outcome: 'dead-lettered', reason: 'MALFORMED_MESSAGE' });

      const [dead] = await drainDeadLetters(h);
      expect(dead.reason).toBe('MALFORMED_MESSAGE');
      expect(handleSpy).not.toHaveBeenCalled();
    });

    it('dead-letters events that are not authenticated, before parsing them', async () => {
      const handleSpy = vi.spyOn(ledgerReconciliationService, 'handle');
      const done = settled(h, 1);
      worker = await startReconciliationWorker(h.bus);

      // No signature headers: an injected event on the internal exchange is not accepted.
      h.inspector.publish(TOPOLOGY.exchange, LEDGER_EVENT_TYPES.WALLET_DRIFT, Buffer.from(
          JSON.stringify(
            createEnvelope(LEDGER_EVENT_TYPES.WALLET_DRIFT, 'forged', {
              walletId: 'wallet_bob',
              currency: 'USD',
              accountType: 'USER',
              balanceCents: '1',
              ledgerBalanceCents: '10000',
              ledgerVersion: '1',
            })
          )
        ));
      expect((await done)[0]).toMatchObject({ outcome: 'dead-lettered' });
      const [dead] = await drainDeadLetters(h);
      expect(dead.reason).toMatch(/^UNTRUSTED_EVENT: .*UNSIGNED_EVENT/);
      expect(handleSpy).not.toHaveBeenCalled();
    });

    it('reconciles every wallet across many audit pages', async () => {
      const count = 12;
      await query(
        `INSERT INTO wallets (wallet_id, owner_id, currency)
         SELECT 'wallet_page_' || lpad(i::text, 3, '0'), 'user_page', 'EUR' FROM generate_series(1, ${count}) AS i`
      );
      await withIntegrityTriggersDisabled(async () => {
        await query(`UPDATE wallets SET balance_cents = 99 WHERE wallet_id LIKE 'wallet_page_%'`);
      });
      const done = settled(h, count);
      worker = await startReconciliationWorker(h.bus);

      const report = await ledgerAuditService.runAudit({ bus: h.bus, pageSize: 5 });
      expect(report.events.published).toBe(count);

      expect((await done).every((e) => e.outcome === 'acked')).toBe(true);
      const remaining = await query(`SELECT 1 FROM wallets WHERE wallet_id LIKE 'wallet_page_%' AND balance_cents <> 0`);
      expect(remaining.rows).toHaveLength(0);
      expect((await ledgerAuditService.runAudit({ publish: false })).ok).toBe(true);
    });

    it('a drifted wallet returns a clean 409 and is repaired by the event it publishes', async () => {
      setEventBus(h.bus); // the write guard publishes on the process-wide bus
      const api = actAs(app);
      await driftWallet('wallet_alice', 9000);
      const repaired = settled(h, 1);
      worker = await startReconciliationWorker(h.bus);

      const blocked = await api.inject({
        method: 'POST',
        url: '/wallets/transfer',
        payload: { fromWalletId: 'wallet_alice', toWalletId: 'wallet_bob', amountCents: 100 },
      });
      expect(blocked.statusCode).toBe(409);
      expect(blocked.json()).toMatchObject({ code: 'WALLET_FROZEN_PENDING_RECONCILIATION', walletId: 'wallet_alice' });
      expect(await balanceOf('wallet_bob')).toBe(10000);

      // No audit run needed: the blocked write itself triggered reconciliation.
      expect((await repaired)[0].outcome).toBe('acked');
      expect(await reconciliationLogCount('wallet_alice')).toBe(1);

      const unblocked = await api.inject({
        method: 'POST',
        url: '/wallets/transfer',
        payload: { fromWalletId: 'wallet_alice', toWalletId: 'wallet_bob', amountCents: 100 },
      });
      expect(unblocked.statusCode).toBe(201);
    });
  });
});
