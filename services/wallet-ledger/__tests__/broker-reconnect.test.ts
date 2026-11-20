import { afterAll, afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { closeDb, query, resetAndSeedDatabase } from '../src/db';
import { closeRedis, flushRedis } from '../src/redis';
import { walletService } from '../src/services/WalletService';
import {
  ledgerReconciliationService,
  startReconciliationWorker,
} from '../src/services/LedgerReconciliationService';
import {
  DeliverySettled,
  EventEnvelope,
  LEDGER_EVENT_TYPES,
  PublishRejectedError,
  Subscription,
  TOPOLOGY,
  createEnvelope,
} from '../src/messaging';
import { BrokerHarness, createBrokerHarness, queueDepth } from './helpers/broker';
import { collectEvents, deferred, waitForEvent } from './helpers/events';

function driftEnvelope(walletId: string): EventEnvelope {
  return createEnvelope(LEDGER_EVENT_TYPES.WALLET_DRIFT, 'reconnect_test', {
    walletId,
    currency: 'USD',
    accountType: 'USER',
    balanceCents: '1',
    ledgerBalanceCents: '10000',
    ledgerVersion: '1',
  });
}

async function driftWallet(walletId: string, balance: number): Promise<void> {
  await query('ALTER TABLE wallets DISABLE TRIGGER USER');
  try {
    await query(`UPDATE wallets SET balance_cents = $1 WHERE wallet_id = $2`, [balance, walletId]);
  } finally {
    await query('ALTER TABLE wallets ENABLE TRIGGER USER');
  }
}

async function workQueueEventIds(h: BrokerHarness): Promise<string[]> {
  const ids: string[] = [];
  for (;;) {
    const msg = await h.inspector.get(TOPOLOGY.workQueue, { noAck: true });
    if (!msg) return ids;
    ids.push((JSON.parse(msg.content.toString()) as EventEnvelope).eventId);
  }
}

/** Simulate the broker dropping the bus's live connection. */
async function dropLiveConnection(h: BrokerHarness): Promise<void> {
  await h.connections.at(-1)!.close();
}

describe('Event bus: atomic reconnection and safe re-publishing', () => {
  let h: BrokerHarness;
  let worker: Subscription | undefined;

  beforeEach(async () => {
    await resetAndSeedDatabase();
    await flushRedis();
    h = await createBrokerHarness();
    worker = undefined;
  });

  afterEach(async () => {
    vi.restoreAllMocks();
    await worker?.cancel();
    await h.close();
  });

  afterAll(async () => {
    await closeDb();
    await closeRedis();
  });

  it('rebuilds the whole session after a broker drop and keeps consuming', async () => {
    worker = await startReconciliationWorker(h.bus);
    const firstEpoch = h.bus.sessionEpoch;

    const reconnected = waitForEvent<number>(h.bus, 'session');
    await dropLiveConnection(h);
    const newEpoch = await reconnected;

    expect(newEpoch).toBeGreaterThan(firstEpoch);
    expect(h.connections).toHaveLength(2);

    // The consumer was re-subscribed on the new session: it processes new work.
    await driftWallet('wallet_bob', 1);
    const handled = waitForEvent<DeliverySettled>(h.bus, 'delivery-settled', (e) => e.outcome === 'acked');
    await h.bus.publish(driftEnvelope('wallet_bob'));
    await handled;
    expect((await walletService.getWallet('wallet_bob'))!.balanceCents).toBe(10000);
  });

  it('holds events published during an outage and delivers them once the broker is back', async () => {
    h.broker.up = false;
    const retrying = waitForEvent(h.bus, 'reconnect-scheduled');
    await dropLiveConnection(h);
    await retrying;

    const envelope = driftEnvelope('wallet_alice');
    const delivered = h.bus.publish(envelope); // broker unreachable: kept in the pending ledger
    expect(h.bus.pendingPublishCount).toBe(1);

    const backUp = waitForEvent(h.bus, 'session');
    h.broker.up = true; // the next scheduled attempt succeeds
    await backUp;
    await delivered; // resolves only once the broker CONFIRMED it

    expect(h.bus.pendingPublishCount).toBe(0);
    expect(await workQueueEventIds(h)).toEqual([envelope.eventId]);
  });

  it('re-publishes a message whose confirm was lost with the connection (at-least-once)', async () => {
    const dropped = waitForEvent(h.bus, 'publish-dropped');
    const republished = waitForEvent<{ count: number }>(h.bus, 'republished');
    const envelope = driftEnvelope('wallet_alice');

    const delivered = h.bus.publish(envelope); // handed to the channel synchronously...
    void dropLiveConnection(h); // ...which dies before the broker's confirm arrives

    await dropped;
    expect((await republished).count).toBe(1);
    await delivered;

    // Never lost; possibly duplicated (the broker may have stored the first
    // copy), which is safe because every consumer is idempotent.
    const ids = await workQueueEventIds(h);
    expect(ids.length).toBeGreaterThanOrEqual(1);
    expect(new Set(ids)).toEqual(new Set([envelope.eventId]));
  });

  it('redelivers an in-flight delivery after a drop and applies its effect exactly once', async () => {
    await driftWallet('wallet_bob', 1);
    const firstHandlerStarted = deferred();
    const releaseFirstHandler = deferred();
    const original = ledgerReconciliationService.reconcileWalletDrift.bind(ledgerReconciliationService);
    let calls = 0;
    vi.spyOn(ledgerReconciliationService, 'reconcileWalletDrift').mockImplementation(async (env) => {
      calls++;
      if (calls === 1) {
        firstHandlerStarted.resolve();
        await releaseFirstHandler.promise; // still running when its channel dies
      }
      return original(env);
    });
    const bothSettled = collectEvents<DeliverySettled>(h.bus, 'delivery-settled', 2);
    worker = await startReconciliationWorker(h.bus);

    await h.bus.publish(driftEnvelope('wallet_bob'));
    await firstHandlerStarted.promise;

    // The broker requeues the unacked delivery and redelivers it to the new
    // session's consumer, which repairs the wallet.
    const reconnected = waitForEvent(h.bus, 'session');
    await dropLiveConnection(h);
    await reconnected;

    // The original handler now finishes: its late ack on the dead channel must
    // not crash the process, and its repair must be a no-op.
    releaseFirstHandler.resolve();
    await bothSettled;

    expect(calls).toBe(2);
    const log = await query(`SELECT 1 FROM reconciliation_log WHERE wallet_id = 'wallet_bob'`);
    expect(log.rows).toHaveLength(1); // corrected exactly once
    expect((await walletService.getWallet('wallet_bob'))!.balanceCents).toBe(10000);
  });

  it('discards a session whose setup fails part-way and never exposes it', async () => {
    const setupReached = deferred();
    const releaseSetup = deferred();
    h.faults.failNextSetup = true;
    h.faults.holdFailingSetup = { reached: setupReached.resolve, release: releaseSetup.promise };
    const sessions: number[] = [];
    h.bus.on('session', (epoch: number) => sessions.push(epoch));
    let droppedSends = 0;
    h.bus.on('publish-dropped', () => droppedSends++);
    const retried = waitForEvent(h.bus, 'reconnect-scheduled');
    const healthy = waitForEvent<number>(h.bus, 'session');

    await dropLiveConnection(h);
    await setupReached.promise; // a new connection is mid-setup RIGHT NOW...

    expect(h.bus.sessionEpoch).toBe(0); // ...and the half-built session is not exposed
    const envelope = driftEnvelope('wallet_alice');
    const delivered = h.bus.publish(envelope); // so this must wait, not go out on it
    expect(h.bus.pendingPublishCount).toBe(1);

    releaseSetup.resolve(); // the setup now fails; the connection is discarded
    await retried; // and a retry scheduled
    await healthy;
    await delivered;

    expect(sessions).toHaveLength(1); // only the complete session ever went live
    expect(droppedSends).toBe(0); // nothing was ever sent on the broken session
    expect(h.connections).toHaveLength(3); // original, broken, healthy
    expect(await workQueueEventIds(h)).toEqual([envelope.eventId]);
  });

  it('bounds the pending-publish buffer while disconnected (memory safety)', async () => {
    await h.close();
    h = await createBrokerHarness({ maxPendingPublishes: 2 });
    h.broker.up = false;
    const retrying = waitForEvent(h.bus, 'reconnect-scheduled');
    await dropLiveConnection(h);
    await retrying;

    const held = [h.bus.publish(driftEnvelope('a')), h.bus.publish(driftEnvelope('b'))];
    await expect(h.bus.publish(driftEnvelope('c'))).rejects.toThrow(/local publish buffer full/);
    expect(h.bus.pendingPublishCount).toBe(2);

    h.broker.up = true;
    await Promise.all(held);
    expect(h.bus.pendingPublishCount).toBe(0);
  });

  it('close() during an outage rejects pending publishes instead of hanging', async () => {
    h.broker.up = false;
    const retrying = waitForEvent(h.bus, 'reconnect-scheduled');
    await dropLiveConnection(h);
    await retrying;

    const pending = h.bus.publish(driftEnvelope('wallet_alice'));
    await h.bus.close();

    await expect(pending).rejects.toBeInstanceOf(PublishRejectedError);
    await expect(pending).rejects.toThrow(/event bus closed/);
  });

  it('gives up after the configured reconnect attempts and fails callers fast', async () => {
    await h.close();
    h = await createBrokerHarness({ reconnect: { initialDelayMs: 1, maxDelayMs: 2, jitter: 0, maxAttempts: 2 } });
    h.broker.up = false;
    const gaveUp = waitForEvent(h.bus, 'reconnect-failed');
    await dropLiveConnection(h);
    await gaveUp;

    await expect(h.bus.publish(driftEnvelope('wallet_alice'))).rejects.toThrow();
  });

  it('keeps publishing and consuming across repeated drops', async () => {
    worker = await startReconciliationWorker(h.bus);
    const wallets = ['wallet_alice', 'wallet_bob', 'wallet_carol'];
    for (const id of wallets) await driftWallet(id, 1);

    for (const id of wallets) {
      const reconnected = waitForEvent(h.bus, 'session');
      await dropLiveConnection(h);
      await reconnected;
      const handled = waitForEvent<DeliverySettled>(h.bus, 'delivery-settled', (e) => e.outcome === 'acked');
      await h.bus.publish(driftEnvelope(id));
      await handled;
    }

    for (const id of wallets) {
      expect((await walletService.getWallet(id))!.frozenPendingReconciliation).toBe(false);
    }
    expect(h.connections).toHaveLength(4);
    expect(await queueDepth(h, TOPOLOGY.workQueue)).toBe(0);
  });
});
