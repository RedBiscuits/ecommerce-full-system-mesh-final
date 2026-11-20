/* eslint-disable @typescript-eslint/no-unused-vars */
import { describe, it, test, expect, beforeEach, afterEach, beforeAll, afterAll, vi } from 'vitest';
/**
 * tests/unit/services/outbox.test.ts
 *
 * The outbox store against the real JSON DB (temp file):
 *   - enqueue is part of the caller's transaction (rolled back together)
 *   - claim = lease: FIFO, bounded, skips leased / backed-off / non-PENDING
 *     rows, and two claimants never get the same row
 *   - only the lease holder can mark a row; an expired lease is reclaimable
 *   - markFailed backs off exponentially and parks after maxAttempts
 *   - release, retryFailed, prune, stats, list
 *   - commit notifications fire after the commit, never on rollback
 *   - deferred verifications: own status, invisible to the publisher's claim,
 *     restored / parked / retried / pruned along the same lifecycle
 */

import { db } from '../../../src/services/jsonDb';
import { backoffDelay, DEFERRED_VERIFICATION, outbox, OUTBOX_COLLECTION } from '../../../src/services/outbox';
import { cleanupTestDb, readTestDb } from '../../helpers/testDb';
import type { DeferInput, EnqueueInput, PersistedOutboxEvent } from '../../../src/services/outbox';

const T0 = new Date('2026-10-28T10:00:00.000Z');
const at = (ms: number): Date => new Date(T0.getTime() + ms);

function input(overrides: Partial<EnqueueInput> = {}): EnqueueInput {
  return {
    eventType: 'PAYMENT_COMPLETED',
    aggregateType: 'payment',
    aggregateId: 'p-1',
    correlationId: 'c-1',
    exchange: 'hyperpay',
    routingKey: 'payment.result',
    payload: {
      correlationId: 'c-1',
      source: 'verify',
      origin: 'checkout',
      transactionId: 'txn-1',
      status: 'SUCCESS',
      amount: '1.00',
      currency: 'SAR',
      resultCode: '000.100.110',
      dbRecordId: 'p-1',
    },
    ...overrides,
  };
}

/** Enqueue standalone (its own transaction); returns the row. */
const enqueue = (overrides: Partial<EnqueueInput> = {}): Promise<PersistedOutboxEvent> =>
  db.transaction((tx) => outbox.enqueue(tx, input(overrides)));

const rows = (): Promise<PersistedOutboxEvent[]> => db.findAll<PersistedOutboxEvent>(OUTBOX_COLLECTION);
const FAILURE = { maxAttempts: 3, retryDelayMs: 100, maxRetryDelayMs: 1000 };

function deferInput(overrides: Partial<DeferInput> = {}): DeferInput {
  return {
    idempotencyKey: '/v1/checkouts/d/payment',
    correlationId: 'c-d',
    payload: { resourcePath: '/v1/checkouts/d/payment', source: 'verify', correlationId: 'c-d', notifyUnchanged: true, reason: 'circuit oppwa OPEN' },
    ...overrides,
  };
}

beforeEach(async () => {
  vi.spyOn(console, 'warn').mockImplementation(() => undefined);
  await db.clear(OUTBOX_COLLECTION);
});
afterEach(() => vi.restoreAllMocks());
afterAll(() => cleanupTestDb());

// ─── enqueue ──────────────────────────────────────────────────────────────────

describe('enqueue()', () => {
  it('inserts a PENDING, unlocked row with zero attempts and an occurredAt stamp', async () => {
    const row = await enqueue();

    expect(row).toMatchObject({ ...input(), status: 'PENDING', attempts: 0 });
    expect(row.occurredAt).toMatch(/^\d{4}-/);
    expect(row.lockedBy).toBeUndefined();
    expect(row.nextAttemptAt).toBeUndefined();
    expect(readTestDb()[OUTBOX_COLLECTION]).toHaveLength(1);
  });

  it('is atomic with the caller\'s other writes: a rollback leaves neither the record nor the event', async () => {
    await expect(
      db.transaction(async (tx) => {
        await tx.insert('things', { n: 1 });
        await outbox.enqueue(tx, input());
        throw new Error('boom');
      }),
    ).rejects.toThrow('boom');

    expect(await rows()).toHaveLength(0);
    expect(await db.findAll('things')).toHaveLength(0);
  });

  it('notifies listeners after the commit, not before, and never on rollback', async () => {
    const seen: string[] = [];
    const off = outbox.onEnqueued(() => seen.push(readTestDb()[OUTBOX_COLLECTION] ? 'committed' : 'not-yet'));
    try {
      await db.transaction(async (tx) => {
        await outbox.enqueue(tx, input());
        expect(seen).toEqual([]); // not fired inside the transaction
      });
      expect(seen).toEqual(['committed']);

      await expect(db.transaction(async (tx) => { await outbox.enqueue(tx, input()); throw new Error('x'); })).rejects.toThrow('x');
      expect(seen).toEqual(['committed']);
    } finally {
      off();
    }
  });

  it('a listener that throws does not break the caller', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => undefined);
    const off = outbox.onEnqueued(() => { throw new Error('listener bug'); });
    try {
      await expect(enqueue()).resolves.toBeDefined();
    } finally {
      off();
    }
  });
});

// ─── claim ────────────────────────────────────────────────────────────────────

describe('claim()', () => {
  it('leases PENDING rows in insertion order, up to the limit, stamping lockedBy / lockedUntil and counting the attempt', async () => {
    const a = await enqueue({ correlationId: 'a' });
    const b = await enqueue({ correlationId: 'b' });
    await enqueue({ correlationId: 'c' });

    const claimed = await outbox.claim({ publisherId: 'pub-1', limit: 2, leaseMs: 30_000, now: T0 });

    expect(claimed.map((r) => r._id)).toEqual([a._id, b._id]);
    expect(claimed[0]).toMatchObject({ lockedBy: 'pub-1', lockedUntil: at(30_000).toISOString(), status: 'PENDING', attempts: 1 });
    const persisted = await rows();
    expect(persisted.filter((r) => r.lockedBy === 'pub-1')).toHaveLength(2);
  });

  it('never hands the same row to two claimants while the lease holds', async () => {
    await enqueue();

    const first = await outbox.claim({ publisherId: 'pub-1', limit: 10, leaseMs: 30_000, now: T0 });
    const second = await outbox.claim({ publisherId: 'pub-2', limit: 10, leaseMs: 30_000, now: at(1000) });

    expect(first).toHaveLength(1);
    expect(second).toHaveLength(0);
  });

  it('is safe under concurrent claimants: the rows are partitioned, never shared', async () => {
    for (let i = 0; i < 6; i += 1) await enqueue({ correlationId: `c-${i}` });

    const results = await Promise.all(['p1', 'p2', 'p3'].map((publisherId) => outbox.claim({ publisherId, limit: 4, leaseMs: 30_000 })));

    const ids = results.flat().map((r) => r._id);
    expect(new Set(ids).size).toBe(ids.length);
    expect(ids).toHaveLength(6);
  });

  it('reclaims a row whose lease expired (its publisher died mid-publish)', async () => {
    await enqueue();
    await outbox.claim({ publisherId: 'dead', limit: 10, leaseMs: 30_000, now: T0 });

    const tooEarly = await outbox.claim({ publisherId: 'alive', limit: 10, leaseMs: 30_000, now: at(29_999) });
    const afterExpiry = await outbox.claim({ publisherId: 'alive', limit: 10, leaseMs: 30_000, now: at(30_000) });

    expect(tooEarly).toHaveLength(0);
    expect(afterExpiry).toHaveLength(1);
    expect(afterExpiry[0]).toMatchObject({ lockedBy: 'alive', attempts: 2 }); // the dead publisher's attempt counts
  });

  it('skips rows in backoff (nextAttemptAt in the future), PUBLISHED rows and FAILED rows', async () => {
    const backoff = await enqueue({ correlationId: 'backoff' });
    const done = await enqueue({ correlationId: 'done' });
    const parked = await enqueue({ correlationId: 'parked' });
    await enqueue({ correlationId: 'ready' });
    await db.update(OUTBOX_COLLECTION, backoff._id, { nextAttemptAt: at(5000).toISOString() });
    await db.update(OUTBOX_COLLECTION, done._id, { status: 'PUBLISHED' });
    await db.update(OUTBOX_COLLECTION, parked._id, { status: 'FAILED' });

    const now = await outbox.claim({ publisherId: 'p', limit: 10, leaseMs: 60_000, now: T0 });
    expect(now.map((r) => r.correlationId)).toEqual(['ready']);

    const later = await outbox.claim({ publisherId: 'p', limit: 10, leaseMs: 60_000, now: at(5000) });
    expect(later.map((r) => r.correlationId)).toEqual(['backoff']); // 'ready' is still leased
  });
});

// ─── markPublished / markFailed / release ─────────────────────────────────────

describe('markPublished()', () => {
  it('moves the row to PUBLISHED, stamps publishedAt and drops the lease', async () => {
    const row = await enqueue();
    await outbox.claim({ publisherId: 'p', limit: 1, leaseMs: 1000, now: T0 });

    const published = await outbox.markPublished(row._id, 'p', at(10));

    expect(published).toMatchObject({ status: 'PUBLISHED', attempts: 1, publishedAt: at(10).toISOString() });
    expect(published?.lockedBy).toBeUndefined();
    expect(published?.lockedUntil).toBeUndefined();
    expect(JSON.stringify(readTestDb())).not.toContain('lockedBy');
  });

  it('still records the publish (with a warning) when the lease had expired and moved on — the broker has the message either way', async () => {
    const row = await enqueue();
    await outbox.claim({ publisherId: 'slow', limit: 1, leaseMs: 10, now: T0 });
    await outbox.claim({ publisherId: 'fast', limit: 1, leaseMs: 1000, now: at(20) });

    const published = await outbox.markPublished(row._id, 'slow');

    expect(published?.status).toBe('PUBLISHED');
    expect(console.warn).toHaveBeenCalledWith(expect.stringContaining('lease expired'));
  });

  it('is idempotent and tolerant of unknown ids', async () => {
    const row = await enqueue();
    await outbox.claim({ publisherId: 'p', limit: 1, leaseMs: 1000 });
    await outbox.markPublished(row._id, 'p', at(10));

    expect(await outbox.markPublished(row._id, 'p', at(20))).toMatchObject({ attempts: 1, publishedAt: at(10).toISOString() });
    expect(await outbox.markPublished('nope', 'p')).toBeNull();
  });
});

describe('markFailed()', () => {
  it('keeps the row PENDING with an exponential backoff, records the error and drops the lease', async () => {
    const row = await enqueue();
    await outbox.claim({ publisherId: 'p', limit: 1, leaseMs: 1000, now: T0 });

    const first = await outbox.markFailed(row._id, 'p', new Error('message nacked'), { ...FAILURE, now: T0 });
    expect(first).toMatchObject({ status: 'PENDING', attempts: 1, lastError: 'message nacked', nextAttemptAt: at(100).toISOString() }); // attempts counted at claim
    expect(first?.lockedBy).toBeUndefined();

    await outbox.claim({ publisherId: 'p', limit: 1, leaseMs: 1000, now: at(100) });
    const second = await outbox.markFailed(row._id, 'p', 'timeout', { ...FAILURE, now: at(100) });
    expect(second).toMatchObject({ status: 'PENDING', attempts: 2, lastError: 'timeout', nextAttemptAt: at(100 + 200).toISOString() });
  });

  it('parks the row as FAILED once maxAttempts is reached', async () => {
    const row = await enqueue();
    for (let attempt = 1; attempt <= 3; attempt += 1) {
      await outbox.claim({ publisherId: 'p', limit: 1, leaseMs: 1000, now: at(attempt * 10_000) });
      await outbox.markFailed(row._id, 'p', new Error(`fail ${attempt}`), { ...FAILURE, now: at(attempt * 10_000) });
    }

    const parked = await db.findById<PersistedOutboxEvent>(OUTBOX_COLLECTION, row._id);
    expect(parked).toMatchObject({ status: 'FAILED', attempts: 3, lastError: 'fail 3' });
    expect(parked?.nextAttemptAt).toBeUndefined();
    expect(await outbox.claim({ publisherId: 'p', limit: 10, leaseMs: 1000, now: at(99_999_999) })).toHaveLength(0);
  });

  it('ignores a failure reported by a publisher that no longer holds the lease', async () => {
    const row = await enqueue();
    await outbox.claim({ publisherId: 'slow', limit: 1, leaseMs: 10, now: T0 });
    await outbox.claim({ publisherId: 'fast', limit: 1, leaseMs: 1000, now: at(20) });

    const result = await outbox.markFailed(row._id, 'slow', new Error('late'), FAILURE);

    expect(result).toMatchObject({ lockedBy: 'fast', attempts: 2, status: 'PENDING' });
    expect(result?.lastError).toBeUndefined();
  });

  it('caps the backoff at maxRetryDelayMs', () => {
    expect(backoffDelay(1, FAILURE)).toBe(100);
    expect(backoffDelay(2, FAILURE)).toBe(200);
    expect(backoffDelay(4, FAILURE)).toBe(800);
    expect(backoffDelay(5, FAILURE)).toBe(1000);
    expect(backoffDelay(50, FAILURE)).toBe(1000);
  });
});

describe('release()', () => {
  it('drops the lease of the given rows and refunds the attempt; other publishers\' leases are untouched', async () => {
    const mine = await enqueue({ correlationId: 'mine' });
    const theirs = await enqueue({ correlationId: 'theirs' });
    await outbox.claim({ publisherId: 'me', limit: 1, leaseMs: 1000 });
    await outbox.claim({ publisherId: 'them', limit: 1, leaseMs: 1000 });

    const released = await outbox.release([mine._id, theirs._id, 'unknown'], 'me');

    expect(released).toBe(1);
    const all = await rows();
    expect(all.find((r) => r._id === mine._id)).toMatchObject({ attempts: 0, status: 'PENDING' });
    expect(all.find((r) => r._id === mine._id)?.lockedBy).toBeUndefined();
    expect(all.find((r) => r._id === theirs._id)?.lockedBy).toBe('them');
    expect(await outbox.release([], 'me')).toBe(0);
  });
});

// ─── Operations ───────────────────────────────────────────────────────────────

describe('retryFailed()', () => {
  async function park(correlationId: string): Promise<PersistedOutboxEvent> {
    const row = await enqueue({ correlationId });
    await db.update(OUTBOX_COLLECTION, row._id, { status: 'FAILED', attempts: 3, lastError: 'nacked' });
    return row;
  }

  it('puts FAILED rows back to PENDING with a fresh attempt budget, keeping lastError for the audit trail', async () => {
    const a = await park('a');
    await park('b');
    const pending = await enqueue({ correlationId: 'pending' });

    const retried = await outbox.retryFailed();

    expect(retried.map((r) => r.correlationId)).toEqual(['a', 'b']);
    expect(retried[0]).toMatchObject({ status: 'PENDING', attempts: 0, lastError: 'nacked' });
    expect((await rows()).find((r) => r._id === pending._id)?.attempts).toBe(0);
    expect(await outbox.claim({ publisherId: 'p', limit: 10, leaseMs: 1000 })).toHaveLength(3);
    expect((await rows()).find((r) => r._id === a._id)?.lockedBy).toBe('p');
  });

  it('can be restricted to specific ids, ignores non-FAILED ids, and wakes listeners', async () => {
    const a = await park('a');
    await park('b');
    const listener = vi.fn();
    const off = outbox.onEnqueued(listener);
    try {
      const retried = await outbox.retryFailed([a._id, 'unknown']);
      expect(retried.map((r) => r._id)).toEqual([a._id]);
      expect(listener).toHaveBeenCalledTimes(1);

      expect(await outbox.retryFailed(['unknown'])).toEqual([]);
      expect(listener).toHaveBeenCalledTimes(1); // nothing retried, nobody woken
    } finally {
      off();
    }
  });
});

describe('prune()', () => {
  it('deletes PUBLISHED rows older than the retention, keeps recent / PENDING / FAILED ones, and 0 disables pruning', async () => {
    const old = await enqueue({ correlationId: 'old' });
    const recent = await enqueue({ correlationId: 'recent' });
    const failed = await enqueue({ correlationId: 'failed' });
    await enqueue({ correlationId: 'pending' });
    await db.update(OUTBOX_COLLECTION, old._id, { status: 'PUBLISHED', publishedAt: at(-10_000).toISOString() });
    await db.update(OUTBOX_COLLECTION, recent._id, { status: 'PUBLISHED', publishedAt: at(-1000).toISOString() });
    await db.update(OUTBOX_COLLECTION, failed._id, { status: 'FAILED' });

    expect(await outbox.prune(0, T0)).toBe(0);
    expect(await outbox.prune(5000, T0)).toBe(1);
    expect((await rows()).map((r) => r.correlationId)).toEqual(['recent', 'failed', 'pending']);
  });
});

describe('stats() / list()', () => {
  it('counts by status, in-flight leases and the age of the oldest pending row', async () => {
    const a = await enqueue({ correlationId: 'a' });
    await enqueue({ correlationId: 'b' });
    const c = await enqueue({ correlationId: 'c' });
    const d = await enqueue({ correlationId: 'd' });
    await db.update(OUTBOX_COLLECTION, a._id, { occurredAt: at(-60_000).toISOString() });
    await db.update(OUTBOX_COLLECTION, c._id, { status: 'PUBLISHED' });
    await db.update(OUTBOX_COLLECTION, d._id, { status: 'FAILED' });
    await outbox.claim({ publisherId: 'p', limit: 1, leaseMs: 30_000, now: T0 });

    expect(await outbox.stats(T0)).toEqual({ pending: 2, inFlight: 1, published: 1, deferred: 0, restored: 0, failed: 1, oldestPendingAgeMs: 60_000, oldestDeferredAgeMs: null });
    expect(await outbox.stats(at(31_000))).toMatchObject({ inFlight: 0 });

    await db.clear(OUTBOX_COLLECTION);
    expect(await outbox.stats(T0)).toEqual({ pending: 0, inFlight: 0, published: 0, deferred: 0, restored: 0, failed: 0, oldestPendingAgeMs: null, oldestDeferredAgeMs: null });
  });

  it('lists rows oldest first, optionally filtered by status and bounded', async () => {
    await enqueue({ correlationId: 'a' });
    const b = await enqueue({ correlationId: 'b' });
    await enqueue({ correlationId: 'c' });
    await db.update(OUTBOX_COLLECTION, b._id, { status: 'FAILED' });

    expect((await outbox.list()).map((r) => r.correlationId)).toEqual(['a', 'b', 'c']);
    expect((await outbox.list({ status: 'FAILED' })).map((r) => r.correlationId)).toEqual(['b']);
    expect((await outbox.list({ status: 'PENDING', limit: 1 })).map((r) => r.correlationId)).toEqual(['a']);
  });
});

// ─── Deferred verifications ───────────────────────────────────────────────────

describe('deferred verifications', () => {
  it('defer() stores a DEFERRED row keyed by the idempotency key, carrying the processor input', async () => {
    const row = await outbox.defer(deferInput());

    expect(row).toMatchObject({
      eventType: DEFERRED_VERIFICATION,
      aggregateType: 'payment',
      aggregateId: '/v1/checkouts/d/payment',
      correlationId: 'c-d',
      status: 'DEFERRED',
      attempts: 0,
      payload: { resourcePath: '/v1/checkouts/d/payment', source: 'verify', notifyUnchanged: true, reason: 'circuit oppwa OPEN' },
    });
  });

  it('is invisible to the publisher\'s claim and only leased with status: DEFERRED, FIFO', async () => {
    const event = await enqueue({ correlationId: 'event' });
    const first = await outbox.defer(deferInput({ correlationId: 'd1' }));
    await outbox.defer(deferInput({ correlationId: 'd2' }));

    const published = await outbox.claim({ publisherId: 'pub', limit: 10, leaseMs: 1000 });
    expect(published.map((r) => r._id)).toEqual([event._id]);

    const deferred = await outbox.claim({ publisherId: 'rec', limit: 1, leaseMs: 1000, status: 'DEFERRED' });
    expect(deferred.map((r) => r._id)).toEqual([first._id]);
    expect(deferred[0]).toMatchObject({ lockedBy: 'rec', attempts: 1, status: 'DEFERRED' });
  });

  it('markRestored() completes a deferred row; a failure keeps it DEFERRED with backoff and parks it after maxAttempts', async () => {
    const good = await outbox.defer(deferInput({ correlationId: 'good' }));
    const bad = await outbox.defer(deferInput({ correlationId: 'bad' }));
    await outbox.claim({ publisherId: 'rec', limit: 2, leaseMs: 1000, status: 'DEFERRED', now: T0 });

    expect(await outbox.markRestored(good._id, 'rec', at(5))).toMatchObject({ status: 'RESTORED', publishedAt: at(5).toISOString() });
    expect(await outbox.markFailed(bad._id, 'rec', new Error('bad path'), { ...FAILURE, now: T0 })).toMatchObject({ status: 'DEFERRED', attempts: 1, nextAttemptAt: at(100).toISOString(), lastError: 'bad path' });

    for (const step of [1, 2]) {
      await outbox.claim({ publisherId: 'rec', limit: 1, leaseMs: 1000, status: 'DEFERRED', now: at(step * 10_000) });
      await outbox.markFailed(bad._id, 'rec', new Error('bad path'), { ...FAILURE, now: at(step * 10_000) });
    }
    expect((await rows()).find((r) => r._id === bad._id)).toMatchObject({ status: 'FAILED', attempts: 3 });
  });

  it('retryFailed() sends a parked deferred row back to DEFERRED (not PENDING) and does not wake the publisher', async () => {
    const row = await outbox.defer(deferInput());
    await db.update(OUTBOX_COLLECTION, row._id, { status: 'FAILED', attempts: 3 });
    const listener = vi.fn();
    const off = outbox.onEnqueued(listener);
    try {
      const [retried] = await outbox.retryFailed();
      expect(retried).toMatchObject({ status: 'DEFERRED', attempts: 0 });
      expect(listener).not.toHaveBeenCalled();
    } finally {
      off();
    }
  });

  it('stats() and prune() cover deferred / restored rows', async () => {
    const old = await outbox.defer(deferInput({ correlationId: 'old' }));
    await outbox.defer(deferInput({ correlationId: 'waiting' }));
    await db.update(OUTBOX_COLLECTION, old._id, { status: 'RESTORED', publishedAt: at(-10_000).toISOString() });
    await db.update(OUTBOX_COLLECTION, (await rows())[1]?._id ?? '', { occurredAt: at(-3000).toISOString() });

    expect(await outbox.stats(T0)).toMatchObject({ deferred: 1, restored: 1, oldestDeferredAgeMs: 3000, oldestPendingAgeMs: null });
    expect(await outbox.prune(5000, T0)).toBe(1);
    expect((await rows()).map((r) => r.correlationId)).toEqual(['waiting']);
  });
});
