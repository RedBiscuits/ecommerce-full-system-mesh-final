/**
 * src/services/outbox.ts
 *
 * The transactional outbox — the DB side of "write the state change and the
 * event announcing it in one transaction, publish later".
 *
 * Why: publishing to RabbitMQ straight after a DB write is a dual write. If
 * the process dies, the channel drops, or the broker is unreachable between
 * the two, the record exists but downstream never hears about it (or the
 * reverse). With the outbox the *only* write on the request path is the DB
 * transaction; the event row is part of it, so it cannot be lost, and a
 * separate publisher (messaging/outboxPublisher.ts) moves rows to the broker
 * with publisher confirms and retries until each one is acknowledged.
 *
 * ─── Row lifecycle ───────────────────────────────────────────────────────────
 *
 *   enqueue()        PENDING, unlocked                       (inside the caller's tx)
 *   claim()          PENDING, lockedBy/lockedUntil, attempts+1  (publisher takes a lease)
 *   markPublished()  PUBLISHED, publishedAt                  (broker confirmed)
 *   markFailed()     PENDING + nextAttemptAt (backoff)       (nack / timeout)
 *                    FAILED after maxAttempts               (parked for an operator)
 *   release()        lock cleared, attempt refunded          (transport gone mid-batch)
 *   retryFailed()    FAILED → PENDING / DEFERRED             (operator)
 *   prune()          PUBLISHED / RESTORED rows older than retention are deleted
 *
 * ─── Deferred work ───────────────────────────────────────────────────────────
 *
 * The same table also holds *inbound* work that could not be done because the
 * gateway circuit was open (see gatewayRecovery.ts): a verification request
 * is stored as a DEFERRED row (`defer()`), invisible to the publisher, and
 * replayed later — `claim({ status: 'DEFERRED' })` → process → `markRestored()`.
 * Failures use the same backoff / parking as publishing.
 *
 * ─── Row-level locking on a JSON file ────────────────────────────────────────
 *
 * `claim()` is the flat-file equivalent of
 *
 *   UPDATE outbox_events SET locked_by = $me, locked_until = now() + $lease
 *   WHERE id IN (SELECT id FROM outbox_events
 *                WHERE status = 'PENDING'
 *                  AND (locked_until IS NULL OR locked_until < now())
 *                  AND (next_attempt_at IS NULL OR next_attempt_at <= now())
 *                ORDER BY id LIMIT $batch FOR UPDATE SKIP LOCKED)
 *   RETURNING *;
 *
 * The select-and-mark happens inside one `db.transaction`, which holds the
 * DB mutex and commits atomically, so two claimants can never take the same
 * row; a row whose lease expired (its claimant died mid-publish) becomes
 * claimable again. Every state transition checks the lease owner, so a slow
 * publisher whose lease expired cannot clobber the work of the one that took
 * over. Rows are processed in insertion (FIFO) order.
 */

import { OUTBOX_COLLECTION } from './collections';
import { db } from './jsonDb';
import type { DbRecord, DbTransaction } from './jsonDb';
import type { DomainEventPayload, DomainEventType } from '../types/events';
import type { PaymentResultSource } from '../types/rabbitmq';

export { OUTBOX_COLLECTION } from './collections';

// ─── Types ────────────────────────────────────────────────────────────────────

/**
 *   PENDING    outbound event waiting for the publisher
 *   PUBLISHED  broker confirmed it
 *   DEFERRED   inbound verification waiting for the gateway circuit to close
 *   RESTORED   deferred verification replayed successfully
 *   FAILED     parked after maxAttempts (either kind) — operator retry
 */
export type OutboxStatus = 'PENDING' | 'PUBLISHED' | 'DEFERRED' | 'RESTORED' | 'FAILED';

export type AggregateType = 'payment' | 'operation' | 'payment_method' | 'refund';

/** Row type of a deferred verification (not a domain event; never published). */
export const DEFERRED_VERIFICATION = 'PAYMENT_VERIFICATION_DEFERRED';

export type OutboxEventType = DomainEventType | typeof DEFERRED_VERIFICATION;

/** A `processPaymentEvent` input, parked until the gateway is reachable again. */
export interface DeferredVerificationPayload {
  resourcePath?: string;
  checkoutId?: string;
  source: PaymentResultSource;
  correlationId?: string;
  notifyUnchanged?: boolean;
  /** Why it was deferred — the circuit state at the time. */
  reason: string;
}

export type OutboxPayload = DomainEventPayload | DeferredVerificationPayload;

/** Row in the `outbox_events` collection. `_id` doubles as the event id. */
export interface OutboxEvent {
  eventType: OutboxEventType;
  aggregateType: AggregateType;
  /** `_id` of the record the event is about (the idempotency key for deferred rows). */
  aggregateId: string;
  correlationId: string;
  exchange: string;
  routingKey: string;
  /** Wire body minus the envelope (eventId / eventType / occurredAt come from the row). */
  payload: OutboxPayload;
  /** When the state change was committed — the transaction that inserted this row. */
  occurredAt: string;
  status: OutboxStatus;
  /**
   * Times the row has been leased for publishing (the successful one
   * included). Counted at claim time, so an attempt that died mid-flight
   * still counts — a row that keeps killing its publisher ends up parked.
   */
  attempts: number;
  /** Do not claim before this instant (retry backoff). */
  nextAttemptAt?: string;
  lockedBy?: string;
  lockedUntil?: string;
  publishedAt?: string;
  lastError?: string;
}

export type PersistedOutboxEvent = OutboxEvent & DbRecord;

export interface EnqueueInput {
  eventType: DomainEventType;
  aggregateType: AggregateType;
  aggregateId: string;
  correlationId: string;
  exchange: string;
  routingKey: string;
  payload: DomainEventPayload;
}

export interface DeferInput {
  /** Idempotency key of the verification — identifies the payment before any record exists. */
  idempotencyKey: string;
  correlationId: string;
  payload: DeferredVerificationPayload;
}

export interface ClaimOptions {
  publisherId: string;
  limit: number;
  leaseMs: number;
  /** Which kind of row to lease. Default PENDING (outbound events). */
  status?: 'PENDING' | 'DEFERRED';
  now?: Date;
}

export interface FailureOptions {
  maxAttempts: number;
  /** Base delay; doubles per attempt, capped at `maxRetryDelayMs`. */
  retryDelayMs: number;
  maxRetryDelayMs: number;
  now?: Date;
}

export interface OutboxStats {
  pending: number;
  /** PENDING / DEFERRED rows currently leased. */
  inFlight: number;
  published: number;
  deferred: number;
  restored: number;
  failed: number;
  /** Age of the oldest unpublished (PENDING) row in ms, or null when none. */
  oldestPendingAgeMs: number | null;
  /** Age of the oldest DEFERRED row in ms, or null when none. */
  oldestDeferredAgeMs: number | null;
}

export interface ListOptions {
  status?: OutboxStatus;
  limit?: number;
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

function iso(date: Date): string {
  return date.toISOString();
}

function plus(date: Date, ms: number): string {
  return iso(new Date(date.getTime() + ms));
}

/** Exponential backoff for the (1-based) attempt that just failed. */
export function backoffDelay(attempt: number, options: Pick<FailureOptions, 'retryDelayMs' | 'maxRetryDelayMs'>): number {
  const exp = options.retryDelayMs * 2 ** Math.max(0, attempt - 1);
  return Math.min(exp, options.maxRetryDelayMs);
}

function isClaimable(row: PersistedOutboxEvent, status: OutboxStatus, now: string): boolean {
  if (row.status !== status) return false;
  if (row.lockedUntil && row.lockedUntil > now) return false;
  if (row.nextAttemptAt && row.nextAttemptAt > now) return false;
  return true;
}

/** Where a row goes back to after a failed attempt or an operator retry. */
function restingStatus(row: OutboxEvent): 'PENDING' | 'DEFERRED' {
  return row.eventType === DEFERRED_VERIFICATION ? 'DEFERRED' : 'PENDING';
}

function isLeased(row: PersistedOutboxEvent, now: string): boolean {
  return row.lockedUntil !== undefined && row.lockedUntil > now;
}

function ageOfOldest(rows: PersistedOutboxEvent[], now: Date): number | null {
  const oldest = rows.reduce<string | null>((min, row) => (min === null || row.occurredAt < min ? row.occurredAt : min), null);
  return oldest === null ? null : Math.max(0, now.getTime() - new Date(oldest).getTime());
}

function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

/** Build the patch that drops the lease fields. */
const UNLOCKED = { lockedBy: undefined, lockedUntil: undefined } as const;

// ─── Commit notifications ─────────────────────────────────────────────────────

type Listener = () => void;
const listeners = new Set<Listener>();

/**
 * Be told whenever a transaction that enqueued events has committed. The
 * publisher uses this to publish promptly instead of waiting for its next
 * poll; polling remains the guarantee.
 */
export function onEnqueued(listener: Listener): () => void {
  listeners.add(listener);
  return (): void => {
    listeners.delete(listener);
  };
}

function notifyEnqueued(): void {
  for (const listener of listeners) {
    try {
      listener();
    } catch (err) {
      console.error('[Outbox] enqueue listener failed:', err);
    }
  }
}

// ─── Write side (inside the caller's transaction) ─────────────────────────────

/**
 * Record an event in the same transaction as the state change it describes.
 * Resolves with the persisted row; its `_id` is the event id.
 */
export async function enqueue(tx: DbTransaction, input: EnqueueInput): Promise<PersistedOutboxEvent> {
  const row: OutboxEvent = {
    ...input,
    occurredAt: iso(new Date()),
    status: 'PENDING',
    attempts: 0,
  };
  const saved = await tx.insert<OutboxEvent>(OUTBOX_COLLECTION, row);
  tx.onCommit(notifyEnqueued);
  return saved;
}

/**
 * Park a verification request that could not reach the gateway. Its own
 * transaction: there is no state change to be atomic with — the point is
 * that the request is durable before the message is ACKed.
 */
export function defer(input: DeferInput): Promise<PersistedOutboxEvent> {
  return db.transaction((tx) =>
    tx.insert<OutboxEvent>(OUTBOX_COLLECTION, {
      eventType: DEFERRED_VERIFICATION,
      aggregateType: 'payment',
      aggregateId: input.idempotencyKey,
      correlationId: input.correlationId,
      exchange: '',
      routingKey: '',
      payload: input.payload,
      occurredAt: iso(new Date()),
      status: 'DEFERRED',
      attempts: 0,
    }),
  );
}

// ─── Publisher / recovery side ────────────────────────────────────────────────

/**
 * Lease up to `limit` claimable rows for `publisherId`. Returns the rows as
 * they are after the lease was written (lock fields set, attempt counted).
 */
export function claim(options: ClaimOptions): Promise<PersistedOutboxEvent[]> {
  const now = options.now ?? new Date();
  const nowIso = iso(now);
  const lockedUntil = plus(now, options.leaseMs);
  const status = options.status ?? 'PENDING';

  return db.transaction(async (tx) => {
    const rows = await tx.findAll<PersistedOutboxEvent>(OUTBOX_COLLECTION);
    const claimable = rows.filter((row) => isClaimable(row, status, nowIso)).slice(0, options.limit);

    const claimed: PersistedOutboxEvent[] = [];
    for (const row of claimable) {
      const leased = await tx.update<PersistedOutboxEvent>(OUTBOX_COLLECTION, row._id, {
        lockedBy: options.publisherId,
        lockedUntil,
        attempts: row.attempts + 1,
      });
      if (leased) claimed.push(leased);
    }
    return claimed;
  });
}

/** The broker confirmed the event. Only the lease holder may mark it. */
export function markPublished(id: string, publisherId: string, now: Date = new Date()): Promise<PersistedOutboxEvent | null> {
  return markDone(id, publisherId, 'PUBLISHED', now);
}

/** A deferred verification was replayed successfully. */
export function markRestored(id: string, publisherId: string, now: Date = new Date()): Promise<PersistedOutboxEvent | null> {
  return markDone(id, publisherId, 'RESTORED', now);
}

function markDone(id: string, publisherId: string, status: 'PUBLISHED' | 'RESTORED', now: Date): Promise<PersistedOutboxEvent | null> {
  return db.transaction(async (tx) => {
    const row = await tx.findById<PersistedOutboxEvent>(OUTBOX_COLLECTION, id);
    if (!row || row.status === status) return row;
    if (row.lockedBy !== publisherId) {
      console.warn(`[Outbox] ${id} completed by ${publisherId} but leased by ${row.lockedBy ?? 'nobody'} — lease expired mid-flight`);
    }
    return tx.update<PersistedOutboxEvent>(OUTBOX_COLLECTION, id, {
      status,
      publishedAt: iso(now),
      nextAttemptAt: undefined,
      lastError: undefined,
      ...UNLOCKED,
    });
  });
}

/**
 * The attempt failed (nack, timeout). Schedules a retry with exponential
 * backoff, or parks the event as FAILED once `maxAttempts` is reached.
 */
export function markFailed(
  id: string,
  publisherId: string,
  error: unknown,
  options: FailureOptions,
): Promise<PersistedOutboxEvent | null> {
  const now = options.now ?? new Date();
  return db.transaction(async (tx) => {
    const row = await tx.findById<PersistedOutboxEvent>(OUTBOX_COLLECTION, id);
    if (!row || (row.status !== 'PENDING' && row.status !== 'DEFERRED')) return row;
    if (row.lockedBy !== publisherId) return row; // someone else owns it now

    const exhausted = row.attempts >= options.maxAttempts;
    return tx.update<PersistedOutboxEvent>(OUTBOX_COLLECTION, id, {
      lastError: errorMessage(error),
      status: exhausted ? 'FAILED' : restingStatus(row),
      nextAttemptAt: exhausted ? undefined : plus(now, backoffDelay(row.attempts, options)),
      ...UNLOCKED,
    });
  });
}

/**
 * Give leases back without charging the attempt (e.g. the channel died
 * mid-batch before these rows were even tried).
 */
export function release(ids: string[], publisherId: string): Promise<number> {
  if (ids.length === 0) return Promise.resolve(0);
  return db.transaction(async (tx) => {
    let released = 0;
    for (const id of ids) {
      const row = await tx.findById<PersistedOutboxEvent>(OUTBOX_COLLECTION, id);
      if (!row || row.lockedBy !== publisherId) continue;
      await tx.update<PersistedOutboxEvent>(OUTBOX_COLLECTION, id, { attempts: Math.max(0, row.attempts - 1), ...UNLOCKED });
      released += 1;
    }
    return released;
  });
}

// ─── Operations ───────────────────────────────────────────────────────────────

/**
 * Put FAILED rows (all, or the given ids) back in their queue — PENDING for
 * events, DEFERRED for verifications — with a fresh attempt budget.
 * `lastError` is kept for the audit trail.
 */
export function retryFailed(ids?: string[]): Promise<PersistedOutboxEvent[]> {
  return db.transaction(async (tx) => {
    const failed = await tx.findWhere<PersistedOutboxEvent>(
      OUTBOX_COLLECTION,
      (row) => row.status === 'FAILED' && (!ids || ids.includes(row._id)),
    );
    const retried: PersistedOutboxEvent[] = [];
    for (const row of failed) {
      const updated = await tx.update<PersistedOutboxEvent>(OUTBOX_COLLECTION, row._id, {
        status: restingStatus(row),
        attempts: 0,
        nextAttemptAt: undefined,
        ...UNLOCKED,
      });
      if (updated) retried.push(updated);
    }
    if (retried.some((row) => row.status === 'PENDING')) tx.onCommit(notifyEnqueued);
    return retried;
  });
}

/** Delete PUBLISHED / RESTORED rows older than `retentionMs`. Returns how many went. */
export function prune(retentionMs: number, now: Date = new Date()): Promise<number> {
  if (retentionMs <= 0) return Promise.resolve(0);
  const cutoff = iso(new Date(now.getTime() - retentionMs));
  return db.remove<PersistedOutboxEvent>(
    OUTBOX_COLLECTION,
    (row) => (row.status === 'PUBLISHED' || row.status === 'RESTORED') && (row.publishedAt ?? row.occurredAt) < cutoff,
  );
}

export async function list(options: ListOptions = {}): Promise<PersistedOutboxEvent[]> {
  const rows = await db.findAll<PersistedOutboxEvent>(OUTBOX_COLLECTION);
  const filtered = options.status ? rows.filter((row) => row.status === options.status) : rows;
  return options.limit === undefined ? filtered : filtered.slice(0, options.limit);
}

export async function stats(now: Date = new Date()): Promise<OutboxStats> {
  const rows = await db.findAll<PersistedOutboxEvent>(OUTBOX_COLLECTION);
  const nowIso = iso(now);
  const count = (status: OutboxStatus): number => rows.filter((row) => row.status === status).length;
  const pending = rows.filter((row) => row.status === 'PENDING');
  const deferred = rows.filter((row) => row.status === 'DEFERRED');
  return {
    pending: pending.length,
    inFlight: [...pending, ...deferred].filter((row) => isLeased(row, nowIso)).length,
    published: count('PUBLISHED'),
    deferred: deferred.length,
    restored: count('RESTORED'),
    failed: count('FAILED'),
    oldestPendingAgeMs: ageOfOldest(pending, now),
    oldestDeferredAgeMs: ageOfOldest(deferred, now),
  };
}

export const outbox = { enqueue, defer, claim, markPublished, markRestored, markFailed, release, retryFailed, prune, list, stats, onEnqueued };
