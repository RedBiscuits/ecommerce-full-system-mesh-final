/**
 * src/messaging/outboxPublisher.ts
 *
 * The relay half of the transactional outbox: moves rows from
 * `outbox_events` to RabbitMQ.
 *
 *   poll / wake ──► claim a batch (lease)
 *                      │
 *                      ▼  for each row, in order
 *                   publish on a confirm channel ──► broker confirms ──► markPublished
 *                      │
 *                      ├─ nack / timeout  ──► markFailed (backoff; FAILED after maxAttempts)
 *                      └─ channel gone    ──► release the rest of the batch, end the cycle
 *
 * Guarantees:
 *   - At-least-once. A row is marked PUBLISHED only after the broker's
 *     publisher confirm. A crash between confirm and mark leaves the lease
 *     to expire and the row is published again — consumers de-duplicate on
 *     `eventId` / AMQP `messageId`.
 *   - No loss while the broker is away. With no connection a cycle is a
 *     no-op; rows wait in the DB. A reconnect wakes the publisher, which
 *     drains the backlog in insertion order.
 *   - One cycle at a time (timeout chain, wake requests coalesce), so a
 *     row is never in flight twice from this process. Leases cover the
 *     multi-process case.
 *   - Poison rows do not block the queue: after `maxAttempts` they are
 *     parked as FAILED for an operator (GET/POST /api/admin/outbox) and the
 *     others carry on.
 *   - Bounded file growth: PUBLISHED rows older than `retentionMs` are pruned.
 *
 * The publisher never touches the consumer's channel. It opens its own
 * confirm channel on the live connection and reopens it after a reconnect.
 */

import crypto from 'crypto';
import os from 'os';
import { config } from '../config/env';
import { outbox } from '../services/outbox';
import { openConfirmChannel, safeClose } from './amqpUtils';
import { publishOutboxEvent } from './publisher';
import type amqplib from 'amqplib';
import type { AmqpConnection } from './amqpClient';

// ─── Connection Registry ──────────────────────────────────────────────────────

let activeConnection: AmqpConnection | null = null;
const connectionListeners = new Set<(conn: AmqpConnection | null) => void>();

export function getActiveConnection(): AmqpConnection | null {
  return activeConnection;
}

export function setActiveConnection(conn: AmqpConnection | null): void {
  activeConnection = conn;
  for (const fn of connectionListeners) {
    fn(conn);
  }
}

export function onActiveConnectionChange(fn: (conn: AmqpConnection | null) => void): () => void {
  connectionListeners.add(fn);
  return () => connectionListeners.delete(fn);
}

// ─── Types ────────────────────────────────────────────────────────────────────

export interface OutboxPublisherOptions {
  /** 0 disables the timer; `runNow()` / `wake()` still work. */
  pollIntervalMs: number;
  batchSize: number;
  leaseMs: number;
  maxAttempts: number;
  retryDelayMs: number;
  maxRetryDelayMs: number;
  publishTimeoutMs: number;
  /** 0 keeps PUBLISHED rows forever. */
  retentionMs: number;
  getConnection: () => AmqpConnection | null;
  /** Identifies this publisher's leases. Defaults to host:pid:random. */
  publisherId?: string;
  /** Attach wake-up sources on `start()`; the returned function is called on `stop()`. */
  subscribe?: (wake: () => void) => () => void;
  /** Injectable transport for tests. */
  publish?: typeof publishOutboxEvent;
}

export interface OutboxCycle {
  startedAt: string;
  finishedAt: string;
  claimed: number;
  published: number;
  /** Attempts that failed (nack / timeout) — rows scheduled for retry or parked. */
  failed: number;
  /** Leases handed back untouched because the channel died mid-batch. */
  released: number;
  pruned: number;
  /** Why nothing was attempted. */
  skipped?: 'no-connection';
  error?: string;
}

export interface OutboxPublisherState {
  enabled: boolean;
  pollIntervalMs: number;
  publisherId: string;
  running: boolean;
  cycles: number;
  lastCycle?: OutboxCycle;
  nextPollAt?: string;
}

export interface OutboxPublisher {
  start(): void;
  /** Stops the timer and waits for an in-progress cycle; closes the publish channel. */
  stop(): Promise<void>;
  /** Run one cycle now (or join the one in progress) and resolve with its outcome. */
  runNow(): Promise<OutboxCycle>;
  /** Ask for a cycle as soon as possible. Coalesces; safe to call from anywhere. */
  wake(): void;
  getState(): OutboxPublisherState;
}

interface PublishChannel {
  owner: AmqpConnection;
  channel: amqplib.ConfirmChannel;
  closed: boolean;
}

/** Idle cycles between retention sweeps (≈ every minute at the default poll interval). */
const PRUNE_EVERY_CYCLES = 60;

// ─── Factory ──────────────────────────────────────────────────────────────────

export function createOutboxPublisher(options: OutboxPublisherOptions): OutboxPublisher {
  const publisherId = options.publisherId ?? `${os.hostname()}:${process.pid}:${crypto.randomBytes(3).toString('hex')}`;
  const publish = options.publish ?? publishOutboxEvent;
  const enabled = options.pollIntervalMs > 0;

  let started = false;
  let timer: NodeJS.Timeout | null = null;
  let inProgress: Promise<OutboxCycle> | null = null;
  let wakeRequested = false;
  let unsubscribe: (() => void) | null = null;
  let cycles = 0;
  let lastCycle: OutboxCycle | undefined;
  let nextPollAt: string | undefined;
  let current: PublishChannel | null = null;
  /** True while a `runNow()` (manual) cycle is in progress — stop() does not cut it short. */
  let manualCycle = false;

  // ── Channel ────────────────────────────────────────────────────────────────

  async function channelFor(conn: AmqpConnection): Promise<amqplib.ConfirmChannel> {
    if (current && current.owner === conn && !current.closed) return current.channel;
    await closeChannel();

    const channel = await openConfirmChannel(conn.connection, 'outbox');
    const entry: PublishChannel = { owner: conn, channel, closed: false };
    channel.on('close', () => {
      entry.closed = true;
      if (current === entry) current = null;
    });
    current = entry;
    return channel;
  }

  async function closeChannel(): Promise<void> {
    const entry = current;
    current = null;
    if (entry && !entry.closed) await safeClose(entry.channel);
  }

  // ── One cycle ──────────────────────────────────────────────────────────────

  async function cycle(): Promise<OutboxCycle> {
    const summary: OutboxCycle = {
      startedAt: new Date().toISOString(),
      finishedAt: '',
      claimed: 0,
      published: 0,
      failed: 0,
      released: 0,
      pruned: 0,
    };

    const conn = options.getConnection();
    if (!conn) {
      summary.skipped = 'no-connection';
      return finish(summary);
    }

    let channel: amqplib.ConfirmChannel;
    try {
      channel = await channelFor(conn);
    } catch (err) {
      summary.error = `could not open publish channel: ${message(err)}`;
      console.error(`[Outbox] ${summary.error}`);
      return finish(summary);
    }

    drain: for (;;) {
      const batch = await outbox.claim({ publisherId, limit: options.batchSize, leaseMs: options.leaseMs });
      summary.claimed += batch.length;
      if (batch.length === 0) break;

      let progressed = false;
      for (const [i, row] of batch.entries()) {
        if (!started && !manualCycle) {
          // stop() was called mid-batch: hand the rest back untouched.
          summary.released += await outbox.release(batch.slice(i).map((r) => r._id), publisherId);
          break drain;
        }

        try {
          await publish(channel, row, options.publishTimeoutMs);
          await outbox.markPublished(row._id, publisherId);
          summary.published += 1;
          progressed = true;
        } catch (err) {
          if (current?.channel !== channel || current.closed) {
            // Transport failure, not the event's fault: no attempt is charged.
            summary.released += await outbox.release(batch.slice(i).map((r) => r._id), publisherId);
            summary.error = `publish channel lost: ${message(err)}`;
            console.warn(`[Outbox] ${summary.error} — ${summary.released} lease(s) released, will retry`);
            break drain;
          }

          const updated = await outbox.markFailed(row._id, publisherId, err, {
            maxAttempts: options.maxAttempts,
            retryDelayMs: options.retryDelayMs,
            maxRetryDelayMs: options.maxRetryDelayMs,
          });
          summary.failed += 1;
          if (updated?.status === 'FAILED') {
            console.error(`[Outbox] ${row.eventType} eventId=${row._id} PARKED after ${row.attempts} attempt(s): ${message(err)}`);
          } else {
            console.warn(
              `[Outbox] ${row.eventType} eventId=${row._id} attempt ${row.attempts} failed (${message(err)}) — ` +
                `next at ${updated?.nextAttemptAt ?? '?'}`,
            );
          }
        }
      }

      // A partial batch means the backlog is drained; a batch with no
      // progress means everything left is failing — wait for the next poll.
      if (batch.length < options.batchSize || !progressed) break;
    }

    // Prune when this cycle created PUBLISHED rows, and periodically otherwise
    // (an idle service still ages rows out) — not on every poll.
    if (options.retentionMs > 0 && (summary.published > 0 || cycles % PRUNE_EVERY_CYCLES === 0)) {
      summary.pruned = await outbox.prune(options.retentionMs);
    }
    return finish(summary);
  }

  function finish(summary: OutboxCycle): OutboxCycle {
    summary.finishedAt = new Date().toISOString();
    if (summary.claimed > 0 || summary.pruned > 0 || summary.error) {
      console.log(
        `[Outbox] cycle: claimed=${summary.claimed} published=${summary.published} failed=${summary.failed} ` +
          `released=${summary.released} pruned=${summary.pruned}${summary.error ? ` error="${summary.error}"` : ''}`,
      );
    }
    return summary;
  }

  // ── Scheduling ─────────────────────────────────────────────────────────────

  function execute(manual: boolean): Promise<OutboxCycle> {
    if (inProgress) {
      wakeRequested = true;
      return inProgress;
    }
    manualCycle = manual;
    wakeRequested = false;
    inProgress = cycle()
      .catch((err: unknown): OutboxCycle => {
        const msg = message(err);
        console.error(`[Outbox] cycle failed: ${msg}`);
        return { startedAt: new Date().toISOString(), finishedAt: new Date().toISOString(), claimed: 0, published: 0, failed: 0, released: 0, pruned: 0, error: msg };
      })
      .then((summary) => {
        cycles += 1;
        lastCycle = summary;
        inProgress = null;
        manualCycle = false;
        if (wakeRequested && started) {
          wakeRequested = false;
          setImmediate(() => void execute(false));
        }
        return summary;
      });
    return inProgress;
  }

  function schedule(): void {
    if (!started || !enabled || timer) return;
    nextPollAt = new Date(Date.now() + options.pollIntervalMs).toISOString();
    timer = setTimeout(() => {
      timer = null;
      void execute(false).finally(schedule);
    }, options.pollIntervalMs);
    timer.unref();
  }

  function clearTimer(): void {
    if (timer) {
      clearTimeout(timer);
      timer = null;
    }
    nextPollAt = undefined;
  }

  function wake(): void {
    if (!started) return;
    // Run now; the poll timer restarts after the cycle so polling keeps its cadence.
    clearTimer();
    void execute(false).finally(schedule);
  }

  return {
    start(): void {
      if (started) return;
      started = true;
      unsubscribe = options.subscribe?.(wake) ?? null;
      if (enabled) {
        console.log(`[Outbox] publisher ${publisherId} polling every ${options.pollIntervalMs}ms (batch ${options.batchSize}, lease ${options.leaseMs}ms)`);
      } else {
        console.log(`[Outbox] publisher ${publisherId} has no poll timer (OUTBOX_POLL_INTERVAL_MS=0) — publish on wake / demand only`);
      }
      wake(); // drain anything left from a previous run, then schedule the timer
    },

    async stop(): Promise<void> {
      started = false;
      clearTimer();
      unsubscribe?.();
      unsubscribe = null;
      if (inProgress) await inProgress;
      await closeChannel();
    },

    runNow(): Promise<OutboxCycle> {
      return execute(true);
    },

    wake,

    getState(): OutboxPublisherState {
      return {
        enabled,
        pollIntervalMs: options.pollIntervalMs,
        publisherId,
        running: inProgress !== null,
        cycles,
        ...(lastCycle ? { lastCycle } : {}),
        ...(nextPollAt && started ? { nextPollAt } : {}),
      };
    },
  };
}

function message(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

// ─── Process-wide instance ────────────────────────────────────────────────────

/**
 * Wired to config and the live connection registry. Started by server.ts
 * after the HTTP server; woken by every committed outbox write and by every
 * (re)connect; stopped on shutdown before the AMQP client.
 */
export const outboxPublisher: OutboxPublisher = createOutboxPublisher({
  ...config.outbox,
  getConnection: getActiveConnection,
  subscribe: (wake): (() => void) => {
    const offEnqueued = outbox.onEnqueued(wake);
    const offConnection = onActiveConnectionChange((conn) => {
      if (conn) wake();
    });
    return (): void => {
      offEnqueued();
      offConnection();
    };
  },
});
