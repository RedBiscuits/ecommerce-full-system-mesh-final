import crypto from 'crypto';
import os from 'os';
import { EventEmitter } from 'events';
import type { Channel, ChannelModel, ConfirmChannel } from 'amqplib';
import type { AmqpConnect } from './connect';
import { ReconnectPolicy, RecoveringConnection } from './RecoveringConnection';
import { RetryPolicy, assertValidRetryPolicy, jitteredRetryDelay } from './backoff';
import { errorMessage, truncate } from './errors';
import { publishConfirmed, type LogFn } from './ReliableConsumer';
import type { ExchangeType } from './topology';

/** One row of a transactional outbox, as the relay sees it. */
export interface OutboxRecord {
  eventId: string;
  eventType: string;
  exchange: string;
  routingKey: string;
  aggregateId: string;
  correlationId: string | null;
  /** Exactly what goes on the wire (the integration event envelope). */
  body: unknown;
  /** Claims so far, this one included. */
  attempts: number;
}

export interface OutboxStats {
  pending: number;
  published: number;
  failed: number;
  oldestPendingAgeMs: number | null;
}

/**
 * Persistence port of the relay. Implementations must make `claim` a lease:
 * a claimed row is invisible to other relays until it is marked, released or
 * its lease expires (a relay that died mid-publish).
 */
export interface OutboxStore {
  claim(input: { relayId: string; limit: number; leaseMs: number }): Promise<OutboxRecord[]>;
  markPublished(eventId: string, relayId: string): Promise<void>;
  /** `retryAt = null` parks the row as FAILED for an operator. */
  markFailed(eventId: string, relayId: string, error: string, retryAt: Date | null): Promise<void>;
  /** Give leases back without charging the attempt (transport lost mid-batch). */
  release(eventIds: string[], relayId: string): Promise<void>;
  /** Put FAILED rows back to PENDING with a fresh attempt budget. */
  retryFailed(eventIds?: string[]): Promise<number>;
  stats(): Promise<OutboxStats>;
}

export interface OutboxRelayOptions {
  name: string;
  store: OutboxStore;
  url: string;
  connect: AmqpConnect;
  /** Exchanges this relay publishes to; declared on every (re)connect. */
  exchanges: Array<{ name: string; type: ExchangeType }>;
  /** Safety-net polling; `wake()` after a commit is the fast path. 0 disables the timer. */
  pollIntervalMs?: number;
  batchSize?: number;
  leaseMs?: number;
  /** Publish attempts before a row is parked as FAILED; delays use full jitter. */
  retry?: Partial<RetryPolicy>;
  publishTimeoutMs?: number;
  reconnect?: Partial<ReconnectPolicy>;
  relayId?: string;
  /**
   * Service-to-service authentication headers for each event (key id, stream, issuer, audience,
   * HMAC-SHA256 over the body; see shared/security). Omitted = unsigned.
   */
  signHeaders?: (row: OutboxRecord, body: Buffer) => Record<string, string>;
  log?: LogFn;
}

export interface RelayCycle {
  claimed: number;
  published: number;
  /** Confirmed by the broker but matched no queue (nobody subscribes to that routing key yet). */
  unroutable: number;
  failed: number;
  released: number;
  skipped?: 'no-connection' | 'closed';
  error?: string;
}

const DEFAULT_RELAY_RETRY: RetryPolicy = { maxAttempts: 10, baseDelayMs: 500, maxDelayMs: 60_000, minDelayMs: 50 };

/**
 * Relay half of a transactional outbox: moves committed outbox rows to the
 * broker. A row is marked PUBLISHED only after the broker's publisher confirm,
 * so delivery is at-least-once; the AMQP messageId is the event id and every
 * consumer de-duplicates on it. Broker outages are a no-op (rows wait in the
 * database) and the relay drains the backlog in commit order on reconnect.
 */
export class OutboxRelay extends EventEmitter {
  readonly relayId: string;
  private readonly retry: RetryPolicy;
  private connection?: RecoveringConnection;
  private channel: ConfirmChannel | null = null;
  private channelAlive = false;
  private started = false;
  private closed = false;
  private timer: NodeJS.Timeout | null = null;
  private running: Promise<RelayCycle> | null = null;
  private wakeRequested = false;

  constructor(private readonly options: OutboxRelayOptions) {
    super();
    this.setMaxListeners(0);
    this.retry = { ...DEFAULT_RELAY_RETRY, ...options.retry };
    assertValidRetryPolicy(this.retry);
    this.relayId = options.relayId ?? `${options.name}:${os.hostname()}:${process.pid}:${crypto.randomBytes(3).toString('hex')}`;
  }

  async start(): Promise<void> {
    if (this.closed) throw new Error(`Outbox relay ${this.options.name} is closed`);
    if (this.started) return;
    this.started = true;
    const connection = new RecoveringConnection(
      () => this.options.connect(this.options.url),
      (conn) => this.setupSession(conn),
      this.options.reconnect
    );
    connection.on('disconnected', (err: Error) => {
      this.channelAlive = false;
      this.channel = null;
      this.log('warn', 'broker session lost; outbox rows wait in the database', { error: err.message });
    });
    connection.on('connected', () => this.wake());
    connection.on('connection-error', (err: Error) => this.log('error', 'broker connection error', { error: err.message }));
    this.connection = connection;
    await connection.start();
    this.schedule();
    this.wake();
  }

  /** Stop polling, finish the cycle in progress, disconnect. Unpublished rows stay in the DB. */
  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
    if (this.running) await this.running;
    this.channelAlive = false;
    this.channel = null;
    await this.connection?.close();
  }

  /** Request a cycle as soon as possible (coalesces; safe to call after every commit). */
  wake(): void {
    if (!this.started || this.closed) return;
    void this.runOnce();
  }

  /** Run one drain cycle now (or join the one in progress). */
  runOnce(): Promise<RelayCycle> {
    if (this.running) {
      this.wakeRequested = true;
      return this.running;
    }
    this.wakeRequested = false;
    this.running = this.cycle()
      .catch((err: unknown): RelayCycle => {
        this.log('error', 'relay cycle failed', { error: errorMessage(err) });
        return { claimed: 0, published: 0, unroutable: 0, failed: 0, released: 0, error: errorMessage(err) };
      })
      .then((summary) => {
        this.running = null;
        this.emit('cycle', summary);
        if (this.wakeRequested && !this.closed) {
          this.wakeRequested = false;
          setImmediate(() => void this.runOnce());
        }
        return summary;
      });
    return this.running;
  }

  /** Resolves once a full cycle finds nothing left to publish (tests / shutdown drains). */
  async drain(maxCycles = 50): Promise<void> {
    for (let i = 0; i < maxCycles; i++) {
      const cycle = await this.runOnce();
      if (cycle.claimed === 0 && !cycle.skipped) return;
      if (cycle.skipped) return;
    }
  }

  stats(): Promise<OutboxStats> {
    return this.options.store.stats();
  }

  // ── internals ──────────────────────────────────────────────────────────────

  private async setupSession(connection: ChannelModel): Promise<void> {
    const channel = await connection.createConfirmChannel();
    this.guardChannel(channel);
    for (const exchange of this.options.exchanges) {
      await channel.assertExchange(exchange.name, exchange.type, { durable: true });
    }
    channel.once('close', () => {
      if (this.channel === channel) {
        this.channelAlive = false;
        this.channel = null;
      }
    });
    this.channel = channel;
    this.channelAlive = true;
  }

  private async cycle(): Promise<RelayCycle> {
    const summary: RelayCycle = { claimed: 0, published: 0, unroutable: 0, failed: 0, released: 0 };
    if (this.closed) return { ...summary, skipped: 'closed' };
    const batchSize = this.options.batchSize ?? 50;
    const leaseMs = this.options.leaseMs ?? 30_000;

    for (;;) {
      const channel = this.channel;
      if (!channel || !this.channelAlive) return { ...summary, skipped: summary.claimed === 0 ? 'no-connection' : undefined };

      const batch = await this.options.store.claim({ relayId: this.relayId, limit: batchSize, leaseMs });
      summary.claimed += batch.length;
      if (batch.length === 0) break;

      let progressed = false;
      for (const [index, row] of batch.entries()) {
        if (this.channel !== channel || !this.channelAlive) {
          const rest = batch.slice(index).map((r) => r.eventId);
          await this.options.store.release(rest, this.relayId);
          summary.released += rest.length;
          return summary;
        }
        try {
          const body = Buffer.from(JSON.stringify(row.body));
          const signature = this.options.signHeaders?.(row, body) ?? {};
          await publishConfirmed(
            channel,
            row.exchange,
            row.routingKey,
            body,
            {
              persistent: true,
              contentType: 'application/json',
              messageId: row.eventId,
              type: row.eventType,
              correlationId: row.correlationId ?? undefined,
              timestamp: Math.floor(Date.now() / 1000),
              appId: this.options.name,
              headers: { 'x-aggregate-id': row.aggregateId, 'x-outbox-attempt': row.attempts, ...signature },
            },
            this.options.publishTimeoutMs ?? 10_000
          );
          await this.options.store.markPublished(row.eventId, this.relayId);
          summary.published += 1;
          progressed = true;
          this.emit('published', row);
        } catch (err) {
          if (isUnroutable(err)) {
            // RabbitMQ confirms a non-mandatory publish that matches no binding (and drops it);
            // the in-process broker reports it instead. Same outcome: the broker has taken it.
            await this.options.store.markPublished(row.eventId, this.relayId);
            summary.published += 1;
            summary.unroutable += 1;
            progressed = true;
            this.log('warn', 'event matched no queue (no subscriber for this routing key)', {
              eventId: row.eventId,
              exchange: row.exchange,
              routingKey: row.routingKey,
            });
            this.emit('published', row);
            continue;
          }
          if (this.channel !== channel || !this.channelAlive) {
            // Transport lost, not the row's fault: hand the rest back uncharged.
            const rest = batch.slice(index).map((r) => r.eventId);
            await this.options.store.release(rest, this.relayId);
            summary.released += rest.length;
            summary.error = errorMessage(err);
            return summary;
          }
          const exhausted = row.attempts >= this.retry.maxAttempts;
          const retryAt = exhausted ? null : new Date(Date.now() + jitteredRetryDelay(row.attempts, this.retry));
          await this.options.store.markFailed(row.eventId, this.relayId, truncate(errorMessage(err)), retryAt);
          summary.failed += 1;
          this.log(exhausted ? 'error' : 'warn', exhausted ? 'outbox row parked as FAILED' : 'publish failed; will retry', {
            eventId: row.eventId,
            attempts: row.attempts,
            error: errorMessage(err),
          });
        }
      }
      if (batch.length < batchSize || !progressed) break;
    }
    return summary;
  }

  private schedule(): void {
    const interval = this.options.pollIntervalMs ?? 1_000;
    if (interval <= 0 || this.closed) return;
    this.timer = setTimeout(() => {
      this.timer = null;
      void this.runOnce().finally(() => this.schedule());
    }, interval);
    this.timer.unref();
  }

  private guardChannel(ch: Channel): void {
    ch.on('error', (err: Error) => this.log('warn', 'publish channel error', { error: err.message }));
  }

  private log(level: 'info' | 'warn' | 'error', msg: string, meta?: Record<string, unknown>): void {
    this.options.log?.(level, `[outbox:${this.options.name}] ${msg}`, meta);
  }
}

/** The in-process broker rejects the confirm of a publish that matched no queue. */
function isUnroutable(err: unknown): boolean {
  return err instanceof Error && err.message === 'message undelivered';
}
