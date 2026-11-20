import { EventEmitter, once } from 'events';
import type { Channel, ChannelModel, ConfirmChannel, ConsumeMessage, Options } from 'amqplib';
import { EventEnvelope, parseEnvelope } from './events';
import { EventAuthError, EventAuthenticator, SERVICES, eventAuthFor } from '../../../../shared/security';
import { NonRetryableError, PublishRejectedError } from './errors';
import { ReconnectPolicy, RecoveringConnection } from './RecoveringConnection';

/** Opens ONE raw connection; reconnection is handled by RecoveringConnection. */
export type AmqpConnect = (url: string) => Promise<ChannelModel>;

export interface EventBusOptions {
  url: string;
  connect: AmqpConnect;
  /** Max unacked deliveries per consumer channel (bounds in-flight memory). */
  prefetch: number;
  /** Total delivery attempts (first try + retries) before dead-lettering. */
  maxAttempts: number;
  /** Backoff per retry tier in ms (broker-side TTL); retry n uses tier min(n, length). */
  retryDelaysMs: number[];
  /** x-max-length for work and retry queues (overflow = reject-publish). */
  maxQueueLength: number;
  /** x-max-length for the dead-letter queue. */
  maxDeadLetterLength: number;
  /** Unconfirmed publishes kept for re-publishing across reconnects (memory bound). */
  maxPendingPublishes?: number;
  /** Reconnect backoff policy. */
  reconnect?: Partial<ReconnectPolicy>;
  /**
   * Service-to-service authentication of the invariant events (HMAC-SHA256 with the
   * `ledger.invariant` stream key, audience = the ledger itself). Default: the ledger's own
   * authenticator; `null` publishes and accepts unsigned events (generic use of the bus).
   */
  auth?: EventAuthenticator | null;
  /** Optional structured logger. */
  log?: (level: 'info' | 'warn' | 'error', msg: string, meta?: Record<string, unknown>) => void;
}

export interface DeliveryContext {
  attempt: number;
}

export type EventHandler = (envelope: EventEnvelope, ctx: DeliveryContext) => Promise<void>;

export interface Subscription {
  cancel(): Promise<void>;
}

export interface PublishOptions {
  /** Abort waiting for the broker confirm (e.g. a deadline). */
  signal?: AbortSignal;
}

/** Outcome of one delivery, emitted as 'delivery-settled' (observability/tests). */
export interface DeliverySettled {
  outcome: 'acked' | 'retry-scheduled' | 'dead-lettered' | 'dead-lettered-native' | 'abandoned';
  envelope?: EventEnvelope;
  attempt: number;
  reason?: string;
  retryQueue?: string;
}

/**
 * RabbitMQ topology (all durable unless noted):
 *
 *   ledger.events (topic) --ledger.invariant.#--> ledger.reconciliation (work queue)
 *   failure, attempt < max:
 *     -> ledger.reconciliation.retry.<tier>  (x-message-ttl = backoff,
 *        dead-letters on expiry via ledger.events.requeue -> work queue)
 *   non-retryable, attempts exhausted, or malformed:
 *     -> ledger.events.dlx (topic) -> ledger.reconciliation.dlq
 *   ledger.events.capacity (fanout, non-durable): "a work message was settled"
 *     signals that let publishers wait for queue space without polling.
 */
export const TOPOLOGY = {
  exchange: 'ledger.events',
  requeueExchange: 'ledger.events.requeue',
  deadLetterExchange: 'ledger.events.dlx',
  capacityExchange: 'ledger.events.capacity',
  workQueue: 'ledger.reconciliation',
  deadLetterQueue: 'ledger.reconciliation.dlq',
  bindingKey: 'ledger.invariant.#',
  retryQueue: (tier: number): string => `ledger.reconciliation.retry.${tier}`,
} as const;

export const HEADERS = {
  attempt: 'x-attempt',
  failureReason: 'x-failure-reason',
  failedAt: 'x-failed-at',
  originalRoutingKey: 'x-original-routing-key',
} as const;

const BROKER_NACK = 'message nacked'; // amqplib and the fake use this exact message

interface Session {
  epoch: number;
  connection: ChannelModel;
  publishChannel: ConfirmChannel;
  alive: boolean;
}

interface PendingPublish {
  id: number;
  exchange: string;
  routingKey: string;
  content: Buffer;
  options: Options.Publish;
  resolve: () => void;
  reject: (err: Error) => void;
  /** Epoch of the session it was last sent on; null = needs (re)sending. */
  sentInEpoch: number | null;
  detachAbort?: () => void;
}

interface SubscriptionState {
  handler: EventHandler;
  active: boolean;
  channel: Channel | null;
  consumerTag: string | null;
  epoch: number;
  starting: Promise<void> | null;
}

/**
 * Reliable event bus over RabbitMQ.
 *
 * Reconnection (atomic): a RecoveringConnection rebuilds the ENTIRE session
 * (topology, publish channel, every consumer, capacity listener) inside its
 * setup step and only then swaps it in. A failure part-way discards the new
 * connection and retries; callers never see a half-built session.
 *
 * Safe re-publishing: every publish stays in a bounded pending ledger until the
 * broker CONFIRMS it. A broker NACK rejects the caller. A confirm lost because
 * the channel/connection died ("dropped in flight"), or a publish attempted
 * while disconnected, stays pending and is re-sent on the next session, so
 * nothing accepted by publish() is lost while the process lives. This is
 * at-least-once: a message the broker had already stored may be sent twice,
 * which is why every consumer is idempotent.
 *
 * Consumers: unacked deliveries on a dead channel are requeued by the broker
 * and redelivered to the new session's consumer; a late ack on the dead
 * channel is swallowed. Acks happen only after the retry/DLQ copy is
 * confirmed. No code path sleeps: waits are driven by broker confirms,
 * session events and capacity signals.
 */
export class AmqpEventBus extends EventEmitter {
  private connection?: RecoveringConnection;
  private session: Session | null = null;
  private epoch = 0;
  private closed = false;
  /** Set when reconnection permanently gave up: the bus fails fast from then on. */
  private failure: Error | null = null;
  private readonly pending = new Map<number, PendingPublish>();
  private nextPendingId = 1;
  private readonly subscriptions = new Set<SubscriptionState>();
  /** In-flight handler promises; bounded by prefetch x subscriptions. */
  private readonly inFlight = new Set<Promise<void>>();
  private readonly maxPending: number;
  /** null = unauthenticated bus (generic use); otherwise signs + verifies every delivery. */
  private readonly auth: EventAuthenticator | null;

  constructor(private readonly options: EventBusOptions) {
    super();
    this.auth = options.auth === undefined ? eventAuthFor(SERVICES.ledger) : options.auth;
    this.setMaxListeners(0); // many concurrent waiters on 'capacity' / 'session' is normal
    if (options.retryDelaysMs.length === 0 || options.retryDelaysMs.some((d) => !Number.isInteger(d) || d < 0)) {
      throw new Error('retryDelaysMs must be a non-empty list of non-negative integers');
    }
    if (!Number.isInteger(options.maxAttempts) || options.maxAttempts < 1) {
      throw new Error('maxAttempts must be a positive integer');
    }
    if (!Number.isInteger(options.prefetch) || options.prefetch < 1) {
      throw new Error('prefetch must be a positive integer');
    }
    if (!Number.isInteger(options.maxQueueLength) || options.maxQueueLength <= options.prefetch) {
      throw new Error('maxQueueLength must be an integer greater than prefetch');
    }
    this.maxPending = options.maxPendingPublishes ?? 10_000;
  }

  // ---- lifecycle ------------------------------------------------------------

  /** Connect (idempotent); resolves once the first full session is live. */
  async start(signal?: AbortSignal): Promise<void> {
    if (this.closed) throw new Error('Event bus is closed');
    if (this.failure) throw this.failure;
    if (!this.connection) {
      const connection = new RecoveringConnection(
        () => this.options.connect(this.options.url),
        (conn) => this.setupSession(conn),
        this.options.reconnect
      );
      connection.on('disconnected', (err: Error) => this.onDisconnected(err));
      connection.on('connection-error', (err: Error) =>
        this.log('error', 'AMQP connection error', { error: err.message })
      );
      connection.on('reconnect-scheduled', (info: { attempt: number; delayMs: number; error: Error }) => {
        this.log('warn', 'AMQP reconnect scheduled', { attempt: info.attempt, delayMs: info.delayMs, error: info.error.message });
        this.emit('reconnect-scheduled', info);
      });
      connection.on('reconnect-failed', (err: Error) => {
        // Permanent failure: nothing pending can ever be confirmed now. Fail
        // every waiting publisher instead of leaving them hanging.
        this.failure = new Error(`Event bus unavailable: reconnection gave up (${err.message})`);
        for (const entry of [...this.pending.values()]) {
          this.settlePending(entry, new PublishRejectedError(`${entry.exchange}/${entry.routingKey}`, this.failure));
        }
        this.emit('reconnect-failed', err);
      });
      this.connection = connection;
    }
    await this.connection.start(signal);
  }

  /** Epoch of the live session (grows with every (re)connect attempt); 0 if none. */
  get sessionEpoch(): number {
    return this.session?.alive ? this.session.epoch : 0;
  }

  /** Number of publishes awaiting a broker confirm (or a session to send on). */
  get pendingPublishCount(): number {
    return this.pending.size;
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    for (const sub of this.subscriptions) sub.active = false;
    // Anything still unconfirmed can no longer be guaranteed by this process.
    for (const entry of [...this.pending.values()]) {
      this.settlePending(entry, new PublishRejectedError(`${entry.exchange}/${entry.routingKey}`, new Error('event bus closed')));
    }
    await this.drainInFlight();
    this.subscriptions.clear();
    this.session = null;
    await this.connection?.close();
  }

  // ---- session (re)building: atomic ------------------------------------------

  private async setupSession(connection: ChannelModel): Promise<void> {
    // Every setup ATTEMPT gets a fresh epoch, so per-session idempotency
    // (e.g. "consumer already started on this session") can never match a
    // previous, failed attempt.
    const epoch = ++this.epoch;
    const publishChannel = await connection.createConfirmChannel();
    this.guardChannel(publishChannel, 'publish');
    const session: Session = { epoch, connection, publishChannel, alive: true };
    publishChannel.once('close', () => {
      session.alive = false;
    });

    await this.assertTopology(publishChannel);
    await this.startCapacityListener(connection);
    for (const sub of this.subscriptions) {
      if (sub.active) await this.startConsumer(session, sub);
    }

    // ---- swap: everything above succeeded on a still-open channel ----
    if (!session.alive) throw new Error('Publish channel closed during session setup');
    this.session = session;
    this.resendPending(session);
    this.emit('session', session.epoch);
    this.emit('capacity');
  }

  private onDisconnected(err: Error): void {
    if (this.session) this.session.alive = false;
    this.session = null;
    for (const sub of this.subscriptions) {
      sub.channel = null;
      sub.consumerTag = null;
    }
    this.log('warn', 'AMQP session lost; reconnecting', { error: err.message, pendingPublishes: this.pending.size });
    this.emit('disconnected', err);
  }

  private async assertTopology(ch: ConfirmChannel): Promise<void> {
    await ch.assertExchange(TOPOLOGY.exchange, 'topic', { durable: true });
    await ch.assertExchange(TOPOLOGY.requeueExchange, 'direct', { durable: true });
    await ch.assertExchange(TOPOLOGY.deadLetterExchange, 'topic', { durable: true });
    await ch.assertExchange(TOPOLOGY.capacityExchange, 'fanout', { durable: false });

    await ch.assertQueue(TOPOLOGY.workQueue, {
      durable: true,
      deadLetterExchange: TOPOLOGY.deadLetterExchange,
      maxLength: this.options.maxQueueLength,
      overflow: 'reject-publish',
    } as Options.AssertQueue);
    await ch.bindQueue(TOPOLOGY.workQueue, TOPOLOGY.exchange, TOPOLOGY.bindingKey);
    await ch.bindQueue(TOPOLOGY.workQueue, TOPOLOGY.requeueExchange, TOPOLOGY.workQueue);

    for (let tier = 1; tier <= this.options.retryDelaysMs.length; tier++) {
      await ch.assertQueue(TOPOLOGY.retryQueue(tier), {
        durable: true,
        messageTtl: this.options.retryDelaysMs[tier - 1],
        deadLetterExchange: TOPOLOGY.requeueExchange,
        deadLetterRoutingKey: TOPOLOGY.workQueue,
        maxLength: this.options.maxQueueLength,
        overflow: 'reject-publish',
      } as Options.AssertQueue);
    }

    await ch.assertQueue(TOPOLOGY.deadLetterQueue, {
      durable: true,
      maxLength: this.options.maxDeadLetterLength,
      overflow: 'reject-publish',
    } as Options.AssertQueue);
    await ch.bindQueue(TOPOLOGY.deadLetterQueue, TOPOLOGY.deadLetterExchange, '#');
  }

  /**
   * Private, exclusive, length-1 queue on the capacity fanout: every settled
   * work message anywhere (any process) wakes local capacity waiters. Length 1
   * coalesces bursts, since only "something changed" matters.
   */
  private async startCapacityListener(connection: ChannelModel): Promise<void> {
    const ch = await connection.createChannel();
    this.guardChannel(ch, 'capacity');
    const { queue } = await ch.assertQueue('', { exclusive: true, autoDelete: true, maxLength: 1 });
    await ch.bindQueue(queue, TOPOLOGY.capacityExchange, '');
    await ch.consume(queue, () => this.emit('capacity'), { noAck: true });
  }

  /** Every channel needs an 'error' listener: an unhandled one crashes Node. */
  private guardChannel(ch: Channel, role: string): void {
    ch.on('error', (err: Error) => this.log('warn', `AMQP ${role} channel error`, { error: err.message }));
  }

  // ---- publishing: confirm-tracked, re-published across reconnects ---------

  /** Publish an event; resolves only once the broker has CONFIRMED it. */
  async publish(envelope: EventEnvelope, opts: PublishOptions = {}): Promise<void> {
    const body = Buffer.from(JSON.stringify(envelope));
    const signature = this.auth?.signHeaders({ eventType: envelope.type, eventId: envelope.eventId, body }) ?? {};
    await this.enqueuePublish(
      TOPOLOGY.exchange,
      envelope.type,
      body,
      {
        persistent: true,
        contentType: 'application/json',
        messageId: envelope.eventId,
        type: envelope.type,
        headers: { [HEADERS.attempt]: 1, ...signature },
      },
      opts.signal
    );
  }

  private enqueuePublish(
    exchange: string,
    routingKey: string,
    content: Buffer,
    options: Options.Publish,
    signal?: AbortSignal
  ): Promise<void> {
    const destination = exchange === '' ? routingKey : `${exchange}/${routingKey}`;
    if (this.closed) {
      return Promise.reject(new PublishRejectedError(destination, new Error('event bus closed')));
    }
    if (this.failure) {
      return Promise.reject(new PublishRejectedError(destination, this.failure));
    }
    if (signal?.aborted) {
      return Promise.reject(new PublishRejectedError(destination, signal.reason));
    }
    if (this.pending.size >= this.maxPending) {
      // Bounded memory: refuse rather than buffer without limit while the
      // broker is unreachable. Callers treat this like a NACK.
      return Promise.reject(new PublishRejectedError(destination, new Error('local publish buffer full')));
    }

    return new Promise<void>((resolve, reject) => {
      const entry: PendingPublish = {
        id: this.nextPendingId++,
        exchange,
        routingKey,
        content,
        options,
        resolve,
        reject,
        sentInEpoch: null,
      };
      if (signal) {
        const onAbort = () => this.settlePending(entry, new PublishRejectedError(destination, signal.reason));
        signal.addEventListener('abort', onAbort, { once: true });
        entry.detachAbort = () => signal.removeEventListener('abort', onAbort);
      }
      this.pending.set(entry.id, entry);

      if (this.session?.alive) {
        this.send(this.session, entry);
      } else if (!this.closed) {
        // Will be sent by resendPending() when the next session goes live.
        this.start().catch(() => undefined);
      }
    });
  }

  private send(session: Session, entry: PendingPublish): void {
    entry.sentInEpoch = session.epoch;
    const destination = entry.exchange === '' ? entry.routingKey : `${entry.exchange}/${entry.routingKey}`;
    try {
      session.publishChannel.publish(entry.exchange, entry.routingKey, entry.content, entry.options, (err: unknown) => {
        if (!this.pending.has(entry.id)) return; // already settled (aborted/closed)
        if (!err) {
          this.settlePending(entry);
          return;
        }
        if (err instanceof Error && err.message === BROKER_NACK) {
          this.settlePending(entry, new PublishRejectedError(destination, err));
          return;
        }
        // Dropped in flight: the channel died before the broker confirmed.
        // Its fate is unknown, so re-send on the next (or current) session.
        this.markDropped(entry, session);
      });
    } catch {
      this.markDropped(entry, session); // channel already closed synchronously
    }
  }

  private markDropped(entry: PendingPublish, session: Session): void {
    if (!this.pending.has(entry.id) || entry.sentInEpoch !== session.epoch) return;
    entry.sentInEpoch = null;
    this.emit('publish-dropped', { destination: `${entry.exchange}/${entry.routingKey}` });
    if (this.session?.alive && this.session !== session) this.send(this.session, entry);
  }

  private resendPending(session: Session): void {
    let resent = 0;
    for (const entry of this.pending.values()) {
      if (entry.sentInEpoch !== session.epoch) {
        this.send(session, entry);
        resent++;
      }
    }
    if (resent > 0) {
      this.log('info', 'Re-published unconfirmed events on new session', { count: resent, epoch: session.epoch });
      this.emit('republished', { count: resent, epoch: session.epoch });
    }
  }

  private settlePending(entry: PendingPublish, err?: Error): void {
    if (!this.pending.delete(entry.id)) return;
    entry.detachAbort?.();
    if (err) entry.reject(err);
    else entry.resolve();
  }

  // ---- capacity (event-driven backpressure) ---------------------------------

  /** Cap on ready messages in the work queue (x-max-length). */
  get workQueueCapacity(): number {
    return this.options.maxQueueLength;
  }

  /**
   * Conservative free space in the work queue: cap - ready - (consumers x
   * prefetch). RabbitMQ's x-max-length counts only READY messages, so reserving
   * room for in-flight deliveries is merely cautious there; brokers/emulators
   * that count unacked deliveries toward the cap (the sandbox fake does, and
   * evicts the oldest message on overflow) can then never drop a confirmed one.
   */
  async workQueueHeadroom(signal?: AbortSignal): Promise<number> {
    const session = await this.liveSession(signal);
    const { messageCount, consumerCount } = await session.publishChannel.checkQueue(TOPOLOGY.workQueue);
    return this.options.maxQueueLength - messageCount - consumerCount * this.options.prefetch;
  }

  /**
   * Resolve on the next capacity signal: a work message was settled by any
   * consumer (any process), or a session (re)connected. Register BEFORE
   * checking headroom so a signal in between is never missed. Rejects if
   * `signal` aborts (use AbortSignal.timeout for a deadline).
   */
  nextCapacitySignal(signal?: AbortSignal): { promise: Promise<void>; cancel: () => void } {
    const controller = new AbortController();
    const combined = signal ? AbortSignal.any([signal, controller.signal]) : controller.signal;
    const promise = once(this, 'capacity', { signal: combined }).then(() => undefined);
    promise.catch(() => undefined); // cancellation is not an unhandled rejection
    return { promise, cancel: () => controller.abort() };
  }

  private async liveSession(signal?: AbortSignal): Promise<Session> {
    if (this.session?.alive) return this.session;
    await this.start(signal);
    if (this.session?.alive) return this.session;
    await once(this, 'session', signal ? { signal } : undefined);
    return this.liveSession(signal);
  }

  /** Fire-and-forget "capacity freed" broadcast (non-persistent, unconfirmed). */
  private broadcastCapacity(): void {
    this.emit('capacity');
    const session = this.session;
    if (!session?.alive) return;
    try {
      session.publishChannel.publish(TOPOLOGY.capacityExchange, '', Buffer.from('1'), { persistent: false }, () => undefined);
    } catch {
      // best-effort signal; the local emit above already woke local waiters
    }
  }

  // ---- consuming --------------------------------------------------------------

  /**
   * Consume the work queue. Handler resolves = ack. NonRetryableError (or
   * attempts exhausted, or a malformed message) dead-letters; any other error
   * schedules a broker-side delayed retry. The original delivery is acked only
   * AFTER its retry/DLQ copy is broker-confirmed. Subscriptions survive
   * reconnects: every new session restarts them.
   */
  async subscribe(handler: EventHandler): Promise<Subscription> {
    await this.start();
    const sub: SubscriptionState = { handler, active: true, channel: null, consumerTag: null, epoch: 0, starting: null };
    this.subscriptions.add(sub);
    const session = await this.liveSession();
    await this.startConsumer(session, sub);

    return {
      cancel: async () => {
        sub.active = false;
        this.subscriptions.delete(sub);
        const { channel, consumerTag } = sub;
        sub.channel = null;
        if (channel && consumerTag) {
          try {
            await channel.cancel(consumerTag);
            await channel.close();
          } catch {
            // channel already gone
          }
        }
        await this.drainInFlight();
      },
    };
  }

  /** Idempotent per (subscription, session): safe from setup and subscribe races. */
  private startConsumer(session: Session, sub: SubscriptionState): Promise<void> {
    if (!sub.active || sub.epoch === session.epoch) return Promise.resolve();
    if (sub.starting) return sub.starting;
    sub.starting = (async () => {
      const ch = await session.connection.createChannel();
      this.guardChannel(ch, 'consumer');
      await ch.prefetch(this.options.prefetch);
      const { consumerTag } = await ch.consume(
        TOPOLOGY.workQueue,
        (msg) => {
          if (!msg) return; // consumer cancelled by broker
          const task = this.handleDelivery(ch, msg, sub.handler);
          this.inFlight.add(task);
          void task.finally(() => this.inFlight.delete(task));
        },
        { noAck: false }
      );
      sub.channel = ch;
      sub.consumerTag = consumerTag;
      sub.epoch = session.epoch;
    })().finally(() => {
      sub.starting = null;
    });
    return sub.starting;
  }

  private async handleDelivery(ch: Channel, msg: ConsumeMessage, handler: EventHandler): Promise<void> {
    const attempt = Number(msg.properties.headers?.[HEADERS.attempt] ?? 1);

    if (this.auth) {
      try {
        this.auth.verifyDelivery({
          headers: (msg.properties.headers ?? {}) as Record<string, unknown>,
          content: msg.content,
          messageId: msg.properties.messageId as string | undefined,
          eventType: (msg.properties.type as string | undefined) ?? msg.fields.routingKey,
        });
      } catch (err) {
        this.log('warn', 'Rejecting a ledger event that failed service-to-service authentication', { error: errorMessage(err) });
        const reason = err instanceof EventAuthError ? `${err.reason}: ${err.message}` : errorMessage(err);
        await this.reroute(ch, msg, attempt, `UNTRUSTED_EVENT: ${reason}`, undefined, 'dead-letter');
        return;
      }
    }

    let envelope: EventEnvelope;
    try {
      envelope = parseEnvelope(msg.content);
    } catch {
      await this.reroute(ch, msg, attempt, 'MALFORMED_MESSAGE', undefined, 'dead-letter');
      return;
    }

    try {
      await handler(envelope, { attempt });
      this.settle(ch, msg, 'ack');
      this.emit('delivery-settled', { outcome: 'acked', envelope, attempt } satisfies DeliverySettled);
    } catch (err) {
      const reason = err instanceof NonRetryableError ? err.reason : errorMessage(err);
      const deadLetter = err instanceof NonRetryableError || attempt >= this.options.maxAttempts;
      this.log(deadLetter ? 'warn' : 'info', deadLetter ? 'Dead-lettering ledger event' : 'Scheduling ledger event retry', {
        eventId: envelope.eventId,
        attempt,
        reason,
      });
      await this.reroute(ch, msg, attempt, reason, envelope, deadLetter ? 'dead-letter' : 'retry');
    }
  }

  /**
   * Publish the retry/DLQ copy, wait for its confirm, then ack the original.
   * If the broker REFUSES the copy (queue full), fall back to the broker's own
   * dead-lettering (nack, no requeue): no hot requeue loop and no sleeping.
   * If the bus is closing, leave the delivery unacked: the broker redelivers it.
   */
  private async reroute(
    ch: Channel,
    msg: ConsumeMessage,
    attempt: number,
    reason: string,
    envelope: EventEnvelope | undefined,
    kind: 'retry' | 'dead-letter'
  ): Promise<void> {
    const tier = Math.min(attempt, this.options.retryDelaysMs.length);
    try {
      if (kind === 'retry') {
        await this.enqueuePublish('', TOPOLOGY.retryQueue(tier), msg.content, this.copyProperties(msg, {
          [HEADERS.attempt]: attempt + 1,
          [HEADERS.failureReason]: truncate(reason),
        }));
      } else {
        await this.enqueuePublish(TOPOLOGY.deadLetterExchange, originalRoutingKey(msg), msg.content, this.copyProperties(msg, {
          [HEADERS.attempt]: attempt,
          [HEADERS.failureReason]: truncate(reason),
          [HEADERS.failedAt]: new Date().toISOString(),
        }));
      }
      this.settle(ch, msg, 'ack');
      this.emit('delivery-settled', {
        outcome: kind === 'retry' ? 'retry-scheduled' : 'dead-lettered',
        envelope,
        attempt,
        reason,
        ...(kind === 'retry' ? { retryQueue: TOPOLOGY.retryQueue(tier) } : {}),
      } satisfies DeliverySettled);
    } catch (err) {
      if (this.closed) {
        this.emit('delivery-settled', { outcome: 'abandoned', envelope, attempt, reason } satisfies DeliverySettled);
        return; // unacked -> broker redelivers to the next consumer
      }
      this.log('error', 'Broker refused the reroute copy; using native dead-lettering', { error: errorMessage(err) });
      this.settle(ch, msg, 'nack');
      this.emit('delivery-settled', { outcome: 'dead-lettered-native', envelope, attempt, reason } satisfies DeliverySettled);
    }
  }

  private copyProperties(msg: ConsumeMessage, headers: Record<string, unknown>): Options.Publish {
    return {
      persistent: true,
      contentType: msg.properties.contentType as string | undefined,
      messageId: msg.properties.messageId as string | undefined,
      type: msg.properties.type as string | undefined,
      headers: { ...msg.properties.headers, ...headers, [HEADERS.originalRoutingKey]: originalRoutingKey(msg) },
    };
  }

  /** Ack/nack on the delivery's own channel; if it died, the broker redelivers. */
  private settle(ch: Channel, msg: ConsumeMessage, action: 'ack' | 'nack'): void {
    try {
      if (action === 'ack') ch.ack(msg);
      else ch.nack(msg, false, false);
    } catch {
      // channel/connection closed: the broker requeues the unacked delivery
    }
    this.broadcastCapacity();
  }

  private async drainInFlight(): Promise<void> {
    while (this.inFlight.size > 0) {
      await Promise.allSettled([...this.inFlight]);
    }
  }

  private log(level: 'info' | 'warn' | 'error', msg: string, meta?: Record<string, unknown>): void {
    this.options.log?.(level, msg, meta);
  }
}

function originalRoutingKey(msg: ConsumeMessage): string {
  const header = msg.properties.headers?.[HEADERS.originalRoutingKey];
  if (typeof header === 'string' && header.length > 0) return header;
  return (msg.properties.type as string | undefined) ?? msg.fields.routingKey;
}

function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

/** Cap header size: error messages must not bloat every retried message. */
function truncate(value: string, max = 500): string {
  return value.length <= max ? value : `${value.slice(0, max)}…`;
}
