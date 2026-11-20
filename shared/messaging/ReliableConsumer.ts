import { EventEmitter, once } from 'events';
import type { Channel, ChannelModel, ConfirmChannel, ConsumeMessage, GetMessage, Options } from 'amqplib';
import type { AmqpConnect } from './connect';
import { ReconnectPolicy, RecoveringConnection } from './RecoveringConnection';
import { RetryPolicy, DEFAULT_RETRY_POLICY, assertValidRetryPolicy, jitteredRetryDelay } from './backoff';
import { errorMessage, failureReasonOf, isNonRetryable, truncate } from './errors';
import {
  ConsumerBinding,
  ConsumerQueues,
  DELIVERY_HEADERS,
  PLATFORM_EXCHANGES,
  consumerQueues,
  declareConsumerTopology,
} from './topology';

export type LogFn = (level: 'info' | 'warn' | 'error', msg: string, meta?: Record<string, unknown>) => void;

/** Console logger honouring PLATFORM_LOG_LEVEL (silent | error | warn | info; default warn). */
export function platformLogFromEnv(level = process.env.PLATFORM_LOG_LEVEL ?? 'warn'): LogFn {
  const order = ['error', 'warn', 'info'];
  const max = level === 'silent' ? -1 : order.indexOf(level);
  return (lvl, msg, meta) => {
    if (order.indexOf(lvl) > max) return;
    const line = meta && Object.keys(meta).length > 0 ? `${msg} ${JSON.stringify(meta)}` : msg;
    (lvl === 'error' ? console.error : lvl === 'warn' ? console.warn : console.log)(line);
  };
}

export interface DeliveryContext {
  consumer: string;
  /** 1-based delivery attempt (retries increment it; a broker redelivery does not). */
  attempt: number;
  messageId?: string;
  redelivered: boolean;
  /** How many times an operator replayed this message from the DLQ. */
  replayCount: number;
}

/**
 * Handler contract: resolve = the message is done (acked); throw = failed.
 * Throw a NonRetryableError (or anything with `retryable: false`) when a retry
 * cannot help. Handlers MUST be idempotent: delivery is at-least-once.
 * The optional string result is reported in the 'settled' event.
 */
export type MessageHandler<T> = (message: T, ctx: DeliveryContext) => Promise<string | void>;

export interface ReliableConsumerOptions<T> {
  /** Consumer name = work queue name (also the routing key of its retry / DLQ traffic). */
  name: string;
  bindings: ConsumerBinding[];
  /** Validate + narrow the parsed JSON body. Throwing dead-letters the message (SCHEMA_INVALID). */
  decode: (raw: unknown) => T;
  /**
   * Service-to-service authentication of the delivery, run BEFORE the body is parsed: the
   * signature, the key id, the issuer and the audience are checked against the authorization
   * matrix (see shared/security). Throwing dead-letters the message at once (UNTRUSTED_EVENT),
   * without burning a retry. Omitted = no authentication (generic consumers, tests).
   */
  verify?: (delivery: { headers: Record<string, unknown>; content: Buffer; messageId?: string; eventType?: string }) => void;
  handler: MessageHandler<T>;
  url: string;
  connect: AmqpConnect;
  prefetch?: number;
  retry?: Partial<RetryPolicy>;
  /**
   * How often to poke the retry queues so expired messages are dead-lettered
   * back. RabbitMQ expires TTL messages by itself (the poke is a passive
   * declare, harmless); the in-process fake broker expires lazily, on access.
   * 0 disables the pump.
   */
  ttlPumpIntervalMs?: number;
  /** Budget for a retry / DLQ copy to be confirmed by the broker. */
  publishTimeoutMs?: number;
  reconnect?: Partial<ReconnectPolicy>;
  log?: LogFn;
}

export type SettledOutcome = 'acked' | 'retry-scheduled' | 'dead-lettered' | 'dead-lettered-native' | 'abandoned';

export interface DeliverySettled {
  consumer: string;
  outcome: SettledOutcome;
  attempt: number;
  messageId?: string;
  eventType?: string;
  /** The handler's own outcome string (e.g. PROCESSED, DUPLICATE_IGNORED). */
  result?: string;
  reason?: string;
  retryQueue?: string;
  delayMs?: number;
}

export interface DeadLetterInfo {
  messageId?: string;
  eventType?: string;
  attempt: number;
  reason?: string;
  deadLetteredAt?: string;
  replayCount: number;
  body: unknown;
}

export interface ReplaySummary {
  requested: number;
  replayed: number;
  remaining: number;
  messages: DeadLetterInfo[];
}

export interface QueueDepths {
  work: number;
  retry: Record<string, number>;
  deadLetter: number;
}

interface Session {
  epoch: number;
  connection: ChannelModel;
  /** Confirm channel for retry / dead-letter copies and passive declares. */
  control: ConfirmChannel;
  consume: Channel;
  consumerTag: string | null;
  alive: boolean;
}

const DEFAULT_PUBLISH_TIMEOUT_MS = 10_000;

/**
 * A reliable, self-healing AMQP consumer for one integration queue.
 *
 * - Topology is (re)declared on every connect; the session (topology, control
 *   channel, consumer) is rebuilt atomically after a broker drop.
 * - A malformed or schema-invalid message is dead-lettered at once (poison
 *   messages never loop).
 * - A failing handler is retried with full-jitter exponential backoff via
 *   per-attempt TTL queues, up to `retry.maxAttempts` deliveries; then the
 *   message is dead-lettered with its failure reason.
 * - The original delivery is acked only AFTER its retry / DLQ copy is
 *   confirmed by the broker, so a crash in between redelivers instead of losing
 *   it (at-least-once; handlers are idempotent).
 * - Operators inspect and replay the DLQ per consumer; a replayed message
 *   returns to THIS consumer's queue only, with a fresh attempt budget.
 */
export class ReliableConsumer<T> extends EventEmitter {
  readonly queues: ConsumerQueues;
  readonly retryPolicy: RetryPolicy;
  private connection?: RecoveringConnection;
  private session: Session | null = null;
  private epoch = 0;
  private closed = false;
  private pumpTimer: NodeJS.Timeout | null = null;
  private readonly inFlight = new Set<Promise<void>>();
  private readonly counters: Record<SettledOutcome, number> = {
    acked: 0,
    'retry-scheduled': 0,
    'dead-lettered': 0,
    'dead-lettered-native': 0,
    abandoned: 0,
  };

  constructor(private readonly options: ReliableConsumerOptions<T>) {
    super();
    this.setMaxListeners(0);
    this.retryPolicy = { ...DEFAULT_RETRY_POLICY, ...options.retry };
    assertValidRetryPolicy(this.retryPolicy);
    const prefetch = options.prefetch ?? 8;
    if (!Number.isInteger(prefetch) || prefetch < 1) throw new Error('prefetch must be a positive integer');
    this.queues = consumerQueues(options.name, this.retryPolicy.maxAttempts);
  }

  get name(): string {
    return this.options.name;
  }

  /** Deliveries settled so far, by outcome. */
  stats(): Record<SettledOutcome, number> & { inFlight: number; connected: boolean } {
    return { ...this.counters, inFlight: this.inFlight.size, connected: this.session?.alive === true };
  }

  // ── lifecycle ──────────────────────────────────────────────────────────────

  /** Connect, declare the topology and start consuming. Resolves once live. */
  async start(signal?: AbortSignal): Promise<void> {
    if (this.closed) throw new Error(`Consumer ${this.name} is closed`);
    if (!this.connection) {
      const connection = new RecoveringConnection(
        () => this.options.connect(this.options.url),
        (conn) => this.setupSession(conn),
        this.options.reconnect
      );
      connection.on('disconnected', (err: Error) => {
        if (this.session) this.session.alive = false;
        this.session = null;
        this.log('warn', 'broker session lost; reconnecting', { error: err.message });
        this.emit('disconnected', err);
      });
      connection.on('connection-error', (err: Error) => this.log('error', 'broker connection error', { error: err.message }));
      connection.on('reconnect-scheduled', (info: { attempt: number; delayMs: number; error: Error }) =>
        this.log('warn', 'reconnect scheduled', { attempt: info.attempt, delayMs: info.delayMs, error: info.error.message })
      );
      this.connection = connection;
    }
    await this.connection.start(signal);
    this.startPump();
  }

  /** Stop consuming, let in-flight handlers finish (and settle), then disconnect. */
  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    this.stopPump();
    const session = this.session;
    if (session?.consumerTag) {
      try {
        await session.consume.cancel(session.consumerTag);
      } catch {
        // channel already gone
      }
    }
    await this.waitForIdle();
    this.session = null;
    await this.connection?.close();
  }

  /** Resolves when no handler is running. */
  async waitForIdle(): Promise<void> {
    while (this.inFlight.size > 0) {
      await Promise.allSettled([...this.inFlight]);
    }
  }

  // ── operations ─────────────────────────────────────────────────────────────

  async queueDepths(): Promise<QueueDepths> {
    const session = await this.liveSession();
    const depth = async (queue: string) => (await session.control.checkQueue(queue)).messageCount;
    const retry: Record<string, number> = {};
    for (const queue of this.queues.retryQueues) retry[queue] = await depth(queue);
    return {
      work: await depth(this.queues.workQueue),
      retry,
      deadLetter: await depth(this.queues.deadLetterQueue),
    };
  }

  /** Non-destructive look at parked messages (they are put back untouched). */
  async inspectDeadLetters(options: { limit?: number } = {}): Promise<DeadLetterInfo[]> {
    return this.withScratchChannel(async (ch) => {
      const { messageCount } = await ch.checkQueue(this.queues.deadLetterQueue);
      const limit = Math.min(options.limit ?? 100, messageCount);
      const held: GetMessage[] = [];
      for (let i = 0; i < limit; i++) {
        const msg = await ch.get(this.queues.deadLetterQueue, { noAck: false });
        if (!msg) break;
        held.push(msg);
      }
      const described = held.map(describeDeadLetter);
      for (const msg of held.reverse()) ch.nack(msg, false, true);
      return described;
    });
  }

  /**
   * Move parked messages back onto THIS consumer's work queue (other consumers
   * are not involved), each with a fresh attempt budget. A message is removed
   * from the DLQ only after the broker confirmed its replayed copy.
   */
  async replayDeadLetters(options: { limit?: number } = {}): Promise<ReplaySummary> {
    return this.withScratchChannel(async (ch) => {
      const { messageCount } = await ch.checkQueue(this.queues.deadLetterQueue);
      const requested = Math.min(options.limit ?? messageCount, messageCount);
      const messages: DeadLetterInfo[] = [];
      for (let i = 0; i < requested; i++) {
        const msg = await ch.get(this.queues.deadLetterQueue, { noAck: false });
        if (!msg) break;
        const info = describeDeadLetter(msg);
        await publishConfirmed(
          ch,
          PLATFORM_EXCHANGES.requeue,
          this.name,
          msg.content,
          copyProperties(msg, {
            [DELIVERY_HEADERS.attempt]: 1,
            [DELIVERY_HEADERS.replayCount]: info.replayCount + 1,
            [DELIVERY_HEADERS.replayedAt]: new Date().toISOString(),
          }),
          this.publishTimeoutMs
        );
        ch.ack(msg);
        messages.push({ ...info, replayCount: info.replayCount + 1 });
      }
      const { messageCount: remaining } = await ch.checkQueue(this.queues.deadLetterQueue);
      this.log('info', 'replayed dead letters', { replayed: messages.length, remaining });
      return { requested, replayed: messages.length, remaining, messages };
    });
  }

  // ── session ────────────────────────────────────────────────────────────────

  private async setupSession(connection: ChannelModel): Promise<void> {
    const epoch = ++this.epoch;
    const control = await connection.createConfirmChannel();
    this.guardChannel(control, 'control');
    await declareConsumerTopology(control, this.name, this.options.bindings, this.retryPolicy.maxAttempts);

    const consume = await connection.createChannel();
    this.guardChannel(consume, 'consume');
    await consume.prefetch(this.options.prefetch ?? 8);

    const session: Session = { epoch, connection, control, consume, consumerTag: null, alive: true };
    const markDead = () => {
      session.alive = false;
    };
    control.once('close', markDead);
    consume.once('close', markDead);

    const { consumerTag } = await consume.consume(
      this.queues.workQueue,
      (msg) => {
        if (!msg) return; // cancelled by the broker
        const task = this.handleDelivery(session, msg);
        this.inFlight.add(task);
        void task.finally(() => this.inFlight.delete(task));
      },
      { noAck: false }
    );
    session.consumerTag = consumerTag;
    if (!session.alive) throw new Error('channel closed during session setup');
    this.session = session;
    this.emit('session', epoch);
  }

  private async liveSession(): Promise<Session> {
    if (this.session?.alive) return this.session;
    if (this.closed) throw new Error(`Consumer ${this.name} is closed`);
    await this.start();
    if (this.session?.alive) return this.session;
    await once(this, 'session');
    return this.liveSession();
  }

  private async withScratchChannel<R>(work: (ch: ConfirmChannel) => Promise<R>): Promise<R> {
    const session = await this.liveSession();
    const ch = await session.connection.createConfirmChannel();
    this.guardChannel(ch, 'operator');
    try {
      return await work(ch);
    } finally {
      try {
        await ch.close();
      } catch {
        // already closed
      }
    }
  }

  // ── delivery ───────────────────────────────────────────────────────────────

  private async handleDelivery(session: Session, msg: ConsumeMessage): Promise<void> {
    const headers = (msg.properties.headers ?? {}) as Record<string, unknown>;
    const attempt = Math.max(1, positiveInt(headers[DELIVERY_HEADERS.attempt]) ?? 1);
    const base: Pick<DeliverySettled, 'consumer' | 'attempt' | 'messageId' | 'eventType'> = {
      consumer: this.name,
      attempt,
      messageId: stringOrUndefined(msg.properties.messageId),
      eventType: stringOrUndefined(msg.properties.type),
    };

    if (this.options.verify) {
      try {
        this.options.verify({ headers, content: msg.content, messageId: base.messageId, eventType: base.eventType });
      } catch (err) {
        const reason = `UNTRUSTED_EVENT: ${failureReasonOf(err)}`;
        this.log('warn', 'rejecting an event that failed service-to-service authentication', { ...base, reason });
        await this.reroute(session, msg, attempt, 'dead-letter', reason, base);
        return;
      }
    }

    let raw: unknown;
    try {
      raw = JSON.parse(msg.content.toString('utf8'));
    } catch {
      await this.reroute(session, msg, attempt, 'dead-letter', 'MALFORMED_MESSAGE: body is not valid JSON', base);
      return;
    }

    let decoded: T;
    try {
      decoded = this.options.decode(raw);
    } catch (err) {
      await this.reroute(session, msg, attempt, 'dead-letter', `SCHEMA_INVALID: ${summarizeError(err)}`, base);
      return;
    }

    let result: string | void;
    try {
      result = await this.options.handler(decoded, {
        consumer: this.name,
        attempt,
        messageId: base.messageId,
        redelivered: msg.fields.redelivered === true,
        replayCount: positiveInt(headers[DELIVERY_HEADERS.replayCount]) ?? 0,
      });
    } catch (err) {
      const reason = failureReasonOf(err);
      const nonRetryable = isNonRetryable(err);
      const exhausted = attempt >= this.retryPolicy.maxAttempts;
      if (nonRetryable || exhausted) {
        const why = nonRetryable ? `NON_RETRYABLE: ${reason}` : `RETRIES_EXHAUSTED after ${attempt} attempt(s): ${reason}`;
        this.log('warn', 'dead-lettering message', { ...base, reason: why });
        await this.reroute(session, msg, attempt, 'dead-letter', why, base);
      } else {
        this.log('info', 'scheduling retry', { ...base, reason });
        await this.reroute(session, msg, attempt, 'retry', reason, base);
      }
      return;
    }

    this.settle(session, msg, 'ack');
    this.record({ ...base, outcome: 'acked', result: result ?? undefined });
  }

  /**
   * Publish the retry / DLQ copy, wait for its confirm, then ack the original.
   * If the copy cannot be confirmed on a live session, fall back to the work
   * queue's native dead-lettering (nack without requeue): no hot requeue loop.
   * If the session died meanwhile, leave the delivery unacked: the broker
   * redelivers it to the next session.
   */
  private async reroute(
    session: Session,
    msg: ConsumeMessage,
    attempt: number,
    kind: 'retry' | 'dead-letter',
    reason: string,
    base: Pick<DeliverySettled, 'consumer' | 'attempt' | 'messageId' | 'eventType'>
  ): Promise<void> {
    const nowIso = new Date().toISOString();
    const headers = (msg.properties.headers ?? {}) as Record<string, unknown>;
    try {
      if (kind === 'retry') {
        const delayMs = jitteredRetryDelay(attempt, this.retryPolicy);
        const retryQueue = this.queues.retryQueues[attempt - 1];
        await publishConfirmed(
          session.control,
          '',
          retryQueue,
          msg.content,
          {
            ...copyProperties(msg, {
              [DELIVERY_HEADERS.attempt]: attempt + 1,
              [DELIVERY_HEADERS.consumer]: this.name,
              [DELIVERY_HEADERS.failureReason]: truncate(reason),
              [DELIVERY_HEADERS.failedAt]: nowIso,
              [DELIVERY_HEADERS.firstFailedAt]: stringOrUndefined(headers[DELIVERY_HEADERS.firstFailedAt]) ?? nowIso,
              [DELIVERY_HEADERS.retryDelayMs]: delayMs,
            }),
            expiration: String(delayMs),
          },
          this.publishTimeoutMs
        );
        this.settle(session, msg, 'ack');
        this.record({ ...base, outcome: 'retry-scheduled', reason, retryQueue, delayMs });
      } else {
        await publishConfirmed(
          session.control,
          PLATFORM_EXCHANGES.deadLetter,
          this.name,
          msg.content,
          copyProperties(msg, {
            [DELIVERY_HEADERS.attempt]: attempt,
            [DELIVERY_HEADERS.consumer]: this.name,
            [DELIVERY_HEADERS.failureReason]: truncate(reason),
            [DELIVERY_HEADERS.deadLetteredAt]: nowIso,
          }),
          this.publishTimeoutMs
        );
        this.settle(session, msg, 'ack');
        this.record({ ...base, outcome: 'dead-lettered', reason });
      }
    } catch (err) {
      if (!session.alive || this.closed) {
        this.record({ ...base, outcome: 'abandoned', reason: `${reason} (copy not confirmed: ${errorMessage(err)})` });
        return;
      }
      this.log('error', 'broker refused the retry/DLQ copy; using native dead-lettering', { ...base, error: errorMessage(err) });
      this.settle(session, msg, 'nack');
      this.record({ ...base, outcome: 'dead-lettered-native', reason });
    }
  }

  private settle(session: Session, msg: ConsumeMessage, action: 'ack' | 'nack'): void {
    try {
      if (action === 'ack') session.consume.ack(msg);
      else session.consume.nack(msg, false, false);
    } catch {
      // channel closed: the broker requeues the unacked delivery
    }
  }

  private record(settled: DeliverySettled): void {
    this.counters[settled.outcome] += 1;
    this.emit('settled', settled);
  }

  // ── TTL pump ───────────────────────────────────────────────────────────────

  private startPump(): void {
    const interval = this.options.ttlPumpIntervalMs ?? 0;
    if (interval <= 0 || this.pumpTimer || this.queues.retryQueues.length === 0) return;
    this.pumpTimer = setInterval(() => {
      const session = this.session;
      if (!session?.alive) return;
      for (const queue of this.queues.retryQueues) {
        session.control.checkQueue(queue).catch(() => undefined);
      }
    }, interval);
    this.pumpTimer.unref();
  }

  private stopPump(): void {
    if (this.pumpTimer) clearInterval(this.pumpTimer);
    this.pumpTimer = null;
  }

  private get publishTimeoutMs(): number {
    return this.options.publishTimeoutMs ?? DEFAULT_PUBLISH_TIMEOUT_MS;
  }

  private guardChannel(ch: Channel, role: string): void {
    // An unhandled 'error' event would crash the process.
    ch.on('error', (err: Error) => this.log('warn', `${role} channel error`, { error: err.message }));
  }

  private log(level: 'info' | 'warn' | 'error', msg: string, meta?: Record<string, unknown>): void {
    this.options.log?.(level, `[${this.name}] ${msg}`, meta);
  }
}

// ─── helpers ────────────────────────────────────────────────────────────────

/** Publish on a confirm channel and resolve once the broker confirmed it. */
export function publishConfirmed(
  ch: ConfirmChannel,
  exchange: string,
  routingKey: string,
  content: Buffer,
  options: Options.Publish,
  timeoutMs: number
): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error(`Timed out after ${timeoutMs}ms waiting for the broker to confirm ${exchange || '(default)'}/${routingKey}`)),
      timeoutMs
    );
    timer.unref();
    try {
      ch.publish(exchange, routingKey, content, options, (err: unknown) => {
        clearTimeout(timer);
        if (err) reject(err instanceof Error ? err : new Error(String(err)));
        else resolve();
      });
    } catch (err) {
      clearTimeout(timer);
      reject(err instanceof Error ? err : new Error(String(err)));
    }
  });
}

function copyProperties(
  msg: { properties: ConsumeMessage['properties'] },
  headers: Record<string, unknown>
): Options.Publish {
  const p = msg.properties;
  return {
    persistent: true,
    contentType: stringOrUndefined(p.contentType),
    messageId: stringOrUndefined(p.messageId),
    type: stringOrUndefined(p.type),
    correlationId: stringOrUndefined(p.correlationId),
    timestamp: typeof p.timestamp === 'number' ? p.timestamp : undefined,
    appId: stringOrUndefined(p.appId),
    headers: { ...(p.headers ?? {}), ...headers },
  };
}

function describeDeadLetter(msg: GetMessage): DeadLetterInfo {
  const headers = (msg.properties.headers ?? {}) as Record<string, unknown>;
  let body: unknown;
  const text = msg.content.toString('utf8');
  try {
    body = JSON.parse(text);
  } catch {
    body = text;
  }
  return {
    messageId: stringOrUndefined(msg.properties.messageId),
    eventType: stringOrUndefined(msg.properties.type),
    attempt: positiveInt(headers[DELIVERY_HEADERS.attempt]) ?? 1,
    reason: stringOrUndefined(headers[DELIVERY_HEADERS.failureReason]),
    deadLetteredAt: stringOrUndefined(headers[DELIVERY_HEADERS.deadLetteredAt]),
    replayCount: positiveInt(headers[DELIVERY_HEADERS.replayCount]) ?? 0,
    body,
  };
}

function positiveInt(value: unknown): number | undefined {
  const n = typeof value === 'string' ? Number(value) : value;
  return typeof n === 'number' && Number.isInteger(n) && n >= 0 ? n : undefined;
}

function stringOrUndefined(value: unknown): string | undefined {
  return typeof value === 'string' && value.length > 0 ? value : undefined;
}

function summarizeError(err: unknown): string {
  const issues = (err as { issues?: Array<{ path: Array<string | number>; message: string }> } | null)?.issues;
  if (Array.isArray(issues) && issues.length > 0) {
    return issues
      .slice(0, 5)
      .map((i) => `${i.path.join('.') || '(root)'}: ${i.message}`)
      .join('; ');
  }
  return errorMessage(err);
}
