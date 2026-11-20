/* eslint-disable @typescript-eslint/no-unused-vars */
import { describe, it, test, expect, beforeEach, afterEach, beforeAll, afterAll, vi } from 'vitest';
/* eslint-disable @typescript-eslint/no-unused-vars */
/**
 * tests/helpers/fakeBroker.ts
 *
 * An in-memory stand-in for RabbitMQ, faithful enough to exercise the
 * messaging layer end-to-end without a real broker:
 *
 *   - connections and channels are EventEmitters that emit 'close' / 'error'
 *     the way amqplib does (server-initiated channel closes emit 'error')
 *   - exchanges, queues *with arguments*, bindings; re-declaring a queue with
 *     different arguments fails with 406 PRECONDITION_FAILED and kills the
 *     channel, exactly like the real thing
 *   - basic.get / consume / ack / nack / nackAll with per-channel unacked
 *     tracking; closing a channel requeues its unacked messages; several
 *     consumers on one queue are served round-robin, like the real broker
 *   - dead-lettering driven by the queue's own `x-dead-letter-*` arguments,
 *     including `x-death` / `x-first-death-*` headers
 *   - publisher confirms (immediate), prefetch (ignored), deleteQueue(ifEmpty)
 *   - `dropConnections()` to simulate a network partition / broker restart
 *
 * Queue state survives connections, like a durable broker would.
 */

import { EventEmitter } from 'events';
import type amqplib from 'amqplib';

// ─── Internal state ───────────────────────────────────────────────────────────

interface StoredMessage {
  content: Buffer;
  properties: amqplib.MessageProperties;
  /** Routing key the message was published with (what a real broker reports in x-death / fields). */
  routingKey?: string;
}

interface FakeQueue {
  name: string;
  options: amqplib.Options.AssertQueue;
  messages: StoredMessage[];
  /** "exchange|routingKey" */
  bindings: Set<string>;
}

interface Delivered {
  stored: StoredMessage;
  queue: string;
}

interface Consumer {
  tag: string;
  queue: string;
  handler: (msg: amqplib.ConsumeMessage | null) => void;
  channel: FakeChannel;
}

type DeliveredMessage = amqplib.Message & { fields: amqplib.ConsumeMessageFields & amqplib.GetMessageFields };

export interface FakeBroker {
  /** Open a new client connection. */
  connect(): amqplib.ChannelModel;
  /** Simulate the broker going away: every open connection and channel emits 'close'. */
  dropConnections(): void;
  /** Pre-create a queue as it might exist from a previous deployment. */
  seedQueue(name: string, options: amqplib.Options.AssertQueue, messages?: StoredMessage[]): void;
  hasQueue(name: string): boolean;
  queueOptions(name: string): amqplib.Options.AssertQueue | undefined;
  depth(queue: string): number;
  /** Deliver the head of `queue` on an internal channel (tracked as unacked). */
  take(queue: string): amqplib.ConsumeMessage | undefined;
  /** Remove and return everything in `queue` — an inspection, not a delivery (nothing is tracked as unacked). */
  drain(queue: string): Array<{ content: Buffer; properties: amqplib.MessageProperties }>;
  unackedCount(): number;
  openConnections(): number;
  /** Active consumers on `queue`. */
  consumerCount(queue: string): number;
  /** Total messages handed to consumers so far. */
  deliveries(): number;
  /** Make a message for tests (e.g. to seed a queue). */
  message(body: unknown, options?: amqplib.Options.Publish): StoredMessage;
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

function emptyProperties(): amqplib.MessageProperties {
  return {
    contentType: undefined,
    contentEncoding: undefined,
    headers: undefined,
    deliveryMode: undefined,
    priority: undefined,
    correlationId: undefined,
    replyTo: undefined,
    expiration: undefined,
    messageId: undefined,
    timestamp: undefined,
    type: undefined,
    userId: undefined,
    appId: undefined,
    clusterId: undefined,
  };
}

function propertiesFromPublish(options: amqplib.Options.Publish = {}): amqplib.MessageProperties {
  return {
    ...emptyProperties(),
    contentType: options.contentType,
    contentEncoding: options.contentEncoding,
    headers: options.headers as amqplib.MessagePropertyHeaders | undefined,
    deliveryMode: options.persistent ? 2 : options.deliveryMode,
    priority: options.priority,
    correlationId: options.correlationId,
    replyTo: options.replyTo,
    expiration: options.expiration,
    messageId: options.messageId,
    timestamp: options.timestamp,
    type: options.type,
    userId: options.userId,
    appId: options.appId,
  };
}

function amqpError(code: number, text: string): Error {
  const err = new Error(`Channel closed by server: ${code} with message "${text}"`) as Error & { code: number };
  err.code = code;
  return err;
}

function argsOf(options: amqplib.Options.AssertQueue): Record<string, unknown> {
  return (options.arguments ?? {}) as Record<string, unknown>;
}

function sameArgs(a: amqplib.Options.AssertQueue, b: amqplib.Options.AssertQueue): boolean {
  const argsA = argsOf(a);
  const argsB = argsOf(b);
  const keys = new Set([...Object.keys(argsA), ...Object.keys(argsB)]);
  return (a.durable ?? false) === (b.durable ?? false) && [...keys].every((k) => argsA[k] === argsB[k]);
}

function firstMismatch(existing: amqplib.Options.AssertQueue, wanted: amqplib.Options.AssertQueue): string {
  if ((existing.durable ?? false) !== (wanted.durable ?? false)) return 'durable';
  const argsE = argsOf(existing);
  const argsW = argsOf(wanted);
  return [...new Set([...Object.keys(argsE), ...Object.keys(argsW)])].find((k) => argsE[k] !== argsW[k]) ?? 'arguments';
}

/** Mirror RabbitMQ's x-death bookkeeping for a rejected message. */
function withDeath(stored: StoredMessage, fromQueue: string, exchange: string, routingKey: string): StoredMessage {
  const headers: amqplib.MessagePropertyHeaders = { ...stored.properties.headers };
  const deaths = [...(headers['x-death'] ?? [])];
  const existing = deaths.findIndex((d) => d.queue === fromQueue && d.reason === 'rejected');
  const now = { '!': 'timestamp' as const, value: Math.floor(Date.now() / 1000) };

  if (existing === -1) {
    deaths.unshift({ count: 1, reason: 'rejected', queue: fromQueue, time: now, exchange, 'routing-keys': [routingKey] });
  } else {
    const [entry] = deaths.splice(existing, 1);
    if (entry) deaths.unshift({ ...entry, count: entry.count + 1, time: now });
  }

  headers['x-death'] = deaths;
  headers['x-first-death-exchange'] ??= exchange;
  headers['x-first-death-queue'] ??= fromQueue;
  headers['x-first-death-reason'] ??= 'rejected';

  return { content: stored.content, properties: { ...stored.properties, headers } };
}

// ─── Broker ───────────────────────────────────────────────────────────────────

class FakeChannel extends EventEmitter {
  public closed = false;
  public readonly unacked = new Map<number, Delivered>();

  constructor(private readonly broker: Core, private readonly connection: FakeConnection) {
    super();
  }

  // Every method is a vi.fn so tests can assert on protocol-level calls.
  assertExchange = vi.fn((name: string): Promise<amqplib.Replies.AssertExchange> => {
    this.guard();
    this.broker.exchanges.add(name);
    return Promise.resolve({ exchange: name });
  });

  assertQueue = vi.fn((name: string, options: amqplib.Options.AssertQueue = {}): Promise<amqplib.Replies.AssertQueue> => {
    this.guard();
    const existing = this.broker.queues.get(name);
    if (existing && !sameArgs(existing.options, options)) {
      return Promise.reject(
        this.fail(406, `PRECONDITION_FAILED - inequivalent arg '${firstMismatch(existing.options, options)}' for queue '${name}' in vhost '/'`),
      );
    }
    if (!existing) this.broker.queues.set(name, { name, options, messages: [], bindings: new Set() });
    return Promise.resolve(this.replyFor(name));
  });

  checkQueue = vi.fn((name: string): Promise<amqplib.Replies.AssertQueue> => {
    this.guard();
    if (!this.broker.queues.has(name)) {
      return Promise.reject(this.fail(404, `NOT_FOUND - no queue '${name}' in vhost '/'`));
    }
    return Promise.resolve(this.replyFor(name));
  });

  deleteQueue = vi.fn((name: string, options: amqplib.Options.DeleteQueue = {}): Promise<amqplib.Replies.DeleteQueue> => {
    this.guard();
    const queue = this.broker.queues.get(name);
    if (!queue) return Promise.reject(this.fail(404, `NOT_FOUND - no queue '${name}' in vhost '/'`));
    if (options.ifEmpty && queue.messages.length > 0) {
      return Promise.reject(this.fail(406, `PRECONDITION_FAILED - queue '${name}' in vhost '/' not empty`));
    }
    const messageCount = queue.messages.length;
    this.broker.queues.delete(name);
    this.broker.consumers = this.broker.consumers.filter((c) => c.queue !== name);
    return Promise.resolve({ messageCount });
  });

  bindQueue = vi.fn((queue: string, exchange: string, routingKey: string): Promise<amqplib.Replies.Empty> => {
    this.guard();
    this.broker.queue(queue).bindings.add(`${exchange}|${routingKey}`);
    return Promise.resolve({});
  });

  prefetch = vi.fn((): Promise<amqplib.Replies.Empty> => Promise.resolve({}));

  publish = vi.fn((exchange: string, routingKey: string, content: Buffer, options?: amqplib.Options.Publish): boolean => {
    this.guard();
    this.broker.route(exchange, routingKey, { content, properties: propertiesFromPublish(options), routingKey });
    return true;
  });

  get = vi.fn((queue: string): Promise<amqplib.GetMessage | false> => {
    this.guard();
    const q = this.broker.queues.get(queue);
    if (!q) return Promise.reject(this.fail(404, `NOT_FOUND - no queue '${queue}' in vhost '/'`));
    const stored = q.messages.shift();
    return Promise.resolve(stored ? this.deliver(queue, stored) : false);
  });

  consume = vi.fn(
    (queue: string, handler: (msg: amqplib.ConsumeMessage | null) => void): Promise<amqplib.Replies.Consume> => {
      this.guard();
      if (!this.broker.queues.has(queue)) return Promise.reject(this.fail(404, `NOT_FOUND - no queue '${queue}' in vhost '/'`));
      const tag = `ctag-${this.broker.nextTag()}`;
      this.broker.consumers.push({ tag, queue, handler, channel: this });
      this.broker.pump(queue);
      return Promise.resolve({ consumerTag: tag });
    },
  );

  cancel = vi.fn((tag: string): Promise<amqplib.Replies.Empty> => {
    this.broker.consumers = this.broker.consumers.filter((c) => c.tag !== tag);
    return Promise.resolve({});
  });

  ack = vi.fn((msg: amqplib.Message): void => {
    this.guard();
    this.unacked.delete(msg.fields.deliveryTag);
  });

  nack = vi.fn((msg: amqplib.Message, _allUpTo?: boolean, requeue?: boolean): void => {
    this.guard();
    this.settle(msg.fields.deliveryTag, requeue ?? true);
  });

  nackAll = vi.fn((requeue?: boolean): void => {
    this.guard();
    for (const tag of [...this.unacked.keys()].reverse()) this.settle(tag, requeue ?? true);
  });

  waitForConfirms = vi.fn((): Promise<void> => Promise.resolve());

  close = vi.fn((): Promise<void> => {
    if (this.closed) return Promise.reject(new Error('Channel closed'));
    this.terminate();
    return Promise.resolve();
  });

  /** Server-side close: requeue unacked, drop consumers, emit 'close'. */
  terminate(): void {
    if (this.closed) return;
    this.closed = true;
    for (const tag of [...this.unacked.keys()].reverse()) this.settle(tag, true);
    this.broker.consumers = this.broker.consumers.filter((c) => c.channel !== this);
    this.connection.channels.delete(this);
    this.emit('close');
  }

  deliver(queue: string, stored: StoredMessage): DeliveredMessage {
    const tag = this.broker.nextTag();
    this.unacked.set(tag, { stored, queue });
    this.broker.delivered += 1;
    return {
      content: stored.content,
      properties: stored.properties,
      fields: {
        deliveryTag: tag,
        redelivered: false,
        exchange: '',
        routingKey: stored.routingKey ?? queue,
        consumerTag: 'fake-ctag',
        messageCount: this.broker.queue(queue).messages.length,
      },
    };
  }

  private settle(tag: number, requeue: boolean): void {
    const delivered = this.unacked.get(tag);
    if (!delivered) return;
    this.unacked.delete(tag);
    const queue = this.broker.queues.get(delivered.queue);
    if (!queue) return; // queue deleted meanwhile — message is gone, like on a real broker

    if (requeue) {
      queue.messages.unshift(delivered.stored);
      this.broker.pump(queue.name);
      return;
    }
    const args = argsOf(queue.options);
    const dlx = args['x-dead-letter-exchange'];
    if (typeof dlx !== 'string') return; // no DLX configured → dropped
    const dlKey = typeof args['x-dead-letter-routing-key'] === 'string' ? args['x-dead-letter-routing-key'] : (delivered.stored.routingKey ?? queue.name);
    this.broker.route(dlx, dlKey, withDeath(delivered.stored, queue.name, 'hyperpay', delivered.stored.routingKey ?? queue.name));
  }

  private guard(): void {
    if (this.closed) throw new Error('Channel closed');
  }

  /** Like amqplib: a server-initiated close emits 'error', then 'close'. */
  private fail(code: number, text: string): Error {
    const err = amqpError(code, text);
    this.emit('error', err);
    this.terminate();
    return err;
  }

  private replyFor(name: string): amqplib.Replies.AssertQueue {
    return {
      queue: name,
      messageCount: this.broker.queue(name).messages.length,
      consumerCount: this.broker.consumers.filter((c) => c.queue === name).length,
    };
  }
}

class FakeConnection extends EventEmitter {
  public closed = false;
  public readonly channels = new Set<FakeChannel>();

  constructor(private readonly broker: Core) {
    super();
  }

  createChannel = vi.fn((): Promise<amqplib.Channel> => Promise.resolve(this.open() as unknown as amqplib.Channel));
  createConfirmChannel = vi.fn((): Promise<amqplib.ConfirmChannel> =>
    Promise.resolve(this.open() as unknown as amqplib.ConfirmChannel),
  );

  close = vi.fn((): Promise<void> => {
    if (this.closed) return Promise.reject(new Error('Connection closed'));
    this.terminate();
    return Promise.resolve();
  });

  /** Either side closing: channels go first, then the connection. */
  terminate(): void {
    if (this.closed) return;
    this.closed = true;
    for (const channel of [...this.channels]) channel.terminate();
    this.broker.connections.delete(this);
    this.emit('close');
  }

  private open(): FakeChannel {
    if (this.closed) throw new Error('Connection closed');
    const channel = new FakeChannel(this.broker, this);
    this.channels.add(channel);
    return channel;
  }
}

class Core {
  public readonly exchanges = new Set<string>();
  public readonly queues = new Map<string, FakeQueue>();
  public readonly connections = new Set<FakeConnection>();
  public consumers: Consumer[] = [];
  public delivered = 0;
  private tag = 0;
  /** Internal channel used by `take()`. */
  private readonly system: FakeChannel;

  constructor() {
    this.system = new FakeChannel(this, new FakeConnection(this));
  }

  nextTag(): number {
    this.tag += 1;
    return this.tag;
  }

  queue(name: string): FakeQueue {
    const q = this.queues.get(name);
    if (!q) throw new Error(`NOT_FOUND - no queue '${name}'`);
    return q;
  }

  route(exchange: string, routingKey: string, stored: StoredMessage): void {
    const targets =
      exchange === ''
        ? [this.queues.get(routingKey)].filter((q): q is FakeQueue => q !== undefined)
        : [...this.queues.values()].filter((q) => q.bindings.has(`${exchange}|${routingKey}`));
    for (const target of targets) {
      target.messages.push(stored);
      this.pump(target.name);
    }
  }

  private roundRobin = new Map<string, number>();

  /** Hand queued messages to the queue's consumers (asynchronously, like a broker push), round-robin. */
  pump(queueName: string): void {
    setImmediate(() => {
      const queue = this.queues.get(queueName);
      const candidates = this.consumers.filter((c) => c.queue === queueName && !c.channel.closed);
      if (!queue || candidates.length === 0) return;
      const stored = queue.messages.shift();
      if (!stored) return;
      const turn = this.roundRobin.get(queueName) ?? 0;
      const consumer = candidates[turn % candidates.length];
      this.roundRobin.set(queueName, turn + 1);
      if (!consumer) return;
      consumer.handler(consumer.channel.deliver(queueName, stored));
      if (queue.messages.length > 0) this.pump(queueName);
    });
  }

  takeOnSystemChannel(queueName: string): amqplib.ConsumeMessage | undefined {
    const stored = this.queue(queueName).messages.shift();
    return stored ? this.system.deliver(queueName, stored) : undefined;
  }

  unackedCount(): number {
    let count = this.system.unacked.size;
    for (const connection of this.connections) {
      for (const channel of connection.channels) count += channel.unacked.size;
    }
    return count;
  }
}

export function makeFakeBroker(): FakeBroker {
  const core = new Core();

  return {
    connect: (): amqplib.ChannelModel => {
      const connection = new FakeConnection(core);
      core.connections.add(connection);
      return connection as unknown as amqplib.ChannelModel;
    },
    dropConnections: (): void => {
      for (const connection of [...core.connections]) connection.terminate();
    },
    seedQueue: (name, options, messages = []): void => {
      core.queues.set(name, { name, options, messages: [...messages], bindings: new Set() });
    },
    hasQueue: (name): boolean => core.queues.has(name),
    queueOptions: (name): amqplib.Options.AssertQueue | undefined => core.queues.get(name)?.options,
    depth: (queue): number => core.queue(queue).messages.length,
    take: (queue): amqplib.ConsumeMessage | undefined => core.takeOnSystemChannel(queue),
    drain: (queue): Array<{ content: Buffer; properties: amqplib.MessageProperties }> => core.queue(queue).messages.splice(0),
    unackedCount: (): number => core.unackedCount(),
    openConnections: (): number => core.connections.size,
    consumerCount: (queue): number => core.consumers.filter((c) => c.queue === queue && !c.channel.closed).length,
    deliveries: (): number => core.delivered,
    message: (body, options = {}): StoredMessage => ({
      content: Buffer.from(JSON.stringify(body)),
      properties: propertiesFromPublish({ persistent: true, contentType: 'application/json', ...options }),
    }),
  };
}
