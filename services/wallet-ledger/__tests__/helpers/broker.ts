import type { Channel, ChannelModel } from 'amqplib';
import {
  AmqpEventBus,
  DeliverySettled,
  EventBusOptions,
  TOPOLOGY,
  createEventBus,
  resolveConnect,
} from '../../src/messaging';

let urlSeq = 0;

/** Retry tiers long enough that the fake never expires them on its own. */
export const TEST_RETRY_DELAYS_MS = [60_000, 60_000];

export interface BrokerHarness {
  url: string;
  bus: AmqpEventBus;
  /** Independent channel on the same fake broker (also keeps the broker alive). */
  inspector: Channel;
  /** Raw connections the bus opened, newest last (to simulate broker drops). */
  connections: ChannelModel[];
  /** Toggle to simulate the broker being unreachable for new connections. */
  broker: { up: boolean };
  /**
   * One-shot fault: the next connection fails part-way through session setup.
   * `holdFailingSetup` pauses that setup mid-build (so a test can observe the
   * window in which a half-built session exists) until `release` resolves.
   */
  faults: {
    failNextSetup: boolean;
    holdFailingSetup?: { reached: () => void; release: Promise<void> };
  };
  close(): Promise<void>;
}

/**
 * An isolated fake broker per test. The fake discards a broker once its last
 * connection closes (RabbitMQ would keep durable queues), so the inspector
 * connection stays open for the harness lifetime to keep state across bus
 * reconnects.
 */
export async function createBrokerHarness(overrides: Partial<EventBusOptions> = {}): Promise<BrokerHarness> {
  const url = `amqp://test-broker-${process.pid}-${++urlSeq}`;
  const rawConnect = resolveConnect('fake');
  const keepAlive = await rawConnect(url);
  const inspector = await keepAlive.createChannel();
  inspector.on('error', () => undefined);

  const connections: ChannelModel[] = [];
  const broker = { up: true };
  const faults: BrokerHarness['faults'] = { failNextSetup: false };
  const bus = createEventBus({
    url,
    connect: async (u) => {
      if (!broker.up) throw new Error('ECONNREFUSED (simulated broker outage)');
      const conn = await rawConnect(u);
      connections.push(conn);
      if (faults.failNextSetup) {
        faults.failNextSetup = false;
        // The connection opens fine, but the session build breaks part-way
        // (after the publish channel exists, while declaring topology).
        const createConfirmChannel = conn.createConfirmChannel.bind(conn);
        conn.createConfirmChannel = async (...args: Parameters<typeof createConfirmChannel>) => {
          const ch = await createConfirmChannel(...args);
          const hold = faults.holdFailingSetup;
          faults.holdFailingSetup = undefined;
          ch.assertExchange = async () => {
            hold?.reached();
            await hold?.release;
            throw new Error('simulated failure during session setup');
          };
          return ch;
        };
      }
      return conn;
    },
    retryDelaysMs: TEST_RETRY_DELAYS_MS,
    maxAttempts: 3,
    prefetch: 4,
    reconnect: { initialDelayMs: 1, maxDelayMs: 5, jitter: 0 },
    ...overrides,
  });
  await bus.start();

  return {
    url,
    bus,
    inspector,
    connections,
    broker,
    faults,
    close: async () => {
      await bus.close();
      await keepAlive.close();
    },
  };
}

/**
 * RabbitMQ expires a retry-tier message after its TTL and dead-letters it back
 * to the work queue. The fake only expires lazily (when touched) and by wall
 * clock, so tests perform that exact move themselves, triggered by the bus's
 * own 'retry-scheduled' event. No sleeping, fully deterministic.
 */
export function simulateBrokerTtlExpiry(h: BrokerHarness): () => void {
  const onSettled = (e: DeliverySettled) => {
    if (e.outcome !== 'retry-scheduled' || !e.retryQueue) return;
    const retryQueue = e.retryQueue;
    void (async () => {
      const msg = await h.inspector.get(retryQueue, { noAck: false });
      if (!msg) return;
      h.inspector.publish(TOPOLOGY.requeueExchange, TOPOLOGY.workQueue, msg.content, {
        persistent: true,
        contentType: msg.properties.contentType as string | undefined,
        messageId: msg.properties.messageId as string | undefined,
        type: msg.properties.type as string | undefined,
        headers: msg.properties.headers,
      });
      h.inspector.ack(msg);
    })();
  };
  h.bus.on('delivery-settled', onSettled);
  return () => h.bus.off('delivery-settled', onSettled);
}

export async function queueDepth(h: BrokerHarness, queue: string): Promise<number> {
  return (await h.inspector.checkQueue(queue)).messageCount;
}

export interface DeadLetter {
  type: string;
  reason: string;
  attempt: number;
  body: string;
}

export async function drainDeadLetters(h: BrokerHarness): Promise<DeadLetter[]> {
  const out: DeadLetter[] = [];
  for (;;) {
    const msg = await h.inspector.get(TOPOLOGY.deadLetterQueue, { noAck: true });
    if (!msg) return out;
    const headers = (msg.properties.headers ?? {}) as Record<string, unknown>;
    out.push({
      type: String(headers['x-original-routing-key'] ?? msg.fields.routingKey),
      reason: String(headers['x-failure-reason']),
      attempt: Number(headers['x-attempt']),
      body: msg.content.toString(),
    });
  }
}
