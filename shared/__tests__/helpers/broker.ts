import type { Channel, ChannelModel } from 'amqplib';
import { AmqpConnect, resolveAmqpConnect } from '../../messaging/connect';
import type { DeliverySettled, ReliableConsumer } from '../../messaging/ReliableConsumer';

let seq = 0;

export interface TestBroker {
  url: string;
  /** Connect function that records every connection it opens (to simulate drops). */
  connect: AmqpConnect;
  connections: ChannelModel[];
  /** Independent channel on the same broker; also keeps the fake broker alive. */
  inspector: Channel;
  /** Simulate an outage for new connections. */
  state: { up: boolean };
  depth(queue: string): Promise<number>;
  publishJson(exchange: string, routingKey: string, body: unknown, options?: Record<string, unknown>): void;
  publishRaw(exchange: string, routingKey: string, content: Buffer, options?: Record<string, unknown>): void;
  /** Drop the newest live connection (the broker closes it). */
  dropLatest(): Promise<void>;
  close(): Promise<void>;
}

/**
 * An isolated in-process broker (@onify/fake-amqplib) per test. The fake
 * discards a broker when its last connection closes (RabbitMQ keeps durable
 * queues), so a keep-alive connection is held for the harness lifetime.
 */
export async function createTestBroker(): Promise<TestBroker> {
  const url = `amqp://shared-test-${process.pid}-${++seq}`;
  const raw = resolveAmqpConnect('fake');
  const keepAlive = await raw(url);
  const inspector = await keepAlive.createChannel();
  inspector.on('error', () => undefined);
  const connections: ChannelModel[] = [];
  const state = { up: true };

  return {
    url,
    connections,
    inspector,
    state,
    connect: async (u) => {
      if (!state.up) throw new Error('ECONNREFUSED (simulated broker outage)');
      const conn = await raw(u);
      connections.push(conn);
      return conn;
    },
    depth: async (queue) => (await inspector.checkQueue(queue)).messageCount,
    publishJson: (exchange, routingKey, body, options = {}) => {
      inspector.publish(exchange, routingKey, Buffer.from(JSON.stringify(body)), {
        persistent: true,
        contentType: 'application/json',
        ...options,
      });
    },
    publishRaw: (exchange, routingKey, content, options = {}) => {
      inspector.publish(exchange, routingKey, content, { persistent: true, ...options });
    },
    dropLatest: async () => {
      const conn = connections.at(-1);
      if (conn) await conn.close();
    },
    close: async () => {
      await keepAlive.close().catch(() => undefined);
    },
  };
}

/** Resolve on the first 'settled' event matching `predicate`. */
export function nextSettled(
  consumer: ReliableConsumer<unknown> | ReliableConsumer<any>,
  predicate: (e: DeliverySettled) => boolean = () => true,
  timeoutMs = 5000
): Promise<DeliverySettled> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      consumer.off('settled', onSettled);
      reject(new Error(`timed out waiting for a settled delivery on ${consumer.name}`));
    }, timeoutMs);
    const onSettled = (e: DeliverySettled) => {
      if (!predicate(e)) return;
      clearTimeout(timer);
      consumer.off('settled', onSettled);
      resolve(e);
    };
    consumer.on('settled', onSettled);
  });
}

export async function waitUntil(
  condition: () => boolean | Promise<boolean>,
  { timeoutMs = 5000, intervalMs = 10, label = 'condition' } = {}
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (await condition()) return;
    if (Date.now() > deadline) throw new Error(`waitUntil: timed out after ${timeoutMs}ms waiting for ${label}`);
    await new Promise((r) => setTimeout(r, intervalMs));
  }
}
