/**
 * src/messaging/runtime.ts
 *
 * Starts the service's broker side: the self-healing AMQP client (topology
 * reconciled on every connect) and the transactional-outbox publisher, which
 * is woken by every committed outbox write and by every (re)connect and moves
 * PAYMENT_COMPLETED events to the `hyperpay` exchange with publisher confirms.
 *
 * The HTTP path never publishes: the webhook only commits the payment and its
 * outbox row (see services/paymentWebhook.ts).
 */

import amqplib from 'amqplib';
import { config } from '../config/env';
import { outbox } from '../services/outbox';
import { AmqpClient } from './amqpClient';
import {
  OutboxCycle,
  OutboxPublisher,
  OutboxPublisherOptions,
  createOutboxPublisher,
  onActiveConnectionChange,
  setActiveConnection,
} from './outboxPublisher';
import { ensureTopology } from './topology';

export interface HyperPayMessagingOptions {
  url?: string;
  /** Injectable connect (the in-process fake broker, or amqplib by default). */
  connect?: (url: string) => Promise<amqplib.ChannelModel>;
  retryDelayMs?: number;
  maxRetryDelayMs?: number;
  outbox?: Partial<Omit<OutboxPublisherOptions, 'getConnection' | 'subscribe'>>;
}

export interface HyperPayMessaging {
  client: AmqpClient;
  publisher: OutboxPublisher;
  /** Publish everything committed so far (one full publisher cycle). */
  flush(): Promise<OutboxCycle>;
  stop(): Promise<void>;
}

export async function startHyperPayMessaging(options: HyperPayMessagingOptions = {}): Promise<HyperPayMessaging> {
  const client = new AmqpClient({
    url: options.url ?? config.rabbitmq.url,
    retryDelayMs: options.retryDelayMs ?? config.rabbitmq.retryDelayMs,
    maxRetryDelayMs: options.maxRetryDelayMs ?? config.rabbitmq.maxRetryDelayMs,
    prefetch: config.charges.concurrency,
    ensureTopology: (connection) => ensureTopology(connection),
    onConnectionChange: setActiveConnection,
    ...(options.connect ? { connect: options.connect } : {}),
  });

  const publisher = createOutboxPublisher({
    ...config.outbox,
    ...options.outbox,
    getConnection: () => client.getConnection(),
    subscribe: (wake) => {
      const offEnqueued = outbox.onEnqueued(wake);
      const offConnection = onActiveConnectionChange((conn) => {
        if (conn) wake();
      });
      return () => {
        offEnqueued();
        offConnection();
      };
    },
  });

  await client.start();
  publisher.start();

  return {
    client,
    publisher,
    flush: () => publisher.runNow(),
    stop: async () => {
      await publisher.stop();
      await client.stop();
    },
  };
}
