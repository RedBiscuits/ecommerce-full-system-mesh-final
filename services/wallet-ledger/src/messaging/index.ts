import { connect as amqplibConnect } from 'amqplib';
import { config } from '../config/env';
import { AmqpConnect, AmqpEventBus, EventBusOptions } from './AmqpEventBus';

export * from './AmqpEventBus';
export * from './RecoveringConnection';
export * from './events';
export * from './errors';

/**
 * Resolve the AMQP connect function for the configured driver:
 *  - 'amqp': real RabbitMQ via amqplib (set RABBITMQ_URL)
 *  - 'fake': in-process @onify/fake-amqplib, which implements the amqplib API
 *            with RabbitMQ semantics (DLX, TTL, max-length, confirms,
 *            prefetch). This is the sandbox default, mirroring how the project
 *            uses ioredis-mock for Redis. The AmqpEventBus code path is
 *            identical for both drivers.
 */
export function resolveConnect(driver: 'fake' | 'amqp'): AmqpConnect {
  if (driver === 'amqp') {
    return (url) => amqplibConnect(url);
  }
  // Loaded lazily so production ('amqp') never requires the fake.
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const fake = require('@onify/fake-amqplib') as { connect: (url: string) => Promise<unknown> };
  return fake.connect as unknown as AmqpConnect;
}

export function createEventBus(overrides: Partial<EventBusOptions> = {}): AmqpEventBus {
  return new AmqpEventBus({
    url: config.eventBus.url,
    connect: resolveConnect(config.eventBus.driver),
    prefetch: config.eventBus.prefetch,
    maxAttempts: config.eventBus.maxAttempts,
    retryDelaysMs: config.eventBus.retryDelaysMs,
    maxQueueLength: config.eventBus.maxQueueLength,
    maxDeadLetterLength: config.eventBus.maxDeadLetterLength,
    maxPendingPublishes: config.eventBus.maxPendingPublishes,
    reconnect: config.eventBus.reconnect,
    ...overrides,
  });
}

let sharedBus: AmqpEventBus | undefined;

/** Process-wide event bus (lazily created from config). */
export function getEventBus(): AmqpEventBus {
  if (!sharedBus) {
    sharedBus = createEventBus();
  }
  return sharedBus;
}

/** Replace the process-wide bus (dependency injection for tests/bootstrapping). */
export function setEventBus(bus: AmqpEventBus | undefined): void {
  sharedBus = bus;
}
