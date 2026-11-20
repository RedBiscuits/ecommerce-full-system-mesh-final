import type { ChannelModel } from 'amqplib';

/** Opens ONE raw broker connection; reconnection is layered on top. */
export type AmqpConnect = (url: string) => Promise<ChannelModel>;

export type BrokerDriver = 'fake' | 'amqp';

/**
 * Resolve the AMQP connect function for a driver:
 *  - 'amqp': real RabbitMQ via amqplib (RABBITMQ_URL)
 *  - 'fake': the in-process @onify/fake-amqplib broker, which implements the
 *            amqplib API with RabbitMQ semantics (exchanges, bindings, DLX,
 *            per-message TTL, confirms, prefetch). Every connection opened to
 *            the same URL reaches the same in-memory broker, which is how the
 *            services share one broker when they run in one process.
 * The code path above the connect function is identical for both drivers.
 */
export function resolveAmqpConnect(driver: BrokerDriver): AmqpConnect {
  if (driver === 'amqp') {
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const amqplib = require('amqplib') as { connect: (url: string) => Promise<ChannelModel> };
    return (url) => amqplib.connect(url);
  }
  // Loaded lazily so production ('amqp') never requires the fake.
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const fake = require('@onify/fake-amqplib') as { connect: (url: string) => Promise<unknown> };
  ensureRabbitMqDirectExchangeSemantics();
  return fake.connect as unknown as AmqpConnect;
}

interface SmqpBinding {
  queue: { name: string; queueMessage: (fields: unknown, content: unknown, properties: unknown) => unknown };
  testPattern: (routingKey: string) => boolean;
}

interface SmqpExchangeProto {
  _onDirectMessage: (routingKey: string, message: any) => number;
  _emitReturn: (routingKey: string, content: unknown, properties: unknown) => void;
  __rabbitDirectSemantics?: boolean;
}

/**
 * Fidelity fix for the in-process broker. AMQP 0-9-1 / RabbitMQ route a message
 * published to a DIRECT exchange to EVERY queue bound with a matching key (each
 * gets its own copy). The engine behind @onify/fake-amqplib (smqp) instead
 * load-balances: it hands each message to ONE matching binding, round-robin.
 * With several consumers bound to the same key (e.g. `hyperpay` /
 * `payment.result` → hyperpay.results, hyperpay.events, billing.payment-results)
 * that silently drops the message for every queue but one — a behaviour no real
 * broker has. Restore RabbitMQ semantics so the sandbox behaves like production.
 * Idempotent; applies to the fake broker only.
 */
export function ensureRabbitMqDirectExchangeSemantics(): void {
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const smqp = require('smqp') as { Exchange: (name: string, type: string) => object };
  const proto = Object.getPrototypeOf(smqp.Exchange('fidelity-probe', 'direct')) as SmqpExchangeProto;
  if (proto.__rabbitDirectSemantics) return;
  const K_BINDINGS = Symbol.for('bindings'); // smqp keeps an exchange's bindings under this registry symbol
  const bindingsKey = (exchange: object): SmqpBinding[] => (exchange as Record<symbol, SmqpBinding[]>)[K_BINDINGS] ?? [];
  proto._onDirectMessage = function direct(this: SmqpExchangeProto & { name: string }, routingKey: string, message: any) {
    const published = message.content;
    message.ack();
    const delivered = new Set<string>();
    for (const binding of bindingsKey(this).slice()) {
      if (!binding.testPattern(routingKey) || delivered.has(binding.queue.name)) continue;
      delivered.add(binding.queue.name);
      binding.queue.queueMessage({ routingKey, exchange: this.name }, published.content, published.properties);
    }
    if (delivered.size === 0) this._emitReturn(routingKey, published.content, published.properties);
    return delivered.size;
  };
  proto.__rabbitDirectSemantics = true;
}

/**
 * Retry tiers are TTL queues. RabbitMQ expires (dead-letters) their messages by
 * itself; the in-process broker only does so when a queue is touched, so the
 * consumers poke their retry queues periodically on the fake driver.
 */
export function defaultTtlPumpIntervalMs(driver: BrokerDriver): number {
  return driver === 'fake' ? 250 : 0;
}

export function brokerDriverFromEnv(env: NodeJS.ProcessEnv = process.env): BrokerDriver {
  const raw = (env.PLATFORM_BROKER_DRIVER || env.EVENT_BUS_DRIVER || 'fake').trim();
  if (raw !== 'fake' && raw !== 'amqp') {
    throw new Error(`PLATFORM_BROKER_DRIVER must be 'fake' or 'amqp', got '${raw}'`);
  }
  return raw;
}
