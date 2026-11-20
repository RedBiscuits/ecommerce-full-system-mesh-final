/**
 * Declare the whole platform topology on a broker, idempotently, before any
 * service starts (so no event can be published to an exchange whose consumer
 * queues do not exist yet). Every service also (re)declares its own part on
 * every connect, so running this is safe at any time.
 *
 *   npm run cluster:topology                                 (in-process broker: prints the plan)
 *   PLATFORM_BROKER_DRIVER=amqp RABBITMQ_URL=amqp://… npm run cluster:topology   (real RabbitMQ)
 */
import type { ChannelModel } from 'amqplib';
import {
  BrokerDriver,
  DEFAULT_RETRY_POLICY,
  PLATFORM_EXCHANGES,
  brokerDriverFromEnv,
  consumerQueues,
  declareConsumerTopology,
  resolveAmqpConnect,
} from '../shared/messaging';
import { CONSUMERS, CONSUMER_BINDINGS, EXCHANGES } from '../shared/contracts';
import { CONSUMER_SERVICE, EVENT_STREAMS, keyIdFor } from '../shared/security';
import { ensureTopology, topologyFromConfig } from '../services/hyperpay-gateway/src/messaging/topology';
import { TOPOLOGY as LEDGER_TOPOLOGY, createEventBus } from '../services/wallet-ledger/src/messaging';
import { config as ledgerConfig } from '../services/wallet-ledger/src/config/env';

export interface PlatformTopologyReport {
  broker: { driver: BrokerDriver; url: string };
  exchanges: string[];
  /** Work queues (services' own and the integration consumers'). */
  queues: string[];
  retryQueues: string[];
  deadLetterQueues: string[];
  /** consumer queue → what it is bound to. */
  consumers: Record<string, string[]>;
  /**
   * The service-to-service authorization matrix: who may publish a stream, who may consume it
   * (the holders of its shared key) and which key id signs it.
   */
  access: Array<{ stream: string; issuer: string; audience: string[]; keyId: string; routingKeys: string[] }>;
  /** consumer queue → the service that authenticates its deliveries. */
  consumerServices: Record<string, string>;
}

export interface TopologyOptions {
  driver?: BrokerDriver;
  url?: string;
  /** Attempts of the integration consumers: one retry tier per attempt. */
  maxAttempts?: number;
}

let seq = 0;

export async function bootstrapPlatformTopology(options: TopologyOptions = {}): Promise<PlatformTopologyReport> {
  const driver = options.driver ?? brokerDriverFromEnv();
  const url =
    options.url ??
    (driver === 'fake' ? `amqp://ksa-topology-${process.pid}-${++seq}` : process.env.RABBITMQ_URL ?? 'amqp://localhost:5672');
  const connect = resolveAmqpConnect(driver);
  const maxAttempts = options.maxAttempts ?? DEFAULT_RETRY_POLICY.maxAttempts;

  const conn: ChannelModel = await connect(url);
  conn.on('error', () => undefined);
  try {
    // hyperpay-gateway: its own self-healing topology (migrates mismatched queues).
    await ensureTopology(conn);
    const hyperpay = topologyFromConfig();

    // wallet-ledger: the invariant-event bus declares its queues, retry tiers and DLQ.
    const bus = createEventBus({ url, connect: () => connect(url) });
    await bus.start();
    await bus.close();

    const ch = await conn.createChannel();
    ch.on('error', () => undefined);
    for (const exchange of Object.values(EXCHANGES)) {
      await ch.assertExchange(exchange.name, exchange.type, { durable: true });
    }
    const consumers: Record<string, string[]> = {};
    const retryQueues: string[] = ledgerConfig.eventBus.retryDelaysMs.map((_, i) => LEDGER_TOPOLOGY.retryQueue(i + 1));
    const deadLetterQueues: string[] = [hyperpay.queues[0].name, LEDGER_TOPOLOGY.deadLetterQueue];
    for (const name of Object.values(CONSUMERS)) {
      const bindings = CONSUMER_BINDINGS[name];
      const queues = await declareConsumerTopology(ch, name, bindings, maxAttempts);
      consumers[name] = bindings.map((b) => `${b.exchange} (${b.exchangeType}) / ${b.routingKey}`);
      retryQueues.push(...queues.retryQueues);
      deadLetterQueues.push(queues.deadLetterQueue);
    }
    await ch.close();

    return {
      broker: { driver, url: driver === 'amqp' ? redactUrl(url) : url },
      exchanges: unique([
        ...hyperpay.exchanges.map((e) => e.name),
        LEDGER_TOPOLOGY.exchange,
        LEDGER_TOPOLOGY.requeueExchange,
        LEDGER_TOPOLOGY.deadLetterExchange,
        ...Object.values(EXCHANGES).map((e) => e.name),
        PLATFORM_EXCHANGES.requeue,
        PLATFORM_EXCHANGES.deadLetter,
      ]),
      queues: unique([
        ...hyperpay.queues.filter((q) => q.name !== hyperpay.queues[0].name).map((q) => q.name),
        LEDGER_TOPOLOGY.workQueue,
        ...Object.values(CONSUMERS).map((name) => consumerQueues(name, maxAttempts).workQueue),
      ]),
      retryQueues,
      deadLetterQueues,
      consumers,
      access: Object.values(EVENT_STREAMS).map((stream) => ({
        stream: stream.id,
        issuer: stream.issuer,
        audience: [...stream.audience],
        keyId: keyIdFor(stream.id),
        routingKeys: [...stream.routingKeys],
      })),
      consumerServices: { ...CONSUMER_SERVICE },
    };
  } finally {
    await conn.close().catch(() => undefined);
  }
}

function unique(values: string[]): string[] {
  return [...new Set(values)];
}

function redactUrl(url: string): string {
  return url.replace(/\/\/([^:@/]+):([^@/]+)@/, '//$1:***@');
}

if (require.main === module) {
  bootstrapPlatformTopology()
    .then((report) => {
      console.log('KSA Fintech & ZATCA AMQP topology declared:');
      console.log(JSON.stringify(report, null, 2));
    })
    .catch((err) => {
      console.error('Topology bootstrap failed:', err);
      process.exit(1);
    });
}
