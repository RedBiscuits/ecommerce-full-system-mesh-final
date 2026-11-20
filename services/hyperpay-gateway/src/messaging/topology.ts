/**
 * src/messaging/topology.ts
 *
 * Declarative broker topology and the logic that makes it self-healing.
 *
 *   exchange  hyperpay      (direct, durable)
 *     └─ queue hyperpay.payments   ← payment.verify
 *            x-dead-letter-exchange    = hyperpay.dlx
 *            x-dead-letter-routing-key = payment.dead
 *     └─ queue hyperpay.charges    ← charge.execute
 *            x-dead-letter-exchange    = hyperpay.dlx
 *            x-dead-letter-routing-key = payment.dead
 *     └─ queue hyperpay.results    ← payment.result
 *     └─ queue hyperpay.events     ← payment.result, token.registered, refund.processed
 *                                    (every domain event from the outbox, in one stream)
 *
 *   exchange  hyperpay.dlx  (direct, durable)
 *     └─ queue hyperpay.dlq        ← payment.dead
 *
 * ─── Migration ───────────────────────────────────────────────────────────────
 *
 * RabbitMQ refuses to re-declare an existing queue with different arguments
 * (406 PRECONDITION_FAILED) and closes the channel. That happens on the first
 * deploy of any change to a queue's arguments — e.g. adding dead-lettering to
 * a `hyperpay.payments` that already exists. `ensureTopology` handles it:
 *
 *   mode `recreate` (default)
 *     1. park every message of the offending queue in `<queue>.migrating`
 *        (durable; moved with publisher confirms, nothing is lost),
 *     2. delete the queue (only if empty — a concurrent producer makes us retry),
 *     3. re-declare it with the new arguments and bindings,
 *     4. restore the parked messages (including any left by an earlier,
 *        interrupted migration) and drop the parking queue.
 *   mode `fail`
 *     Throw a TopologyMismatchError that spells out the manual fix.
 */

import type amqplib from 'amqplib';
import { config } from '../config/env';
import type { TopologyMigrationMode } from '../config/env';
import {
  isPreconditionFailed,
  moveMessages,
  openChannel,
  openConfirmChannel,
  safeClose,
} from './amqpUtils';

// ─── Types ────────────────────────────────────────────────────────────────────

export interface ExchangeSpec {
  name: string;
  type: 'direct';
  options: amqplib.Options.AssertExchange;
}

export interface QueueBinding {
  exchange: string;
  routingKey: string;
}

export interface QueueSpec {
  name: string;
  options: amqplib.Options.AssertQueue;
  bindings: QueueBinding[];
}

export interface Topology {
  exchanges: ExchangeSpec[];
  /** Declared in order — put queues other queues depend on (DLQ) first. */
  queues: QueueSpec[];
}

export interface EnsureTopologyOptions {
  migration: TopologyMigrationMode;
}

export class TopologyMismatchError extends Error {
  public readonly queue: string;

  constructor(queue: string, cause: string) {
    super(
      `Queue "${queue}" already exists with different arguments and RABBITMQ_TOPOLOGY_MIGRATION=fail. ` +
        `Either set RABBITMQ_TOPOLOGY_MIGRATION=recreate, delete the queue while it is empty ` +
        `(rabbitmqadmin delete queue name=${queue}), or apply the same arguments via a policy. ` +
        `Broker said: ${cause}`,
    );
    this.name = 'TopologyMismatchError';
    this.queue = queue;
  }
}

// ─── Declarations ─────────────────────────────────────────────────────────────

/** The topology this service needs, derived from config. */
export function topologyFromConfig(): Topology {
  const { rabbitmq } = config;
  return {
    exchanges: [
      { name: rabbitmq.deadLetterExchange, type: 'direct', options: { durable: true } },
      { name: rabbitmq.exchange, type: 'direct', options: { durable: true } },
    ],
    queues: [
      // Dead-letter side first, so the main queue's DLX reference always resolves.
      {
        name: rabbitmq.deadLetterQueue,
        options: { durable: true },
        bindings: [{ exchange: rabbitmq.deadLetterExchange, routingKey: rabbitmq.deadLetterRoutingKey }],
      },
      {
        name: rabbitmq.queue,
        options: {
          durable: true,
          arguments: {
            'x-dead-letter-exchange': rabbitmq.deadLetterExchange,
            'x-dead-letter-routing-key': rabbitmq.deadLetterRoutingKey,
          },
        },
        bindings: [{ exchange: rabbitmq.exchange, routingKey: rabbitmq.routingKey }],
      },
      {
        name: rabbitmq.chargesQueue,
        options: {
          durable: true,
          arguments: {
            'x-dead-letter-exchange': rabbitmq.deadLetterExchange,
            'x-dead-letter-routing-key': rabbitmq.deadLetterRoutingKey,
          },
        },
        bindings: [{ exchange: rabbitmq.exchange, routingKey: rabbitmq.chargeRoutingKey }],
      },
      {
        name: rabbitmq.resultsQueue,
        options: { durable: true },
        bindings: [{ exchange: rabbitmq.exchange, routingKey: rabbitmq.resultsRoutingKey }],
      },
      {
        name: rabbitmq.eventsQueue,
        options: { durable: true },
        bindings: [
          { exchange: rabbitmq.exchange, routingKey: rabbitmq.resultsRoutingKey },
          { exchange: rabbitmq.exchange, routingKey: rabbitmq.tokenRegisteredRoutingKey },
          { exchange: rabbitmq.exchange, routingKey: rabbitmq.refundProcessedRoutingKey },
        ],
      },
    ],
  };
}

/** Name of the durable queue that holds a queue's messages while it is recreated. */
export function parkingQueueFor(queue: string): string {
  return `${queue}.migrating`;
}

async function declareQueue(channel: amqplib.Channel, spec: QueueSpec): Promise<void> {
  await channel.assertQueue(spec.name, spec.options);
  for (const binding of spec.bindings) {
    await channel.bindQueue(spec.name, binding.exchange, binding.routingKey);
  }
}

/** Declare everything on the given channel. Throws on the first mismatch. */
export async function declareTopology(channel: amqplib.Channel, topology: Topology): Promise<void> {
  for (const exchange of topology.exchanges) {
    await channel.assertExchange(exchange.name, exchange.type, exchange.options);
  }
  for (const queue of topology.queues) {
    await declareQueue(channel, queue);
  }
}

// ─── Migration ────────────────────────────────────────────────────────────────

/** Pull the offending queue name out of a 406 message. */
export function queueNameFromError(err: unknown): string | undefined {
  if (!(err instanceof Error)) return undefined;
  return /for queue '([^']+)'/.exec(err.message)?.[1];
}

/**
 * Recreate a queue with its current spec without losing messages.
 * Uses a dedicated confirm channel; safe to re-run after an interruption.
 */
export async function recreateQueue(connection: amqplib.ChannelModel, spec: QueueSpec): Promise<void> {
  const parking = parkingQueueFor(spec.name);
  const channel = await openConfirmChannel(connection, 'migration');
  try {
    const { messageCount } = await channel.checkQueue(spec.name);
    await channel.assertQueue(parking, { durable: true });

    if (messageCount > 0) {
      const parked = await moveMessages(channel, spec.name, parking, messageCount);
      console.warn(`[Topology] Parked ${parked} message(s) from "${spec.name}" in "${parking}".`);
    }

    // `ifEmpty` guards against a producer racing us: a 406 here simply fails
    // this attempt and the connection retry loop runs the migration again.
    await channel.deleteQueue(spec.name, { ifEmpty: true });
    await declareQueue(channel, spec);
    console.warn(`[Topology] Recreated "${spec.name}" with current arguments.`);

    // Restore everything in the parking queue — including leftovers from an
    // earlier migration that was interrupted before this step.
    const { messageCount: toRestore } = await channel.checkQueue(parking);
    if (toRestore > 0) {
      const restored = await moveMessages(channel, parking, spec.name, toRestore);
      console.warn(`[Topology] Restored ${restored} message(s) to "${spec.name}".`);
    }
    await channel.deleteQueue(parking, { ifEmpty: true });
  } finally {
    await safeClose(channel);
  }
}

/**
 * Make the broker match `topology`, migrating mismatched queues as allowed by
 * `options.migration`. Scratch channels only — the caller's channels are never
 * touched, so a 406 can never take the consumer down.
 */
export async function ensureTopology(
  connection: amqplib.ChannelModel,
  topology: Topology = topologyFromConfig(),
  options: EnsureTopologyOptions = { migration: config.rabbitmq.topologyMigration },
): Promise<void> {
  const migrated = new Set<string>();

  // Each mismatch costs one extra pass; there can be at most one per queue.
  for (let pass = 0; pass <= topology.queues.length; pass += 1) {
    const channel = await openChannel(connection, 'topology');
    try {
      await declareTopology(channel, topology);
      await safeClose(channel);
      return;
    } catch (err) {
      await safeClose(channel);
      if (!isPreconditionFailed(err)) throw err;

      const cause = err instanceof Error ? err.message : String(err);
      const name = queueNameFromError(err);
      const spec = topology.queues.find((q) => q.name === name);
      if (!name || !spec) {
        throw new Error(`[Topology] Broker rejected the topology and the queue could not be identified: ${cause}`);
      }
      if (options.migration === 'fail') throw new TopologyMismatchError(name, cause);
      if (migrated.has(name)) {
        throw new Error(`[Topology] Queue "${name}" still mismatched after recreation: ${cause}`);
      }

      console.warn(`[Topology] Queue "${name}" exists with different arguments — migrating. (${cause})`);
      await recreateQueue(connection, spec);
      migrated.add(name);
    }
  }

  throw new Error('[Topology] Gave up reconciling the topology after repeated mismatches.');
}
