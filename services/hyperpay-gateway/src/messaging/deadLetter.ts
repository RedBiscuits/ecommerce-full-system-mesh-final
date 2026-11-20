/**
 * src/messaging/deadLetter.ts
 *
 * Operations on the dead-letter queue (`hyperpay.dlq`):
 *
 *   inspectDlq(conn)  — non-destructive dump of parked messages
 *   replayDlq(conn)   — move parked messages back onto the main queue
 *
 * Both run on a *dedicated* channel created for the call and closed
 * afterwards, so a protocol error (e.g. the queue not existing) can never
 * take down the consumer's channel.
 *
 * Replay guarantees:
 *   - Bounded. At most `messageCount`-at-start messages are pulled, so a
 *     message that fails again immediately and is dead-lettered back onto the
 *     DLQ while we run is not replayed twice in the same pass.
 *   - At-least-once. Each message is published on a confirm channel and the
 *     DLQ copy is ACKed only after the broker confirms the publish. A crash in
 *     between duplicates the message rather than losing it — which is safe
 *     because `processPaymentEvent` is idempotent.
 *   - Serialised. Only one DLQ operation runs at a time (409 otherwise).
 *   - Traceable. Republished messages keep every original property and the
 *     broker's `x-death` history, and gain `x-replay-count` / `x-replayed-at`.
 *   - Routed home. A message goes back under the routing key it originally
 *     carried (from `x-death`), so a dead-lettered charge returns to the
 *     charges queue and a verify event to the payments queue.
 */

import type amqplib from 'amqplib';
import { config } from '../config/env';
import { HttpError } from '../utils/httpError';
import { asNumber, asString, carryProperties, openChannel, openConfirmChannel, safeClose } from './amqpUtils';
import type { AmqpConnection } from './amqpClient';

// ─── Types ────────────────────────────────────────────────────────────────────

/** Operator-facing description of one parked message. */
export interface DeadLetteredMessage {
  /** `correlationId` from the PaymentVerifyEvent body, when parseable. */
  correlationId?: string;
  messageId?: string;
  /** Parsed JSON body, or the raw UTF-8 string if the body is not JSON. */
  body: unknown;
  /** Routing key the message originally carried (before dead-lettering). */
  originalRoutingKey?: string;
  /** How many times the broker has dead-lettered this message. */
  deathCount: number;
  /** Broker's reason for the most recent dead-lettering. */
  lastReason?: amqplib.XDeath['reason'];
  /** ISO-8601 time of the most recent dead-lettering. */
  lastDeadLetteredAt?: string;
  /** How many times an operator has already replayed this message. */
  replayCount: number;
}

export interface DlqSnapshot {
  queue: string;
  messageCount: number;
  consumerCount: number;
  /** Up to `limit` messages, head of queue first. */
  messages: DeadLetteredMessage[];
}

export interface ReplaySummary {
  /** Messages this pass set out to replay (queue depth at start, capped by `limit`). */
  requested: number;
  replayed: number;
  /** Queue depth after the pass. Non-zero means something was re-dead-lettered or arrived meanwhile. */
  remaining: number;
  messages: DeadLetteredMessage[];
}

export interface DlqOptions {
  /** Maximum number of messages to touch. Defaults to the whole queue. */
  limit?: number;
}

const DEFAULT_INSPECT_LIMIT = 100;

export const REPLAY_COUNT_HEADER = 'x-replay-count';
export const REPLAYED_AT_HEADER = 'x-replayed-at';

// ─── Mutual exclusion ─────────────────────────────────────────────────────────

let busy = false;

async function exclusive<T>(operation: () => Promise<T>): Promise<T> {
  if (busy) {
    throw new HttpError(409, 'A dead-letter queue operation is already in progress');
  }
  busy = true;
  try {
    return await operation();
  } finally {
    busy = false;
  }
}

// ─── Message helpers ──────────────────────────────────────────────────────────

function parseBody(content: Buffer): unknown {
  const text = content.toString('utf-8');
  try {
    return JSON.parse(text) as unknown;
  } catch {
    return text;
  }
}

function correlationIdOf(body: unknown): string | undefined {
  if (typeof body !== 'object' || body === null) return undefined;
  return asString((body as Record<string, unknown>)['correlationId']);
}

export function replayCountOf(msg: amqplib.Message): number {
  return asNumber(msg.properties.headers?.[REPLAY_COUNT_HEADER]) ?? 0;
}

/** Build the operator-facing view of a parked message. */
/** The routing key the message carried before the broker dead-lettered it, when known. */
export function homeRoutingKeyOf(msg: amqplib.Message): string | undefined {
  // RabbitMQ keeps the most recent death first.
  return msg.properties.headers?.['x-death']?.[0]?.['routing-keys'][0];
}

export function describeDeadLetter(msg: amqplib.Message): DeadLetteredMessage {
  const body = parseBody(msg.content);
  const deaths = msg.properties.headers?.['x-death'] ?? [];
  const [latest] = deaths;

  return {
    correlationId: correlationIdOf(body),
    messageId: asString(msg.properties.messageId),
    body,
    originalRoutingKey: homeRoutingKeyOf(msg) ?? msg.fields.routingKey,
    deathCount: deaths.reduce((sum, d) => sum + d.count, 0),
    lastReason: latest?.reason,
    lastDeadLetteredAt: latest ? new Date(latest.time.value * 1000).toISOString() : undefined,
    replayCount: replayCountOf(msg),
  };
}

/** Carry every original property across the republish, plus replay markers. */
function republishOptions(msg: amqplib.Message, replayedAt: Date): amqplib.Options.Publish {
  return carryProperties(msg, {
    [REPLAY_COUNT_HEADER]: replayCountOf(msg) + 1,
    [REPLAYED_AT_HEADER]: replayedAt.toISOString(),
  });
}

function resolveLimit(limit: number | undefined, available: number, fallback: number): number {
  return Math.min(limit ?? fallback, available);
}

// ─── Public API ───────────────────────────────────────────────────────────────

/**
 * Peek at parked messages without removing them.
 *
 * Messages are fetched unacknowledged on a throw-away channel and then
 * requeued in one go; closing the channel would requeue them anyway.
 */
export function inspectDlq(conn: AmqpConnection, options: DlqOptions = {}): Promise<DlqSnapshot> {
  const { deadLetterQueue } = config.rabbitmq;

  return exclusive(async () => {
    const channel = await openChannel(conn.connection, 'dlq-inspect');
    try {
      const { messageCount, consumerCount } = await channel.checkQueue(deadLetterQueue);
      const toPeek = resolveLimit(options.limit, messageCount, DEFAULT_INSPECT_LIMIT);

      const held: amqplib.GetMessage[] = [];
      for (let i = 0; i < toPeek; i += 1) {
        const msg = await channel.get(deadLetterQueue, { noAck: false });
        if (!msg) break;
        held.push(msg);
      }

      const messages = held.map(describeDeadLetter);
      channel.nackAll(true); // put everything back, untouched

      return { queue: deadLetterQueue, messageCount, consumerCount, messages };
    } finally {
      await safeClose(channel);
    }
  });
}

/**
 * Move parked messages back onto the main queue for reprocessing.
 * See the module header for the guarantees.
 */
export function replayDlq(conn: AmqpConnection, options: DlqOptions = {}): Promise<ReplaySummary> {
  const { deadLetterQueue, exchange, routingKey } = config.rabbitmq;

  return exclusive(async () => {
    const channel = await openConfirmChannel(conn.connection, 'dlq-replay');
    try {
      const { messageCount } = await channel.checkQueue(deadLetterQueue);
      const requested = resolveLimit(options.limit, messageCount, messageCount);
      const replayed: DeadLetteredMessage[] = [];

      console.log(`[DLQ] Replaying up to ${requested} of ${messageCount} parked message(s)…`);

      for (let i = 0; i < requested; i += 1) {
        const msg = await channel.get(deadLetterQueue, { noAck: false });
        if (!msg) break;

        channel.publish(exchange, homeRoutingKeyOf(msg) ?? routingKey, msg.content, republishOptions(msg, new Date()));
        await channel.waitForConfirms(); // broker has it — only now is the DLQ copy disposable
        channel.ack(msg);

        const base = describeDeadLetter(msg);
        const described: DeadLetteredMessage = { ...base, replayCount: base.replayCount + 1 };
        replayed.push(described);
        console.log(
          `[DLQ] Replayed correlationId=${described.correlationId ?? '-'} ` +
            `deaths=${described.deathCount} replay#${described.replayCount}`,
        );
      }

      const { messageCount: remaining } = await channel.checkQueue(deadLetterQueue);
      console.log(`[DLQ] Replay finished: ${replayed.length} replayed, ${remaining} remaining.`);

      return { requested, replayed: replayed.length, remaining, messages: replayed };
    } finally {
      await safeClose(channel);
    }
  });
}
