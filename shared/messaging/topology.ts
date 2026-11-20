import type { Channel, Options } from 'amqplib';

/**
 * Broker topology of one integration consumer. Every consumer owns its queues,
 * so a message that fails for one consumer is retried, dead-lettered and
 * replayed for THAT consumer only; consumers that already processed it never
 * see it again.
 *
 *   <producer exchange> ──binding──► <name>                    work queue
 *                                     │ handler throws, attempt n < max
 *                                     ▼
 *                                   <name>.retry.<n>           per-message TTL = jittered backoff
 *                                     │ TTL expires (broker dead-letters)
 *   platform.requeue (direct) ◄───────┘ routing key <name> ──► <name>      (only this consumer)
 *
 *   non-retryable / malformed / attempts exhausted:
 *   platform.dlx (direct) ──routing key <name>──► <name>.dlq   parked for an operator
 *
 * One retry tier per attempt keeps RabbitMQ's head-of-queue expiry honest:
 * per-message TTLs inside a tier differ only by jitter, so a message never
 * waits behind one with a much longer delay.
 */
export const PLATFORM_EXCHANGES = {
  requeue: 'platform.requeue',
  deadLetter: 'platform.dlx',
} as const;

export type ExchangeType = 'direct' | 'topic' | 'fanout';

export interface ConsumerBinding {
  exchange: string;
  exchangeType: ExchangeType;
  routingKey: string;
}

export interface ConsumerQueues {
  workQueue: string;
  /** retryQueues[i] receives messages whose attempt i + 1 failed. */
  retryQueues: string[];
  deadLetterQueue: string;
}

export function consumerQueues(name: string, maxAttempts: number): ConsumerQueues {
  const retryQueues: string[] = [];
  for (let attempt = 1; attempt < maxAttempts; attempt++) retryQueues.push(`${name}.retry.${attempt}`);
  return { workQueue: name, retryQueues, deadLetterQueue: `${name}.dlq` };
}

/**
 * Declare (idempotently) everything the consumer needs, including the
 * producer exchanges it binds to, so consumers and producers can start in any
 * order. Uses amqplib's named queue options (`deadLetterExchange`, ...), which
 * amqplib turns into `x-*` arguments for RabbitMQ and the in-process fake
 * broker understands natively.
 */
export async function declareConsumerTopology(
  ch: Channel,
  name: string,
  bindings: readonly ConsumerBinding[],
  maxAttempts: number
): Promise<ConsumerQueues> {
  const queues = consumerQueues(name, maxAttempts);

  for (const binding of bindings) {
    await ch.assertExchange(binding.exchange, binding.exchangeType, { durable: true });
  }
  await ch.assertExchange(PLATFORM_EXCHANGES.requeue, 'direct', { durable: true });
  await ch.assertExchange(PLATFORM_EXCHANGES.deadLetter, 'direct', { durable: true });

  // Work queue. Its own DLX is the safety net for a native nack (requeue=false)
  // when the broker refuses the explicit retry / dead-letter copy.
  await ch.assertQueue(queues.workQueue, {
    durable: true,
    deadLetterExchange: PLATFORM_EXCHANGES.deadLetter,
    deadLetterRoutingKey: name,
  } as Options.AssertQueue);
  for (const binding of bindings) {
    await ch.bindQueue(queues.workQueue, binding.exchange, binding.routingKey);
  }
  await ch.bindQueue(queues.workQueue, PLATFORM_EXCHANGES.requeue, name);

  for (const retryQueue of queues.retryQueues) {
    await ch.assertQueue(retryQueue, {
      durable: true,
      deadLetterExchange: PLATFORM_EXCHANGES.requeue,
      deadLetterRoutingKey: name,
    } as Options.AssertQueue);
  }

  await ch.assertQueue(queues.deadLetterQueue, { durable: true });
  await ch.bindQueue(queues.deadLetterQueue, PLATFORM_EXCHANGES.deadLetter, name);

  return queues;
}

export const DELIVERY_HEADERS = {
  attempt: 'x-attempt',
  consumer: 'x-consumer',
  failureReason: 'x-failure-reason',
  firstFailedAt: 'x-first-failed-at',
  failedAt: 'x-failed-at',
  retryDelayMs: 'x-retry-delay-ms',
  deadLetteredAt: 'x-dead-lettered-at',
  replayCount: 'x-replay-count',
  replayedAt: 'x-replayed-at',
} as const;
