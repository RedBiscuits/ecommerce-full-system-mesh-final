/**
 * src/messaging/amqpUtils.ts
 *
 * Small, shared AMQP helpers.
 *
 * Important: amqplib emits `'error'` on a channel whenever the *server* closes
 * it (406 inequivalent arguments, 404 unknown queue, …). An EventEmitter with
 * no `'error'` listener throws — which would crash the process. Every channel
 * this service opens therefore goes through `openChannel` / `openConfirmChannel`,
 * which attach a listener before the channel is used.
 */

import type amqplib from 'amqplib';

// ─── Channels ─────────────────────────────────────────────────────────────────

function attachErrorListener(channel: amqplib.Channel, label: string): void {
  channel.on('error', (err: Error) => {
    console.error(`[AMQP] ${label} channel error: ${err.message}`);
  });
}

/** Open a plain channel with an error listener already attached. */
export async function openChannel(
  connection: amqplib.ChannelModel,
  label = 'worker',
): Promise<amqplib.Channel> {
  const channel = await connection.createChannel();
  attachErrorListener(channel, label);
  return channel;
}

/** Open a publisher-confirms channel with an error listener already attached. */
export async function openConfirmChannel(
  connection: amqplib.ChannelModel,
  label = 'publisher',
): Promise<amqplib.ConfirmChannel> {
  const channel = await connection.createConfirmChannel();
  attachErrorListener(channel, label);
  return channel;
}

/** Close a channel or connection, ignoring "already closed" style errors. */
export async function safeClose(closable: { close(): Promise<void> }): Promise<void> {
  try {
    await closable.close();
  } catch {
    // Already closed by the peer — nothing to do.
  }
}

// ─── Errors ───────────────────────────────────────────────────────────────────

/** Error shape amqplib attaches to server-initiated channel closes. */
export interface AmqpChannelError extends Error {
  code?: number;
}

export function amqpErrorCode(err: unknown): number | undefined {
  return err instanceof Error ? (err as AmqpChannelError).code : undefined;
}

export function isPreconditionFailed(err: unknown): boolean {
  return amqpErrorCode(err) === 406 || (err instanceof Error && /PRECONDITION[_-]FAILED/i.test(err.message));
}

export function isNotFound(err: unknown): boolean {
  return amqpErrorCode(err) === 404 || (err instanceof Error && /NOT[_-]FOUND/i.test(err.message));
}

// ─── Message properties ───────────────────────────────────────────────────────

/** amqplib types every message property as `any`; narrow at the boundary. */
export function asString(value: unknown): string | undefined {
  return typeof value === 'string' ? value : undefined;
}

export function asNumber(value: unknown): number | undefined {
  return typeof value === 'number' && Number.isFinite(value) ? value : undefined;
}

/**
 * Build publish options that carry every property of an existing message
 * across a republish (headers included), optionally adding/overriding headers.
 */
export function carryProperties(
  msg: amqplib.Message,
  extraHeaders: amqplib.MessagePropertyHeaders = {},
): amqplib.Options.Publish {
  const p = msg.properties;
  return {
    persistent: true,
    contentType: asString(p.contentType),
    contentEncoding: asString(p.contentEncoding),
    priority: asNumber(p.priority),
    correlationId: asString(p.correlationId),
    replyTo: asString(p.replyTo),
    expiration: asString(p.expiration),
    messageId: asString(p.messageId),
    timestamp: asNumber(p.timestamp),
    type: asString(p.type),
    userId: asString(p.userId),
    appId: asString(p.appId),
    headers: { ...p.headers, ...extraHeaders },
  };
}

/**
 * Move up to `max` messages from one queue to another via the default
 * exchange, one at a time with publisher confirms: a message is ACKed on the
 * source only after the broker has confirmed it on the destination.
 * Returns the number of messages moved.
 */
export async function moveMessages(
  channel: amqplib.ConfirmChannel,
  from: string,
  to: string,
  max: number,
): Promise<number> {
  let moved = 0;
  for (; moved < max; moved += 1) {
    const msg = await channel.get(from, { noAck: false });
    if (!msg) break;
    channel.publish('', to, msg.content, carryProperties(msg));
    await channel.waitForConfirms();
    channel.ack(msg);
  }
  return moved;
}
