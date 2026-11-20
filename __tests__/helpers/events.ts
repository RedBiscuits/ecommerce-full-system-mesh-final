import type { Channel } from 'amqplib';
import {
  SERVICES,
  eventAuthFor,
  signedPublishProperties,
  streamForEventType,
  type ServiceId,
} from '../../shared/security';

export interface RawEvent {
  eventId?: string;
  eventType?: string;
  /** Anything else the event carries (bodies are serialised as-is). */
  [key: string]: unknown;
}

/**
 * Put an event on the broker exactly as its issuing service would: signed with that stream's
 * shared key (x-key-id / x-event-stream / x-event-issuer / x-event-audience / x-signature).
 * Use it to simulate broker redeliveries and re-announcements in tests.
 */
export function publishSigned(
  ch: Channel,
  exchange: string,
  routingKey: string,
  body: RawEvent,
  options: { messageId?: string; issuer?: ServiceId; content?: Buffer; eventType?: string } = {}
): void {
  const eventType = options.eventType ?? (body.eventType as string);
  const issuer = options.issuer ?? (streamForEventType(eventType)?.issuer as ServiceId | undefined);
  if (!issuer) throw new Error(`No stream (and so no issuer) declares event type ${eventType}`);
  const content = options.content ?? Buffer.from(JSON.stringify(body));
  const messageId = options.messageId ?? body.eventId ?? '';
  ch.publish(exchange, routingKey, content, {
    ...signedPublishProperties(eventAuthFor(issuer), { eventType, eventId: messageId, body: content }),
    messageId,
  });
}

/** Put an event on the broker WITHOUT authentication headers (a foreign / rogue producer). */
export function publishUnsigned(
  ch: Channel,
  exchange: string,
  routingKey: string,
  body: RawEvent | null,
  options: { messageId?: string; content?: Buffer } = {}
): void {
  const content = options.content ?? Buffer.from(JSON.stringify(body));
  ch.publish(exchange, routingKey, content, {
    persistent: true,
    contentType: 'application/json',
    type: body?.eventType,
    messageId: options.messageId ?? body?.eventId,
  });
}

export { SERVICES };
