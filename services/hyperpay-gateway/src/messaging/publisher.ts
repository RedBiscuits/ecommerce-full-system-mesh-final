/**
 * src/messaging/publisher.ts
 *
 * Puts one outbox event on the wire and waits for the broker to confirm it.
 *
 * This is the only place the service publishes domain events, and it is only
 * called by the outbox publisher (outboxPublisher.ts) with a row it has
 * leased. Nothing on the request path publishes — the request path writes
 * the outbox row in its DB transaction and stops there.
 *
 * Wire format:
 *   body        JSON: the row's payload plus the envelope
 *               { eventId, eventType, occurredAt, ...payload }
 *   messageId   eventId          ← de-duplicate on this
 *   type        eventType
 *   correlationId, timestamp (occurredAt), contentType application/json
 *   headers     x-aggregate-type, x-aggregate-id, x-source, x-attempt
 *               plus the service-to-service authentication headers (x-key-id, x-event-stream,
 *               x-event-issuer, x-event-audience, x-signed-at, x-signature): only the services in
 *               the stream's audience hold the key and can accept the event.
 *   persistent  true
 */

import type amqplib from 'amqplib';
import type { PersistedOutboxEvent } from '../services/outbox';
import type { DomainEvent } from '../types/events';
import { EventAuthenticator, SERVICES, eventAuthFor } from '../../../../shared/security';

/** The body exactly as it goes on the wire. */
export function toWireEvent(row: PersistedOutboxEvent): DomainEvent {
  return {
    eventId: row._id,
    eventType: row.eventType,
    occurredAt: row.occurredAt,
    ...row.payload,
  } as DomainEvent;
}

export function publishOptionsFor(row: PersistedOutboxEvent, signature: Record<string, string> = {}): amqplib.Options.Publish {
  return {
    persistent: true, // survives broker restart
    contentType: 'application/json',
    messageId: row._id,
    type: row.eventType,
    correlationId: row.correlationId,
    timestamp: Math.floor(new Date(row.occurredAt).getTime() / 1000),
    headers: {
      'x-aggregate-type': row.aggregateType,
      'x-aggregate-id': row.aggregateId,
      'x-source': row.payload.source,
      'x-attempt': row.attempts, // counted when the row was leased
      ...signature,
    },
  };
}

/**
 * Publish `row` on a confirm channel and resolve once the broker has
 * acknowledged it. Rejects on a nack ("message nacked"), on a channel that
 * closed before confirming ("channel closed"), or when `timeoutMs` elapses.
 */
export async function publishOutboxEvent(
  channel: amqplib.ConfirmChannel,
  row: PersistedOutboxEvent,
  timeoutMs: number,
  auth: EventAuthenticator = eventAuthFor(SERVICES.hyperpay),
): Promise<void> {
  const buffer = Buffer.from(JSON.stringify(toWireEvent(row)));
  const signature = auth.signHeadersIfIssued({ eventType: row.eventType, eventId: row._id, body: buffer });

  const ok = channel.publish(row.exchange, row.routingKey, buffer, publishOptionsFor(row, signature));
  if (!ok) console.warn('[Publisher] Channel write buffer full — back-pressure active.');

  await withTimeout(channel.waitForConfirms(), timeoutMs, `publisher confirm for event ${row._id}`);

  console.log(
    `[Publisher] Published ${row.eventType} eventId=${row._id} routingKey=${row.routingKey} ` +
      `correlationId=${row.correlationId} attempt=${row.attempts}`,
  );
}

function withTimeout<T>(promise: Promise<T>, ms: number, what: string): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(`Timed out after ${ms}ms waiting for ${what}`)), ms);
    timer.unref();
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}
