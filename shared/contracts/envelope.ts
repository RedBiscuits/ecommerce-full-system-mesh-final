import crypto from 'crypto';
import { z } from 'zod';

/**
 * The envelope every platform integration event travels in (billing, ledger
 * and ZATCA events). HyperPay keeps its own, older wire format (see
 * `hyperpay.ts`) so existing `hyperpay.results` consumers are unaffected.
 *
 * - `eventId`       unique per logical event and the AMQP `messageId`;
 *                   consumers de-duplicate on it. Downstream events use
 *                   DETERMINISTIC ids derived from the business key they
 *                   describe, so even a re-emission of the same fact carries
 *                   the same id and is de-duplicated by every consumer.
 * - `correlationId` the order id: it ties every event of one checkout
 *                   together across all services (tracing, checkpoints).
 * - `causationId`   the eventId of the message that caused this one.
 */
export interface IntegrationEvent<TType extends string = string, TPayload = unknown> {
  eventId: string;
  eventType: TType;
  eventVersion: number;
  occurredAt: string;
  producer: string;
  correlationId: string;
  causationId: string | null;
  aggregateId: string;
  payload: TPayload;
}

export function integrationEventSchema<TType extends string, TPayload extends z.ZodTypeAny>(
  eventType: TType,
  payload: TPayload
) {
  return z.object({
    eventId: z.string().min(1).max(100),
    eventType: z.literal(eventType),
    eventVersion: z.number().int().positive(),
    occurredAt: z.string().datetime({ offset: true }),
    producer: z.string().min(1),
    correlationId: z.string().min(1).max(160),
    causationId: z.string().min(1).max(100).nullable(),
    aggregateId: z.string().min(1).max(160),
    payload,
  });
}

/** Stable event id for "this fact about this business key" (idempotent re-emission). */
export function deterministicEventId(...parts: string[]): string {
  const digest = crypto.createHash('sha256').update(parts.join('\u001f'), 'utf8').digest('hex');
  return `evt_${digest.slice(0, 40)}`;
}

export function buildIntegrationEvent<TType extends string, TPayload>(input: {
  eventId: string;
  eventType: TType;
  producer: string;
  correlationId: string;
  causationId: string | null;
  aggregateId: string;
  payload: TPayload;
  occurredAt?: Date;
  eventVersion?: number;
}): IntegrationEvent<TType, TPayload> {
  return {
    eventId: input.eventId,
    eventType: input.eventType,
    eventVersion: input.eventVersion ?? 1,
    occurredAt: (input.occurredAt ?? new Date()).toISOString(),
    producer: input.producer,
    correlationId: input.correlationId,
    causationId: input.causationId,
    aggregateId: input.aggregateId,
    payload: input.payload,
  };
}
