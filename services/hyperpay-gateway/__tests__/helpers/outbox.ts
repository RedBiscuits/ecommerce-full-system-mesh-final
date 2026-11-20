/* eslint-disable @typescript-eslint/no-unused-vars */
import { describe, it, test, expect, beforeEach, afterEach, beforeAll, afterAll, vi } from 'vitest';
/* eslint-disable @typescript-eslint/no-unused-vars */
/**
 * tests/helpers/outbox.ts
 *
 * Outbox helpers for integration tests: inspect what the request paths
 * recorded, and run the publisher over a given (fake) connection.
 */

import { config } from '../../src/config/env';
import { createOutboxPublisher } from '../../src/messaging/outboxPublisher';
import { toWireEvent } from '../../src/messaging/publisher';
import { outbox } from '../../src/services/outbox';
import type { AmqpConnection } from '../../src/messaging/amqpClient';
import type { OutboxCycle, OutboxPublisherOptions } from '../../src/messaging/outboxPublisher';
import type { OutboxStatus, PersistedOutboxEvent } from '../../src/services/outbox';
import type { DomainEvent, DomainEventType } from '../../src/types/events';

/** Outbox rows, oldest first, optionally filtered by event type / status. */
export async function outboxRows(filter: { eventType?: DomainEventType; status?: OutboxStatus } = {}): Promise<PersistedOutboxEvent[]> {
  const rows = await outbox.list({ status: filter.status });
  return filter.eventType ? rows.filter((r) => r.eventType === filter.eventType) : rows;
}

/** The wire bodies of the recorded events (what a consumer will receive). */
export async function outboxEvents(filter: { eventType?: DomainEventType; status?: OutboxStatus } = {}): Promise<DomainEvent[]> {
  return (await outboxRows(filter)).map(toWireEvent);
}

/** Publisher options suitable for tests: no timer, short timeouts. */
export function testPublisherOptions(overrides: Partial<OutboxPublisherOptions> = {}): OutboxPublisherOptions {
  return {
    ...config.outbox,
    pollIntervalMs: 0,
    publishTimeoutMs: 500,
    retryDelayMs: 10,
    maxRetryDelayMs: 50,
    getConnection: () => null,
    ...overrides,
  };
}

/** Run one publisher cycle over `conn` (a fresh publisher each time — no timer, no channel left open). */
export async function drainOutbox(conn: AmqpConnection, overrides: Partial<OutboxPublisherOptions> = {}): Promise<OutboxCycle> {
  const publisher = createOutboxPublisher(testPublisherOptions({ getConnection: () => conn, ...overrides }));
  try {
    return await publisher.runNow();
  } finally {
    await publisher.stop();
  }
}
