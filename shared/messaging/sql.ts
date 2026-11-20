import type { OutboxRecord, OutboxStats, OutboxStore } from './OutboxRelay';
import type { IntegrationEvent } from '../contracts/envelope';

/**
 * Anything that runs parameterised SQL and returns rows: a `pg` Pool /
 * PoolClient, an embedded PGlite instance or a PGlite transaction. Services
 * pass the handle of the transaction they are in, so outbox rows and
 * checkpoints commit (or roll back) atomically with the business change.
 */
export interface SqlExecutor {
  query(text: string, params?: any[]): Promise<{ rows: any[] }>;
}

/**
 * Integration tables every SQL-backed service installs in its own database.
 *
 *  integration_outbox       transactional outbox: events written in the same
 *                           transaction as the state change they describe,
 *                           published afterwards by the OutboxRelay.
 *  integration_checkpoints  per-consumer, per-event progress. A consumer
 *                           claims (consumer, event, step) in the transaction
 *                           that performs the step, so a step is done exactly
 *                           once no matter how often the event is delivered;
 *                           multi-step consumers resume after the last
 *                           completed step. The final step is 'done'.
 */
export const INTEGRATION_TABLES_DROP_SQL = `
DROP TABLE IF EXISTS integration_checkpoints CASCADE;
DROP TABLE IF EXISTS integration_outbox CASCADE;
`;

export const INTEGRATION_TABLES_SQL = `
CREATE TABLE IF NOT EXISTS integration_outbox (
    seq BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(100) NOT NULL UNIQUE,
    event_type VARCHAR(120) NOT NULL,
    exchange VARCHAR(160) NOT NULL,
    routing_key VARCHAR(200) NOT NULL,
    aggregate_id VARCHAR(160) NOT NULL,
    correlation_id VARCHAR(160),
    body JSONB NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'PUBLISHED', 'FAILED')),
    attempts INT NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    locked_by VARCHAR(200),
    locked_until TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_integration_outbox_claim ON integration_outbox (status, next_attempt_at, seq);

CREATE TABLE IF NOT EXISTS integration_checkpoints (
    consumer VARCHAR(160) NOT NULL,
    event_id VARCHAR(100) NOT NULL,
    step VARCHAR(60) NOT NULL,
    correlation_id VARCHAR(160),
    outcome VARCHAR(60) NOT NULL DEFAULT 'IN_PROGRESS',
    detail JSONB NOT NULL DEFAULT '{}'::jsonb,
    completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (consumer, event_id, step)
);
CREATE INDEX IF NOT EXISTS idx_integration_checkpoints_correlation ON integration_checkpoints (correlation_id);
`;

export const INTEGRATION_TABLES = ['integration_checkpoints', 'integration_outbox'] as const;

// ─── Outbox: write side ────────────────────────────────────────────────────────

export interface EnqueueIntegrationEvent {
  exchange: string;
  routingKey: string;
  event: IntegrationEvent;
}

/**
 * Record an event in the caller's transaction. The event id is unique: a
 * second insert of the same (deterministic) id is a no-op, so a fact can never
 * be announced twice. Resolves `true` when the row was inserted.
 */
export async function enqueueIntegrationEvent(exec: SqlExecutor, input: EnqueueIntegrationEvent): Promise<boolean> {
  const { event } = input;
  const res = await exec.query(
    `INSERT INTO integration_outbox (event_id, event_type, exchange, routing_key, aggregate_id, correlation_id, body)
     VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
     ON CONFLICT (event_id) DO NOTHING
     RETURNING event_id`,
    [
      event.eventId,
      event.eventType,
      input.exchange,
      input.routingKey,
      event.aggregateId,
      event.correlationId,
      JSON.stringify(event),
    ]
  );
  return res.rows.length > 0;
}

// ─── Outbox: relay side ────────────────────────────────────────────────────────

interface OutboxRow {
  seq: string | number;
  event_id: string;
  event_type: string;
  exchange: string;
  routing_key: string;
  aggregate_id: string;
  correlation_id: string | null;
  body: unknown;
  attempts: number | string;
}

/**
 * OutboxStore over the `integration_outbox` table. `claim` is the classic
 * lease: UPDATE ... WHERE event_id IN (SELECT ... FOR UPDATE SKIP LOCKED), so
 * concurrent relays never take the same row and a dead relay's lease expires.
 */
export class SqlOutboxStore implements OutboxStore {
  constructor(private readonly exec: SqlExecutor) {}

  async claim({ relayId, limit, leaseMs }: { relayId: string; limit: number; leaseMs: number }): Promise<OutboxRecord[]> {
    const res = await this.exec.query(
      `UPDATE integration_outbox
       SET locked_by = $1,
           locked_until = NOW() + ($2::int * INTERVAL '1 millisecond'),
           attempts = attempts + 1
       WHERE event_id IN (
         SELECT event_id FROM integration_outbox
         WHERE status = 'PENDING'
           AND next_attempt_at <= NOW()
           AND (locked_until IS NULL OR locked_until < NOW())
         ORDER BY seq
         LIMIT $3
         FOR UPDATE SKIP LOCKED
       )
       RETURNING seq, event_id, event_type, exchange, routing_key, aggregate_id, correlation_id, body, attempts`,
      [relayId, leaseMs, limit]
    );
    return (res.rows as OutboxRow[])
      .sort((a, b) => Number(a.seq) - Number(b.seq))
      .map((row) => ({
        eventId: row.event_id,
        eventType: row.event_type,
        exchange: row.exchange,
        routingKey: row.routing_key,
        aggregateId: row.aggregate_id,
        correlationId: row.correlation_id,
        body: typeof row.body === 'string' ? JSON.parse(row.body) : row.body,
        attempts: Number(row.attempts),
      }));
  }

  async markPublished(eventId: string, relayId: string): Promise<void> {
    await this.exec.query(
      `UPDATE integration_outbox
       SET status = 'PUBLISHED', published_at = NOW(), locked_by = NULL, locked_until = NULL, last_error = NULL
       WHERE event_id = $1 AND status = 'PENDING' AND (locked_by = $2 OR locked_by IS NULL)`,
      [eventId, relayId]
    );
  }

  async markFailed(eventId: string, relayId: string, error: string, retryAt: Date | null): Promise<void> {
    await this.exec.query(
      `UPDATE integration_outbox
       SET status = CASE WHEN $3::timestamptz IS NULL THEN 'FAILED' ELSE 'PENDING' END,
           next_attempt_at = COALESCE($3::timestamptz, next_attempt_at),
           last_error = $4, locked_by = NULL, locked_until = NULL
       WHERE event_id = $1 AND status = 'PENDING' AND locked_by = $2`,
      [eventId, relayId, retryAt ? retryAt.toISOString() : null, error]
    );
  }

  async release(eventIds: string[], relayId: string): Promise<void> {
    if (eventIds.length === 0) return;
    await this.exec.query(
      `UPDATE integration_outbox
       SET locked_by = NULL, locked_until = NULL, attempts = GREATEST(attempts - 1, 0)
       WHERE event_id = ANY($1::varchar[]) AND locked_by = $2 AND status = 'PENDING'`,
      [eventIds, relayId]
    );
  }

  async retryFailed(eventIds?: string[]): Promise<number> {
    const res = await this.exec.query(
      `UPDATE integration_outbox
       SET status = 'PENDING', attempts = 0, next_attempt_at = NOW(), locked_by = NULL, locked_until = NULL
       WHERE status = 'FAILED' AND ($1::varchar[] IS NULL OR event_id = ANY($1::varchar[]))
       RETURNING event_id`,
      [eventIds ?? null]
    );
    return res.rows.length;
  }

  async stats(): Promise<OutboxStats> {
    const res = await this.exec.query(
      `SELECT
         COUNT(*) FILTER (WHERE status = 'PENDING')::int AS pending,
         COUNT(*) FILTER (WHERE status = 'PUBLISHED')::int AS published,
         COUNT(*) FILTER (WHERE status = 'FAILED')::int AS failed,
         (EXTRACT(EPOCH FROM (NOW() - MIN(created_at) FILTER (WHERE status = 'PENDING'))) * 1000)::bigint AS oldest
       FROM integration_outbox`
    );
    const row = res.rows[0] ?? {};
    return {
      pending: Number(row.pending ?? 0),
      published: Number(row.published ?? 0),
      failed: Number(row.failed ?? 0),
      oldestPendingAgeMs: row.oldest === null || row.oldest === undefined ? null : Number(row.oldest),
    };
  }
}

// ─── Checkpoints ───────────────────────────────────────────────────────────────

export interface CheckpointKey {
  consumer: string;
  eventId: string;
  step: string;
}

export interface Checkpoint extends CheckpointKey {
  correlationId: string | null;
  outcome: string;
  detail: Record<string, unknown>;
  completedAt: string;
}

/**
 * Claim a step inside the caller's transaction. Resolves `false` when the
 * step was already completed (by an earlier delivery of the same event): the
 * caller must then skip the step. On real PostgreSQL a concurrent duplicate
 * delivery blocks on the primary key until the first transaction commits
 * (→ false) or rolls back (→ it claims the step itself).
 */
export async function claimCheckpoint(
  exec: SqlExecutor,
  key: CheckpointKey & { correlationId?: string | null }
): Promise<boolean> {
  const res = await exec.query(
    `INSERT INTO integration_checkpoints (consumer, event_id, step, correlation_id)
     VALUES ($1, $2, $3, $4)
     ON CONFLICT (consumer, event_id, step) DO NOTHING
     RETURNING event_id`,
    [key.consumer, key.eventId, key.step, key.correlationId ?? null]
  );
  return res.rows.length > 0;
}

/** Record the outcome of a claimed step (same transaction as the claim). */
export async function completeCheckpoint(
  exec: SqlExecutor,
  key: CheckpointKey & { outcome: string; detail?: Record<string, unknown> }
): Promise<void> {
  await exec.query(
    `UPDATE integration_checkpoints
     SET outcome = $4, detail = $5::jsonb, completed_at = NOW()
     WHERE consumer = $1 AND event_id = $2 AND step = $3`,
    [key.consumer, key.eventId, key.step, key.outcome, JSON.stringify(key.detail ?? {})]
  );
}

export async function readCheckpoint(exec: SqlExecutor, key: CheckpointKey): Promise<Checkpoint | null> {
  const res = await exec.query(
    `SELECT consumer, event_id, step, correlation_id, outcome, detail, completed_at
     FROM integration_checkpoints WHERE consumer = $1 AND event_id = $2 AND step = $3`,
    [key.consumer, key.eventId, key.step]
  );
  return res.rows.length > 0 ? toCheckpoint(res.rows[0]) : null;
}

export async function listCheckpoints(
  exec: SqlExecutor,
  filter: { correlationId?: string; eventId?: string; consumer?: string } = {}
): Promise<Checkpoint[]> {
  const res = await exec.query(
    `SELECT consumer, event_id, step, correlation_id, outcome, detail, completed_at
     FROM integration_checkpoints
     WHERE ($1::text IS NULL OR correlation_id = $1)
       AND ($2::text IS NULL OR event_id = $2)
       AND ($3::text IS NULL OR consumer = $3)
     ORDER BY completed_at, consumer, step`,
    [filter.correlationId ?? null, filter.eventId ?? null, filter.consumer ?? null]
  );
  return res.rows.map(toCheckpoint);
}

function toCheckpoint(row: any): Checkpoint {
  return {
    consumer: row.consumer,
    eventId: row.event_id,
    step: row.step,
    correlationId: row.correlation_id ?? null,
    outcome: row.outcome,
    detail: (typeof row.detail === 'string' ? JSON.parse(row.detail) : row.detail) ?? {},
    completedAt: new Date(row.completed_at).toISOString(),
  };
}
