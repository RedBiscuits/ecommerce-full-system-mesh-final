/**
 * src/config/env.ts
 *
 * Centralised, validated environment configuration.
 * Fails fast at startup if any required variable is missing.
 */

import dotenv from 'dotenv';

dotenv.config();

export function _requireEnv(key: string): string {
  const value = process.env[key];
  if (!value || value.trim() === '') {
    throw new Error(
      `[Config] Missing required environment variable: "${key}". ` +
        'Copy .env.example to .env and fill in the values.',
    );
  }
  return value.trim();
}

function optionalEnv(key: string, defaultValue: string): string {
  return (process.env[key] ?? defaultValue).trim();
}

/** Like optionalEnv but yields `null` when the variable is unset or blank. */
function nullableEnv(key: string): string | null {
  const value = process.env[key]?.trim();
  return value ? value : null;
}

function intEnv(key: string, defaultValue: number): number {
  const raw = optionalEnv(key, String(defaultValue));
  const value = Number(raw);
  if (!Number.isInteger(value) || value < 0) {
    throw new Error(`[Config] "${key}" must be a non-negative integer, got "${raw}".`);
  }
  return value;
}

function positiveIntEnv(key: string, defaultValue: number): number {
  const value = intEnv(key, defaultValue);
  if (value < 1) throw new Error(`[Config] "${key}" must be a positive integer, got "${value}".`);
  return value;
}

function listEnv(key: string): string[] {
  return optionalEnv(key, '')
    .split(',')
    .map((v) => v.trim())
    .filter((v) => v !== '');
}

function floatEnv(key: string, defaultValue: number): number {
  const raw = optionalEnv(key, String(defaultValue));
  const value = Number(raw);
  if (!Number.isFinite(value) || value < 0) {
    throw new Error(`[Config] "${key}" must be a non-negative number, got "${raw}".`);
  }
  return value;
}

function enumEnv<T extends string>(key: string, allowed: readonly T[], defaultValue: T): T {
  const raw = optionalEnv(key, defaultValue);
  if (!(allowed as readonly string[]).includes(raw)) {
    throw new Error(`[Config] "${key}" must be one of ${allowed.join(', ')}; got "${raw}".`);
  }
  return raw as T;
}

/**
 * What to do when a queue already exists on the broker with different
 * arguments (RabbitMQ answers 406 PRECONDITION_FAILED):
 *   - recreate: park its messages, delete it, re-declare it, restore the messages.
 *   - fail:     refuse to start and print the manual fix.
 */
export const TOPOLOGY_MIGRATION_MODES = ['recreate', 'fail'] as const;
export type TopologyMigrationMode = (typeof TOPOLOGY_MIGRATION_MODES)[number];

const nodeEnv = optionalEnv('NODE_ENV', 'development');
const webhookSigningKey = nullableEnv('WEBHOOK_SECRET');
const vaultMasterKey = nullableEnv('VAULT_MASTER_KEY');

if (nodeEnv === 'production' && !webhookSigningKey) {
  throw new Error(
    '[Config] WEBHOOK_SECRET is required in production — the webhook must not be unauthenticated.',
  );
}

if (nodeEnv === 'production' && !vaultMasterKey) {
  throw new Error(
    '[Config] VAULT_MASTER_KEY is required in production — registration tokens would otherwise be discarded.',
  );
}

export const config = {
  port: parseInt(optionalEnv('PORT', '3000'), 10),
  nodeEnv,

  hyperpay: {
    host: optionalEnv('HYPERPAY_HOST', 'eu-test.oppwa.com'),
    userId: optionalEnv('HYPERPAY_USER_ID', 'oppwa_user_dev'),
    password: optionalEnv('HYPERPAY_CREDENTIAL', 'oppwa_cred_dev'),
    entityId: optionalEnv('HYPERPAY_ENTITY_ID', 'oppwa_entity_dev'),
  },

  rabbitmq: {
    url: optionalEnv('RABBITMQ_URL', 'amqp://localhost:5672'),
    exchange: optionalEnv('RABBITMQ_EXCHANGE', 'hyperpay'),
    queue: optionalEnv('RABBITMQ_QUEUE', 'hyperpay.payments'),
    routingKey: optionalEnv('RABBITMQ_ROUTING_KEY', 'payment.verify'),
    resultsQueue: optionalEnv('RABBITMQ_RESULTS_QUEUE', 'hyperpay.results'),
    /** Routing key result events are published with; `resultsQueue` is bound to it. */
    resultsRoutingKey: optionalEnv('RABBITMQ_RESULTS_ROUTING_KEY', 'payment.result'),

    /**
     * Domain-event stream. `eventsQueue` is bound to every domain event
     * routing key (PAYMENT_COMPLETED → `resultsRoutingKey`, TOKEN_REGISTERED →
     * `tokenRegisteredRoutingKey`, REFUND_PROCESSED → `refundProcessedRoutingKey`),
     * so one consumer can follow everything; `resultsQueue` keeps receiving
     * payment results only, unchanged.
     */
    eventsQueue: optionalEnv('RABBITMQ_EVENTS_QUEUE', 'hyperpay.events'),
    tokenRegisteredRoutingKey: optionalEnv('RABBITMQ_TOKEN_REGISTERED_ROUTING_KEY', 'token.registered'),
    refundProcessedRoutingKey: optionalEnv('RABBITMQ_REFUND_PROCESSED_ROUTING_KEY', 'refund.processed'),

    /**
     * Recurring-charge batches. Items are published to `chargesQueue` (bound
     * with `chargeRoutingKey`, dead-lettered like the main queue) and consumed
     * by the charge worker pool (workers/chargeWorkerPool.ts).
     */
    chargesQueue: optionalEnv('RABBITMQ_CHARGES_QUEUE', 'hyperpay.charges'),
    chargeRoutingKey: optionalEnv('RABBITMQ_CHARGE_ROUTING_KEY', 'charge.execute'),

    /** Base reconnect delay; doubles on every consecutive failure up to `maxRetryDelayMs`. */
    retryDelayMs: intEnv('RABBITMQ_RETRY_DELAY_MS', 5000),
    maxRetryDelayMs: intEnv('RABBITMQ_MAX_RETRY_DELAY_MS', 30_000),

    /**
     * Dead-lettering. Messages NACKed (requeue=false) from `queue` are routed
     * by the broker to `deadLetterExchange` with `deadLetterRoutingKey`, which
     * is bound to `deadLetterQueue` — where they park until replayed.
     */
    deadLetterExchange: optionalEnv('RABBITMQ_DLX', 'hyperpay.dlx'),
    deadLetterQueue: optionalEnv('RABBITMQ_DLQ', 'hyperpay.dlq'),
    deadLetterRoutingKey: optionalEnv('RABBITMQ_DL_ROUTING_KEY', 'payment.dead'),

    topologyMigration: enumEnv('RABBITMQ_TOPOLOGY_MIGRATION', TOPOLOGY_MIGRATION_MODES, 'recreate'),
  },

  webhook: {
    /**
     * HMAC-SHA256 secret shared with the webhook sender. `null` disables
     * signature verification — allowed outside production only.
     */
    secret: webhookSigningKey,
    /** Max age (seconds) of a signature timestamp before it is rejected as a replay. */
    signatureToleranceSec: intEnv('WEBHOOK_SIGNATURE_TOLERANCE_SEC', 300),
  },

  vault: {
    /**
     * 32-byte AES-256-GCM key (base64 or hex) used to encrypt new vault
     * records. `null` disables the vault: tokenisation data is *discarded*
     * (never stored in clear). Required in production.
     */
    masterKey: vaultMasterKey,
    /** Identifier stored alongside each ciphertext so the right key is used to decrypt. */
    masterKeyId: optionalEnv('VAULT_MASTER_KEY_ID', 'v1'),
    /**
     * Previous keys, decrypt-only, as `id=key,id=key`. Lets records encrypted
     * under an old key be read (and re-encrypted via rotation) after the
     * master key changes.
     */
    retiredKeys: optionalEnv('VAULT_RETIRED_KEYS', ''),
  },

  commerce: {
    /**
     * Merchant (ZATCA taxpayer / ledger revenue account) a checkout belongs to
     * when the payment's `customParameters.merchantId` does not name one.
     */
    defaultMerchantId: optionalEnv('COMMERCE_DEFAULT_MERCHANT_ID', 'txp_riyadh_retail'),
  },

  reconciliation: {
    /**
     * How often the background reconciler re-checks non-terminal transactions
     * against the gateway. 0 disables the background job (the admin command
     * still works).
     */
    intervalMs: intEnv('RECONCILE_INTERVAL_MS', 300_000),
    /** Max records a single run will verify — keeps a run bounded. */
    batchLimit: intEnv('RECONCILE_BATCH_LIMIT', 200),
  },

  redis: {
    /**
     * Redis nodes backing the distributed lock manager (locking/redisLockManager.ts).
     * One URL (`REDIS_URL`) is a single-node deployment; several
     * (`REDIS_NODES=url,url,url`, independent masters) give Redlock quorum
     * semantics. Empty: locking is disabled and the charge worker does not run
     * (fail closed — no exclusive lock, no charge).
     */
    nodes: listEnv('REDIS_NODES').length > 0 ? listEnv('REDIS_NODES') : listEnv('REDIS_URL'),
    connectTimeoutMs: positiveIntEnv('REDIS_CONNECT_TIMEOUT_MS', 5000),
    /** A command that takes longer than this is a failure — locks must answer fast or not at all. */
    commandTimeoutMs: positiveIntEnv('REDIS_COMMAND_TIMEOUT_MS', 2000),
    /** Reconnect backoff: starts here, doubles, capped. Retries forever. */
    retryDelayMs: positiveIntEnv('REDIS_RETRY_DELAY_MS', 500),
    maxRetryDelayMs: positiveIntEnv('REDIS_MAX_RETRY_DELAY_MS', 10_000),
  },

  locks: {
    /** How long a lock lives without its watchdog; must comfortably exceed one gateway round-trip. */
    ttlMs: positiveIntEnv('LOCK_TTL_MS', 10_000),
    /** Redlock clock-drift allowance as a fraction of the TTL. */
    driftFactor: floatEnv('LOCK_DRIFT_FACTOR', 0.01),
    /** Watchdog heartbeat that extends held locks. 0 = a third of the TTL. */
    watchdogIntervalMs: intEnv('LOCK_WATCHDOG_INTERVAL_MS', 0),
  },

  charges: {
    /** Parallel charge workers per instance (also the AMQP prefetch of the charges queue). */
    concurrency: positiveIntEnv('CHARGE_WORKER_CONCURRENCY', 8),
    /** Times an item is put back in the queue (lock held, Redis / gateway unavailable) before it is dead-lettered. */
    maxDeferrals: positiveIntEnv('CHARGE_MAX_DEFERRALS', 10),
    /** Deferral delay: full jitter over `base × 2^attempt`, capped at `max`, never below `min`. */
    deferBaseMs: positiveIntEnv('CHARGE_DEFER_BASE_MS', 250),
    deferMaxMs: positiveIntEnv('CHARGE_DEFER_MAX_MS', 30_000),
    deferMinMs: intEnv('CHARGE_DEFER_MIN_MS', 50),
  },

  gateway: {
    /**
     * Circuit breaker in front of the OPPWA client (resilience/circuitBreaker.ts).
     * `failureThreshold` outage-class failures (no reply, 5xx, 429) within
     * `windowMs` open the circuit for `openDurationMs`; every failed half-open
     * probe doubles that, up to `maxOpenDurationMs`.
     */
    breaker: {
      windowMs: positiveIntEnv('GATEWAY_BREAKER_WINDOW_MS', 30_000),
      failureThreshold: positiveIntEnv('GATEWAY_BREAKER_FAILURE_THRESHOLD', 5),
      openDurationMs: positiveIntEnv('GATEWAY_BREAKER_OPEN_MS', 60_000),
      maxOpenDurationMs: positiveIntEnv('GATEWAY_BREAKER_MAX_OPEN_MS', 600_000),
      halfOpenMaxCalls: positiveIntEnv('GATEWAY_BREAKER_HALF_OPEN_CALLS', 1),
    },
    /**
     * How often the recovery service sweeps deferred verifications while the
     * circuit is closed (state changes trigger it immediately). 0 disables the timer.
     */
    recoveryIntervalMs: intEnv('GATEWAY_RECOVERY_INTERVAL_MS', 30_000),
  },

  outbox: {
    /**
     * How often the outbox publisher polls for unpublished events. Polling is
     * the safety net — a commit also wakes the publisher immediately. 0
     * disables the background publisher (events stay in the outbox until
     * POST /api/admin/outbox/publish).
     */
    pollIntervalMs: intEnv('OUTBOX_POLL_INTERVAL_MS', 1000),
    /** Max events claimed per pass; a full batch is followed by another pass at once. */
    batchSize: positiveIntEnv('OUTBOX_BATCH_SIZE', 50),
    /**
     * How long a claimed event stays invisible to other publishers. Must
     * exceed `publishTimeoutMs` × `batchSize` in the worst case; an expired
     * lease means the claimant died and the event is picked up again.
     */
    leaseMs: positiveIntEnv('OUTBOX_LEASE_MS', 30_000),
    /** Broker nacks / timeouts before an event is parked as FAILED (retry via the admin API). */
    maxAttempts: positiveIntEnv('OUTBOX_MAX_ATTEMPTS', 10),
    /** Backoff between attempts: starts here, doubles, capped at `maxRetryDelayMs`. */
    retryDelayMs: intEnv('OUTBOX_RETRY_DELAY_MS', 1000),
    maxRetryDelayMs: intEnv('OUTBOX_MAX_RETRY_DELAY_MS', 60_000),
    /** How long to wait for the broker's publisher confirm before treating the attempt as failed. */
    publishTimeoutMs: positiveIntEnv('OUTBOX_PUBLISH_TIMEOUT_MS', 10_000),
    /** PUBLISHED events older than this are deleted from the outbox. 0 keeps them forever. */
    retentionMs: intEnv('OUTBOX_RETENTION_MS', 86_400_000),
  },

  admin: {
    /**
     * Shared secret for the /api/admin routes. When unset, the admin API is
     * disabled and every admin request is answered with 503.
     */
    apiKey: nullableEnv('ADMIN_API_KEY'),
  },

  /** Path to the JSON flat-file database. Overrideable in tests via DB_PATH env var. */
  get dbPath(): string {
    return optionalEnv('DB_PATH', './db.json');
  },
};

export type Config = typeof config;
