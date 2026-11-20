import dotenv from 'dotenv';
import { assertValidFeeBps } from '../money/fees';

dotenv.config();

export const config = {
  port: parseInt(process.env.PORT || '3002', 10),
  nodeEnv: process.env.NODE_ENV || 'development',
  defaultCurrency: process.env.DEFAULT_CURRENCY || 'USD',
  /**
   * Transfers strictly above this amount are not credited to the recipient
   * immediately; the funds are held in wallet_escrow pending AML review.
   * Default: $500.00.
   */
  amlReviewThresholdCents: parseInt(process.env.AML_REVIEW_THRESHOLD_CENTS || '50000', 10),
  /**
   * Platform fee in basis points (1 bps = 0.01%), charged on top of the
   * transferred principal and routed to the fee treasury of the source currency.
   * Read at call time so it can be changed without a restart. Default: 0.
   */
  platformFeeBps: assertValidFeeBps(parseInt(process.env.PLATFORM_FEE_BPS || '0', 10)),
  eventBus: {
    /** 'fake' = in-process @onify/fake-amqplib (sandbox); 'amqp' = real RabbitMQ. */
    driver: (process.env.EVENT_BUS_DRIVER || 'fake') as 'fake' | 'amqp',
    url: process.env.RABBITMQ_URL || 'amqp://localhost',
    /** Max unacked deliveries per consumer: bounds in-flight memory. */
    prefetch: parseInt(process.env.RECONCILIATION_PREFETCH || '10', 10),
    /** Total delivery attempts before a message is dead-lettered. */
    maxAttempts: parseInt(process.env.RECONCILIATION_MAX_ATTEMPTS || '4', 10),
    /** Delay tiers (ms) for attempt 1, 2, 3...; the last tier repeats. */
    retryDelaysMs: (process.env.RECONCILIATION_RETRY_DELAYS_MS || '1000,5000,30000')
      .split(',')
      .map((v) => parseInt(v.trim(), 10)),
    /** Queue length caps (x-max-length + reject-publish): bounds broker memory. */
    maxQueueLength: parseInt(process.env.RECONCILIATION_MAX_QUEUE_LENGTH || '10000', 10),
    maxDeadLetterLength: parseInt(process.env.RECONCILIATION_MAX_DLQ_LENGTH || '100000', 10),
    /** Unconfirmed publishes retained for re-publishing across reconnects. */
    maxPendingPublishes: parseInt(process.env.EVENT_BUS_MAX_PENDING_PUBLISHES || '10000', 10),
    reconnect: {
      initialDelayMs: parseInt(process.env.EVENT_BUS_RECONNECT_INITIAL_MS || '100', 10),
      maxDelayMs: parseInt(process.env.EVENT_BUS_RECONNECT_MAX_MS || '30000', 10),
    },
  },
  audit: {
    /** Rows fetched per keyset page by the ledger auditor: bounds app memory. */
    pageSize: parseInt(process.env.LEDGER_AUDIT_PAGE_SIZE || '500', 10),
    /** Max violations echoed back in an audit report (the rest are counted). */
    reportSampleSize: parseInt(process.env.LEDGER_AUDIT_SAMPLE_SIZE || '20', 10),
    /**
     * When the work queue is at its cap, the audit waits for consumers to
     * drain it (woken by capacity signals, never by polling) before publishing
     * more. If no capacity appears within this window the audit stops and
     * reports itself INCOMPLETE rather than dropping events.
     */
    drainTimeoutMs: parseInt(process.env.LEDGER_AUDIT_DRAIN_TIMEOUT_MS || '60000', 10),
  },
  db: {
    /**
     * Create + seed the schema at server start. The embedded sandbox database
     * is in-memory per process, so this defaults to on; set to 'false' when
     * pointing the service at a persistent database.
     */
    seedOnStart: (process.env.DB_SEED_ON_START ?? 'true') !== 'false',
  },
  auth: {
    /**
     * 'enforce' (default): every non-public route requires a verified bearer token.
     * 'legacy': migration mode for pre-auth clients. Requests WITHOUT an
     *   Authorization header keep the old unauthenticated behaviour (and get
     *   Deprecation/Warning headers); presented tokens are still fully verified
     *   and authorized. Read per request, so it can be flipped without a restart.
     */
    mode: (process.env.AUTH_MODE || 'enforce') as 'enforce' | 'legacy',
    /** HS256 shared secret (>= 32 bytes). Ignored when a public key is configured. */
    jwtSecret: process.env.AUTH_JWT_SIGNING_MATERIAL || 'z'.repeat(40),
    /** PEM (SPKI) public key for asymmetric tokens (RS256/ES256). '\n' escapes allowed. */
    jwtPublicKey: (process.env.AUTH_JWT_PUBLIC_KEY || '').replace(/\\n/g, '\n'),
    /** Defaults to RS256 with a public key, HS256 with a secret. */
    algorithm: process.env.AUTH_JWT_ALGORITHM || (process.env.AUTH_JWT_PUBLIC_KEY ? 'RS256' : 'HS256'),
    issuer: process.env.AUTH_JWT_ISSUER || 'wallet-ledger-auth',
    audience: process.env.AUTH_JWT_AUDIENCE || 'wallet-ledger-api',
    clockToleranceSec: parseInt(process.env.AUTH_CLOCK_TOLERANCE_SECONDS || '30', 10),
  },
  settlement: {
    /**
     * Merchant settlement sweep: order settlements claimed per chunk. Each chunk is one short
     * transaction (bounded lock time, O(chunk) memory); the event loop is yielded between chunks.
     */
    chunkSize: parseInt(process.env.SETTLEMENT_SWEEP_CHUNK_SIZE || '500', 10),
    /** Cap on the entries of one batch; the rest of the window is left for the next batch. */
    maxItemsPerBatch: parseInt(process.env.SETTLEMENT_MAX_ITEMS_PER_BATCH || '100000', 10),
    /** Optional pause between chunks (throttles a very large sweep further). */
    pauseMs: parseInt(process.env.SETTLEMENT_SWEEP_PAUSE_MS || '0', 10),
  },
  frozenWallet: {
    /** One reconciliation event per frozen wallet per window (Redis SET NX EX). */
    eventDedupeSeconds: parseInt(process.env.FROZEN_WALLET_EVENT_DEDUPE_SECONDS || '30', 10),
    /** Upper bound on how long a 409 response waits for the event publish. */
    publishTimeoutMs: parseInt(process.env.FROZEN_WALLET_PUBLISH_TIMEOUT_MS || '2000', 10),
  },
};
