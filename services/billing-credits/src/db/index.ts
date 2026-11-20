import { PGlite } from '@electric-sql/pglite';
import { Pool as PgPool } from 'pg';
import type { PoolClient, QueryResult, QueryResultRow } from 'pg';
import fs from 'fs';
import path from 'path';
import { INTEGRATION_TABLES, INTEGRATION_TABLES_DROP_SQL, INTEGRATION_TABLES_SQL } from '../../../../shared/messaging/sql';

let pgInstance: PGlite | null = null;
let initPromise: Promise<PGlite> | null = null;

async function getPgInstance(): Promise<PGlite> {
  if (pgInstance) return pgInstance;
  if (!initPromise) {
    initPromise = PGlite.create().then(async (db) => {
      await db.exec("SET TIME ZONE 'UTC';");
      pgInstance = db;
      return db;
    });
  }
  return initPromise;
}

/**
 * Embedded PGlite backend. PGlite is a single connection, so this shim serializes whole
 * transactions between clients (a coarse stand-in for row locks).
 */
class PgliteShimPool implements DbBackend {
  private activeTxOwner: symbol | null = null;
  private txWaitQueue: Array<() => void> = [];

  private async acquireTxSlot(owner: symbol): Promise<void> {
    if (this.activeTxOwner === owner) return;
    while (this.activeTxOwner !== null && this.activeTxOwner !== owner) {
      await new Promise<void>((resolve) => this.txWaitQueue.push(resolve));
    }
    this.activeTxOwner = owner;
  }

  private releaseTxSlot(owner: symbol): void {
    if (this.activeTxOwner === owner) {
      this.activeTxOwner = null;
      const next = this.txWaitQueue.shift();
      if (next) next();
    }
  }

  private async waitForNoActiveTx(caller?: symbol): Promise<void> {
    while (this.activeTxOwner !== null && this.activeTxOwner !== caller) {
      await new Promise<void>((resolve) => this.txWaitQueue.push(resolve));
    }
  }

  async executeQuery<T extends QueryResultRow = any>(
    text: string,
    params?: any[],
    owner?: symbol
  ): Promise<QueryResult<T>> {
    const db = await getPgInstance();
    const trimmed = text.trim();
    const upper = trimmed.toUpperCase();

    if (owner) {
      if (upper.startsWith('BEGIN') || upper.startsWith('START TRANSACTION')) {
        await this.acquireTxSlot(owner);
        await db.exec('BEGIN');
        return {
          command: 'BEGIN',
          rowCount: 0,
          oid: 0,
          rows: [],
          fields: [],
        };
      }

      if (upper.startsWith('COMMIT') || upper.startsWith('END')) {
        try {
          await db.exec('COMMIT');
        } finally {
          this.releaseTxSlot(owner);
        }
        return {
          command: 'COMMIT',
          rowCount: 0,
          oid: 0,
          rows: [],
          fields: [],
        };
      }

      // `ROLLBACK TO SAVEPOINT` is a partial rollback inside the transaction: pass it through.
      if (upper.startsWith('ROLLBACK') && !/^ROLLBACK\s+TO\b/.test(upper)) {
        try {
          await db.exec('ROLLBACK');
        } finally {
          this.releaseTxSlot(owner);
        }
        return {
          command: 'ROLLBACK',
          rowCount: 0,
          oid: 0,
          rows: [],
          fields: [],
        };
      }
    }

    await this.waitForNoActiveTx(owner);

    if (!owner || this.activeTxOwner !== owner) {
      await new Promise<void>((resolve) => setImmediate(resolve));
      await this.waitForNoActiveTx(owner);
    }

    const res = await db.query<T>(text, params);
    const rowCount =
      typeof res.affectedRows === 'number' && res.affectedRows > 0
        ? res.affectedRows
        : res.rows.length;

    if (!owner || this.activeTxOwner !== owner) {
      await new Promise<void>((resolve) => setImmediate(resolve));
    }

    return {
      command: upper.split(/\s+/)[0] || 'SELECT',
      rowCount,
      oid: 0,
      rows: res.rows,
      fields: (res.fields || []) as any,
    };
  }

  async query<T extends QueryResultRow = any>(
    text: string,
    params?: any[]
  ): Promise<QueryResult<T>> {
    return this.executeQuery<T>(text, params);
  }

  async connect(): Promise<PoolClient> {
    const owner = Symbol('PoolClient');
    let released = false;

    const client = {
      query: async <T extends QueryResultRow = any>(
        textOrConfig: string | { text: string; values?: any[] },
        values?: any[]
      ): Promise<QueryResult<T>> => {
        if (released) {
          throw new Error('Client has already been released to the pool');
        }
        const sql = typeof textOrConfig === 'string' ? textOrConfig : textOrConfig.text;
        const params =
          typeof textOrConfig === 'string' ? values : textOrConfig.values ?? values;
        return this.executeQuery<T>(sql, params, owner);
      },
      release: async () => {
        if (released) return;
        released = true;
        if (this.activeTxOwner === owner) {
          try {
            const db = await getPgInstance();
            await db.exec('ROLLBACK');
          } catch {
            // ignore cleanup rollback errors
          } finally {
            this.releaseTxSlot(owner);
          }
        }
      },
    };

    return client as unknown as PoolClient;
  }

  async execMulti(sql: string): Promise<void> {
    await this.waitForNoActiveTx();
    const db = await getPgInstance();
    await db.exec(sql);
  }

  async end(): Promise<void> {
    if (pgInstance) {
      await pgInstance.close();
      pgInstance = null;
      initPromise = null;
    }
  }
}

interface DbBackend {
  query<T extends QueryResultRow = any>(text: string, params?: any[]): Promise<QueryResult<T>>;
  connect(): Promise<PoolClient>;
  execMulti(sql: string): Promise<void>;
  end(): Promise<void>;
}

/** Real PostgreSQL backend (used when DATABASE_URL is set), with genuine row-level locking. */
class PostgresBackend implements DbBackend {
  private readonly pgPool: PgPool;

  constructor(connectionString: string) {
    this.pgPool = new PgPool({
      connectionString,
      max: Number(process.env.DATABASE_POOL_MAX || 20),
      options: '-c timezone=UTC',
    });
  }

  query<T extends QueryResultRow = any>(text: string, params?: any[]): Promise<QueryResult<T>> {
    return this.pgPool.query<T>(text, params);
  }

  connect(): Promise<PoolClient> {
    return this.pgPool.connect();
  }

  async execMulti(sql: string): Promise<void> {
    await this.pgPool.query(sql);
  }

  end(): Promise<void> {
    return this.pgPool.end();
  }
}

/**
 * Backend is chosen lazily on first use: real PostgreSQL when DATABASE_URL is set,
 * otherwise the embedded PGlite instance.
 */
class DatabasePool implements DbBackend {
  private backend: DbBackend | null = null;
  /** Set once the schema has been installed on the current backend (reset on `end`). */
  schemaInstalled = false;

  private get impl(): DbBackend {
    if (!this.backend) {
      const url = process.env.DATABASE_URL;
      this.backend = url ? new PostgresBackend(url) : new PgliteShimPool();
    }
    return this.backend;
  }

  query<T extends QueryResultRow = any>(text: string, params?: any[]): Promise<QueryResult<T>> {
    return this.impl.query<T>(text, params);
  }

  connect(): Promise<PoolClient> {
    return this.impl.connect();
  }

  execMulti(sql: string): Promise<void> {
    return this.impl.execMulti(sql);
  }

  async end(): Promise<void> {
    if (this.backend) {
      const backend = this.backend;
      this.backend = null;
      this.schemaInstalled = false;
      await backend.end();
    }
  }
}

export const pool = new DatabasePool();

export async function query<T extends QueryResultRow = any>(
  text: string,
  params?: any[]
): Promise<QueryResult<T>> {
  return pool.query<T>(text, params);
}

export async function getClient(): Promise<PoolClient> {
  return pool.connect();
}

/**
 * Run `fn` inside a single BEGIN/COMMIT transaction on a dedicated client.
 * Any error thrown by `fn` rolls the whole unit of work back and is re-thrown.
 *
 * Every statement in `fn` must go through the provided client; calling the
 * top-level `query()` helper from inside the callback would run outside the
 * transaction (and, with the embedded PGlite pool, wait on it forever).
 */
const transactionScopes = new WeakMap<PoolClient, Record<string, unknown>>();

/**
 * Per-transaction scratch space attached to a client (e.g. the wallet lock ledger). A fresh scope
 * is created at every BEGIN, so state never leaks between transactions on a pooled connection.
 */
export function txScope(client: PoolClient): Record<string, unknown> {
  let scope = transactionScopes.get(client);
  if (!scope) {
    scope = {};
    transactionScopes.set(client, scope);
  }
  return scope;
}

export async function withTransaction<T>(fn: (client: PoolClient) => Promise<T>): Promise<T> {
  const client = await pool.connect();
  let result: T;
  try {
    await client.query('BEGIN');
    transactionScopes.set(client, {});
    result = await fn(client);
  } catch (err) {
    try {
      await client.query('ROLLBACK');
    } catch {
      // Surface the original failure, not the rollback failure.
    }
    client.release();
    throw err;
  }

  // COMMIT is kept out of the catch above: after a failed COMMIT the server has
  // already ended the transaction, so issuing a ROLLBACK would be wrong.
  try {
    await client.query('COMMIT');
  } finally {
    client.release();
  }
  return result;
}

/**
 * Shared transaction helper: join the caller's open transaction when `client` is given,
 * otherwise open (and own) a new one. Lets service methods compose into a single atomic
 * unit of work without nesting BEGINs.
 */
export async function runInTransaction<T>(
  client: PoolClient | undefined,
  fn: (client: PoolClient) => Promise<T>
): Promise<T> {
  return client ? fn(client) : withTransaction(fn);
}

const ALL_TABLES = [
  ...INTEGRATION_TABLES,
  'order_credit_refunds',
  'settlement_fee_quotes',
  'settlement_fee_rules',
  'order_credit_settlements',
  'order_credit_hold_keys',
  'usage_rollup_deltas',
  'usage_daily_rollups',
  'auto_recharge_outbox',
  'credit_hold_portions',
  'auto_recharge_orders',
  'billing_discrepancies',
  'stripe_webhook_events',
  'credit_lot_allocations',
  'usage_events',
  'credit_holds',
  'credit_lots',
  'credit_grants',
  'org_wallet_stripes',
  'org_balances',
];

export async function resetAndSeedDatabase(): Promise<void> {
  if (pool.schemaInstalled) {
    // Schema already installed on this connection: emptying the tables is much cheaper than
    // dropping and recreating them.
    await pool.execMulti(`TRUNCATE TABLE ${ALL_TABLES.join(', ')} CASCADE`);
  } else {
    const schemaPath = path.join(__dirname, 'schema.sql');
    await pool.execMulti(fs.readFileSync(schemaPath, 'utf-8'));
    // Transactional outbox + consumer checkpoints of the platform integration.
    await pool.execMulti(INTEGRATION_TABLES_DROP_SQL + INTEGRATION_TABLES_SQL);
    pool.schemaInstalled = true;
  }

  await pool.query(
    `INSERT INTO org_balances (
       org_id, plan_tier, auto_recharge_enabled, auto_recharge_threshold_credits,
       auto_recharge_amount_credits, monthly_recharge_cap_credits
     )
     VALUES
       ('org_acme', 'GROWTH', FALSE, 250, 2000, 10000),
       ('org_globex', 'ENTERPRISE', TRUE, 500, 3000, 15000)`
  );

  await pool.query(
    `INSERT INTO credit_grants (grant_id, org_id, source, invoice_id, initial_credits, priority, status, expires_at)
     VALUES
       ('grant_promo_01', 'org_acme', 'PROMOTIONAL', NULL, 500, 1, 'ACTIVE', NOW() + INTERVAL '7 days'),
       ('grant_inv_01', 'org_acme', 'PREPAID_INVOICE', 'inv_initial_1000', 1000, 10, 'ACTIVE', NOW() + INTERVAL '90 days'),
       ('grant_ent_01', 'org_globex', 'ENTERPRISE_COMMIT', 'inv_globex_5000', 5000, 10, 'ACTIVE', NOW() + INTERVAL '365 days')`
  );

  await seedWalletStripes();
  await seedSettlementFeeRules();
}

/**
 * The platform's default merchant-settlement pricing (marginal tiers on the settled gross, VAT on
 * top): first 50,000.00 SAR at 2.50 %, up to 250,000.00 SAR at 2.00 %, beyond at 1.50 %, plus
 * 0.50 SAR per settled transaction.
 */
export const DEFAULT_SETTLEMENT_FEE_RULE = {
  ruleId: 'std-tiered',
  version: 1,
  tiers: [
    { upToHalalas: 5_000_000, bps: 250 },
    { upToHalalas: 25_000_000, bps: 200 },
    { upToHalalas: null, bps: 150 },
  ],
  perTransactionHalalas: 50,
  minimumFeeHalalas: 0,
  vatRateBps: 1500,
} as const;

async function seedSettlementFeeRules(): Promise<void> {
  const rule = DEFAULT_SETTLEMENT_FEE_RULE;
  await pool.query(
    `INSERT INTO settlement_fee_rules
       (rule_id, version, merchant_id, currency, tiers, per_transaction_halalas, minimum_fee_halalas, vat_rate_bps, active)
     VALUES ($1, $2, NULL, 'SAR', $3::jsonb, $4, $5, $6, TRUE)`,
    [rule.ruleId, rule.version, JSON.stringify(rule.tiers), rule.perTransactionHalalas, rule.minimumFeeHalalas, rule.vatRateBps]
  );
}

/** Create wallet stripes for every org and split each seeded grant evenly across them. */
async function seedWalletStripes(): Promise<void> {
  await pool.query(
    `INSERT INTO org_wallet_stripes (org_id, stripe_no)
     SELECT o.org_id, s.stripe_no
     FROM org_balances o
     CROSS JOIN LATERAL generate_series(0, o.wallet_stripe_count - 1) AS s(stripe_no)
     ON CONFLICT DO NOTHING`
  );
  await pool.query(
    `INSERT INTO credit_lots (grant_id, stripe_no, org_id, remaining_credits)
     SELECT g.grant_id, s.stripe_no, g.org_id,
            g.initial_credits / o.wallet_stripe_count
              + CASE WHEN s.stripe_no < g.initial_credits % o.wallet_stripe_count THEN 1 ELSE 0 END
     FROM credit_grants g
     JOIN org_balances o ON o.org_id = g.org_id
     CROSS JOIN LATERAL generate_series(0, o.wallet_stripe_count - 1) AS s(stripe_no)`
  );
  await pool.query(
    `UPDATE org_wallet_stripes ws
     SET available_credits = COALESCE((
       SELECT SUM(l.remaining_credits)
       FROM credit_lots l
       JOIN credit_grants g ON g.grant_id = l.grant_id
       WHERE l.org_id = ws.org_id AND l.stripe_no = ws.stripe_no AND g.status = 'ACTIVE'
     ), 0)`
  );
}

/** True for PostgreSQL unique_violation errors (SQLSTATE 23505), on both pg and PGlite. */
export function isUniqueViolation(err: unknown): boolean {
  return typeof err === 'object' && err !== null && (err as { code?: string }).code === '23505';
}

export async function closeDb(): Promise<void> {
  await pool.end();
}
