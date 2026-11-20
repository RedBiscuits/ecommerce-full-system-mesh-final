import { PGlite } from '@electric-sql/pglite';
import type { PoolClient, QueryResult, QueryResultRow } from 'pg';
import fs from 'fs';
import path from 'path';
import { INTEGRATION_TABLES_DROP_SQL, INTEGRATION_TABLES_SQL } from '../../../../shared/messaging/sql';

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

class SandboxPool {
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

  async executeQuery<T extends QueryResultRow = QueryResultRow>(
    text: string,
    params?: unknown[],
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

      if (upper.startsWith('ROLLBACK')) {
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
      fields: (res.fields || []) as unknown as QueryResult<T>['fields'],
    };
  }

  async query<T extends QueryResultRow = QueryResultRow>(
    text: string,
    params?: unknown[]
  ): Promise<QueryResult<T>> {
    return this.executeQuery<T>(text, params);
  }

  async connect(): Promise<PoolClient> {
    const owner = Symbol('PoolClient');
    let released = false;

    const client = {
      query: async <T extends QueryResultRow = QueryResultRow>(
        textOrConfig: string | { text: string; values?: unknown[] },
        values?: unknown[]
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
            // ignore rollback error on cleanup
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

export const pool = new SandboxPool();

export async function query<T extends QueryResultRow = QueryResultRow>(
  text: string,
  params?: unknown[]
): Promise<QueryResult<T>> {
  return pool.query<T>(text, params);
}

export async function getClient(): Promise<PoolClient> {
  return pool.connect();
}

/**
 * Run `fn` inside a single database transaction on a dedicated client.
 *
 * - COMMIT if `fn` resolves, ROLLBACK if it throws (the original error is re-thrown).
 * - A failed COMMIT is not followed by ROLLBACK: PostgreSQL already aborts the
 *   transaction when COMMIT fails, and issuing ROLLBACK on a connection we no
 *   longer own a transaction on is unsafe.
 * - The client is always released back to the pool.
 */
export async function withTransaction<T>(
  fn: (client: PoolClient) => Promise<T>
): Promise<T> {
  const client = await getClient();
  try {
    await client.query('BEGIN');

    let result: T;
    try {
      result = await fn(client);
    } catch (err) {
      try {
        await client.query('ROLLBACK');
      } catch {
        // Swallow rollback failures so the original error surfaces.
      }
      throw err;
    }

    await client.query('COMMIT');
    return result;
  } finally {
    client.release();
  }
}

export async function resetAndSeedDatabase(): Promise<void> {
  const schemaPath = path.join(__dirname, 'schema.sql');
  const sql = fs.readFileSync(schemaPath, 'utf-8');
  await pool.execMulti(sql);
  // Transactional outbox + consumer checkpoints of the platform integration.
  await pool.execMulti(INTEGRATION_TABLES_DROP_SQL + INTEGRATION_TABLES_SQL);

  // Opening balances as one balanced journal per currency against each
  // currency's clearing account: Alice/Bob USD 100.00, Carol EUR 100.00, Dave JPY
  // 15,000, Noura SAR 500.00. The delta trigger maintains every running total (and the live
  // balance of ledger-maintained accounts); app-maintained balances are then
  // set from their running totals, all in one transaction so the deferred sync
  // check passes at COMMIT. Version stays at 1 (opening state, not a change).
  await withTransaction(async (client) => {
    await client.query(
      `INSERT INTO ledger_entries (entry_id, transaction_ref, wallet_id, currency, direction, signed_amount_cents)
       VALUES
         ('le_seed_alice', 'seed_init', 'wallet_alice', 'USD', 'CREDIT', 10000),
         ('le_seed_bob', 'seed_init', 'wallet_bob', 'USD', 'CREDIT', 10000),
         ('le_seed_carol', 'seed_init', 'wallet_carol', 'EUR', 'CREDIT', 10000),
         ('le_seed_dave', 'seed_init', 'wallet_dave', 'JPY', 'CREDIT', 15000),
         ('le_seed_treasury', 'seed_init', 'wallet_treasury', 'USD', 'DEBIT', -20000),
         ('le_seed_treasury_eur', 'seed_init', 'wallet_treasury_eur', 'EUR', 'DEBIT', -10000),
         ('le_seed_treasury_jpy', 'seed_init', 'wallet_treasury_jpy', 'JPY', 'DEBIT', -15000),
         ('le_seed_noura', 'seed_init', 'wallet_noura_sar', 'SAR', 'CREDIT', 50000),
         ('le_seed_treasury_sar', 'seed_init', 'wallet_treasury_sar', 'SAR', 'DEBIT', -50000)`
    );
    await client.query(
      `UPDATE wallets SET balance_cents = ledger_balance_cents
       WHERE account_type IN ('USER', 'ESCROW')`
    );
  });
}

export async function closeDb(): Promise<void> {
  await pool.end();
}
