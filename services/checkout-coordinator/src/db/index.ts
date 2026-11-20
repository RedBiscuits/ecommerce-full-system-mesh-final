import { PGlite } from '@electric-sql/pglite';
import fs from 'fs';
import path from 'path';
import { INTEGRATION_TABLES_SQL } from '../../../../shared/messaging/sql';

let dbInstance: PGlite | null = null;
let initPromise: Promise<PGlite> | null = null;

/** The coordinator's embedded database (one per process; concurrent first callers share the init). */
export async function getDb(): Promise<PGlite> {
  if (dbInstance) return dbInstance;
  initPromise ??= (async () => {
    const db = new PGlite();
    await db.exec("SET TIME ZONE 'UTC';");
    await db.exec(fs.readFileSync(path.join(__dirname, 'schema.sql'), 'utf8'));
    await db.exec(INTEGRATION_TABLES_SQL);
    dbInstance = db;
    return db;
  })();
  return initPromise;
}

export async function resetDatabase(): Promise<PGlite> {
  const db = await getDb();
  await db.exec(`TRUNCATE TABLE checkout_steps, checkouts, integration_checkpoints, integration_outbox RESTART IDENTITY CASCADE;`);
  return db;
}
