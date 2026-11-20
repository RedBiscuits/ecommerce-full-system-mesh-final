import { resetAndSeedDatabase, closeDb } from './index';
import { flushRedis, closeRedis } from '../redis';

async function main() {
  try {
    console.log('Resetting PostgreSQL schema and seeding initial wallets & ledger entries...');
    await resetAndSeedDatabase();
    console.log('Flushing Redis idempotency & lock keys...');
    await flushRedis();
    console.log('Seed completed: wallet_alice (USD 100.00), wallet_bob (USD 100.00), wallet_carol (EUR 100.00), wallet_dave (JPY 15,000) + per-currency system accounts and FX rates.');
  } catch (err) {
    console.error('Failed to seed wallet ledger database:', err);
    process.exitCode = 1;
  } finally {
    await closeDb();
    await closeRedis();
  }
}

void main();
