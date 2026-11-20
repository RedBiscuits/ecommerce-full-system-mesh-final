import { resetAndSeedDatabase, closeDb } from './index';
import { flushRedis, closeRedis } from '../redis';

async function main() {
  try {
    console.log('Initializing database schema and seeding organization credit grants...');
    await resetAndSeedDatabase();
    await flushRedis();
    console.log('Seed completed: org_acme initialized with 1,500 credits across 2 active lots.');
  } catch (err) {
    console.error('Failed to seed database:', err);
    process.exitCode = 1;
  } finally {
    await closeDb();
    await closeRedis();
  }
}

main();
