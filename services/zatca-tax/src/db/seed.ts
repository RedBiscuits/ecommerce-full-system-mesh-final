import { resetDatabaseAndSeed } from './index';

async function main(): Promise<void> {
  console.log('Initializing ZATCA database schema and seeding KSA taxpayer...');
  await resetDatabaseAndSeed();
  console.log('Seed completed: Taxpayer txp_riyadh_retail (VAT 310123456700003) initialized.');
}

main().catch((err) => {
  console.error('Seed failed:', err);
  process.exit(1);
});
