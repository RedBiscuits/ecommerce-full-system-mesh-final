import { PGlite } from '@electric-sql/pglite';
import RedisMock from 'ioredis-mock';
import type Redis from 'ioredis';
import crypto from 'crypto';
import fs from 'fs';
import path from 'path';
import { INTEGRATION_TABLES_SQL } from '../../../../shared/messaging/sql';

let dbInstance: PGlite | null = null;
export const redis: Redis = new RedisMock() as unknown as Redis;

/** The platform's taxpayer id (seller of the processing service billed at merchant settlement). */
export const PLATFORM_TAXPAYER_ID = process.env.PLATFORM_TAXPAYER_ID || 'txp_ksa_platform';

export const ZATCA_GENESIS_PIH = crypto.createHash('sha256').update('0', 'utf8').digest('base64');

const { privateKey, publicKey } = crypto.generateKeyPairSync('ec', {
  namedCurve: 'secp256k1',
  publicKeyEncoding: { type: 'spki', format: 'der' },
  privateKeyEncoding: { type: 'pkcs8', format: 'pem' },
});

export const DEFAULT_CSID_KEYS = {
  privateKeyPem: privateKey,
  publicKeyBase64: publicKey.toString('base64'),
  csidCertificate: Buffer.from('ZATCA-PHASE2-CSID-CERT-RIYADH-COMMERCE-2026').toString('base64'),
};

let initPromise: Promise<PGlite> | null = null;

/**
 * The service's embedded database, created once per process. Concurrent first
 * callers share one initialization, so nobody ever sees the instance before
 * its schema is installed.
 */
export async function getDb(): Promise<PGlite> {
  if (dbInstance) return dbInstance;
  initPromise ??= (async () => {
    const db = new PGlite();
    await db.exec("SET TIME ZONE 'UTC';");
    const schemaPath = path.join(__dirname, 'schema.sql');
    const schemaSql = fs.readFileSync(schemaPath, 'utf8');
    await db.exec(schemaSql);
    // Transactional outbox + consumer checkpoints of the platform integration.
    await db.exec(INTEGRATION_TABLES_SQL);
    dbInstance = db;
    return db;
  })();
  return initPromise;
}

export async function resetDatabaseAndSeed(): Promise<PGlite> {
  const db = await getDb();
  await db.exec(`
    TRUNCATE TABLE
      integration_checkpoints,
      integration_outbox,
      invoice_pdf_artifacts,
      zatca_submission_logs,
      invoice_line_items,
      invoices,
      egs_devices,
      taxpayers
    RESTART IDENTITY CASCADE;
  `);
  await redis.flushall();

  await db.query(
    `INSERT INTO taxpayers (
      id, legal_name_ar, legal_name_en, vat_number, crn_number,
      egs_serial_number, business_category, onboarding_status,
      street_name, building_number, district_name, city, postal_code, country_code,
      csid_certificate, private_key_pem, public_key_base64, last_icv, last_invoice_hash
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, 'PRODUCTION_CSID', $8, $9, $10, $11, $12, 'SA', $13, $14, $15, 0, $16)`,
    [
      'txp_riyadh_retail',
      'شركة مدار الرياض للتجارة الإلكترونية',
      'Riyadh Orbit E-Commerce Co.',
      '310123456700003',
      '1010987654',
      '1-ORBIT|2-POS-RYD|3-900120',
      'Electronics & Specialty Retail',
      'King Fahd Road',
      '4210',
      'Al Olaya',
      'Riyadh',
      '12214',
      DEFAULT_CSID_KEYS.csidCertificate,
      DEFAULT_CSID_KEYS.privateKeyPem,
      DEFAULT_CSID_KEYS.publicKeyBase64,
      ZATCA_GENESIS_PIH,
    ]
  );

  await db.query(
    `INSERT INTO egs_devices (
      id, taxpayer_id, egs_unit_id, serial_number, branch_name, city, model_version, status
    ) VALUES ($1, $2, $3, $4, $5, $6, 'ORBIT-POS-V2', 'ACTIVE')`,
    [
      'egs_ryd_main_01',
      'txp_riyadh_retail',
      'EGS-RYD-OLAYA-01',
      '1-ORBIT|2-POS-RYD|3-900120',
      'Riyadh Olaya Flagship Store',
      'Riyadh',
    ]
  );

  // The platform itself: seller of the payment-processing service it charges merchants at
  // settlement (fee tax invoices + payout credit notes, in its own ICV / PIH chain).
  await db.query(
    `INSERT INTO taxpayers (
      id, legal_name_ar, legal_name_en, vat_number, crn_number,
      egs_serial_number, business_category, onboarding_status,
      street_name, building_number, district_name, city, postal_code, country_code,
      csid_certificate, private_key_pem, public_key_base64, last_icv, last_invoice_hash
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, 'PRODUCTION_CSID', $8, $9, $10, $11, $12, 'SA', $13, $14, $15, 0, $16)`,
    [
      PLATFORM_TAXPAYER_ID,
      'شركة منصة المدفوعات السعودية',
      'KSA Commerce Payments Platform Co.',
      '311234567800003',
      '1010765432',
      '1-ORBIT|2-SETTLEMENT|3-000001',
      'Payment Processing Services',
      'Prince Turki Road',
      '7788',
      'Al Aqiq',
      'Riyadh',
      '13515',
      DEFAULT_CSID_KEYS.csidCertificate,
      DEFAULT_CSID_KEYS.privateKeyPem,
      DEFAULT_CSID_KEYS.publicKeyBase64,
      ZATCA_GENESIS_PIH,
    ]
  );
  await db.query(
    `INSERT INTO egs_devices (
      id, taxpayer_id, egs_unit_id, serial_number, branch_name, city, model_version, status
    ) VALUES ($1, $2, $3, $4, $5, $6, 'ORBIT-SETTLEMENT-V1', 'ACTIVE')`,
    ['egs_platform_settlement_01', PLATFORM_TAXPAYER_ID, 'EGS-PLT-SETTLE-01', '1-ORBIT|2-SETTLEMENT|3-000001', 'Settlement Engine', 'Riyadh']
  );

  return db;
}
