/* eslint-disable @typescript-eslint/no-unused-vars */
import { describe, it, test, expect, beforeEach, afterEach, beforeAll, afterAll, vi } from 'vitest';
/* eslint-disable @typescript-eslint/no-unused-vars */
/**
 * tests/helpers/testDb.ts
 *
 * Helpers for the per-test-file temporary JSON DB.
 * The DB_PATH itself is assigned in jestSetup.ts before any module loads.
 *
 * Usage in a test file:
 *
 *   import { cleanupTestDb, readTestDb } from '../helpers/testDb';
 *
 *   afterAll(() => cleanupTestDb());
 */

import fs from 'fs';

/** Absolute path of the temp DB file assigned to this test file. */
export function testDbPath(): string {
  const dbPath = process.env['DB_PATH'];
  if (!dbPath) {
    throw new Error('DB_PATH is not set — is tests/helpers/jestSetup.ts configured in vi.config.ts?');
  }
  return dbPath;
}

/** Remove the temp file after the test suite finishes. */
export function cleanupTestDb(): void {
  fs.rmSync(testDbPath(), { force: true });
}

/** Read the raw JSON of the current test DB (useful for assertions). */
export function readTestDb(): Record<string, unknown[]> {
  const dbPath = testDbPath();
  if (!fs.existsSync(dbPath)) return {};
  return JSON.parse(fs.readFileSync(dbPath, 'utf-8')) as Record<string, unknown[]>;
}
