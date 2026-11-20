/* eslint-disable @typescript-eslint/no-unused-vars */
import { describe, it, test, expect, beforeEach, afterEach, beforeAll, afterAll, vi } from 'vitest';
/**
 * tests/unit/services/jsonDb.test.ts
 *
 * Unit tests for the JSON DB service.
 * Uses a real temp file (assigned per test file in jestSetup.ts) — no mocking
 * needed, it's pure FS logic.
 */

import fs from 'fs';
import fsPromises from 'fs/promises';
import path from 'path';
import { db } from '../../../src/services/jsonDb';
import type { DbRecord } from '../../../src/services/jsonDb';
import { cleanupTestDb, readTestDb, testDbPath } from '../../helpers/testDb';

interface Doc extends DbRecord {
  [key: string]: unknown;
}

afterAll(() => {
  cleanupTestDb();
});

// ─── insert ───────────────────────────────────────────────────────────────────

describe('db.insert()', () => {
  it('returns the record with _id and _createdAt injected', async () => {
    const record = await db.insert('test_col', { foo: 'bar', num: 42 });

    expect(record._id).toEqual(expect.stringMatching(/^[0-9a-f-]{36}$/));
    expect(record._createdAt).toMatch(/^\d{4}-\d{2}-\d{2}T/);
    expect(record._updatedAt).toBeUndefined();
    expect(record.foo).toBe('bar');
    expect(record.num).toBe(42);
  });

  it('persists data to the JSON file', async () => {
    await db.insert('persisted_col', { x: 1 });
    const raw = readTestDb();
    expect(raw['persisted_col']).toHaveLength(1);
  });

  it('creates the collection if it does not exist', async () => {
    await db.insert('brand_new', { value: 'hello' });
    const all = await db.findAll('brand_new');
    expect(all).toHaveLength(1);
  });

  it('appends to an existing collection', async () => {
    await db.insert('append_col', { n: 1 });
    await db.insert('append_col', { n: 2 });
    await db.insert('append_col', { n: 3 });
    const all = await db.findAll('append_col');
    expect(all).toHaveLength(3);
  });

  it('does not lose writes when inserts run concurrently', async () => {
    const COUNT = 25;
    await Promise.all(
      Array.from({ length: COUNT }, (_, i) => db.insert('concurrent_col', { i })),
    );

    const all = await db.findAll<Doc>('concurrent_col');
    expect(all).toHaveLength(COUNT);
    expect(new Set(all.map((r) => r._id)).size).toBe(COUNT);
    expect(new Set(all.map((r) => r['i'])).size).toBe(COUNT);
  });

  it('writes atomically — no temp files are left behind', async () => {
    await db.insert('atomic_col', { a: 1 });

    const dir = path.dirname(testDbPath());
    const base = path.basename(testDbPath());
    const leftovers = fs.readdirSync(dir).filter((f) => f.startsWith(base) && f.endsWith('.tmp'));
    expect(leftovers).toEqual([]);
  });
});

// ─── update ───────────────────────────────────────────────────────────────────

describe('db.update()', () => {
  it('merges the patch and stamps _updatedAt', async () => {
    const inserted = await db.insert('update_col', { status: 'PENDING', amount: '10.00' });

    const updated = await db.update<Doc>('update_col', inserted._id, { status: 'SUCCESS' });

    expect(updated).not.toBeNull();
    expect(updated?.['status']).toBe('SUCCESS');
    expect(updated?.['amount']).toBe('10.00'); // untouched field preserved
    expect(updated?._updatedAt).toMatch(/^\d{4}-\d{2}-\d{2}T/);
  });

  it('never changes _id or _createdAt', async () => {
    const inserted = await db.insert('update_col', { v: 1 });

    const updated = await db.update<Doc>('update_col', inserted._id, { v: 2 });

    expect(updated?._id).toBe(inserted._id);
    expect(updated?._createdAt).toBe(inserted._createdAt);
  });

  it('persists the update to the JSON file in place (no extra record)', async () => {
    await db.clear('update_inplace');
    const inserted = await db.insert('update_inplace', { v: 1 });

    await db.update<Doc>('update_inplace', inserted._id, { v: 2 });

    const all = await db.findAll<Doc>('update_inplace');
    expect(all).toHaveLength(1);
    expect(all[0]?.['v']).toBe(2);
  });

  it('returns null when the _id does not exist', async () => {
    const result = await db.update<Doc>('update_col', 'nonexistent-uuid', { v: 9 });
    expect(result).toBeNull();
  });

  it('returns null for an unknown collection', async () => {
    const result = await db.update<Doc>('no_such_collection', 'x', { v: 9 });
    expect(result).toBeNull();
  });
});

// ─── findAll ─────────────────────────────────────────────────────────────────

describe('db.findAll()', () => {
  it('returns an empty array for an unknown collection', async () => {
    const result = await db.findAll('does_not_exist_xyz');
    expect(result).toEqual([]);
  });

  it('returns all inserted records', async () => {
    await db.insert('all_test', { label: 'a' });
    await db.insert('all_test', { label: 'b' });
    const result = await db.findAll('all_test');
    expect(result.length).toBeGreaterThanOrEqual(2);
  });
});

// ─── findById ────────────────────────────────────────────────────────────────

describe('db.findById()', () => {
  it('returns the record with a matching _id', async () => {
    const inserted = await db.insert('by_id_col', { name: 'Alice' });
    const found = await db.findById<Doc>('by_id_col', inserted._id);
    expect(found).not.toBeNull();
    expect(found?._id).toBe(inserted._id);
    expect(found?.['name']).toBe('Alice');
  });

  it('returns null when _id is not found', async () => {
    const result = await db.findById('by_id_col', 'nonexistent-uuid');
    expect(result).toBeNull();
  });
});

// ─── findWhere ───────────────────────────────────────────────────────────────

describe('db.findWhere()', () => {
  it('returns only records that match the predicate', async () => {
    await db.insert('where_col', { status: 'SUCCESS' });
    await db.insert('where_col', { status: 'FAILED' });
    await db.insert('where_col', { status: 'SUCCESS' });

    const successes = await db.findWhere<Doc>('where_col', (r) => r['status'] === 'SUCCESS');

    expect(successes.every((r) => r['status'] === 'SUCCESS')).toBe(true);
    expect(successes.length).toBeGreaterThanOrEqual(2);
  });

  it('returns an empty array when nothing matches', async () => {
    const result = await db.findWhere<Doc>('where_col', (r) => r['status'] === 'PENDING');
    expect(result).toEqual([]);
  });
});

// ─── findOne ─────────────────────────────────────────────────────────────────

describe('db.findOne()', () => {
  it('returns the first record matching the predicate', async () => {
    await db.clear('one_col');
    await db.insert('one_col', { key: 'k1', order: 1 });
    await db.insert('one_col', { key: 'k1', order: 2 });
    await db.insert('one_col', { key: 'k2', order: 3 });

    const found = await db.findOne<Doc>('one_col', (r) => r['key'] === 'k1');

    expect(found?.['order']).toBe(1);
  });

  it('returns null when nothing matches', async () => {
    const found = await db.findOne<Doc>('one_col', (r) => r['key'] === 'missing');
    expect(found).toBeNull();
  });

  it('returns null for an unknown collection', async () => {
    const found = await db.findOne<Doc>('nope_col', () => true);
    expect(found).toBeNull();
  });
});

// ─── clear ───────────────────────────────────────────────────────────────────

describe('db.clear()', () => {
  it('removes all records from a collection', async () => {
    await db.insert('clear_col', { a: 1 });
    await db.insert('clear_col', { a: 2 });
    await db.clear('clear_col');
    const result = await db.findAll('clear_col');
    expect(result).toHaveLength(0);
  });
});

// ─── Serialisation ───────────────────────────────────────────────────────────

describe('operation ordering', () => {
  it('runs operations strictly in call order', async () => {
    await db.clear('order_col');

    // Fire without awaiting; the mutex must preserve call order.
    const ops = [
      db.insert('order_col', { seq: 1 }),
      db.insert('order_col', { seq: 2 }),
      db.clear('order_col'),
      db.insert('order_col', { seq: 3 }),
    ];
    await Promise.all(ops);

    const all = await db.findAll<Doc>('order_col');
    expect(all.map((r) => r['seq'])).toEqual([3]);
  });

  it('keeps serving after a failed operation', async () => {
    vi
      .spyOn(fsPromises, 'readFile')
      .mockRejectedValueOnce(Object.assign(new Error('EIO: disk error'), { code: 'EIO' }));

    const failing = db.insert('order_col', { seq: 0 });
    const next = db.insert('order_col', { seq: 4 });

    await expect(failing).rejects.toThrow('EIO');
    await expect(next).resolves.toMatchObject({ seq: 4 });

    vi.restoreAllMocks();
  });
});
