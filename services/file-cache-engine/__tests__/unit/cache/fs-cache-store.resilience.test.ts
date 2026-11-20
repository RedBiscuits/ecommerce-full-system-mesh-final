import { randomBytes } from 'node:crypto';
import {
  mkdir,
  readdir,
  readFile,
  rm,
  stat,
  writeFile,
} from 'node:fs/promises';
import { join } from 'node:path';

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { hashKey } from '../../../src/cache/cache-key';
import { CacheIntegrityError } from '../../../src/cache/errors';
import { FsCacheStore } from '../../../src/cache/fs-cache-store';
import type { EntryLifetime } from '../../../src/cache/types';
import { errno } from '../../helpers/faulty-store';
import { makeTempDir, type TempDir } from '../../helpers/temp-dir';

// Fault injection into node:fs/promises, scoped to this file.
const fsHooks = vi.hoisted(() => ({
  /** Errors thrown by the next `write` calls on any file handle, in order. */
  writeErrors: [] as NodeJS.ErrnoException[],
  /** Errors thrown by the next `open` calls, in order. */
  openErrors: [] as NodeJS.ErrnoException[],
  /** Errors thrown by the next `rename` calls, in order. */
  renameErrors: [] as NodeJS.ErrnoException[],
}));

vi.mock('node:fs/promises', async (importOriginal) => {
  const actual = await importOriginal<typeof import('node:fs/promises')>();
  return {
    ...actual,
    open: async (
      ...args: Parameters<typeof actual.open>
    ): ReturnType<typeof actual.open> => {
      const error = fsHooks.openErrors.shift();
      if (error !== undefined) {
        throw error;
      }
      const handle = await actual.open(...args);
      const originalWritev = handle.writev.bind(handle);
      // The store lands data with a single writev; inject failures there.
      handle.writev = (async (...writeArgs: unknown[]) => {
        const writeError = fsHooks.writeErrors.shift();
        if (writeError !== undefined) {
          throw writeError;
        }
        return Reflect.apply(originalWritev, handle, writeArgs);
      }) as typeof handle.writev;
      const originalWrite = handle.write.bind(handle);
      // Only intercept data writes (the store's writeFully path).
      handle.write = (async (...writeArgs: unknown[]) => {
        const writeError = fsHooks.writeErrors.shift();
        if (writeError !== undefined) {
          throw writeError;
        }
        return Reflect.apply(originalWrite, handle, writeArgs);
      }) as typeof handle.write;
      return handle;
    },
    rename: async (
      ...args: Parameters<typeof actual.rename>
    ): ReturnType<typeof actual.rename> => {
      const error = fsHooks.renameErrors.shift();
      if (error !== undefined) {
        throw error;
      }
      return actual.rename(...args);
    },
  };
});

const lifetime: EntryLifetime = {
  createdAt: 1000,
  expiresAt: 2000,
  staleUntil: 3000,
};

describe('FsCacheStore – resilience & edge cases', () => {
  let dir: TempDir;
  let store: FsCacheStore;
  let corruptions: CacheIntegrityError[];

  beforeEach(async () => {
    dir = await makeTempDir();
    corruptions = [];
    store = await FsCacheStore.open({
      directory: dir.path,
      fsync: false,
      onCorruptEntry: (error) => corruptions.push(error),
    });
  });

  afterEach(async () => {
    fsHooks.writeErrors.length = 0;
    fsHooks.openErrors.length = 0;
    fsHooks.renameErrors.length = 0;
    await dir.cleanup();
  });

  const entryPath = (key: string): string => {
    const hash = hashKey(key);
    return join(store.entriesDirectory, hash.slice(0, 2), `${hash}.cache`);
  };

  const tmpFiles = (): Promise<string[]> => readdir(join(dir.path, 'tmp'));

  describe('corruption matrix – every variant is quarantined and reported as a miss', () => {
    it.each([
      ['empty file', (): Buffer => Buffer.alloc(0)],
      [
        'zero-filled file',
        (bytes: Buffer): Buffer => Buffer.alloc(bytes.length),
      ],
      [
        'flipped magic byte',
        (bytes: Buffer): Buffer => {
          const b = Buffer.from(bytes);
          b[0] = b[0]! ^ 0xff;
          return b;
        },
      ],
      [
        'flipped header byte',
        (bytes: Buffer): Buffer => {
          const b = Buffer.from(bytes);
          b[12] = b[12]! ^ 0xff;
          return b;
        },
      ],
      [
        'flipped payload byte',
        (bytes: Buffer): Buffer => {
          const b = Buffer.from(bytes);
          b[b.length - 1] = b[b.length - 1]! ^ 0xff;
          return b;
        },
      ],
      [
        'truncated inside the preamble',
        (bytes: Buffer): Buffer => bytes.subarray(0, 5),
      ],
      [
        'truncated inside the header',
        (bytes: Buffer): Buffer => bytes.subarray(0, 30),
      ],
      [
        'truncated payload',
        (bytes: Buffer): Buffer => bytes.subarray(0, bytes.length - 3),
      ],
      [
        'trailing garbage',
        (bytes: Buffer): Buffer => Buffer.concat([bytes, Buffer.from('junk')]),
      ],
      [
        'header length pointing past EOF',
        (bytes: Buffer): Buffer => {
          const b = Buffer.from(bytes);
          b.writeUInt32BE(100_000, 4);
          return b;
        },
      ],
    ])('%s', async (_label, mutate) => {
      await store.write('k', Buffer.from('payload-bytes'), lifetime);
      const path = entryPath('k');
      await writeFile(path, mutate(await readFile(path)));

      await expect(store.read('k')).resolves.toBeUndefined();
      expect(corruptions).toHaveLength(1);
      await expect(stat(path)).rejects.toMatchObject({ code: 'ENOENT' });

      // The slot is immediately reusable.
      await store.write('k', Buffer.from('fresh'), lifetime);
      expect((await store.read('k'))?.value.toString()).toBe('fresh');
    });

    it('quarantines a directory sitting where an entry should be', async () => {
      const path = entryPath('k');
      await mkdir(path, { recursive: true });
      await writeFile(join(path, 'nested'), 'x');

      await expect(store.read('k')).resolves.toBeUndefined();
      expect(corruptions[0]?.message).toContain('directory');
      await expect(stat(path)).rejects.toMatchObject({ code: 'ENOENT' });
      await store.write('k', Buffer.from('fresh'), lifetime);
      expect((await store.read('k'))?.value.toString()).toBe('fresh');
    });

    it('skips corrupt files during scan without aborting it', async () => {
      await store.write('good-1', Buffer.from('a'), lifetime);
      await store.write('bad', Buffer.from('b'), lifetime);
      await store.write('good-2', Buffer.from('c'), lifetime);
      await writeFile(entryPath('bad'), 'garbage');

      const keys: string[] = [];
      for await (const metadata of store.scan()) {
        keys.push(metadata.key);
      }
      expect(keys.sort()).toEqual(['good-1', 'good-2']);
      expect(corruptions).toHaveLength(1);
    });

    it('a header claiming a different key is treated as corruption', async () => {
      await store.write('a', Buffer.from('a'), lifetime);
      await store.write('b', Buffer.from('b'), lifetime);
      // Copy b's file over a's slot: valid file, wrong key.
      await writeFile(entryPath('a'), await readFile(entryPath('b')));
      await expect(store.read('a')).resolves.toBeUndefined();
      expect(corruptions[0]?.message).toContain('does not match');
    });
  });

  describe('disk full / I/O failures during write', () => {
    it('ENOSPC while writing leaves no temp file and no partial entry', async () => {
      await store.write('k', Buffer.from('old'), lifetime);
      fsHooks.writeErrors.push(errno('ENOSPC'));

      await expect(
        store.write('k', Buffer.from('new'), lifetime),
      ).rejects.toMatchObject({ code: 'ENOSPC' });
      await expect(tmpFiles()).resolves.toEqual([]);
      // The previous entry is untouched: atomic replace never half-applies.
      expect((await store.read('k'))?.value.toString()).toBe('old');
    });

    it('EIO on rename leaves the old entry intact and cleans the temp file', async () => {
      await store.write('k', Buffer.from('old'), lifetime);
      fsHooks.renameErrors.push(errno('EIO'));
      await expect(
        store.write('k', Buffer.from('new'), lifetime),
      ).rejects.toMatchObject({ code: 'EIO' });
      await expect(tmpFiles()).resolves.toEqual([]);
      expect((await store.read('k'))?.value.toString()).toBe('old');
    });

    it('propagates EMFILE from open so a resilient wrapper can retry it', async () => {
      fsHooks.openErrors.push(errno('EMFILE'));
      await expect(store.read('k')).rejects.toMatchObject({ code: 'EMFILE' });
      await expect(store.read('k')).resolves.toBeUndefined();
    });
  });

  describe('layout removed underneath the store', () => {
    it('recreates the directory tree and completes the write', async () => {
      await store.write('k', Buffer.from('v1'), lifetime);
      await rm(dir.path, { recursive: true, force: true });

      await expect(
        store.write('k', Buffer.from('v2'), lifetime),
      ).resolves.toBeDefined();
      expect((await store.read('k'))?.value.toString()).toBe('v2');
      await expect(tmpFiles()).resolves.toEqual([]);
    });

    it('reads, deletes and scans on a missing tree behave as empty', async () => {
      await rm(dir.path, { recursive: true, force: true });
      await expect(store.read('k')).resolves.toBeUndefined();
      await expect(store.delete('k')).resolves.toBe(false);
      await expect(store.deleteIf('k', () => true)).resolves.toBe(false);
      const seen = [];
      for await (const m of store.scan()) {
        seen.push(m);
      }
      expect(seen).toEqual([]);
    });

    it('a concurrent clear() does not break in-flight writes', async () => {
      const writes = Array.from({ length: 20 }, (_, i) =>
        store.write(`k${i}`, Buffer.from(`v${i}`), lifetime),
      );
      await store.clear();
      await expect(Promise.all(writes)).resolves.toHaveLength(20);
      await expect(tmpFiles()).resolves.toEqual([]);
      for await (const m of store.scan()) {
        expect((await store.read(m.key))?.value.toString()).toBe(
          m.key.replace('k', 'v'),
        );
      }
    });
  });

  describe('deleteIf (conditional delete)', () => {
    it('evaluates the predicate against the current header under the key lock', async () => {
      await store.write('k', Buffer.from('v'), lifetime);
      await expect(store.deleteIf('k', (m) => m.expiresAt < 0)).resolves.toBe(
        false,
      );
      await expect(store.read('k')).resolves.toBeDefined();
      await expect(
        store.deleteIf('k', (m) => m.expiresAt === 2000),
      ).resolves.toBe(true);
      await expect(store.read('k')).resolves.toBeUndefined();
      await expect(store.deleteIf('k', () => true)).resolves.toBe(false);
    });

    it('never removes an entry rewritten after the decision was made (TOCTOU)', async () => {
      await store.write('k', Buffer.from('cold'), {
        createdAt: 0,
        expiresAt: 1,
        staleUntil: 2,
      });
      // A sweeper decided this entry is cold, but a writer wins the lock first.
      const rewrite = store.write('k', Buffer.from('hot'), {
        createdAt: 10,
        expiresAt: 1e12,
        staleUntil: 1e12,
      });
      const sweep = store.deleteIf('k', (m) => m.staleUntil < 1e6);
      await Promise.all([rewrite, sweep]);
      expect((await store.read('k'))?.value.toString()).toBe('hot');
    });
  });

  describe('concurrency chaos', () => {
    it('random interleaved reads/writes/deletes/scans keep every invariant', async () => {
      const keys = ['a', 'b', 'c', 'd'];
      for (const key of keys) {
        await store.write(key, randomBytes(32), lifetime);
      }
      const ops: Promise<void>[] = [];
      const violations: string[] = [];
      let reads = 0;
      let validReads = 0;

      const checkEntry = (
        entry:
          | { value: Buffer; metadata: { size: number; key: string } }
          | undefined,
      ): void => {
        if (entry === undefined) {
          return;
        }
        validReads += 1;
        // Any observed value must be a complete write, never a blend.
        if (entry.value.length !== entry.metadata.size) {
          violations.push(`torn read for ${entry.metadata.key}`);
        }
      };

      for (let i = 0; i < 300; i += 1) {
        const key = keys[i % keys.length]!;
        switch (i % 5) {
          case 0:
          case 1: {
            const value = randomBytes(1 + (i % 64));
            ops.push(store.write(key, value, lifetime).then(() => undefined));
            break;
          }
          case 2:
            ops.push(
              store.read(key).then((entry) => {
                reads += 1;
                checkEntry(entry);
              }),
            );
            break;
          case 3:
            ops.push(
              store.deleteIf(key, () => i % 2 === 0).then(() => undefined),
            );
            break;
          default:
            ops.push(
              (async (): Promise<void> => {
                for await (const m of store.scan()) {
                  if (!keys.includes(m.key)) {
                    violations.push(`unexpected key ${m.key}`);
                  }
                }
              })(),
            );
        }
      }

      await Promise.all(ops);
      for (const key of keys) {
        checkEntry(await store.read(key));
      }
      expect(reads).toBeGreaterThan(0);
      expect(validReads).toBeGreaterThan(0);
      expect(violations).toEqual([]);
      expect(corruptions).toEqual([]);
      await expect(tmpFiles()).resolves.toEqual([]);
    });
  });
});
