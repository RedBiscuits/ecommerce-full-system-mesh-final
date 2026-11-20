import { randomBytes } from 'node:crypto';
import { readdir, readFile, stat, writeFile } from 'node:fs/promises';
import { join } from 'node:path';

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

// Hooks that let individual tests inject failures into `node:fs/promises`
// without touching the store's real implementation.
const fsHooks = vi.hoisted(() => ({
  renameError: undefined as Error | undefined,
  onSync: undefined as (() => void) | undefined,
}));

vi.mock('node:fs/promises', async (importOriginal) => {
  const actual = await importOriginal<typeof import('node:fs/promises')>();
  return {
    ...actual,
    rename: async (
      ...args: Parameters<typeof actual.rename>
    ): ReturnType<typeof actual.rename> => {
      if (fsHooks.renameError !== undefined) {
        const error = fsHooks.renameError;
        fsHooks.renameError = undefined;
        throw error;
      }
      return actual.rename(...args);
    },
    open: async (
      ...args: Parameters<typeof actual.open>
    ): ReturnType<typeof actual.open> => {
      const handle = await actual.open(...args);
      if (fsHooks.onSync !== undefined) {
        const originalSync = handle.sync.bind(handle);
        handle.sync = async (): Promise<void> => {
          fsHooks.onSync?.();
          await originalSync();
        };
      }
      return handle;
    },
  };
});

import { hashKey } from '../../../src/cache/cache-key';
import { CacheIntegrityError } from '../../../src/cache/errors';
import { FsCacheStore } from '../../../src/cache/fs-cache-store';
import type { EntryLifetime } from '../../../src/cache/types';
import { makeTempDir, type TempDir } from '../../helpers/temp-dir';

const lifetime: EntryLifetime = {
  createdAt: 1000,
  expiresAt: 2000,
  staleUntil: 3000,
};

describe('FsCacheStore', () => {
  let dir: TempDir;
  let store: FsCacheStore;

  beforeEach(async () => {
    dir = await makeTempDir();
    store = await FsCacheStore.open({ directory: dir.path });
  });

  afterEach(async () => {
    fsHooks.renameError = undefined;
    fsHooks.onSync = undefined;
    await dir.cleanup();
  });

  const entryPath = (key: string): string => {
    const hash = hashKey(key);
    return join(store.entriesDirectory, hash.slice(0, 2), `${hash}.cache`);
  };

  describe('read / write', () => {
    it('round-trips arbitrary binary payloads with metadata', async () => {
      const payload = randomBytes(64 * 1024);
      const written = await store.write('bin', payload, lifetime, {
        'content-type': 'application/octet-stream',
      });

      const entry = await store.read('bin');
      expect(entry).toBeDefined();
      expect(entry?.value.equals(payload)).toBe(true);
      expect(entry?.metadata).toEqual(written);
      expect(written).toMatchObject({
        key: 'bin',
        ...lifetime,
        size: payload.length,
        attributes: { 'content-type': 'application/octet-stream' },
      });
      expect(written.checksum).toMatch(/^sha256:[0-9a-f]{64}$/);
    });

    it('supports empty payloads', async () => {
      await store.write('empty', new Uint8Array(0), lifetime);
      const entry = await store.read('empty');
      expect(entry?.value.length).toBe(0);
    });

    it('reports a miss for unknown keys', async () => {
      await expect(store.read('nope')).resolves.toBeUndefined();
      await expect(store.readMetadata('nope')).resolves.toBeUndefined();
    });

    it('replaces an existing entry in place', async () => {
      await store.write('k', Buffer.from('v1'), lifetime);
      await store.write('k', Buffer.from('v2'), lifetime);
      expect((await store.read('k'))?.value.toString()).toBe('v2');
    });

    it('readMetadata returns the header without the payload', async () => {
      const metadata = await store.write('k', Buffer.from('hello'), lifetime);
      await expect(store.readMetadata('k')).resolves.toEqual(metadata);
    });

    it('stores files without the executable bit and readable only by owner', async () => {
      if (process.platform === 'win32') {
        return;
      }
      await store.write('k', Buffer.from('#!/bin/sh\necho pwned'), lifetime);
      const { mode } = await stat(entryPath('k'));
      expect(mode & 0o777).toBe(0o600);
    });

    it('hashes keys so hostile keys cannot escape the cache directory', async () => {
      const hostile = '../../escape';
      await store.write(hostile, Buffer.from('x'), lifetime);
      const files = await readdir(store.entriesDirectory, { recursive: true });
      expect(files.some((f) => f.endsWith('.cache'))).toBe(true);
      expect(files.every((f) => !f.includes('..'))).toBe(true);
      expect((await store.read(hostile))?.value.toString()).toBe('x');
    });
  });

  describe('atomicity', () => {
    it('leaves no final file and no temp file when publishing fails', async () => {
      fsHooks.renameError = new Error('disk on fire');

      await expect(
        store.write('k', Buffer.from('v'), lifetime),
      ).rejects.toThrow('disk on fire');

      await expect(store.read('k')).resolves.toBeUndefined();
      await expect(readdir(join(dir.path, 'tmp'))).resolves.toEqual([]);
    });

    it('never exposes a partially written entry to concurrent readers', async () => {
      const big = randomBytes(2 * 1024 * 1024);
      const small = Buffer.from('small');
      await store.write('k', small, lifetime);

      const writing = store.write('k', big, lifetime);
      const reads: Promise<number | undefined>[] = [];
      for (let i = 0; i < 20; i += 1) {
        reads.push(store.read('k').then((e) => e?.value.length));
      }
      await writing;

      for (const length of await Promise.all(reads)) {
        expect([small.length, big.length]).toContain(length);
      }
    });

    it('serialises concurrent writes for the same key and ends consistent', async () => {
      await Promise.all(
        Array.from({ length: 10 }, (_, i) =>
          store.write('k', Buffer.from(`value-${i}`), lifetime),
        ),
      );
      const entry = await store.read('k');
      expect(entry?.value.toString()).toMatch(/^value-\d$/);
      await expect(readdir(join(dir.path, 'tmp'))).resolves.toEqual([]);
    });

    it('purges orphaned temp files left by a crashed writer on open', async () => {
      const orphan = join(dir.path, 'tmp', 'deadbeef.000000.tmp');
      await writeFile(orphan, 'partial');
      await FsCacheStore.open({ directory: dir.path });
      await expect(readdir(join(dir.path, 'tmp'))).resolves.toEqual([]);
    });
  });

  describe('integrity (self-healing)', () => {
    it('discards an entry whose payload was tampered with', async () => {
      const onCorruptEntry = vi.fn<(error: CacheIntegrityError) => void>();
      store = await FsCacheStore.open({
        directory: dir.path,
        onCorruptEntry,
      });
      await store.write('k', Buffer.from('original'), lifetime);

      const path = entryPath('k');
      const bytes = await readFile(path);
      bytes[bytes.length - 1] = bytes[bytes.length - 1]! ^ 0xff;
      await writeFile(path, bytes);

      await expect(store.read('k')).resolves.toBeUndefined();
      expect(onCorruptEntry).toHaveBeenCalledOnce();
      expect(onCorruptEntry.mock.calls[0]?.[0]).toBeInstanceOf(
        CacheIntegrityError,
      );
      expect(onCorruptEntry.mock.calls[0]?.[0]).toMatchObject({
        message: expect.stringContaining('checksum mismatch'),
      });
      await expect(stat(path)).rejects.toMatchObject({ code: 'ENOENT' });
    });

    it('discards a truncated entry', async () => {
      await store.write('k', Buffer.from('0123456789'), lifetime);
      const path = entryPath('k');
      const bytes = await readFile(path);
      await writeFile(path, bytes.subarray(0, bytes.length - 4));

      await expect(store.read('k')).resolves.toBeUndefined();
      await expect(stat(path)).rejects.toMatchObject({ code: 'ENOENT' });
    });

    it('discards garbage files that are not cache entries', async () => {
      const path = entryPath('k');
      await store.write('k', Buffer.from('x'), lifetime);
      await writeFile(path, 'definitely not a cache file');

      await expect(store.readMetadata('k')).resolves.toBeUndefined();
      await expect(stat(path)).rejects.toMatchObject({ code: 'ENOENT' });
    });

    it('skips checksum verification when disabled', async () => {
      store = await FsCacheStore.open({
        directory: dir.path,
        verifyChecksum: false,
      });
      await store.write('k', Buffer.from('original'), lifetime);
      const path = entryPath('k');
      const bytes = await readFile(path);
      bytes[bytes.length - 1] = bytes[bytes.length - 1]! ^ 0xff;
      await writeFile(path, bytes);

      await expect(store.read('k')).resolves.toBeDefined();
    });
  });

  describe('delete / clear / scan', () => {
    it('delete is idempotent', async () => {
      await store.write('k', Buffer.from('v'), lifetime);
      await expect(store.delete('k')).resolves.toBe(true);
      await expect(store.delete('k')).resolves.toBe(false);
      await expect(store.read('k')).resolves.toBeUndefined();
    });

    it('clear removes every entry and keeps the store usable', async () => {
      await store.write('a', Buffer.from('1'), lifetime);
      await store.write('b', Buffer.from('2'), lifetime);
      await store.clear();
      await expect(store.read('a')).resolves.toBeUndefined();
      await expect(store.read('b')).resolves.toBeUndefined();
      await store.write('c', Buffer.from('3'), lifetime);
      expect((await store.read('c'))?.value.toString()).toBe('3');
    });

    it('scan yields metadata for every entry without loading payloads', async () => {
      const keys = ['a', 'b', 'c', 'd'];
      for (const key of keys) {
        await store.write(key, randomBytes(32), lifetime);
      }
      const seen: string[] = [];
      for await (const metadata of store.scan()) {
        seen.push(metadata.key);
        expect(metadata.size).toBe(32);
      }
      expect(seen.sort()).toEqual(keys);
    });

    it('scan on an empty store yields nothing', async () => {
      const items = [];
      for await (const m of store.scan()) {
        items.push(m);
      }
      expect(items).toEqual([]);
    });
  });

  describe('durability toggles', () => {
    it('fsyncs file and directory by default and skips when disabled', async () => {
      const syncSpy = vi.fn<() => void>();
      fsHooks.onSync = syncSpy;

      await store.write('k', Buffer.from('v'), lifetime);
      // One for the temp file, one for the shard directory.
      expect(syncSpy).toHaveBeenCalledTimes(2);

      syncSpy.mockClear();
      const lazy = await FsCacheStore.open({
        directory: dir.path,
        fsync: false,
      });
      await lazy.write('k', Buffer.from('v'), lifetime);
      expect(syncSpy).not.toHaveBeenCalled();
    });
  });
});

describe('FsCacheStore – purgeTempFiles option', () => {
  it('keeps orphaned temp files when asked to observe only', async () => {
    const dir = await makeTempDir();
    await FsCacheStore.open({ directory: dir.path });
    await writeFile(join(dir.path, 'tmp', 'orphan.000000.tmp'), 'partial');
    await FsCacheStore.open({ directory: dir.path, purgeTempFiles: false });
    await expect(readdir(join(dir.path, 'tmp'))).resolves.toEqual([
      'orphan.000000.tmp',
    ]);
    await FsCacheStore.open({ directory: dir.path });
    await expect(readdir(join(dir.path, 'tmp'))).resolves.toEqual([]);
    await dir.cleanup();
  });
});
