import { describe, expect, it } from 'vitest';

import { KeyedMutex } from '../../../src/shared/keyed-mutex';

const tick = (): Promise<void> => new Promise((r) => setTimeout(r, 0));

describe('KeyedMutex', () => {
  it('serialises tasks for the same key', async () => {
    const mutex = new KeyedMutex();
    const order: string[] = [];

    const first = mutex.runExclusive('k', async () => {
      order.push('first:start');
      await tick();
      order.push('first:end');
    });
    const second = mutex.runExclusive('k', async () => {
      order.push('second:start');
      order.push('second:end');
    });

    await Promise.all([first, second]);
    expect(order).toEqual([
      'first:start',
      'first:end',
      'second:start',
      'second:end',
    ]);
  });

  it('lets different keys run concurrently', async () => {
    const mutex = new KeyedMutex();
    const order: string[] = [];

    const a = mutex.runExclusive('a', async () => {
      order.push('a:start');
      await tick();
      order.push('a:end');
    });
    const b = mutex.runExclusive('b', async () => {
      order.push('b:start');
      order.push('b:end');
    });

    await Promise.all([a, b]);
    expect(order).toEqual(['a:start', 'b:start', 'b:end', 'a:end']);
  });

  it('releases the lock when a task throws and cleans up bookkeeping', async () => {
    const mutex = new KeyedMutex();

    await expect(
      mutex.runExclusive('k', async () => {
        throw new Error('boom');
      }),
    ).rejects.toThrow('boom');

    await expect(mutex.runExclusive('k', async () => 42)).resolves.toBe(42);
    expect(mutex.size).toBe(0);
  });

  it('isLocked reports running and queued tasks, and clears afterwards', async () => {
    const mutex = new KeyedMutex();
    expect(mutex.isLocked('k')).toBe(false);
    const { promise, resolve } = Promise.withResolvers<void>();
    const first = mutex.runExclusive('k', () => promise);
    const second = mutex.runExclusive('k', async () => undefined);
    expect(mutex.isLocked('k')).toBe(true);
    expect(mutex.isLocked('other')).toBe(false);
    resolve();
    await Promise.all([first, second]);
    expect(mutex.isLocked('k')).toBe(false);
  });
});
