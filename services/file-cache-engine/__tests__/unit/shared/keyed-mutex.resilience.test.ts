import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { LockTimeoutError } from '../../../src/resilience/errors';
import { KeyedMutex } from '../../../src/shared/keyed-mutex';

describe('KeyedMutex – timeouts, aborts and deadlocks', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('rejects a waiter after timeoutMs without blocking later waiters', async () => {
    const mutex = new KeyedMutex();
    const gate = Promise.withResolvers<void>();
    const holder = mutex.runExclusive('k', () => gate.promise);

    const impatient = mutex
      .runExclusive('k', async () => 'ran', { timeoutMs: 100 })
      .then(
        () => 'resolved',
        (e: unknown) => e,
      );
    const patient = mutex.runExclusive('k', async () => 'patient ran');

    await vi.advanceTimersByTimeAsync(100);
    await expect(impatient).resolves.toBeInstanceOf(LockTimeoutError);

    gate.resolve();
    await holder;
    await expect(patient).resolves.toBe('patient ran');
    expect(mutex.size).toBe(0);
  });

  it('a re-entrant task (self-deadlock) is broken by the timeout instead of hanging', async () => {
    const mutex = new KeyedMutex();
    const outcome = mutex
      .runExclusive('k', () =>
        mutex.runExclusive('k', async () => 'inner', { timeoutMs: 50 }),
      )
      .then(
        () => 'resolved',
        (e: unknown) => e,
      );
    await vi.advanceTimersByTimeAsync(50);
    await expect(outcome).resolves.toBeInstanceOf(LockTimeoutError);
    // The outer task finished (by rejecting), so the key is free again.
    await expect(mutex.runExclusive('k', async () => 'free')).resolves.toBe(
      'free',
    );
  });

  it('honours an abort signal while waiting', async () => {
    const mutex = new KeyedMutex();
    const gate = Promise.withResolvers<void>();
    const holder = mutex.runExclusive('k', () => gate.promise);
    const controller = new AbortController();
    const waiting = mutex
      .runExclusive('k', async () => 'ran', { signal: controller.signal })
      .then(
        () => 'resolved',
        (e: unknown) => e,
      );
    controller.abort(new Error('cancelled'));
    await expect(waiting).resolves.toMatchObject({ message: 'cancelled' });
    gate.resolve();
    await holder;
  });

  it('rejects immediately on an already-aborted signal', async () => {
    const mutex = new KeyedMutex();
    const controller = new AbortController();
    controller.abort();
    await expect(
      mutex.runExclusive('k', async () => 'ran', { signal: controller.signal }),
    ).rejects.toMatchObject({ name: 'AbortError' });
  });

  it('keeps FIFO order and cleans up under a burst of mixed outcomes', async () => {
    const mutex = new KeyedMutex();
    const order: number[] = [];
    const tasks = Array.from({ length: 50 }, (_, i) =>
      mutex
        .runExclusive('k', async () => {
          order.push(i);
          if (i % 7 === 0) {
            throw new Error(`task ${i} failed`);
          }
        })
        .catch(() => undefined),
    );
    await Promise.all(tasks);
    expect(order).toEqual(Array.from({ length: 50 }, (_, i) => i));
    expect(mutex.size).toBe(0);
  });
});
