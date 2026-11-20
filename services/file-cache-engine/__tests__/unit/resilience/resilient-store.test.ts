import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import type { EntryLifetime } from '../../../src/cache/types';
import { StoreUnavailableError } from '../../../src/resilience/errors';
import type { ResilienceEvent } from '../../../src/resilience/resilient-store';
import { ResilientCacheStore } from '../../../src/resilience/resilient-store';
import { FakeClock } from '../../helpers/fake-clock';
import { FaultyStore, errno } from '../../helpers/faulty-store';

const lifetime: EntryLifetime = {
  createdAt: 0,
  expiresAt: 1_000,
  staleUntil: 2_000,
};

describe('ResilientCacheStore', () => {
  let inner: FaultyStore;
  let clock: FakeClock;
  let events: ResilienceEvent[];

  const make = (
    options: ConstructorParameters<typeof ResilientCacheStore>[1] = {},
  ): ResilientCacheStore =>
    new ResilientCacheStore(inner, {
      clock,
      random: () => 0.5,
      retry: { jitterRatio: 0, baseDelayMs: 1, maxDelayMs: 1 },
      onEvent: (event): number => events.push(event),
      ...options,
    });

  beforeEach(() => {
    inner = new FaultyStore();
    clock = new FakeClock();
    events = [];
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('is transparent when the inner store is healthy', async () => {
    const store = make();
    await store.write('k', Buffer.from('v'), lifetime, { a: '1' });
    expect((await store.read('k'))?.value.toString()).toBe('v');
    expect((await store.readMetadata('k'))?.attributes).toEqual({ a: '1' });
    await expect(store.deleteIf('k', () => false)).resolves.toBe(false);
    await expect(store.delete('k')).resolves.toBe(true);
    await store.clear();
    expect(events).toEqual([]);
    expect(store.health()).toEqual({
      circuit: expect.objectContaining({ state: 'closed' }),
      lastFailure: undefined,
      lastReclaimAt: undefined,
    });
  });

  it('retries transient errors and reports each retry', async () => {
    inner.failNext('read', errno('EMFILE'), 2);
    const store = make();
    const pending = store.read('k');
    await vi.advanceTimersByTimeAsync(10);
    await expect(pending).resolves.toBeUndefined();
    expect(inner.calls.read).toBe(3);
    expect(events.filter((e) => e.type === 'retry')).toHaveLength(2);
    expect(store.health().circuit?.consecutiveFailures).toBe(0);
  });

  it('wraps final failures in StoreUnavailableError with the classified kind', async () => {
    inner.failNext('write', errno('EROFS'));
    const store = make();
    const error = await store
      .write('k', Buffer.from('v'), lifetime)
      .catch((e: unknown) => e);
    expect(error).toBeInstanceOf(StoreUnavailableError);
    expect(error).toMatchObject({
      operation: 'write',
      kind: 'permission',
      attempts: 1,
    });
    expect((error as StoreUnavailableError).cause).toMatchObject({
      code: 'EROFS',
    });
    expect(inner.calls.write).toBe(1);
    expect(events.at(-1)).toMatchObject({
      type: 'store-failure',
      kind: 'permission',
      key: 'k',
    });
    expect(store.health().lastFailure).toMatchObject({ kind: 'permission' });
  });

  it('opens the circuit after repeated failures and fails fast without touching the store', async () => {
    inner.failNext('read', errno('EIO'), 2);
    const store = make({ circuit: { failureThreshold: 2, cooldownMs: 1_000 } });
    await expect(store.read('a')).rejects.toBeInstanceOf(StoreUnavailableError);
    await expect(store.read('b')).rejects.toBeInstanceOf(StoreUnavailableError);
    expect(store.health().circuit?.state).toBe('open');
    expect(events.some((e) => e.type === 'circuit' && e.state === 'open')).toBe(
      true,
    );

    const callsBefore = inner.calls.read;
    const fast = await store.read('c').catch((e: unknown) => e);
    expect(fast).toMatchObject({ kind: 'transient' });
    expect((fast as StoreUnavailableError).cause).toMatchObject({
      retryAfterMs: 1_000,
    });
    expect(inner.calls.read).toBe(callsBefore);

    // After the cooldown one probe goes through; its success closes the circuit.
    clock.advance(1_000);
    await expect(store.read('c')).resolves.toBeUndefined();
    expect(inner.calls.read).toBe(callsBefore + 1);
    expect(store.health().circuit?.state).toBe('closed');
    expect(events.at(-1)).toMatchObject({ type: 'circuit', state: 'closed' });
  });

  it('reclaims space on ENOSPC and retries the write once', async () => {
    inner.failNext('write', errno('ENOSPC'));
    const reclaim = vi.fn<() => Promise<void>>(async () => undefined);
    const store = make({ onCapacityExhausted: reclaim });

    const metadata = await store.write('k', Buffer.from('v'), lifetime);
    expect(metadata.key).toBe('k');
    expect(reclaim).toHaveBeenCalledOnce();
    expect(inner.calls.write).toBe(2);
    expect(events).toContainEqual(
      expect.objectContaining({ type: 'capacity-exhausted', reclaimed: true }),
    );
  });

  it('fails with kind=capacity when no reclaim is configured or it does not help', async () => {
    inner.failNext('write', errno('ENOSPC'), 2);
    const store = make({
      onCapacityExhausted: async () => undefined,
      reclaimCooldownMs: 0,
    });
    await expect(
      store.write('k', Buffer.from('v'), lifetime),
    ).rejects.toMatchObject({ kind: 'capacity' });

    inner.failNext('write', errno('EDQUOT'));
    const bare = make();
    await expect(
      bare.write('k', Buffer.from('v'), lifetime),
    ).rejects.toMatchObject({ kind: 'capacity' });
    expect(
      events.filter((e) => e.type === 'capacity-exhausted' && !e.reclaimed),
    ).toHaveLength(1);
  });

  it('rate-limits reclaims and shares one in-flight reclaim across writers', async () => {
    const gate = Promise.withResolvers<void>();
    const reclaim = vi.fn<() => Promise<void>>(() => gate.promise);
    const store = make({
      onCapacityExhausted: reclaim,
      reclaimCooldownMs: 5_000,
    });

    inner.failNext('write', errno('ENOSPC'), 2);
    const a = store.write('a', Buffer.from('v'), lifetime);
    const b = store.write('b', Buffer.from('v'), lifetime);
    await vi.advanceTimersByTimeAsync(1);
    expect(reclaim).toHaveBeenCalledOnce();
    gate.resolve();
    await expect(a).resolves.toBeDefined();
    await expect(b).resolves.toBeDefined();

    // Within the cooldown, a further ENOSPC is not allowed to reclaim again.
    inner.failNext('write', errno('ENOSPC'));
    await expect(
      store.write('c', Buffer.from('v'), lifetime),
    ).rejects.toMatchObject({ kind: 'capacity' });
    expect(reclaim).toHaveBeenCalledOnce();

    clock.advance(5_000);
    inner.failNext('write', errno('ENOSPC'));
    await expect(
      store.write('d', Buffer.from('v'), lifetime),
    ).resolves.toBeDefined();
    expect(reclaim).toHaveBeenCalledTimes(2);
  });

  it('passes scan through untouched and exposes the inner store', async () => {
    const store = make();
    await inner.write('k', Buffer.from('v'), lifetime);
    const keys = [];
    for await (const m of store.scan()) {
      keys.push(m.key);
    }
    expect(keys).toEqual(['k']);
    expect(store.inner).toBe(inner);
  });

  it('can run without a circuit breaker', async () => {
    inner.failNext('read', errno('EIO'), 5);
    const store = make({ circuit: false });
    for (let i = 0; i < 5; i += 1) {
      await expect(store.read('k')).rejects.toBeInstanceOf(
        StoreUnavailableError,
      );
    }
    expect(inner.calls.read).toBe(5);
    expect(store.health().circuit).toBeUndefined();
  });
});
