import { describe, expect, it } from 'vitest';

import { KeyedRateLimiter } from '../../../src/rate-limit/keyed-rate-limiter';
import { FakeClock } from '../../helpers/fake-clock';

describe('KeyedRateLimiter', () => {
  it('keeps an independent bucket per key', () => {
    const limiter = new KeyedRateLimiter({ capacity: 1, refillPerSecond: 1 });
    expect(limiter.tryAcquire('alice')).toBe(true);
    expect(limiter.tryAcquire('alice')).toBe(false);
    expect(limiter.tryAcquire('bob')).toBe(true);
    expect(limiter.size).toBe(2);
  });

  it('returns the same limiter instance for a key', () => {
    const limiter = new KeyedRateLimiter({ capacity: 1, refillPerSecond: 1 });
    expect(limiter.limiter('k')).toBe(limiter.limiter('k'));
  });

  it('prunes buckets idle for longer than idleTtlMs', () => {
    const clock = new FakeClock();
    const limiter = new KeyedRateLimiter({
      capacity: 1,
      refillPerSecond: 1,
      idleTtlMs: 1_000,
      clock,
    });
    limiter.tryAcquire('old');
    clock.advance(500);
    limiter.tryAcquire('recent');
    clock.advance(600);

    expect(limiter.prune()).toBe(1);
    expect(limiter.size).toBe(1);
    expect(limiter.limiter('recent')).toBeDefined();
    expect(limiter.size).toBe(1);
  });

  it('prunes lazily on access once the prune interval has elapsed', () => {
    const clock = new FakeClock();
    const limiter = new KeyedRateLimiter({
      capacity: 1,
      refillPerSecond: 1,
      idleTtlMs: 1_000,
      pruneIntervalMs: 1_000,
      clock,
    });
    limiter.tryAcquire('a');
    clock.advance(1_000);
    limiter.tryAcquire('b');
    expect(limiter.size).toBe(1);
  });

  it('acquire delegates to the per-key bucket', async () => {
    const limiter = new KeyedRateLimiter({ capacity: 2, refillPerSecond: 1 });
    await expect(limiter.acquire('k', 2)).resolves.toBeUndefined();
    expect(limiter.tryAcquire('k')).toBe(false);
  });
});

describe('KeyedRateLimiter – key floods', () => {
  it('caps tracked keys and evicts the least recently used idle bucket', () => {
    const limiter = new KeyedRateLimiter({
      capacity: 1,
      refillPerSecond: 1,
      maxKeys: 3,
    });
    limiter.tryAcquire('a');
    limiter.tryAcquire('b');
    limiter.tryAcquire('c');
    limiter.tryAcquire('a'); // touch a → b is now least recently used
    limiter.tryAcquire('d');
    expect(limiter.size).toBe(3);
    // b was evicted: a fresh bucket admits it again; a is still drained.
    expect(limiter.tryAcquire('b')).toBe(true);
    expect(limiter.tryAcquire('a')).toBe(false);
  });

  it('never evicts a bucket that has callers waiting', async () => {
    // Time is frozen: with a real clock, 1 ms between the two acquires
    // refills the token, the second acquire never queues, and 'busy' is
    // (correctly) evicted as idle — which made this test flaky.
    const clock = new FakeClock();
    const limiter = new KeyedRateLimiter({
      capacity: 1,
      refillPerSecond: 1000,
      maxKeys: 1,
      clock,
    });
    await limiter.acquire('busy');
    const waiting = limiter.acquire('busy'); // queued on 'busy'
    expect(limiter.limiter('busy').queueLength).toBe(1);
    limiter.tryAcquire('other'); // would need to evict 'busy' — must not
    expect(limiter.size).toBe(2);
    clock.advance(1); // refill one token so the queued caller is released
    await waiting;
  });

  it('bounds memory under a flood of never-repeating keys', () => {
    const limiter = new KeyedRateLimiter({
      capacity: 1,
      refillPerSecond: 1,
      maxKeys: 100,
    });
    for (let i = 0; i < 10_000; i += 1) {
      limiter.tryAcquire(`203.0.113.${i}`);
    }
    expect(limiter.size).toBe(100);
  });
});
