import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { RateLimitExceededError } from '../../../src/rate-limit/errors';
import { TokenBucketRateLimiter } from '../../../src/rate-limit/token-bucket';

describe('TokenBucketRateLimiter', () => {
  beforeEach(() => {
    // Fake timers also fake Date.now(), which the default clock reads lazily.
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('allows a burst up to capacity and then refuses', () => {
    const limiter = new TokenBucketRateLimiter({
      capacity: 3,
      refillPerSecond: 1,
    });
    expect(limiter.tryAcquire()).toBe(true);
    expect(limiter.tryAcquire()).toBe(true);
    expect(limiter.tryAcquire()).toBe(true);
    expect(limiter.tryAcquire()).toBe(false);
    expect(limiter.availableTokens).toBe(0);
  });

  it('refills continuously over time, capped at capacity', () => {
    const limiter = new TokenBucketRateLimiter({
      capacity: 2,
      refillPerSecond: 2,
    });
    expect(limiter.tryAcquire(2)).toBe(true);
    vi.advanceTimersByTime(500);
    expect(limiter.availableTokens).toBeCloseTo(1);
    expect(limiter.tryAcquire(1)).toBe(true);
    vi.advanceTimersByTime(10_000);
    expect(limiter.availableTokens).toBe(2);
  });

  it('acquire waits for refill and serves waiters in FIFO order', async () => {
    const limiter = new TokenBucketRateLimiter({
      capacity: 1,
      refillPerSecond: 10, // one token per 100 ms
    });
    const order: string[] = [];

    await limiter.acquire(); // drains the bucket
    const a = limiter.acquire().then(() => order.push('a'));
    const b = limiter.acquire().then(() => order.push('b'));
    expect(limiter.queueLength).toBe(2);

    await vi.advanceTimersByTimeAsync(100);
    expect(order).toEqual(['a']);
    await vi.advanceTimersByTimeAsync(100);
    expect(order).toEqual(['a', 'b']);
    await Promise.all([a, b]);
    expect(limiter.queueLength).toBe(0);
  });

  it('does not let newcomers jump a non-empty queue', async () => {
    const limiter = new TokenBucketRateLimiter({
      capacity: 1,
      refillPerSecond: 10,
    });
    await limiter.acquire();
    const waiting = limiter.acquire();
    vi.advanceTimersByTime(100);
    // A token is available now, but it belongs to the queued caller.
    expect(limiter.tryAcquire()).toBe(false);
    await vi.advanceTimersByTimeAsync(0);
    await waiting;
  });

  it('rejects with timeout when capacity does not arrive in time', async () => {
    const limiter = new TokenBucketRateLimiter({
      capacity: 1,
      refillPerSecond: 1,
    });
    await limiter.acquire();
    // Attach the handler before advancing so the rejection is never unhandled.
    const outcome = limiter.acquire(1, { timeoutMs: 200 }).then(
      () => 'resolved',
      (error: unknown) => error,
    );
    await vi.advanceTimersByTimeAsync(200);
    await expect(outcome).resolves.toMatchObject({
      name: 'RateLimitExceededError',
      reason: 'timeout',
      retryAfterMs: expect.any(Number),
    });
    expect(limiter.queueLength).toBe(0);
  });

  it('rejects when aborted, before and during the wait', async () => {
    const limiter = new TokenBucketRateLimiter({
      capacity: 1,
      refillPerSecond: 1,
    });
    await limiter.acquire();

    const pre = new AbortController();
    pre.abort();
    await expect(limiter.acquire(1, { signal: pre.signal })).rejects.toThrow(
      RateLimitExceededError,
    );

    const mid = new AbortController();
    const pending = limiter.acquire(1, { signal: mid.signal });
    mid.abort(new Error('client went away'));
    await expect(pending).rejects.toMatchObject({
      reason: 'aborted',
      cause: expect.objectContaining({ message: 'client went away' }),
    });
    expect(limiter.queueLength).toBe(0);
  });

  it('applies back-pressure when the queue is full', async () => {
    const limiter = new TokenBucketRateLimiter({
      capacity: 1,
      refillPerSecond: 1,
      maxQueueSize: 1,
    });
    await limiter.acquire();
    const queued = limiter.acquire();
    await expect(limiter.acquire()).rejects.toMatchObject({
      reason: 'queue-full',
    });
    await vi.advanceTimersByTimeAsync(1_000);
    await queued;
  });

  it('rejects impossible or invalid costs up front', () => {
    const limiter = new TokenBucketRateLimiter({
      capacity: 2,
      refillPerSecond: 1,
    });
    expect(() => limiter.tryAcquire(3)).toThrow(RangeError);
    expect(() => limiter.tryAcquire(0)).toThrow(RangeError);
    expect(() => limiter.tryAcquire(Number.NaN)).toThrow(RangeError);
  });

  it('validates configuration', () => {
    expect(
      () => new TokenBucketRateLimiter({ capacity: 0, refillPerSecond: 1 }),
    ).toThrow(RangeError);
    expect(
      () => new TokenBucketRateLimiter({ capacity: 1, refillPerSecond: 0 }),
    ).toThrow(RangeError);
  });

  it('handles multi-token costs and a cost-aware head-of-line wait', async () => {
    const limiter = new TokenBucketRateLimiter({
      capacity: 5,
      refillPerSecond: 10,
      initialTokens: 0,
    });
    const big = limiter.acquire(5);
    const small = limiter.acquire(1);
    let bigDone = false;
    let smallDone = false;
    void big.then(() => (bigDone = true));
    void small.then(() => (smallDone = true));

    await vi.advanceTimersByTimeAsync(200);
    expect(bigDone).toBe(false);
    expect(smallDone).toBe(false); // FIFO: small waits behind big
    await vi.advanceTimersByTimeAsync(300);
    expect(bigDone).toBe(true);
    await vi.advanceTimersByTimeAsync(100);
    expect(smallDone).toBe(true);
  });
});
