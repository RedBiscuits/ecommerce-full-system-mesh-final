import { describe, expect, it, vi } from 'vitest';

import { CircuitBreaker } from '../../../src/resilience/circuit-breaker';
import { CircuitOpenError } from '../../../src/resilience/errors';
import { FakeClock } from '../../helpers/fake-clock';

const fail = async (): Promise<never> => {
  throw new Error('io');
};
const ok = async (): Promise<string> => 'ok';

describe('CircuitBreaker', () => {
  it('opens after the failure threshold and fails fast during cooldown', async () => {
    const clock = new FakeClock();
    const onStateChange = vi.fn<(s: string, e?: unknown) => void>();
    const breaker = new CircuitBreaker({
      failureThreshold: 3,
      cooldownMs: 1_000,
      clock,
      onStateChange,
    });

    for (let i = 0; i < 3; i += 1) {
      await expect(breaker.execute(fail)).rejects.toThrow('io');
    }
    expect(breaker.state).toBe('open');
    expect(onStateChange).toHaveBeenLastCalledWith('open', expect.any(Error));

    const task = vi.fn<() => Promise<string>>(ok);
    await expect(breaker.execute(task)).rejects.toBeInstanceOf(
      CircuitOpenError,
    );
    await expect(breaker.execute(task)).rejects.toMatchObject({
      retryAfterMs: 1_000,
    });
    expect(task).not.toHaveBeenCalled();
  });

  it('goes half-open after cooldown, closes on a successful probe', async () => {
    const clock = new FakeClock();
    const breaker = new CircuitBreaker({
      failureThreshold: 1,
      cooldownMs: 500,
      clock,
    });
    await expect(breaker.execute(fail)).rejects.toThrow('io');
    clock.advance(500);
    expect(breaker.state).toBe('half-open');

    await expect(breaker.execute(ok)).resolves.toBe('ok');
    expect(breaker.state).toBe('closed');
    expect(breaker.snapshot().consecutiveFailures).toBe(0);
  });

  it('re-opens immediately when the probe fails', async () => {
    const clock = new FakeClock();
    const breaker = new CircuitBreaker({
      failureThreshold: 2,
      cooldownMs: 500,
      clock,
    });
    await expect(breaker.execute(fail)).rejects.toThrow('io');
    await expect(breaker.execute(fail)).rejects.toThrow('io');
    clock.advance(500);
    await expect(breaker.execute(fail)).rejects.toThrow('io');
    expect(breaker.state).toBe('open');
    expect(breaker.snapshot().openedAt).toBe(clock.now());
  });

  it('admits only halfOpenMaxCalls concurrent probes', async () => {
    const clock = new FakeClock();
    const breaker = new CircuitBreaker({
      failureThreshold: 1,
      cooldownMs: 1,
      halfOpenMaxCalls: 1,
      clock,
    });
    await expect(breaker.execute(fail)).rejects.toThrow('io');
    clock.advance(1);

    const gate = Promise.withResolvers<string>();
    const probe = breaker.execute(() => gate.promise);
    await expect(breaker.execute(ok)).rejects.toBeInstanceOf(CircuitOpenError);
    gate.resolve('ok');
    await expect(probe).resolves.toBe('ok');
    expect(breaker.state).toBe('closed');
  });

  it('ignores errors excluded by isFailure and resets on success', async () => {
    const breaker = new CircuitBreaker({
      failureThreshold: 2,
      isFailure: (error): boolean => !(error instanceof RangeError),
    });
    await expect(
      breaker.execute(async () => {
        throw new RangeError('bad arg');
      }),
    ).rejects.toThrow('bad arg');
    await expect(
      breaker.execute(async () => {
        throw new RangeError('bad arg');
      }),
    ).rejects.toThrow('bad arg');
    expect(breaker.state).toBe('closed');

    await expect(breaker.execute(fail)).rejects.toThrow('io');
    await breaker.execute(ok);
    expect(breaker.snapshot().consecutiveFailures).toBe(0);
  });

  it('can be reset by an operator', async () => {
    const breaker = new CircuitBreaker({
      failureThreshold: 1,
      cooldownMs: 60_000,
    });
    await expect(breaker.execute(fail)).rejects.toThrow('io');
    expect(breaker.state).toBe('open');
    breaker.reset();
    expect(breaker.state).toBe('closed');
    await expect(breaker.execute(ok)).resolves.toBe('ok');
  });
});

describe('CircuitBreaker – sliding window, jitter, auto half-open', () => {
  it('only counts a failure streak that fits in the sliding window', async () => {
    const clock = new FakeClock();
    const breaker = new CircuitBreaker({
      failureThreshold: 5,
      windowMs: 10_000,
      clock,
    });
    // Five consecutive failures, but spread over 12 s: never 5 within 10 s.
    for (let i = 0; i < 5; i++) {
      await expect(breaker.execute(fail)).rejects.toThrow('io');
      clock.advance(3_000);
    }
    expect(breaker.state).toBe('closed');
    expect(breaker.snapshot().consecutiveFailures).toBe(4); // 3, 6, 9, 12 s

    // Let the whole streak age out, then fail 5 times within the window.
    clock.advance(11_000);
    for (let i = 0; i < 4; i++) {
      await expect(breaker.execute(fail)).rejects.toThrow('io');
      clock.advance(1_000);
    }
    expect(breaker.state).toBe('closed'); // 4 in the window
    await expect(breaker.execute(fail)).rejects.toThrow('io');
    expect(breaker.state).toBe('open'); // the 5th trips it
  });

  it('a success resets the streak', async () => {
    const breaker = new CircuitBreaker({ failureThreshold: 3 });
    await expect(breaker.execute(fail)).rejects.toThrow('io');
    await expect(breaker.execute(fail)).rejects.toThrow('io');
    await breaker.execute(ok);
    await expect(breaker.execute(fail)).rejects.toThrow('io');
    expect(breaker.state).toBe('closed');
  });

  it.each([
    { random: 0, retryInMs: 30_000 },
    { random: 0.5, retryInMs: 33_000 },
    { random: 0.999, retryInMs: 35_994 },
  ])(
    'adds jitter on top of the cooldown, never before it (random=$random)',
    async ({ random, retryInMs }) => {
      const clock = new FakeClock();
      const breaker = new CircuitBreaker({
        failureThreshold: 1,
        cooldownMs: 30_000,
        jitterRatio: 0.2,
        random: (): number => random,
        clock,
      });
      await expect(breaker.execute(fail)).rejects.toThrow('io');
      const opened = clock.now();
      expect(breaker.snapshot().retryAt).toBeCloseTo(opened + retryInMs, 0);

      clock.advance(retryInMs - 1);
      expect(breaker.state).toBe('open');
      clock.advance(1);
      expect(breaker.state).toBe('half-open');
    },
  );

  it('re-rolls the jitter every time it opens', async () => {
    const clock = new FakeClock();
    const rolls = [0, 1];
    const breaker = new CircuitBreaker({
      failureThreshold: 1,
      cooldownMs: 1_000,
      jitterRatio: 0.5,
      random: (): number => rolls.shift() ?? 0,
      clock,
    });
    await expect(breaker.execute(fail)).rejects.toThrow('io');
    expect(breaker.retryAfterMs).toBe(1_000);
    clock.advance(1_000);
    await expect(breaker.execute(fail)).rejects.toThrow('io'); // failed probe
    expect(breaker.retryAfterMs).toBe(1_500);
  });

  it('failures of calls in flight when it opened do not push the retry back', async () => {
    const clock = new FakeClock();
    const breaker = new CircuitBreaker({
      failureThreshold: 1,
      cooldownMs: 1_000,
      clock,
    });
    const { promise, reject } = Promise.withResolvers<never>();
    const slow = breaker.execute(() => promise); // admitted while closed
    await expect(breaker.execute(fail)).rejects.toThrow('io'); // opens
    const retryAt = breaker.snapshot().retryAt;
    clock.advance(500);
    reject(new Error('late'));
    await expect(slow).rejects.toThrow('late');
    expect(breaker.snapshot().retryAt).toBe(retryAt);
  });

  it('autoHalfOpen announces half-open on time, with no traffic needed', async () => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'Date'] });
    try {
      const states: string[] = [];
      const breaker = new CircuitBreaker({
        failureThreshold: 1,
        cooldownMs: 30_000,
        autoHalfOpen: true,
        onStateChange: (state): void => {
          states.push(state);
        },
      });
      await expect(breaker.execute(fail)).rejects.toThrow('io');
      expect(states).toEqual(['open']);
      await vi.advanceTimersByTimeAsync(29_999);
      expect(states).toEqual(['open']);
      await vi.advanceTimersByTimeAsync(1);
      expect(states).toEqual(['open', 'half-open']); // nobody called it
    } finally {
      vi.useRealTimers();
    }
  });

  it('re-arms when its timer fires before the clock reaches retryAt', async () => {
    // Timers run on the event loop's cached time, which can lag the clock:
    // a timer that fires "early" must not leave the circuit stuck open.
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] });
    try {
      const clock = new FakeClock(); // independent of the timer clock
      const states: string[] = [];
      const breaker = new CircuitBreaker({
        failureThreshold: 1,
        cooldownMs: 1_000,
        autoHalfOpen: true,
        clock,
        onStateChange: (state): void => {
          states.push(state);
        },
      });
      await expect(breaker.execute(fail)).rejects.toThrow('io');
      clock.advance(999); // 1 ms short when the timer fires
      await vi.advanceTimersByTimeAsync(1_000);
      expect(states).toEqual(['open']);
      expect(vi.getTimerCount()).toBe(1); // re-armed, not given up

      clock.advance(1);
      await vi.advanceTimersByTimeAsync(1);
      expect(states).toEqual(['open', 'half-open']);
      expect(vi.getTimerCount()).toBe(0);
    } finally {
      vi.useRealTimers();
    }
  });

  it('dispose() and closing cancel the half-open timer', async () => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'Date'] });
    try {
      const breaker = new CircuitBreaker({
        failureThreshold: 1,
        autoHalfOpen: true,
      });
      await expect(breaker.execute(fail)).rejects.toThrow('io');
      expect(vi.getTimerCount()).toBe(1);
      breaker.dispose();
      expect(vi.getTimerCount()).toBe(0);

      await expect(breaker.execute(fail)).rejects.toBeInstanceOf(
        CircuitOpenError,
      );
      breaker.reset(); // closing also clears it
      await expect(breaker.execute(fail)).rejects.toThrow('io');
      expect(vi.getTimerCount()).toBe(1);
      breaker.reset();
      expect(vi.getTimerCount()).toBe(0);
    } finally {
      vi.useRealTimers();
    }
  });

  it('validates windowMs and jitterRatio', () => {
    expect(() => new CircuitBreaker({ windowMs: 0 })).toThrow(RangeError);
    expect(() => new CircuitBreaker({ jitterRatio: -1 })).toThrow(RangeError);
  });
});
