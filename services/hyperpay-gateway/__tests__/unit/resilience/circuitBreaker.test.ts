/* eslint-disable @typescript-eslint/no-unused-vars */
import { describe, it, test, expect, beforeEach, afterEach, beforeAll, afterAll, vi } from 'vitest';
/**
 * tests/unit/resilience/circuitBreaker.test.ts
 *
 * The breaker in isolation, with an injectable clock:
 *   - CLOSED: calls pass, failures counted in a sliding window, threshold opens
 *   - OPEN: refused at once with CircuitOpenError (retryAfterMs), dependency untouched
 *   - OPEN → HALF_OPEN after the open period (timer and lazily)
 *   - HALF_OPEN: one probe at a time; success closes and resets, failure re-opens
 *     for twice as long, capped
 *   - isFailure: non-failures are completed calls (success for the breaker)
 *   - metrics, bounded state log, stateChange events, trip / reset / stop
 */

import { CircuitBreaker, CircuitOpenError } from '../../../src/resilience/circuitBreaker';
import type { CircuitBreakerOptions, StateChange } from '../../../src/resilience/circuitBreaker';

class Clock {
  private t = 1_000_000;
  now = (): number => this.t;
  tick(ms: number): void {
    this.t += ms;
  }
}

let clock: Clock;
let breaker: CircuitBreaker;

function make(overrides: Partial<CircuitBreakerOptions> = {}): CircuitBreaker {
  breaker = new CircuitBreaker({
    name: 'test',
    windowMs: 1000,
    failureThreshold: 3,
    openDurationMs: 5000,
    maxOpenDurationMs: 20_000,
    now: clock.now,
    ...overrides,
  });
  return breaker;
}

const ok = (): Promise<string> => Promise.resolve('ok');
const boom = (message = 'boom'): Promise<never> => Promise.reject(new Error(message));

/** Drive the breaker to OPEN with `n` failures. */
async function trip(b: CircuitBreaker, n = 3, message = 'boom'): Promise<void> {
  for (let i = 0; i < n; i += 1) await expect(b.execute(() => boom(message))).rejects.toThrow(message);
}

beforeEach(() => {
  clock = new Clock();
  vi.spyOn(console, 'warn').mockImplementation(() => undefined);
});

afterEach(() => {
  breaker.stop();
  vi.restoreAllMocks();
});

// ─── CLOSED ───────────────────────────────────────────────────────────────────

describe('CLOSED', () => {
  it('passes calls through and returns their result', async () => {
    const b = make();
    await expect(b.execute(ok)).resolves.toBe('ok');
    expect(b.getState()).toBe('CLOSED');
    expect(b.getMetrics()).toMatchObject({ window: { calls: 1, successes: 1, failures: 0, rejected: 0 }, totals: { calls: 1, successes: 1 } });
  });

  it('rethrows failures and counts them; opens once the threshold is reached inside the window', async () => {
    const b = make();
    const changes: StateChange[] = [];
    b.on('stateChange', (c) => changes.push(c));

    await expect(b.execute(boom)).rejects.toThrow('boom');
    await expect(b.execute(boom)).rejects.toThrow('boom');
    expect(b.getState()).toBe('CLOSED');
    await expect(b.execute(boom)).rejects.toThrow('boom');

    expect(b.getState()).toBe('OPEN');
    expect(changes).toEqual([{ from: 'CLOSED', to: 'OPEN', at: expect.any(String) as string, reason: expect.stringMatching(/3 failure\(s\) in 1000ms \(threshold 3\) — open for 5000ms/) as string }]);
    expect(b.getMetrics()).toMatchObject({ totals: { opens: 1, failures: 3 }, lastFailure: { message: 'boom' } });
  });

  it('only counts failures that fall inside the sliding window', async () => {
    const b = make({ windowMs: 1000, failureThreshold: 3 });
    await expect(b.execute(boom)).rejects.toThrow();
    await expect(b.execute(boom)).rejects.toThrow();
    clock.tick(1100); // both slide out
    await expect(b.execute(boom)).rejects.toThrow();
    expect(b.getState()).toBe('CLOSED');
    expect(b.getMetrics().window.failures).toBe(1);

    await expect(b.execute(boom)).rejects.toThrow();
    await expect(b.execute(boom)).rejects.toThrow();
    expect(b.getState()).toBe('OPEN');
  });

  it('successes do not dilute the failure count (threshold is absolute, not a rate)', async () => {
    const b = make({ failureThreshold: 2 });
    for (let i = 0; i < 20; i += 1) await b.execute(ok);
    await expect(b.execute(boom)).rejects.toThrow();
    await expect(b.execute(boom)).rejects.toThrow();
    expect(b.getState()).toBe('OPEN');
  });

  it('treats errors rejected by isFailure as completed calls (no failure counted)', async () => {
    const decline = Object.assign(new Error('declined'), { business: true });
    const b = make({ failureThreshold: 1, isFailure: (e) => !(e instanceof Error && 'business' in e) });

    await expect(b.execute(() => Promise.reject(decline))).rejects.toThrow('declined');

    expect(b.getState()).toBe('CLOSED');
    expect(b.getMetrics().window).toMatchObject({ calls: 1, successes: 1, failures: 0 });
  });
});

// ─── OPEN ─────────────────────────────────────────────────────────────────────

describe('OPEN', () => {
  it('refuses calls at once with CircuitOpenError without invoking the function', async () => {
    const b = make();
    await trip(b);
    const fn = vi.fn(ok);

    await expect(b.execute(fn)).rejects.toBeInstanceOf(CircuitOpenError);
    await expect(b.execute(fn)).rejects.toMatchObject({ circuit: 'test', state: 'OPEN', retryAfterMs: 5000, message: expect.stringContaining('is OPEN') as string });

    expect(fn).not.toHaveBeenCalled();
    expect(b.getMetrics()).toMatchObject({ window: { rejected: 2 }, totals: { rejected: 2, calls: 3 }, openUntil: new Date(clock.now() + 5000).toISOString() });
  });

  it('retryAfterMs counts down as time passes', async () => {
    const b = make();
    await trip(b);
    clock.tick(3000);
    await expect(b.execute(ok)).rejects.toMatchObject({ retryAfterMs: 2000 });
  });

  it('moves to HALF_OPEN lazily once the open period has elapsed (no timers needed)', async () => {
    const b = make();
    await trip(b);
    clock.tick(4999);
    expect(b.getState()).toBe('OPEN');
    clock.tick(1);
    expect(b.getState()).toBe('HALF_OPEN');
    expect(b.getStateLog().at(-1)).toMatchObject({ from: 'OPEN', to: 'HALF_OPEN', reason: 'open period of 5000ms elapsed' });
  });

  it('the timer transitions even if the wall clock has not quite reached openUntil (timers can fire a hair early)', async () => {
    vi.useFakeTimers();
    try {
      // Real timers vs. an injected clock that never advances: the timer firing is authoritative.
      const b = make({ openDurationMs: 50 });
      await trip(b);
      vi.advanceTimersByTime(50);
      expect(b.getState()).toBe('HALF_OPEN');
    } finally {
      vi.useRealTimers();
    }
  });

  it('moves to HALF_OPEN on a timer as well, emitting the change', async () => {
    vi.useFakeTimers();
    try {
      const b = make({ openDurationMs: 50, now: Date.now });
      const onChange = vi.fn();
      b.on('stateChange', onChange);
      await trip(b);
      expect(b.getState()).toBe('OPEN');

      vi.advanceTimersByTime(50);

      expect(b.getState()).toBe('HALF_OPEN');
      expect(onChange).toHaveBeenLastCalledWith(expect.objectContaining({ to: 'HALF_OPEN' }));
    } finally {
      vi.useRealTimers();
    }
  });
});

// ─── HALF_OPEN ────────────────────────────────────────────────────────────────

describe('HALF_OPEN', () => {
  async function halfOpen(overrides: Partial<CircuitBreakerOptions> = {}, failure = 'boom'): Promise<CircuitBreaker> {
    const b = make(overrides);
    await trip(b, 3, failure);
    clock.tick(5000);
    expect(b.getState()).toBe('HALF_OPEN');
    return b;
  }

  it('lets one probe through and refuses the rest while it is in flight', async () => {
    const b = await halfOpen();
    let release: (() => void) | undefined;
    const probe = b.execute(() => new Promise<string>((resolve) => { release = (): void => resolve('probe'); }));
    expect(b.getMetrics().halfOpen).toEqual({ inFlight: 1, maxCalls: 1 });

    await expect(b.execute(ok)).rejects.toMatchObject({ state: 'HALF_OPEN' });

    release?.();
    await expect(probe).resolves.toBe('probe');
    expect(b.getMetrics()).toMatchObject({ halfOpen: { inFlight: 0 }, totals: { probes: 1 } });
  });

  it('closes on a successful probe, resetting the window and the open duration', async () => {
    const b = await halfOpen();
    const changes: StateChange[] = [];
    b.on('stateChange', (c) => changes.push(c));

    await expect(b.execute(ok)).resolves.toBe('ok');

    expect(b.getState()).toBe('CLOSED');
    expect(changes).toEqual([expect.objectContaining({ from: 'HALF_OPEN', to: 'CLOSED', reason: 'probe succeeded' })]);
    expect(b.getMetrics()).toMatchObject({ window: { failures: 0 }, openDurationMs: 5000 });
    // Fresh window: the old failures are gone, it takes a full threshold to open again.
    await expect(b.execute(boom)).rejects.toThrow();
    await expect(b.execute(boom)).rejects.toThrow();
    expect(b.getState()).toBe('CLOSED');
  });

  it('re-opens on a failed probe for twice as long, doubling on every failed probe up to the cap', async () => {
    const b = await halfOpen({ openDurationMs: 5000, maxOpenDurationMs: 12_000 });

    await expect(b.execute(() => boom('still down'))).rejects.toThrow('still down');
    expect(b.getState()).toBe('OPEN');
    expect(b.getStateLog().at(-1)).toMatchObject({ to: 'OPEN', reason: 'probe failed: still down — open for 10000ms' });
    expect(b.getMetrics().openDurationMs).toBe(10_000);
    clock.tick(9999);
    expect(b.getState()).toBe('OPEN');
    clock.tick(1);
    expect(b.getState()).toBe('HALF_OPEN');

    await expect(b.execute(boom)).rejects.toThrow();
    expect(b.getMetrics().openDurationMs).toBe(12_000); // capped
    clock.tick(12_000);
    await expect(b.execute(ok)).resolves.toBe('ok');
    expect(b.getMetrics().openDurationMs).toBe(5000); // back to the base after closing
  });

  it('a probe ending in a non-failure error (e.g. a decline) counts as a successful probe', async () => {
    const b = await halfOpen({ isFailure: (e) => e instanceof Error && e.message === 'outage' }, 'outage');

    await expect(b.execute(() => boom('declined'))).rejects.toThrow('declined');

    expect(b.getState()).toBe('CLOSED');
  });

  it('allows several concurrent probes when halfOpenMaxCalls > 1', async () => {
    const b = await halfOpen({ halfOpenMaxCalls: 2 });
    const releases: Array<() => void> = [];
    const slow = (): Promise<string> => new Promise((resolve) => releases.push(() => resolve('x')));

    const p1 = b.execute(slow);
    const p2 = b.execute(slow);
    await expect(b.execute(slow)).rejects.toBeInstanceOf(CircuitOpenError);

    releases.forEach((r) => r());
    await Promise.all([p1, p2]);
    expect(b.getState()).toBe('CLOSED');
    expect(b.getMetrics().halfOpen.inFlight).toBe(0);
  });
});

// ─── Observability & operator controls ────────────────────────────────────────

describe('observability', () => {
  it('reports full metrics', async () => {
    const b = make();
    await b.execute(ok);
    await expect(b.execute(boom)).rejects.toThrow();

    expect(b.getMetrics()).toEqual({
      name: 'test',
      state: 'CLOSED',
      stateSince: new Date(1_000_000).toISOString(),
      failureThreshold: 3,
      window: { windowMs: 1000, calls: 2, successes: 1, failures: 1, rejected: 0 },
      openDurationMs: 5000,
      halfOpen: { inFlight: 0, maxCalls: 1 },
      totals: { calls: 2, successes: 1, failures: 1, rejected: 0, opens: 0, probes: 0 },
      lastFailure: { at: new Date(1_000_000).toISOString(), message: 'boom' },
    });
  });

  it('keeps a bounded, ordered state-change log and emits every change', async () => {
    const b = make({ stateLogSize: 3, failureThreshold: 1, openDurationMs: 10, maxOpenDurationMs: 10 });
    const emitted: string[] = [];
    b.on('stateChange', (c) => emitted.push(`${c.from}>${c.to}`));

    for (let round = 0; round < 3; round += 1) {
      await expect(b.execute(boom)).rejects.toThrow(); // CLOSED/HALF_OPEN → OPEN
      clock.tick(10); // → HALF_OPEN (lazily, on the next call)
    }
    await b.execute(ok); // → CLOSED

    expect(emitted).toEqual(['CLOSED>OPEN', 'OPEN>HALF_OPEN', 'HALF_OPEN>OPEN', 'OPEN>HALF_OPEN', 'HALF_OPEN>OPEN', 'OPEN>HALF_OPEN', 'HALF_OPEN>CLOSED']);
    expect(b.getStateLog().map((c) => `${c.from}>${c.to}`)).toEqual(['HALF_OPEN>OPEN', 'OPEN>HALF_OPEN', 'HALF_OPEN>CLOSED']);
    expect(b.getStateLog()[0]).toEqual({ from: 'HALF_OPEN', to: 'OPEN', at: expect.any(String) as string, reason: expect.any(String) as string });
  });

  it('trip() opens immediately; reset() closes, clears the window and the backoff', async () => {
    const b = make();
    const onChange = vi.fn();
    b.on('stateChange', onChange);

    b.trip('maintenance');
    expect(b.getState()).toBe('OPEN');
    await expect(b.execute(ok)).rejects.toBeInstanceOf(CircuitOpenError);
    expect(onChange).toHaveBeenLastCalledWith(expect.objectContaining({ to: 'OPEN', reason: 'maintenance — open for 5000ms' }));
    b.trip(); // already open: no-op
    expect(onChange).toHaveBeenCalledTimes(1);

    clock.tick(5000);
    await expect(b.execute(boom)).rejects.toThrow(); // failed probe → 10s
    expect(b.getMetrics().openDurationMs).toBe(10_000);

    b.reset('operator');
    expect(b.getState()).toBe('CLOSED');
    expect(b.getMetrics()).toMatchObject({ openDurationMs: 5000, window: { failures: 0 } });
    expect(onChange).toHaveBeenLastCalledWith(expect.objectContaining({ to: 'CLOSED', reason: 'operator' }));
    await expect(b.execute(ok)).resolves.toBe('ok');
  });

  it('stop() clears the half-open timer without changing state', async () => {
    vi.useFakeTimers();
    try {
      const b = make({ openDurationMs: 50, now: Date.now });
      await trip(b);
      b.stop();
      vi.advanceTimersByTime(100);
      expect(vi.getTimerCount()).toBe(0);
    } finally {
      vi.useRealTimers();
    }
  });
});
