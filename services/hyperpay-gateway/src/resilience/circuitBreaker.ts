/**
 * src/resilience/circuitBreaker.ts
 *
 * A zero-dependency circuit breaker (Node's `events` only).
 *
 *   CLOSED ──failures in window ≥ threshold──► OPEN
 *     ▲                                         │ openDuration elapses
 *     │ probe succeeded                         ▼
 *     └────────────────────────────────── HALF_OPEN
 *                                               │ probe failed
 *                                               └──► OPEN (duration doubled, capped)
 *
 *   CLOSED     every call goes through; failures are counted in a sliding
 *              time window (`windowMs`, split into `buckets`). Reaching
 *              `failureThreshold` failures inside the window opens the circuit.
 *   OPEN       every call is rejected at once with `CircuitOpenError` — the
 *              dependency is not touched. After `openDurationMs` the circuit
 *              moves to HALF_OPEN (timer, or lazily on the next call).
 *   HALF_OPEN  up to `halfOpenMaxCalls` trial calls pass; the rest are
 *              rejected. A successful trial closes the circuit and resets the
 *              open duration; a failed one re-opens it for twice as long
 *              (capped at `maxOpenDurationMs`).
 *
 * Only errors for which `isFailure(error)` is true count (default: all). An
 * error that is *not* a failure — e.g. a business decline — is a completed
 * call: it is rethrown but counted as a success.
 *
 * Observability: `getState()`, `getMetrics()`, `getStateLog()` and a
 * `stateChange` event carrying `{ from, to, at, reason }`.
 */

import { EventEmitter } from 'events';

// ─── Types ────────────────────────────────────────────────────────────────────

export type CircuitState = 'CLOSED' | 'OPEN' | 'HALF_OPEN';

export interface CircuitBreakerOptions {
  /** Label used in errors, logs and metrics. */
  name: string;
  /** Length of the sliding window over which failures are counted. */
  windowMs: number;
  /** Failures inside the window that open the circuit. */
  failureThreshold: number;
  /** How long the circuit stays open after tripping from CLOSED. */
  openDurationMs: number;
  /** Upper bound for the doubling applied after every failed probe. */
  maxOpenDurationMs: number;
  /** Trial calls allowed at a time while HALF_OPEN. Default 1. */
  halfOpenMaxCalls?: number;
  /** Resolution of the sliding window. Default 10. */
  buckets?: number;
  /** Which errors count as failures. Default: every error. */
  isFailure?: (error: unknown) => boolean;
  /** Entries kept in the state-change log. Default 100. */
  stateLogSize?: number;
  /** Injectable clock (ms since epoch). */
  now?: () => number;
}

export interface StateChange {
  from: CircuitState;
  to: CircuitState;
  /** ISO-8601 */
  at: string;
  reason: string;
}

export interface WindowMetrics {
  windowMs: number;
  calls: number;
  successes: number;
  failures: number;
  /** Calls refused without touching the dependency (OPEN / HALF_OPEN full). */
  rejected: number;
}

export interface CircuitMetrics {
  name: string;
  state: CircuitState;
  /** When the current state was entered (ISO-8601). */
  stateSince: string;
  failureThreshold: number;
  window: WindowMetrics;
  /** What the next OPEN period would last (doubles after every failed probe). */
  openDurationMs: number;
  /** Present while OPEN. */
  openUntil?: string;
  halfOpen: { inFlight: number; maxCalls: number };
  totals: { calls: number; successes: number; failures: number; rejected: number; opens: number; probes: number };
  lastFailure?: { at: string; message: string };
}

export interface CircuitBreakerEvents {
  stateChange: [change: StateChange];
}

/** Thrown for calls refused while the circuit is OPEN (or HALF_OPEN and busy). */
export class CircuitOpenError extends Error {
  public readonly circuit: string;
  public readonly state: CircuitState;
  /** Hint for callers: how long until the circuit will try again. */
  public readonly retryAfterMs: number;

  constructor(circuit: string, state: CircuitState, retryAfterMs: number) {
    super(`Circuit "${circuit}" is ${state} — call refused, retry in ${retryAfterMs}ms`);
    this.name = 'CircuitOpenError';
    this.circuit = circuit;
    this.state = state;
    this.retryAfterMs = retryAfterMs;
  }
}

// ─── Sliding window ───────────────────────────────────────────────────────────

interface Bucket {
  start: number;
  calls: number;
  successes: number;
  failures: number;
  rejected: number;
}

type Counter = Exclude<keyof Bucket, 'start'>;

/**
 * Fixed number of time buckets reused in a ring. Memory is constant; a count
 * is the sum of the buckets that still fall inside the window.
 */
class SlidingWindow {
  private readonly buckets: Bucket[];
  private readonly bucketMs: number;

  constructor(private readonly windowMs: number, bucketCount: number) {
    this.bucketMs = Math.max(1, Math.floor(windowMs / bucketCount));
    this.buckets = Array.from({ length: bucketCount }, () => ({ start: -1, calls: 0, successes: 0, failures: 0, rejected: 0 }));
  }

  record(counter: Counter, now: number): void {
    const start = now - (now % this.bucketMs);
    const bucket = this.buckets[Math.floor(now / this.bucketMs) % this.buckets.length];
    if (bucket.start !== start) {
      bucket.start = start;
      bucket.calls = bucket.successes = bucket.failures = bucket.rejected = 0;
    }
    bucket[counter] += 1;
  }

  count(counter: Counter, now: number): number {
    const oldest = now - this.windowMs;
    return this.buckets.reduce((sum, b) => (b.start > oldest ? sum + b[counter] : sum), 0);
  }

  clear(): void {
    for (const bucket of this.buckets) bucket.start = -1;
  }
}

// ─── Breaker ──────────────────────────────────────────────────────────────────

export class CircuitBreaker extends EventEmitter<CircuitBreakerEvents> {
  public readonly name: string;

  private readonly options: Required<Omit<CircuitBreakerOptions, 'isFailure' | 'now'>>;
  private readonly isFailure: (error: unknown) => boolean;
  private readonly now: () => number;
  private readonly window: SlidingWindow;

  private state: CircuitState = 'CLOSED';
  private stateSince: number;
  private currentOpenMs: number;
  private openUntil = 0;
  private halfOpenInFlight = 0;
  private timer: NodeJS.Timeout | null = null;
  private readonly log: StateChange[] = [];
  private readonly totals = { calls: 0, successes: 0, failures: 0, rejected: 0, opens: 0, probes: 0 };
  private lastFailure?: { at: string; message: string };

  constructor(options: CircuitBreakerOptions) {
    super();
    this.name = options.name;
    this.options = {
      name: options.name,
      windowMs: options.windowMs,
      failureThreshold: options.failureThreshold,
      openDurationMs: options.openDurationMs,
      maxOpenDurationMs: Math.max(options.openDurationMs, options.maxOpenDurationMs),
      halfOpenMaxCalls: options.halfOpenMaxCalls ?? 1,
      buckets: options.buckets ?? 10,
      stateLogSize: options.stateLogSize ?? 100,
    };
    this.isFailure = options.isFailure ?? ((): boolean => true);
    this.now = options.now ?? Date.now;
    this.window = new SlidingWindow(this.options.windowMs, this.options.buckets);
    this.currentOpenMs = this.options.openDurationMs;
    this.stateSince = this.now();
  }

  // ── Public API ─────────────────────────────────────────────────────────────

  /** Run `fn` through the breaker. Rejects with `CircuitOpenError` when refused. */
  async execute<T>(fn: () => Promise<T>): Promise<T> {
    const now = this.now();
    this.settleOpenPeriod(now);

    if (this.state === 'OPEN' || (this.state === 'HALF_OPEN' && this.halfOpenInFlight >= this.options.halfOpenMaxCalls)) {
      this.window.record('rejected', now);
      this.totals.rejected += 1;
      throw new CircuitOpenError(this.name, this.state, Math.max(0, this.openUntil - now));
    }

    const probe = this.state === 'HALF_OPEN';
    if (probe) {
      this.halfOpenInFlight += 1;
      this.totals.probes += 1;
    }
    this.window.record('calls', now);
    this.totals.calls += 1;

    try {
      const result = await fn();
      this.onSuccess(probe);
      return result;
    } catch (err) {
      if (this.isFailure(err)) this.onFailure(err, probe);
      else this.onSuccess(probe);
      throw err;
    } finally {
      if (probe) this.halfOpenInFlight -= 1;
    }
  }

  getState(): CircuitState {
    this.settleOpenPeriod(this.now());
    return this.state;
  }

  getMetrics(): CircuitMetrics {
    const now = this.now();
    this.settleOpenPeriod(now);
    return {
      name: this.name,
      state: this.state,
      stateSince: new Date(this.stateSince).toISOString(),
      failureThreshold: this.options.failureThreshold,
      window: {
        windowMs: this.options.windowMs,
        calls: this.window.count('calls', now),
        successes: this.window.count('successes', now),
        failures: this.window.count('failures', now),
        rejected: this.window.count('rejected', now),
      },
      openDurationMs: this.currentOpenMs,
      ...(this.state === 'OPEN' ? { openUntil: new Date(this.openUntil).toISOString() } : {}),
      halfOpen: { inFlight: this.halfOpenInFlight, maxCalls: this.options.halfOpenMaxCalls },
      totals: { ...this.totals },
      ...(this.lastFailure ? { lastFailure: { ...this.lastFailure } } : {}),
    };
  }

  /** Most recent state changes, oldest first. */
  getStateLog(): StateChange[] {
    return [...this.log];
  }

  /** Open the circuit now (operator / tests). The open period is the current one. */
  trip(reason = 'tripped manually'): void {
    if (this.state === 'OPEN') return;
    this.open(reason);
  }

  /** Close the circuit now and forget the window and the backoff (operator). */
  reset(reason = 'reset manually'): void {
    this.window.clear();
    this.currentOpenMs = this.options.openDurationMs;
    if (this.state !== 'CLOSED') this.transition('CLOSED', reason);
  }

  /** Clear the recovery timer (shutdown). State is left as is. */
  stop(): void {
    this.clearTimer();
  }

  // ── Transitions ────────────────────────────────────────────────────────────

  private onSuccess(probe: boolean): void {
    this.window.record('successes', this.now());
    this.totals.successes += 1;
    if (probe && this.state === 'HALF_OPEN') {
      this.currentOpenMs = this.options.openDurationMs;
      this.window.clear();
      this.transition('CLOSED', 'probe succeeded');
    }
  }

  private onFailure(err: unknown, probe: boolean): void {
    const now = this.now();
    const message = err instanceof Error ? err.message : String(err);
    this.window.record('failures', now);
    this.totals.failures += 1;
    this.lastFailure = { at: new Date(now).toISOString(), message };

    if (probe && this.state === 'HALF_OPEN') {
      this.currentOpenMs = Math.min(this.currentOpenMs * 2, this.options.maxOpenDurationMs);
      this.open(`probe failed: ${message}`);
      return;
    }
    if (this.state === 'CLOSED') {
      const failures = this.window.count('failures', now);
      if (failures >= this.options.failureThreshold) {
        this.open(`${failures} failure(s) in ${this.options.windowMs}ms (threshold ${this.options.failureThreshold})`);
      }
    }
  }

  private open(reason: string): void {
    this.openUntil = this.now() + this.currentOpenMs;
    this.totals.opens += 1;
    this.transition('OPEN', `${reason} — open for ${this.currentOpenMs}ms`);
    this.clearTimer();
    // The timer firing *is* the period elapsing — no clock check here: a
    // timer can run a hair before Date.now() shows the full delay.
    this.timer = setTimeout(() => {
      this.timer = null;
      this.halfOpen();
    }, this.currentOpenMs);
    this.timer.unref();
  }

  /** OPEN → HALF_OPEN lazily, for callers that arrive after the period without a timer. */
  private settleOpenPeriod(now: number): void {
    if (this.state === 'OPEN' && now >= this.openUntil) {
      this.clearTimer();
      this.halfOpen();
    }
  }

  private halfOpen(): void {
    if (this.state === 'OPEN') this.transition('HALF_OPEN', `open period of ${this.currentOpenMs}ms elapsed`);
  }

  private transition(to: CircuitState, reason: string): void {
    const from = this.state;
    if (from === to) return;
    this.state = to;
    this.stateSince = this.now();

    const change: StateChange = { from, to, at: new Date(this.stateSince).toISOString(), reason };
    this.log.push(change);
    if (this.log.length > this.options.stateLogSize) this.log.shift();
    console.warn(`[Circuit:${this.name}] ${from} → ${to}: ${reason}`);
    this.emit('stateChange', change);
  }

  private clearTimer(): void {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
  }
}
