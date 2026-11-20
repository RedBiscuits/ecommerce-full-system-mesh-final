import type { Clock } from '../shared/clock';
import { systemClock } from '../shared/clock';
import { CircuitOpenError } from './errors';

export type CircuitState = 'closed' | 'open' | 'half-open';

export interface CircuitBreakerOptions {
  readonly name?: string;
  /** Consecutive failures that trip the circuit. Default: `5`. */
  readonly failureThreshold?: number;
  /**
   * Sliding window for the failure streak: only consecutive failures that
   * happened within the last `windowMs` count towards `failureThreshold`,
   * so sparse failures spread over minutes never trip the circuit.
   * Default: unbounded.
   */
  readonly windowMs?: number;
  /** How long to reject calls before probing again. Default: 30 s. */
  readonly cooldownMs?: number;
  /**
   * Adds `random() × jitterRatio × cooldownMs` to each cooldown. Instances
   * that tripped together then probe at different times, and never before
   * `cooldownMs`. Default: `0`.
   */
  readonly jitterRatio?: number;
  /**
   * Move to half-open with a timer when the cooldown ends, and notify
   * `onStateChange` then, instead of waiting for the next call to notice.
   * Lets an owner start a recovery probe on its own. The timer is unref'd.
   * Default: `false`.
   */
  readonly autoHalfOpen?: boolean;
  readonly random?: () => number;
  /** Probe calls admitted while half-open. Default: `1`. */
  readonly halfOpenMaxCalls?: number;
  /** Which errors count as failures. Default: every error except {@link CircuitOpenError}. */
  readonly isFailure?: (error: unknown) => boolean;
  readonly onStateChange?: (state: CircuitState, error?: unknown) => void;
  readonly clock?: Clock;
}

export interface CircuitSnapshot {
  readonly name: string;
  readonly state: CircuitState;
  readonly consecutiveFailures: number;
  readonly openedAt: number | undefined;
  /** When an open circuit will go half-open (cooldown plus jitter). */
  readonly retryAt: number | undefined;
  readonly lastError: unknown;
}

/**
 * Classic three-state circuit breaker.
 *
 * After `failureThreshold` consecutive failures the circuit opens and every
 * call fails fast with {@link CircuitOpenError} for `cooldownMs`. It then
 * goes half-open and lets a limited number of probes through: one success
 * closes it, one failure re-opens it. This keeps a sick disk from turning
 * every request into a slow failure.
 */
export class CircuitBreaker {
  readonly #name: string;
  readonly #failureThreshold: number;
  readonly #windowMs: number;
  readonly #cooldownMs: number;
  readonly #jitterRatio: number;
  readonly #autoHalfOpen: boolean;
  readonly #random: () => number;
  readonly #halfOpenMaxCalls: number;
  readonly #isFailure: (error: unknown) => boolean;
  readonly #onStateChange: CircuitBreakerOptions['onStateChange'];
  readonly #clock: Clock;

  #state: CircuitState = 'closed';
  /** Times of the current failure streak, oldest first (within the window). */
  #failureTimes: number[] = [];
  #openedAt: number | undefined;
  #retryAt: number | undefined;
  #timer: NodeJS.Timeout | undefined;
  #halfOpenInFlight = 0;
  #lastError: unknown;

  constructor(options: CircuitBreakerOptions = {}) {
    this.#name = options.name ?? 'circuit';
    this.#failureThreshold = Math.max(1, options.failureThreshold ?? 5);
    this.#windowMs = options.windowMs ?? Number.POSITIVE_INFINITY;
    if (!(this.#windowMs > 0)) {
      throw new RangeError('windowMs must be positive');
    }
    this.#cooldownMs = Math.max(0, options.cooldownMs ?? 30_000);
    this.#jitterRatio = options.jitterRatio ?? 0;
    if (!(this.#jitterRatio >= 0) || !Number.isFinite(this.#jitterRatio)) {
      throw new RangeError('jitterRatio must be a non-negative finite number');
    }
    this.#autoHalfOpen = options.autoHalfOpen ?? false;
    this.#random = options.random ?? Math.random;
    this.#halfOpenMaxCalls = Math.max(1, options.halfOpenMaxCalls ?? 1);
    this.#isFailure =
      options.isFailure ??
      ((error): boolean => !(error instanceof CircuitOpenError));
    this.#onStateChange = options.onStateChange;
    this.#clock = options.clock ?? systemClock;
  }

  get state(): CircuitState {
    this.#maybeHalfOpen();
    return this.#state;
  }

  /** Milliseconds until an open circuit goes half-open (`0` otherwise). */
  get retryAfterMs(): number {
    if (this.#state !== 'open' || this.#retryAt === undefined) {
      return 0;
    }
    return Math.max(0, this.#retryAt - this.#clock.now());
  }

  snapshot(): CircuitSnapshot {
    return {
      name: this.#name,
      state: this.state,
      consecutiveFailures: this.#failureTimes.length,
      openedAt: this.#openedAt,
      retryAt: this.#retryAt,
      lastError: this.#lastError,
    };
  }

  /** Runs `task` if the circuit admits it; records the outcome. */
  async execute<T>(task: () => Promise<T>): Promise<T> {
    this.#admit();
    try {
      const result = await task();
      this.recordSuccess();
      return result;
    } catch (error) {
      this.recordFailure(error);
      throw error;
    }
  }

  recordSuccess(): void {
    if (this.#state === 'half-open') {
      this.#halfOpenInFlight = Math.max(0, this.#halfOpenInFlight - 1);
    }
    this.#failureTimes = [];
    this.#transition('closed');
  }

  recordFailure(error: unknown): void {
    if (!this.#isFailure(error)) {
      if (this.#state === 'half-open') {
        this.#halfOpenInFlight = Math.max(0, this.#halfOpenInFlight - 1);
      }
      return;
    }
    this.#lastError = error;
    const now = this.#clock.now();
    this.#failureTimes.push(now);
    // Slide the window: failures older than `windowMs` no longer count.
    while (
      this.#failureTimes.length > 0 &&
      now - (this.#failureTimes[0] ?? now) > this.#windowMs
    ) {
      this.#failureTimes.shift();
    }
    if (this.#failureTimes.length > this.#failureThreshold) {
      this.#failureTimes.shift(); // only the streak's tail matters
    }
    if (
      this.#state === 'half-open' ||
      (this.#state === 'closed' &&
        this.#failureTimes.length >= this.#failureThreshold)
    ) {
      this.#open(now, error);
    }
  }

  /** Forces the circuit closed (e.g. after an operator fixed the disk). */
  reset(): void {
    this.#failureTimes = [];
    this.#halfOpenInFlight = 0;
    this.#transition('closed');
  }

  /** Cancels the half-open timer (see `autoHalfOpen`). Idempotent. */
  dispose(): void {
    clearTimeout(this.#timer);
    this.#timer = undefined;
  }

  // --- internals ----------------------------------------------------------

  #admit(): void {
    this.#maybeHalfOpen();
    if (this.#state === 'open') {
      throw new CircuitOpenError(this.#name, this.retryAfterMs);
    }
    if (this.#state === 'half-open') {
      if (this.#halfOpenInFlight >= this.#halfOpenMaxCalls) {
        throw new CircuitOpenError(this.#name, 0);
      }
      this.#halfOpenInFlight += 1;
    }
  }

  #open(now: number, error: unknown): void {
    this.#halfOpenInFlight = 0;
    this.#openedAt = now;
    const jitter = this.#random() * this.#jitterRatio * this.#cooldownMs;
    this.#retryAt = now + this.#cooldownMs + jitter;
    if (this.#autoHalfOpen) {
      this.#armHalfOpenTimer(now);
    }
    this.#transition('open', error);
  }

  /**
   * Timers are scheduled against the event loop's cached time, which can lag
   * the clock, so one may fire while `clock.now()` is still short of
   * `retryAt`. It then re-arms for the remainder. Otherwise the circuit
   * would stay open until some call happened to look at it.
   */
  #armHalfOpenTimer(now: number): void {
    clearTimeout(this.#timer);
    this.#timer = setTimeout(
      () => {
        this.#timer = undefined;
        if (this.#state !== 'open') {
          return;
        }
        const current = this.#clock.now();
        if (current < (this.#retryAt ?? current)) {
          this.#armHalfOpenTimer(current);
          return;
        }
        this.#maybeHalfOpen();
      },
      Math.max(1, (this.#retryAt ?? now) - now),
    );
    this.#timer.unref();
  }

  #maybeHalfOpen(): void {
    if (
      this.#state === 'open' &&
      this.#clock.now() >= (this.#retryAt ?? Number.POSITIVE_INFINITY)
    ) {
      this.#halfOpenInFlight = 0;
      this.#transition('half-open');
    }
  }

  #transition(state: CircuitState, error?: unknown): void {
    if (this.#state === state) {
      return;
    }
    this.#state = state;
    if (state === 'closed') {
      this.#openedAt = undefined;
      this.#retryAt = undefined;
      this.dispose();
    }
    this.#onStateChange?.(state, error);
  }
}
