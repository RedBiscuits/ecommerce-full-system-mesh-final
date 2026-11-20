import type { Clock } from '../../src/shared/clock';

/** Manually advanced clock for deterministic expiry tests. */
export class FakeClock implements Clock {
  #now: number;

  constructor(start = 1_700_000_000_000) {
    this.#now = start;
  }

  now(): number {
    return this.#now;
  }

  advance(ms: number): void {
    this.#now += ms;
  }

  set(ms: number): void {
    this.#now = ms;
  }
}
