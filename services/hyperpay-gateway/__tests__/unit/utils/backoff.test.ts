/* eslint-disable @typescript-eslint/no-unused-vars */
import { describe, it, test, expect, beforeEach, afterEach, beforeAll, afterAll, vi } from 'vitest';
/**
 * tests/unit/utils/backoff.test.ts
 */

import { backoffCeiling, jitteredDelay } from '../../../src/utils/backoff';

const opts = { baseMs: 100, maxMs: 1000, minMs: 10 };

describe('backoffCeiling()', () => {
  it('doubles per attempt from the base and caps at max', () => {
    expect([0, 1, 2, 3, 4, 10].map((a) => backoffCeiling(a, opts))).toEqual([100, 200, 400, 800, 1000, 1000]);
  });
});

describe('jitteredDelay()', () => {
  it('is full jitter: uniformly anywhere between min and the attempt\'s ceiling', () => {
    expect(jitteredDelay(2, { ...opts, random: () => 0 })).toBe(10);
    expect(jitteredDelay(2, { ...opts, random: () => 0.5 })).toBe(205);
    expect(jitteredDelay(2, { ...opts, random: () => 0.999999 })).toBe(400);
  });

  it('never goes below min, even when the ceiling would', () => {
    expect(jitteredDelay(0, { baseMs: 1, maxMs: 1, minMs: 50, random: () => 0.3 })).toBe(50);
  });

  it('spreads a crowd out instead of lining it up', () => {
    const delays = Array.from({ length: 200 }, () => jitteredDelay(3, opts));
    expect(Math.min(...delays)).toBeGreaterThanOrEqual(10);
    expect(Math.max(...delays)).toBeLessThanOrEqual(800);
    expect(new Set(delays).size).toBeGreaterThan(50);
  });
});
