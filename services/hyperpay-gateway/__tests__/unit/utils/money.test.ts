/* eslint-disable @typescript-eslint/no-unused-vars */
import { describe, it, test, expect, beforeEach, afterEach, beforeAll, afterAll, vi } from 'vitest';
/**
 * tests/unit/utils/money.test.ts
 */

import { addAmounts, fromMinor, isGreater, isZero, subtractAmounts, toMinor } from '../../../src/utils/money';

describe('money', () => {
  it('converts to and from minor units exactly', () => {
    expect(toMinor('49.90')).toBe(4990);
    expect(toMinor('0.1')).toBe(10);
    expect(toMinor('5')).toBe(500);
    expect(toMinor(' 7.05 ')).toBe(705);
    expect(fromMinor(4990)).toBe('49.90');
    expect(fromMinor(5)).toBe('0.05');
    expect(fromMinor(0)).toBe('0.00');
    expect(fromMinor(-150)).toBe('-1.50');
  });

  it.each(['', '-1', '1.005', '1,000', 'abc', '1.'])('rejects %p', (raw) => {
    expect(() => toMinor(raw)).toThrow(/Invalid amount/);
  });

  it('adds and subtracts without floating-point drift', () => {
    expect(addAmounts('0.10', '0.20')).toBe('0.30');
    expect(addAmounts('33.33', '33.33', '33.34')).toBe('100.00');
    expect(subtractAmounts('100.00', '99.99')).toBe('0.01');
    expect(subtractAmounts('1.00', '2.00')).toBe('-1.00');
  });

  it('compares', () => {
    expect(isGreater('10.00', '9.99')).toBe(true);
    expect(isGreater('9.99', '10.00')).toBe(false);
    expect(isGreater('1.0', '1.00')).toBe(false);
    expect(isZero('0')).toBe(true);
    expect(isZero('0.01')).toBe(false);
  });
});
