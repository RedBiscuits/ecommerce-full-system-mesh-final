/**
 * Exact integer money primitives. All monetary arithmetic in this service is
 * done in `bigint` minor units; `number` only appears at the API boundary and
 * is range-checked on the way in and out.
 */

export class MoneyMathError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'MoneyMathError';
  }
}

/** PostgreSQL BIGINT range. Every persisted money value must fit. */
export const PG_BIGINT_MAX = 9223372036854775807n;
export const PG_BIGINT_MIN = -9223372036854775808n;

const MAX_SAFE = BigInt(Number.MAX_SAFE_INTEGER);
const MIN_SAFE = BigInt(Number.MIN_SAFE_INTEGER);

/**
 * Normalize a DB/driver value to bigint. PGlite returns BIGINT columns as
 * `bigint` and SUM()/NUMERIC as `string`; node-postgres returns BIGINT as
 * `string`. Only exact integers are accepted: never silently truncate.
 */
export function toBigInt(value: unknown, label = 'value'): bigint {
  if (typeof value === 'bigint') return value;
  if (typeof value === 'number') {
    if (!Number.isSafeInteger(value)) {
      throw new MoneyMathError(`${label} must be a safe integer, got ${value}`);
    }
    return BigInt(value);
  }
  if (typeof value === 'string' && /^-?\d+$/.test(value.trim())) {
    return BigInt(value.trim());
  }
  throw new MoneyMathError(`${label} is not an exact integer: ${String(value)}`);
}

/** Convert bigint to number for JSON responses, refusing any precision loss. */
export function toSafeNumber(value: bigint, label = 'value'): number {
  if (value > MAX_SAFE || value < MIN_SAFE) {
    throw new MoneyMathError(`${label} ${value} exceeds the JavaScript safe integer range`);
  }
  return Number(value);
}

/** Ensure a value fits a PostgreSQL BIGINT column before it is written. */
export function assertFitsPgBigint(value: bigint, label = 'value'): bigint {
  if (value > PG_BIGINT_MAX || value < PG_BIGINT_MIN) {
    throw new MoneyMathError(`${label} ${value} overflows PostgreSQL BIGINT`);
  }
  return value;
}

/**
 * Integer division with banker's rounding (round half to even).
 *
 * Exact for all bigint inputs: the quotient is truncated, and the remainder is
 * compared against half the divisor by doubling it (no fractions ever exist).
 * Ties (remainder exactly half) go to the even neighbour, which makes rounding
 * unbiased across many operations. Works for negative numerators symmetrically.
 */
export function divRoundHalfEven(numerator: bigint, denominator: bigint): bigint {
  if (denominator === 0n) {
    throw new MoneyMathError('Division by zero');
  }
  if (denominator < 0n) {
    return divRoundHalfEven(-numerator, -denominator);
  }

  const negative = numerator < 0n;
  const magnitude = negative ? -numerator : numerator;

  const quotient = magnitude / denominator;
  const remainder = magnitude % denominator;
  const twiceRemainder = remainder * 2n;

  let rounded = quotient;
  if (twiceRemainder > denominator) {
    rounded = quotient + 1n;
  } else if (twiceRemainder === denominator && quotient % 2n !== 0n) {
    rounded = quotient + 1n; // exact tie: move to the even neighbour
  }

  return negative ? -rounded : rounded;
}
