import { MoneyMathError, divRoundHalfEven } from './integer';

/** 1 basis point = 1/10,000. */
export const BPS_DENOMINATOR = 10_000n;
export const MAX_FEE_BPS = 10_000; // 100%

export function assertValidFeeBps(feeBps: number): number {
  if (!Number.isInteger(feeBps) || feeBps < 0 || feeBps > MAX_FEE_BPS) {
    throw new MoneyMathError(
      `Platform fee must be an integer number of basis points in [0, ${MAX_FEE_BPS}], got ${feeBps}`
    );
  }
  return feeBps;
}

/**
 * fee = amount * bps / 10_000, banker's-rounded to the nearest minor unit.
 * Exact integer math: the only rounding is the single final division.
 */
export function calculateFee(amountMinor: bigint, feeBps: number): bigint {
  assertValidFeeBps(feeBps);
  if (amountMinor < 0n) {
    throw new MoneyMathError(`Fee base amount must be non-negative, got ${amountMinor}`);
  }
  return divRoundHalfEven(amountMinor * BigInt(feeBps), BPS_DENOMINATOR);
}
