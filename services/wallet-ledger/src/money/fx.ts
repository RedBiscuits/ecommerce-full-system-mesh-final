import { CurrencyCode, minorUnitsOf } from './currencies';
import { MoneyMathError, divRoundHalfEven } from './integer';

/**
 * FX rates are stored as scaled integers: rate_scaled = rate * 10^8, where
 * `rate` is units of QUOTE currency per 1 unit of BASE currency (major units).
 * e.g. USD->EUR 0.92 => 92_000_000; USD->JPY 151.25 => 15_125_000_000.
 */
export const FX_RATE_SCALE_DIGITS = 8;
export const FX_RATE_SCALE = 10n ** BigInt(FX_RATE_SCALE_DIGITS);

/** Sanity bounds: reject zero/negative or absurd rates (> 10^8 per unit). */
export const MAX_FX_RATE_SCALED = 10n ** 16n;

export class FxRateUnavailableError extends MoneyMathError {
  public readonly statusCode = 422;
  public readonly code = 'FX_RATE_UNAVAILABLE';
  constructor(base: string, quote: string) {
    super(`No FX rate available for ${base} -> ${quote}`);
    this.name = 'FxRateUnavailableError';
  }
}

export function assertValidRate(rateScaled: bigint): bigint {
  if (rateScaled <= 0n || rateScaled > MAX_FX_RATE_SCALED) {
    throw new MoneyMathError(`FX rate_scaled out of range: ${rateScaled}`);
  }
  return rateScaled;
}

/**
 * Deterministic integer conversion between minor units of two currencies.
 *
 *   converted = amount * rate_scaled * 10^dstExp
 *               ---------------------------------   (banker's rounding)
 *                   10^8 * 10^srcExp
 *
 * The numerator and denominator are exact bigints; the single final division
 * is the only rounding step, so the result is reproducible bit-for-bit.
 */
export function convertMinorUnits(
  amountMinor: bigint,
  from: CurrencyCode,
  to: CurrencyCode,
  rateScaled: bigint
): bigint {
  if (amountMinor < 0n) {
    throw new MoneyMathError(`Cannot convert a negative amount: ${amountMinor}`);
  }
  assertValidRate(rateScaled);

  const srcScale = 10n ** BigInt(minorUnitsOf(from));
  const dstScale = 10n ** BigInt(minorUnitsOf(to));

  return divRoundHalfEven(amountMinor * rateScaled * dstScale, FX_RATE_SCALE * srcScale);
}
