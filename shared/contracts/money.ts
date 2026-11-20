/**
 * Exact integer money primitives shared by every service that splits amounts
 * (the ZATCA tax engine, the ledger's order totals, the checkout coordinator).
 *
 * Rules:
 *  - all arithmetic on integer halalas, through BigInt where a product can
 *    exceed 2^53 (amount × amount, amount × basis points);
 *  - a division that must round uses banker's rounding (round half to even),
 *    which has no systematic bias: over many amounts nothing is gained or lost;
 *  - a split of an amount into parts always sums back to the amount exactly
 *    (largest-remainder allocation), so no halala is created or destroyed.
 */

export class MoneyArithmeticError extends Error {
  public readonly retryable = false;
  constructor(
    public readonly reason: string,
    message: string
  ) {
    super(message);
    this.name = 'MoneyArithmeticError';
  }
}

function toBig(value: number | bigint, label: string): bigint {
  if (typeof value === 'bigint') return value;
  if (!Number.isSafeInteger(value)) throw new MoneyArithmeticError('NOT_AN_INTEGER', `${label} must be a safe integer, got ${value}`);
  return BigInt(value);
}

function toSafe(value: bigint, label: string): number {
  if (value > BigInt(Number.MAX_SAFE_INTEGER) || value < BigInt(Number.MIN_SAFE_INTEGER)) {
    throw new MoneyArithmeticError('OUT_OF_RANGE', `${label} does not fit a safe integer: ${value}`);
  }
  return Number(value);
}

/** numerator / denominator rounded half to even (banker's rounding). denominator > 0. */
export function divideHalfEven(numerator: number | bigint, denominator: number | bigint): number {
  const n = toBig(numerator, 'numerator');
  const d = toBig(denominator, 'denominator');
  if (d <= 0n) throw new MoneyArithmeticError('INVALID_DENOMINATOR', `denominator must be positive, got ${d}`);
  const negative = n < 0n;
  const abs = negative ? -n : n;
  let q = abs / d;
  const r = abs % d;
  const twice = r * 2n;
  if (twice > d || (twice === d && q % 2n === 1n)) q += 1n;
  return toSafe(negative ? -q : q, 'quotient');
}

/** amount × bps / 10 000, banker's rounding (e.g. the VAT of a net amount at 1500 bps). */
export function applyBpsHalfEven(amount: number, bps: number): number {
  return divideHalfEven(toBig(amount, 'amount') * toBig(bps, 'bps'), 10_000n);
}

/**
 * Split `total` over `weights` in proportion, in integers, summing to `total`
 * EXACTLY (largest-remainder / Hamilton method). Each part first gets the
 * floor of its exact share; the halalas left over go, one each, to the parts
 * with the largest remainders (ties: larger weight first, then input order).
 * A part with weight 0 gets 0. Deterministic for the same input.
 */
export function allocateProportionally(total: number, weights: readonly (number | bigint)[]): number[] {
  const t = toBig(total, 'total');
  if (t < 0n) throw new MoneyArithmeticError('NEGATIVE_TOTAL', `cannot allocate a negative amount ${total}`);
  const w = weights.map((x, i) => {
    const b = toBig(x, `weight[${i}]`);
    if (b < 0n) throw new MoneyArithmeticError('NEGATIVE_WEIGHT', `weight[${i}] is negative`);
    return b;
  });
  const sum = w.reduce((a, b) => a + b, 0n);
  if (sum === 0n) {
    if (t === 0n) return w.map(() => 0);
    throw new MoneyArithmeticError('NO_WEIGHT', `cannot allocate ${total} over zero weights`);
  }
  const parts = w.map((x) => (t * x) / sum);
  let left = t - parts.reduce((a, b) => a + b, 0n);
  const order = w
    .map((x, i) => ({ i, remainder: (t * x) % sum, weight: x }))
    .filter((e) => e.weight > 0n)
    .sort((a, b) => (a.remainder === b.remainder ? (a.weight === b.weight ? a.i - b.i : a.weight > b.weight ? -1 : 1) : a.remainder > b.remainder ? -1 : 1));
  for (const entry of order) {
    if (left === 0n) break;
    parts[entry.i] += 1n;
    left -= 1n;
  }
  return parts.map((p, i) => toSafe(p, `part[${i}]`));
}
