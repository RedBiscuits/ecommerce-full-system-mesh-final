import { z } from 'zod';
import {
  STANDARD_VAT_RATE_BPS,
  allocateProportionally,
  computeOrderTotals,
  divideHalfEven,
  type SettlementFee,
} from '../../../../shared/contracts';

/**
 * Tiered processing-fee pricing for merchant settlements, in integer halalas.
 *
 *   percentage fee = Σ over the marginal tiers of (part of the gross in the tier × tier bps)
 *                    / 10 000, rounded ONCE with banker's rounding (no per-tier rounding drift);
 *                    each tier's share is then allocated back exactly (largest remainder)
 *   fixed fee      = perTransactionHalalas × settled transactions
 *   net fee        = max(percentage + fixed, minimum)              — the platform's revenue
 *   VAT            = VAT of the net fee computed EXACTLY as the ZATCA engine computes the fee
 *                    invoice (one standard-rated line), so the fee invoice always reproduces it
 *   total fee      = net + VAT, never more than the gross (capped: the largest net whose
 *                    net + VAT fits the gross)
 *   payout         = gross − total fee
 *
 * All products go through BigInt; nothing is ever held as a float.
 */

export const FeeTierSchema = z.object({
  /** Upper bound (exclusive of the next tier) of the gross this tier applies to; null = unbounded. */
  upToHalalas: z.number().int().positive().safe().nullable(),
  bps: z.number().int().min(0).max(10_000),
});

export const SettlementFeeRuleSchema = z
  .object({
    ruleId: z.string().min(1).max(64),
    version: z.number().int().positive(),
    merchantId: z.string().min(1).max(64).nullable(),
    tiers: z.array(FeeTierSchema).min(1).max(20),
    perTransactionHalalas: z.number().int().nonnegative().safe(),
    minimumFeeHalalas: z.number().int().nonnegative().safe(),
    vatRateBps: z.literal(STANDARD_VAT_RATE_BPS),
  })
  .superRefine((rule, ctx) => {
    let previous = 0;
    rule.tiers.forEach((tier, i) => {
      const last = i === rule.tiers.length - 1;
      if (last !== (tier.upToHalalas === null)) {
        ctx.addIssue({ code: z.ZodIssueCode.custom, message: 'only the last tier is unbounded (upToHalalas: null)', path: ['tiers', i] });
      }
      if (tier.upToHalalas !== null) {
        if (tier.upToHalalas <= previous) {
          ctx.addIssue({ code: z.ZodIssueCode.custom, message: 'tier bounds must be strictly ascending', path: ['tiers', i] });
        }
        previous = tier.upToHalalas;
      }
    });
  });
export type SettlementFeeRule = z.infer<typeof SettlementFeeRuleSchema>;

export class SettlementFeeError extends Error {
  public readonly retryable = false;
  constructor(
    public readonly reason: string,
    message: string
  ) {
    super(message);
    this.name = 'SettlementFeeError';
  }
}

export interface FeeQuote {
  fee: SettlementFee;
  payoutHalalas: number;
}

/** VAT on a net fee, exactly as ZATCA's engine computes a one-line standard-rated invoice. */
export function feeVatHalalas(netHalalas: number): number {
  if (netHalalas === 0) return 0;
  return computeOrderTotals(
    [{ sku: 'SETTLEMENT-FEE', nameEn: 'fee', nameAr: 'رسوم', quantity: 1, unitPriceHalalas: netHalalas, vatCategory: 'S' }],
    0
  ).vatHalalas;
}

/** The largest net fee whose net + VAT does not exceed `gross`. */
export function maxNetFeeWithin(grossHalalas: number): number {
  let net = Number((BigInt(grossHalalas) * 10_000n) / BigInt(10_000 + STANDARD_VAT_RATE_BPS));
  while (net > 0 && net + feeVatHalalas(net) > grossHalalas) net -= 1;
  while (net + 1 + feeVatHalalas(net + 1) <= grossHalalas) net += 1;
  return net;
}

export function calculateSettlementFee(rule: SettlementFeeRule, grossHalalas: number, itemCount: number): FeeQuote {
  const parsed = SettlementFeeRuleSchema.safeParse(rule);
  if (!parsed.success) {
    throw new SettlementFeeError('INVALID_FEE_RULE', `Fee rule ${rule.ruleId} v${rule.version} is invalid: ${parsed.error.message}`);
  }
  if (!Number.isSafeInteger(grossHalalas) || grossHalalas <= 0) {
    throw new SettlementFeeError('INVALID_GROSS', `gross must be a positive integer of halalas, got ${grossHalalas}`);
  }
  if (!Number.isSafeInteger(itemCount) || itemCount <= 0) {
    throw new SettlementFeeError('INVALID_ITEM_COUNT', `itemCount must be a positive integer, got ${itemCount}`);
  }

  const gross = BigInt(grossHalalas);
  let lower = 0n;
  const bands: Array<{ fromHalalas: number; toHalalas: number | null; bps: number; base: bigint }> = [];
  for (const tier of rule.tiers) {
    const upper = tier.upToHalalas === null ? null : BigInt(tier.upToHalalas);
    const top = upper === null || gross < upper ? gross : upper;
    const base = top > lower ? top - lower : 0n;
    bands.push({ fromHalalas: Number(lower), toHalalas: tier.upToHalalas, bps: tier.bps, base });
    if (upper !== null) lower = upper;
  }
  const exact = bands.map((b) => b.base * BigInt(b.bps)); // halalas × 1/10 000
  const percentageFee = divideHalfEven(
    exact.reduce((a, b) => a + b, 0n),
    10_000n
  );
  // Per-tier shares of the (once-rounded) percentage fee, summing to it exactly.
  const shares = exact.some((w) => w > 0n) ? allocateProportionally(percentageFee, exact) : exact.map(() => 0);

  const fixedFee = Number(BigInt(rule.perTransactionHalalas) * BigInt(itemCount));
  if (!Number.isSafeInteger(fixedFee)) throw new SettlementFeeError('FEE_OUT_OF_RANGE', 'fixed fee does not fit a safe integer');
  let net = Math.max(percentageFee + fixedFee, rule.minimumFeeHalalas);
  let vat = feeVatHalalas(net);
  let capped = false;
  if (net + vat > grossHalalas) {
    net = maxNetFeeWithin(grossHalalas);
    vat = feeVatHalalas(net);
    capped = true;
  }
  const total = net + vat;

  return {
    fee: {
      ruleId: rule.ruleId,
      ruleVersion: rule.version,
      tiers: bands.map((b, i) => ({
        fromHalalas: b.fromHalalas,
        toHalalas: b.toHalalas,
        bps: b.bps,
        baseHalalas: Number(b.base),
        feeHalalas: shares[i],
      })),
      percentageFeeHalalas: percentageFee,
      perTransactionHalalas: rule.perTransactionHalalas,
      fixedFeeHalalas: fixedFee,
      minimumFeeHalalas: rule.minimumFeeHalalas,
      netHalalas: net,
      vatRateBps: rule.vatRateBps,
      vatHalalas: vat,
      totalHalalas: total,
      capped,
    },
    payoutHalalas: grossHalalas - total,
  };
}
