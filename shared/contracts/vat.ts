import type { OrderLine, OrderVatCategory } from './order';
import { allocateProportionally, applyBpsHalfEven } from './money';

/**
 * Order totals in integer halalas, computed EXACTLY the way the ZATCA tax
 * engine (services/zatca-tax/src/services/TaxCalculationEngine.ts) computes an
 * invoice:
 *
 *   per line      net = qty × unit − line discount
 *                 vat = round(net × rate / 10 000)   (rate: S = 1500 bps, Z / E / O = 0)
 *   per category  allowance share = the document allowance split over the
 *                 categories in proportion to their net (largest remainder:
 *                 the shares sum to the allowance exactly)
 *                 taxable = Σ net − share
 *                 vat     = max(0, Σ line vat − round_half_even(share × rate / 10 000))
 *   document      taxable = Σ taxable = Σ net − allowance,  vat = Σ vat,  total = taxable + vat
 *
 * The ledger posts its revenue / VAT split from this function and ZATCA
 * invoices with its own engine; a property test pins the two together and the
 * ZATCA consumer refuses (dead-letters) any settlement whose split differs
 * from the invoice it would issue, so the books and the tax authority can
 * never silently disagree by a halala.
 */
export const STANDARD_VAT_RATE_BPS = 1500;

export interface CategoryTotals {
  vatCategory: OrderVatCategory;
  vatRateBps: number;
  lineNetHalalas: number;
  allowanceHalalas: number;
  taxableHalalas: number;
  vatHalalas: number;
}

export interface OrderTotals {
  subtotalHalalas: number;
  lineDiscountHalalas: number;
  lineNetHalalas: number;
  allowanceHalalas: number;
  taxableHalalas: number;
  vatHalalas: number;
  totalHalalas: number;
  /** Per VAT category, in order of first appearance. */
  categories: CategoryTotals[];
}

export class OrderTotalsError extends Error {
  public readonly retryable = false;
  constructor(
    public readonly reason: string,
    message: string
  ) {
    super(message);
    this.name = 'OrderTotalsError';
  }
}

export function vatRateBps(category: OrderVatCategory | undefined): number {
  return (category ?? 'S') === 'S' ? STANDARD_VAT_RATE_BPS : 0;
}

export function computeOrderTotals(lines: readonly OrderLine[], allowanceHalalas = 0): OrderTotals {
  if (lines.length === 0) throw new OrderTotalsError('EMPTY_ORDER', 'An order needs at least one line');
  if (!Number.isSafeInteger(allowanceHalalas) || allowanceHalalas < 0) {
    throw new OrderTotalsError('INVALID_ALLOWANCE', `Invalid allowance ${allowanceHalalas}`);
  }

  let subtotal = 0;
  let discount = 0;
  let net = 0;
  const byCategory = new Map<OrderVatCategory, CategoryTotals>();

  for (const [index, line] of lines.entries()) {
    const gross = line.quantity * line.unitPriceHalalas;
    const lineDiscount = line.discountHalalas ?? 0;
    if (lineDiscount > gross) {
      throw new OrderTotalsError('INVALID_LINE_DISCOUNT', `Line ${index + 1} discount exceeds its amount`);
    }
    const lineNet = gross - lineDiscount;
    const category = line.vatCategory ?? 'S';
    const rate = vatRateBps(category);
    const lineVat = Math.round((lineNet * rate) / 10000);
    subtotal += gross;
    discount += lineDiscount;
    net += lineNet;
    const entry = byCategory.get(category);
    if (entry) {
      entry.lineNetHalalas += lineNet;
      entry.taxableHalalas += lineNet;
      entry.vatHalalas += lineVat;
    } else {
      byCategory.set(category, {
        vatCategory: category,
        vatRateBps: rate,
        lineNetHalalas: lineNet,
        allowanceHalalas: 0,
        taxableHalalas: lineNet,
        vatHalalas: lineVat,
      });
    }
  }

  if (allowanceHalalas > net) {
    throw new OrderTotalsError(
      'ALLOWANCE_EXCEEDS_NET',
      `Credit allowance ${allowanceHalalas} exceeds the order net amount ${net}`
    );
  }

  const categories = [...byCategory.values()];
  const shares = allocateProportionally(allowanceHalalas, categories.map((c) => c.lineNetHalalas));
  categories.forEach((c, i) => {
    c.allowanceHalalas = shares[i];
    c.taxableHalalas = c.lineNetHalalas - shares[i];
    c.vatHalalas = Math.max(0, c.vatHalalas - applyBpsHalfEven(shares[i], c.vatRateBps));
  });

  const taxable = net - allowanceHalalas;
  const vat = categories.reduce((sum, c) => sum + c.vatHalalas, 0);

  return {
    subtotalHalalas: subtotal,
    lineDiscountHalalas: discount,
    lineNetHalalas: net,
    allowanceHalalas,
    taxableHalalas: taxable,
    vatHalalas: vat,
    totalHalalas: taxable + vat,
    categories,
  };
}

/** "207.00" → 20700. Strict: non-negative, at most two decimals. */
export function sarToHalalas(amount: string): number {
  const match = /^(\d+)(?:\.(\d{1,2}))?$/.exec(amount.trim());
  if (!match) throw new OrderTotalsError('INVALID_AMOUNT', `Invalid SAR amount "${amount}"`);
  const value = Number(match[1]) * 100 + Number((match[2] ?? '').padEnd(2, '0'));
  if (!Number.isSafeInteger(value)) throw new OrderTotalsError('INVALID_AMOUNT', `SAR amount out of range "${amount}"`);
  return value;
}

/** 20700 → "207.00" */
export function halalasToSar(halalas: number): string {
  const sign = halalas < 0 ? '-' : '';
  const abs = Math.abs(halalas);
  return `${sign}${Math.floor(abs / 100)}.${String(abs % 100).padStart(2, '0')}`;
}
