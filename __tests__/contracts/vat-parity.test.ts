import { describe, expect, it } from 'vitest';
import {
  MoneyArithmeticError,
  OrderLine,
  OrderTotalsError,
  allocateProportionally,
  applyBpsHalfEven,
  computeOrderTotals,
  divideHalfEven,
  halalasToSar,
  sarToHalalas,
} from '../../shared/contracts';
import { TaxCalculationEngine } from '../../services/zatca-tax/src/services/TaxCalculationEngine';

/**
 * The wallet ledger splits a payment into merchant revenue and VAT with the
 * shared `computeOrderTotals`; ZATCA invoices with its own TaxCalculationEngine.
 * They must agree to the halala on every possible order, or the books and the
 * reported tax would drift apart.
 */
function prng(seed: number): () => number {
  let a = seed;
  return () => {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const EXEMPTIONS = { Z: 'VATEX-SA-32', E: 'VATEX-SA-29', O: 'VATEX-SA-OOS' } as const;

function randomOrder(rand: () => number): { lines: OrderLine[]; allowance: number } {
  const count = 1 + Math.floor(rand() * 6);
  const lines: OrderLine[] = [];
  for (let i = 0; i < count; i++) {
    const quantity = 1 + Math.floor(rand() * 9);
    const unit = Math.floor(rand() * 250_000);
    const gross = quantity * unit;
    const discount = rand() < 0.4 ? Math.floor(rand() * (gross + 1)) : 0;
    const r = rand();
    const category = r < 0.7 ? 'S' : r < 0.8 ? 'Z' : r < 0.9 ? 'E' : 'O';
    lines.push({
      sku: `SKU-${i}`,
      nameEn: `Item ${i}`,
      nameAr: `صنف ${i}`,
      quantity,
      unitPriceHalalas: unit,
      discountHalalas: discount,
      vatCategory: category,
      ...(category === 'S' ? {} : { vatExemptionCode: EXEMPTIONS[category] }),
    });
  }
  const net = lines.reduce((s, l) => s + l.quantity * l.unitPriceHalalas - (l.discountHalalas ?? 0), 0);
  const allowance = rand() < 0.6 ? Math.floor(rand() * (net + 1)) : 0;
  return { lines, allowance };
}

describe('VAT parity: shared order totals == ZATCA TaxCalculationEngine', () => {
  it('agrees to the halala on 5,000 random orders (mixed categories, line discounts, credit allowances)', () => {
    const rand = prng(2026);
    for (let i = 0; i < 5000; i++) {
      const { lines, allowance } = randomOrder(rand);
      const shared = computeOrderTotals(lines, allowance);
      const zatca = TaxCalculationEngine.calculateInvoiceTotals(
        lines.map((l) => ({
          itemNameAr: l.nameAr,
          itemNameEn: l.nameEn,
          sku: l.sku,
          quantity: l.quantity,
          unitPriceHalalas: l.unitPriceHalalas,
          discountHalalas: l.discountHalalas,
          vatCategory: l.vatCategory,
          vatExemptionCode: l.vatExemptionCode,
        })),
        allowance
      );
      expect({
        subtotal: shared.subtotalHalalas,
        discount: shared.lineDiscountHalalas,
        taxable: shared.taxableHalalas,
        vat: shared.vatHalalas,
        total: shared.totalHalalas,
        categories: shared.categories.map((c) => [c.vatCategory, c.vatRateBps, c.lineNetHalalas, c.allowanceHalalas, c.taxableHalalas, c.vatHalalas]),
      }).toEqual({
        subtotal: zatca.subtotalHalalas,
        discount: zatca.discountHalalas,
        taxable: zatca.taxableAmountHalalas,
        vat: zatca.vatAmountHalalas,
        total: zatca.totalAmountHalalas,
        categories: zatca.categorySubtotals.map((c) => [c.vatCategory, c.vatRateBps, c.lineNetHalalas, c.allowanceHalalas, c.taxableAmountHalalas, c.vatAmountHalalas]),
      });
      // Conservation: the allowance is split without losing or inventing a halala.
      expect(shared.categories.reduce((s, c) => s + c.allowanceHalalas, 0)).toBe(allowance);
      expect(shared.categories.reduce((s, c) => s + c.taxableHalalas, 0)).toBe(shared.taxableHalalas);
      expect(shared.categories.reduce((s, c) => s + c.vatHalalas, 0)).toBe(shared.vatHalalas);
      for (const c of shared.categories) {
        expect(c.allowanceHalalas).toBeGreaterThanOrEqual(0);
        expect(c.allowanceHalalas).toBeLessThanOrEqual(c.lineNetHalalas);
        if (c.vatRateBps === 0) expect(c.vatHalalas).toBe(0);
      }
      // The ledger's 3-leg split always balances against what the customer pays.
      expect(shared.taxableHalalas + shared.vatHalalas).toBe(shared.totalHalalas);
    }
  });

  it('computes the canonical example: 200 SAR of standard-rated goods with a 20 SAR credit allowance', () => {
    const totals = computeOrderTotals(
      [{ sku: 'A', nameEn: 'Router', nameAr: 'موجه', quantity: 2, unitPriceHalalas: 10000 }],
      2000
    );
    expect(totals).toMatchObject({
      subtotalHalalas: 20000,
      lineDiscountHalalas: 0,
      lineNetHalalas: 20000,
      allowanceHalalas: 2000,
      taxableHalalas: 18000,
      vatHalalas: 2700,
      totalHalalas: 20700,
    });
  });

  it('splits a promotion over mixed S (1500 bps), Z (0) and E (0, VATEX-SA-29) lines with exact halala parity', () => {
    const lines: OrderLine[] = [
      { sku: 'RTR', nameEn: 'Router', nameAr: 'موجه', quantity: 1, unitPriceHalalas: 10000, vatCategory: 'S' },
      { sku: 'EXP', nameEn: 'Export', nameAr: 'تصدير', quantity: 1, unitPriceHalalas: 10000, vatCategory: 'Z', vatExemptionCode: 'VATEX-SA-32' },
      { sku: 'FIN', nameEn: 'Financing fee', nameAr: 'رسوم تمويل', quantity: 1, unitPriceHalalas: 5000, vatCategory: 'E', vatExemptionCode: 'VATEX-SA-29' },
    ];
    const totals = computeOrderTotals(lines, 2500);
    expect(totals.categories).toEqual([
      { vatCategory: 'S', vatRateBps: 1500, lineNetHalalas: 10000, allowanceHalalas: 1000, taxableHalalas: 9000, vatHalalas: 1350 },
      { vatCategory: 'Z', vatRateBps: 0, lineNetHalalas: 10000, allowanceHalalas: 1000, taxableHalalas: 9000, vatHalalas: 0 },
      { vatCategory: 'E', vatRateBps: 0, lineNetHalalas: 5000, allowanceHalalas: 500, taxableHalalas: 4500, vatHalalas: 0 },
    ]);
    // Only the S share of the promotion reduces VAT: 15.00 − 15 % × 10.00 = 13.50 SAR.
    expect(totals).toMatchObject({ taxableHalalas: 22500, vatHalalas: 1350, totalHalalas: 23850 });
  });

  it('never reduces standard-rated VAT with the share of a promotion that belongs to Z / E lines', () => {
    const lines: OrderLine[] = [
      { sku: 'S', nameEn: 's', nameAr: 's', quantity: 1, unitPriceHalalas: 10000 },
      { sku: 'Z', nameEn: 'z', nameAr: 'z', quantity: 1, unitPriceHalalas: 10000, vatCategory: 'Z', vatExemptionCode: 'VATEX-SA-33' },
    ];
    // A 100.00 SAR promotion over 100.00 S + 100.00 Z: 50.00 each, VAT on the remaining 50.00 S.
    expect(computeOrderTotals(lines, 10000)).toMatchObject({ taxableHalalas: 10000, vatHalalas: 750, totalHalalas: 10750 });
  });

  it('rejects an allowance larger than the order net amount, like ZATCA does', () => {
    const lines: OrderLine[] = [{ sku: 'A', nameEn: 'a', nameAr: 'a', quantity: 1, unitPriceHalalas: 100 }];
    expect(() => computeOrderTotals(lines, 101)).toThrow(OrderTotalsError);
    expect(() => TaxCalculationEngine.calculateInvoiceTotals(
      [{ itemNameAr: 'a', itemNameEn: 'a', sku: 'A', quantity: 1, unitPriceHalalas: 100 }],
      101
    )).toThrow('INVALID_DOCUMENT_ALLOWANCE_EXCEEDS_NET');
  });

  it('converts SAR strings and halalas exactly', () => {
    expect(sarToHalalas('207.00')).toBe(20700);
    expect(sarToHalalas('0.5')).toBe(50);
    expect(sarToHalalas('12')).toBe(1200);
    expect(halalasToSar(20700)).toBe('207.00');
    expect(halalasToSar(5)).toBe('0.05');
    expect(() => sarToHalalas('1.234')).toThrow(OrderTotalsError);
    expect(() => sarToHalalas('-1.00')).toThrow(OrderTotalsError);
  });
});

describe('exact money primitives (banker\'s rounding, largest remainder)', () => {
  it('rounds half to even, without bias', () => {
    expect([5, 15, 25, 35, -5, -15].map((n) => divideHalfEven(n, 10))).toEqual([0, 2, 2, 4, 0, -2]);
    expect(divideHalfEven(149, 10)).toBe(15);
    expect(divideHalfEven(151, 10)).toBe(15);
    expect(applyBpsHalfEven(1010, 1500)).toBe(152); // 151.5 → 152 (even)
    expect(applyBpsHalfEven(1030, 1500)).toBe(154); // 154.5 → 154 (even)
    // Over every amount 0..9 999 at 15 %, rounding never drifts by more than half a halala in total.
    let exact = 0;
    let rounded = 0;
    for (let n = 0; n < 10000; n++) {
      exact += (n * 1500) / 10000;
      rounded += applyBpsHalfEven(n, 1500);
    }
    expect(Math.abs(rounded - exact)).toBeLessThanOrEqual(0.5);
  });

  it('splits an amount so the parts always sum back exactly', () => {
    expect(allocateProportionally(1000, [1, 1, 1])).toEqual([334, 333, 333]);
    expect(allocateProportionally(100, [0, 3, 1])).toEqual([0, 75, 25]);
    expect(allocateProportionally(0, [5, 5])).toEqual([0, 0]);
    expect(allocateProportionally(7, [2, 3, 5])).toEqual([1, 2, 4]);
    const rand = prng(99);
    for (let i = 0; i < 2000; i++) {
      const weights = Array.from({ length: 1 + Math.floor(rand() * 6) }, () => Math.floor(rand() * 1e9));
      if (weights.every((w) => w === 0)) continue;
      const total = Math.floor(rand() * 1e10);
      const parts = allocateProportionally(total, weights);
      expect(parts.reduce((a, b) => a + b, 0)).toBe(total);
      parts.forEach((p, j) => {
        const exact = (total * weights[j]) / weights.reduce((a, b) => a + b, 0);
        expect(Math.abs(p - exact)).toBeLessThan(1 + 1e-6);
      });
    }
  });

  it('works beyond 2^53 intermediate products and refuses nonsense', () => {
    expect(allocateProportionally(9_000_000_000_000, [3_000_000_000_000, 6_000_000_000_000])).toEqual([3_000_000_000_000, 6_000_000_000_000]);
    expect(() => allocateProportionally(5, [0, 0])).toThrow(MoneyArithmeticError);
    expect(() => allocateProportionally(-1, [1])).toThrow(MoneyArithmeticError);
    expect(() => divideHalfEven(1, 0)).toThrow(MoneyArithmeticError);
    expect(() => applyBpsHalfEven(1.5, 1500)).toThrow(MoneyArithmeticError);
  });
});
