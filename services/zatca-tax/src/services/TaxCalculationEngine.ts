import {
  CalculatedLineItem,
  InvoiceLineInput,
  InvoiceWithLines,
  VatCategory,
  VatExemptionCode,
} from '../types/zatca';
import { allocateProportionally, applyBpsHalfEven } from '../../../../shared/contracts/money';

const VAT_RATES_BPS: Record<VatCategory, number> = {
  S: 1500, // 15.00% KSA Standard Rate
  Z: 0,    // 0.00% Zero-rated export / qualifying medical & educational
  E: 0,    // 0.00% Exempt financial / residential real estate
  O: 0,    // 0.00% Out of VAT scope
};

const VALID_EXEMPTION_CODES_BY_CATEGORY: Record<Exclude<VatCategory, 'S'>, VatExemptionCode[]> = {
  Z: ['VATEX-SA-32', 'VATEX-SA-33', 'VATEX-SA-34-1', 'VATEX-SA-EDU', 'VATEX-SA-HEA'],
  E: ['VATEX-SA-29', 'VATEX-SA-29-7', 'VATEX-SA-30'],
  O: ['VATEX-SA-OOS'],
};

const DEFAULT_EXEMPTION_REASONS: Record<VatExemptionCode, string> = {
  'VATEX-SA-29': 'Financial services mentioned in Article 29 of the VAT Regulations',
  'VATEX-SA-29-7': 'Life insurance services mentioned in Article 29 of the VAT Regulations',
  'VATEX-SA-30': 'Real estate transactions mentioned in Article 30 of the VAT Regulations',
  'VATEX-SA-32': 'Export of goods from the Kingdom of Saudi Arabia',
  'VATEX-SA-33': 'Export of services from the Kingdom of Saudi Arabia',
  'VATEX-SA-34-1': 'International transport of goods and passengers',
  'VATEX-SA-EDU': 'Private education to Saudi citizens',
  'VATEX-SA-HEA': 'Private healthcare to Saudi citizens',
  'VATEX-SA-OOS': 'Supply outside the scope of KSA Value Added Tax',
};

export interface VatCategorySubtotal {
  vatCategory: VatCategory;
  vatRateBps: number;
  /** Σ line net amounts of the category (before the document-level allowance). */
  lineNetHalalas: number;
  /** The category's share of the document-level allowance. */
  allowanceHalalas: number;
  /** Category taxable amount = line nets − its allowance share. */
  taxableAmountHalalas: number;
  /** Category VAT = Σ line VAT − VAT on its allowance share (banker's rounding, category rate). */
  vatAmountHalalas: number;
  exemptionCode: VatExemptionCode | null;
  exemptionReason: string | null;
}

export interface CalculationOptions {
  /**
   * How far an explicit line VAT (`InvoiceLineInput.vatAmountHalalas`) may deviate from
   * net × rate, in halalas. Default 0: explicit VAT must equal the computed VAT.
   */
  explicitVatToleranceHalalas?: number;
}

export interface InvoiceCalculationSummary {
  lines: CalculatedLineItem[];
  categorySubtotals: VatCategorySubtotal[];
  subtotalHalalas: number;
  discountHalalas: number;
  documentAllowanceHalalas: number;
  taxableAmountHalalas: number;
  vatAmountHalalas: number;
  totalAmountHalalas: number;
}

export class TaxCalculationEngine {
  static getVatRateBps(category: VatCategory): number {
    return VAT_RATES_BPS[category] ?? 1500;
  }

  static formatSar(halalas: number): string {
    return (halalas / 100).toFixed(2);
  }

  static resolveExemptionMetadata(
    category: VatCategory,
    code?: VatExemptionCode,
    reason?: string
  ): { code: VatExemptionCode | null; reason: string | null } {
    if (category === 'S') {
      if (code) {
        throw new Error('INVALID_EXEMPTION_ON_STANDARD_RATE');
      }
      return { code: null, reason: null };
    }

    const allowedCodes = VALID_EXEMPTION_CODES_BY_CATEGORY[category];
    const resolvedCode = code ?? allowedCodes[0];

    if (!allowedCodes.includes(resolvedCode)) {
      throw new Error(`INVALID_VAT_EXEMPTION_CODE:${resolvedCode}_FOR_${category}`);
    }

    const resolvedReason = reason?.trim() || DEFAULT_EXEMPTION_REASONS[resolvedCode];
    return { code: resolvedCode, reason: resolvedReason };
  }

  static calculateInvoiceTotals(
    items: InvoiceLineInput[],
    documentAllowanceHalalas = 0,
    options: CalculationOptions = {}
  ): InvoiceCalculationSummary {
    if (!items || items.length === 0) {
      throw new Error('EMPTY_LINE_ITEMS');
    }
    if (documentAllowanceHalalas < 0 || !Number.isInteger(documentAllowanceHalalas)) {
      throw new Error('INVALID_DOCUMENT_ALLOWANCE');
    }

    let subtotalHalalas = 0;
    let discountHalalas = 0;
    let lineNetSumHalalas = 0;

    const subtotalsMap = new Map<VatCategory, VatCategorySubtotal>();

    const lines: CalculatedLineItem[] = items.map((item, idx) => {
      if (item.quantity <= 0 || !Number.isInteger(item.quantity)) {
        throw new Error(`INVALID_LINE_QUANTITY at line ${idx + 1}`);
      }
      if (item.unitPriceHalalas < 0 || !Number.isInteger(item.unitPriceHalalas)) {
        throw new Error(`INVALID_UNIT_PRICE at line ${idx + 1}`);
      }

      const grossLine = item.quantity * item.unitPriceHalalas;
      const lineDiscount = item.discountHalalas ?? 0;
      if (lineDiscount < 0 || lineDiscount > grossLine) {
        throw new Error(`INVALID_LINE_DISCOUNT at line ${idx + 1}`);
      }

      const netAmount = grossLine - lineDiscount;
      const vatCategory: VatCategory = item.vatCategory ?? 'S';
      const vatRateBps = this.getVatRateBps(vatCategory);
      const exemption = this.resolveExemptionMetadata(
        vatCategory,
        item.vatExemptionCode,
        item.vatExemptionReason
      );

      const computedVat = Math.round((netAmount * vatRateBps) / 10000);
      let vatAmount = computedVat;
      if (item.vatAmountHalalas !== undefined) {
        // Pro-rata credit / debit notes carry the VAT actually being credited back: the same share
        // of the VAT the referenced invoice charged. It may differ from net × rate by the rounding
        // that invoice accumulated over its lines and allowance, within the caller's tolerance.
        if (!Number.isInteger(item.vatAmountHalalas) || item.vatAmountHalalas < 0) {
          throw new Error(`INVALID_LINE_VAT at line ${idx + 1}`);
        }
        const tolerance = options.explicitVatToleranceHalalas ?? 0;
        if (vatRateBps === 0 && item.vatAmountHalalas !== 0) {
          throw new Error(`INVALID_LINE_VAT at line ${idx + 1}: category ${vatCategory} carries no VAT`);
        }
        if (Math.abs(item.vatAmountHalalas - computedVat) > tolerance) {
          throw new Error(`INVALID_LINE_VAT at line ${idx + 1}: ${item.vatAmountHalalas} is not within ${tolerance} halalas of ${computedVat}`);
        }
        vatAmount = item.vatAmountHalalas;
      }
      const lineTotal = netAmount + vatAmount;

      subtotalHalalas += grossLine;
      discountHalalas += lineDiscount;
      lineNetSumHalalas += netAmount;

      const existingSub = subtotalsMap.get(vatCategory);
      if (existingSub) {
        existingSub.lineNetHalalas += netAmount;
        existingSub.taxableAmountHalalas += netAmount;
        existingSub.vatAmountHalalas += vatAmount;
      } else {
        subtotalsMap.set(vatCategory, {
          vatCategory,
          vatRateBps,
          lineNetHalalas: netAmount,
          allowanceHalalas: 0,
          taxableAmountHalalas: netAmount,
          vatAmountHalalas: vatAmount,
          exemptionCode: exemption.code,
          exemptionReason: exemption.reason,
        });
      }

      return {
        lineNumber: idx + 1,
        itemNameAr: item.itemNameAr,
        itemNameEn: item.itemNameEn,
        sku: item.sku,
        quantity: item.quantity,
        unitPriceHalalas: item.unitPriceHalalas,
        discountHalalas: lineDiscount,
        netAmountHalalas: netAmount,
        vatCategory,
        vatRateBps,
        vatExemptionCode: exemption.code,
        vatExemptionReason: exemption.reason,
        vatAmountHalalas: vatAmount,
        lineTotalHalalas: lineTotal,
      };
    });

    if (documentAllowanceHalalas > lineNetSumHalalas) {
      throw new Error('INVALID_DOCUMENT_ALLOWANCE_EXCEEDS_NET');
    }

    // Document-level allowance (e.g. a promotional credit): split over the VAT categories in
    // proportion to their net amounts (largest remainder: the shares sum to the allowance exactly),
    // then each category's VAT is reduced by the VAT of its own share at its own rate (banker's
    // rounding). A zero-rated or exempt share therefore never reduces standard-rated VAT.
    const categorySubtotals = Array.from(subtotalsMap.values());
    const shares = allocateProportionally(
      documentAllowanceHalalas,
      categorySubtotals.map((c) => c.lineNetHalalas)
    );
    categorySubtotals.forEach((category, i) => {
      const share = shares[i];
      category.allowanceHalalas = share;
      category.taxableAmountHalalas = category.lineNetHalalas - share;
      category.vatAmountHalalas = Math.max(0, category.vatAmountHalalas - applyBpsHalfEven(share, category.vatRateBps));
    });

    const taxableAmountHalalas = lineNetSumHalalas - documentAllowanceHalalas;
    const vatAmountHalalas = categorySubtotals.reduce((sum, c) => sum + c.vatAmountHalalas, 0);
    const totalAmountHalalas = taxableAmountHalalas + vatAmountHalalas;

    return {
      lines,
      categorySubtotals,
      subtotalHalalas,
      discountHalalas,
      documentAllowanceHalalas,
      taxableAmountHalalas,
      vatAmountHalalas,
      totalAmountHalalas,
    };
  }

  /**
   * Per-VAT-category breakdown of a stored invoice (allowance share, taxable amount, VAT),
   * recomputed from its lines and document allowance with the same engine that priced it.
   */
  static breakdownForInvoice(invoice: Pick<InvoiceWithLines, 'lines' | 'document_allowance_halalas'>): VatCategorySubtotal[] {
    if (invoice.lines.length === 0) return [];
    return this.calculateInvoiceTotals(
      invoice.lines.map((l) => ({
        itemNameAr: l.item_name_ar,
        itemNameEn: l.item_name_en,
        sku: l.sku,
        quantity: l.quantity,
        unitPriceHalalas: l.unit_price_halalas,
        discountHalalas: l.discount_halalas,
        vatCategory: l.vat_category,
        vatExemptionCode: l.vat_exemption_code ?? undefined,
        vatExemptionReason: l.vat_exemption_reason ?? undefined,
        // The VAT as issued (validated when the document was created): the breakdown must
        // describe the document, not recompute it.
        vatAmountHalalas: l.vat_amount_halalas,
      })),
      invoice.document_allowance_halalas,
      { explicitVatToleranceHalalas: Number.MAX_SAFE_INTEGER }
    ).categorySubtotals;
  }
}
