import { describe, it, expect, beforeEach } from 'vitest';
import { TaxCalculationEngine } from '../src/services/TaxCalculationEngine';
import { UblXmlBuilder } from '../src/services/UblXmlBuilder';
import { buildApp } from '../src/server';
import { resetDatabaseAndSeed } from '../src/db';

describe('KSA VAT Calculation Engine & UBL 2.1 XML Builder', () => {
  beforeEach(async () => {
    await resetDatabaseAndSeed();
  });

  it('calculates 15% standard VAT (S) and line-level discounts in integer halalas', () => {
    const summary = TaxCalculationEngine.calculateInvoiceTotals([
      {
        itemNameAr: 'لابتوب احترافي',
        itemNameEn: 'Pro Laptop 16"',
        sku: 'LAP-16',
        quantity: 2,
        unitPriceHalalas: 450000, // 4,500.00 SAR x 2 = 9,000.00 SAR
        discountHalalas: 50000,   // 500.00 SAR line discount -> 8,500.00 SAR net
        vatCategory: 'S',         // 15% of 8,500.00 = 1,275.00 SAR (127,500 halalas)
      },
    ]);

    expect(summary.subtotalHalalas).toBe(900000);
    expect(summary.discountHalalas).toBe(50000);
    expect(summary.taxableAmountHalalas).toBe(850000);
    expect(summary.vatAmountHalalas).toBe(127500);
    expect(summary.totalAmountHalalas).toBe(977500);
    expect(TaxCalculationEngine.formatSar(summary.totalAmountHalalas)).toBe('9775.00');
  });

  it('handles mixed S, Z, E, and O VAT categories and resolves official VATEX-SA codes', () => {
    const summary = TaxCalculationEngine.calculateInvoiceTotals([
      {
        itemNameAr: 'استشارة تقنية',
        itemNameEn: 'IT Consulting',
        sku: 'SRV-IT',
        quantity: 1,
        unitPriceHalalas: 100000,
        vatCategory: 'S',
      },
      {
        itemNameAr: 'تصدير برمجيات للخارج',
        itemNameEn: 'Exported Software License',
        sku: 'EXP-SW',
        quantity: 1,
        unitPriceHalalas: 200000,
        vatCategory: 'Z',
        vatExemptionCode: 'VATEX-SA-33',
      },
      {
        itemNameAr: 'تمويل عقاري معفى',
        itemNameEn: 'Exempt Margin Financing Fee',
        sku: 'FIN-29',
        quantity: 1,
        unitPriceHalalas: 50000,
        vatCategory: 'E',
        vatExemptionCode: 'VATEX-SA-29',
      },
      {
        itemNameAr: 'رسوم حكومية خارج النطاق',
        itemNameEn: 'Out-of-Scope Municipal Fee',
        sku: 'GOV-OOS',
        quantity: 1,
        unitPriceHalalas: 10000,
        vatCategory: 'O',
      },
    ]);

    expect(summary.taxableAmountHalalas).toBe(360000);
    expect(summary.vatAmountHalalas).toBe(15000); // Only the 1,000 SAR 'S' line has 15% VAT
    expect(summary.totalAmountHalalas).toBe(375000);
    expect(summary.categorySubtotals).toHaveLength(4);
    expect(summary.lines[1].vatExemptionCode).toBe('VATEX-SA-33');
    expect(summary.lines[2].vatExemptionCode).toBe('VATEX-SA-29');
    expect(summary.lines[3].vatExemptionCode).toBe('VATEX-SA-OOS');
  });

  it('rejects mismatched VAT exemption codes (BR-KSA-DEC-01)', () => {
    expect(() =>
      TaxCalculationEngine.calculateInvoiceTotals([
        {
          itemNameAr: 'منتج قياسي',
          itemNameEn: 'Standard Item with Invalid Exemption',
          sku: 'BAD-01',
          quantity: 1,
          unitPriceHalalas: 10000,
          vatCategory: 'S',
          vatExemptionCode: 'VATEX-SA-32',
        },
      ])
    ).toThrow('INVALID_EXEMPTION_ON_STANDARD_RATE');

    expect(() =>
      TaxCalculationEngine.calculateInvoiceTotals([
        {
          itemNameAr: 'منتج صفري',
          itemNameEn: 'Zero Rated with Exempt Code',
          sku: 'BAD-02',
          quantity: 1,
          unitPriceHalalas: 10000,
          vatCategory: 'Z',
          vatExemptionCode: 'VATEX-SA-29', // E code used on Z category
        },
      ])
    ).toThrow('INVALID_VAT_EXEMPTION_CODE');
  });

  it('applies document-level allowances and reduces taxable and VAT totals proportionally', () => {
    const summary = TaxCalculationEngine.calculateInvoiceTotals(
      [
        {
          itemNameAr: 'اشتراك سنوي',
          itemNameEn: 'Annual SaaS Plan',
          sku: 'SAAS-YR',
          quantity: 1,
          unitPriceHalalas: 200000, // 2,000.00 SAR
          vatCategory: 'S',
        },
      ],
      20000 // 200.00 SAR document-level allowance -> 1,800.00 SAR taxable, 270.00 SAR VAT
    );

    expect(summary.subtotalHalalas).toBe(200000);
    expect(summary.documentAllowanceHalalas).toBe(20000);
    expect(summary.taxableAmountHalalas).toBe(180000);
    expect(summary.vatAmountHalalas).toBe(27000);
    expect(summary.totalAmountHalalas).toBe(207000);
  });

  it('rounds fractional halala VAT amounts using standard half-up rounding', () => {
    const summary = TaxCalculationEngine.calculateInvoiceTotals([
      {
        itemNameAr: 'قطعة غيار',
        itemNameEn: 'Precision Spare Part',
        sku: 'PRT-33',
        quantity: 1,
        unitPriceHalalas: 333, // 3.33 SAR * 15% = 49.95 halalas -> rounds to 50 halalas
        vatCategory: 'S',
      },
    ]);

    expect(summary.vatAmountHalalas).toBe(50);
    expect(summary.totalAmountHalalas).toBe(383);
  });

  it('builds a valid OASIS UBL 2.1 XML document containing ICV, PIH, QR, and Supplier VAT', async () => {
    const { app, invoiceService } = await buildApp();

    const draft = await invoiceService.createDraftInvoice({
      taxpayerId: 'txp_riyadh_retail',
      customerName: 'Abdullah Al-Ghamdi',
      lines: [
        {
          itemNameAr: 'شاشة 4K',
          itemNameEn: '4K Monitor 27"',
          sku: 'MON-4K',
          quantity: 1,
          unitPriceHalalas: 150000,
          vatCategory: 'S',
        },
      ],
    });

    const issued = await invoiceService.issueInvoice(draft.id);
    const taxpayer = await invoiceService.getTaxpayer('txp_riyadh_retail');
    const xml = UblXmlBuilder.buildUblInvoiceXml(issued, taxpayer);

    expect(xml).toContain('<cbc:ProfileID>reporting:1.0</cbc:ProfileID>');
    expect(xml).toContain(`<cbc:UUID>${issued.uuid}</cbc:UUID>`);
    expect(xml).toContain('<cbc:CompanyID>310123456700003</cbc:CompanyID>');
    expect(xml).toContain(`<cbc:EmbeddedDocumentBinaryObject mimeCode="text/plain">${issued.tlv_qr_base64}</cbc:EmbeddedDocumentBinaryObject>`);
    expect(xml).toContain('<cbc:TaxInclusiveAmount currencyID="SAR">1725.00</cbc:TaxInclusiveAmount>');

    await app.close();
  });
});
