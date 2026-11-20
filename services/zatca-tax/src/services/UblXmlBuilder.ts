import { InvoiceWithLines, TaxpayerRecord } from '../types/zatca';
import { TaxCalculationEngine, VatCategorySubtotal } from './TaxCalculationEngine';

/** UNCL 5189 allowance reason code "Discount" (used for promotional credits). */
export const ALLOWANCE_REASON_CODE_DISCOUNT = '95';

function taxCategoryXml(category: VatCategorySubtotal, indent: string, withExemption: boolean): string {
  const exemption =
    withExemption && category.exemptionCode
      ? `
${indent}  <cbc:TaxExemptionReasonCode>${escapeXml(category.exemptionCode)}</cbc:TaxExemptionReasonCode>
${indent}  <cbc:TaxExemptionReason>${escapeXml(category.exemptionReason ?? '')}</cbc:TaxExemptionReason>`
      : '';
  return `${indent}<cac:TaxCategory>
${indent}  <cbc:ID>${category.vatCategory}</cbc:ID>
${indent}  <cbc:Percent>${(category.vatRateBps / 100).toFixed(2)}</cbc:Percent>${exemption}
${indent}  <cac:TaxScheme>
${indent}    <cbc:ID>VAT</cbc:ID>
${indent}  </cac:TaxScheme>
${indent}</cac:TaxCategory>`;
}

function escapeXml(str: string): string {
  return str
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&apos;');
}

export class UblXmlBuilder {
  static buildUblInvoiceXml(invoice: InvoiceWithLines, taxpayer: TaxpayerRecord): string {
    const issuedDateObj = invoice.issued_at ? new Date(invoice.issued_at) : new Date();
    const issueDate = issuedDateObj.toISOString().slice(0, 10);
    const issueTime = issuedDateObj.toISOString().slice(11, 19);

    const billingRefXml = invoice.billing_reference_id
      ? `
  <cac:BillingReference>
    <cac:InvoiceDocumentReference>
      <cbc:ID>${escapeXml(invoice.billing_reference_id)}</cbc:ID>
    </cac:InvoiceDocumentReference>
  </cac:BillingReference>`
      : '';

    // B2B (standard) documents identify the buyer by its VAT registration number.
    const buyerTaxXml = invoice.customer_vat_number
      ? `
      <cac:PartyTaxScheme>
        <cbc:CompanyID>${escapeXml(invoice.customer_vat_number)}</cbc:CompanyID>
        <cac:TaxScheme>
          <cbc:ID>VAT</cbc:ID>
        </cac:TaxScheme>
      </cac:PartyTaxScheme>`
      : '';
    // Credit / debit notes state why they were issued (KSA-10).
    const instructionNoteXml = invoice.instruction_note
      ? `
    <cbc:InstructionNote>${escapeXml(invoice.instruction_note)}</cbc:InstructionNote>`
      : '';

    // Document-level allowance (e.g. a promotional credit), one AllowanceCharge per VAT category
    // it was allocated to (EN 16931 BG-20), and the VAT breakdown per category (BG-23).
    const breakdown = TaxCalculationEngine.breakdownForInvoice(invoice);
    const allowanceReason = invoice.document_allowance_reason ?? 'Discount';
    const allowanceXml = breakdown
      .filter((c) => c.allowanceHalalas > 0)
      .map(
        (c) => `
  <cac:AllowanceCharge>
    <cbc:ChargeIndicator>false</cbc:ChargeIndicator>
    <cbc:AllowanceChargeReasonCode>${ALLOWANCE_REASON_CODE_DISCOUNT}</cbc:AllowanceChargeReasonCode>
    <cbc:AllowanceChargeReason>${escapeXml(allowanceReason)}</cbc:AllowanceChargeReason>
    <cbc:Amount currencyID="SAR">${TaxCalculationEngine.formatSar(c.allowanceHalalas)}</cbc:Amount>
${taxCategoryXml(c, '    ', false)}
  </cac:AllowanceCharge>`
      )
      .join('');
    const taxSubtotalsXml = breakdown
      .map(
        (c) => `
    <cac:TaxSubtotal>
      <cbc:TaxableAmount currencyID="SAR">${TaxCalculationEngine.formatSar(c.taxableAmountHalalas)}</cbc:TaxableAmount>
      <cbc:TaxAmount currencyID="SAR">${TaxCalculationEngine.formatSar(c.vatAmountHalalas)}</cbc:TaxAmount>
${taxCategoryXml(c, '      ', true)}
    </cac:TaxSubtotal>`
      )
      .join('');

    const lineItemsXml = invoice.lines
      .map(
        (line) => `
  <cac:InvoiceLine>
    <cbc:ID>${line.line_number}</cbc:ID>
    <cbc:InvoicedQuantity unitCode="PCE">${line.quantity}</cbc:InvoicedQuantity>
    <cbc:LineExtensionAmount currencyID="SAR">${TaxCalculationEngine.formatSar(line.net_amount_halalas)}</cbc:LineExtensionAmount>
    <cac:TaxTotal>
      <cbc:TaxAmount currencyID="SAR">${TaxCalculationEngine.formatSar(line.vat_amount_halalas)}</cbc:TaxAmount>
      <cbc:RoundingAmount currencyID="SAR">${TaxCalculationEngine.formatSar(line.line_total_halalas)}</cbc:RoundingAmount>
    </cac:TaxTotal>
    <cac:Item>
      <cbc:Name>${escapeXml(line.item_name_ar)} | ${escapeXml(line.item_name_en)}</cbc:Name>
      <cac:SellersItemIdentification>
        <cbc:ID>${escapeXml(line.sku)}</cbc:ID>
      </cac:SellersItemIdentification>
      <cac:ClassifiedTaxCategory>
        <cbc:ID>${line.vat_category}</cbc:ID>
        <cbc:Percent>${(line.vat_rate_bps / 100).toFixed(2)}</cbc:Percent>
        <cac:TaxScheme>
          <cbc:ID>VAT</cbc:ID>
        </cac:TaxScheme>
      </cac:ClassifiedTaxCategory>
    </cac:Item>
    <cac:Price>
      <cbc:PriceAmount currencyID="SAR">${TaxCalculationEngine.formatSar(line.unit_price_halalas)}</cbc:PriceAmount>
    </cac:Price>
  </cac:InvoiceLine>`
      )
      .join('');

    return `<?xml version="1.0" encoding="UTF-8"?>
<Invoice xmlns="urn:oasis:names:specification:ubl:schema:xsd:Invoice-2"
         xmlns:cac="urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2"
         xmlns:cbc="urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2">
  <cbc:ProfileID>reporting:1.0</cbc:ProfileID>
  <cbc:ID>${escapeXml(invoice.invoice_number)}</cbc:ID>
  <cbc:UUID>${escapeXml(invoice.uuid)}</cbc:UUID>
  <cbc:IssueDate>${issueDate}</cbc:IssueDate>
  <cbc:IssueTime>${issueTime}</cbc:IssueTime>
  <cbc:InvoiceTypeCode name="${invoice.invoice_subtype}">${invoice.invoice_type_code}</cbc:InvoiceTypeCode>
  <cbc:DocumentCurrencyCode>${invoice.currency}</cbc:DocumentCurrencyCode>
  <cbc:TaxCurrencyCode>SAR</cbc:TaxCurrencyCode>${billingRefXml}
  <cac:AdditionalDocumentReference>
    <cbc:ID>ICV</cbc:ID>
    <cbc:UUID>${invoice.icv ?? 0}</cbc:UUID>
  </cac:AdditionalDocumentReference>
  <cac:AdditionalDocumentReference>
    <cbc:ID>PIH</cbc:ID>
    <cac:Attachment>
      <cbc:EmbeddedDocumentBinaryObject mimeCode="text/plain">${invoice.previous_invoice_hash ?? ''}</cbc:EmbeddedDocumentBinaryObject>
    </cac:Attachment>
  </cac:AdditionalDocumentReference>
  <cac:AdditionalDocumentReference>
    <cbc:ID>QR</cbc:ID>
    <cac:Attachment>
      <cbc:EmbeddedDocumentBinaryObject mimeCode="text/plain">${invoice.tlv_qr_base64 ?? ''}</cbc:EmbeddedDocumentBinaryObject>
    </cac:Attachment>
  </cac:AdditionalDocumentReference>
  <cac:AccountingSupplierParty>
    <cac:Party>
      <cac:PartyIdentification>
        <cbc:ID schemeID="CRN">${escapeXml(taxpayer.crn_number)}</cbc:ID>
      </cac:PartyIdentification>
      <cac:PostalAddress>
        <cbc:StreetName>${escapeXml(taxpayer.street_name)}</cbc:StreetName>
        <cbc:BuildingNumber>${escapeXml(taxpayer.building_number)}</cbc:BuildingNumber>
        <cbc:CitySubdivisionName>${escapeXml(taxpayer.district_name)}</cbc:CitySubdivisionName>
        <cbc:CityName>${escapeXml(taxpayer.city)}</cbc:CityName>
        <cbc:PostalZone>${escapeXml(taxpayer.postal_code)}</cbc:PostalZone>
        <cac:Country>
          <cbc:IdentificationCode>${escapeXml(taxpayer.country_code)}</cbc:IdentificationCode>
        </cac:Country>
      </cac:PostalAddress>
      <cac:PartyTaxScheme>
        <cbc:CompanyID>${escapeXml(taxpayer.vat_number)}</cbc:CompanyID>
        <cac:TaxScheme>
          <cbc:ID>VAT</cbc:ID>
        </cac:TaxScheme>
      </cac:PartyTaxScheme>
      <cac:PartyLegalEntity>
        <cbc:RegistrationName>${escapeXml(taxpayer.legal_name_ar)}</cbc:RegistrationName>
      </cac:PartyLegalEntity>
    </cac:Party>
  </cac:AccountingSupplierParty>
  <cac:AccountingCustomerParty>
    <cac:Party>${buyerTaxXml}
      <cac:PartyLegalEntity>
        <cbc:RegistrationName>${escapeXml(invoice.customer_name)}</cbc:RegistrationName>
      </cac:PartyLegalEntity>
    </cac:Party>
  </cac:AccountingCustomerParty>
  <cac:Delivery>
    <cbc:ActualDeliveryDate>${invoice.supply_date}</cbc:ActualDeliveryDate>
  </cac:Delivery>
  <cac:PaymentMeans>
    <cbc:PaymentMeansCode>${invoice.payment_means_code}</cbc:PaymentMeansCode>${instructionNoteXml}
  </cac:PaymentMeans>${allowanceXml}
  <cac:TaxTotal>
    <cbc:TaxAmount currencyID="SAR">${TaxCalculationEngine.formatSar(invoice.vat_amount_halalas)}</cbc:TaxAmount>${taxSubtotalsXml}
  </cac:TaxTotal>
  <cac:LegalMonetaryTotal>
    <cbc:LineExtensionAmount currencyID="SAR">${TaxCalculationEngine.formatSar(invoice.subtotal_halalas - invoice.discount_halalas)}</cbc:LineExtensionAmount>
    <cbc:TaxExclusiveAmount currencyID="SAR">${TaxCalculationEngine.formatSar(invoice.taxable_amount_halalas)}</cbc:TaxExclusiveAmount>
    <cbc:TaxInclusiveAmount currencyID="SAR">${TaxCalculationEngine.formatSar(invoice.total_amount_halalas)}</cbc:TaxInclusiveAmount>
    <cbc:AllowanceTotalAmount currencyID="SAR">${TaxCalculationEngine.formatSar(invoice.document_allowance_halalas)}</cbc:AllowanceTotalAmount>
    <cbc:PayableAmount currencyID="SAR">${TaxCalculationEngine.formatSar(invoice.total_amount_halalas)}</cbc:PayableAmount>
  </cac:LegalMonetaryTotal>${lineItemsXml}
</Invoice>`;
  }
}
