import { renderInvoicePdf } from '../services/PdfInvoiceGenerator';
import type { InvoiceWithLines } from '../types/zatca';
import type { InvoiceDocument, InvoiceReportedPayload } from '../../../../shared/contracts';

/**
 * Render the PDF of a reported invoice from the `zatca.invoice.reported` event alone (the event
 * carries the full document snapshot), so the renderer needs no access to the ZATCA database.
 */
export function renderReportedInvoicePdf(payload: InvoiceReportedPayload): Promise<Buffer> {
  return renderInvoiceDocumentPdf(payload);
}

/**
 * Render any issued ZATCA document (invoice, credit / debit note; simplified or standard) from its
 * event snapshot — e.g. the settlement fee invoice and payout credit note of
 * `zatca.settlement-documents.reported`.
 */
export function renderInvoiceDocumentPdf(
  payload: InvoiceDocument,
  options: { creditNotes?: Array<{ invoiceNumber: string; icv: number; totalHalalas: number; refundId?: string | null }> } = {}
): Promise<Buffer> {
  const invoice: InvoiceWithLines = {
    id: payload.invoiceId,
    invoice_number: payload.invoiceNumber,
    uuid: payload.uuid,
    taxpayer_id: payload.taxpayerId,
    egs_device_id: null,
    invoice_type_code: payload.invoiceTypeCode,
    invoice_subtype: payload.invoiceSubtype as InvoiceWithLines['invoice_subtype'],
    payment_means_code: (payload.paymentMeansCode ?? '48') as InvoiceWithLines['payment_means_code'],
    billing_reference_id: payload.billingReference?.invoiceId ?? null,
    instruction_note: payload.instructionNote ?? null,
    icv: payload.icv,
    previous_invoice_hash: payload.previousInvoiceHash,
    invoice_hash: payload.invoiceHash,
    ecdsa_signature: null,
    tlv_qr_base64: payload.qrBase64,
    xml_ubl_base64: null,
    customer_name: payload.customerName,
    customer_vat_number: payload.customerVatNumber ?? null,
    customer_phone: null,
    customer_email: null,
    currency: 'SAR',
    subtotal_halalas: payload.totals.subtotalHalalas,
    discount_halalas: payload.totals.discountHalalas,
    document_allowance_halalas: payload.totals.allowanceHalalas,
    document_allowance_reason: payload.allowanceReason ?? null,
    taxable_amount_halalas: payload.totals.taxableHalalas,
    vat_amount_halalas: payload.totals.vatHalalas,
    total_amount_halalas: payload.totals.totalHalalas,
    status: payload.status,
    zatca_reporting_status: payload.status === 'CLEARED' ? 'CLEARED' : 'REPORTED',
    zatca_warnings: [],
    zatca_errors: [],
    zatca_reported_at: payload.reportedAt,
    supply_date: payload.issuedAt.slice(0, 10),
    issued_at: payload.issuedAt,
    created_at: payload.issuedAt,
    lines: payload.lines.map((l) => ({
      id: `${payload.invoiceId}-${l.lineNumber}`,
      invoice_id: payload.invoiceId,
      line_number: l.lineNumber,
      item_name_ar: l.nameAr,
      item_name_en: l.nameEn,
      sku: l.sku,
      quantity: l.quantity,
      unit_price_halalas: l.unitPriceHalalas,
      discount_halalas: l.discountHalalas,
      net_amount_halalas: l.netAmountHalalas,
      vat_category: l.vatCategory,
      vat_rate_bps: l.vatRateBps,
      vat_exemption_code: l.vatExemptionCode ?? null,
      vat_exemption_reason: null,
      vat_amount_halalas: l.vatAmountHalalas,
      line_total_halalas: l.lineTotalHalalas,
      refunded_quantity: 0,
    })),
  };
  return renderInvoicePdf(
    invoice,
    {
      legal_name_en: payload.seller.legalNameEn,
      vat_number: payload.seller.vatNumber,
      crn_number: payload.seller.crnNumber,
      building_number: payload.seller.buildingNumber,
      street_name: payload.seller.streetName,
      city: payload.seller.city,
      postal_code: payload.seller.postalCode,
      country_code: payload.seller.countryCode,
    },
    { billingReferenceNumber: payload.billingReference?.invoiceNumber, creditNotes: options.creditNotes }
  );
}
