export type VatCategory = 'S' | 'Z' | 'E' | 'O';
export type InvoiceTypeCode = '388' | '381' | '383';
export type InvoiceSubtype = '0200000' | '0100000';
/** UNCL 4461: 10 cash, 30 credit transfer, 42 payment to bank account, 48 bank card, 97 clearing between partners. */
export type PaymentMeansCode = '10' | '30' | '42' | '48' | '97';
export type OnboardingStatus = 'COMPLIANCE_CSID' | 'PRODUCTION_CSID' | 'REVOKED';

export type VatExemptionCode =
  | 'VATEX-SA-29'
  | 'VATEX-SA-29-7'
  | 'VATEX-SA-30'
  | 'VATEX-SA-32'
  | 'VATEX-SA-33'
  | 'VATEX-SA-34-1'
  | 'VATEX-SA-EDU'
  | 'VATEX-SA-HEA'
  | 'VATEX-SA-OOS';

export type InvoiceStatus =
  | 'DRAFT'
  | 'ISSUED'
  | 'REPORTED'
  | 'REPORTED_WITH_WARNINGS'
  | 'CLEARED'
  | 'REJECTED';

export interface TaxpayerRecord {
  id: string;
  legal_name_ar: string;
  legal_name_en: string;
  vat_number: string;
  crn_number: string;
  egs_serial_number: string;
  business_category: string;
  onboarding_status: OnboardingStatus;
  street_name: string;
  building_number: string;
  district_name: string;
  city: string;
  postal_code: string;
  country_code: string;
  csid_certificate: string;
  private_key_pem: string;
  public_key_base64: string;
  csid_expires_at: string;
  last_icv: number;
  last_invoice_hash: string;
  created_at: string;
}

export interface EgsDeviceRecord {
  id: string;
  taxpayer_id: string;
  egs_unit_id: string;
  serial_number: string;
  branch_name: string;
  city: string;
  model_version: string;
  status: 'ACTIVE' | 'SUSPENDED';
  created_at: string;
}

export interface InvoiceLineInput {
  itemNameAr: string;
  itemNameEn: string;
  sku: string;
  quantity: number;
  unitPriceHalalas: number;
  discountHalalas?: number;
  vatCategory?: VatCategory;
  vatExemptionCode?: VatExemptionCode;
  vatExemptionReason?: string;
  /**
   * Explicit line VAT (pro-rata credit / debit notes): the share of the VAT the referenced
   * invoice charged. Accepted within the engine's tolerance; omitted = net × rate.
   */
  vatAmountHalalas?: number;
}

export interface CalculatedLineItem {
  lineNumber: number;
  itemNameAr: string;
  itemNameEn: string;
  sku: string;
  quantity: number;
  unitPriceHalalas: number;
  discountHalalas: number;
  netAmountHalalas: number;
  vatCategory: VatCategory;
  vatRateBps: number;
  vatExemptionCode: VatExemptionCode | null;
  vatExemptionReason: string | null;
  vatAmountHalalas: number;
  lineTotalHalalas: number;
}

export interface InvoiceRecord {
  id: string;
  invoice_number: string;
  uuid: string;
  taxpayer_id: string;
  egs_device_id: string | null;
  invoice_type_code: InvoiceTypeCode;
  invoice_subtype: InvoiceSubtype;
  payment_means_code: PaymentMeansCode;
  billing_reference_id: string | null;
  instruction_note: string | null;
  icv: number | null;
  previous_invoice_hash: string | null;
  invoice_hash: string | null;
  ecdsa_signature: string | null;
  tlv_qr_base64: string | null;
  xml_ubl_base64: string | null;
  customer_name: string;
  customer_vat_number: string | null;
  customer_phone: string | null;
  customer_email: string | null;
  currency: string;
  subtotal_halalas: number;
  discount_halalas: number;
  document_allowance_halalas: number;
  document_allowance_reason: string | null;
  taxable_amount_halalas: number;
  vat_amount_halalas: number;
  total_amount_halalas: number;
  status: InvoiceStatus;
  zatca_reporting_status: string | null;
  zatca_warnings: Array<{ code: string; message: string }>;
  zatca_errors: Array<{ code: string; message: string }>;
  zatca_reported_at: string | null;
  supply_date: string;
  issued_at: string | null;
  created_at: string;
  /** Provenance (order flow / settlement / refund documents); null for API-created documents. */
  source_order_id?: string | null;
  source_journal_ref?: string | null;
  source_event_id?: string | null;
  source_settlement_batch_id?: string | null;
  source_refund_id?: string | null;
}

export interface InvoiceLineRecord {
  id: string;
  invoice_id: string;
  line_number: number;
  item_name_ar: string;
  item_name_en: string;
  sku: string;
  quantity: number;
  unit_price_halalas: number;
  discount_halalas: number;
  net_amount_halalas: number;
  vat_category: VatCategory;
  vat_rate_bps: number;
  vat_exemption_code: VatExemptionCode | null;
  vat_exemption_reason: string | null;
  vat_amount_halalas: number;
  line_total_halalas: number;
  refunded_quantity: number;
}

export interface InvoiceWithLines extends InvoiceRecord {
  lines: InvoiceLineRecord[];
  taxpayer?: TaxpayerRecord;
}

export interface SubmissionLogRecord {
  id: string;
  invoice_id: string;
  attempt_number: number;
  submission_type: 'REPORTING' | 'CLEARANCE';
  request_payload: Record<string, unknown>;
  http_status: number;
  response_body: Record<string, unknown>;
  outcome: 'REPORTED' | 'REPORTED_WITH_WARNINGS' | 'CLEARED' | 'REJECTED' | 'TRANSIENT_ERROR';
  created_at: string;
}
