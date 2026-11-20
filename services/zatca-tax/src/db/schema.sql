CREATE TABLE IF NOT EXISTS taxpayers (
  id TEXT PRIMARY KEY,
  legal_name_ar TEXT NOT NULL,
  legal_name_en TEXT NOT NULL,
  vat_number TEXT NOT NULL UNIQUE,
  crn_number TEXT NOT NULL,
  egs_serial_number TEXT NOT NULL DEFAULT '1-ORBIT|2-EGS-KSA|3-0001',
  business_category TEXT NOT NULL DEFAULT 'E-Commerce & Retail',
  onboarding_status TEXT NOT NULL DEFAULT 'PRODUCTION_CSID' CHECK (onboarding_status IN ('COMPLIANCE_CSID', 'PRODUCTION_CSID', 'REVOKED')),
  street_name TEXT NOT NULL,
  building_number TEXT NOT NULL,
  district_name TEXT NOT NULL DEFAULT 'Al Olaya',
  city TEXT NOT NULL,
  postal_code TEXT NOT NULL,
  country_code TEXT NOT NULL DEFAULT 'SA',
  csid_certificate TEXT NOT NULL,
  private_key_pem TEXT NOT NULL,
  public_key_base64 TEXT NOT NULL,
  csid_expires_at TIMESTAMPTZ NOT NULL DEFAULT (NOW() + INTERVAL '365 days'),
  last_icv INTEGER NOT NULL DEFAULT 0,
  last_invoice_hash TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS egs_devices (
  id TEXT PRIMARY KEY,
  taxpayer_id TEXT NOT NULL REFERENCES taxpayers(id) ON DELETE CASCADE,
  egs_unit_id TEXT NOT NULL UNIQUE,
  serial_number TEXT NOT NULL,
  branch_name TEXT NOT NULL,
  city TEXT NOT NULL,
  model_version TEXT NOT NULL DEFAULT 'ORBIT-POS-V2',
  status TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'SUSPENDED')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS invoices (
  id TEXT PRIMARY KEY,
  invoice_number TEXT NOT NULL UNIQUE,
  uuid TEXT NOT NULL UNIQUE,
  taxpayer_id TEXT NOT NULL REFERENCES taxpayers(id),
  egs_device_id TEXT REFERENCES egs_devices(id),
  invoice_type_code TEXT NOT NULL DEFAULT '388' CHECK (invoice_type_code IN ('388', '381', '383')),
  invoice_subtype TEXT NOT NULL DEFAULT '0200000' CHECK (invoice_subtype IN ('0200000', '0100000')),
  payment_means_code TEXT NOT NULL DEFAULT '48' CHECK (payment_means_code IN ('10', '30', '42', '48', '97')),
  billing_reference_id TEXT REFERENCES invoices(id),
  instruction_note TEXT,
  icv INTEGER,
  previous_invoice_hash TEXT,
  invoice_hash TEXT,
  ecdsa_signature TEXT,
  tlv_qr_base64 TEXT,
  xml_ubl_base64 TEXT,
  customer_name TEXT NOT NULL,
  customer_vat_number TEXT,
  customer_phone TEXT,
  customer_email TEXT,
  currency TEXT NOT NULL DEFAULT 'SAR',
  subtotal_halalas BIGINT NOT NULL CHECK (subtotal_halalas >= 0),
  discount_halalas BIGINT NOT NULL DEFAULT 0 CHECK (discount_halalas >= 0),
  document_allowance_halalas BIGINT NOT NULL DEFAULT 0 CHECK (document_allowance_halalas >= 0),
  -- Why the document-level allowance was granted (e.g. 'Promotional credit'); stated on the UBL
  -- AllowanceCharge (reason code 95) and the PDF.
  document_allowance_reason TEXT,
  taxable_amount_halalas BIGINT NOT NULL CHECK (taxable_amount_halalas >= 0),
  vat_amount_halalas BIGINT NOT NULL CHECK (vat_amount_halalas >= 0),
  total_amount_halalas BIGINT NOT NULL CHECK (total_amount_halalas >= 0),
  status TEXT NOT NULL DEFAULT 'DRAFT' CHECK (status IN ('DRAFT', 'ISSUED', 'REPORTED', 'REPORTED_WITH_WARNINGS', 'CLEARED', 'REJECTED')),
  zatca_reporting_status TEXT,
  zatca_warnings JSONB NOT NULL DEFAULT '[]'::jsonb,
  zatca_errors JSONB NOT NULL DEFAULT '[]'::jsonb,
  zatca_reported_at TIMESTAMPTZ,
  supply_date DATE NOT NULL DEFAULT CURRENT_DATE,
  issued_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  -- Provenance of invoices issued by the order flow (NULL for API-created documents).
  source_order_id TEXT,
  source_journal_ref TEXT,
  source_event_id TEXT,
  -- Provenance of documents issued by the merchant settlement workflow (fee invoice / payout credit note).
  source_settlement_batch_id TEXT,
  -- Provenance of credit notes issued for customer refunds.
  source_refund_id TEXT
);

CREATE INDEX IF NOT EXISTS idx_invoices_taxpayer_icv ON invoices(taxpayer_id, icv);
CREATE INDEX IF NOT EXISTS idx_invoices_taxpayer_status ON invoices(taxpayer_id, status, issued_at);
CREATE INDEX IF NOT EXISTS idx_invoices_billing_ref ON invoices(billing_reference_id);

-- Hash-chain integrity, enforced by the database: per taxpayer, every ICV, every previous-invoice
-- hash (PIH) and every invoice hash is used at most once, so the chain can never fork or repeat,
-- whatever the caller does. (ICV/PIH are assigned under the taxpayer row lock in issueInvoice.)
CREATE UNIQUE INDEX IF NOT EXISTS uq_invoices_taxpayer_icv
  ON invoices(taxpayer_id, icv) WHERE icv IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_invoices_taxpayer_pih
  ON invoices(taxpayer_id, previous_invoice_hash) WHERE previous_invoice_hash IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_invoices_taxpayer_hash
  ON invoices(taxpayer_id, invoice_hash) WHERE invoice_hash IS NOT NULL;
-- One simplified tax invoice per order (the order flow can never invoice an order twice).
CREATE UNIQUE INDEX IF NOT EXISTS uq_invoices_source_order
  ON invoices(taxpayer_id, source_order_id) WHERE source_order_id IS NOT NULL AND invoice_type_code = '388';

-- One fee invoice and one payout credit note per settlement batch, ever.
CREATE UNIQUE INDEX IF NOT EXISTS uq_invoices_source_settlement
  ON invoices(taxpayer_id, source_settlement_batch_id, invoice_type_code) WHERE source_settlement_batch_id IS NOT NULL;

-- One credit note per customer refund, ever.
CREATE UNIQUE INDEX IF NOT EXISTS uq_invoices_source_refund
  ON invoices(taxpayer_id, source_refund_id) WHERE source_refund_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS invoice_line_items (
  id TEXT PRIMARY KEY,
  invoice_id TEXT NOT NULL REFERENCES invoices(id) ON DELETE CASCADE,
  line_number INTEGER NOT NULL,
  item_name_ar TEXT NOT NULL,
  item_name_en TEXT NOT NULL,
  sku TEXT NOT NULL,
  quantity INTEGER NOT NULL CHECK (quantity > 0),
  unit_price_halalas BIGINT NOT NULL CHECK (unit_price_halalas >= 0),
  discount_halalas BIGINT NOT NULL DEFAULT 0 CHECK (discount_halalas >= 0),
  net_amount_halalas BIGINT NOT NULL CHECK (net_amount_halalas >= 0),
  vat_category TEXT NOT NULL DEFAULT 'S' CHECK (vat_category IN ('S', 'Z', 'E', 'O')),
  vat_rate_bps INTEGER NOT NULL DEFAULT 1500,
  vat_exemption_code TEXT,
  vat_exemption_reason TEXT,
  vat_amount_halalas BIGINT NOT NULL CHECK (vat_amount_halalas >= 0),
  line_total_halalas BIGINT NOT NULL CHECK (line_total_halalas >= 0),
  refunded_quantity INTEGER NOT NULL DEFAULT 0 CHECK (refunded_quantity >= 0)
);

CREATE INDEX IF NOT EXISTS idx_line_items_invoice ON invoice_line_items(invoice_id, line_number);

CREATE TABLE IF NOT EXISTS zatca_submission_logs (
  id TEXT PRIMARY KEY,
  invoice_id TEXT NOT NULL REFERENCES invoices(id) ON DELETE CASCADE,
  attempt_number INTEGER NOT NULL,
  submission_type TEXT NOT NULL DEFAULT 'REPORTING' CHECK (submission_type IN ('REPORTING', 'CLEARANCE')),
  request_payload JSONB NOT NULL,
  http_status INTEGER NOT NULL,
  response_body JSONB NOT NULL,
  outcome TEXT NOT NULL CHECK (outcome IN ('REPORTED', 'REPORTED_WITH_WARNINGS', 'CLEARED', 'REJECTED', 'TRANSIENT_ERROR')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS invoice_pdf_artifacts (
  invoice_id TEXT PRIMARY KEY REFERENCES invoices(id) ON DELETE CASCADE,
  content_sha256 TEXT NOT NULL,
  byte_size INTEGER NOT NULL,
  pdf_base64 TEXT NOT NULL,
  generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
