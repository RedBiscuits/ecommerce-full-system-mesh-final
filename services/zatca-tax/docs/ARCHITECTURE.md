# Architecture & Data Model (`zatca-tax-service`)

## Component Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Fastify HTTP API Layer                          │
│         (/api/v1/taxpayers/*, /api/v1/invoices/*, /health)              │
└───────┬──────────────────┬──────────────────┬───────────────────┬───────┘
        │                  │                  │                   │
        ▼                  ▼                  ▼                   ▼
┌───────────────┐  ┌───────────────┐  ┌───────────────┐  ┌────────────────┐
│InvoiceService │  │TaxCalculation │  │PdfInvoiceGen  │  │ReportingService│
│ & UblBuilder  │  │    Engine     │  │   (PDFKit)    │  │(Fatoora Client)│
└───────┬───────┘  └───────────────┘  └───────┬───────┘  └────────┬───────┘
        │                                     │                   │
        ▼                                     ▼                   ▼
┌─────────────────────────────────────────────────────────────────────────┐
│             PostgreSQL 16 (@electric-sql/pglite) & Redis                │
│ (taxpayers, egs_devices, invoices, invoice_line_items,                  │
│  zatca_submission_logs, invoice_pdf_artifacts)                          │
└─────────────────────────────────────────────────────────────────────────┘
```

## Database Tables (`src/db/schema.sql`)

1. **`taxpayers`**: Stores seller legal names (Arabic & English), 15-digit KSA VAT registration number, CRN, national address, `secp256k1` CSID cryptographic keys, `onboarding_status`, `last_icv`, and `last_invoice_hash`.
2. **`egs_devices`**: Tracks E-Invoice Generation Solution (`EGS`) units deployed across retail branches (`egs_unit_id`, `serial_number`, `branch_name`, `status`).
3. **`invoices`**: Stores `388` (Tax Invoice), `381` (Credit Note), and `383` (Debit Note) documents, monetary totals in integer SAR halalas, cryptographic fields (`icv`, `previous_invoice_hash`, `invoice_hash`, `ecdsa_signature`, `tlv_qr_base64`, `xml_ubl_base64`), and Fatoora reporting state.
4. **`invoice_line_items`**: Line-level quantities, unit prices in halalas, discounts, VAT categories (`S`, `Z`, `E`, `O`), `vat_exemption_code`, `vat_exemption_reason`, and `refunded_quantity`.
5. **`zatca_submission_logs`**: Immutable audit trail of every reporting attempt sent to ZATCA, capturing HTTP status codes, warning arrays, and error codes.
6. **`invoice_pdf_artifacts`**: Stores rendered PDF binary buffers (Base64) along with their `content_sha256` digest and byte size.
