# ZATCA Phase 2 Tax Invoicing & Fatoora Reporting Service (`zatca-tax-service`)

Backend microservice for Kingdom of Saudi Arabia (KSA) **ZATCA Phase 2 (Fatoora)** electronic invoicing, supporting Simplified Tax Invoices (`0200000` / `388`), Simplified Credit Notes (`381`), and Simplified Debit Notes (`383`).

---

## Core Capabilities

1. **KSA Taxpayer & EGS Unit Management (`/api/v1/taxpayers`)**
   - Enforces KSA 15-digit VAT Registration Number format (`3.............3`) and Commercial Registration Number (`CRN`).
   - Manages Cryptographic Stamp Identifiers (`CSID`), `secp256k1` ECDSA keypairs, and multi-branch E-Invoice Generation Solution (`EGS`) POS/API devices.
2. **KSA Value Added Tax (VAT) Engine (`src/services/TaxCalculationEngine.ts`)**
   - Integer SAR Halala precision (`1 SAR = 100 halalas`).
   - Supports KSA VAT Categories:
     - `S`: Standard Rate (`15.00%` / `1500 bps`)
     - `Z`: Zero-Rated (`0.00%` — requires official `VATEX-SA-32`, `VATEX-SA-33`, `VATEX-SA-34-1`, `VATEX-SA-EDU`, or `VATEX-SA-HEA` exemption code)
     - `E`: Exempt (`0.00%` — requires `VATEX-SA-29`, `VATEX-SA-29-7`, or `VATEX-SA-30` exemption code)
     - `O`: Out of Scope (`0.00%` — `VATEX-SA-OOS`)
   - Supports both line-item discounts and document-level allowances (`documentAllowanceHalalas`).
3. **Cryptographic Hash Chain & Stamping (`src/services/ZatcaCryptoService.ts`)**
   - Maintains sequential Invoice Counter Values (`ICV`) and links each issued document's `previous_invoice_hash` (`PIH`) to the preceding document's SHA-256 digest (anchored at `SHA256("0")` for `ICV = 1`).
   - Signs invoice digests using `secp256k1` ECDSA-SHA256 and encodes ZATCA Tag-Length-Value (TLV) Base64 QR payloads.
4. **UBL 2.1 XML & PDF Invoice Generation (`UblXmlBuilder.ts` & `PdfInvoiceGenerator.ts`)**
   - Generates OASIS UBL 2.1 XML (`reporting:1.0` profile) with embedded `ICV`, `PIH`, and `QR` document references.
   - Renders printable A4 KSA Tax Invoices, Credit Notes, and Debit Notes via `pdfkit` with SHA-256 artifact checksums (`X-Content-SHA256`).
5. **ZATCA Fatoora Reporting Pipeline (`src/services/ReportingService.ts`)**
   - Submits issued B2C documents to the Fatoora reporting gateway, recording `REPORTED` (`200`), `REPORTED_WITH_WARNINGS` (`202`), `REJECTED` (`4xx`), and `TRANSIENT_ERROR` (`5xx`) outcomes in `zatca_submission_logs`.

---

## Documentation
- [System Architecture & Data Model](docs/ARCHITECTURE.md)
- [ZATCA Phase 2 Business & Cryptographic Rules](docs/ZATCA_PHASE2_SPEC.md)

---

## REST API Reference

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/v1/taxpayers` | Register a KSA taxpayer (`3...3` 15-digit VAT number) |
| `GET` | `/api/v1/taxpayers` | List registered taxpayers |
| `GET` | `/api/v1/taxpayers/:id` | Get taxpayer profile & current `last_icv` / `last_invoice_hash` |
| `POST` | `/api/v1/taxpayers/:id/egs-devices` | Register an EGS POS/API terminal for a taxpayer branch |
| `GET` | `/api/v1/taxpayers/:id/egs-devices` | List EGS terminals registered under a taxpayer |
| `GET` | `/api/v1/taxpayers/:id/compliance-summary` | Get invoice status counts, `lastIcv`, and cumulative SAR totals |
| `POST` | `/api/v1/invoices` | Create a `DRAFT` B2C invoice with line items |
| `GET` | `/api/v1/invoices` | List invoices (filter by `taxpayerId`, `status`, `invoiceTypeCode`) |
| `GET` | `/api/v1/invoices/:id` | Retrieve invoice details and line items |
| `PATCH` | `/api/v1/invoices/:id` | Update customer details, payment means, or lines on a `DRAFT` invoice |
| `DELETE` | `/api/v1/invoices/:id` | Delete a `DRAFT` invoice |
| `POST` | `/api/v1/invoices/:id/issue` | Issue a `DRAFT` invoice (`ICV`, `PIH`, SHA-256 hash, ECDSA stamp, TLV QR, UBL XML) |
| `POST` | `/api/v1/invoices/:id/credit-notes` | Create and issue a Simplified Credit Note (`381`) |
| `POST` | `/api/v1/invoices/:id/debit-notes` | Create and issue a Simplified Debit Note (`383`) |
| `GET` | `/api/v1/invoices/:id/xml` | Download the UBL 2.1 XML document (`application/xml`) |
| `GET` | `/api/v1/invoices/:id/pdf` | Render and stream the official KSA Tax Invoice PDF (`application/pdf`) |
| `POST` | `/api/v1/invoices/:id/report` | Report an issued B2C document to the ZATCA Fatoora gateway |
| `GET` | `/api/v1/invoices/:id/submissions` | List Fatoora gateway submission attempts and responses |

---

## Development & Verification
```bash
npm install
npm run lint
npm run typecheck
npm run build
npm test
```

---

## Platform order flow

- `src/integration/OrderInvoicingService.ts` consumes `ledger.payment.settled` (queue `zatca.ledger-settlements`) in two checkpointed steps: **issued** (draft 388/0200000 with the captured credits as `documentAllowanceHalalas`, totals verified against the ledger split, next ICV / PIH / ECDSA / QR / UBL — one transaction under the taxpayer row lock) and **done** (reported to Fatoora; submission log, status, `zatca.invoice.reported` outbox event and checkpoint in one transaction). A Fatoora 5xx is retried without issuing again; a rejection is dead-lettered.
- Chain integrity is enforced by the database (unique ICV, PIH and invoice hash per taxpayer; one invoice per source order) and verified end to end by `verifyHashChain()` — `GET /api/v1/taxpayers/:id/hash-chain`.
- `GET /api/v1/orders/:orderId/invoicing` — invoices issued for an order, with the checkpoints. The PDF shows the document-level credit allowance; `renderReportedInvoicePdf()` renders a reported invoice from its event alone.

### Document allowance across VAT categories (mixed baskets)

- `TaxCalculationEngine` splits a document-level allowance (e.g. promotional credits) across the VAT categories **in proportion to their net line amounts**. It uses exact BigInt largest-remainder allocation (`shared/contracts/money.ts`), so the shares always add up to the allowance. Each category's VAT is reduced by `applyBpsHalfEven(share, rate)` (banker's rounding). Every `VatCategorySubtotal` carries `lineNetHalalas`, `allowanceHalalas`, `taxableAmountHalalas` and `vatAmountHalalas`.
  - Before this change, the whole allowance was taken off the standard-rated base. A basket of S 100 + Z 100 + E 50 SAR with a 25 SAR allowance was charged 11.25 SAR VAT; the proportional split charges 13.50 SAR.
- UBL 2.1: one `cac:AllowanceCharge` per category (reason code `95`, reason "Promotional credit (N credits)") and one `cac:TaxSubtotal` per category, with a VATEX exemption code for Z / E. Payment means are `48` for card and `30` for wallet.
- PDF: an "Allowance – <reason>" line, "Total VAT" and a "VAT Breakdown" table per category. The `zatca.invoice.reported` event carries `allowanceReason`, `vatBreakdown` and each line's `vatExemptionCode`, so the file cache can render the PDF from the event alone.
- The shared `computeOrderTotals` (used by the ledger and the coordinator) mirrors the engine. `__tests__/contracts/vat-parity.test.ts` pins them together.

### Merchant settlement documents

- The platform is a taxpayer of its own (`txp_ksa_platform`, `PLATFORM_TAXPAYER_ID`), with its own EGS and ICV / PIH chain.
- `src/integration/SettlementInvoicingService.ts` consumes `ledger.merchant-settlement.posted` (queue `zatca.merchant-settlements`). In one transaction under the taxpayer lock, with the `issued` checkpoint, it issues:
  - a **388 tax invoice** for the processing fee. Its taxable amount is the platform's net fee revenue, VAT 15 %, payment means 97 (clearing). It must reproduce the fee the ledger booked to the halala, or the message is dead-lettered and nothing is issued.
  - a **381 credit note** for the merchant's payout. It references the fee invoice, states the reason for issuance (KSA-10) and carries one line, out of scope (O / VATEX-SA-OOS), so no VAT.

  The two documents get consecutive ICVs. A VAT-registered merchant gets standard B2B documents (0100000), submitted to the **clearance** API (`ReportingService.clearStandardInvoice`, status `CLEARED`). Otherwise the documents are simplified (0200000) and reported. A Fatoora outage retries the submission only, so no ICV is burned. A rejection is dead-lettered. Once both are accepted, `zatca.settlement-documents.reported` carries both document snapshots, including the UBL, in the outbox (`done` checkpoint). A unique index allows one fee invoice and one credit note per batch.
- UBL / PDF: the buyer's VAT number (`PartyTaxScheme`), `InstructionNote`, the billing reference, and document titles by type and subtype. `renderInvoiceDocumentPdf()` renders any document from its event snapshot.
- API: `GET /api/v1/settlements/:batchId/documents`, `POST /api/v1/invoices/:id/clearance`.

### Event authentication

Every event this service publishes is signed with its stream's shared key (HMAC-SHA256,
`x-key-id` / `x-event-stream` / `x-event-issuer` / `x-event-audience` / `x-signature`), and every
delivery it consumes is verified against the authorization matrix before the body is parsed: wrong
key, wrong issuer, wrong audience or a tampered byte means the message is dead-lettered at once
(`UNTRUSTED_EVENT`). See `shared/security/streams.ts` and the root README.

### Customer refunds (credit notes)

`src/integration/RefundInvoicingService.ts` consumes `ledger.payment.refunded` (queue `zatca.ledger-refunds`) and issues a **381 credit note** against the order's invoice: one line per VAT category of the ledger's split, with the pro-rata VAT stated explicitly (`InvoiceLineInput.vatAmountHalalas`, accepted by the engine within the rounding the invoice itself accumulated), the billing reference, the reason for issuance, the next ICV in the merchant's chain; reported or cleared like the invoice. Σ credit notes of an invoice can never exceed its total; one credit note per refund id (unique index); a Fatoora outage retries the submission only. `zatca.credit-note.reported` carries the credit note, the invoice snapshot and every credit note applied so far. `GET /api/v1/orders/:orderId/credit-notes`.
