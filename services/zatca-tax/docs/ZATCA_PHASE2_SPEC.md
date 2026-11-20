# KSA ZATCA Phase 2 (Fatoora) Business & Cryptographic Specification

## 1. VAT Registration Number Format (`BR-KSA-40`)
- Must be exactly 15 numeric digits.
- Starts with `3` (GCC country prefix for KSA) and ends with `3`.
- Example: `310123456700003`.

## 2. Document Type Codes (`UN/CEFACT 1001`)
- `388`: Tax Invoice (`0200000` Simplified B2C / `0100000` Standard B2B)
- `381`: Credit Note (issued for customer returns, order cancellations, or downward price adjustments; must reference `billing_reference_id` and include an `instruction_note`)
- `383`: Debit Note (issued for upward price/quantity adjustments; must reference `billing_reference_id` and include an `instruction_note`)

## 3. Payment Means Codes (`UN/CEFACT 4461`)
- `10`: In cash
- `30`: Credit / BNPL
- `42`: Payment to bank account (SARIE / IBAN transfer)
- `48`: Bank card (MADA / Visa / Mastercard / Apple Pay)

## 4. VAT Categories & Exemption Reason Codes (`BR-KSA-DEC-01`)
- **`S` (Standard Rate — 15%):** Default for domestic goods and services. Must not carry an exemption code.
- **`Z` (Zero-Rated — 0%):** Allowed exemption codes:
  - `VATEX-SA-32`: Export of goods from KSA
  - `VATEX-SA-33`: Export of services from KSA
  - `VATEX-SA-34-1`: International transport
  - `VATEX-SA-EDU`: Qualifying private education for Saudi citizens
  - `VATEX-SA-HEA`: Qualifying private healthcare for Saudi citizens
- **`E` (Exempt — 0%):** Allowed exemption codes:
  - `VATEX-SA-29`: Margin-based financial services
  - `VATEX-SA-29-7`: Life insurance contracts
  - `VATEX-SA-30`: Residential real estate lease/sale
- **`O` (Out of Scope — 0%):** Allowed code: `VATEX-SA-OOS`.

## 5. Cryptographic Chaining (`ICV` & `PIH`)
- Every issued document (`388`, `381`, `383`) increments the taxpayer's Invoice Counter Value (`ICV`) by 1 (`1, 2, 3, ...`).
- The first invoice (`ICV = 1`) uses the genesis Previous Invoice Hash (`PIH`): `SHA256("0")` in Base64 (`NWZlY2ViNjZmZmM4NmYzOGQ5NTI3ODZjNmQ2OTZjNzljMmRiYzIzOWRkNGU5MWI0NjcyOWQ3M2EyN2ZiNTdlOQ==` / raw digest Base64).
- Subsequent documents set `previous_invoice_hash` equal to the `invoice_hash` of document `ICV - 1`.
