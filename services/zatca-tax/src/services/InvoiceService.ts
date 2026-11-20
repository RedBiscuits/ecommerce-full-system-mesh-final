import type { PGlite, Transaction } from '@electric-sql/pglite';
import { v4 as uuidv4 } from 'uuid';
import { DEFAULT_CSID_KEYS, ZATCA_GENESIS_PIH } from '../db';
import {
  EgsDeviceRecord,
  InvoiceLineInput,
  InvoiceLineRecord,
  InvoiceRecord,
  InvoiceStatus,
  InvoiceSubtype,
  InvoiceTypeCode,
  InvoiceWithLines,
  PaymentMeansCode,
  TaxpayerRecord,
} from '../types/zatca';
import { TaxCalculationEngine } from './TaxCalculationEngine';
import { UblXmlBuilder } from './UblXmlBuilder';
import { ZatcaCryptoService } from './ZatcaCryptoService';

/** The database or an open transaction on it: lets a caller compose several steps atomically. */
export type ZatcaExecutor = Pick<PGlite, 'query'> | Pick<Transaction, 'query'>;

export interface HashChainReport {
  taxpayerId: string;
  ok: boolean;
  /** Issued documents in the chain. */
  length: number;
  lastIcv: number;
  /** Every break found: gaps / repeats in the ICV, PIH links, hashes or signatures. */
  breaks: string[];
}

export interface CreateTaxpayerInput {
  id?: string;
  legalNameAr: string;
  legalNameEn: string;
  vatNumber: string;
  crnNumber: string;
  egsSerialNumber?: string;
  businessCategory?: string;
  streetName: string;
  buildingNumber: string;
  districtName?: string;
  city: string;
  postalCode: string;
}

export interface RegisterEgsDeviceInput {
  taxpayerId: string;
  egsUnitId: string;
  serialNumber: string;
  branchName: string;
  city: string;
  modelVersion?: string;
}

export interface CreateDraftInvoiceInput {
  taxpayerId: string;
  egsDeviceId?: string;
  invoiceNumber?: string;
  invoiceTypeCode?: InvoiceTypeCode;
  invoiceSubtype?: InvoiceSubtype;
  paymentMeansCode?: PaymentMeansCode;
  customerName: string;
  customerVatNumber?: string;
  customerPhone?: string;
  customerEmail?: string;
  documentAllowanceHalalas?: number;
  /** Why the allowance is granted (e.g. 'Promotional credit'). */
  documentAllowanceReason?: string;
  supplyDate?: string;
  lines: InvoiceLineInput[];
  /** Provenance of invoices created by the order flow. */
  sourceOrderId?: string;
  sourceJournalRef?: string;
  sourceEventId?: string;
  /** Merchant settlement documents: the settlement batch they were issued for. */
  sourceSettlementBatchId?: string;
  /** Credit / debit notes: the document they adjust, and why (KSA-10). */
  billingReferenceId?: string;
  instructionNote?: string;
  /** Customer refunds: the refund the credit note documents. */
  sourceRefundId?: string;
  /** Pro-rata notes: allowed deviation of an explicit line VAT from net × rate (halalas). */
  explicitVatToleranceHalalas?: number;
}

export interface UpdateDraftInvoiceInput {
  customerName?: string;
  customerPhone?: string;
  customerEmail?: string;
  paymentMeansCode?: PaymentMeansCode;
  documentAllowanceHalalas?: number;
  lines?: InvoiceLineInput[];
}

export interface CreateAdjustmentNoteInput {
  instructionNote: string;
  paymentMeansCode?: PaymentMeansCode;
  lines: InvoiceLineInput[];
}

export interface TaxpayerComplianceSummary {
  taxpayerId: string;
  vatNumber: string;
  onboardingStatus: string;
  lastIcv: number;
  lastInvoiceHash: string;
  countsByStatus: Record<string, number>;
  totalsHalalas: {
    issuedSalesHalalas: number;
    issuedVatHalalas: number;
    creditNotesTotalHalalas: number;
    debitNotesTotalHalalas: number;
  };
}

export class InvoiceService {
  constructor(private readonly db: PGlite) {}

  async createTaxpayer(input: CreateTaxpayerInput): Promise<TaxpayerRecord> {
    if (!ZatcaCryptoService.validateKsaVatNumber(input.vatNumber)) {
      throw new Error('INVALID_KSA_VAT_NUMBER');
    }

    const id = input.id || `txp_${uuidv4().slice(0, 8)}`;
    const res = await this.db.query<TaxpayerRecord>(
      `INSERT INTO taxpayers (
        id, legal_name_ar, legal_name_en, vat_number, crn_number,
        egs_serial_number, business_category, onboarding_status,
        street_name, building_number, district_name, city, postal_code, country_code,
        csid_certificate, private_key_pem, public_key_base64, last_icv, last_invoice_hash
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, 'PRODUCTION_CSID', $8, $9, $10, $11, $12, 'SA', $13, $14, $15, 0, $16)
      RETURNING *`,
      [
        id,
        input.legalNameAr,
        input.legalNameEn,
        input.vatNumber,
        input.crnNumber,
        input.egsSerialNumber ?? `1-ORBIT|2-POS|3-${uuidv4().slice(0, 6)}`,
        input.businessCategory ?? 'E-Commerce & Retail',
        input.streetName,
        input.buildingNumber,
        input.districtName ?? 'Al Olaya',
        input.city,
        input.postalCode,
        DEFAULT_CSID_KEYS.csidCertificate,
        DEFAULT_CSID_KEYS.privateKeyPem,
        DEFAULT_CSID_KEYS.publicKeyBase64,
        ZATCA_GENESIS_PIH,
      ]
    );

    return this.normalizeTaxpayer(res.rows[0]);
  }

  async getTaxpayer(taxpayerId: string, exec: ZatcaExecutor = this.db): Promise<TaxpayerRecord> {
    const res = await exec.query<TaxpayerRecord>(
      `SELECT * FROM taxpayers WHERE id = $1`,
      [taxpayerId]
    );
    if (res.rows.length === 0) {
      throw new Error('TAXPAYER_NOT_FOUND');
    }
    return this.normalizeTaxpayer(res.rows[0]);
  }

  async listTaxpayers(): Promise<TaxpayerRecord[]> {
    const res = await this.db.query<TaxpayerRecord>(
      `SELECT * FROM taxpayers ORDER BY created_at ASC`
    );
    return res.rows.map((r) => this.normalizeTaxpayer(r));
  }

  async registerEgsDevice(input: RegisterEgsDeviceInput): Promise<EgsDeviceRecord> {
    await this.getTaxpayer(input.taxpayerId);
    const id = `egs_${uuidv4().slice(0, 8)}`;
    const res = await this.db.query<EgsDeviceRecord>(
      `INSERT INTO egs_devices (
        id, taxpayer_id, egs_unit_id, serial_number, branch_name, city, model_version, status
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, 'ACTIVE')
      RETURNING *`,
      [
        id,
        input.taxpayerId,
        input.egsUnitId,
        input.serialNumber,
        input.branchName,
        input.city,
        input.modelVersion ?? 'ORBIT-POS-V2',
      ]
    );
    return {
      ...res.rows[0],
      created_at: new Date(res.rows[0].created_at).toISOString(),
    };
  }

  async listEgsDevices(taxpayerId: string): Promise<EgsDeviceRecord[]> {
    await this.getTaxpayer(taxpayerId);
    const res = await this.db.query<EgsDeviceRecord>(
      `SELECT * FROM egs_devices WHERE taxpayer_id = $1 ORDER BY created_at ASC`,
      [taxpayerId]
    );
    return res.rows.map((r) => ({
      ...r,
      created_at: new Date(r.created_at).toISOString(),
    }));
  }

  async getTaxpayerComplianceSummary(taxpayerId: string): Promise<TaxpayerComplianceSummary> {
    const taxpayer = await this.getTaxpayer(taxpayerId);
    const statusRes = await this.db.query<{ status: string; cnt: string }>(
      `SELECT status, COUNT(*)::text AS cnt FROM invoices WHERE taxpayer_id = $1 GROUP BY status`,
      [taxpayerId]
    );

    const countsByStatus: Record<string, number> = {
      DRAFT: 0,
      ISSUED: 0,
      REPORTED: 0,
      REPORTED_WITH_WARNINGS: 0,
      CLEARED: 0,
      REJECTED: 0,
    };
    for (const row of statusRes.rows) {
      countsByStatus[row.status] = parseInt(row.cnt, 10);
    }

    const totalsRes = await this.db.query<{
      invoice_type_code: string;
      total_net: string;
      total_vat: string;
      total_gross: string;
    }>(
      `SELECT
         invoice_type_code,
         COALESCE(SUM(taxable_amount_halalas), 0)::text AS total_net,
         COALESCE(SUM(vat_amount_halalas), 0)::text AS total_vat,
         COALESCE(SUM(total_amount_halalas), 0)::text AS total_gross
       FROM invoices
       WHERE taxpayer_id = $1 AND status != 'DRAFT'
       GROUP BY invoice_type_code`,
      [taxpayerId]
    );

    let issuedSalesHalalas = 0;
    let issuedVatHalalas = 0;
    let creditNotesTotalHalalas = 0;
    let debitNotesTotalHalalas = 0;

    for (const row of totalsRes.rows) {
      if (row.invoice_type_code === '388') {
        issuedSalesHalalas += parseInt(row.total_net, 10);
        issuedVatHalalas += parseInt(row.total_vat, 10);
      } else if (row.invoice_type_code === '381') {
        creditNotesTotalHalalas += parseInt(row.total_gross, 10);
      } else if (row.invoice_type_code === '383') {
        debitNotesTotalHalalas += parseInt(row.total_gross, 10);
      }
    }

    return {
      taxpayerId: taxpayer.id,
      vatNumber: taxpayer.vat_number,
      onboardingStatus: taxpayer.onboarding_status,
      lastIcv: taxpayer.last_icv,
      lastInvoiceHash: taxpayer.last_invoice_hash,
      countsByStatus,
      totalsHalalas: {
        issuedSalesHalalas,
        issuedVatHalalas,
        creditNotesTotalHalalas,
        debitNotesTotalHalalas,
      },
    };
  }

  async createDraftInvoice(
    input: CreateDraftInvoiceInput,
    exec: ZatcaExecutor = this.db
  ): Promise<InvoiceWithLines> {
    await this.getTaxpayer(input.taxpayerId, exec);
    const calc = TaxCalculationEngine.calculateInvoiceTotals(input.lines, input.documentAllowanceHalalas ?? 0, {
      explicitVatToleranceHalalas: input.explicitVatToleranceHalalas,
    });

    const invoiceId = `inv_${uuidv4()}`;
    const invoiceUuid = uuidv4();
    const invoiceNumber =
      input.invoiceNumber ||
      `INV-${new Date().getUTCFullYear()}-${uuidv4().slice(0, 6).toUpperCase()}`;
    const supplyDate = input.supplyDate || new Date().toISOString().slice(0, 10);

    await exec.query(
      `INSERT INTO invoices (
        id, invoice_number, uuid, taxpayer_id, egs_device_id,
        invoice_type_code, invoice_subtype, payment_means_code,
        customer_name, customer_vat_number, customer_phone, customer_email, currency,
        subtotal_halalas, discount_halalas, document_allowance_halalas,
        taxable_amount_halalas, vat_amount_halalas, total_amount_halalas,
        supply_date, status, source_order_id, source_journal_ref, source_event_id, document_allowance_reason,
        source_settlement_batch_id, billing_reference_id, instruction_note, source_refund_id
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 'SAR', $13, $14, $15, $16, $17, $18, $19, 'DRAFT', $20, $21, $22, $23, $24, $25, $26, $27)`,
      [
        invoiceId,
        invoiceNumber,
        invoiceUuid,
        input.taxpayerId,
        input.egsDeviceId ?? null,
        input.invoiceTypeCode ?? '388',
        input.invoiceSubtype ?? '0200000',
        input.paymentMeansCode ?? '48',
        input.customerName,
        input.customerVatNumber ?? null,
        input.customerPhone ?? null,
        input.customerEmail ?? null,
        calc.subtotalHalalas,
        calc.discountHalalas,
        calc.documentAllowanceHalalas,
        calc.taxableAmountHalalas,
        calc.vatAmountHalalas,
        calc.totalAmountHalalas,
        supplyDate,
        input.sourceOrderId ?? null,
        input.sourceJournalRef ?? null,
        input.sourceEventId ?? null,
        calc.documentAllowanceHalalas > 0 ? input.documentAllowanceReason ?? null : null,
        input.sourceSettlementBatchId ?? null,
        input.billingReferenceId ?? null,
        input.instructionNote ?? null,
        input.sourceRefundId ?? null,
      ]
    );

    await this.insertCalculatedLines(invoiceId, calc.lines, exec);
    return this.getInvoice(invoiceId, exec);
  }

  async getInvoice(invoiceId: string, exec: ZatcaExecutor = this.db): Promise<InvoiceWithLines> {
    const invRes = await exec.query<InvoiceRecord>(
      `SELECT * FROM invoices WHERE id = $1`,
      [invoiceId]
    );
    if (invRes.rows.length === 0) {
      throw new Error('INVOICE_NOT_FOUND');
    }

    const lineRes = await exec.query<InvoiceLineRecord>(
      `SELECT * FROM invoice_line_items WHERE invoice_id = $1 ORDER BY line_number ASC`,
      [invoiceId]
    );

    return {
      ...this.normalizeInvoice(invRes.rows[0]),
      lines: lineRes.rows.map((l) => this.normalizeLine(l)),
    };
  }

  async listInvoices(filters?: {
    taxpayerId?: string;
    status?: InvoiceStatus;
    invoiceTypeCode?: InvoiceTypeCode;
  }): Promise<InvoiceRecord[]> {
    const clauses: string[] = [];
    const params: unknown[] = [];

    if (filters?.taxpayerId) {
      params.push(filters.taxpayerId);
      clauses.push(`taxpayer_id = $${params.length}`);
    }
    if (filters?.status) {
      params.push(filters.status);
      clauses.push(`status = $${params.length}`);
    }
    if (filters?.invoiceTypeCode) {
      params.push(filters.invoiceTypeCode);
      clauses.push(`invoice_type_code = $${params.length}`);
    }

    const whereSql = clauses.length > 0 ? `WHERE ${clauses.join(' AND ')}` : '';
    const res = await this.db.query<InvoiceRecord>(
      `SELECT * FROM invoices ${whereSql} ORDER BY created_at DESC`,
      params
    );

    return res.rows.map((r) => this.normalizeInvoice(r));
  }

  async updateDraftInvoice(
    invoiceId: string,
    input: UpdateDraftInvoiceInput
  ): Promise<InvoiceWithLines> {
    const current = await this.getInvoice(invoiceId);
    if (current.status !== 'DRAFT') {
      throw new Error('INVOICE_IMMUTABLE_AFTER_ISSUE');
    }

    const customerName = input.customerName ?? current.customer_name;
    const customerPhone =
      input.customerPhone !== undefined ? input.customerPhone : current.customer_phone;
    const customerEmail =
      input.customerEmail !== undefined ? input.customerEmail : current.customer_email;
    const paymentMeansCode = input.paymentMeansCode ?? current.payment_means_code;
    const documentAllowanceHalalas =
      input.documentAllowanceHalalas ?? current.document_allowance_halalas;

    if (input.lines) {
      const calc = TaxCalculationEngine.calculateInvoiceTotals(
        input.lines,
        documentAllowanceHalalas
      );
      await this.db.query(`DELETE FROM invoice_line_items WHERE invoice_id = $1`, [invoiceId]);
      await this.insertCalculatedLines(invoiceId, calc.lines);

      await this.db.query(
        `UPDATE invoices SET
          customer_name = $2,
          customer_phone = $3,
          customer_email = $4,
          payment_means_code = $5,
          subtotal_halalas = $6,
          discount_halalas = $7,
          document_allowance_halalas = $8,
          taxable_amount_halalas = $9,
          vat_amount_halalas = $10,
          total_amount_halalas = $11
         WHERE id = $1`,
        [
          invoiceId,
          customerName,
          customerPhone,
          customerEmail,
          paymentMeansCode,
          calc.subtotalHalalas,
          calc.discountHalalas,
          calc.documentAllowanceHalalas,
          calc.taxableAmountHalalas,
          calc.vatAmountHalalas,
          calc.totalAmountHalalas,
        ]
      );
    } else {
      await this.db.query(
        `UPDATE invoices SET
          customer_name = $2,
          customer_phone = $3,
          customer_email = $4,
          payment_means_code = $5
         WHERE id = $1`,
        [invoiceId, customerName, customerPhone, customerEmail, paymentMeansCode]
      );
    }

    return this.getInvoice(invoiceId);
  }

  async deleteDraftInvoice(invoiceId: string): Promise<void> {
    const current = await this.getInvoice(invoiceId);
    if (current.status !== 'DRAFT') {
      throw new Error('CANNOT_DELETE_ISSUED_INVOICE');
    }
    await this.db.query(`DELETE FROM invoices WHERE id = $1`, [invoiceId]);
  }

  /**
   * Transitions a DRAFT invoice to ISSUED:
   * - Assigns next sequential ICV for the taxpayer
   * - Chains previous_invoice_hash (PIH) from taxpayer.last_invoice_hash
   * - Computes SHA-256 invoice_hash, ECDSA signature, TLV QR Base64, and UBL 2.1 XML
   */
  async issueInvoice(invoiceId: string): Promise<InvoiceWithLines> {
    await this.db.transaction(async (tx) => this.issueInvoiceWithin(tx, invoiceId));
    return this.getInvoice(invoiceId);
  }

  /**
   * The issuance itself, inside the caller's transaction (the order flow issues and records its
   * checkpoint atomically). The taxpayer row is locked FOR UPDATE, so ICVs are handed out strictly
   * one after another and each PIH is the hash of the document issued right before it.
   */
  async issueInvoiceWithin(tx: ZatcaExecutor, invoiceId: string): Promise<void> {
    const invRes = await tx.query<InvoiceRecord>(
      `SELECT * FROM invoices WHERE id = $1 FOR UPDATE`,
      [invoiceId]
    );
    if (invRes.rows.length === 0) {
      throw new Error('INVOICE_NOT_FOUND');
    }
    const invoiceRow = this.normalizeInvoice(invRes.rows[0]);
    if (invoiceRow.status !== 'DRAFT') {
      return;
    }

    const txpRes = await tx.query<TaxpayerRecord>(
      `SELECT * FROM taxpayers WHERE id = $1 FOR UPDATE`,
      [invoiceRow.taxpayer_id]
    );
    if (txpRes.rows.length === 0) {
      throw new Error('TAXPAYER_NOT_FOUND');
    }
    const taxpayer = this.normalizeTaxpayer(txpRes.rows[0]);
    if (taxpayer.onboarding_status === 'REVOKED') {
      throw new Error('TAXPAYER_CSID_REVOKED');
    }

    const lineRes = await tx.query<InvoiceLineRecord>(
      `SELECT * FROM invoice_line_items WHERE invoice_id = $1 ORDER BY line_number ASC`,
      [invoiceId]
    );
    const lines = lineRes.rows.map((l) => this.normalizeLine(l));

    const nextIcv = taxpayer.last_icv + 1;
    const previousInvoiceHash = taxpayer.last_invoice_hash;
    const issuedAtIso = new Date().toISOString();

    const invoiceHash = ZatcaCryptoService.computeInvoiceHash({
      uuid: invoiceRow.uuid,
      invoiceNumber: invoiceRow.invoice_number,
      invoiceTypeCode: invoiceRow.invoice_type_code,
      invoiceSubtype: invoiceRow.invoice_subtype,
      icv: nextIcv,
      previousInvoiceHash,
      sellerVatNumber: taxpayer.vat_number,
      issuedAtIso,
      taxableAmountHalalas: invoiceRow.taxable_amount_halalas,
      vatAmountHalalas: invoiceRow.vat_amount_halalas,
      totalAmountHalalas: invoiceRow.total_amount_halalas,
    });

    const ecdsaSignature = ZatcaCryptoService.signInvoiceHash(
      invoiceHash,
      taxpayer.private_key_pem
    );

    const tlvQrBase64 = ZatcaCryptoService.encodeTlvQrBase64({
      sellerName: taxpayer.legal_name_ar,
      vatNumber: taxpayer.vat_number,
      timestampIso: issuedAtIso,
      totalWithVatHalalas: invoiceRow.total_amount_halalas,
      vatAmountHalalas: invoiceRow.vat_amount_halalas,
      invoiceHash,
      ecdsaSignature,
      publicKeyBase64: taxpayer.public_key_base64,
    });

    const stampedInvoicePreview: InvoiceWithLines = {
      ...invoiceRow,
      lines,
      icv: nextIcv,
      previous_invoice_hash: previousInvoiceHash,
      invoice_hash: invoiceHash,
      ecdsa_signature: ecdsaSignature,
      tlv_qr_base64: tlvQrBase64,
      status: 'ISSUED',
      issued_at: issuedAtIso,
    };

    const ublXml = UblXmlBuilder.buildUblInvoiceXml(stampedInvoicePreview, taxpayer);
    const xmlUblBase64 = Buffer.from(ublXml, 'utf8').toString('base64');

    await tx.query(
      `UPDATE invoices SET
        icv = $2,
        previous_invoice_hash = $3,
        invoice_hash = $4,
        ecdsa_signature = $5,
        tlv_qr_base64 = $6,
        xml_ubl_base64 = $7,
        status = 'ISSUED',
        issued_at = $8
       WHERE id = $1`,
      [
        invoiceRow.id,
        nextIcv,
        previousInvoiceHash,
        invoiceHash,
        ecdsaSignature,
        tlvQrBase64,
        xmlUblBase64,
        issuedAtIso,
      ]
    );

    await tx.query(
      `UPDATE taxpayers SET
        last_icv = $2,
        last_invoice_hash = $3
       WHERE id = $1`,
      [taxpayer.id, nextIcv, invoiceHash]
    );
  }

  /**
   * Walk a taxpayer's chain in ICV order and check it end to end: ICVs are 1..n without gaps, the
   * first PIH is the genesis hash, every PIH equals the previous document's hash, every stored
   * hash recomputes from the document, every ECDSA stamp verifies, and the taxpayer's
   * last_icv / last_invoice_hash point at the chain head.
   */
  async verifyHashChain(taxpayerId: string): Promise<HashChainReport> {
    const taxpayer = await this.getTaxpayer(taxpayerId);
    const res = await this.db.query<InvoiceRecord>(
      `SELECT * FROM invoices WHERE taxpayer_id = $1 AND icv IS NOT NULL ORDER BY icv ASC`,
      [taxpayerId]
    );
    const breaks: string[] = [];
    let expectedPih = ZATCA_GENESIS_PIH;
    let expectedIcv = 1;
    for (const raw of res.rows) {
      const inv = this.normalizeInvoice(raw);
      const label = `ICV ${inv.icv} (${inv.invoice_number})`;
      if (inv.icv !== expectedIcv) breaks.push(`${label}: expected ICV ${expectedIcv}`);
      if (inv.previous_invoice_hash !== expectedPih) breaks.push(`${label}: PIH does not link to the previous document`);
      const recomputed = ZatcaCryptoService.computeInvoiceHash({
        uuid: inv.uuid,
        invoiceNumber: inv.invoice_number,
        invoiceTypeCode: inv.invoice_type_code,
        invoiceSubtype: inv.invoice_subtype,
        icv: inv.icv as number,
        previousInvoiceHash: inv.previous_invoice_hash as string,
        sellerVatNumber: taxpayer.vat_number,
        issuedAtIso: inv.issued_at as string,
        taxableAmountHalalas: inv.taxable_amount_halalas,
        vatAmountHalalas: inv.vat_amount_halalas,
        totalAmountHalalas: inv.total_amount_halalas,
      });
      if (recomputed !== inv.invoice_hash) breaks.push(`${label}: stored hash does not match the document`);
      if (
        !inv.invoice_hash ||
        !inv.ecdsa_signature ||
        !ZatcaCryptoService.verifyInvoiceSignature(inv.invoice_hash, inv.ecdsa_signature, taxpayer.public_key_base64)
      ) {
        breaks.push(`${label}: ECDSA stamp does not verify`);
      }
      expectedPih = inv.invoice_hash ?? '';
      expectedIcv = (inv.icv ?? expectedIcv) + 1;
    }
    const lastIcv = res.rows.length;
    if (taxpayer.last_icv !== lastIcv) breaks.push(`taxpayer.last_icv=${taxpayer.last_icv} but the chain has ${lastIcv} documents`);
    if (taxpayer.last_invoice_hash !== expectedPih) breaks.push('taxpayer.last_invoice_hash is not the chain head');
    return { taxpayerId, ok: breaks.length === 0, length: res.rows.length, lastIcv, breaks };
  }

  /**
   * Creates and immediately issues a Simplified Credit Note (381) referencing an issued invoice.
   */
  async createCreditNote(
    originalInvoiceId: string,
    input: CreateAdjustmentNoteInput
  ): Promise<InvoiceWithLines> {
    const original = await this.getInvoice(originalInvoiceId);
    if (original.status === 'DRAFT') {
      throw new Error('CANNOT_CREDIT_DRAFT_INVOICE');
    }

    const calc = TaxCalculationEngine.calculateInvoiceTotals(input.lines);
    if (calc.totalAmountHalalas > original.total_amount_halalas) {
      throw new Error('CREDIT_NOTE_EXCEEDS_ORIGINAL_TOTAL');
    }

    const creditNoteId = `inv_${uuidv4()}`;
    const creditNoteUuid = uuidv4();
    const creditNoteNumber = `CN-${new Date().getUTCFullYear()}-${uuidv4().slice(0, 6).toUpperCase()}`;

    await this.db.query(
      `INSERT INTO invoices (
        id, invoice_number, uuid, taxpayer_id, egs_device_id,
        invoice_type_code, invoice_subtype, payment_means_code,
        billing_reference_id, instruction_note,
        customer_name, customer_vat_number, customer_phone, customer_email, currency,
        subtotal_halalas, discount_halalas, document_allowance_halalas,
        taxable_amount_halalas, vat_amount_halalas, total_amount_halalas,
        status
      ) VALUES ($1, $2, $3, $4, $5, '381', $6, $7, $8, $9, $10, $11, $12, $13, 'SAR', $14, $15, 0, $16, $17, $18, 'DRAFT')`,
      [
        creditNoteId,
        creditNoteNumber,
        creditNoteUuid,
        original.taxpayer_id,
        original.egs_device_id,
        original.invoice_subtype,
        input.paymentMeansCode ?? original.payment_means_code,
        original.id,
        input.instructionNote,
        original.customer_name,
        original.customer_vat_number,
        original.customer_phone,
        original.customer_email,
        calc.subtotalHalalas,
        calc.discountHalalas,
        calc.taxableAmountHalalas,
        calc.vatAmountHalalas,
        calc.totalAmountHalalas,
      ]
    );

    await this.insertCalculatedLines(creditNoteId, calc.lines);
    return this.issueInvoice(creditNoteId);
  }

  /**
   * Creates and immediately issues a Simplified Debit Note (383) for post-issuance price/quantity upward adjustments.
   */
  async createDebitNote(
    originalInvoiceId: string,
    input: CreateAdjustmentNoteInput
  ): Promise<InvoiceWithLines> {
    const original = await this.getInvoice(originalInvoiceId);
    if (original.status === 'DRAFT') {
      throw new Error('CANNOT_DEBIT_DRAFT_INVOICE');
    }

    const calc = TaxCalculationEngine.calculateInvoiceTotals(input.lines);
    const debitNoteId = `inv_${uuidv4()}`;
    const debitNoteUuid = uuidv4();
    const debitNoteNumber = `DN-${new Date().getUTCFullYear()}-${uuidv4().slice(0, 6).toUpperCase()}`;

    await this.db.query(
      `INSERT INTO invoices (
        id, invoice_number, uuid, taxpayer_id, egs_device_id,
        invoice_type_code, invoice_subtype, payment_means_code,
        billing_reference_id, instruction_note,
        customer_name, customer_vat_number, customer_phone, customer_email, currency,
        subtotal_halalas, discount_halalas, document_allowance_halalas,
        taxable_amount_halalas, vat_amount_halalas, total_amount_halalas,
        status
      ) VALUES ($1, $2, $3, $4, $5, '383', $6, $7, $8, $9, $10, $11, $12, $13, 'SAR', $14, $15, 0, $16, $17, $18, 'DRAFT')`,
      [
        debitNoteId,
        debitNoteNumber,
        debitNoteUuid,
        original.taxpayer_id,
        original.egs_device_id,
        original.invoice_subtype,
        input.paymentMeansCode ?? original.payment_means_code,
        original.id,
        input.instructionNote,
        original.customer_name,
        original.customer_vat_number,
        original.customer_phone,
        original.customer_email,
        calc.subtotalHalalas,
        calc.discountHalalas,
        calc.taxableAmountHalalas,
        calc.vatAmountHalalas,
        calc.totalAmountHalalas,
      ]
    );

    await this.insertCalculatedLines(debitNoteId, calc.lines);
    return this.issueInvoice(debitNoteId);
  }

  private async insertCalculatedLines(
    invoiceId: string,
    lines: ReturnType<typeof TaxCalculationEngine.calculateInvoiceTotals>['lines'],
    exec: ZatcaExecutor = this.db
  ): Promise<void> {
    for (const line of lines) {
      await exec.query(
        `INSERT INTO invoice_line_items (
          id, invoice_id, line_number, item_name_ar, item_name_en, sku,
          quantity, unit_price_halalas, discount_halalas, net_amount_halalas,
          vat_category, vat_rate_bps, vat_exemption_code, vat_exemption_reason,
          vat_amount_halalas, line_total_halalas
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)`,
        [
          `line_${uuidv4()}`,
          invoiceId,
          line.lineNumber,
          line.itemNameAr,
          line.itemNameEn,
          line.sku,
          line.quantity,
          line.unitPriceHalalas,
          line.discountHalalas,
          line.netAmountHalalas,
          line.vatCategory,
          line.vatRateBps,
          line.vatExemptionCode,
          line.vatExemptionReason,
          line.vatAmountHalalas,
          line.lineTotalHalalas,
        ]
      );
    }
  }

  private normalizeTaxpayer(row: TaxpayerRecord): TaxpayerRecord {
    return {
      ...row,
      last_icv: Number(row.last_icv),
      csid_expires_at: new Date(row.csid_expires_at).toISOString(),
      created_at: new Date(row.created_at).toISOString(),
    };
  }

  private normalizeInvoice(row: InvoiceRecord): InvoiceRecord {
    return {
      ...row,
      icv: row.icv !== null ? Number(row.icv) : null,
      subtotal_halalas: Number(row.subtotal_halalas),
      discount_halalas: Number(row.discount_halalas),
      document_allowance_halalas: Number(row.document_allowance_halalas),
      taxable_amount_halalas: Number(row.taxable_amount_halalas),
      vat_amount_halalas: Number(row.vat_amount_halalas),
      total_amount_halalas: Number(row.total_amount_halalas),
      zatca_warnings: Array.isArray(row.zatca_warnings) ? row.zatca_warnings : [],
      zatca_errors: Array.isArray(row.zatca_errors) ? row.zatca_errors : [],
      zatca_reported_at: row.zatca_reported_at
        ? new Date(row.zatca_reported_at).toISOString()
        : null,
      supply_date:
        typeof row.supply_date === 'string'
          ? row.supply_date.slice(0, 10)
          : new Date(row.supply_date).toISOString().slice(0, 10),
      issued_at: row.issued_at ? new Date(row.issued_at).toISOString() : null,
      created_at: new Date(row.created_at).toISOString(),
    };
  }

  private normalizeLine(row: InvoiceLineRecord): InvoiceLineRecord {
    return {
      ...row,
      line_number: Number(row.line_number),
      quantity: Number(row.quantity),
      unit_price_halalas: Number(row.unit_price_halalas),
      discount_halalas: Number(row.discount_halalas),
      net_amount_halalas: Number(row.net_amount_halalas),
      vat_rate_bps: Number(row.vat_rate_bps),
      vat_amount_halalas: Number(row.vat_amount_halalas),
      line_total_halalas: Number(row.line_total_halalas),
      refunded_quantity: Number(row.refunded_quantity),
    };
  }
}
