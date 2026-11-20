import PDFDocument from 'pdfkit';
import crypto from 'crypto';
import type { PGlite } from '@electric-sql/pglite';
import { InvoiceWithLines, TaxpayerRecord } from '../types/zatca';
import { TaxCalculationEngine } from './TaxCalculationEngine';

export interface GeneratedPdfArtifact {
  invoiceId: string;
  contentSha256: string;
  byteSize: number;
  buffer: Buffer;
  generatedAt: string;
}

export class PdfInvoiceGenerator {
  constructor(private readonly db: PGlite) {}

  async generateAndStorePdf(
    invoice: InvoiceWithLines,
    taxpayer: TaxpayerRecord
  ): Promise<GeneratedPdfArtifact> {
    const buffer = await this.renderInvoicePdfBuffer(invoice, taxpayer);
    const contentSha256 = crypto.createHash('sha256').update(buffer).digest('hex');
    const pdfBase64 = buffer.toString('base64');

    const inserted = await this.db.query<{ generated_at: string }>(
      `INSERT INTO invoice_pdf_artifacts (invoice_id, content_sha256, byte_size, pdf_base64)
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (invoice_id) DO UPDATE SET
         content_sha256 = EXCLUDED.content_sha256,
         byte_size = EXCLUDED.byte_size,
         pdf_base64 = EXCLUDED.pdf_base64,
         generated_at = NOW()
       RETURNING generated_at`,
      [invoice.id, contentSha256, buffer.byteLength, pdfBase64]
    );

    return {
      invoiceId: invoice.id,
      contentSha256,
      byteSize: buffer.byteLength,
      buffer,
      generatedAt: new Date(inserted.rows[0].generated_at).toISOString(),
    };
  }

  private renderInvoicePdfBuffer(
    invoice: InvoiceWithLines,
    taxpayer: TaxpayerRecord
  ): Promise<Buffer> {
    return renderInvoicePdf(invoice, taxpayer);
  }
}

/** The heading of a document: type (388 / 381 / 383) and subtype (standard B2B / simplified B2C). */
export function documentTitle(typeCode: string, subtype: string): string {
  const standard = subtype === '0100000';
  if (typeCode === '381') return standard ? 'CREDIT NOTE (381 - 0100000)' : 'SIMPLIFIED CREDIT NOTE (381)';
  if (typeCode === '383') return standard ? 'DEBIT NOTE (383 - 0100000)' : 'SIMPLIFIED DEBIT NOTE (383)';
  return standard ? 'TAX INVOICE (B2B - 0100000)' : 'SIMPLIFIED TAX INVOICE (B2C - 0200000)';
}

/** Seller fields printed on the invoice. */
export type PdfSeller = Pick<
  TaxpayerRecord,
  'legal_name_en' | 'vat_number' | 'crn_number' | 'building_number' | 'street_name' | 'city' | 'postal_code' | 'country_code'
>;

/**
 * Render a KSA simplified tax invoice / credit note / debit note to PDF bytes. Pure: no database,
 * so any service holding the document (e.g. the file cache, from the invoice-reported event) can
 * render it.
 */
export interface PdfRenderOptions {
  billingReferenceNumber?: string;
  /** Credit notes issued against this invoice (printed with the amount still standing). */
  creditNotes?: Array<{ invoiceNumber: string; icv: number; totalHalalas: number; refundId?: string | null }>;
}

export function renderInvoicePdf(invoice: InvoiceWithLines, taxpayer: PdfSeller, options: PdfRenderOptions = {}): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const doc = new PDFDocument({
      size: 'A4',
      margin: 45,
      info: {
        Title: `ZATCA Tax Invoice ${invoice.invoice_number}`,
        Author: taxpayer.legal_name_en,
        Subject: `UUID:${invoice.uuid} ICV:${invoice.icv ?? 'DRAFT'}`,
      },
    });

    const chunks: Buffer[] = [];
    doc.on('data', (chunk: Buffer) => chunks.push(chunk));
    doc.on('end', () => resolve(Buffer.concat(chunks)));
    doc.on('error', (err) => reject(err));

    const docTitle = documentTitle(invoice.invoice_type_code, invoice.invoice_subtype);

    doc.fontSize(16).text(docTitle, { align: 'center' });
    doc.moveDown(0.5);
    doc.fontSize(10).text(`Seller: ${taxpayer.legal_name_en}`, { align: 'left' });
    doc.text(`VAT Registration Number: ${taxpayer.vat_number}`);
    doc.text(`Commercial Registration (CRN): ${taxpayer.crn_number}`);
    doc.text(
      `Address: ${taxpayer.building_number} ${taxpayer.street_name}, ${taxpayer.city} ${taxpayer.postal_code}, ${taxpayer.country_code}`
    );

    doc.moveDown(0.8);
    doc.text(`Invoice Number: ${invoice.invoice_number}`);
    doc.text(`Invoice UUID: ${invoice.uuid}`);
    doc.text(`Status: ${invoice.status}`);
    doc.text(`ICV (Invoice Counter Value): ${invoice.icv ?? 'UNASSIGNED'}`);
    doc.text(`Issued At: ${invoice.issued_at ?? 'N/A'}`);
    doc.text(`Customer: ${invoice.customer_name}`);
    if (invoice.customer_vat_number) {
      doc.text(`Customer VAT Registration Number: ${invoice.customer_vat_number}`);
    }
    if (invoice.billing_reference_id) {
      const number = options.billingReferenceNumber ? `${options.billingReferenceNumber} ` : '';
      doc.text(`Billing Reference Invoice: ${number}(${invoice.billing_reference_id})`);
    }
    if (invoice.instruction_note) {
      doc.text(`Reason for Issuance: ${invoice.instruction_note}`);
    }

    doc.moveDown(0.8);
    doc.fontSize(11).text('Line Items (Amounts in SAR)', { underline: true });
    doc.moveDown(0.4);

    doc.fontSize(9);
    for (const line of invoice.lines) {
      const unitSar = TaxCalculationEngine.formatSar(line.unit_price_halalas);
      const netSar = TaxCalculationEngine.formatSar(line.net_amount_halalas);
      const vatSar = TaxCalculationEngine.formatSar(line.vat_amount_halalas);
      const totalSar = TaxCalculationEngine.formatSar(line.line_total_halalas);
      doc.text(
        `${line.line_number}. [${line.sku}] ${line.item_name_en} | Qty: ${line.quantity} x ${unitSar} SAR | Net: ${netSar} SAR | VAT (${line.vat_category} ${line.vat_rate_bps / 100}%): ${vatSar} SAR | Total: ${totalSar} SAR`
      );
    }

    doc.moveDown(0.8);
    doc.fontSize(10);
    doc.text(`Subtotal (Excl. VAT): ${TaxCalculationEngine.formatSar(invoice.subtotal_halalas)} SAR`);
    doc.text(`Total Discount: ${TaxCalculationEngine.formatSar(invoice.discount_halalas)} SAR`);
    if (invoice.document_allowance_halalas > 0) {
      const reason = invoice.document_allowance_reason ?? 'Document-level discount';
      doc.text(`Allowance – ${reason}: -${TaxCalculationEngine.formatSar(invoice.document_allowance_halalas)} SAR`);
    }
    doc.text(`Taxable Amount: ${TaxCalculationEngine.formatSar(invoice.taxable_amount_halalas)} SAR`);
    doc.text(`Total VAT: ${TaxCalculationEngine.formatSar(invoice.vat_amount_halalas)} SAR`);
    doc.fontSize(12).text(
      `Grand Total (Incl. VAT): ${TaxCalculationEngine.formatSar(invoice.total_amount_halalas)} SAR`
    );

    // VAT breakdown per category: line net, allowance share, taxable amount, rate, VAT.
    const breakdown = TaxCalculationEngine.breakdownForInvoice(invoice);
    if (breakdown.length > 0) {
      doc.moveDown(0.6);
      doc.fontSize(10).text('VAT Breakdown (Amounts in SAR)', { underline: true });
      doc.fontSize(9);
      for (const c of breakdown) {
        const exemption = c.exemptionCode ? ` ${c.exemptionCode}` : '';
        doc.text(
          `${c.vatCategory} ${(c.vatRateBps / 100).toFixed(2)}%${exemption} | Net: ${TaxCalculationEngine.formatSar(c.lineNetHalalas)} | ` +
            `Allowance: -${TaxCalculationEngine.formatSar(c.allowanceHalalas)} | Taxable: ${TaxCalculationEngine.formatSar(c.taxableAmountHalalas)} | ` +
            `VAT: ${TaxCalculationEngine.formatSar(c.vatAmountHalalas)}`
        );
      }
    }

    if (options.creditNotes && options.creditNotes.length > 0) {
      const credited = options.creditNotes.reduce((s, n) => s + n.totalHalalas, 0);
      doc.moveDown(0.6);
      doc.fontSize(10).text('Credit Notes Applied (Amounts in SAR)', { underline: true });
      doc.fontSize(9);
      for (const n of options.creditNotes) {
        doc.text(`${n.invoiceNumber} (ICV ${n.icv})${n.refundId ? ` refund ${n.refundId}` : ''}: -${TaxCalculationEngine.formatSar(n.totalHalalas)}`);
      }
      doc.fontSize(10).text(`Total Credited: -${TaxCalculationEngine.formatSar(credited)} SAR`);
      doc.text(`Amount Standing After Credits: ${TaxCalculationEngine.formatSar(invoice.total_amount_halalas - credited)} SAR`);
    }

    doc.moveDown(1);
    doc.fontSize(8);
    doc.text(`Previous Invoice Hash (PIH): ${invoice.previous_invoice_hash ?? 'N/A'}`);
    doc.text(`Invoice Cryptographic Hash: ${invoice.invoice_hash ?? 'N/A'}`);
    doc.text(`TLV QR Code Base64: ${invoice.tlv_qr_base64 ?? 'N/A'}`);

    doc.end();
  });
}
