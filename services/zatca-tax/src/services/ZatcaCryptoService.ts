import crypto from 'crypto';
import { TaxCalculationEngine } from './TaxCalculationEngine';

export interface CanonicalInvoicePayload {
  uuid: string;
  invoiceNumber: string;
  invoiceTypeCode: string;
  invoiceSubtype: string;
  icv: number;
  previousInvoiceHash: string;
  sellerVatNumber: string;
  issuedAtIso: string;
  taxableAmountHalalas: number;
  vatAmountHalalas: number;
  totalAmountHalalas: number;
}

export interface ZatcaTlvInput {
  sellerName: string;
  vatNumber: string;
  timestampIso: string;
  totalWithVatHalalas: number;
  vatAmountHalalas: number;
  invoiceHash?: string;
  ecdsaSignature?: string;
  publicKeyBase64?: string;
}

export class ZatcaCryptoService {
  static validateKsaVatNumber(vatNumber: string): boolean {
    return /^3\d{13}3$/.test(vatNumber);
  }

  static computeInvoiceHash(payload: CanonicalInvoicePayload): string {
    const canonical = [
      `UUID=${payload.uuid}`,
      `INV=${payload.invoiceNumber}`,
      `TYPE=${payload.invoiceTypeCode}:${payload.invoiceSubtype}`,
      `ICV=${payload.icv}`,
      `PIH=${payload.previousInvoiceHash}`,
      `VAT=${payload.sellerVatNumber}`,
      `TS=${payload.issuedAtIso}`,
      `NET=${payload.taxableAmountHalalas}`,
      `TAX=${payload.vatAmountHalalas}`,
      `TOT=${payload.totalAmountHalalas}`,
    ].join('|');

    return crypto.createHash('sha256').update(canonical, 'utf8').digest('base64');
  }

  static signInvoiceHash(invoiceHashBase64: string, privateKeyPem: string): string {
    const sign = crypto.createSign('SHA256');
    sign.update(Buffer.from(invoiceHashBase64, 'base64'));
    sign.end();
    return sign.sign(privateKeyPem, 'base64');
  }

  static verifyInvoiceSignature(
    invoiceHashBase64: string,
    signatureBase64: string,
    publicKeyBase64: string
  ): boolean {
    const pubKeyObj = crypto.createPublicKey({
      key: Buffer.from(publicKeyBase64, 'base64'),
      format: 'der',
      type: 'spki',
    });
    const verify = crypto.createVerify('SHA256');
    verify.update(Buffer.from(invoiceHashBase64, 'base64'));
    verify.end();
    return verify.verify(pubKeyObj, Buffer.from(signatureBase64, 'base64'));
  }

  /**
   * Encodes ZATCA Tag-Length-Value (TLV) fields (Tags 1-5) into Base64 for QR codes.
   * Uses UTF-8 byte lengths for full Arabic and English seller name support.
   */
  static encodeTlvQrBase64(input: ZatcaTlvInput): string {
    const fields: Array<{ tag: number; value: string }> = [
      { tag: 1, value: input.sellerName },
      { tag: 2, value: input.vatNumber },
      { tag: 3, value: input.timestampIso },
      { tag: 4, value: TaxCalculationEngine.formatSar(input.totalWithVatHalalas) },
      { tag: 5, value: TaxCalculationEngine.formatSar(input.vatAmountHalalas) },
    ];

    const buffers: Buffer[] = fields.map(({ tag, value }) => {
      const valueBuf = Buffer.from(value, 'utf8');
      const header = Buffer.from([tag, valueBuf.byteLength]);
      return Buffer.concat([header, valueBuf]);
    });

    return Buffer.concat(buffers).toString('base64');
  }
}
