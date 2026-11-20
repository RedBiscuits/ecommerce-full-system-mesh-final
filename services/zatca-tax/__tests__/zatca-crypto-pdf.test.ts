import { describe, it, expect, beforeEach } from 'vitest';
import { buildApp } from '../src/server';
import { resetDatabaseAndSeed } from '../src/db';
import { ZatcaCryptoService } from '../src/services/ZatcaCryptoService';

describe('ZATCA Cryptographic Stamping, UBL XML & PDF Generation', () => {
  beforeEach(async () => {
    await resetDatabaseAndSeed();
  });

  it('verifies ECDSA invoice hash signature and detects hash tampering', async () => {
    const { app } = await buildApp();

    const draft = (
      await app.inject({
        method: 'POST',
        url: '/api/v1/invoices',
        payload: {
          taxpayerId: 'txp_riyadh_retail',
          customerName: 'Khalid Al-Harbi',
          lines: [
            {
              itemNameAr: 'لوحة مفاتيح ميكانيكية',
              itemNameEn: 'Mechanical Keyboard',
              sku: 'KBD-900',
              quantity: 1,
              unitPriceHalalas: 35000,
              vatCategory: 'S',
            },
          ],
        },
      })
    ).json();

    const issued = (
      await app.inject({
        method: 'POST',
        url: `/api/v1/invoices/${draft.id}/issue`,
      })
    ).json();

    const taxpayer = (
      await app.inject({
        method: 'GET',
        url: '/api/v1/taxpayers/txp_riyadh_retail',
      })
    ).json();

    const isValidSig = ZatcaCryptoService.verifyInvoiceSignature(
      issued.invoice_hash,
      issued.ecdsa_signature,
      taxpayer.public_key_base64
    );
    expect(isValidSig).toBe(true);

    const tamperedHash = Buffer.from('tampered-digest-payload-00000000', 'utf8').toString('base64');
    const isTamperedValid = ZatcaCryptoService.verifyInvoiceSignature(
      tamperedHash,
      issued.ecdsa_signature,
      taxpayer.public_key_base64
    );
    expect(isTamperedValid).toBe(false);

    await app.close();
  });

  it('generates and streams a valid PDF tax invoice with SHA-256 header', async () => {
    const { app } = await buildApp();

    const draft = (
      await app.inject({
        method: 'POST',
        url: '/api/v1/invoices',
        payload: {
          taxpayerId: 'txp_riyadh_retail',
          customerName: 'Sultan Al-Dossary',
          lines: [
            {
              itemNameAr: 'كاميرا رقمية',
              itemNameEn: 'Mirrorless Digital Camera',
              sku: 'CAM-4K',
              quantity: 1,
              unitPriceHalalas: 320000,
              vatCategory: 'S',
            },
          ],
        },
      })
    ).json();

    const issued = (
      await app.inject({
        method: 'POST',
        url: `/api/v1/invoices/${draft.id}/issue`,
      })
    ).json();

    const pdfRes = await app.inject({
      method: 'GET',
      url: `/api/v1/invoices/${issued.id}/pdf`,
    });

    expect(pdfRes.statusCode).toBe(200);
    expect(pdfRes.headers['content-type']).toBe('application/pdf');
    expect(pdfRes.headers['x-content-sha256']).toMatch(/^[a-f0-9]{64}$/);
    expect(pdfRes.rawPayload.subarray(0, 5).toString('ascii')).toBe('%PDF-');
    expect(pdfRes.rawPayload.byteLength).toBeGreaterThan(800);

    await app.close();
  });

  it('renders a Credit Note (381) PDF with billing reference metadata', async () => {
    const { app } = await buildApp();

    const draft = (
      await app.inject({
        method: 'POST',
        url: '/api/v1/invoices',
        payload: {
          taxpayerId: 'txp_riyadh_retail',
          customerName: 'Lama Al-Shehri',
          lines: [
            {
              itemNameAr: 'حقيبة سفر',
              itemNameEn: 'Hard-Shell Carry-On Luggage',
              sku: 'BAG-20',
              quantity: 1,
              unitPriceHalalas: 60000,
              vatCategory: 'S',
            },
          ],
        },
      })
    ).json();

    const issued = (
      await app.inject({
        method: 'POST',
        url: `/api/v1/invoices/${draft.id}/issue`,
      })
    ).json();

    const cn = (
      await app.inject({
        method: 'POST',
        url: `/api/v1/invoices/${issued.id}/credit-notes`,
        payload: {
          instructionNote: 'Full return within 7-day policy',
          lines: [
            {
              itemNameAr: 'حقيبة سفر',
              itemNameEn: 'Hard-Shell Carry-On Luggage',
              sku: 'BAG-20',
              quantity: 1,
              unitPriceHalalas: 60000,
              vatCategory: 'S',
            },
          ],
        },
      })
    ).json();

    const pdfRes = await app.inject({
      method: 'GET',
      url: `/api/v1/invoices/${cn.id}/pdf`,
    });

    expect(pdfRes.statusCode).toBe(200);
    expect(pdfRes.rawPayload.subarray(0, 5).toString('ascii')).toBe('%PDF-');

    await app.close();
  });

  it('serves the UBL 2.1 XML document via GET /api/v1/invoices/:id/xml', async () => {
    const { app } = await buildApp();

    const draft = (
      await app.inject({
        method: 'POST',
        url: '/api/v1/invoices',
        payload: {
          taxpayerId: 'txp_riyadh_retail',
          customerName: 'Yasser Al-Shammari',
          lines: [
            {
              itemNameAr: 'ماوس لاسلكي',
              itemNameEn: 'Ergonomic Wireless Mouse',
              sku: 'MSE-10',
              quantity: 2,
              unitPriceHalalas: 15000,
              vatCategory: 'S',
            },
          ],
        },
      })
    ).json();

    const issued = (
      await app.inject({
        method: 'POST',
        url: `/api/v1/invoices/${draft.id}/issue`,
      })
    ).json();

    const xmlRes = await app.inject({
      method: 'GET',
      url: `/api/v1/invoices/${issued.id}/xml`,
    });

    expect(xmlRes.statusCode).toBe(200);
    expect(xmlRes.headers['content-type']).toContain('application/xml');
    expect(xmlRes.body).toContain('<Invoice xmlns="urn:oasis:names:specification:ubl:schema:xsd:Invoice-2"');
    expect(xmlRes.body).toContain(`<cbc:UUID>${issued.uuid}</cbc:UUID>`);

    await app.close();
  });

  it('encodes Base64 TLV QR payloads on issued invoices', async () => {
    const { app } = await buildApp();

    const draft = (
      await app.inject({
        method: 'POST',
        url: '/api/v1/invoices',
        payload: {
          taxpayerId: 'txp_riyadh_retail',
          customerName: 'Tariq Al-Jasser',
          lines: [
            {
              itemNameAr: 'ميكروفون احترافي',
              itemNameEn: 'Studio Condenser Microphone',
              sku: 'MIC-PRO',
              quantity: 1,
              unitPriceHalalas: 50000,
              vatCategory: 'S',
            },
          ],
        },
      })
    ).json();

    const issued = (
      await app.inject({
        method: 'POST',
        url: `/api/v1/invoices/${draft.id}/issue`,
      })
    ).json();

    const rawTlv = Buffer.from(issued.tlv_qr_base64, 'base64');
    expect(rawTlv[0]).toBe(1); // Tag 1: Seller Name
    expect(rawTlv.byteLength).toBeGreaterThan(30);

    await app.close();
  });
});
