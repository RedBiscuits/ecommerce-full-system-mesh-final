import { describe, it, expect, beforeEach } from 'vitest';
import { buildApp } from '../src/server';
import { resetDatabaseAndSeed, ZATCA_GENESIS_PIH } from '../src/db';

describe('ZATCA Tax Invoices, EGS Devices & Taxpayer CRUD API', () => {
  beforeEach(async () => {
    await resetDatabaseAndSeed();
  });

  it('registers a KSA taxpayer and validates 15-digit VAT format', async () => {
    const { app } = await buildApp();

    const invalidRes = await app.inject({
      method: 'POST',
      url: '/api/v1/taxpayers',
      payload: {
        legalNameAr: 'شركة جدة للتجارة',
        legalNameEn: 'Jeddah Trading Co.',
        vatNumber: '123456789012345',
        crnNumber: '4030112233',
        streetName: 'Tahlia Street',
        buildingNumber: '8120',
        city: 'Jeddah',
        postalCode: '23432',
      },
    });
    expect(invalidRes.statusCode).toBe(400);

    const validRes = await app.inject({
      method: 'POST',
      url: '/api/v1/taxpayers',
      payload: {
        id: 'txp_jeddah_store',
        legalNameAr: 'شركة جدة للتجارة',
        legalNameEn: 'Jeddah Trading Co.',
        vatNumber: '310998877600003',
        crnNumber: '4030112233',
        streetName: 'Tahlia Street',
        buildingNumber: '8120',
        city: 'Jeddah',
        postalCode: '23432',
      },
    });
    expect(validRes.statusCode).toBe(201);
    const body = validRes.json();
    expect(body.vat_number).toBe('310998877600003');
    expect(body.last_icv).toBe(0);
    expect(body.last_invoice_hash).toBe(ZATCA_GENESIS_PIH);

    await app.close();
  });

  it('registers and lists EGS POS terminals for a taxpayer branch', async () => {
    const { app } = await buildApp();

    const regRes = await app.inject({
      method: 'POST',
      url: '/api/v1/taxpayers/txp_riyadh_retail/egs-devices',
      payload: {
        egsUnitId: 'EGS-RYD-MALQA-02',
        serialNumber: '1-ORBIT|2-POS-RYD|3-900121',
        branchName: 'Al Malqa North Branch',
        city: 'Riyadh',
      },
    });

    expect(regRes.statusCode).toBe(201);
    expect(regRes.json().egs_unit_id).toBe('EGS-RYD-MALQA-02');

    const listRes = await app.inject({
      method: 'GET',
      url: '/api/v1/taxpayers/txp_riyadh_retail/egs-devices',
    });
    expect(listRes.statusCode).toBe(200);
    expect(listRes.json().devices).toHaveLength(2);

    await app.close();
  });

  it('creates, updates, and deletes a DRAFT B2C invoice with 15% KSA VAT in halalas', async () => {
    const { app } = await buildApp();

    const createRes = await app.inject({
      method: 'POST',
      url: '/api/v1/invoices',
      payload: {
        taxpayerId: 'txp_riyadh_retail',
        egsDeviceId: 'egs_ryd_main_01',
        paymentMeansCode: '48',
        customerName: 'Fahad Al-Otaibi',
        customerPhone: '+966501234567',
        lines: [
          {
            itemNameAr: 'قهوة مختصة 250 جرام',
            itemNameEn: 'Specialty Coffee Beans 250g',
            sku: 'COF-250',
            quantity: 2,
            unitPriceHalalas: 5000,
            discountHalalas: 1000,
            vatCategory: 'S',
          },
          {
            itemNameAr: 'كتاب تعليمي معفى',
            itemNameEn: 'Zero-Rated Educational Supply',
            sku: 'EDU-001',
            quantity: 1,
            unitPriceHalalas: 3000,
            vatCategory: 'Z',
            vatExemptionCode: 'VATEX-SA-EDU',
          },
        ],
      },
    });

    expect(createRes.statusCode).toBe(201);
    const created = createRes.json();
    expect(created.status).toBe('DRAFT');
    expect(created.subtotal_halalas).toBe(13000);
    expect(created.discount_halalas).toBe(1000);
    expect(created.taxable_amount_halalas).toBe(12000);
    expect(created.vat_amount_halalas).toBe(1350);
    expect(created.total_amount_halalas).toBe(13350);
    expect(created.lines).toHaveLength(2);

    const patchRes = await app.inject({
      method: 'PATCH',
      url: `/api/v1/invoices/${created.id}`,
      payload: {
        customerName: 'Fahad Al-Otaibi (VIP)',
        lines: [
          {
            itemNameAr: 'قهوة مختصة 250 جرام',
            itemNameEn: 'Specialty Coffee Beans 250g',
            sku: 'COF-250',
            quantity: 1,
            unitPriceHalalas: 10000,
            vatCategory: 'S',
          },
        ],
      },
    });

    expect(patchRes.statusCode).toBe(200);
    const updated = patchRes.json();
    expect(updated.customer_name).toBe('Fahad Al-Otaibi (VIP)');
    expect(updated.total_amount_halalas).toBe(11500);

    const delRes = await app.inject({
      method: 'DELETE',
      url: `/api/v1/invoices/${created.id}`,
    });
    expect(delRes.statusCode).toBe(204);

    await app.close();
  });

  it('prevents modifying or deleting an invoice once it has been ISSUED', async () => {
    const { app } = await buildApp();

    const draft = (
      await app.inject({
        method: 'POST',
        url: '/api/v1/invoices',
        payload: {
          taxpayerId: 'txp_riyadh_retail',
          customerName: 'Reem Al-Subaie',
          lines: [
            {
              itemNameAr: 'ساعة ذكية',
              itemNameEn: 'Smart Watch',
              sku: 'WCH-01',
              quantity: 1,
              unitPriceHalalas: 80000,
              vatCategory: 'S',
            },
          ],
        },
      })
    ).json();

    await app.inject({
      method: 'POST',
      url: `/api/v1/invoices/${draft.id}/issue`,
    });

    const patchRes = await app.inject({
      method: 'PATCH',
      url: `/api/v1/invoices/${draft.id}`,
      payload: { customerName: 'Tampered Name' },
    });
    expect(patchRes.statusCode).toBe(400);
    expect(patchRes.json().error).toBe('INVOICE_IMMUTABLE_AFTER_ISSUE');

    const delRes = await app.inject({
      method: 'DELETE',
      url: `/api/v1/invoices/${draft.id}`,
    });
    expect(delRes.statusCode).toBe(400);
    expect(delRes.json().error).toBe('CANNOT_DELETE_ISSUED_INVOICE');

    await app.close();
  });

  it('issues sequential B2C invoices chaining ICV and PIH and supports Credit Notes (381)', async () => {
    const { app } = await buildApp();

    const draft1 = (
      await app.inject({
        method: 'POST',
        url: '/api/v1/invoices',
        payload: {
          taxpayerId: 'txp_riyadh_retail',
          customerName: 'Sara Al-Qahtani',
          lines: [
            {
              itemNameAr: 'سماعات لاسلكية',
              itemNameEn: 'Wireless Earbuds',
              sku: 'AUD-100',
              quantity: 2,
              unitPriceHalalas: 20000,
              vatCategory: 'S',
            },
          ],
        },
      })
    ).json();

    const issue1 = (
      await app.inject({
        method: 'POST',
        url: `/api/v1/invoices/${draft1.id}/issue`,
      })
    ).json();

    expect(issue1.status).toBe('ISSUED');
    expect(issue1.icv).toBe(1);
    expect(issue1.previous_invoice_hash).toBe(ZATCA_GENESIS_PIH);
    expect(issue1.invoice_hash).toBeTruthy();
    expect(issue1.ecdsa_signature).toBeTruthy();
    expect(issue1.tlv_qr_base64).toBeTruthy();

    const cnRes = await app.inject({
      method: 'POST',
      url: `/api/v1/invoices/${issue1.id}/credit-notes`,
      payload: {
        instructionNote: 'Customer returned 1 unit due to defective charging case',
        lines: [
          {
            itemNameAr: 'سماعات لاسلكية',
            itemNameEn: 'Wireless Earbuds',
            sku: 'AUD-100',
            quantity: 1,
            unitPriceHalalas: 20000,
            vatCategory: 'S',
          },
        ],
      },
    });

    expect(cnRes.statusCode).toBe(201);
    const creditNote = cnRes.json();
    expect(creditNote.invoice_type_code).toBe('381');
    expect(creditNote.billing_reference_id).toBe(issue1.id);
    expect(creditNote.icv).toBe(2);
    expect(creditNote.previous_invoice_hash).toBe(issue1.invoice_hash);
    expect(creditNote.total_amount_halalas).toBe(23000);

    await app.close();
  });

  it('issues a Simplified Debit Note (383) for post-issuance upward price adjustments', async () => {
    const { app } = await buildApp();

    const draft = (
      await app.inject({
        method: 'POST',
        url: '/api/v1/invoices',
        payload: {
          taxpayerId: 'txp_riyadh_retail',
          customerName: 'Majed Al-mutairi',
          lines: [
            {
              itemNameAr: 'خدمة تركيب ميداني',
              itemNameEn: 'On-Site Installation Service',
              sku: 'INST-01',
              quantity: 1,
              unitPriceHalalas: 30000,
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

    const dnRes = await app.inject({
      method: 'POST',
      url: `/api/v1/invoices/${issued.id}/debit-notes`,
      payload: {
        instructionNote: 'Additional cabling materials added during installation',
        lines: [
          {
            itemNameAr: 'تمديد كوابل إضافية',
            itemNameEn: 'Extra Cabling Extension',
            sku: 'CBL-EXT',
            quantity: 1,
            unitPriceHalalas: 10000,
            vatCategory: 'S',
          },
        ],
      },
    });

    expect(dnRes.statusCode).toBe(201);
    const debitNote = dnRes.json();
    expect(debitNote.invoice_type_code).toBe('383');
    expect(debitNote.billing_reference_id).toBe(issued.id);
    expect(debitNote.icv).toBe(2);
    expect(debitNote.previous_invoice_hash).toBe(issued.invoice_hash);
    expect(debitNote.total_amount_halalas).toBe(11500);

    await app.close();
  });
});
