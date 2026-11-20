import { describe, it, expect, beforeEach } from 'vitest';
import { buildApp } from '../src/server';
import { resetDatabaseAndSeed } from '../src/db';

describe('ZATCA Fatoora Portal Reporting & Compliance Dashboard', () => {
  beforeEach(async () => {
    await resetDatabaseAndSeed();
  });

  it('reports an issued B2C invoice to ZATCA (200 PASS) and records submission log', async () => {
    const { app } = await buildApp();

    const draft = (
      await app.inject({
        method: 'POST',
        url: '/api/v1/invoices',
        payload: {
          taxpayerId: 'txp_riyadh_retail',
          customerName: 'Noura Al-Dosari',
          lines: [
            {
              itemNameAr: 'شاحن سريع 65 واط',
              itemNameEn: '65W USB-C Fast Charger',
              sku: 'CHG-65W',
              quantity: 1,
              unitPriceHalalas: 12000,
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

    const reportRes = await app.inject({
      method: 'POST',
      url: `/api/v1/invoices/${draft.id}/report`,
    });

    expect(reportRes.statusCode).toBe(200);
    const reported = reportRes.json();
    expect(reported.status).toBe('REPORTED');
    expect(reported.zatca_reporting_status).toBe('REPORTED');
    expect(reported.zatca_reported_at).toBeTruthy();

    const logsRes = await app.inject({
      method: 'GET',
      url: `/api/v1/invoices/${draft.id}/submissions`,
    });
    expect(logsRes.statusCode).toBe(200);
    expect(logsRes.json().submissions).toHaveLength(1);
    expect(logsRes.json().submissions[0].outcome).toBe('REPORTED');

    await app.close();
  });

  it('handles 202 REPORTED_WITH_WARNINGS responses from Fatoora and stores warning messages', async () => {
    const { app, reportingService } = await buildApp();

    reportingService.setTransport(async () => ({
      httpStatus: 202,
      reportingStatus: 'REPORTED',
      validationResults: {
        status: 'WARNING',
        warningMessages: [
          {
            code: 'BR-KSA-EN16931-08',
            message: 'Buyer postal address building number is recommended',
          },
        ],
        errorMessages: [],
      },
    }));

    const draft = (
      await app.inject({
        method: 'POST',
        url: '/api/v1/invoices',
        payload: {
          taxpayerId: 'txp_riyadh_retail',
          customerName: 'Badr Al-Anzi',
          lines: [
            {
              itemNameAr: 'حامل شاشة',
              itemNameEn: 'Dual Monitor Arm',
              sku: 'ARM-02',
              quantity: 1,
              unitPriceHalalas: 25000,
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

    const reportRes = await app.inject({
      method: 'POST',
      url: `/api/v1/invoices/${draft.id}/report`,
    });

    expect(reportRes.statusCode).toBe(200);
    const reported = reportRes.json();
    expect(reported.status).toBe('REPORTED_WITH_WARNINGS');
    expect(reported.zatca_warnings).toHaveLength(1);
    expect(reported.zatca_warnings[0].code).toBe('BR-KSA-EN16931-08');

    await app.close();
  });

  it('marks invoice REJECTED when Fatoora returns 400 validation errors', async () => {
    const { app, reportingService } = await buildApp();

    reportingService.setTransport(async () => ({
      httpStatus: 400,
      reportingStatus: 'NOT_REPORTED',
      validationResults: {
        status: 'ERROR',
        warningMessages: [],
        errorMessages: [
          {
            code: 'BR-KSA-F-04',
            message: 'Cryptographic stamp CSID signature does not match taxpayer VAT number',
          },
        ],
      },
    }));

    const draft = (
      await app.inject({
        method: 'POST',
        url: '/api/v1/invoices',
        payload: {
          taxpayerId: 'txp_riyadh_retail',
          customerName: 'Huda Al-Maliki',
          lines: [
            {
              itemNameAr: 'كابل شحن',
              itemNameEn: 'Braided USB-C Cable',
              sku: 'CBL-1M',
              quantity: 1,
              unitPriceHalalas: 4000,
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

    const reportRes = await app.inject({
      method: 'POST',
      url: `/api/v1/invoices/${draft.id}/report`,
    });

    expect(reportRes.statusCode).toBe(200);
    const rejected = reportRes.json();
    expect(rejected.status).toBe('REJECTED');
    expect(rejected.zatca_errors).toHaveLength(1);
    expect(rejected.zatca_errors[0].code).toBe('BR-KSA-F-04');

    await app.close();
  });

  it('rejects reporting an un-issued DRAFT invoice', async () => {
    const { app } = await buildApp();

    const draft = (
      await app.inject({
        method: 'POST',
        url: '/api/v1/invoices',
        payload: {
          taxpayerId: 'txp_riyadh_retail',
          customerName: 'Unissued Customer',
          lines: [
            {
              itemNameAr: 'منتج تجريبي',
              itemNameEn: 'Draft Product',
              sku: 'DRF-01',
              quantity: 1,
              unitPriceHalalas: 5000,
              vatCategory: 'S',
            },
          ],
        },
      })
    ).json();

    const reportRes = await app.inject({
      method: 'POST',
      url: `/api/v1/invoices/${draft.id}/report`,
    });

    expect(reportRes.statusCode).toBe(400);
    expect(reportRes.json().error).toBe('CANNOT_REPORT_DRAFT_INVOICE');

    await app.close();
  });

  it('aggregates taxpayer compliance summary across issued invoices, credit notes, and debit notes', async () => {
    const { app } = await buildApp();

    const draft = (
      await app.inject({
        method: 'POST',
        url: '/api/v1/invoices',
        payload: {
          taxpayerId: 'txp_riyadh_retail',
          customerName: 'Nawaf Al-Harthy',
          lines: [
            {
              itemNameAr: 'طابعة ليزر',
              itemNameEn: 'Laser Printer',
              sku: 'PRN-100',
              quantity: 2,
              unitPriceHalalas: 50000, // 1,000.00 SAR net -> 150.00 SAR VAT -> 1,150.00 SAR total
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

    await app.inject({
      method: 'POST',
      url: `/api/v1/invoices/${issued.id}/report`,
    });

    await app.inject({
      method: 'POST',
      url: `/api/v1/invoices/${issued.id}/credit-notes`,
      payload: {
        instructionNote: 'Returned 1 printer',
        lines: [
          {
            itemNameAr: 'طابعة ليزر',
            itemNameEn: 'Laser Printer',
            sku: 'PRN-100',
            quantity: 1,
            unitPriceHalalas: 50000,
            vatCategory: 'S',
          },
        ],
      },
    });

    const summaryRes = await app.inject({
      method: 'GET',
      url: '/api/v1/taxpayers/txp_riyadh_retail/compliance-summary',
    });

    expect(summaryRes.statusCode).toBe(200);
    const summary = summaryRes.json();
    expect(summary.lastIcv).toBe(2);
    expect(summary.countsByStatus.REPORTED).toBe(1);
    expect(summary.countsByStatus.ISSUED).toBe(1);
    expect(summary.totalsHalalas.issuedSalesHalalas).toBe(100000);
    expect(summary.totalsHalalas.issuedVatHalalas).toBe(15000);
    expect(summary.totalsHalalas.creditNotesTotalHalalas).toBe(57500);

    await app.close();
  });
});
