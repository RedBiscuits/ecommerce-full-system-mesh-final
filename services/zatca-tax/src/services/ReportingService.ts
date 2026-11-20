import type { PGlite } from '@electric-sql/pglite';
import { v4 as uuidv4 } from 'uuid';
import { InvoiceService, ZatcaExecutor } from './InvoiceService';
import { InvoiceWithLines, SubmissionLogRecord } from '../types/zatca';

export interface ZatcaGatewayResponse {
  httpStatus: number;
  reportingStatus: 'REPORTED' | 'NOT_REPORTED';
  /** Clearance API (standard B2B invoices): whether ZATCA cleared the document. */
  clearanceStatus?: 'CLEARED' | 'NOT_CLEARED';
  validationResults: {
    status: 'PASS' | 'WARNING' | 'ERROR';
    warningMessages: Array<{ code: string; message: string }>;
    errorMessages: Array<{ code: string; message: string }>;
  };
}

/**
 * Which Fatoora API a submission goes to: REPORTING (simplified B2C documents, reported after
 * issuance) or CLEARANCE (standard B2B documents, cleared by ZATCA before they are shared).
 */
export type SubmissionMode = 'REPORTING' | 'CLEARANCE';

export type ZatcaPortalTransport = (
  payload: {
    invoiceHash: string;
    uuid: string;
    invoice: string;
  },
  context?: { mode: SubmissionMode }
) => Promise<ZatcaGatewayResponse>;

export type ReportingOutcome = 'REPORTED' | 'REPORTED_WITH_WARNINGS' | 'CLEARED' | 'REJECTED' | 'TRANSIENT_ERROR';

export interface ReportOptions {
  /**
   * Runs in the SAME transaction that records the submission and the invoice's new status, so a
   * caller can commit its own follow-up (outbox event, checkpoint) atomically with the outcome.
   */
  onRecorded?: (tx: ZatcaExecutor, outcome: ReportingOutcome, invoiceId: string) => Promise<void>;
}

export class ReportingService {
  private transport: ZatcaPortalTransport;

  constructor(
    private readonly db: PGlite,
    private readonly invoiceService: InvoiceService,
    customTransport?: ZatcaPortalTransport
  ) {
    this.transport = customTransport ?? ReportingService.defaultPortalTransport;
  }

  setTransport(transport: ZatcaPortalTransport): void {
    this.transport = transport;
  }

  static async defaultPortalTransport(
    payload: {
      invoiceHash: string;
      uuid: string;
      invoice: string;
    },
    context?: { mode: SubmissionMode }
  ): Promise<ZatcaGatewayResponse> {
    if (!payload.invoiceHash || !payload.uuid || !payload.invoice) {
      return {
        httpStatus: 400,
        reportingStatus: 'NOT_REPORTED',
        validationResults: {
          status: 'ERROR',
          warningMessages: [],
          errorMessages: [{ code: 'BR-KSA-01', message: 'Missing UUID, InvoiceHash, or UBL payload' }],
        },
      };
    }

    return {
      httpStatus: 200,
      reportingStatus: 'REPORTED',
      ...(context?.mode === 'CLEARANCE' ? { clearanceStatus: 'CLEARED' as const } : {}),
      validationResults: {
        status: 'PASS',
        warningMessages: [],
        errorMessages: [],
      },
    };
  }

  async reportSimplifiedInvoice(invoiceId: string, options: ReportOptions = {}): Promise<InvoiceWithLines> {
    return this.submitInvoice(invoiceId, { ...options, mode: 'REPORTING' });
  }

  /** Standard (B2B, subtype 0100000) documents are cleared by ZATCA (status CLEARED). */
  async clearStandardInvoice(invoiceId: string, options: ReportOptions = {}): Promise<InvoiceWithLines> {
    return this.submitInvoice(invoiceId, { ...options, mode: 'CLEARANCE' });
  }

  /**
   * Submit an issued document to Fatoora: reporting (→ REPORTED / REPORTED_WITH_WARNINGS) or
   * clearance (→ CLEARED). 4xx → REJECTED; 5xx → TRANSIENT_ERROR (the document stays ISSUED with
   * its ICV and is submitted again later). The submission log, the new status and the caller's
   * `onRecorded` follow-up commit in ONE transaction.
   */
  async submitInvoice(invoiceId: string, options: ReportOptions & { mode: SubmissionMode }): Promise<InvoiceWithLines> {
    const invoice = await this.invoiceService.getInvoice(invoiceId);
    if (invoice.status === 'DRAFT') {
      throw new Error('CANNOT_REPORT_DRAFT_INVOICE');
    }
    if (options.mode === 'CLEARANCE' && invoice.invoice_subtype !== '0100000') {
      throw new Error('CANNOT_CLEAR_SIMPLIFIED_INVOICE');
    }

    const requestPayload = {
      invoiceHash: invoice.invoice_hash!,
      uuid: invoice.uuid,
      invoice: invoice.xml_ubl_base64 || Buffer.from(JSON.stringify(invoice)).toString('base64'),
    };

    const countRes = await this.db.query<{ cnt: string }>(
      `SELECT COUNT(*)::text AS cnt FROM zatca_submission_logs WHERE invoice_id = $1`,
      [invoiceId]
    );
    const attemptNumber = parseInt(countRes.rows[0]?.cnt || '0', 10) + 1;

    const gatewayRes = await this.transport(requestPayload, { mode: options.mode });

    let outcome: ReportingOutcome;
    let nextStatus: 'REPORTED' | 'REPORTED_WITH_WARNINGS' | 'CLEARED' | 'REJECTED' | null;
    let zatcaStatus: string = gatewayRes.reportingStatus;

    if (options.mode === 'CLEARANCE') {
      if (gatewayRes.httpStatus >= 500) {
        outcome = 'TRANSIENT_ERROR';
        nextStatus = null;
      } else if (gatewayRes.httpStatus >= 400 || gatewayRes.clearanceStatus === 'NOT_CLEARED') {
        outcome = 'REJECTED';
        nextStatus = 'REJECTED';
        zatcaStatus = 'NOT_CLEARED';
      } else {
        // 200 cleared, 202 cleared with warnings (kept in zatca_warnings).
        outcome = 'CLEARED';
        nextStatus = 'CLEARED';
        zatcaStatus = 'CLEARED';
      }
    } else if (gatewayRes.httpStatus === 202 || gatewayRes.validationResults.warningMessages.length > 0) {
      outcome = 'REPORTED_WITH_WARNINGS';
      nextStatus = 'REPORTED_WITH_WARNINGS';
    } else if (gatewayRes.httpStatus >= 400 && gatewayRes.httpStatus < 500) {
      outcome = 'REJECTED';
      nextStatus = 'REJECTED';
    } else if (gatewayRes.httpStatus >= 500) {
      outcome = 'TRANSIENT_ERROR';
      nextStatus = null;
    } else {
      outcome = 'REPORTED';
      nextStatus = 'REPORTED';
    }

    // The submission log, the invoice's new status and the caller's follow-up commit together.
    await this.db.transaction(async (tx) => {
      await tx.query(
        `INSERT INTO zatca_submission_logs (
          id, invoice_id, attempt_number, submission_type, request_payload, http_status, response_body, outcome
        ) VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7::jsonb, $8)`,
        [
          `sub_${uuidv4()}`,
          invoiceId,
          attemptNumber,
          options.mode,
          JSON.stringify(requestPayload),
          gatewayRes.httpStatus,
          JSON.stringify(gatewayRes),
          outcome,
        ]
      );

      if (nextStatus !== null) {
        await tx.query(
          `UPDATE invoices SET
            status = $2,
            zatca_reporting_status = $3,
            zatca_warnings = $4::jsonb,
            zatca_errors = $5::jsonb,
            zatca_reported_at = NOW()
           WHERE id = $1`,
          [
            invoiceId,
            nextStatus,
            zatcaStatus,
            JSON.stringify(gatewayRes.validationResults.warningMessages),
            JSON.stringify(gatewayRes.validationResults.errorMessages),
          ]
        );
      }

      await options.onRecorded?.(tx, outcome, invoiceId);
    });

    return this.invoiceService.getInvoice(invoiceId);
  }

  async listSubmissionLogs(invoiceId: string): Promise<SubmissionLogRecord[]> {
    await this.invoiceService.getInvoice(invoiceId);
    const res = await this.db.query<SubmissionLogRecord>(
      `SELECT * FROM zatca_submission_logs WHERE invoice_id = $1 ORDER BY attempt_number ASC`,
      [invoiceId]
    );
    return res.rows.map((r) => ({
      ...r,
      attempt_number: Number(r.attempt_number),
      http_status: Number(r.http_status),
      created_at: new Date(r.created_at).toISOString(),
    }));
  }
}
