import { FastifyError, FastifyReply, FastifyRequest } from 'fastify';
import { ZodError } from 'zod';

export function registerErrorHandler(
  error: FastifyError | Error,
  _request: FastifyRequest,
  reply: FastifyReply
): void {
  if (error instanceof ZodError) {
    reply.status(400).send({
      error: 'VALIDATION_ERROR',
      details: error.errors,
    });
    return;
  }

  const msg = error.message || 'INTERNAL_SERVER_ERROR';

  if (msg === 'TAXPAYER_NOT_FOUND' || msg === 'INVOICE_NOT_FOUND') {
    reply.status(404).send({ error: msg });
    return;
  }

  if (
    msg.startsWith('INVALID_') ||
    msg === 'EMPTY_LINE_ITEMS' ||
    msg === 'INVOICE_IMMUTABLE_AFTER_ISSUE' ||
    msg === 'CANNOT_DELETE_ISSUED_INVOICE' ||
    msg === 'CANNOT_CREDIT_DRAFT_INVOICE' ||
    msg === 'CANNOT_DEBIT_DRAFT_INVOICE' ||
    msg === 'CREDIT_NOTE_EXCEEDS_ORIGINAL_TOTAL' ||
    msg === 'CANNOT_REPORT_DRAFT_INVOICE' ||
    msg === 'CANNOT_CLEAR_SIMPLIFIED_INVOICE' ||
    msg === 'TAXPAYER_CSID_REVOKED'
  ) {
    reply.status(400).send({ error: msg });
    return;
  }

  reply.status(500).send({
    error: 'INTERNAL_SERVER_ERROR',
    message: msg,
  });
}
