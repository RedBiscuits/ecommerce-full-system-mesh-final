/**
 * Read API of the settlement files kept by the file cache (framework-free `node:http` handler):
 *
 *   GET /settlements/:merchantId/:batchId          the statement JSON (status PENDING_TAX_REPORTING
 *                                                  until ZATCA accepted the documents, then FINAL)
 *   GET /documents/:taxpayerId/:invoiceId.pdf      a settlement document's PDF
 *   GET /documents/:taxpayerId/:invoiceId.xml      a settlement document's UBL XML
 *   GET /health
 *
 * Statements are fetched through the store's hotness tracker (single flight + hot pinning), so a
 * burst of concurrent requests for one statement costs one cache read. Responses carry a strong
 * ETag (SHA-256 of the bytes) and honour If-None-Match (304).
 */
import type { IncomingMessage, RequestListener, ServerResponse } from 'node:http';

import type { SettlementStatementStore, SettlementStatementWorker } from './settlement-statements';

function send(res: ServerResponse, status: number, body: string | Buffer, headers: Record<string, string> = {}): void {
  const bytes = typeof body === 'string' ? Buffer.from(body, 'utf8') : body;
  res.writeHead(status, { 'content-length': String(bytes.byteLength), ...headers });
  res.end(bytes);
}

function json(res: ServerResponse, status: number, body: unknown): void {
  send(res, status, JSON.stringify(body), { 'content-type': 'application/json' });
}

function notModified(req: IncomingMessage, etag: string): boolean {
  const header = req.headers['if-none-match'];
  if (typeof header !== 'string') return false;
  return header
    .split(',')
    .map((t) => t.trim())
    .some((t) => t === etag || t === '*');
}

export function createSettlementHttpHandler(deps: {
  statements: SettlementStatementStore;
  worker: SettlementStatementWorker;
}): RequestListener {
  return (req, res) => {
    void (async () => {
      const url = new URL(req.url ?? '/', 'http://file-cache.local');
      if (req.method !== 'GET' && req.method !== 'HEAD') return json(res, 405, { error: 'Method not allowed' });
      const parts = url.pathname.split('/').filter(Boolean).map(decodeURIComponent);

      if (parts.length === 1 && parts[0] === 'health') return json(res, 200, { status: 'ok', statements: deps.statements.stats() });

      if (parts.length === 3 && parts[0] === 'settlements') {
        const batchId = parts[2].replace(/\.json$/, '');
        const read = await deps.statements.fetch(parts[1], batchId);
        if (read === undefined) return json(res, 404, { error: 'Statement not found (never written, or older than 72 hours)', code: 'STATEMENT_NOT_FOUND' });
        const etag = `"${read.etag}"`;
        const headers = {
          etag,
          'cache-control': 'private, no-cache',
          expires: new Date(read.expiresAt).toUTCString(),
          'x-settlement-status': read.statement.status,
        };
        if (notModified(req, etag)) {
          res.writeHead(304, headers);
          return res.end();
        }
        return send(res, 200, req.method === 'HEAD' ? Buffer.alloc(0) : read.bytes, { ...headers, 'content-type': 'application/json' });
      }

      if (parts.length === 3 && parts[0] === 'documents') {
        const match = /^(.+)\.(pdf|xml)$/.exec(parts[2]);
        if (!match) return json(res, 404, { error: 'Unknown document format' });
        const doc = await deps.worker.getDocument(parts[1], match[1], match[2] as 'pdf' | 'xml');
        if (doc === undefined) return json(res, 404, { error: 'Document not found', code: 'DOCUMENT_NOT_FOUND' });
        const etag = `"${doc.attributes['x-content-sha256'] ?? ''}"`;
        if (notModified(req, etag)) {
          res.writeHead(304, { etag });
          return res.end();
        }
        return send(res, 200, doc.bytes, {
          etag,
          'content-type': doc.attributes['content-type'] ?? 'application/octet-stream',
          'x-invoice-number': doc.attributes['x-invoice-number'] ?? '',
          'x-icv': doc.attributes['x-icv'] ?? '',
        });
      }

      return json(res, 404, { error: 'Not found' });
    })().catch((err: unknown) => {
      if (!res.headersSent) json(res, 500, { error: err instanceof Error ? err.message : String(err) });
      else res.destroy();
    });
  };
}
