/**
 * Customer refunds across services: what each service recorded for an order's refunds, and the
 * invariants they must satisfy together (eventual consensus, once the platform settled).
 */
import { db as hyperpayDb, type DbRecord } from '../services/hyperpay-gateway/src/services/jsonDb';
import { REFUNDS_COLLECTION } from '../services/hyperpay-gateway/src/services/collections';
import type { RefundRecord } from '../services/hyperpay-gateway/src/services/refundWebhook';
import { orderCreditRefundService } from '../services/billing-credits/src/integration/OrderCreditRefundService';
import { orderRefundService } from '../services/wallet-ledger/src/services/OrderRefundService';
import { query as walletQuery } from '../services/wallet-ledger/src/db';
import { query as billingQuery } from '../services/billing-credits/src/db';
import type { AppContext as ZatcaContext } from '../services/zatca-tax/src/server';
import type { InvoiceWithLines } from '../services/zatca-tax/src/types/zatca';
import type { CreditNoteWorkerRuntime } from '../services/file-cache-engine/src/integration/credit-note-worker';
import type { InvoicePdfWorkerRuntime } from '../services/file-cache-engine/src/integration/invoice-pdf-worker';
import { invoiceXmlKey } from '../services/file-cache-engine/src/integration/settlement-statements';
import type { ConsistencyCheck } from './platform';

export interface OrderRefundTrace {
  hyperpay: Array<RefundRecord & DbRecord>;
  billing: Awaited<ReturnType<typeof orderCreditRefundService.getOrderRefunds>>;
  wallet: Awaited<ReturnType<typeof orderRefundService.getOrderRefunds>>;
  zatca: { creditNotes: InvoiceWithLines[] };
  fileCache: {
    /** Attributes of the (re-rendered) invoice PDF, if cached. */
    invoicePdf: Record<string, string> | null;
    creditNotes: Array<{ invoiceId: string; pdf: boolean; xml: boolean; pdfHash: string | null }>;
  };
}

export async function traceOrderRefunds(
  orderId: string,
  invoices: InvoiceWithLines[],
  deps: { zatca: ZatcaContext; pdfs: InvoicePdfWorkerRuntime; creditNotes: CreditNoteWorkerRuntime }
): Promise<OrderRefundTrace> {
  const hyperpay = await hyperpayDb.findWhere<RefundRecord & DbRecord>(REFUNDS_COLLECTION, (r) => r.orderId === orderId);
  const billing = await orderCreditRefundService.getOrderRefunds(orderId);
  const wallet = await orderRefundService.getOrderRefunds(orderId);
  const zatca = await deps.zatca.refundInvoicingService.getOrderCreditNotes(orderId);
  const invoice = invoices.find((i) => i.invoice_type_code === '388');
  const invoicePdf = invoice ? await deps.pdfs.worker.get(invoice.taxpayer_id, invoice.id) : undefined;
  const creditNotes: OrderRefundTrace['fileCache']['creditNotes'] = [];
  for (const note of zatca.creditNotes) {
    const pdf = await deps.pdfs.worker.get(note.taxpayer_id, note.id);
    const xml = await deps.pdfs.worker['cache'].get(invoiceXmlKey(note.taxpayer_id, note.id));
    creditNotes.push({ invoiceId: note.id, pdf: !!pdf, xml: !!xml, pdfHash: pdf?.metadata.attributes['x-invoice-hash'] ?? null });
  }
  return {
    hyperpay,
    billing,
    wallet,
    zatca: { creditNotes: zatca.creditNotes },
    fileCache: { invoicePdf: invoicePdf ? { ...invoicePdf.metadata.attributes } : null, creditNotes },
  };
}

/**
 * Refund invariants of one order (appended to the order's consistency report when it has refunds):
 * the gateway, billing, the ledger, ZATCA and the file cache describe the same refunds, the total
 * never exceeds what was captured, every reversal is balanced, every refund has exactly one credit
 * note of the same amount, the credits restored are the pro-rata share, and the cached invoice
 * PDF reflects the credits.
 */
export function checkRefundConsistency(
  trace: OrderRefundTrace,
  capture: { grossHalalas: number; netRevenueHalalas: number; vatHalalas: number; capturedCredits: number } | null,
  journalOf: (ref: string) => Promise<Array<{ walletId: string; signedAmountCents: number }>>
): Promise<ConsistencyCheck[]> {
  return (async () => {
    const checks: ConsistencyCheck[] = [];
    const add = (name: string, ok: boolean, detail: string) => checks.push({ name, ok, detail });
    const successful = trace.hyperpay.filter((r) => r.status === 'SUCCESS');
    const refundedAtGateway = successful.reduce((s, r) => s + r.amountHalalas, 0);
    const gross = capture?.grossHalalas ?? 0;

    add('refunds never exceed the captured amount', refundedAtGateway <= gross, `${refundedAtGateway} refunded of ${gross} captured`);
    add(
      'billing, the ledger and ZATCA saw every successful refund, once',
      successful.length === trace.billing.refunds.length &&
        successful.length === trace.wallet.refunds.length &&
        successful.length === trace.zatca.creditNotes.length &&
        new Set(trace.wallet.refunds.map((r) => r.refundId)).size === trace.wallet.refunds.length,
      `${successful.length} gateway / ${trace.billing.refunds.length} billing / ${trace.wallet.refunds.length} ledger / ${trace.zatca.creditNotes.length} credit notes`
    );
    const ledgerTotal = trace.wallet.refunds.reduce((s, r) => s + r.amountHalalas, 0);
    add(
      'ledger refunded total == gateway refunds == running total on the settlement',
      ledgerTotal === refundedAtGateway && (trace.wallet.totals?.refundedGrossHalalas ?? 0) === ledgerTotal,
      `${ledgerTotal} vs ${refundedAtGateway} vs ${trace.wallet.totals?.refundedGrossHalalas}`
    );
    if (trace.wallet.totals) {
      const t = trace.wallet.totals;
      add('refunded net + VAT == refunded gross (never beyond the capture)', t.refundedNetHalalas + t.refundedVatHalalas === t.refundedGrossHalalas && t.refundedNetHalalas <= (capture?.netRevenueHalalas ?? 0) && t.refundedVatHalalas <= (capture?.vatHalalas ?? 0), `${t.refundedNetHalalas} + ${t.refundedVatHalalas} = ${t.refundedGrossHalalas}`);
      if (capture && t.refundedGrossHalalas === capture.grossHalalas) {
        add('a full refund reversed the revenue and the VAT exactly', t.refundedNetHalalas === capture.netRevenueHalalas && t.refundedVatHalalas === capture.vatHalalas, `${t.refundedNetHalalas}/${capture.netRevenueHalalas} net, ${t.refundedVatHalalas}/${capture.vatHalalas} VAT`);
      }
    }
    for (const refund of trace.wallet.refunds) {
      const journal = await journalOf(refund.journalRef);
      const sum = journal.reduce((s, l) => s + l.signedAmountCents, 0);
      const credited = journal.find((l) => l.walletId === refund.refundedToWalletId)?.signedAmountCents;
      add(`reversal ${refund.journalRef} balanced, 3 legs, customer side credited the refund`, journal.length > 0 && sum === 0 && credited === refund.amountHalalas && journal.length === [refund.netHalalas, refund.vatHalalas, refund.amountHalalas].filter((x) => x > 0).length, `sum ${sum}, ${journal.length} legs, credited ${credited}`);
      const note = trace.zatca.creditNotes.find((n) => n.source_refund_id === refund.refundId);
      add(
        `credit note for refund ${refund.refundId} == reversal (net, VAT, total), accepted by Fatoora`,
        !!note && note.taxable_amount_halalas === refund.netHalalas && note.vat_amount_halalas === refund.vatHalalas && note.total_amount_halalas === refund.amountHalalas && ['REPORTED', 'REPORTED_WITH_WARNINGS', 'CLEARED'].includes(note.status),
        note ? `${note.invoice_number} ${note.taxable_amount_halalas}+${note.vat_amount_halalas}=${note.total_amount_halalas} ${note.status}` : 'missing'
      );
      const cachedNote = trace.fileCache.creditNotes.find((c) => c.invoiceId === note?.id);
      add(`credit note ${note?.invoice_number ?? refund.refundId} PDF + XML cached`, !!cachedNote && cachedNote.pdf && cachedNote.xml && cachedNote.pdfHash === note?.invoice_hash, cachedNote ? `pdf ${cachedNote.pdf} xml ${cachedNote.xml}` : 'missing');
    }
    const restored = trace.billing.refunds.reduce((s, r) => s + r.restoredCredits, 0);
    const capturedCredits = capture?.capturedCredits ?? 0;
    const expectedRestored = gross > 0 ? Number((BigInt(capturedCredits) * BigInt(refundedAtGateway)) / BigInt(gross)) : 0;
    add('credits restored == pro-rata share of the captured credits (never more)', restored === expectedRestored && restored <= capturedCredits, `${restored} of ${capturedCredits} (expected ${expectedRestored})`);
    const cachedCredited = trace.fileCache.invoicePdf?.['x-credited-halalas'];
    add('cached invoice PDF reflects the credit notes', trace.zatca.creditNotes.length === 0 || cachedCredited === String(trace.zatca.creditNotes.reduce((s, n) => s + n.total_amount_halalas, 0)), `x-credited-halalas=${cachedCredited ?? 'none'}`);
    const creditNotesTotal = trace.zatca.creditNotes.reduce((s, n) => s + n.total_amount_halalas, 0);
    add('Σ credit notes == refunded total', creditNotesTotal === refundedAtGateway, `${creditNotesTotal} vs ${refundedAtGateway}`);
    const chain = trace.zatca.creditNotes.map((n) => n.icv);
    add('credit notes are in the merchant\'s chain (ICVs assigned)', chain.every((icv) => typeof icv === 'number' && icv > 0), chain.join(', ') || 'none');
    return checks;
  })();
}

/** Platform-wide refund invariants for the doctor (no order in particular). */
export async function refundInvariants(zatca: ZatcaContext): Promise<{ refunds: number; orders: number; violations: string[]; awaitingCreditNotes: number }> {
  const violations: string[] = [];
  const ledger = await walletQuery<{ n: number; orders: number }>(`SELECT COUNT(*)::int AS n, COUNT(DISTINCT order_id)::int AS orders FROM order_refunds`);
  const overRefunded = await walletQuery<{ order_id: string }>(
    `SELECT os.order_id FROM order_settlements os
     WHERE os.refunded_gross_cents > os.gross_cents
        OR os.refunded_gross_cents <> COALESCE((SELECT SUM(amount_cents) FROM order_refunds r WHERE r.order_id = os.order_id), 0)`
  );
  for (const row of overRefunded.rows) violations.push(`ledger: order ${row.order_id} refunded total disagrees with its refunds (or exceeds the capture)`);
  const unbalanced = await walletQuery<{ journal_ref: string; total: string }>(
    `SELECT r.journal_ref, SUM(le.signed_amount_cents)::text AS total FROM order_refunds r
     JOIN ledger_entries le ON le.transaction_ref = r.journal_ref GROUP BY r.journal_ref HAVING SUM(le.signed_amount_cents) <> 0`
  );
  for (const row of unbalanced.rows) violations.push(`ledger: reversal ${row.journal_ref} is unbalanced by ${row.total}`);
  const orphanJournals = await walletQuery<{ transaction_ref: string }>(
    `SELECT DISTINCT transaction_ref FROM ledger_entries WHERE transaction_ref LIKE 'rfd_%'
       AND transaction_ref NOT IN (SELECT journal_ref FROM order_refunds)`
  );
  for (const row of orphanJournals.rows) violations.push(`ledger: reversal journal ${row.transaction_ref} has no refund record`);
  const billing = await billingQuery<{ order_id: string }>(
    `SELECT order_id FROM order_credit_refunds GROUP BY order_id, captured_credits HAVING SUM(restored_credits) > captured_credits`
  );
  for (const row of billing.rows) violations.push(`billing: order ${row.order_id} restored more credits than it captured`);

  // ZATCA: every ledger refund has at most one credit note, Σ credit notes ≤ invoice total.
  const ledgerRefunds = await walletQuery<{ refund_id: string; amount_cents: string }>(`SELECT refund_id, amount_cents::text FROM order_refunds`);
  const db = await (await import('../services/zatca-tax/src/db')).getDb();
  const notes = await db.query<{ source_refund_id: string; total: string; n: number }>(
    `SELECT source_refund_id, SUM(total_amount_halalas)::text AS total, COUNT(*)::int AS n FROM invoices
     WHERE invoice_type_code = '381' AND source_refund_id IS NOT NULL AND status <> 'REJECTED' GROUP BY source_refund_id`
  );
  const byRefund = new Map(notes.rows.map((n) => [n.source_refund_id, n]));
  let awaiting = 0;
  for (const r of ledgerRefunds.rows) {
    const note = byRefund.get(r.refund_id);
    if (!note) awaiting += 1;
    else if (Number(note.n) !== 1 || note.total !== r.amount_cents) violations.push(`zatca: refund ${r.refund_id} has ${note.n} credit note(s) totalling ${note.total}, the ledger reversed ${r.amount_cents}`);
  }
  const over = await db.query<{ invoice_number: string; credited: string; total: string }>(
    `SELECT i.invoice_number, SUM(n.total_amount_halalas)::text AS credited, i.total_amount_halalas::text AS total
     FROM invoices n JOIN invoices i ON i.id = n.billing_reference_id
     WHERE n.invoice_type_code = '381' AND n.source_refund_id IS NOT NULL AND n.status <> 'REJECTED'
     GROUP BY i.id, i.invoice_number, i.total_amount_halalas HAVING SUM(n.total_amount_halalas) > i.total_amount_halalas`
  );
  for (const row of over.rows) violations.push(`zatca: credit notes against ${row.invoice_number} total ${row.credited}, more than its ${row.total}`);
  void zatca;
  return { refunds: Number(ledger.rows[0].n), orders: Number(ledger.rows[0].orders), violations, awaitingCreditNotes: awaiting };
}
