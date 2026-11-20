/**
 * Merchant settlement batches across services: what each one recorded for a batch (trace) and
 * whether they agree (eventual consensus, once the platform settled).
 */
import crypto from 'node:crypto';
import { query as walletQuery } from '../services/wallet-ledger/src/db';
import type { MerchantSettlementService } from '../services/wallet-ledger/src/services/MerchantSettlementService';
import type { SettlementFeeService } from '../services/billing-credits/src/integration/SettlementFeeService';
import type { AppContext as ZatcaContext } from '../services/zatca-tax/src/server';
import type { InvoiceWithLines } from '../services/zatca-tax/src/types/zatca';
import type { SettlementStatementRuntime, StatementRead } from '../services/file-cache-engine/src/integration/settlement-statements';
import { CONSUMERS } from '../shared/contracts';
import type { Checkpoint } from '../shared/messaging';
import type { ConsistencyCheck, SettlementConsistencyReport } from './platform';

export interface SettlementTrace {
  batchId: string;
  wallet: Awaited<ReturnType<MerchantSettlementService['traceBatch']>> & {
    /** What the batch claimed, recomputed from order_settlements (never the batch's own counters). */
    entries: { count: number; sumHalalas: number; outsideWindow: number; otherMerchant: number };
    /** Settlement journals that exist for the batch (must be exactly one once posted). */
    journalsForBatch: number;
  };
  billing: Awaited<ReturnType<SettlementFeeService['getBatchFee']>>;
  zatca: { invoices: InvoiceWithLines[]; checkpoints: Checkpoint[] };
  fileCache: {
    statement: StatementRead | undefined;
    documents: Array<{ invoiceId: string; format: 'pdf' | 'xml'; bytes: number; sha256: string; attributes: Record<string, string> }>;
  };
  /** Every consumer checkpoint recorded for this batch, across services. */
  checkpoints: Checkpoint[];
}

export async function traceSettlementBatch(
  batchId: string,
  deps: { settlements: MerchantSettlementService; fees: SettlementFeeService; zatca: ZatcaContext; statements: SettlementStatementRuntime }
): Promise<SettlementTrace> {
  const wallet = await deps.settlements.traceBatch(batchId);
  const entries = await walletQuery<{ n: number; total: string; outside: number; other: number }>(
    `SELECT COUNT(*)::int AS n, COALESCE(SUM(os.settled_net_cents), 0)::text AS total,
            COUNT(*) FILTER (WHERE (b.window_from IS NOT NULL AND os.created_at < b.window_from) OR os.created_at >= b.window_to)::int AS outside,
            COUNT(*) FILTER (WHERE os.merchant_id <> b.merchant_id)::int AS other
     FROM order_settlements os JOIN merchant_settlement_batches b ON b.batch_id = os.settlement_batch_id
     WHERE os.settlement_batch_id = $1`,
    [batchId]
  );
  const journals = await walletQuery<{ n: number }>(
    `SELECT COUNT(DISTINCT transaction_ref)::int AS n FROM ledger_entries WHERE transaction_ref = $1`,
    [wallet.batch?.journalRef ?? `stl_${batchId}`]
  );
  const billing = await deps.fees.getBatchFee(batchId);
  const zatca = await deps.zatca.settlementInvoicingService.getSettlementDocuments(batchId);
  const statement = wallet.batch ? await deps.statements.statements.fetch(wallet.batch.merchantId, batchId) : undefined;
  const documents: SettlementTrace['fileCache']['documents'] = [];
  for (const invoice of zatca.invoices) {
    for (const format of ['pdf', 'xml'] as const) {
      const doc = await deps.statements.worker.getDocument(invoice.taxpayer_id, invoice.id, format);
      if (doc) {
        documents.push({
          invoiceId: invoice.id,
          format,
          bytes: doc.bytes.byteLength,
          sha256: crypto.createHash('sha256').update(doc.bytes).digest('hex'),
          attributes: doc.attributes,
        });
      }
    }
  }
  return {
    batchId,
    wallet: {
      ...wallet,
      entries: {
        count: Number(entries.rows[0].n),
        sumHalalas: Number(entries.rows[0].total),
        outsideWindow: Number(entries.rows[0].outside),
        otherMerchant: Number(entries.rows[0].other),
      },
      journalsForBatch: Number(journals.rows[0].n),
    },
    billing,
    zatca,
    fileCache: { statement, documents },
    checkpoints: [...wallet.checkpoints, ...billing.checkpoints, ...zatca.checkpoints],
  };
}

/**
 * The ledger swept and booked, billing priced, ZATCA documented and the file cache keeps the same
 * settlement — to the halala — and every step was done exactly once.
 */
export async function checkSettlementConsistency(trace: SettlementTrace, zatca: ZatcaContext): Promise<SettlementConsistencyReport> {
  const checks: ConsistencyCheck[] = [];
  const add = (name: string, ok: boolean, detail: string) => checks.push({ name, ok, detail });
  const batch = trace.wallet.batch;
  if (!batch) {
    add('batch exists', false, 'missing');
    return { batchId: trace.batchId, ok: false, status: 'MISSING', checks };
  }

  add('batch completed (posted and documented)', batch.status === 'COMPLETED', batch.status);
  const e = trace.wallet.entries;
  add('claimed entries == batch item count', e.count === batch.itemCount, `${e.count} vs ${batch.itemCount}`);
  add('Σ claimed revenue == batch gross', e.sumHalalas === batch.grossHalalas, `${e.sumHalalas} vs ${batch.grossHalalas}`);
  add('every claimed entry is in the window and of this merchant', e.outsideWindow === 0 && e.otherMerchant === 0, `${e.outsideWindow} outside, ${e.otherMerchant} foreign`);

  // billing
  const quote = trace.billing.quote;
  add('billing priced the batch once', !!quote, quote ? `${quote.fee.ruleId} v${quote.fee.ruleVersion}` : 'missing');
  if (quote) {
    add('priced gross / items == collected', quote.grossHalalas === batch.grossHalalas && quote.itemCount === batch.itemCount, `${quote.grossHalalas}/${quote.itemCount} vs ${batch.grossHalalas}/${batch.itemCount}`);
    add(
      'ledger fee == billing fee (net, VAT, total)',
      batch.feeNetHalalas === quote.fee.netHalalas && batch.feeVatHalalas === quote.fee.vatHalalas && batch.feeTotalHalalas === quote.fee.totalHalalas,
      `${batch.feeNetHalalas}+${batch.feeVatHalalas}=${batch.feeTotalHalalas} vs ${quote.fee.netHalalas}+${quote.fee.vatHalalas}=${quote.fee.totalHalalas}`
    );
    add('payout == gross − fee', batch.payoutHalalas === batch.grossHalalas - quote.fee.totalHalalas && quote.payoutHalalas === batch.payoutHalalas, `${batch.payoutHalalas}`);
  }

  // ledger journal
  const journal = trace.wallet.journal;
  const sum = journal.reduce((s, l) => s + l.signedAmountCents, 0);
  add('exactly one settlement journal', trace.wallet.journalsForBatch === 1, `${trace.wallet.journalsForBatch} journal(s) ${batch.journalRef}`);
  add('settlement journal balanced', journal.length > 0 && sum === 0, `sum ${sum}`);
  const expectedLegs = [batch.grossHalalas, batch.feeTotalHalalas ?? 0, batch.payoutHalalas ?? 0].filter((x) => x > 0).length;
  add('settlement journal has 3 legs (merchant DEBIT gross, platform fee CREDIT, payout CREDIT)', journal.length === expectedLegs && expectedLegs === 3, `${journal.length} legs`);
  const fees = batch.fee ? journal.find((l) => l.walletId === 'wallet_fee_treasury_sar') : undefined;
  const debit = journal.find((l) => l.signedAmountCents < 0);
  add('merchant revenue debited the gross', debit?.signedAmountCents === -batch.grossHalalas, `${debit?.walletId} ${debit?.signedAmountCents}`);
  add('platform fee treasury credited the fee', fees?.signedAmountCents === batch.feeTotalHalalas, `${fees?.signedAmountCents}`);
  const payout = journal.find((l) => l.signedAmountCents > 0 && l.walletId !== 'wallet_fee_treasury_sar');
  add('merchant payout credited gross − fee', payout?.signedAmountCents === batch.payoutHalalas, `${payout?.walletId} ${payout?.signedAmountCents}`);

  // ZATCA
  const fee = trace.zatca.invoices.filter((i) => i.invoice_type_code === '388');
  const notes = trace.zatca.invoices.filter((i) => i.invoice_type_code === '381');
  add('one fee invoice and one payout credit note', fee.length === 1 && notes.length === 1, `${fee.length} invoice(s), ${notes.length} credit note(s)`);
  const accepted = (status: string) => ['REPORTED', 'REPORTED_WITH_WARNINGS', 'CLEARED'].includes(status);
  if (fee[0]) {
    const inv = fee[0];
    add('fee invoice accepted by Fatoora', accepted(inv.status), inv.status);
    add(
      'fee invoice == ledger fee (taxable = platform net revenue, VAT, total)',
      inv.taxable_amount_halalas === batch.feeNetHalalas && inv.vat_amount_halalas === batch.feeVatHalalas && inv.total_amount_halalas === batch.feeTotalHalalas,
      `${inv.taxable_amount_halalas}+${inv.vat_amount_halalas}=${inv.total_amount_halalas}`
    );
  }
  if (notes[0] && fee[0]) {
    const note = notes[0];
    add('credit note accepted by Fatoora', accepted(note.status), note.status);
    add('credit note == payout, no VAT', note.total_amount_halalas === batch.payoutHalalas && note.vat_amount_halalas === 0, `${note.total_amount_halalas} VAT ${note.vat_amount_halalas}`);
    add('credit note references the fee invoice', note.billing_reference_id === fee[0].id, `${note.billing_reference_id}`);
    add('credit note follows the fee invoice in the chain', note.icv === (fee[0].icv ?? 0) + 1 && note.previous_invoice_hash === fee[0].invoice_hash, `ICV ${fee[0].icv} → ${note.icv}`);
  }
  const taxpayerId = fee[0]?.taxpayer_id;
  if (taxpayerId) {
    const chain = await zatca.invoiceService.verifyHashChain(taxpayerId);
    add('platform ICV / PIH hash chain intact', chain.ok, chain.ok ? `${chain.length} documents` : chain.breaks.join('; '));
  }

  // file cache
  const statement = trace.fileCache.statement?.statement;
  add('statement cached and FINAL', statement?.status === 'FINAL', statement?.status ?? 'missing');
  if (statement) {
    add(
      'statement amounts == ledger',
      statement.amounts.grossHalalas === batch.grossHalalas &&
        statement.amounts.feeTotalHalalas === batch.feeTotalHalalas &&
        statement.amounts.feeNetHalalas === batch.feeNetHalalas &&
        statement.amounts.feeVatHalalas === batch.feeVatHalalas &&
        statement.amounts.netPayoutHalalas === batch.payoutHalalas &&
        statement.itemCount === batch.itemCount,
      `gross ${statement.amounts.gross}, fee ${statement.amounts.feeTotal}, payout ${statement.amounts.netPayout}`
    );
    add('statement journal transaction id == ledger', statement.journal.transactionId === batch.journalRef, statement.journal.transactionId);
    const listed = new Set((statement.tax?.documents ?? []).map((d) => `${d.invoiceId}:${d.invoiceHash}`));
    const issued = trace.zatca.invoices.map((i) => `${i.id}:${i.invoice_hash}`);
    add('statement lists the reported documents', issued.length > 0 && issued.every((k) => listed.has(k)) && listed.size === issued.length, `${listed.size} listed`);
  }
  for (const invoice of trace.zatca.invoices) {
    const pdf = trace.fileCache.documents.find((d) => d.invoiceId === invoice.id && d.format === 'pdf');
    const xml = trace.fileCache.documents.find((d) => d.invoiceId === invoice.id && d.format === 'xml');
    add(
      `${invoice.invoice_type_code} ${invoice.invoice_number}: PDF + XML cached for this hash`,
      !!pdf && !!xml && pdf.attributes['x-invoice-hash'] === invoice.invoice_hash && xml.attributes['x-content-sha256'] === xml.sha256 && pdf.attributes['x-content-sha256'] === pdf.sha256,
      `${pdf?.bytes ?? 0} / ${xml?.bytes ?? 0} bytes`
    );
  }

  const step = (consumer: string, name: string) => trace.checkpoints.some((c) => c.consumer === consumer && c.step === name);
  add(
    'every consumer checkpointed the batch',
    step(CONSUMERS.billingSettlementFees, 'done') &&
      step(CONSUMERS.ledgerSettlementFees, 'done') &&
      step(CONSUMERS.zatcaMerchantSettlements, 'issued') &&
      step(CONSUMERS.zatcaMerchantSettlements, 'done') &&
      step(CONSUMERS.ledgerSettlementDocuments, 'done'),
    trace.checkpoints.map((c) => `${c.consumer}:${c.step}`).join(', ')
  );

  const ok = checks.every((c) => c.ok);
  return { batchId: trace.batchId, ok, status: batch.status, checks };
}
