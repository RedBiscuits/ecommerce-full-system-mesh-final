/**
 * One checkout through the whole platform, end to end:
 *
 *   npm run platform:demo
 *
 * 1. the storefront reserves 20 credits of org_acme for the order (billing)
 * 2. HyperPay POSTs a signed payment notification (207.00 SAR = 2 × 100.00 − 20.00 credit + 15 % VAT)
 * 3. the event travels hyperpay → billing → ledger → ZATCA → file cache over the broker
 * 4. the cross-service consistency report and the cached invoice PDF are printed
 * 5. a split-tender checkout through the coordinator: 25 promotional credits + the remainder from
 *    the customer's SAR wallet, on a mixed S (15 %) / Z / E (VATEX-SA-29) basket
 * 6. the merchant settlement batch: the ledger sweeps the merchant's settled revenue since the
 *    beginning, billing prices the tiered processing fee, the ledger posts the 3-leg settlement
 *    journal, ZATCA issues + clears the platform's fee invoice and the payout credit note, and the
 *    file cache keeps the statement JSON (72 h) with the document PDFs
 * 7. a partial customer refund of the first order: the gateway records the RF notification,
 *    billing restores credits pro rata, the ledger posts the reversal, ZATCA issues the credit
 *    note and the file cache refreshes the invoice PDF
 */
import crypto from 'node:crypto';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { bootPlatform } from './platform';
import { SIGNATURE_HEADER, sign } from '../services/hyperpay-gateway/src/security/webhookSignature';

async function main(): Promise<void> {
  const platform = await bootPlatform({ log: () => undefined, httpLogs: false });
  try {
    const orderId = `ord_demo_${crypto.randomUUID().slice(0, 8)}`;
    const hold = await platform.billing.app.inject({
      method: 'POST',
      url: `/credits/orders/${orderId}/hold`,
      payload: { orgId: 'org_acme', credits: 20 },
    });
    console.log(`1. credit hold placed: ${hold.statusCode} ${hold.body}`);

    const paymentId = `8ac7a4a1${crypto.randomBytes(12).toString('hex')}`;
    const body = JSON.stringify({
      id: paymentId,
      paymentType: 'DB',
      paymentBrand: 'MADA',
      amount: '207.00',
      currency: 'SAR',
      merchantTransactionId: orderId,
      result: { code: '000.100.110', description: 'Request successfully processed' },
      customer: { givenName: 'Noura', surname: 'Al-Qahtani', email: 'noura@example.sa' },
      customParameters: { merchantId: 'txp_riyadh_retail', creditOrgId: 'org_acme' },
      cart: { items: [{ merchantItemId: 'RTR-AX6000', name: 'Wi-Fi 6 Router', nameAr: 'موجه واي فاي 6', quantity: '2', price: '100.00' }] },
    });
    const webhook = await platform.hyperpay.app.inject({
      method: 'POST',
      url: '/api/webhook/hyperpay',
      headers: { 'content-type': 'application/json', [SIGNATURE_HEADER]: sign(platform.webhookSecret, body, Math.floor(Date.now() / 1000)) },
      payload: body,
    });
    console.log(`2. signed webhook: ${webhook.statusCode} ${webhook.body}`);

    await platform.settle();
    const trace = await platform.traceOrder(orderId);
    const report = await platform.verifyOrderConsistency(orderId);
    console.log('3. billing   :', JSON.stringify(trace.billing.settlements.map((s) => ({ outcome: s.outcome, capturedCredits: s.capturedCredits, allowanceHalalas: s.allowanceHalalas }))));
    console.log('   ledger    :', JSON.stringify(trace.wallet.journal));
    const invoice = trace.zatca.invoices[0];
    console.log('   zatca     :', JSON.stringify({ number: invoice?.invoice_number, status: invoice?.status, icv: invoice?.icv, pih: invoice?.previous_invoice_hash, allowance: invoice?.document_allowance_halalas, total: invoice?.total_amount_halalas }));
    console.log('   file cache:', JSON.stringify(trace.fileCache));
    console.log('   checkpoints:', trace.checkpoints.map((c) => `${c.consumer}:${c.step}=${c.outcome}`).join(', '));
    console.log(`4. consistency: ${report.ok ? 'OK' : 'NOT OK'} (${report.outcome})`);
    for (const check of report.checks) console.log(`   ${check.ok ? '✔' : '✘'} ${check.name} — ${check.detail}`);

    if (invoice) {
      const pdf = await platform.fileCache.pdfs.worker.get(invoice.taxpayer_id, invoice.id);
      if (pdf) {
        const out = path.join(os.tmpdir(), `${invoice.invoice_number}.pdf`);
        await fs.writeFile(out, pdf.pdf);
        console.log(`   invoice PDF (${pdf.pdf.byteLength} bytes) written to ${out}`);
      }
    }
    if (!report.ok) process.exitCode = 1;

    // ── 5. split tender ───────────────────────────────────────────────────────
    const checkoutRes = await platform.checkout.context.app.inject({
      method: 'POST',
      url: '/checkouts',
      payload: {
        checkoutId: `co_demo_${crypto.randomUUID().slice(0, 8)}`,
        orderId: `ord_split_${crypto.randomUUID().slice(0, 8)}`,
        customer: { name: 'Noura Al-Qahtani', email: 'noura@example.sa' },
        lines: [
          { sku: 'RTR-AX6000', nameEn: 'Wi-Fi 6 Router', nameAr: 'موجه واي فاي 6', quantity: 1, unitPriceHalalas: 10000, vatCategory: 'S' },
          { sku: 'EXP-LIC', nameEn: 'Export licence', nameAr: 'ترخيص تصدير', quantity: 1, unitPriceHalalas: 10000, vatCategory: 'Z', vatExemptionCode: 'VATEX-SA-32' },
          { sku: 'FIN-FEE', nameEn: 'Financing fee', nameAr: 'رسوم تمويل', quantity: 1, unitPriceHalalas: 5000, vatCategory: 'E', vatExemptionCode: 'VATEX-SA-29' },
        ],
        promotion: { orgId: 'org_acme', credits: 25 },
        remainder: { tender: 'WALLET', walletId: 'wallet_noura_sar' },
      },
    });
    const checkout = checkoutRes.json() as { checkoutId: string; orderId: string; status: string; payment: { amountSar: string } };
    console.log(`5. split-tender checkout: ${checkoutRes.statusCode} ${checkout.status}, remainder ${checkout.payment.amountSar} SAR from the wallet`);
    await platform.checkout.context.app.inject({ method: 'POST', url: `/checkouts/${checkout.checkoutId}/confirm` });
    await platform.settle();
    const split = await platform.traceOrder(checkout.orderId);
    const splitReport = await platform.verifyOrderConsistency(checkout.orderId);
    console.log('   ledger    :', JSON.stringify(split.wallet.journal.map((l) => [l.walletId, l.signedAmountCents])));
    const splitInvoice = split.zatca.invoices[0];
    console.log('   zatca     :', JSON.stringify({ allowance: splitInvoice?.document_allowance_halalas, reason: splitInvoice?.document_allowance_reason, vat: splitInvoice?.vat_amount_halalas, total: splitInvoice?.total_amount_halalas }));
    console.log(`   consistency: ${splitReport.ok ? 'OK' : 'NOT OK'} (${splitReport.outcome}), checkout ${split.checkouts[0]?.status}`);
    for (const check of splitReport.checks.filter((c) => !c.ok)) console.log(`   ✘ ${check.name} — ${check.detail}`);
    if (splitInvoice) {
      const splitPdf = await platform.fileCache.pdfs.worker.get(splitInvoice.taxpayer_id, splitInvoice.id);
      if (splitPdf) {
        const out = path.join(os.tmpdir(), `${splitInvoice.invoice_number}.pdf`);
        await fs.writeFile(out, splitPdf.pdf);
        console.log(`   split-tender invoice PDF written to ${out}`);
      }
    }
    if (!splitReport.ok) process.exitCode = 1;

    // ── 6. merchant settlement ────────────────────────────────────────────────
    const batch = await platform.settlements.sweep({ merchantId: 'txp_riyadh_retail' });
    console.log(`6. settlement batch ${batch.batchId}: ${batch.status}, ${batch.itemCount} settled orders, gross ${(batch.grossHalalas / 100).toFixed(2)} SAR (swept in ${batch.chunks} chunk(s))`);
    await platform.settle();
    const settlement = await platform.traceSettlement(batch.batchId);
    const settlementReport = await platform.verifySettlementConsistency(batch.batchId);
    console.log('   billing   :', JSON.stringify({ rule: `${settlement.billing.quote?.fee.ruleId} v${settlement.billing.quote?.fee.ruleVersion}`, feeNet: settlement.billing.quote?.fee.netHalalas, feeVat: settlement.billing.quote?.fee.vatHalalas, feeTotal: settlement.billing.quote?.fee.totalHalalas, payout: settlement.billing.quote?.payoutHalalas }));
    console.log('   ledger    :', JSON.stringify(settlement.wallet.journal.map((l) => [l.walletId, l.signedAmountCents])));
    console.log('   zatca     :', JSON.stringify(settlement.zatca.invoices.map((i) => [i.invoice_type_code, i.invoice_number, `ICV ${i.icv}`, i.status, i.total_amount_halalas])));
    const statement = settlement.fileCache.statement?.statement;
    console.log(`   statement : ${statement?.status}, gross ${statement?.amounts.gross} − fee ${statement?.amounts.feeTotal} = payout ${statement?.amounts.netPayout} SAR, journal ${statement?.journal.transactionId}, kept until ${statement?.expiresAt}`);
    console.log(`   consistency: ${settlementReport.ok ? 'OK' : 'NOT OK'} (${settlementReport.status})`);
    for (const check of settlementReport.checks.filter((c) => !c.ok)) console.log(`   ✘ ${check.name} — ${check.detail}`);
    for (const doc of statement?.tax?.documents ?? []) {
      const pdf = await platform.fileCache.settlements.worker.getDocument(settlement.zatca.invoices[0].taxpayer_id, doc.invoiceId, 'pdf');
      if (pdf) {
        const out = path.join(os.tmpdir(), `${doc.invoiceNumber}.pdf`);
        await fs.writeFile(out, pdf.bytes);
        console.log(`   ${doc.role} PDF written to ${out}`);
      }
    }
    if (!settlementReport.ok) process.exitCode = 1;

    // ── 7. partial customer refund ────────────────────────────────────────────
    const refundBody = {
      id: `rf_demo_${crypto.randomUUID().slice(0, 8)}`,
      resourcePath: `/v1/payments/${paymentId}/refunds/1`,
      paymentType: 'RF',
      referencedId: paymentId,
      amount: '50.00',
      currency: 'SAR',
      result: { code: '000.100.110', description: 'refund processed' },
      reason: 'one item returned',
    };
    const refundRaw = JSON.stringify(refundBody);
    const refundRes = await platform.hyperpay.app.inject({
      method: 'POST',
      url: '/api/webhook/hyperpay',
      headers: { 'content-type': 'application/json', [SIGNATURE_HEADER]: sign(platform.webhookSecret, refundRaw, Math.floor(Date.now() / 1000)) },
      payload: refundRaw,
    });
    console.log(`7. refund webhook: ${refundRes.statusCode} ${refundRes.json().outcome}, ${refundRes.json().amountHalalas / 100} SAR (refundable left ${refundRes.json().refundableHalalas / 100} SAR)`);
    await platform.settle();
    const refunded = await platform.traceOrder(orderId);
    const refundReport = await platform.verifyOrderConsistency(orderId);
    console.log('   billing   :', JSON.stringify(refunded.refunds.billing.refunds.map((r) => ({ restored: r.restoredCredits, grant: r.grantId }))));
    console.log('   ledger    :', JSON.stringify(refunded.refunds.wallet.refunds.map((r) => [r.journalRef, r.merchantAccount, r.netHalalas, r.vatHalalas, r.amountHalalas])));
    console.log('   zatca     :', JSON.stringify(refunded.refunds.zatca.creditNotes.map((n) => [n.invoice_number, `ICV ${n.icv}`, n.status, n.total_amount_halalas])));
    console.log(`   file cache: invoice PDF re-rendered with x-credited-halalas=${refunded.refunds.fileCache.invoicePdf?.['x-credited-halalas']}`);
    console.log(`   consistency: ${refundReport.ok ? 'OK' : 'NOT OK'}`);
    for (const check of refundReport.checks.filter((c) => !c.ok)) console.log(`   ✘ ${check.name} — ${check.detail}`);
    if (!refundReport.ok) process.exitCode = 1;
  } finally {
    await platform.shutdown();
  }
}

main().catch((err) => {
  console.error('platform demo failed:', err);
  process.exit(1);
});
