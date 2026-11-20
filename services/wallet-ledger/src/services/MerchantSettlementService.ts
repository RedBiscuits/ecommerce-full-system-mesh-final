import crypto from 'crypto';
import type { PoolClient } from 'pg';
import { query, withTransaction } from '../db';
import { config } from '../config/env';
import { JournalLine, systemAccountsFor, toBigInt } from '../money';
import { postJournal } from './ledgerWriter';
import { assertWalletNotFrozen, withMoneyTransaction } from './moneyTransaction';
import { WalletVersionConflictError } from '../types/wallet';
import {
  CONSUMERS,
  EVENT_TYPES,
  EXCHANGES,
  PRODUCERS,
  ROUTING_KEYS,
  buildIntegrationEvent,
  deterministicEventId,
  settlementJournalRef,
  type MerchantSettlementCollectedPayload,
  type MerchantSettlementPostedPayload,
  type SettlementDocumentsReportedEvent,
  type SettlementFee,
  type SettlementFeeCalculatedEvent,
} from '../../../../shared/contracts';
import {
  NonRetryableError,
  claimCheckpoint,
  completeCheckpoint,
  enqueueIntegrationEvent,
  listCheckpoints,
  type Checkpoint,
} from '../../../../shared/messaging';

export const LEDGER_SETTLEMENT_FEE_CONSUMER = CONSUMERS.ledgerSettlementFees;
export const LEDGER_SETTLEMENT_DOCUMENTS_CONSUMER = CONSUMERS.ledgerSettlementDocuments;

export type SettlementBatchStatus = 'COLLECTING' | 'COLLECTED' | 'EMPTY' | 'POSTED' | 'COMPLETED';

export interface SettlementBatchView {
  batchId: string;
  merchantId: string;
  currency: string;
  status: SettlementBatchStatus;
  /** `from` null = since the beginning of the system. */
  window: { from: string | null; to: string };
  idempotencyKey: string | null;
  maxItems: number;
  itemCount: number;
  grossHalalas: number;
  /** Keyset chunks the sweep took (each one short transaction). */
  chunks: number;
  firstSettledAt: string | null;
  lastSettledAt: string | null;
  /** The batch hit maxItems: unswept entries remain in the window for the next batch. */
  hasMore: boolean;
  fee: SettlementFee | null;
  feeNetHalalas: number | null;
  feeVatHalalas: number | null;
  feeTotalHalalas: number | null;
  payoutHalalas: number | null;
  journalRef: string | null;
  taxDocuments: unknown;
  createdAt: string;
  collectedAt: string | null;
  postedAt: string | null;
  completedAt: string | null;
}

export interface OpenBatchInput {
  merchantId: string;
  /** Start of the window (inclusive). Omitted / null: since the beginning of the system. */
  from?: string | Date | null;
  /** End of the window (exclusive). Default: now. */
  to?: string | Date;
  /** Same key → same batch (a retried request never opens a second batch). */
  idempotencyKey?: string;
  /** Cap on the entries of this batch (default: SETTLEMENT_MAX_ITEMS_PER_BATCH). */
  maxItems?: number;
}

export interface CollectOptions {
  chunkSize?: number;
  pauseMs?: number;
  /** Stop between chunks (the batch stays COLLECTING and can be resumed). */
  signal?: AbortSignal;
  /** Observe progress (tests, logs). */
  onChunk?: (progress: { batchId: string; chunk: number; claimed: number; itemCount: number; grossHalalas: number }) => void;
}

export type SettlementPostOutcome = 'POSTED' | 'ALREADY_POSTED' | 'DUPLICATE_IGNORED';
export type SettlementCompleteOutcome = 'COMPLETED' | 'ALREADY_COMPLETED' | 'DUPLICATE_IGNORED';

export class SettlementRequestError extends Error {
  constructor(
    public readonly statusCode: number,
    public readonly code: string,
    message: string,
    public readonly batch?: SettlementBatchView
  ) {
    super(message);
    this.name = 'SettlementRequestError';
  }
}

interface BatchRow {
  batch_id: string;
  merchant_id: string;
  currency: string;
  idempotency_key: string | null;
  window_from: Date | string | null;
  window_to: Date | string;
  max_items: number;
  status: SettlementBatchStatus;
  item_count: string | number;
  gross_cents: string | number;
  chunks: number;
  first_settled_at: Date | string | null;
  last_settled_at: Date | string | null;
  has_more: boolean;
  fee_rule_id: string | null;
  fee_rule_version: number | null;
  fee: SettlementFee | null;
  fee_net_cents: string | number | null;
  fee_vat_cents: string | number | null;
  fee_total_cents: string | number | null;
  payout_cents: string | number | null;
  journal_ref: string | null;
  collected_event_id: string | null;
  posted_event_id: string | null;
  tax_documents: unknown;
  created_at: Date | string;
  collected_at: Date | string | null;
  posted_at: Date | string | null;
  completed_at: Date | string | null;
}

interface MerchantRow {
  merchant_id: string;
  revenue_wallet_id: string;
  payout_wallet_id: string | null;
  currency: string;
  display_name: string;
}

interface LockedWallet {
  wallet_id: string;
  version: number;
  balance_cents: unknown;
  ledger_balance_cents: unknown;
}

const iso = (v: Date | string | null): string | null => (v === null || v === undefined ? null : new Date(v).toISOString());
const num = (v: string | number | null): number | null => (v === null || v === undefined ? null : Number(v));

function toView(row: BatchRow): SettlementBatchView {
  return {
    batchId: row.batch_id,
    merchantId: row.merchant_id,
    currency: row.currency,
    status: row.status,
    window: { from: iso(row.window_from), to: iso(row.window_to) as string },
    idempotencyKey: row.idempotency_key,
    maxItems: Number(row.max_items),
    itemCount: Number(row.item_count),
    grossHalalas: Number(row.gross_cents),
    chunks: Number(row.chunks),
    firstSettledAt: iso(row.first_settled_at),
    lastSettledAt: iso(row.last_settled_at),
    hasMore: row.has_more,
    fee: row.fee,
    feeNetHalalas: num(row.fee_net_cents),
    feeVatHalalas: num(row.fee_vat_cents),
    feeTotalHalalas: num(row.fee_total_cents),
    payoutHalalas: num(row.payout_cents),
    journalRef: row.journal_ref,
    taxDocuments: row.tax_documents ?? null,
    createdAt: iso(row.created_at) as string,
    collectedAt: iso(row.collected_at),
    postedAt: iso(row.posted_at),
    completedAt: iso(row.completed_at),
  };
}

function toDate(value: string | Date, label: string): Date {
  const d = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(d.getTime())) throw new SettlementRequestError(400, 'INVALID_WINDOW', `${label} is not a valid timestamp: ${String(value)}`);
  return d;
}

const yieldToEventLoop = (): Promise<void> => new Promise((resolve) => setImmediate(resolve));
const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * The ledger's part of the merchant settlement workflow.
 *
 * 1. Sweep (collect). A batch claims the merchant's settled, not-yet-swept order revenue in its
 *    window (`from` omitted = since the beginning) in keyset chunks: each chunk is ONE short
 *    transaction that claims up to `chunkSize` entries (UPDATE … SET settlement_batch_id under
 *    FOR UPDATE SKIP LOCKED) and adds their sum to the batch, all inside PostgreSQL (the app only
 *    ever holds one aggregate row); the event loop is yielded between chunks. Memory and lock
 *    time are O(chunk) whatever the history length. A claimed entry leaves the unswept partial
 *    index, so it is never scanned again, by this or any later sweep; a trigger forbids moving it.
 *    Closing the batch and writing `ledger.merchant-settlement.collected` to the outbox commit
 *    together. A crashed sweep is resumed (`resumeCollecting`): chunks are idempotent.
 * 2. Post. On billing's `billing.settlement-fee.calculated`, ONE transaction: checkpoint, ledger
 *    cross-check (Σ revenue legs of the claimed capture journals = batch gross), wallet locks, the
 *    balanced 3-leg settlement journal
 *        merchant revenue DEBIT gross | platform fee treasury CREDIT fee | merchant payout CREDIT payout
 *    the batch → POSTED (journal_ref UNIQUE: never posted twice) and the
 *    `ledger.merchant-settlement.posted` outbox event.
 * 3. Complete. On ZATCA's `zatca.settlement-documents.reported`: the batch → COMPLETED with the
 *    document references (checkpointed).
 */
export class MerchantSettlementService {
  private readonly collecting = new Map<string, Promise<SettlementBatchView>>();

  constructor(
    private readonly options: {
      chunkSize?: number;
      maxItemsPerBatch?: number;
      pauseMs?: number;
      /** Called after a transaction that wrote an outbox event committed (wakes the relay). */
      onOutboxEnqueued?: () => void;
    } = {}
  ) {}

  /** Wire the relay wake-up after construction (the runtime owns the relay). */
  setOutboxListener(listener: () => void): void {
    this.options.onOutboxEnqueued = listener;
  }

  // ── 1. sweep ───────────────────────────────────────────────────────────────

  /** Open a batch and sweep it to the end (convenience for schedulers, tests and `wait=true`). */
  async sweep(input: OpenBatchInput, options: CollectOptions = {}): Promise<SettlementBatchView> {
    const { batch } = await this.openBatch(input);
    return batch.status === 'COLLECTING' ? this.collect(batch.batchId, options) : batch;
  }

  /**
   * Open a batch (status COLLECTING). Idempotent per idempotencyKey; one COLLECTING batch per
   * merchant at a time (409 SETTLEMENT_IN_PROGRESS, with that batch).
   */
  async openBatch(input: OpenBatchInput): Promise<{ batch: SettlementBatchView; created: boolean }> {
    const maxItems = input.maxItems ?? this.options.maxItemsPerBatch ?? config.settlement.maxItemsPerBatch;
    if (!Number.isInteger(maxItems) || maxItems <= 0) {
      throw new SettlementRequestError(400, 'INVALID_MAX_ITEMS', `maxItems must be a positive integer, got ${maxItems}`);
    }
    const from = input.from === undefined || input.from === null ? null : toDate(input.from, 'from');
    const to = input.to === undefined ? null : toDate(input.to, 'to');
    if (from && to && from >= to) throw new SettlementRequestError(400, 'INVALID_WINDOW', '`from` must be before `to`');

    return withTransaction(async (client) => {
      const merchant = await this.merchantFor(client, input.merchantId);
      if (!merchant.payout_wallet_id) {
        throw new SettlementRequestError(422, 'MERCHANT_PAYOUT_WALLET_MISSING', `Merchant ${merchant.merchant_id} has no payout wallet`);
      }
      if (input.idempotencyKey) {
        const existing = await client.query<BatchRow>(
          `SELECT * FROM merchant_settlement_batches WHERE merchant_id = $1 AND idempotency_key = $2`,
          [merchant.merchant_id, input.idempotencyKey]
        );
        if (existing.rows.length > 0) return { batch: toView(existing.rows[0]), created: false };
      }
      const open = await client.query<BatchRow>(
        `SELECT * FROM merchant_settlement_batches WHERE merchant_id = $1 AND status = 'COLLECTING'`,
        [merchant.merchant_id]
      );
      if (open.rows.length > 0) {
        throw new SettlementRequestError(
          409,
          'SETTLEMENT_IN_PROGRESS',
          `Merchant ${merchant.merchant_id} already has batch ${open.rows[0].batch_id} collecting`,
          toView(open.rows[0])
        );
      }
      const inserted = await client.query<BatchRow>(
        `INSERT INTO merchant_settlement_batches
           (batch_id, merchant_id, currency, idempotency_key, window_from, window_to, max_items, status)
         VALUES ($1, $2, $3, $4, $5::timestamptz, COALESCE($6::timestamptz, NOW()), $7, 'COLLECTING')
         RETURNING *`,
        [
          `stb_${crypto.randomUUID()}`,
          merchant.merchant_id,
          merchant.currency,
          input.idempotencyKey ?? null,
          from ? from.toISOString() : null,
          to ? to.toISOString() : null,
          maxItems,
        ]
      );
      const row = inserted.rows[0];
      if (row.window_from !== null && new Date(row.window_from) >= new Date(row.window_to)) {
        throw new SettlementRequestError(400, 'INVALID_WINDOW', '`from` must be before `to`');
      }
      return { batch: toView(row), created: true };
    });
  }

  /**
   * Sweep a COLLECTING batch to the end, chunk by chunk. Safe to call concurrently and to resume
   * after a crash: within a process, concurrent calls for one batch share one run; across
   * processes every chunk re-reads the batch under its row lock and only claims unswept entries.
   */
  async collect(batchId: string, options: CollectOptions = {}): Promise<SettlementBatchView> {
    const running = this.collecting.get(batchId);
    if (running) return running;
    const run = this.collectLoop(batchId, options).finally(() => this.collecting.delete(batchId));
    this.collecting.set(batchId, run);
    return run;
  }

  /** Resume every batch left COLLECTING (crash, abort). */
  async resumeCollecting(options: CollectOptions = {}): Promise<SettlementBatchView[]> {
    const res = await query<{ batch_id: string }>(
      `SELECT batch_id FROM merchant_settlement_batches WHERE status = 'COLLECTING' ORDER BY created_at`
    );
    const done: SettlementBatchView[] = [];
    for (const row of res.rows) done.push(await this.collect(row.batch_id, options));
    return done;
  }

  private async collectLoop(batchId: string, options: CollectOptions): Promise<SettlementBatchView> {
    const chunkSize = options.chunkSize ?? this.options.chunkSize ?? config.settlement.chunkSize;
    const pauseMs = options.pauseMs ?? this.options.pauseMs ?? config.settlement.pauseMs;
    if (!Number.isInteger(chunkSize) || chunkSize <= 0) throw new Error(`chunkSize must be a positive integer, got ${chunkSize}`);
    for (;;) {
      options.signal?.throwIfAborted();
      const step = await withTransaction(async (client) => this.collectChunk(client, batchId, chunkSize));
      if (step.enqueued) this.options.onOutboxEnqueued?.();
      if (step.batch.status !== 'COLLECTING') return step.batch;
      options.onChunk?.({
        batchId,
        chunk: step.batch.chunks,
        claimed: step.claimed,
        itemCount: step.batch.itemCount,
        grossHalalas: step.batch.grossHalalas,
      });
      // Never hog the event loop or the (single-connection) database: other requests run between chunks.
      await yieldToEventLoop();
      if (pauseMs > 0) await sleep(pauseMs);
    }
  }

  /** One chunk: claim up to `chunkSize` unswept entries, or close the batch. One transaction. */
  private async collectChunk(
    client: PoolClient,
    batchId: string,
    chunkSize: number
  ): Promise<{ batch: SettlementBatchView; claimed: number; enqueued: boolean }> {
    const res = await client.query<BatchRow>(`SELECT * FROM merchant_settlement_batches WHERE batch_id = $1 FOR UPDATE`, [batchId]);
    if (res.rows.length === 0) throw new SettlementRequestError(404, 'BATCH_NOT_FOUND', `Settlement batch ${batchId} not found`);
    const batch = res.rows[0];
    if (batch.status !== 'COLLECTING') return { batch: toView(batch), claimed: 0, enqueued: false };

    const room = Number(batch.max_items) - Number(batch.item_count);
    const limit = Math.min(chunkSize, room);
    if (limit <= 0) {
      const more = await client.query<{ more: boolean }>(
        `SELECT EXISTS (
           SELECT 1 FROM order_settlements
           WHERE merchant_id = $1 AND settlement_batch_id IS NULL
             AND ($2::timestamptz IS NULL OR created_at >= $2::timestamptz) AND created_at < $3::timestamptz
         ) AS more`,
        [batch.merchant_id, iso(batch.window_from), iso(batch.window_to)]
      );
      return this.closeBatch(client, batch, more.rows[0].more === true);
    }

    // Claim the next chunk entirely inside PostgreSQL: the app receives one aggregate row.
    const claim = await client.query<{ n: number; total: string; first_at: Date | null; last_at: Date | null }>(
      `WITH picked AS (
         SELECT order_id FROM order_settlements
         WHERE merchant_id = $1 AND settlement_batch_id IS NULL
           AND ($2::timestamptz IS NULL OR created_at >= $2::timestamptz) AND created_at < $3::timestamptz
         ORDER BY created_at, order_id
         LIMIT $4
         FOR UPDATE SKIP LOCKED
       ), claimed AS (
         UPDATE order_settlements os
           SET settlement_batch_id = $5, settled_net_cents = os.net_revenue_cents - os.refunded_net_cents
         FROM picked
         WHERE os.order_id = picked.order_id AND os.settlement_batch_id IS NULL
         RETURNING os.settled_net_cents AS net_revenue_cents, os.created_at
       )
       SELECT COUNT(*)::int AS n, COALESCE(SUM(net_revenue_cents), 0)::text AS total,
              MIN(created_at) AS first_at, MAX(created_at) AS last_at
       FROM claimed`,
      [batch.merchant_id, iso(batch.window_from), iso(batch.window_to), limit, batchId]
    );
    const claimed = Number(claim.rows[0].n);
    const updated = await client.query<BatchRow>(
      `UPDATE merchant_settlement_batches SET
         item_count = item_count + $2,
         gross_cents = gross_cents + $3::bigint,
         chunks = chunks + 1,
         first_settled_at = LEAST(first_settled_at, $4::timestamptz),
         last_settled_at = GREATEST(last_settled_at, $5::timestamptz),
         updated_at = NOW()
       WHERE batch_id = $1
       RETURNING *`,
      [batchId, claimed, claim.rows[0].total, iso(claim.rows[0].first_at), iso(claim.rows[0].last_at)]
    );
    const row = updated.rows[0];
    if (claimed < limit) {
      // The window has no more unswept entries (fewer than asked for were left).
      return { ...(await this.closeBatch(client, row, false)), claimed };
    }
    return { batch: toView(row), claimed, enqueued: false };
  }

  private async closeBatch(
    client: PoolClient,
    batch: BatchRow,
    hasMore: boolean
  ): Promise<{ batch: SettlementBatchView; claimed: number; enqueued: boolean }> {
    const gross = Number(batch.gross_cents);
    const items = Number(batch.item_count);
    if (items === 0 || gross === 0) {
      // Nothing to settle (no entries, or only zero-revenue ones, which stay claimed by this batch).
      const res = await client.query<BatchRow>(
        `UPDATE merchant_settlement_batches SET status = 'EMPTY', has_more = $2, collected_at = NOW(), updated_at = NOW()
         WHERE batch_id = $1 RETURNING *`,
        [batch.batch_id, hasMore]
      );
      return { batch: toView(res.rows[0]), claimed: 0, enqueued: false };
    }
    const eventId = deterministicEventId(EVENT_TYPES.merchantSettlementCollected, batch.batch_id);
    const res = await client.query<BatchRow>(
      `UPDATE merchant_settlement_batches SET status = 'COLLECTED', has_more = $2, collected_at = NOW(),
         collected_event_id = $3, updated_at = NOW()
       WHERE batch_id = $1 RETURNING *`,
      [batch.batch_id, hasMore, eventId]
    );
    const row = res.rows[0];
    const payload: MerchantSettlementCollectedPayload = {
      batchId: row.batch_id,
      merchantId: row.merchant_id,
      currency: 'SAR',
      window: { from: iso(row.window_from), to: iso(row.window_to) as string },
      itemCount: items,
      grossHalalas: gross,
      firstSettledAt: iso(row.first_settled_at) as string,
      lastSettledAt: iso(row.last_settled_at) as string,
      hasMore,
      collectedAt: iso(row.collected_at) as string,
    };
    await enqueueIntegrationEvent(client, {
      exchange: EXCHANGES.ledger.name,
      routingKey: ROUTING_KEYS.merchantSettlementCollected,
      event: buildIntegrationEvent({
        eventId,
        eventType: EVENT_TYPES.merchantSettlementCollected,
        producer: PRODUCERS.ledger,
        correlationId: row.batch_id,
        causationId: null,
        aggregateId: row.batch_id,
        payload,
      }),
    });
    return { batch: toView(row), claimed: 0, enqueued: true };
  }

  // ── 2. post the settlement journal ─────────────────────────────────────────

  async handleFeeCalculated(event: SettlementFeeCalculatedEvent): Promise<{
    outcome: SettlementPostOutcome;
    batchId: string;
    journalRef?: string;
    eventId?: string;
  }> {
    const p = event.payload;
    const result = await withMoneyTransaction(async (client) => {
      const key = { consumer: LEDGER_SETTLEMENT_FEE_CONSUMER, eventId: event.eventId, step: 'done' };
      if (!(await claimCheckpoint(client, { ...key, correlationId: p.batchId }))) {
        return { outcome: 'DUPLICATE_IGNORED' as const, batchId: p.batchId };
      }
      const res = await client.query<BatchRow>(`SELECT * FROM merchant_settlement_batches WHERE batch_id = $1 FOR UPDATE`, [p.batchId]);
      if (res.rows.length === 0) throw new NonRetryableError('BATCH_NOT_FOUND', `Settlement batch ${p.batchId} does not exist`);
      const batch = res.rows[0];

      if (batch.status === 'POSTED' || batch.status === 'COMPLETED') {
        // Posted by an earlier fee event: the journal exists once (journal_ref UNIQUE); nothing moves.
        await completeCheckpoint(client, { ...key, outcome: 'ALREADY_POSTED', detail: { journalRef: batch.journal_ref } });
        return { outcome: 'ALREADY_POSTED' as const, batchId: p.batchId, journalRef: batch.journal_ref ?? undefined };
      }
      if (batch.status !== 'COLLECTED') {
        throw new NonRetryableError('BATCH_NOT_COLLECTED', `Settlement batch ${p.batchId} is ${batch.status}`);
      }
      const gross = Number(batch.gross_cents);
      const items = Number(batch.item_count);
      if (batch.merchant_id !== p.merchantId || gross !== p.grossHalalas || items !== p.itemCount) {
        throw new NonRetryableError(
          'BATCH_MISMATCH',
          `Batch ${p.batchId} collected ${items} entries / ${gross} for ${batch.merchant_id}; ` +
            `the fee was priced for ${p.itemCount} / ${p.grossHalalas} for ${p.merchantId}`
        );
      }
      const fee = p.fee;
      if (fee.totalHalalas !== fee.netHalalas + fee.vatHalalas || p.payoutHalalas !== gross - fee.totalHalalas || p.payoutHalalas < 0) {
        throw new NonRetryableError(
          'FEE_ARITHMETIC_MISMATCH',
          `Batch ${p.batchId}: gross ${gross}, fee ${fee.netHalalas} + VAT ${fee.vatHalalas} = ${fee.totalHalalas}, payout ${p.payoutHalalas}`
        );
      }

      const merchant = await this.merchantFor(client, batch.merchant_id);
      if (!merchant.payout_wallet_id) {
        throw new NonRetryableError('MERCHANT_PAYOUT_WALLET_MISSING', `Merchant ${merchant.merchant_id} has no payout wallet`);
      }
      await this.assertLedgerBacksBatch(client, batch, merchant);

      // Lock the two app-maintained balances in wallet_id order (the fee treasury is sharded and
      // ledger-maintained: its bucket is updated by the delta trigger).
      const feeWalletId = systemAccountsFor('SAR').feeTreasury;
      const wallets = await this.lockWallets(client, [merchant.revenue_wallet_id, merchant.payout_wallet_id]);
      const revenue = wallets.get(merchant.revenue_wallet_id);
      const payoutWallet = wallets.get(merchant.payout_wallet_id);
      if (!revenue || !payoutWallet) {
        throw new NonRetryableError('MERCHANT_WALLET_NOT_FOUND', `Wallets of merchant ${merchant.merchant_id} are missing`);
      }
      assertWalletNotFrozen(merchant.revenue_wallet_id, revenue);
      assertWalletNotFrozen(merchant.payout_wallet_id, payoutWallet);
      const available = toBigInt(revenue.balance_cents, 'balance_cents');
      if (available < BigInt(gross)) {
        throw new NonRetryableError(
          'INSUFFICIENT_MERCHANT_REVENUE',
          `Wallet ${merchant.revenue_wallet_id} holds ${available} but batch ${p.batchId} settles ${gross}`
        );
      }
      await this.adjustBalance(client, revenue, -BigInt(gross));
      if (p.payoutHalalas > 0) await this.adjustBalance(client, payoutWallet, BigInt(p.payoutHalalas));

      const journalRef = settlementJournalRef(p.batchId);
      const legs: Array<JournalLine & { role: MerchantSettlementPostedPayload['journal'][number]['role'] }> = [
        { walletId: merchant.revenue_wallet_id, currency: 'SAR', signedAmountMinor: -BigInt(gross), role: 'MERCHANT_REVENUE' },
        { walletId: feeWalletId, currency: 'SAR', signedAmountMinor: BigInt(fee.totalHalalas), role: 'PLATFORM_FEES' },
        { walletId: merchant.payout_wallet_id, currency: 'SAR', signedAmountMinor: BigInt(p.payoutHalalas), role: 'MERCHANT_PAYOUT' },
      ];
      const posted = legs.filter((l) => l.signedAmountMinor !== 0n);
      await postJournal(
        client,
        journalRef,
        posted.map(({ walletId, currency, signedAmountMinor }) => ({ walletId, currency, signedAmountMinor }))
      );

      const eventId = deterministicEventId(EVENT_TYPES.merchantSettlementPosted, p.batchId);
      const updated = await client.query<BatchRow>(
        `UPDATE merchant_settlement_batches SET
           status = 'POSTED', fee_rule_id = $2, fee_rule_version = $3, fee = $4::jsonb,
           fee_net_cents = $5, fee_vat_cents = $6, fee_total_cents = $7, payout_cents = $8,
           journal_ref = $9, posted_event_id = $10, posted_at = NOW(), updated_at = NOW()
         WHERE batch_id = $1
         RETURNING *`,
        [p.batchId, fee.ruleId, fee.ruleVersion, JSON.stringify(fee), fee.netHalalas, fee.vatHalalas, fee.totalHalalas, p.payoutHalalas, journalRef, eventId]
      );
      const row = updated.rows[0];
      const payload: MerchantSettlementPostedPayload = {
        batchId: row.batch_id,
        merchantId: row.merchant_id,
        merchantName: merchant.display_name,
        currency: 'SAR',
        window: { from: iso(row.window_from), to: iso(row.window_to) as string },
        itemCount: items,
        grossHalalas: gross,
        fee,
        payoutHalalas: p.payoutHalalas,
        journalRef,
        journal: posted.map((l) => ({
          walletId: l.walletId,
          role: l.role,
          direction: l.signedAmountMinor < 0n ? ('DEBIT' as const) : ('CREDIT' as const),
          amountHalalas: Number(l.signedAmountMinor < 0n ? -l.signedAmountMinor : l.signedAmountMinor),
        })),
        accounts: { revenueWalletId: merchant.revenue_wallet_id, feeWalletId, payoutWalletId: merchant.payout_wallet_id },
        firstSettledAt: iso(row.first_settled_at) as string,
        lastSettledAt: iso(row.last_settled_at) as string,
        postedAt: iso(row.posted_at) as string,
      };
      await enqueueIntegrationEvent(client, {
        exchange: EXCHANGES.ledger.name,
        routingKey: ROUTING_KEYS.merchantSettlementPosted,
        event: buildIntegrationEvent({
          eventId,
          eventType: EVENT_TYPES.merchantSettlementPosted,
          producer: PRODUCERS.ledger,
          correlationId: p.batchId,
          causationId: event.eventId,
          aggregateId: p.batchId,
          payload,
        }),
      });
      await completeCheckpoint(client, {
        ...key,
        outcome: 'POSTED',
        detail: { journalRef, grossHalalas: gross, feeTotalHalalas: fee.totalHalalas, payoutHalalas: p.payoutHalalas, emittedEventId: eventId },
      });
      return { outcome: 'POSTED' as const, batchId: p.batchId, journalRef, eventId };
    });
    if (result.outcome === 'POSTED') this.options.onOutboxEnqueued?.();
    return result;
  }

  /**
   * The batch must be what the ledger booked: Σ of the merchant-revenue legs of the claimed
   * entries' capture journals equals the batch gross (and so does Σ of their recorded revenue).
   * Computed by PostgreSQL over the batch index; the app holds one row.
   */
  private async assertLedgerBacksBatch(client: PoolClient, batch: BatchRow, merchant: MerchantRow): Promise<void> {
    // Booked = the revenue legs of the claimed captures, minus the revenue legs of the refunds
    // reversed from the revenue wallet before the sweep (refunds after the sweep hit the payout
    // wallet, so they do not touch this batch).
    const res = await client.query<{ n: number; claimed: string; recorded: string; booked: string }>(
      `WITH claimed AS (
         SELECT os.order_id, os.journal_ref, os.settled_net_cents AS held,
                os.net_revenue_cents - COALESCE((SELECT SUM(r.net_cents) FROM order_refunds r
                                                   WHERE r.order_id = os.order_id AND r.merchant_account = 'REVENUE'), 0) AS recorded
         FROM order_settlements os WHERE os.settlement_batch_id = $1
       ), refs AS (
         SELECT journal_ref FROM claimed
         UNION ALL
         SELECT r.journal_ref FROM order_refunds r JOIN claimed c ON c.order_id = r.order_id WHERE r.merchant_account = 'REVENUE'
       )
       SELECT (SELECT COUNT(*)::int FROM claimed) AS n,
              (SELECT COALESCE(SUM(held), 0)::text FROM claimed) AS claimed,
              (SELECT COALESCE(SUM(recorded), 0)::text FROM claimed) AS recorded,
              (SELECT COALESCE(SUM(le.signed_amount_cents), 0)::text
                 FROM ledger_entries le JOIN refs ON refs.journal_ref = le.transaction_ref
                WHERE le.wallet_id = $2) AS booked`,
      [batch.batch_id, merchant.revenue_wallet_id]
    );
    const { n, claimed, recorded, booked } = res.rows[0];
    const gross = String(batch.gross_cents);
    if (Number(n) !== Number(batch.item_count) || claimed !== gross || recorded !== gross || booked !== gross) {
      throw new NonRetryableError(
        'SETTLEMENT_LEDGER_MISMATCH',
        `Batch ${batch.batch_id}: ${batch.item_count} entries / ${gross} collected, ` +
          `but ${n} entries / ${claimed} claimed / ${recorded} recorded / ${booked} booked to ${merchant.revenue_wallet_id}`
      );
    }
  }

  // ── 3. complete ────────────────────────────────────────────────────────────

  async handleDocumentsReported(event: SettlementDocumentsReportedEvent): Promise<{ outcome: SettlementCompleteOutcome; batchId: string }> {
    const p = event.payload;
    return withTransaction(async (client) => {
      const key = { consumer: LEDGER_SETTLEMENT_DOCUMENTS_CONSUMER, eventId: event.eventId, step: 'done' };
      if (!(await claimCheckpoint(client, { ...key, correlationId: p.batchId }))) {
        return { outcome: 'DUPLICATE_IGNORED' as const, batchId: p.batchId };
      }
      const res = await client.query<BatchRow>(`SELECT * FROM merchant_settlement_batches WHERE batch_id = $1 FOR UPDATE`, [p.batchId]);
      if (res.rows.length === 0) throw new NonRetryableError('BATCH_NOT_FOUND', `Settlement batch ${p.batchId} does not exist`);
      const batch = res.rows[0];
      if (batch.journal_ref !== p.journalRef) {
        throw new NonRetryableError('BATCH_MISMATCH', `Batch ${p.batchId} posted ${batch.journal_ref}, ZATCA reported ${p.journalRef}`);
      }
      let outcome: SettlementCompleteOutcome = 'ALREADY_COMPLETED';
      if (batch.status === 'POSTED') {
        const documents = p.documents.map((d) => ({
          role: d.role,
          taxpayerId: d.document.taxpayerId,
          invoiceId: d.document.invoiceId,
          invoiceNumber: d.document.invoiceNumber,
          invoiceTypeCode: d.document.invoiceTypeCode,
          icv: d.document.icv,
          invoiceHash: d.document.invoiceHash,
          status: d.document.status,
          totalHalalas: d.document.totals.totalHalalas,
        }));
        await client.query(
          `UPDATE merchant_settlement_batches SET status = 'COMPLETED', tax_documents = $2::jsonb, completed_at = NOW(), updated_at = NOW()
           WHERE batch_id = $1`,
          [p.batchId, JSON.stringify(documents)]
        );
        outcome = 'COMPLETED';
      } else if (batch.status !== 'COMPLETED') {
        throw new NonRetryableError('BATCH_NOT_POSTED', `Settlement batch ${p.batchId} is ${batch.status}`);
      }
      await completeCheckpoint(client, { ...key, outcome, detail: { documents: p.documents.length } });
      return { outcome, batchId: p.batchId };
    });
  }

  // ── reads ──────────────────────────────────────────────────────────────────

  async getBatch(batchId: string): Promise<SettlementBatchView | null> {
    const res = await query<BatchRow>(`SELECT * FROM merchant_settlement_batches WHERE batch_id = $1`, [batchId]);
    return res.rows[0] ? toView(res.rows[0]) : null;
  }

  /** A batch with its settlement journal and every ledger checkpoint recorded for it. */
  async traceBatch(batchId: string): Promise<{
    batch: SettlementBatchView | null;
    journal: Array<{ walletId: string; currency: string; direction: string; signedAmountCents: number }>;
    checkpoints: Checkpoint[];
  }> {
    const batch = await this.getBatch(batchId);
    const journal = batch?.journalRef
      ? await query<{ wallet_id: string; currency: string; direction: string; signed_amount_cents: string }>(
          `SELECT wallet_id, currency, direction, signed_amount_cents::text
           FROM ledger_entries WHERE transaction_ref = $1 ORDER BY wallet_id`,
          [batch.journalRef]
        )
      : { rows: [] };
    return {
      batch,
      journal: journal.rows.map((r) => ({
        walletId: r.wallet_id,
        currency: r.currency,
        direction: r.direction,
        signedAmountCents: Number(r.signed_amount_cents),
      })),
      checkpoints: await listCheckpoints({ query: (text, params) => query(text, params) }, { correlationId: batchId }),
    };
  }

  async listBatches(merchantId: string, limit = 50): Promise<SettlementBatchView[]> {
    const res = await query<BatchRow>(
      `SELECT * FROM merchant_settlement_batches WHERE merchant_id = $1 ORDER BY created_at DESC, batch_id DESC LIMIT $2`,
      [merchantId, Math.min(Math.max(limit, 1), 500)]
    );
    return res.rows.map(toView);
  }

  /** The entries a batch settled, a keyset page at a time (cursor = the last item's `cursor`). */
  async listBatchItems(
    batchId: string,
    page: { after?: string; limit?: number } = {}
  ): Promise<{ items: Array<{ orderId: string; journalRef: string; netRevenueHalalas: number; settledAt: string }>; next: string | null }> {
    const limit = Math.min(Math.max(page.limit ?? 100, 1), 1000);
    let afterAt: string | null = null;
    let afterId: string | null = null;
    if (page.after) {
      const [at, ...id] = Buffer.from(page.after, 'base64url').toString('utf8').split('|');
      afterAt = at;
      afterId = id.join('|');
    }
    const res = await query<{ order_id: string; journal_ref: string; net_revenue_cents: string; created_at: Date }>(
      `SELECT order_id, journal_ref, settled_net_cents::text AS net_revenue_cents, created_at FROM order_settlements
       WHERE settlement_batch_id = $1
         AND ($2::timestamptz IS NULL OR (created_at, order_id) > ($2::timestamptz, $3::varchar))
       ORDER BY created_at, order_id
       LIMIT $4`,
      [batchId, afterAt, afterId, limit]
    );
    const items = res.rows.map((r) => ({
      orderId: r.order_id,
      journalRef: r.journal_ref,
      netRevenueHalalas: Number(r.net_revenue_cents),
      settledAt: new Date(r.created_at).toISOString(),
    }));
    const last = items[items.length - 1];
    return {
      items,
      next: items.length === limit && last ? Buffer.from(`${last.settledAt}|${last.orderId}`, 'utf8').toString('base64url') : null,
    };
  }

  // ── internals ──────────────────────────────────────────────────────────────

  private async merchantFor(client: PoolClient, merchantId: string): Promise<MerchantRow> {
    const res = await client.query<MerchantRow>(
      `SELECT merchant_id, revenue_wallet_id, payout_wallet_id, currency, display_name FROM merchant_accounts WHERE merchant_id = $1`,
      [merchantId]
    );
    if (res.rows.length === 0) throw new SettlementRequestError(404, 'MERCHANT_NOT_FOUND', `No merchant account for ${merchantId}`);
    if (res.rows[0].currency !== 'SAR') {
      throw new SettlementRequestError(422, 'UNSUPPORTED_CURRENCY', `Merchant ${merchantId} settles in ${res.rows[0].currency}`);
    }
    return res.rows[0];
  }

  private async lockWallets(client: PoolClient, walletIds: string[]): Promise<Map<string, LockedWallet>> {
    const locked = new Map<string, LockedWallet>();
    for (const walletId of [...new Set(walletIds)].sort()) {
      const res = await client.query<LockedWallet>(
        `SELECT wallet_id, version, balance_cents, ledger_balance_cents FROM wallets WHERE wallet_id = $1 FOR UPDATE`,
        [walletId]
      );
      if (res.rows.length > 0) locked.set(walletId, res.rows[0]);
    }
    return locked;
  }

  /** Version-guarded compare-and-swap of an app-maintained (USER) balance. */
  private async adjustBalance(client: PoolClient, wallet: LockedWallet, delta: bigint): Promise<void> {
    const res = await client.query<{ version: number }>(
      `UPDATE wallets SET balance_cents = balance_cents + $1, version = version + 1, updated_at = NOW()
       WHERE wallet_id = $2 AND version = $3 AND balance_cents + $1 >= 0
       RETURNING version`,
      [delta.toString(), wallet.wallet_id, wallet.version]
    );
    if (res.rows.length === 0) throw new WalletVersionConflictError(wallet.wallet_id, Number(wallet.version));
    wallet.version = Number(res.rows[0].version);
  }
}

export const merchantSettlementService = new MerchantSettlementService();
