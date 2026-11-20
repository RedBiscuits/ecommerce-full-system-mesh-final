import crypto from 'crypto';
import { query } from '../db';
import { config } from '../config/env';
import {
  AmqpEventBus,
  EventEnvelope,
  GlobalZeroSumPayload,
  LEDGER_EVENT_TYPES,
  LedgerEventType,
  NegativeBalancePayload,
  PayloadByType,
  UnbalancedJournalPayload,
  WalletDriftPayload,
  createEnvelope,
  getEventBus,
} from '../messaging';

export interface CheckResult<T> {
  violations: number;
  /** First N violations only; `violations` is the full count. */
  sample: T[];
}

export interface LedgerAuditReport {
  auditRunId: string;
  startedAt: string;
  finishedAt: string;
  /** True only if the audit completed AND found no violations. */
  ok: boolean;
  /**
   * False if the audit stopped early because events could not be delivered
   * (queue stayed full / broker unreachable past the drain timeout). Nothing
   * is silently dropped: `events.unpublished` counts what was left, and a
   * re-run re-detects it because detection is stateless.
   */
  completed: boolean;
  incompleteReason?: string;
  checks: {
    globalZeroSum: CheckResult<GlobalZeroSumPayload>;
    unbalancedJournals: CheckResult<UnbalancedJournalPayload>;
    walletDrift: CheckResult<WalletDriftPayload>;
    negativeBalances: CheckResult<NegativeBalancePayload>;
  };
  events: {
    published: number;
    /** Violations detected but not delivered (only when completed = false). */
    unpublished: number;
    /** Times the audit paused because the work queue was at capacity. */
    backpressureWaits: number;
  };
}

export interface RunAuditOptions {
  /** Publish one event per violation to the reconciliation pipeline. Default true. */
  publish?: boolean;
  bus?: AmqpEventBus;
  pageSize?: number;
  sampleSize?: number;
  /** Max time to wait for queue capacity / confirms before the audit gives up (ms). */
  drainTimeoutMs?: number;
}

/** Internal: aborts the audit when the pipeline stops accepting events. */
class PublishStalledError extends Error {
  constructor(
    public readonly unpublished: number,
    reason: string
  ) {
    super(reason);
    this.name = 'PublishStalledError';
  }
}


/**
 * Account types whose ledger balance can never legitimately be negative
 * (FX_POOL and CLEARING positions may be). Inlined as constants, not user input.
 */
const NON_NEGATIVE_ACCOUNT_TYPES_SQL = `('USER', 'ESCROW', 'FEE_TREASURY')`;

/**
 * Verifies ledger integrity directly in PostgreSQL:
 *
 *  Global invariants
 *   - GLOBAL_ZERO_SUM: every currency's ledger sums to exactly zero.
 *   - UNBALANCED_JOURNAL: every (transaction_ref, currency) sums to zero.
 *  Per-wallet invariants
 *   - WALLET_DRIFT: balance_cents, the precalculated ledger_balance_cents and
 *     ledger_version all match a full recomputed SUM / COUNT of the ledger.
 *   - NEGATIVE_BALANCE: USER / ESCROW / FEE_TREASURY ledger sums are >= 0.
 *
 * Memory model: all aggregation happens inside PostgreSQL; results are read
 * with KEYSET pagination (WHERE key > cursor ORDER BY key LIMIT n), so the
 * process holds at most one page of rows at a time regardless of ledger size.
 * The report keeps counts plus a capped sample, never the full violation list,
 * and each page is fully delivered (with queue-drain backpressure) before the
 * next page is read, so at most one page of events is ever held.
 *
 * Consistency: each check row is computed within a single statement snapshot
 * (e.g. a wallet's balance and its ledger sum are read together), so pages
 * cannot produce false positives from concurrent writes. The reconciler
 * re-verifies every event under a row lock before acting anyway.
 */
export class LedgerAuditService {
  async runAudit(options: RunAuditOptions = {}): Promise<LedgerAuditReport> {
    const publish = options.publish ?? true;
    const pageSize = options.pageSize ?? config.audit.pageSize;
    const sampleSize = options.sampleSize ?? config.audit.reportSampleSize;
    const drainTimeoutMs = options.drainTimeoutMs ?? config.audit.drainTimeoutMs;
    const bus = publish ? (options.bus ?? getEventBus()) : undefined;

    if (!Number.isInteger(pageSize) || pageSize < 1 || pageSize > 10_000) {
      throw new Error(`Audit pageSize must be an integer in [1, 10000], got ${pageSize}`);
    }

    const auditRunId = `audit_${crypto.randomUUID()}`;
    const startedAt = new Date().toISOString();
    const events = { published: 0, unpublished: 0, backpressureWaits: 0 };

    const emptyCheck = <T>(): CheckResult<T> => ({ violations: 0, sample: [] });
    const checks: LedgerAuditReport['checks'] = {
      globalZeroSum: emptyCheck(),
      unbalancedJournals: emptyCheck(),
      walletDrift: emptyCheck(),
      negativeBalances: emptyCheck(),
    };

    const runCheck = async <T extends LedgerEventType>(
      type: T,
      result: CheckResult<PayloadByType[T]>,
      pages: AsyncGenerator<PayloadByType[T][]>
    ): Promise<void> => {
      for await (const page of pages) {
        result.violations += page.length;
        for (const violation of page) {
          if (result.sample.length < sampleSize) result.sample.push(violation);
        }
        if (bus) {
          // Between pages: block until this page is fully delivered, waiting
          // for consumers to drain the queue if it is at capacity.
          await this.publishWithBackpressure(
            bus,
            page.map((p) => createEnvelope(type, auditRunId, p)),
            events,
            drainTimeoutMs
          );
        }
      }
    };

    let completed = true;
    let incompleteReason: string | undefined;
    try {
      await runCheck(LEDGER_EVENT_TYPES.GLOBAL_ZERO_SUM, checks.globalZeroSum, this.globalZeroSumPages());
      await runCheck(
        LEDGER_EVENT_TYPES.UNBALANCED_JOURNAL,
        checks.unbalancedJournals,
        this.unbalancedJournalPages(pageSize)
      );
      await runCheck(LEDGER_EVENT_TYPES.WALLET_DRIFT, checks.walletDrift, this.walletDriftPages(pageSize));
      await runCheck(
        LEDGER_EVENT_TYPES.NEGATIVE_BALANCE,
        checks.negativeBalances,
        this.negativeBalancePages(pageSize)
      );
    } catch (err) {
      if (!(err instanceof PublishStalledError)) throw err;
      completed = false;
      incompleteReason = err.message;
      events.unpublished += err.unpublished;
    }

    const violationsFound =
      checks.globalZeroSum.violations +
      checks.unbalancedJournals.violations +
      checks.walletDrift.violations +
      checks.negativeBalances.violations;

    return {
      auditRunId,
      startedAt,
      finishedAt: new Date().toISOString(),
      ok: completed && violationsFound === 0,
      completed,
      ...(incompleteReason ? { incompleteReason } : {}),
      checks,
      events,
    };
  }

  /**
   * Deliver one page of events without dropping any, with event-driven waits:
   *  - register for the next capacity signal BEFORE reading headroom (no lost
   *    wake-ups), then publish at most the queue's free space;
   *  - if the queue is full, or the broker NACKed anything, await that signal:
   *    it fires when any consumer (in any process) settles a work message, or
   *    when a broker session (re)connects;
   *  - publishes are confirm-tracked by the bus, so ones dropped by a broker
   *    disconnect are re-sent on reconnect and still count as delivered.
   * Every wait has a deadline (AbortSignal.timeout); if nothing frees capacity
   * within it the audit stops (INCOMPLETE) instead of dropping or spinning.
   * Memory is bounded by one page.
   */
  private async publishWithBackpressure(
    bus: AmqpEventBus,
    envelopes: EventEnvelope[],
    counters: LedgerAuditReport['events'],
    drainTimeoutMs: number
  ): Promise<void> {
    let pending = envelopes;

    while (pending.length > 0) {
      const capacity = bus.nextCapacitySignal(AbortSignal.timeout(drainTimeoutMs));
      try {
        const headroom = await bus.workQueueHeadroom(AbortSignal.timeout(drainTimeoutMs));

        if (headroom > 0) {
          const batch = pending.slice(0, headroom);
          const rest = pending.slice(headroom);
          const results = await Promise.allSettled(
            batch.map((e) => bus.publish(e, { signal: AbortSignal.timeout(drainTimeoutMs) }))
          );
          const rejected = batch.filter((_, i) => results[i].status === 'rejected');
          counters.published += batch.length - rejected.length;
          pending = [...rejected, ...rest];
          if (rejected.length === 0) continue;
        }

        // Queue full, or a competing publisher took the space (NACK): wait for
        // a consumer to settle something. Registered before the headroom read.
        counters.backpressureWaits++;
        await capacity.promise;
      } catch (err) {
        throw new PublishStalledError(
          pending.length,
          `PUBLISH_STALLED: no queue capacity or broker confirm within ${drainTimeoutMs}ms (${
            err instanceof Error ? err.message : String(err)
          })`
        );
      } finally {
        capacity.cancel();
      }
    }
  }

  // ---- Global invariants ---------------------------------------------------

  /** One row per currency at most: inherently bounded by the currency count. */
  private async *globalZeroSumPages(): AsyncGenerator<GlobalZeroSumPayload[]> {
    const res = await query<{ currency: string; total: string }>(
      `SELECT currency, SUM(signed_amount_cents)::text AS total
       FROM ledger_entries
       GROUP BY currency
       HAVING SUM(signed_amount_cents) <> 0
       ORDER BY currency`
    );
    if (res.rows.length > 0) {
      yield res.rows.map((r) => ({ currency: r.currency, totalCents: r.total }));
    }
  }

  private async *unbalancedJournalPages(pageSize: number): AsyncGenerator<UnbalancedJournalPayload[]> {
    let cursorRef = '';
    let cursorCcy = '';
    for (;;) {
      const res = await query<{ transaction_ref: string; currency: string; total: string }>(
        `SELECT transaction_ref, currency, SUM(signed_amount_cents)::text AS total
         FROM ledger_entries
         WHERE (transaction_ref, currency) > ($1, $2)
         GROUP BY transaction_ref, currency
         HAVING SUM(signed_amount_cents) <> 0
         ORDER BY transaction_ref, currency
         LIMIT $3`,
        [cursorRef, cursorCcy, pageSize]
      );
      if (res.rows.length === 0) return;
      yield res.rows.map((r) => ({ transactionRef: r.transaction_ref, currency: r.currency, totalCents: r.total }));
      if (res.rows.length < pageSize) return;
      const last = res.rows[res.rows.length - 1];
      cursorRef = last.transaction_ref;
      cursorCcy = last.currency;
    }
  }

  // ---- Per-wallet invariants -----------------------------------------------

  /**
   * Full verification of every wallet's O(1) precalculated state against a
   * recomputed SUM/COUNT of its ledger rows:
   *  - unsharded: balance_cents, ledger_balance_cents and ledger_version;
   *  - sharded: the SUM / entry SUM over its balance buckets, and that it has
   *    exactly shard_count buckets.
   * This is the only place history is re-summed, and it is offline + keyset-paged.
   */
  private async *walletDriftPages(pageSize: number): AsyncGenerator<WalletDriftPayload[]> {
    let cursor = '';
    for (;;) {
      const res = await query<{
        wallet_id: string;
        currency: string;
        account_type: string;
        balance: string;
        ledger_balance: string;
        ledger_version: string;
        ledger_total: string;
        entry_count: string;
      }>(
        `SELECT w.wallet_id, w.currency, w.account_type,
                (CASE WHEN w.shard_count > 0 THEN sh.total ELSE w.balance_cents END)::text AS balance,
                (CASE WHEN w.shard_count > 0 THEN sh.total ELSE w.ledger_balance_cents END)::text AS ledger_balance,
                (CASE WHEN w.shard_count > 0 THEN sh.entries ELSE w.ledger_version END)::text AS ledger_version,
                l.total::text AS ledger_total,
                l.cnt::text AS entry_count
         FROM wallets w
         CROSS JOIN LATERAL (
           SELECT COALESCE(SUM(s.balance_cents), 0) AS total,
                  COALESCE(SUM(s.entry_count), 0) AS entries,
                  COUNT(*) AS buckets
           FROM wallet_balance_shards s
           WHERE s.wallet_id = w.wallet_id
         ) sh
         CROSS JOIN LATERAL (
           SELECT COALESCE(SUM(le.signed_amount_cents), 0) AS total, COUNT(*) AS cnt
           FROM ledger_entries le
           WHERE le.wallet_id = w.wallet_id
         ) l
         WHERE w.wallet_id > $1
           AND (
             CASE WHEN w.shard_count > 0
               THEN sh.total <> l.total OR sh.entries <> l.cnt OR sh.buckets <> w.shard_count
               ELSE w.balance_cents <> l.total
                 OR w.ledger_balance_cents <> l.total
                 OR w.ledger_version <> l.cnt
             END
           )
         ORDER BY w.wallet_id
         LIMIT $2`,
        [cursor, pageSize]
      );
      if (res.rows.length === 0) return;
      yield res.rows.map((r) => ({
        walletId: r.wallet_id,
        currency: r.currency,
        accountType: r.account_type,
        balanceCents: r.balance,
        ledgerBalanceCents: r.ledger_balance,
        ledgerVersion: r.ledger_version,
        ledgerSumCents: r.ledger_total,
        ledgerEntryCount: r.entry_count,
      }));
      if (res.rows.length < pageSize) return;
      cursor = res.rows[res.rows.length - 1].wallet_id;
    }
  }

  private async *negativeBalancePages(pageSize: number): AsyncGenerator<NegativeBalancePayload[]> {
    let cursor = '';
    for (;;) {
      const res = await query<{ wallet_id: string; currency: string; account_type: string; ledger_total: string }>(
        `SELECT w.wallet_id, w.currency, w.account_type, l.total::text AS ledger_total
         FROM wallets w
         CROSS JOIN LATERAL (
           SELECT COALESCE(SUM(le.signed_amount_cents), 0) AS total
           FROM ledger_entries le
           WHERE le.wallet_id = w.wallet_id
         ) l
         WHERE w.account_type IN ${NON_NEGATIVE_ACCOUNT_TYPES_SQL}
           AND w.wallet_id > $1
           AND l.total < 0
         ORDER BY w.wallet_id
         LIMIT $2`,
        [cursor, pageSize]
      );
      if (res.rows.length === 0) return;
      yield res.rows.map((r) => ({
        walletId: r.wallet_id,
        currency: r.currency,
        accountType: r.account_type,
        ledgerSumCents: r.ledger_total,
      }));
      if (res.rows.length < pageSize) return;
      cursor = res.rows[res.rows.length - 1].wallet_id;
    }
  }
}

export const ledgerAuditService = new LedgerAuditService();
