import crypto from 'crypto';
import type { PoolClient } from 'pg';
import { isUniqueViolation, query, withTransaction } from '../db';
import { balanceCacheKey, redis } from '../redis';
import {
  BalanceSummary,
  CreateGrantInput,
  DailyUsageReport,
  DeductUsageInput,
  DiscrepancyType,
  FundingBreakdown,
  FundingSource,
  fundingSourceFor,
  GrantSource,
  IdempotencyKeyConflictError,
  PlanTier,
  ResourceNotFoundError,
  UsageHistoryItem,
} from '../types/credits';
import { pricingCalculator } from './PricingCalculator';
import { autoRechargeService } from './AutoRechargeService';
import { CreditBurstRateLimiter, creditBurstRateLimiter } from './CreditBurstRateLimiter';
import {
  adjustStripe,
  clientQuery,
  creditNewGrant,
  ensureOrgWallet,
  loadOrgChain,
  lockFullWallet,
  OrgConfig,
  QueryFn,
  readOrgWallet,
  rebalanceReservations,
  totalsOf,
  WalletLock,
} from './OrgWallet';
import { ChainDraw, drawAlongChain, estimatedNetSpendable } from './WalletChain';

export interface DeductUsageResult {
  usageId: string;
  orgId: string;
  creditsCharged: number;
  remainingSpendableCredits: number;
  allocations: Array<{ grantId: string; creditsDeducted: number }>;
  /** An auto-recharge intent is in flight for a pool this deduction drew on (committed with it). */
  autoRechargeQueued: boolean;
  /** True when this response is a replay of an earlier request with the same idempotency key. */
  idempotentReplay: boolean;
}

export interface ExpireStaleGrantsResult {
  expiredLotsCount: number;
  expiredCreditsTotal: number;
  /** Number of EXPIRED_DURING_ACTIVE_HOLD discrepancy rows written. */
  discrepanciesLogged: number;
  /** Held credits that lost their backing because the lots they relied on expired. */
  unbackedHoldCredits: number;
}

interface RecordedDeduction {
  result: DeductUsageResult;
  /** Orgs with an auto-recharge intent in flight (kicked after commit). */
  rechargeOrgIds: string[];
}

const emptyBreakdown = (): FundingBreakdown => ({ PROMO: 0, PURCHASE: 0, AUTO_RECHARGE: 0 });

/** Aggregate per-stripe slices into per-grant allocations, keeping FIFO order of first appearance. */
function aggregateByGrant(
  rows: Array<{ grantId: string; credits: number }>
): Array<{ grantId: string; creditsDeducted: number }> {
  const totals = new Map<string, number>();
  for (const row of rows) {
    totals.set(row.grantId, (totals.get(row.grantId) ?? 0) + row.credits);
  }
  return [...totals.entries()].map(([grantId, creditsDeducted]) => ({ grantId, creditsDeducted }));
}

export class CreditLedgerService {
  constructor(private readonly limiter: CreditBurstRateLimiter = creditBurstRateLimiter) {}

  /**
   * Return an organization's credit balance summary and active credit lots ordered by expiration and priority.
   * Lots past `expires_at` are excluded even before the maintenance sweep retires them.
   */
  async getBalanceSummary(orgId: string): Promise<BalanceSummary> {
    const chain = await loadOrgChain(query, orgId);
    const wallet = await readOrgWallet(query, orgId, chain[0]);
    let poolSpendableCredits = 0;
    for (const ancestor of chain.slice(1)) {
      poolSpendableCredits += Math.max(0, (await readOrgWallet(query, ancestor.orgId, ancestor)).netSpendableCredits);
    }

    const lotsRes = await query<{
      grant_id: string;
      source: GrantSource;
      initial_credits: string | number;
      remaining_credits: string | number;
      priority: number;
      expires_at: Date;
    }>(
      `SELECT grant_id, source, initial_credits, remaining_credits, priority, expires_at
       FROM credit_grant_balances
       WHERE org_id = $1 AND lifecycle_status = 'ACTIVE' AND remaining_credits > 0 AND expires_at > NOW()
       ORDER BY expires_at ASC, priority ASC, created_at ASC, grant_id ASC`,
      [orgId]
    );

    return {
      orgId,
      planTier: wallet.config.planTier,
      totalGrantedCredits: lotsRes.rows.reduce((sum, r) => sum + Number(r.initial_credits), 0),
      availableCredits: wallet.effectiveAvailableCredits,
      reservedCredits: wallet.reservedCredits,
      netSpendableCredits: Math.max(0, wallet.netSpendableCredits),
      parentOrgId: wallet.config.parentOrgId,
      poolSpendableCredits,
      walletStripes: wallet.stripes.map((s) => ({
        stripeNo: s.stripeNo,
        availableCredits: s.availableCredits,
        reservedCredits: s.reservedCredits,
      })),
      autoRecharge: wallet.config.autoRecharge,
      activeLots: lotsRes.rows.map((r) => ({
        grantId: r.grant_id,
        source: r.source,
        remainingCredits: Number(r.remaining_credits),
        priority: r.priority,
        expiresAt: new Date(r.expires_at).toISOString(),
      })),
    };
  }

  /**
   * Issue a new credit grant lot for an organization. The grant is split evenly across the org's
   * wallet stripes under a FULL wallet lock. Pass `wallet` to join a caller's FULL lock.
   */
  async createGrant(
    input: CreateGrantInput,
    options?: { wallet?: WalletLock }
  ): Promise<{
    grantId: string;
    orgId: string;
    credits: number;
    priority: number;
    expiresAt: string;
  }> {
    const { orgId, source, credits, expiresAt, invoiceId } = input;
    const priority = input.priority ?? (source === 'PROMOTIONAL' ? 1 : 10);
    const grantId = `grant_${crypto.randomUUID()}`;

    const issue = async (lock: WalletLock) => {
      await lock.client.query(
        `INSERT INTO credit_grants (grant_id, org_id, source, invoice_id, initial_credits, priority, status, expires_at)
         VALUES ($1, $2, $3, $4, $5, $6, 'ACTIVE', $7)`,
        [grantId, orgId, source, invoiceId ?? null, credits, priority, new Date(expiresAt)]
      );
      await creditNewGrant(lock, grantId, credits);
      await rebalanceReservations(lock);
    };

    if (options?.wallet) {
      await issue(options.wallet);
    } else {
      await withTransaction(async (client) => {
        await ensureOrgWallet(client, orgId);
        await issue(await lockFullWallet(client, orgId));
      });
      await redis.del(balanceCacheKey(orgId));
    }

    return { grantId, orgId, credits, priority, expiresAt };
  }

  /**
   * Deduct metered API usage across active credit lots in earliest-expiration-first (FIFO) order.
   * Supports either explicit integer `credits` or token counts (`inputTokens`, `outputTokens`, `cachedInputTokens`).
   *
   * Metered (top-level) calls:
   *   1. replay an already-recorded idempotency key without any cost (no rate limit, no locks),
   *   2. admit the spend through the sliding-window credit burst limiter (fails open),
   *   3. in one transaction, draw own-pool-first along the org chain (sub-org, then ancestors'
   *      shared pools), each org on a single idle stripe when that is exact, FULL lock otherwise,
   *   4. record the usage event, FIFO allocations, append-only rollup deltas and - if a drawn pool
   *      fell to its auto-recharge threshold - an outbox intent, all in that transaction,
   *   5. after commit, kick the outbox relay without waiting for it.
   *
   * With `inTransaction`, an already applied chain draw (hold settlement) is recorded on the
   * caller's transaction handle: no rate limiting and no nested transaction.
   */
  async deductUsage(
    input: DeductUsageInput,
    options?: {
      holdId?: string;
      inTransaction?: { client: PoolClient; chain: OrgConfig[]; draw: ChainDraw };
    }
  ): Promise<DeductUsageResult> {
    if (options?.inTransaction) {
      const { client, chain, draw } = options.inTransaction;
      const recorded = await this.recordUsage(client, chain, input, draw.charged, draw, options.holdId ?? null);
      return recorded.result;
    }

    const { orgId, idempotencyKey } = input;
    const chain = await loadOrgChain(query, orgId);
    const config = chain[0];

    if (idempotencyKey) {
      const replay = await this.findIdempotentReplay(query, input, null);
      if (replay) return replay;
    }

    const credits = this.priceOf(input, config.planTier);
    const admission = await this.limiter.admitOrThrow(
      orgId,
      credits,
      this.limiter.limitFor(config.planTier, config.burstLimitOverride),
      idempotencyKey ? `idem:${idempotencyKey}` : undefined
    );

    let recorded: RecordedDeduction;
    try {
      recorded = await this.runMeteredDeduction(input, credits, chain);
    } catch (err) {
      await this.limiter.release(orgId, admission);
      throw err;
    }

    const { result } = recorded;
    if (result.idempotentReplay) {
      await this.limiter.release(orgId, admission);
      return result;
    }

    autoRechargeService.kickOutbox(recorded.rechargeOrgIds);
    await redis.set(balanceCacheKey(orgId), String(result.remainingSpendableCredits), 'EX', 60);
    return result;
  }

  priceOf(input: DeductUsageInput, planTier: PlanTier): number {
    if (typeof input.credits === 'number') return input.credits;
    return pricingCalculator.calculateQuote({
      modelName: input.modelName,
      inputTokens: input.inputTokens ?? 0,
      outputTokens: input.outputTokens ?? 0,
      cachedInputTokens: input.cachedInputTokens ?? 0,
      planTier,
    }).billableCredits;
  }

  private async runMeteredDeduction(
    input: DeductUsageInput,
    credits: number,
    chain: OrgConfig[]
  ): Promise<RecordedDeduction> {
    try {
      return await withTransaction((client) => this.deductInTransaction(client, input, credits, chain, false));
    } catch (err) {
      // Two requests with the same idempotency key raced (possibly on different stripes): the
      // loser's usage insert hit the unique index once the winner committed. Retry -> replay.
      if (input.idempotencyKey && isUniqueViolation(err)) {
        return withTransaction((client) => this.deductInTransaction(client, input, credits, chain, true));
      }
      throw err;
    }
  }

  private async deductInTransaction(
    client: PoolClient,
    input: DeductUsageInput,
    credits: number,
    chain: OrgConfig[],
    recheckReplay: boolean
  ): Promise<RecordedDeduction> {
    if (recheckReplay && input.idempotencyKey) {
      const replay = await this.findIdempotentReplay(clientQuery(client), input, null);
      if (replay) return { result: replay, rechargeOrgIds: [] };
    }
    const draw = await drawAlongChain(client, chain, credits, input.idempotencyKey ?? input.requestId);
    return this.recordUsage(client, chain, input, credits, draw, null);
  }

  /**
   * Record an applied chain draw: usage event, FIFO allocations, append-only daily rollup deltas and
   * auto-recharge outbox intents for drawn pools that fell to their threshold - all on `client`.
   */
  private async recordUsage(
    client: PoolClient,
    chain: OrgConfig[],
    input: DeductUsageInput,
    credits: number,
    draw: ChainDraw,
    holdId: string | null
  ): Promise<RecordedDeduction> {
    const { orgId, requestId, modelName, idempotencyKey } = input;
    const usageId = `use_${crypto.randomUUID()}`;

    // Spendable after this deduction: own pool plus what the ancestor pools could still cover.
    let remainingSpendableCredits = 0;
    const intents: Array<{ orgId: string; intentId: string }> = [];
    for (const org of chain) {
      const lock = draw.locks.get(org.orgId);
      const snapshot =
        draw.snapshots.get(org.orgId) ??
        (!lock || lock.mode === 'STRIPE' ? await readOrgWallet(clientQuery(client), org.orgId, org) : undefined);
      const net = lock ? estimatedNetSpendable(lock, snapshot) : snapshot!.netSpendableCredits;
      remainingSpendableCredits += Math.max(0, net);
      if (lock) {
        const intentId = await autoRechargeService.enqueueIntentIfNeeded(client, org, net, usageId);
        if (intentId) intents.push({ orgId: org.orgId, intentId });
      }
    }

    await client.query(
      `INSERT INTO usage_events (
         usage_id, org_id, request_id, hold_id, model_name,
         input_tokens, output_tokens, cached_input_tokens, credits_charged, idempotency_key,
         spendable_after_credits, auto_recharge_intent_id
       )
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
      [
        usageId,
        orgId,
        requestId,
        holdId,
        modelName,
        input.inputTokens ?? 0,
        input.outputTokens ?? 0,
        input.cachedInputTokens ?? 0,
        credits,
        idempotencyKey ?? null,
        remainingSpendableCredits,
        intents[0]?.intentId ?? null,
      ]
    );

    for (const [index, slice] of draw.slices.entries()) {
      await client.query(
        `INSERT INTO credit_lot_allocations (allocation_id, usage_id, grant_id, stripe_no, credits_deducted, allocation_order)
         VALUES ($1, $2, $3, $4, $5, $6)`,
        [`alloc_${crypto.randomUUID()}`, usageId, slice.grantId, slice.stripeNo, slice.credits, index]
      );
    }

    await this.appendRollupDeltas(client, orgId, modelName, draw);

    return {
      rechargeOrgIds: intents.map((i) => i.orgId),
      result: {
        usageId,
        orgId,
        creditsCharged: credits,
        remainingSpendableCredits,
        allocations: aggregateByGrant(draw.slices),
        autoRechargeQueued: intents.length > 0,
        idempotentReplay: false,
      },
    };
  }

  /**
   * Append one immutable rollup delta per (funding source, funding org) of this usage. Plain
   * INSERTs with fresh keys: no upsert, no shared row, so deductions never serialize on rollups.
   */
  private async appendRollupDeltas(
    client: PoolClient,
    orgId: string,
    modelName: string,
    draw: ChainDraw
  ): Promise<void> {
    const groups = new Map<string, { source: FundingSource; fundingOrgId: string; credits: number }>();
    for (const slice of draw.slices) {
      const source = fundingSourceFor(slice.source);
      const key = `${source}:${slice.orgId}`;
      const group = groups.get(key) ?? { source, fundingOrgId: slice.orgId, credits: 0 };
      group.credits += slice.credits;
      groups.set(key, group);
    }
    for (const group of groups.values()) {
      await client.query(
        `INSERT INTO usage_rollup_deltas (delta_id, org_id, usage_date, model_name, funding_source, funding_org_id, credits)
         VALUES ($1, $2, (NOW() AT TIME ZONE 'UTC')::date, $3, $4, $5, $6)`,
        [`urd_${crypto.randomUUID()}`, orgId, modelName, group.source, group.fundingOrgId, group.credits]
      );
    }
  }

  /**
   * Fold rollup deltas into the daily totals. Each batch is a single statement: the deltas are
   * deleted (SKIP LOCKED, so concurrent compactors split the work) and their sums upserted into
   * usage_daily_rollups atomically - readers never see a delta both pending and compacted.
   */
  async compactUsageRollups(batchSize = 5_000): Promise<{ deltasCompacted: number }> {
    let deltasCompacted = 0;
    for (;;) {
      const res = await query<{ moved: string }>(
        `WITH moved AS (
           DELETE FROM usage_rollup_deltas
           WHERE delta_id IN (
             SELECT delta_id FROM usage_rollup_deltas
             ORDER BY created_at, delta_id
             LIMIT $1
             FOR UPDATE SKIP LOCKED
           )
           RETURNING org_id, usage_date, model_name, funding_source, funding_org_id, credits, event_count
         ),
         folded AS (
           INSERT INTO usage_daily_rollups (org_id, usage_date, model_name, funding_source, funding_org_id, credits, event_count)
           SELECT org_id, usage_date, model_name, funding_source, funding_org_id, SUM(credits), SUM(event_count)
           FROM moved
           GROUP BY org_id, usage_date, model_name, funding_source, funding_org_id
           ON CONFLICT (org_id, usage_date, model_name, funding_source, funding_org_id) DO UPDATE
           SET credits = usage_daily_rollups.credits + EXCLUDED.credits,
               event_count = usage_daily_rollups.event_count + EXCLUDED.event_count,
               updated_at = NOW()
         )
         SELECT COUNT(*)::text AS moved FROM moved`,
        [batchSize]
      );
      const moved = Number(res.rows[0].moved);
      deltasCompacted += moved;
      if (moved < batchSize) return { deltasCompacted };
    }
  }

  /**
   * Look up a prior deduction made with the same idempotency key. Returns the original result,
   * or throws if the key is being reused for a different request.
   */
  private async findIdempotentReplay(
    run: QueryFn,
    input: DeductUsageInput,
    holdId: string | null
  ): Promise<DeductUsageResult | null> {
    const idempotencyKey = input.idempotencyKey as string;
    const existingRes = await run<{
      usage_id: string;
      request_id: string;
      hold_id: string | null;
      model_name: string;
      input_tokens: number;
      output_tokens: number;
      cached_input_tokens: number;
      credits_charged: string | number;
      spendable_after_credits: string | number | null;
      auto_recharge_intent_id: string | null;
    }>(
      `SELECT usage_id, request_id, hold_id, model_name, input_tokens, output_tokens,
              cached_input_tokens, credits_charged, spendable_after_credits, auto_recharge_intent_id
       FROM usage_events
       WHERE org_id = $1 AND idempotency_key = $2`,
      [input.orgId, idempotencyKey]
    );

    if (existingRes.rowCount === 0) return null;

    const existing = existingRes.rows[0];
    const samePayload =
      existing.request_id === input.requestId &&
      existing.model_name === input.modelName &&
      existing.hold_id === holdId &&
      Number(existing.input_tokens) === (input.inputTokens ?? 0) &&
      Number(existing.output_tokens) === (input.outputTokens ?? 0) &&
      Number(existing.cached_input_tokens) === (input.cachedInputTokens ?? 0) &&
      (typeof input.credits !== 'number' || Number(existing.credits_charged) === input.credits);

    if (!samePayload) {
      throw new IdempotencyKeyConflictError(idempotencyKey);
    }

    const allocRes = await run<{ grant_id: string; credits_deducted: string | number }>(
      `SELECT grant_id, credits_deducted
       FROM credit_lot_allocations
       WHERE usage_id = $1
       ORDER BY allocation_order ASC`,
      [existing.usage_id]
    );

    return {
      usageId: existing.usage_id,
      orgId: input.orgId,
      creditsCharged: Number(existing.credits_charged),
      remainingSpendableCredits: Number(existing.spendable_after_credits ?? 0),
      allocations: aggregateByGrant(
        allocRes.rows.map((a) => ({ grantId: a.grant_id, credits: Number(a.credits_deducted) }))
      ),
      autoRechargeQueued: existing.auto_recharge_intent_id !== null,
      idempotentReplay: true,
    };
  }

  /**
   * Fetch recent usage ledger events and their FIFO lot allocations for an organization.
   */
  async listUsageHistory(orgId: string, limit = 20): Promise<UsageHistoryItem[]> {
    const eventsRes = await query<{
      usage_id: string;
      org_id: string;
      request_id: string;
      hold_id: string | null;
      model_name: string;
      input_tokens: number;
      output_tokens: number;
      cached_input_tokens: number;
      credits_charged: string | number;
      created_at: Date;
    }>(
      `SELECT
         usage_id, org_id, request_id, hold_id, model_name,
         input_tokens, output_tokens, cached_input_tokens, credits_charged, created_at
       FROM usage_events
       WHERE org_id = $1
       ORDER BY created_at DESC
       LIMIT $2`,
      [orgId, limit]
    );

    if (eventsRes.rowCount === 0) {
      return [];
    }

    const usageIds = eventsRes.rows.map((r) => r.usage_id);
    const allocRes = await query<{
      usage_id: string;
      grant_id: string;
      credits_deducted: string | number;
    }>(
      `SELECT usage_id, grant_id, credits_deducted
       FROM credit_lot_allocations
       WHERE usage_id = ANY($1::varchar[])
       ORDER BY created_at ASC, allocation_order ASC`,
      [usageIds]
    );

    const slicesByUsage = new Map<string, Array<{ grantId: string; credits: number }>>();
    for (const a of allocRes.rows) {
      const list = slicesByUsage.get(a.usage_id) ?? [];
      list.push({ grantId: a.grant_id, credits: Number(a.credits_deducted) });
      slicesByUsage.set(a.usage_id, list);
    }

    return eventsRes.rows.map((e) => ({
      usageId: e.usage_id,
      orgId: e.org_id,
      requestId: e.request_id,
      holdId: e.hold_id,
      modelName: e.model_name,
      inputTokens: e.input_tokens,
      outputTokens: e.output_tokens,
      cachedInputTokens: e.cached_input_tokens,
      creditsCharged: Number(e.credits_charged),
      createdAt: new Date(e.created_at).toISOString(),
      allocations: aggregateByGrant(slicesByUsage.get(e.usage_id) ?? []),
    }));
  }

  /**
   * Credits consumed per UTC day, broken down by model, funding source and funding org (own pool
   * vs. ancestor pools). One statement over compacted totals plus not-yet-compacted deltas (no
   * per-day / per-event fan-out); the LEFT JOIN against org_balances distinguishes "unknown org"
   * (no rows) from "no usage".
   */
  async getDailyUsage(orgId: string, from: string, to: string): Promise<DailyUsageReport> {
    const res = await query<{
      usage_date: string | null;
      model_name: string | null;
      funding_source: FundingSource | null;
      funding_org_id: string | null;
      credits: string | null;
      events: string | null;
    }>(
      `SELECT u.usage_date::text AS usage_date, u.model_name, u.funding_source, u.funding_org_id,
              SUM(u.credits)::text AS credits, SUM(u.event_count)::text AS events
       FROM org_balances o
       LEFT JOIN (
         SELECT org_id, usage_date, model_name, funding_source, funding_org_id, credits, event_count
         FROM usage_daily_rollups
         WHERE org_id = $1 AND usage_date BETWEEN $2::date AND $3::date
         UNION ALL
         SELECT org_id, usage_date, model_name, funding_source, funding_org_id, credits, event_count
         FROM usage_rollup_deltas
         WHERE org_id = $1 AND usage_date BETWEEN $2::date AND $3::date
       ) u ON u.org_id = o.org_id
       WHERE o.org_id = $1
       GROUP BY u.usage_date, u.model_name, u.funding_source, u.funding_org_id
       ORDER BY u.usage_date, u.model_name, u.funding_source, u.funding_org_id`,
      [orgId, from, to]
    );

    if (res.rowCount === 0) {
      throw new ResourceNotFoundError('Organization', orgId);
    }

    const report: DailyUsageReport = {
      orgId,
      from,
      to,
      totalCredits: 0,
      bySource: emptyBreakdown(),
      pooledCredits: 0,
      days: [],
    };

    for (const row of res.rows) {
      if (!row.usage_date || !row.model_name || !row.funding_source || !row.funding_org_id) continue;
      const credits = Number(row.credits);
      const events = Number(row.events);
      const pooled = row.funding_org_id !== orgId;

      let day = report.days[report.days.length - 1];
      if (!day || day.date !== row.usage_date) {
        day = {
          date: row.usage_date,
          totalCredits: 0,
          bySource: emptyBreakdown(),
          byFundingOrg: {},
          models: [],
        };
        report.days.push(day);
      }
      let model = day.models[day.models.length - 1];
      if (!model || model.modelName !== row.model_name) {
        model = {
          modelName: row.model_name,
          totalCredits: 0,
          pooledCredits: 0,
          bySource: emptyBreakdown(),
          eventsBySource: emptyBreakdown(),
        };
        day.models.push(model);
      }

      model.totalCredits += credits;
      model.bySource[row.funding_source] += credits;
      model.eventsBySource[row.funding_source] += events;
      if (pooled) model.pooledCredits += credits;
      day.totalCredits += credits;
      day.bySource[row.funding_source] += credits;
      day.byFundingOrg[row.funding_org_id] = (day.byFundingOrg[row.funding_org_id] ?? 0) + credits;
      report.totalCredits += credits;
      report.bySource[row.funding_source] += credits;
      if (pooled) report.pooledCredits += credits;
    }

    return report;
  }

  /**
   * Expire credit lots whose `expires_at` has passed.
   *
   * Each organization is processed in its own transaction under a FULL wallet lock:
   *   - expired grants are marked EXPIRED and their per-stripe lots zeroed,
   *   - every stripe's available credits are reconciled to its remaining non-expired lots
   *     (instead of subtracting, so a drifted counter can never be pushed below zero),
   *   - if active holds now reserve more than the non-expired lots can cover, the newly created
   *     shortfall is logged as EXPIRED_DURING_ACTIVE_HOLD billing discrepancies,
   *   - free credits are rebalanced into stripes whose reservations lost their backing.
   * Reservations are deliberately left untouched: they must keep matching the active holds.
   */
  async expireStaleGrants(): Promise<ExpireStaleGrantsResult> {
    const orgsRes = await query<{ org_id: string }>(
      `SELECT DISTINCT org_id
       FROM credit_grants
       WHERE status = 'ACTIVE' AND expires_at <= NOW()
       ORDER BY org_id`
    );

    const totals: ExpireStaleGrantsResult = {
      expiredLotsCount: 0,
      expiredCreditsTotal: 0,
      discrepanciesLogged: 0,
      unbackedHoldCredits: 0,
    };

    for (const { org_id: orgId } of orgsRes.rows) {
      const orgResult = await withTransaction(async (client) =>
        this.expireOrgGrantsLocked(await lockFullWallet(client, orgId))
      );
      totals.expiredLotsCount += orgResult.expiredLotsCount;
      totals.expiredCreditsTotal += orgResult.expiredCreditsTotal;
      totals.discrepanciesLogged += orgResult.discrepanciesLogged;
      totals.unbackedHoldCredits += orgResult.unbackedHoldCredits;
      if (orgResult.expiredLotsCount > 0) {
        await redis.del(balanceCacheKey(orgId));
      }
    }

    return totals;
  }

  private async expireOrgGrantsLocked(lock: WalletLock): Promise<ExpireStaleGrantsResult> {
    const { client } = lock;
    const orgId = lock.config.orgId;
    const result: ExpireStaleGrantsResult = {
      expiredLotsCount: 0,
      expiredCreditsTotal: 0,
      discrepanciesLogged: 0,
      unbackedHoldCredits: 0,
    };

    // Re-checked under the FULL lock: a concurrent sweep may already have expired these grants.
    const expiredRes = await client.query<{ grant_id: string; invoice_id: string | null }>(
      `SELECT grant_id, invoice_id
       FROM credit_grants
       WHERE org_id = $1 AND status = 'ACTIVE' AND expires_at <= NOW()
       ORDER BY expires_at ASC, priority ASC, created_at ASC, grant_id ASC
       FOR UPDATE`,
      [orgId]
    );
    if (expiredRes.rowCount === 0) return result;
    const expiredIds = expiredRes.rows.map((g) => g.grant_id);

    const lotTotals = await client.query<{ grant_id: string; stripe_no: number; remaining: string; expired: boolean }>(
      `SELECT l.grant_id, l.stripe_no, l.remaining_credits::text AS remaining,
              (l.grant_id = ANY($2::varchar[])) AS expired
       FROM credit_lots l
       JOIN credit_grants g ON g.grant_id = l.grant_id
       WHERE l.org_id = $1 AND g.status = 'ACTIVE'`,
      [orgId, expiredIds]
    );

    const expiredByGrant = new Map<string, number>();
    const liveByStripe = new Map<number, number>();
    let expiredCredits = 0;
    let liveCredits = 0;
    for (const lot of lotTotals.rows) {
      const credits = Math.max(0, Number(lot.remaining));
      if (lot.expired) {
        expiredByGrant.set(lot.grant_id, (expiredByGrant.get(lot.grant_id) ?? 0) + credits);
        expiredCredits += credits;
      } else {
        liveByStripe.set(Number(lot.stripe_no), (liveByStripe.get(Number(lot.stripe_no)) ?? 0) + credits);
        liveCredits += credits;
      }
    }

    await client.query(
      `UPDATE credit_lots SET remaining_credits = 0, updated_at = NOW() WHERE grant_id = ANY($1::varchar[])`,
      [expiredIds]
    );
    await client.query(
      `UPDATE credit_grants SET status = 'EXPIRED', updated_at = NOW() WHERE grant_id = ANY($1::varchar[])`,
      [expiredIds]
    );

    for (const stripe of [...lock.stripes.values()].sort((a, b) => a.stripeNo - b.stripeNo)) {
      const live = liveByStripe.get(stripe.stripeNo) ?? 0;
      await adjustStripe(lock, stripe.stripeNo, { available: live - stripe.availableCredits });
      stripe.unsweptExpiredCredits = 0;
    }

    result.expiredLotsCount = expiredIds.length;
    result.expiredCreditsTotal = expiredCredits;

    // Only the shortfall introduced by *this* expiry is logged, so a hold that was already
    // under-backed (and logged by an earlier sweep) is not double counted.
    const reserved = totalsOf(lock.stripes.values()).reservedCredits;
    const uncoveredBefore = Math.max(0, reserved - (liveCredits + expiredCredits));
    const uncoveredAfter = Math.max(0, reserved - liveCredits);
    let shortfall = uncoveredAfter - uncoveredBefore;

    if (shortfall > 0) {
      result.unbackedHoldCredits = shortfall;
      const holdIds = await this.activeHoldIds(lock);
      for (const grant of expiredRes.rows) {
        if (shortfall <= 0) break;
        const unrecovered = Math.min(expiredByGrant.get(grant.grant_id) ?? 0, shortfall);
        if (unrecovered <= 0) continue;
        await this.logDiscrepancy(lock, {
          type: 'EXPIRED_DURING_ACTIVE_HOLD',
          invoiceId: grant.invoice_id,
          grantId: grant.grant_id,
          unrecovered,
          reason:
            `Credit lot expired while active holds reserved ${reserved} credits; ` +
            `non-expired lots cover only ${liveCredits}. Active holds: ${holdIds}`,
        });
        result.discrepanciesLogged += 1;
        shortfall -= unrecovered;
      }
    }

    await rebalanceReservations(lock);
    return result;
  }

  async activeHoldIds(lock: WalletLock): Promise<string> {
    // Holds with a reservation on this org's pool: its own holds and sub-orgs' cascaded portions.
    const holdsRes = await lock.client.query<{ hold_id: string }>(
      `SELECT h.hold_id
       FROM credit_holds h
       WHERE h.status = 'ACTIVE'
         AND EXISTS (SELECT 1 FROM credit_hold_portions p WHERE p.hold_id = h.hold_id AND p.org_id = $1)
       ORDER BY h.created_at ASC, h.hold_id ASC`,
      [lock.config.orgId]
    );
    return holdsRes.rows.map((h) => h.hold_id).join(', ') || 'none';
  }

  async logDiscrepancy(
    lock: WalletLock,
    entry: {
      type: DiscrepancyType;
      unrecovered: number;
      reason: string;
      invoiceId?: string | null;
      grantId?: string | null;
      holdId?: string | null;
      usageId?: string | null;
    }
  ): Promise<void> {
    await lock.client.query(
      `INSERT INTO billing_discrepancies (
         discrepancy_id, org_id, discrepancy_type, invoice_id, grant_id, hold_id, usage_id, unrecovered_credits, reason
       )
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
      [
        `disc_${crypto.randomUUID()}`,
        lock.config.orgId,
        entry.type,
        entry.invoiceId ?? null,
        entry.grantId ?? null,
        entry.holdId ?? null,
        entry.usageId ?? null,
        entry.unrecovered,
        entry.reason,
      ]
    );
  }
}

export const creditLedgerService = new CreditLedgerService();
