import crypto from 'crypto';
import type { PoolClient } from 'pg';
import { isUniqueViolation, query, runInTransaction, withTransaction } from '../db';
import { balanceCacheKey, holdRedisKey, redis } from '../redis';
import { config } from '../config/env';
import {
  CreateStreamHoldInput,
  ExtendStreamHoldInput,
  HoldStateConflictError,
  HoldStatus,
  InsufficientCreditsError,
  ReleaseStreamHoldInput,
  ResourceNotFoundError,
  SettleStreamHoldInput,
} from '../types/credits';
import { autoRechargeService } from './AutoRechargeService';
import { CreditBurstRateLimiter, creditBurstRateLimiter } from './CreditBurstRateLimiter';
import { CreditLedgerService, creditLedgerService } from './CreditLedgerService';
import {
  adjustStripe,
  applyDraw,
  clientQuery,
  DrawSlice,
  headroomOf,
  loadOrgChain,
  lockFullWallet,
  lockStripes,
  moveFreeCredits,
  OrgConfig,
  OrgWalletSnapshot,
  planDraw,
  readOrgWallet,
  totalsOf,
  WalletLock,
  withStripeOrFullLock,
} from './OrgWallet';
import { ChainDraw, drawAlongChain, estimatedNetSpendable, reserveAlongChain } from './WalletChain';
import { pricingCalculator } from './PricingCalculator';

interface LockedHold {
  hold_id: string;
  org_id: string;
  home_stripe_no: number;
  request_id: string;
  model_name: string;
  estimated_credits: string | number;
  status: HoldStatus;
  expires_at: Date;
  is_expired: boolean;
}

interface Portion {
  seq: number;
  orgId: string;
  stripeNo: number;
  credits: number;
}

type HoldAction = 'extend' | 'release' | 'settle';

type Apply<T> = () => Promise<T>;

/** State available to a hold transition once the consumer org is locked. */
interface HoldContext {
  client: PoolClient;
  chain: OrgConfig[];
  /** Consumer-org lock: its home stripe (STRIPE) or every stripe (FULL). */
  lock: WalletLock;
  hold: LockedHold;
  /** Reservation portions in acquisition order (consumer first, then ancestors). */
  portions: Portion[];
}

interface HoldTransitionHandlers<T> {
  /** Consumer org locked on the hold's home stripe only. Return null to escalate to FULL. */
  fast: (ctx: HoldContext) => Promise<Apply<T> | null>;
  /** Consumer org locked FULL. */
  full: (ctx: HoldContext) => Promise<T>;
}

export interface SettleStreamHoldResult {
  holdId: string;
  orgId: string;
  status: 'SETTLED';
  estimatedCredits: number;
  actualCredits: number;
  /** Credits actually charged; lower than `actualCredits` when the hold could not fund its usage. */
  chargedCredits: number;
  /** Usage that could not be charged without touching other holds' reservations (logged as a discrepancy). */
  shortfallCredits: number;
  /** Credits charged to ancestor pools (after the org's own reservation and free credits). */
  pooledCredits: number;
  usageId: string | null;
}

/**
 * Run a hold transition inside a transaction the caller owns (e.g. an integration consumer that
 * records its checkpoint and outbox event atomically with the transition). Post-commit side effects
 * (cache invalidation, auto-recharge relay kicks) are handed to `afterCommit` instead of running
 * before the caller's COMMIT.
 */
export interface JoinedTransaction {
  client: PoolClient;
  afterCommit: (effect: () => Promise<void> | void) => void;
}

/** Pseudo model of credit holds that fund an order (not metered against a model rate card). */
export const ORDER_CREDIT_MODEL = 'order-credit';

/** request_id under which the credit hold of an order is stored. */
export function orderCreditRequestId(orderId: string): string {
  return `order:${orderId}`;
}

export interface CreateOrderCreditHoldInput {
  orgId: string;
  orderId: string;
  credits: number;
  ttlSeconds?: number;
  /**
   * Idempotency key of the reservation (e.g. `checkout:<id>:credits` from the checkout
   * coordinator). The same key always resolves to the same hold; a key that was released (or
   * cancelled before the hold existed) can never place a hold again.
   */
  holdKey?: string;
}

export interface OrderCreditHoldView {
  holdId: string;
  orgId: string;
  orderId: string;
  estimatedCredits: number;
  status: HoldStatus;
  expiresAt: string;
  created: boolean;
  holdKey?: string;
}

export interface ReleaseOrderCreditHoldInput {
  orderId: string;
  holdKey: string;
  reason: string;
}

export interface ReleaseOrderCreditHoldResult {
  holdKey: string;
  orderId: string;
  /** RELEASED: the hold was active and is released now. ALREADY_RELEASED: nothing left to do.
   *  TOMBSTONED: no hold existed for the key; the key is cancelled so it can never place one. */
  outcome: 'RELEASED' | 'ALREADY_RELEASED' | 'TOMBSTONED';
  holdId: string | null;
  releasedCredits: number;
}

/** A checkout released (or cancelled) this reservation key: it can never place a hold again. */
export class OrderCreditHoldCancelledError extends Error {
  public readonly statusCode = 409;
  public readonly code = 'HOLD_CANCELLED';
  constructor(
    public readonly holdKey: string,
    public readonly reason: string | null
  ) {
    super(`Order credit hold ${holdKey} was cancelled${reason ? `: ${reason}` : ''}`);
    this.name = 'OrderCreditHoldCancelledError';
  }
}

/** Internal: another transaction claimed the hold key first (the reservation rolls back). */
class HoldKeyTakenError extends Error {}

interface HoldKeyRow {
  hold_key: string;
  order_id: string;
  org_id: string | null;
  credits: string | number | null;
  hold_id: string | null;
  status: 'PLACED' | 'CANCELLED';
  reason: string | null;
}

const portionsOn = (portions: Portion[], orgId: string) => portions.filter((p) => p.orgId === orgId);

/**
 * Streaming pre-authorization hold lifecycle on the striped, hierarchical wallet.
 *
 * A hold's reservation is a list of portions: the consumer's own portion on its home stripe, then
 * any deficit cascaded to ancestor pools. Transitions lock the consumer first (home stripe, or FULL
 * on escalation), then the hold row, then the ancestors' portion stripes in chain order - the
 * canonical lock order - and only then unwind portions LIFO (the latest cascaded portion is released
 * first). So parent deductions, children cascading holds into the parent and settlements unwinding
 * them can run concurrently without deadlocks.
 *
 * Invariants: per stripe, reserved_credits == SUM(portions of ACTIVE holds on the stripe);
 * settling a hold never consumes credits reserved by other holds, in any pool.
 */
export class StreamHoldService {
  constructor(
    private readonly ledger: CreditLedgerService = creditLedgerService,
    private readonly limiter: CreditBurstRateLimiter = creditBurstRateLimiter
  ) {}

  /**
   * Place a pre-authorization credit hold before starting a streaming inference request.
   * Reserves own-pool-first and cascades any deficit to ancestor pools. Counts against the org's
   * credit burst limit.
   */
  async createStreamHold(input: CreateStreamHoldInput): Promise<{
    holdId: string;
    orgId: string;
    estimatedCredits: number;
    status: 'ACTIVE';
    expiresAt: string;
  }> {
    // Validate model against catalog before touching the database.
    pricingCalculator.getRateCard(input.modelName);
    return this.placeHold(input);
  }

  /**
   * Reserve credits that will fund (part of) an order: captured when the order's payment succeeds,
   * released when it fails (see integration/OrderCreditSettlementService.ts). Same reservation
   * mechanics as a stream hold (own pool first, deficit cascaded to ancestor pools, burst limit).
   * An order has at most one ACTIVE credit hold (enforced by a unique index): placing it again
   * returns the existing hold, so a retried checkout never reserves twice.
   */
  async createOrderCreditHold(input: CreateOrderCreditHoldInput): Promise<OrderCreditHoldView> {
    const requestId = orderCreditRequestId(input.orderId);
    if (input.holdKey) return this.createKeyedOrderCreditHold(input, requestId);

    const existing = await this.activeOrderHold(requestId);
    if (existing) return this.describeExistingOrderHold(existing, input);
    try {
      const hold = await this.placeHold({
        orgId: input.orgId,
        requestId,
        modelName: ORDER_CREDIT_MODEL,
        estimatedCredits: input.credits,
        ttlSeconds: input.ttlSeconds ?? config.orderCredits.holdTtlSeconds,
      });
      return { ...hold, orderId: input.orderId, created: true };
    } catch (err) {
      // A concurrent placement for the same order won the unique index.
      if (!isUniqueViolation(err)) throw err;
      const winner = await this.activeOrderHold(requestId);
      if (!winner) throw err;
      return this.describeExistingOrderHold(winner, input);
    }
  }

  /** Keyed placement: exactly one hold per key, atomically claimed with the reservation. */
  private async createKeyedOrderCreditHold(input: CreateOrderCreditHoldInput, requestId: string): Promise<OrderCreditHoldView> {
    const holdKey = input.holdKey as string;
    const known = await this.holdKeyRow(holdKey);
    if (known) return this.describeKeyedHold(known, input);
    try {
      const hold = await this.placeHold(
        {
          orgId: input.orgId,
          requestId,
          modelName: ORDER_CREDIT_MODEL,
          estimatedCredits: input.credits,
          ttlSeconds: input.ttlSeconds ?? config.orderCredits.holdTtlSeconds,
        },
        async (client, holdId) => {
          const claimed = await client.query(
            `INSERT INTO order_credit_hold_keys (hold_key, order_id, org_id, credits, hold_id, status)
             VALUES ($1, $2, $3, $4, $5, 'PLACED')
             ON CONFLICT (hold_key) DO NOTHING
             RETURNING hold_key`,
            [holdKey, input.orderId, input.orgId, input.credits, holdId]
          );
          if (claimed.rowCount === 0) throw new HoldKeyTakenError();
        }
      );
      return { ...hold, orderId: input.orderId, created: true, holdKey };
    } catch (err) {
      if (err instanceof HoldKeyTakenError) {
        // A concurrent placement with the same key, or a release that came first (tombstone).
        const row = await this.holdKeyRow(holdKey);
        if (row) return this.describeKeyedHold(row, input);
      }
      if (isUniqueViolation(err)) {
        const row = await this.holdKeyRow(holdKey);
        if (row) return this.describeKeyedHold(row, input);
        const winner = await this.activeOrderHold(requestId);
        throw new HoldStateConflictError(
          winner?.hold_id ?? holdKey,
          `Order ${input.orderId} already has an active credit hold under another reservation`
        );
      }
      throw err;
    }
  }

  private async holdKeyRow(holdKey: string): Promise<HoldKeyRow | null> {
    const res = await query<HoldKeyRow>(`SELECT * FROM order_credit_hold_keys WHERE hold_key = $1`, [holdKey]);
    return res.rows[0] ?? null;
  }

  private async describeKeyedHold(row: HoldKeyRow, input: CreateOrderCreditHoldInput): Promise<OrderCreditHoldView> {
    if (row.status === 'CANCELLED' || !row.hold_id) throw new OrderCreditHoldCancelledError(row.hold_key, row.reason);
    if (row.order_id !== input.orderId || row.org_id !== input.orgId || Number(row.credits) !== input.credits) {
      throw new HoldStateConflictError(
        row.hold_id,
        `Reservation ${row.hold_key} was placed for order ${row.order_id}, ${row.credits} credits of ${row.org_id}`
      );
    }
    const hold = await query<{ status: HoldStatus; expires_at: Date; estimated_credits: string | number }>(
      `SELECT status, expires_at, estimated_credits FROM credit_holds WHERE hold_id = $1`,
      [row.hold_id]
    );
    return {
      holdId: row.hold_id,
      orgId: row.org_id as string,
      orderId: row.order_id,
      estimatedCredits: Number(hold.rows[0].estimated_credits),
      status: hold.rows[0].status,
      expiresAt: new Date(hold.rows[0].expires_at).toISOString(),
      created: false,
      holdKey: row.hold_key,
    };
  }

  /**
   * Release the credit hold of a reservation key (checkout cancelled, expired, or its other tender
   * failed). Idempotent, and safe against a placement that is still in flight: a key that has no
   * hold yet is cancelled (tombstone), so the late placement is refused instead of leaving an
   * orphan hold. A hold that was already captured cannot be released (409).
   */
  async releaseOrderCreditHold(input: ReleaseOrderCreditHoldInput): Promise<ReleaseOrderCreditHoldResult> {
    const afterCommit: Array<() => Promise<void> | void> = [];
    const result = await withTransaction(async (client): Promise<ReleaseOrderCreditHoldResult> => {
      const tombstone = await client.query(
        `INSERT INTO order_credit_hold_keys (hold_key, order_id, status, reason)
         VALUES ($1, $2, 'CANCELLED', $3)
         ON CONFLICT (hold_key) DO NOTHING
         RETURNING hold_key`,
        [input.holdKey, input.orderId, input.reason]
      );
      const base = { holdKey: input.holdKey, orderId: input.orderId };
      if ((tombstone.rowCount ?? 0) > 0) return { ...base, outcome: 'TOMBSTONED', holdId: null, releasedCredits: 0 };

      const rowRes = await client.query<HoldKeyRow>(`SELECT * FROM order_credit_hold_keys WHERE hold_key = $1 FOR UPDATE`, [
        input.holdKey,
      ]);
      const row = rowRes.rows[0];
      if (row.order_id !== input.orderId) {
        throw new HoldStateConflictError(row.hold_id ?? row.hold_key, `Reservation ${row.hold_key} belongs to order ${row.order_id}`);
      }
      if (row.status === 'CANCELLED' || !row.hold_id) return { ...base, outcome: 'ALREADY_RELEASED', holdId: row.hold_id, releasedCredits: 0 };

      const hold = await client.query<{ status: HoldStatus }>(`SELECT status FROM credit_holds WHERE hold_id = $1`, [row.hold_id]);
      const status = hold.rows[0].status;
      if (status === 'SETTLED') {
        throw new HoldStateConflictError(row.hold_id, 'Cannot release: the order credits were already captured');
      }
      let releasedCredits = 0;
      if (status === 'ACTIVE') {
        try {
          const released = await this.releaseStreamHold(
            { holdId: row.hold_id, reason: input.reason },
            { client, afterCommit: (fn) => afterCommit.push(fn) }
          );
          releasedCredits = released.releasedCredits;
        } catch (err) {
          // Past its TTL: the expiry just returned the credits to the pool.
          if (!(err instanceof HoldStateConflictError)) throw err;
        }
      }
      await client.query(
        `UPDATE order_credit_hold_keys SET status = 'CANCELLED', reason = $2, updated_at = NOW() WHERE hold_key = $1`,
        [input.holdKey, input.reason]
      );
      return { ...base, outcome: status === 'ACTIVE' ? 'RELEASED' : 'ALREADY_RELEASED', holdId: row.hold_id, releasedCredits };
    });
    for (const effect of afterCommit) await effect();
    return result;
  }

  private async activeOrderHold(requestId: string) {
    const res = await query<{ hold_id: string; org_id: string; estimated_credits: string | number; expires_at: Date }>(
      `SELECT hold_id, org_id, estimated_credits, expires_at FROM credit_holds
       WHERE request_id = $1 AND model_name = $2 AND status = 'ACTIVE'`,
      [requestId, ORDER_CREDIT_MODEL]
    );
    return res.rows[0] ?? null;
  }

  private describeExistingOrderHold(
    existing: { hold_id: string; org_id: string; estimated_credits: string | number; expires_at: Date },
    input: CreateOrderCreditHoldInput
  ) {
    if (existing.org_id !== input.orgId || Number(existing.estimated_credits) !== input.credits) {
      throw new HoldStateConflictError(
        existing.hold_id,
        `Order ${input.orderId} already has an active credit hold of ${existing.estimated_credits} credits for ${existing.org_id}`
      );
    }
    return {
      holdId: existing.hold_id,
      orgId: existing.org_id,
      orderId: input.orderId,
      estimatedCredits: Number(existing.estimated_credits),
      status: 'ACTIVE' as HoldStatus,
      expiresAt: new Date(existing.expires_at).toISOString(),
      created: false,
    };
  }

  private async placeHold(
    input: CreateStreamHoldInput,
    onReserved?: (client: PoolClient, holdId: string) => Promise<void>
  ): Promise<{
    holdId: string;
    orgId: string;
    estimatedCredits: number;
    status: 'ACTIVE';
    expiresAt: string;
  }> {
    const { orgId, requestId, modelName, estimatedCredits } = input;
    const ttlSeconds = input.ttlSeconds ?? config.defaultHoldTtlSeconds;
    const chain = await loadOrgChain(query, orgId);

    const holdId = `hold_${crypto.randomUUID()}`;
    const expiresAt = new Date(Date.now() + ttlSeconds * 1000);

    const admission = await this.limiter.admitOrThrow(
      orgId,
      estimatedCredits,
      this.limiter.limitFor(chain[0].planTier, chain[0].burstLimitOverride),
      `hold:${holdId}`
    );

    let rechargeOrgIds: string[];
    try {
      rechargeOrgIds = await withTransaction(async (client) => {
        const reservation = await reserveAlongChain(client, chain, estimatedCredits, holdId);
        await client.query(
          `INSERT INTO credit_holds (hold_id, org_id, home_stripe_no, request_id, model_name, estimated_credits, status, expires_at)
           VALUES ($1, $2, $3, $4, $5, $6, 'ACTIVE', $7)`,
          [holdId, orgId, reservation.homeStripe, requestId, modelName, estimatedCredits, expiresAt]
        );
        await this.insertPortions(client, holdId, 0, reservation.portions);
        await onReserved?.(client, holdId);
        return this.enqueueRecharges(client, chain, reservation.locks, reservation.snapshots);
      });
    } catch (err) {
      await this.limiter.release(orgId, admission);
      throw err;
    }

    autoRechargeService.kickOutbox(rechargeOrgIds);
    await redis.set(
      holdRedisKey(holdId),
      JSON.stringify({ holdId, orgId, estimatedCredits }),
      'EX',
      Math.max(1, Math.ceil(ttlSeconds))
    );

    return {
      holdId,
      orgId,
      estimatedCredits,
      status: 'ACTIVE',
      expiresAt: expiresAt.toISOString(),
    };
  }

  /**
   * Extend the TTL and optionally increase reserved credits for a long-running reasoning stream.
   * Additional credits count against the org's credit burst limit and are reserved own-pool-first
   * (on the hold's home stripe), cascading any deficit to ancestor pools.
   */
  async extendStreamHold(input: ExtendStreamHoldInput): Promise<{
    holdId: string;
    orgId: string;
    estimatedCredits: number;
    expiresAt: string;
    status: 'ACTIVE';
  }> {
    const { holdId, additionalTtlSeconds } = input;
    const additionalCredits = input.additionalCredits ?? 0;
    const orgId = await this.holdOrgId(holdId);
    const chain = await loadOrgChain(query, orgId);

    const admission =
      additionalCredits > 0
        ? await this.limiter.admitOrThrow(
            orgId,
            additionalCredits,
            this.limiter.limitFor(chain[0].planTier, chain[0].burstLimitOverride),
            `extend:${holdId}:${crypto.randomUUID()}`
          )
        : null;

    const finish = async (ctx: HoldContext, newPortions: Portion[]) => {
      const nextEstimated = Number(ctx.hold.estimated_credits) + additionalCredits;
      const baseTime = Math.max(Date.now(), new Date(ctx.hold.expires_at).getTime());
      const nextExpiresAt = new Date(baseTime + additionalTtlSeconds * 1000);
      await this.insertPortions(ctx.client, holdId, nextSeq(ctx.portions), newPortions);
      await ctx.client.query(
        `UPDATE credit_holds SET estimated_credits = $1, expires_at = $2, updated_at = NOW() WHERE hold_id = $3`,
        [nextEstimated, nextExpiresAt, holdId]
      );
      return {
        holdId,
        orgId,
        estimatedCredits: nextEstimated,
        expiresAt: nextExpiresAt.toISOString(),
        status: 'ACTIVE' as const,
      };
    };

    let rechargeOrgIds: string[] = [];
    try {
      const result = await this.runHoldTransition(holdId, 'extend', chain, {
        fast: async (ctx) => {
          const home = ctx.lock.stripes.get(ctx.hold.home_stripe_no)!;
          if (additionalCredits > 0 && headroomOf(home) < additionalCredits) return null;
          return async () => {
            if (additionalCredits > 0) {
              await adjustStripe(ctx.lock, home.stripeNo, { reserved: additionalCredits });
            }
            const out = await finish(
              ctx,
              additionalCredits > 0
                ? [{ seq: 0, orgId, stripeNo: home.stripeNo, credits: additionalCredits }]
                : []
            );
            rechargeOrgIds = await this.enqueueRecharges(ctx.client, chain, new Map([[orgId, ctx.lock]]), new Map());
            return out;
          };
        },
        full: async (ctx) => {
          // Own pool first: everything the consumer can free up, gathered onto the home stripe.
          const home = ctx.hold.home_stripe_no;
          const own = Math.min(
            additionalCredits,
            Math.max(0, totalsOf(ctx.lock.stripes.values()).netSpendableCredits)
          );
          const newPortions: Portion[] = [];
          if (own > 0) {
            const missing = own - headroomOf(ctx.lock.stripes.get(home)!);
            if (missing > 0) await moveFreeCredits(ctx.lock, home, missing);
            await adjustStripe(ctx.lock, home, { reserved: own });
            newPortions.push({ seq: 0, orgId, stripeNo: home, credits: own });
          }
          const locks = new Map([[orgId, ctx.lock]]);
          const snapshots = new Map<string, OrgWalletSnapshot>();
          const deficit = additionalCredits - own;
          if (deficit > 0) {
            if (chain.length === 1) {
              throw new InsufficientCreditsError(orgId, additionalCredits, own);
            }
            // Cascade the deficit to the ancestors (locked after the consumer: canonical order).
            const cascaded = await reserveAlongChain(ctx.client, chain.slice(1), deficit, holdId, {
              error: { orgId, requested: additionalCredits, covered: own },
            });
            newPortions.push(...cascaded.portions.map((p) => ({ seq: 0, ...p })));
            cascaded.locks.forEach((lock, id) => locks.set(id, lock));
            cascaded.snapshots.forEach((snap, id) => snapshots.set(id, snap));
          }
          const out = await finish(ctx, newPortions);
          rechargeOrgIds = await this.enqueueRecharges(ctx.client, chain, locks, snapshots);
          return out;
        },
      });
      autoRechargeService.kickOutbox(rechargeOrgIds);
      return result;
    } catch (err) {
      if (admission) await this.limiter.release(orgId, admission);
      throw err;
    }
  }

  /**
   * Explicitly release an active streaming hold when a client aborts before completion.
   */
  async releaseStreamHold(
    input: ReleaseStreamHoldInput,
    joined?: JoinedTransaction
  ): Promise<{
    holdId: string;
    orgId: string;
    releasedCredits: number;
    status: 'RELEASED';
  }> {
    const { holdId } = input;

    const release = async (ctx: HoldContext) => {
      await this.unwindPortions(ctx, 'RELEASED');
      await ctx.client.query(
        `UPDATE credit_holds SET actual_credits = 0, charged_credits = 0 WHERE hold_id = $1`,
        [holdId]
      );
      return {
        holdId,
        orgId: ctx.hold.org_id,
        releasedCredits: Number(ctx.hold.estimated_credits),
        status: 'RELEASED' as const,
      };
    };

    const result = await this.runHoldTransition(
      holdId,
      'release',
      null,
      {
        fast: async (ctx) => () => release(ctx),
        full: release,
      },
      joined
    );

    await this.afterCommit(joined, async () => {
      await redis.del(holdRedisKey(holdId));
    });
    return result;
  }

  /**
   * Settle an active streaming hold once the response stream finishes and actual usage is known.
   *
   * Usage is charged own-pool-first: the consumer's own portion and free credits, then - only for
   * the excess - each ancestor pool's portion of this hold plus that pool's free credits. Credits
   * reserved by other holds are never consumed, in any pool. Unfundable usage (over-budget stream,
   * or reservation lost to expiry/refunds) is logged as a SETTLEMENT_SHORTFALL discrepancy; the hold
   * still settles. All portions are then released LIFO. One transaction, one handle.
   */
  async settleStreamHold(input: SettleStreamHoldInput, joined?: JoinedTransaction): Promise<SettleStreamHoldResult> {
    const { holdId } = input;
    let rechargeOrgIds: string[] = [];

    const actualOf = (ctx: HoldContext) =>
      typeof input.actualCredits === 'number'
        ? input.actualCredits
        : pricingCalculator.calculateQuote({
            modelName: ctx.hold.model_name,
            inputTokens: input.inputTokens ?? 0,
            outputTokens: input.outputTokens ?? 0,
            cachedInputTokens: input.cachedInputTokens ?? 0,
            planTier: ctx.chain[0].planTier,
          }).billableCredits;

    const ownPortionCredits = (ctx: HoldContext, stripeNos: Iterable<number>) => {
      const wanted = new Set(stripeNos);
      return portionsOn(ctx.portions, ctx.hold.org_id)
        .filter((p) => wanted.has(p.stripeNo))
        .reduce((sum, p) => sum + p.credits, 0);
    };

    const complete = async (ctx: HoldContext, actual: number, consumerSlices: DrawSlice[]) => {
      const drawn = consumerSlices.reduce((sum, s) => sum + s.credits, 0);
      const draw: ChainDraw = {
        locks: new Map([[ctx.hold.org_id, ctx.lock]]),
        snapshots: new Map(),
        slices: [...consumerSlices],
        charged: drawn,
        shortfall: 0,
      };
      const excess = actual - drawn;
      const ancestorChain = ctx.chain.slice(1);
      const lastPortionLevel = ancestorChain.reduce(
        (last, org, i) => (portionsOn(ctx.portions, org.orgId).length > 0 ? i : last),
        -1
      );
      if (ancestorChain.length > 0 && (excess > 0 || lastPortionLevel >= 0)) {
        // Ancestors are locked after the consumer (canonical order). Each pool may fund the excess
        // with this hold's portion on it plus its free credits; its portion stripes are locked even
        // when nothing is drawn there, so the portions can be released.
        const upstream = await drawAlongChain(
          ctx.client,
          ancestorChain,
          Math.max(0, excess),
          holdId,
          {
            pinnedStripes: (org) => [...new Set(portionsOn(ctx.portions, org.orgId).map((p) => p.stripeNo))],
            portionCredits: (org) => {
              const byStripe = new Map<number, number>();
              for (const p of portionsOn(ctx.portions, org.orgId)) {
                byStripe.set(p.stripeNo, (byStripe.get(p.stripeNo) ?? 0) + p.credits);
              }
              return byStripe;
            },
            lastRequiredLevel: () => lastPortionLevel,
          },
          true
        );
        upstream.locks.forEach((lock, id) => draw.locks.set(id, lock));
        upstream.snapshots.forEach((snap, id) => draw.snapshots.set(id, snap));
        draw.slices.push(...upstream.slices);
        draw.charged += upstream.charged;
        draw.shortfall = upstream.shortfall;
      } else {
        draw.shortfall = Math.max(0, excess);
      }
      return this.completeSettlement(ctx, input, ctx.chain, actual, draw, (ids) => (rechargeOrgIds = ids));
    };

    const result = await this.runHoldTransition(holdId, 'settle', null, {
      fast: async (ctx) => {
        const home = ctx.lock.stripes.get(ctx.hold.home_stripe_no)!;
        const actual = actualOf(ctx);
        // Own reservation + this stripe's free credits; other holds' reservations stay untouched.
        const budget = headroomOf(home) + ownPortionCredits(ctx, [home.stripeNo]);
        if (actual > budget) return null; // own pool beyond this stripe first: needs FULL
        const slices = await planDraw(ctx.lock, actual);
        if (!slices) return null;
        return async () => {
          await applyDraw(ctx.lock, slices);
          return complete(ctx, actual, slices);
        };
      },
      full: async (ctx) => {
        const actual = actualOf(ctx);
        const totals = totalsOf(ctx.lock.stripes.values());
        const budget = Math.max(
          0,
          totals.netSpendableCredits + ownPortionCredits(ctx, ctx.lock.stripes.keys())
        );
        const own = Math.min(actual, budget);
        const slices = await planDraw(ctx.lock, own);
        if (!slices) {
          // Stripe counters claim more than the lots hold (drift): nothing has been written yet.
          throw new InsufficientCreditsError(ctx.hold.org_id, own, budget);
        }
        await applyDraw(ctx.lock, slices);
        return complete(ctx, actual, slices);
      },
    }, joined);

    await this.afterCommit(joined, async () => {
      autoRechargeService.kickOutbox(rechargeOrgIds);
      await redis.del(holdRedisKey(holdId));
      await redis.del(balanceCacheKey(result.orgId));
    });
    return result;
  }

  private async completeSettlement(
    ctx: HoldContext,
    input: SettleStreamHoldInput,
    chain: OrgConfig[],
    actualCredits: number,
    draw: ChainDraw,
    reportRecharges: (orgIds: string[]) => void
  ): Promise<SettleStreamHoldResult> {
    const { hold } = ctx;
    const estimatedCredits = Number(hold.estimated_credits);

    // Every portion stripe is locked by now (consumer + ancestors): release them LIFO.
    await this.unwindPortions({ ...ctx, lock: ctx.lock }, 'SETTLED', draw.locks);

    const chargedCredits = draw.charged;
    let usageId: string | null = null;
    if (chargedCredits > 0) {
      // Same transaction handle: the usage is recorded without a nested transaction.
      const deduction = await this.ledger.deductUsage(
        {
          orgId: hold.org_id,
          requestId: hold.request_id,
          modelName: hold.model_name,
          credits: chargedCredits,
          inputTokens: input.inputTokens,
          outputTokens: input.outputTokens,
          cachedInputTokens: input.cachedInputTokens,
        },
        { holdId: hold.hold_id, inTransaction: { client: ctx.client, chain, draw } }
      );
      usageId = deduction.usageId;
      reportRecharges(
        deduction.autoRechargeQueued ? [...draw.locks.keys()] : []
      );
    } else {
      reportRecharges(await this.enqueueRecharges(ctx.client, chain, draw.locks, draw.snapshots));
    }

    const shortfallCredits = draw.shortfall;
    if (shortfallCredits > 0) {
      const overBudget = Math.min(shortfallCredits, Math.max(0, actualCredits - estimatedCredits));
      await this.ledger.logDiscrepancy(ctx.lock, {
        type: 'SETTLEMENT_SHORTFALL',
        holdId: hold.hold_id,
        usageId,
        unrecovered: shortfallCredits,
        reason:
          `Hold ${hold.hold_id} used ${actualCredits} credits (estimate ${estimatedCredits}) but only ` +
          `${chargedCredits} could be charged without consuming other holds' reservations; ` +
          `unfunded ${shortfallCredits} (over-budget ${overBudget}, unbacked reservation ` +
          `${shortfallCredits - overBudget})`,
      });
    }

    await ctx.client.query(
      `UPDATE credit_holds SET actual_credits = $1, charged_credits = $2, updated_at = NOW() WHERE hold_id = $3`,
      [actualCredits, chargedCredits, hold.hold_id]
    );

    const pooledCredits = draw.slices
      .filter((s) => s.orgId !== hold.org_id)
      .reduce((sum, s) => sum + s.credits, 0);

    return {
      holdId: hold.hold_id,
      orgId: hold.org_id,
      status: 'SETTLED',
      estimatedCredits,
      actualCredits,
      chargedCredits,
      shortfallCredits,
      pooledCredits,
      usageId,
    };
  }

  /**
   * Release abandoned streaming holds that have passed their expiration timestamp. One transaction
   * per consumer org: its home stripes of expired holds first, then each ancestor's portion stripes
   * in chain order (canonical lock order), then portions unwind LIFO.
   */
  async releaseExpiredHolds(): Promise<{ releasedCount: number; releasedCredits: number }> {
    const candidatesRes = await query<{ org_id: string; stripe_nos: number[] }>(
      `SELECT org_id, array_agg(DISTINCT home_stripe_no::int ORDER BY home_stripe_no::int) AS stripe_nos
       FROM credit_holds
       WHERE status = 'ACTIVE' AND expires_at <= NOW()
       GROUP BY org_id
       ORDER BY org_id`
    );

    let releasedCount = 0;
    let releasedCredits = 0;

    for (const { org_id: orgId, stripe_nos: stripeNos } of candidatesRes.rows) {
      const expired = await withTransaction(async (client) => {
        const chain = await loadOrgChain(clientQuery(client), orgId);
        const consumerLock = await lockStripes(client, chain[0], stripeNos.map(Number));
        // Re-selected under lock: a concurrent settle/release/extend may have won the race.
        const holdsRes = await client.query<LockedHold>(
          `SELECT hold_id, org_id, home_stripe_no, request_id, model_name, estimated_credits, status,
                  expires_at, TRUE AS is_expired
           FROM credit_holds
           WHERE org_id = $1 AND home_stripe_no = ANY($2::int[]) AND status = 'ACTIVE' AND expires_at <= NOW()
           ORDER BY hold_id
           FOR UPDATE`,
          [orgId, [...consumerLock.stripes.keys()]]
        );
        const portionsByHold = await this.loadPortions(client, holdsRes.rows.map((h) => h.hold_id));
        const allPortions = [...portionsByHold.values()].flat();
        const locks = await this.lockAncestorPortionStripes(client, chain, allPortions, new Map([[orgId, consumerLock]]));
        for (const hold of holdsRes.rows) {
          await this.unwindPortions(
            { client, chain, lock: consumerLock, hold, portions: portionsByHold.get(hold.hold_id) ?? [] },
            'EXPIRED',
            locks
          );
        }
        return holdsRes.rows;
      });

      for (const hold of expired) {
        await redis.del(holdRedisKey(hold.hold_id));
        releasedCount += 1;
        releasedCredits += Number(hold.estimated_credits);
      }
    }

    return { releasedCount, releasedCredits };
  }

  /**
   * Run a state transition on an ACTIVE hold inside one transaction. The consumer org is locked
   * first - the hold's home stripe (fast) or, on escalation, every stripe of the org - then the hold
   * row; `handlers` continue from there and lock ancestors afterwards.
   *
   * - Holds that are SETTLED / RELEASED / EXPIRED are rejected with 409.
   * - Holds past `expires_at` that the sweep has not reached yet are expired on the spot
   *   (portions released, committed) and the request is rejected with 409.
   */
  private async runHoldTransition<T>(
    holdId: string,
    action: HoldAction,
    knownChain: OrgConfig[] | null,
    handlers: HoldTransitionHandlers<T>,
    joined?: JoinedTransaction
  ): Promise<T> {
    type Outcome = { expired: true } | { expired: false; value: T };

    const outcome = await runInTransaction(joined?.client, async (client): Promise<Outcome> => {
      // home_stripe_no is immutable, so reading it without a lock is safe; it tells us which
      // stripe to lock *before* locking the hold row.
      const refRes = await client.query<{ org_id: string; home_stripe_no: number }>(
        `SELECT org_id, home_stripe_no FROM credit_holds WHERE hold_id = $1`,
        [holdId]
      );
      if (refRes.rowCount === 0) {
        throw new ResourceNotFoundError('CreditHold', holdId);
      }
      const homeStripe = Number(refRes.rows[0].home_stripe_no);
      const chain = knownChain ?? (await loadOrgChain(clientQuery(client), refRes.rows[0].org_id));
      const context = async (lock: WalletLock): Promise<HoldContext> => {
        const hold = await this.lockActiveHold(client, holdId, action);
        // Portions only change under the hold row lock, so this read is stable from here on.
        const portions = (await this.loadPortions(client, [holdId])).get(holdId) ?? [];
        return { client, chain, lock, hold, portions };
      };
      const expireNow = async (ctx: HoldContext): Promise<Outcome> => {
        await this.unwindPortions(ctx, 'EXPIRED');
        return { expired: true };
      };

      return withStripeOrFullLock<Outcome>(
        client,
        async () => {
          const ctx = await context(await lockStripes(client, chain[0], [homeStripe]));
          if (ctx.hold.is_expired) return () => expireNow(ctx);
          const apply = await handlers.fast(ctx);
          return apply ? async () => ({ expired: false, value: await apply() }) : null;
        },
        async () => {
          // The savepoint rollback released the hold lock: re-validate under the FULL lock.
          const ctx = await context(await lockFullWallet(client, chain[0].orgId, chain[0]));
          if (ctx.hold.is_expired) return expireNow(ctx);
          return { expired: false, value: await handlers.full(ctx) };
        }
      );
    });

    if (outcome.expired) {
      // Own transaction: the expiry is committed, then the request is rejected. Joined transaction:
      // the caller decides - its rollback undoes the expiry too (the sweep expires the hold later).
      if (!joined) await redis.del(holdRedisKey(holdId));
      throw new HoldStateConflictError(
        holdId,
        `Cannot ${action} hold: it expired before the request arrived`
      );
    }

    return outcome.value;
  }

  /** Run a post-commit effect now (own transaction) or after the caller's COMMIT (joined). */
  private async afterCommit(joined: JoinedTransaction | undefined, effect: () => Promise<void>): Promise<void> {
    if (joined) joined.afterCommit(effect);
    else await effect();
  }

  private async holdOrgId(holdId: string): Promise<string> {
    const res = await query<{ org_id: string }>(`SELECT org_id FROM credit_holds WHERE hold_id = $1`, [holdId]);
    if (res.rowCount === 0) {
      throw new ResourceNotFoundError('CreditHold', holdId);
    }
    return res.rows[0].org_id;
  }

  private async lockActiveHold(client: PoolClient, holdId: string, action: HoldAction): Promise<LockedHold> {
    const holdRes = await client.query<LockedHold>(
      `SELECT hold_id, org_id, home_stripe_no, request_id, model_name, estimated_credits, status, expires_at,
              (expires_at <= NOW()) AS is_expired
       FROM credit_holds
       WHERE hold_id = $1
       FOR UPDATE`,
      [holdId]
    );
    const hold = holdRes.rows[0];
    hold.home_stripe_no = Number(hold.home_stripe_no);
    if (hold.status !== 'ACTIVE') {
      throw new HoldStateConflictError(holdId, `Cannot ${action} hold in status ${hold.status}`);
    }
    return hold;
  }

  private async loadPortions(client: PoolClient, holdIds: string[]): Promise<Map<string, Portion[]>> {
    const byHold = new Map<string, Portion[]>();
    if (holdIds.length === 0) return byHold;
    const res = await client.query<{
      hold_id: string;
      seq: number;
      org_id: string;
      stripe_no: number;
      reserved_credits: string | number;
    }>(
      `SELECT hold_id, seq, org_id, stripe_no, reserved_credits
       FROM credit_hold_portions
       WHERE hold_id = ANY($1::varchar[])
       ORDER BY hold_id, seq`,
      [holdIds]
    );
    for (const row of res.rows) {
      const list = byHold.get(row.hold_id) ?? [];
      list.push({
        seq: Number(row.seq),
        orgId: row.org_id,
        stripeNo: Number(row.stripe_no),
        credits: Number(row.reserved_credits),
      });
      byHold.set(row.hold_id, list);
    }
    return byHold;
  }

  private async insertPortions(
    client: PoolClient,
    holdId: string,
    firstSeq: number,
    portions: Array<{ orgId: string; stripeNo: number; credits: number }>
  ): Promise<void> {
    for (const [i, portion] of portions.entries()) {
      await client.query(
        `INSERT INTO credit_hold_portions (hold_id, seq, org_id, stripe_no, reserved_credits)
         VALUES ($1, $2, $3, $4, $5)`,
        [holdId, firstSeq + i, portion.orgId, portion.stripeNo, portion.credits]
      );
    }
  }

  /**
   * Lock the stripes carrying ancestor portions, org by org in chain order (after the consumer),
   * reusing locks the transaction already holds for an org.
   */
  private async lockAncestorPortionStripes(
    client: PoolClient,
    chain: OrgConfig[],
    portions: Portion[],
    locks: Map<string, WalletLock>
  ): Promise<Map<string, WalletLock>> {
    const chainIds = new Set(chain.map((o) => o.orgId));
    for (const portion of portions) {
      if (!chainIds.has(portion.orgId)) {
        throw new Error(`Hold portion on ${portion.orgId} is outside the org chain of ${chain[0].orgId}`);
      }
    }
    for (const org of chain.slice(1)) {
      const stripeNos = [...new Set(portionsOn(portions, org.orgId).map((p) => p.stripeNo))];
      if (stripeNos.length === 0) continue;
      const existing = locks.get(org.orgId);
      if (existing && stripeNos.every((s) => existing.stripes.has(s))) continue;
      if (existing) {
        throw new Error(`Stripes of ${org.orgId} must be locked in one step to keep the canonical order`);
      }
      locks.set(org.orgId, await lockStripes(client, org, stripeNos));
    }
    return locks;
  }

  /**
   * Release a hold's portions LIFO (latest cascaded portion first) and move it to `status`.
   * Ancestor portion stripes are locked first (in canonical order) when not already held.
   */
  private async unwindPortions(
    ctx: HoldContext,
    status: 'RELEASED' | 'EXPIRED' | 'SETTLED',
    heldLocks?: Map<string, WalletLock>
  ): Promise<void> {
    const locks = await this.lockAncestorPortionStripes(
      ctx.client,
      ctx.chain,
      ctx.portions,
      heldLocks ?? new Map([[ctx.hold.org_id, ctx.lock]])
    );
    if (!locks.has(ctx.hold.org_id)) locks.set(ctx.hold.org_id, ctx.lock);
    for (const portion of [...ctx.portions].sort((a, b) => b.seq - a.seq)) {
      await adjustStripe(locks.get(portion.orgId)!, portion.stripeNo, { reserved: -portion.credits });
    }
    await ctx.client.query(
      `UPDATE credit_holds SET status = $1, updated_at = NOW() WHERE hold_id = $2`,
      [status, ctx.hold.hold_id]
    );
  }

  /** Commit auto-recharge outbox intents for pools this transaction reduced to their threshold. */
  private async enqueueRecharges(
    client: PoolClient,
    chain: OrgConfig[],
    locks: Map<string, WalletLock>,
    snapshots: Map<string, OrgWalletSnapshot>
  ): Promise<string[]> {
    const orgIds: string[] = [];
    for (const org of chain) {
      const lock = locks.get(org.orgId);
      if (!lock || !org.autoRecharge.enabled) continue;
      const snapshot =
        snapshots.get(org.orgId) ??
        (lock.mode === 'STRIPE' ? await readOrgWallet(clientQuery(client), org.orgId, org) : undefined);
      const intentId = await autoRechargeService.enqueueIntentIfNeeded(
        client,
        org,
        estimatedNetSpendable(lock, snapshot),
        null
      );
      if (intentId) orgIds.push(org.orgId);
    }
    return orgIds;
  }
}

const nextSeq = (portions: Portion[]) => portions.reduce((max, p) => Math.max(max, p.seq + 1), 0);

export const streamHoldService = new StreamHoldService();
