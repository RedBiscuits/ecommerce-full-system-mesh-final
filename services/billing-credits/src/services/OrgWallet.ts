import type { PoolClient, QueryResult, QueryResultRow } from 'pg';
import { txScope } from '../db';
import { GrantSource, PlanTier, ResourceNotFoundError } from '../types/credits';

/*
 * Striped, hierarchical wallet
 * ============================
 * An organization's balance is sharded across `wallet_stripe_count` rows of org_wallet_stripes,
 * and every grant is split into one credit lot per stripe. Per stripe:
 *   available_credits = SUM(remaining_credits of the stripe's lots of ACTIVE grants)   (>= 0, CHECK)
 *   reserved_credits  = SUM(hold portions reserved on the stripe, ACTIVE holds)      (>= 0, CHECK)
 * so org-wide totals (sums over stripes) keep the exact zero floor and reservation invariants.
 *
 * Orgs may have a parent (org_balances.parent_org_id). A sub-org draws on its own pool first and
 * cascades any remaining deduction or reservation deficit up its ancestor chain.
 *
 * Lock modes per org:
 *   - STRIPE: specific stripes (usually one). Independent writes of an org run in parallel.
 *   - FULL:   every stripe of the org in ascending order - exclusive access to the org's holds,
 *             lots and grants. Used when one stripe cannot serve an operation exactly, and for
 *             org-wide maintenance (grants, refunds, sweeps, recharges).
 * Escalation STRIPE -> FULL happens inside the transaction: roll back to a savepoint taken before
 * the org was locked (releasing its locks) and lock every stripe of the org.
 *
 * Canonical lock order (strict, enforced at runtime by the per-transaction LockLedger):
 *   1. stripe_webhook_events row (webhook idempotency gate; webhook transactions only)
 *   2. org_wallet_stripes rows, ordered by (org depth DESC, org_id, stripe_no):
 *      descendants before ancestors, ascending stripes within an org
 *   3. rows owned by a locked stripe (holds homed on it, hold portions and lots on it, outbox and
 *      grant rows of an org under FULL lock) - only ever touched while that stripe is held.
 * Every transaction acquires level-2 locks in strictly increasing canonical order (re-entry is
 * allowed), so no wait-for cycle - hence no deadlock - can form: not between parallel deductions of
 * one org, nor between a parent's own deductions, children cascading holds/deductions into it and
 * LIFO settlements unwinding those cascaded portions. A STRIPE-mode critical section never waits
 * for another wallet lock once its stripes are held, which bounds every lock wait.
 */

export type QueryFn = <T extends QueryResultRow = any>(
  text: string,
  params?: any[]
) => Promise<QueryResult<T>>;

/** Maximum length of an org chain (the org plus its ancestors). */
export const MAX_POOL_LEVELS = 4;

export interface OrgConfig {
  orgId: string;
  parentOrgId: string | null;
  /** Distance from the root of the org's hierarchy (root = 0). */
  depth: number;
  planTier: PlanTier;
  stripeCount: number;
  burstLimitOverride: number | null;
  autoRecharge: {
    enabled: boolean;
    thresholdCredits: number;
    amountCredits: number;
    monthlyCapCredits: number;
  };
}

export interface StripeBalance {
  stripeNo: number;
  availableCredits: number;
  reservedCredits: number;
  /** Credits in lots past `expires_at` that the maintenance sweep has not retired yet. */
  unsweptExpiredCredits: number;
}

export interface WalletTotals {
  availableCredits: number;
  reservedCredits: number;
  unsweptExpiredCredits: number;
  /** Available minus unswept expired credits: what can really be drawn on. */
  effectiveAvailableCredits: number;
  /** Effective minus reservations (negative when over-committed). */
  netSpendableCredits: number;
}

export interface OrgWalletSnapshot extends WalletTotals {
  config: OrgConfig;
  stripes: StripeBalance[];
}

export interface WalletLock {
  client: PoolClient;
  config: OrgConfig;
  mode: 'STRIPE' | 'FULL';
  /** Locked stripes; kept in sync with every write made through this module. */
  stripes: Map<number, StripeBalance>;
}

export interface DrawSlice {
  /** Org whose pool funded the slice (the consumer itself or an ancestor). */
  orgId: string;
  grantId: string;
  stripeNo: number;
  source: GrantSource;
  credits: number;
}

export class LockOrderViolationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'LockOrderViolationError';
  }
}

export const effectiveOf = (s: StripeBalance): number =>
  Math.max(0, s.availableCredits - s.unsweptExpiredCredits);
export const headroomOf = (s: StripeBalance): number => effectiveOf(s) - s.reservedCredits;

export function totalsOf(stripes: Iterable<StripeBalance>): WalletTotals {
  let availableCredits = 0;
  let reservedCredits = 0;
  let unsweptExpiredCredits = 0;
  let effectiveAvailableCredits = 0;
  for (const s of stripes) {
    availableCredits += s.availableCredits;
    reservedCredits += s.reservedCredits;
    unsweptExpiredCredits += s.unsweptExpiredCredits;
    effectiveAvailableCredits += effectiveOf(s);
  }
  return {
    availableCredits,
    reservedCredits,
    unsweptExpiredCredits,
    effectiveAvailableCredits,
    netSpendableCredits: effectiveAvailableCredits - reservedCredits,
  };
}

export const clientQuery =
  (client: PoolClient): QueryFn =>
  (text, params) =>
    client.query(text, params);

/* ------------------------------------------------------------------------------------------------
 * Canonical lock ordering
 * ---------------------------------------------------------------------------------------------- */

interface LockKey {
  depth: number;
  orgId: string;
  stripeNo: number;
}

/** Canonical order: deeper orgs first (descendants before ancestors), then org id, then stripe. */
export function compareLockKeys(a: LockKey, b: LockKey): number {
  if (a.depth !== b.depth) return b.depth - a.depth;
  if (a.orgId !== b.orgId) return a.orgId < b.orgId ? -1 : 1;
  return a.stripeNo - b.stripeNo;
}

const keyId = (k: LockKey) => `${k.orgId}#${k.stripeNo}`;

/**
 * Per-transaction record of wallet stripe locks. Every acquisition must be strictly greater (in
 * canonical order) than every lock already held, except re-entry of a held lock. Savepoint
 * rollbacks rewind the ledger together with the locks PostgreSQL releases.
 */
export class LockLedger {
  private held: LockKey[] = [];
  private ids = new Set<string>();

  acquire(keys: LockKey[]): void {
    const sorted = [...keys].sort(compareLockKeys);
    for (const key of sorted) {
      if (this.ids.has(keyId(key))) continue;
      const last = this.held[this.held.length - 1];
      if (last && compareLockKeys(last, key) >= 0) {
        throw new LockOrderViolationError(
          `Lock order violation: acquiring ${keyId(key)} (depth ${key.depth}) after ` +
            `${keyId(last)} (depth ${last.depth})`
        );
      }
      this.held.push(key);
      this.ids.add(keyId(key));
    }
  }

  mark(): number {
    return this.held.length;
  }

  rollbackTo(mark: number): void {
    for (const key of this.held.splice(mark)) this.ids.delete(keyId(key));
  }

  holds(orgId: string, stripeNo: number): boolean {
    return this.ids.has(`${orgId}#${stripeNo}`);
  }
}

export function ledgerOf(client: PoolClient): LockLedger {
  const scope = txScope(client);
  if (!scope.walletLedger) scope.walletLedger = new LockLedger();
  return scope.walletLedger as LockLedger;
}

/* ------------------------------------------------------------------------------------------------
 * Reads
 * ---------------------------------------------------------------------------------------------- */

/**
 * Load an org and its ancestors (nearest first) with their absolute depth, in one recursive query.
 */
export async function loadOrgChain(run: QueryFn, orgId: string): Promise<OrgConfig[]> {
  const res = await run<{
    org_id: string;
    parent_org_id: string | null;
    plan_tier: PlanTier;
    wallet_stripe_count: number;
    burst_limit_credits_per_minute: string | number | null;
    auto_recharge_enabled: boolean;
    auto_recharge_threshold_credits: string | number;
    auto_recharge_amount_credits: string | number;
    monthly_recharge_cap_credits: string | number;
    distance: number;
  }>(
    `WITH RECURSIVE chain AS (
       SELECT org_id, parent_org_id, plan_tier, wallet_stripe_count, burst_limit_credits_per_minute,
              auto_recharge_enabled, auto_recharge_threshold_credits, auto_recharge_amount_credits,
              monthly_recharge_cap_credits, 0 AS distance
       FROM org_balances WHERE org_id = $1
       UNION ALL
       SELECT p.org_id, p.parent_org_id, p.plan_tier, p.wallet_stripe_count, p.burst_limit_credits_per_minute,
              p.auto_recharge_enabled, p.auto_recharge_threshold_credits, p.auto_recharge_amount_credits,
              p.monthly_recharge_cap_credits, c.distance + 1
       FROM org_balances p
       JOIN chain c ON p.org_id = c.parent_org_id
       WHERE c.distance < $2
     )
     SELECT * FROM chain ORDER BY distance`,
    [orgId, MAX_POOL_LEVELS]
  );
  if (res.rowCount === 0) {
    throw new ResourceNotFoundError('Organization', orgId);
  }
  const rows = res.rows;
  if (rows[rows.length - 1].parent_org_id !== null) {
    throw new Error(`Org hierarchy above ${orgId} is deeper than ${MAX_POOL_LEVELS} levels or cyclic`);
  }
  const rootDepth = rows.length - 1;
  return rows.map((row, i) => ({
    orgId: row.org_id,
    parentOrgId: row.parent_org_id,
    depth: rootDepth - i,
    planTier: row.plan_tier,
    stripeCount: Number(row.wallet_stripe_count),
    burstLimitOverride:
      row.burst_limit_credits_per_minute === null ? null : Number(row.burst_limit_credits_per_minute),
    autoRecharge: {
      enabled: row.auto_recharge_enabled,
      thresholdCredits: Number(row.auto_recharge_threshold_credits),
      amountCredits: Number(row.auto_recharge_amount_credits),
      monthlyCapCredits: Number(row.monthly_recharge_cap_credits),
    },
  }));
}

export async function readOrgConfig(run: QueryFn, orgId: string): Promise<OrgConfig> {
  return (await loadOrgChain(run, orgId))[0];
}

/** Credits per stripe in lots past `expires_at` that the sweep has not retired yet. */
async function unsweptExpiredByStripe(
  run: QueryFn,
  orgId: string,
  stripeNos: number[] | null
): Promise<Map<number, number>> {
  const res = await run<{ stripe_no: number; total: string }>(
    `SELECT l.stripe_no, SUM(l.remaining_credits)::text AS total
     FROM credit_lots l
     JOIN credit_grants g ON g.grant_id = l.grant_id
     WHERE l.org_id = $1 ${stripeNos ? 'AND l.stripe_no = ANY($2::int[])' : ''}
       AND g.status = 'ACTIVE' AND g.expires_at <= NOW() AND l.remaining_credits > 0
     GROUP BY l.stripe_no`,
    stripeNos ? [orgId, stripeNos] : [orgId]
  );
  return new Map(res.rows.map((r) => [Number(r.stripe_no), Number(r.total)]));
}

async function fetchStripes(
  run: QueryFn,
  orgId: string,
  stripeNos: number[] | null,
  lock: boolean
): Promise<StripeBalance[]> {
  const stripesRes = await run<{
    stripe_no: number;
    available_credits: string | number;
    reserved_credits: string | number;
  }>(
    `SELECT stripe_no, available_credits, reserved_credits
     FROM org_wallet_stripes
     WHERE org_id = $1 ${stripeNos ? 'AND stripe_no = ANY($2::int[])' : ''}
     ORDER BY stripe_no
     ${lock ? 'FOR UPDATE' : ''}`,
    stripeNos ? [orgId, stripeNos] : [orgId]
  );
  // Separate statement on purpose: under READ COMMITTED it sees everything committed before the
  // row locks above were granted.
  const expiredByStripe = await unsweptExpiredByStripe(run, orgId, stripeNos);
  return stripesRes.rows.map((r) => ({
    stripeNo: Number(r.stripe_no),
    availableCredits: Number(r.available_credits),
    reservedCredits: Number(r.reserved_credits),
    unsweptExpiredCredits: expiredByStripe.get(Number(r.stripe_no)) ?? 0,
  }));
}

/** Lock-free wallet snapshot of one org (config, per-stripe balances and totals). */
export async function readOrgWallet(
  run: QueryFn,
  orgId: string,
  knownConfig?: OrgConfig
): Promise<OrgWalletSnapshot> {
  const config = knownConfig ?? (await readOrgConfig(run, orgId));
  const stripes = await fetchStripes(run, orgId, null, false);
  return { config, stripes, ...totalsOf(stripes) };
}

/** Make sure the org row and its wallet stripes exist (first grant for an unknown org). */
export async function ensureOrgWallet(client: PoolClient, orgId: string): Promise<void> {
  await client.query(`INSERT INTO org_balances (org_id) VALUES ($1) ON CONFLICT (org_id) DO NOTHING`, [
    orgId,
  ]);
  await client.query(
    `INSERT INTO org_wallet_stripes (org_id, stripe_no)
     SELECT o.org_id, s.stripe_no
     FROM org_balances o
     CROSS JOIN LATERAL generate_series(0, o.wallet_stripe_count - 1) AS s(stripe_no)
     WHERE o.org_id = $1
     ON CONFLICT DO NOTHING`,
    [orgId]
  );
}

/* ------------------------------------------------------------------------------------------------
 * Locking
 * ---------------------------------------------------------------------------------------------- */

const stripeKeys = (config: OrgConfig, stripeNos: number[]): LockKey[] =>
  stripeNos.map((stripeNo) => ({ depth: config.depth, orgId: config.orgId, stripeNo }));

/** FULL mode: lock every stripe of the org in ascending stripe_no order. */
export async function lockFullWallet(
  client: PoolClient,
  orgId: string,
  config?: OrgConfig
): Promise<WalletLock> {
  const cfg = config ?? (await readOrgConfig(clientQuery(client), orgId));
  ledgerOf(client).acquire(
    stripeKeys(cfg, Array.from({ length: cfg.stripeCount }, (_, i) => i))
  );
  const stripes = await fetchStripes(clientQuery(client), orgId, null, true);
  return { client, config: cfg, mode: 'FULL', stripes: new Map(stripes.map((s) => [s.stripeNo, s])) };
}

/** STRIPE mode on specific stripes, locked in ascending order. */
export async function lockStripes(
  client: PoolClient,
  config: OrgConfig,
  stripeNos: number[]
): Promise<WalletLock> {
  const ordered = [...new Set(stripeNos)].sort((a, b) => a - b);
  ledgerOf(client).acquire(stripeKeys(config, ordered));
  const stripes = await fetchStripes(clientQuery(client), config.orgId, ordered, true);
  return { client, config, mode: 'STRIPE', stripes: new Map(stripes.map((s) => [s.stripeNo, s])) };
}

/** FNV-1a: cheap, stable hash for deterministic stripe affinity. */
export function stableHash(value: string): number {
  let hash = 0x811c9dc5;
  for (let i = 0; i < value.length; i++) {
    hash ^= value.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193);
  }
  return hash >>> 0;
}

/**
 * Deterministic stripe preference for an operation: the eligible stripes rotated to start at
 * hash(affinityKey). The same key always prefers the same stripe; different keys spread evenly.
 */
export function stripeRotation(candidates: number[], affinityKey: string): number[] {
  const ordered = [...new Set(candidates)].sort((a, b) => a - b);
  if (ordered.length === 0) return ordered;
  const start = stableHash(affinityKey) % ordered.length;
  return [...ordered.slice(start), ...ordered.slice(0, start)];
}

/**
 * Deterministic stripe acquisition (no polling, no sleeping):
 *   1. one `FOR UPDATE SKIP LOCKED` statement takes the first idle stripe of the rotation;
 *   2. if every eligible stripe is busy, queue on the rotation's home stripe in PostgreSQL's FIFO
 *      lock queue. Holders of a stripe never wait for another wallet lock (see header), so that
 *      wait is bounded by the critical sections ahead of us.
 * At most two statements; the choice depends only on the affinity key and which stripes are busy.
 */
export async function lockIdleStripe(
  client: PoolClient,
  config: OrgConfig,
  candidates: number[],
  affinityKey: string
): Promise<WalletLock> {
  const rotation = stripeRotation(candidates, affinityKey);
  const ledger = ledgerOf(client);
  // Every candidate must be acquirable in canonical order before we try any of them.
  const mark = ledger.mark();
  ledger.acquire(stripeKeys(config, rotation));
  ledger.rollbackTo(mark);

  const idle = await client.query<{
    stripe_no: number;
    available_credits: string | number;
    reserved_credits: string | number;
  }>(
    `SELECT stripe_no, available_credits, reserved_credits
     FROM org_wallet_stripes
     WHERE org_id = $1 AND stripe_no = ANY($2::int[])
     ORDER BY array_position($2::int[], stripe_no::int)
     LIMIT 1
     FOR UPDATE SKIP LOCKED`,
    [config.orgId, rotation]
  );
  if (!idle.rowCount) {
    return lockStripes(client, config, [rotation[0]]);
  }

  const row = idle.rows[0];
  const stripeNo = Number(row.stripe_no);
  ledger.acquire(stripeKeys(config, [stripeNo]));
  const expired = await unsweptExpiredByStripe(clientQuery(client), config.orgId, [stripeNo]);
  const stripe: StripeBalance = {
    stripeNo,
    availableCredits: Number(row.available_credits),
    reservedCredits: Number(row.reserved_credits),
    unsweptExpiredCredits: expired.get(stripeNo) ?? 0,
  };
  return { client, config, mode: 'STRIPE', stripes: new Map([[stripeNo, stripe]]) };
}

/** Process-wide counters of wallet lock outcomes (observability: how often the fast path wins). */
export const walletLockMetrics = { stripeFastPath: 0, escalatedToFull: 0 };

let savepointSeq = 0;

/**
 * Try a STRIPE-mode plan for one org; fall back to a FULL lock of that org in the same transaction.
 *
 * `planFast` may only take locks and read. It returns an `apply` closure when the locked stripes
 * can serve the operation (writes happen after the savepoint is released), or null to escalate:
 * the savepoint rollback releases every lock taken since the savepoint (PostgreSQL releases the
 * row locks, the ledger forgets them), then `runFull` locks the org's stripes in canonical order.
 * Locks held before the savepoint (e.g. a descendant's stripes) are kept, which the canonical
 * order allows because the org being escalated is never deeper than them.
 */
export async function withStripeOrFullLock<T>(
  client: PoolClient,
  planFast: () => Promise<(() => Promise<T>) | null>,
  runFull: () => Promise<T>
): Promise<T> {
  const name = `wallet_sp_${(savepointSeq = (savepointSeq + 1) % 1_000_000)}`;
  const ledger = ledgerOf(client);
  const mark = ledger.mark();
  await client.query(`SAVEPOINT ${name}`);
  const apply = await planFast();
  if (apply) {
    walletLockMetrics.stripeFastPath += 1;
    await client.query(`RELEASE SAVEPOINT ${name}`);
    return apply();
  }
  walletLockMetrics.escalatedToFull += 1;
  await client.query(`ROLLBACK TO SAVEPOINT ${name}`);
  await client.query(`RELEASE SAVEPOINT ${name}`);
  ledger.rollbackTo(mark);
  return runFull();
}

/** Eligible stripes (headroom covers `credits`) from a lock-free snapshot, ascending. */
export function candidateStripes(snapshot: OrgWalletSnapshot, credits: number): number[] {
  return snapshot.stripes.filter((s) => headroomOf(s) >= credits).map((s) => s.stripeNo);
}

/* ------------------------------------------------------------------------------------------------
 * Draws, reservations, transfers
 * ---------------------------------------------------------------------------------------------- */

/**
 * Plan a FIFO draw of `credits` from the locked stripes of one org (locks the lots it will touch).
 *
 * FULL mode walks every live lot of the org in FIFO order. STRIPE mode walks the locked stripes'
 * lots and is only accepted when indistinguishable from an org-wide FIFO draw: no grant preceding
 * the last grant touched may still have live credits on another stripe. (Other stripes' lots can
 * only shrink while we hold ours - growing them needs a FULL lock - so the lock-free check is sound.)
 * Returns null when the draw cannot be served (insufficient lots, or inexact in STRIPE mode).
 */
export async function planDraw(lock: WalletLock, credits: number): Promise<DrawSlice[] | null> {
  if (credits <= 0) return [];
  const { client, config } = lock;
  const stripeNos = [...lock.stripes.keys()];

  const lotsRes = await client.query<{
    grant_id: string;
    stripe_no: number;
    source: GrantSource;
    remaining_credits: string | number;
  }>(
    `SELECT l.grant_id, l.stripe_no, g.source, l.remaining_credits
     FROM credit_lots l
     JOIN credit_grants g ON g.grant_id = l.grant_id
     WHERE l.org_id = $1 AND l.stripe_no = ANY($2::int[])
       AND l.remaining_credits > 0 AND g.status = 'ACTIVE' AND g.expires_at > NOW()
     ORDER BY g.expires_at ASC, g.priority ASC, g.created_at ASC, g.grant_id ASC, l.stripe_no ASC
     FOR UPDATE OF l`,
    [config.orgId, stripeNos]
  );

  const slices: DrawSlice[] = [];
  let remaining = credits;
  for (const lot of lotsRes.rows) {
    if (remaining <= 0) break;
    const take = Math.min(Number(lot.remaining_credits), remaining);
    slices.push({
      orgId: config.orgId,
      grantId: lot.grant_id,
      stripeNo: Number(lot.stripe_no),
      source: lot.source,
      credits: take,
    });
    remaining -= take;
  }
  if (remaining > 0) return null;

  if (lock.mode === 'STRIPE') {
    const lastGrantId = slices[slices.length - 1].grantId;
    const earlierElsewhere = await client.query(
      `SELECT 1
       FROM credit_lots l
       JOIN credit_grants g ON g.grant_id = l.grant_id
       WHERE l.org_id = $1 AND NOT (l.stripe_no = ANY($2::int[]))
         AND l.remaining_credits > 0 AND g.status = 'ACTIVE' AND g.expires_at > NOW()
         AND (g.expires_at, g.priority, g.created_at, g.grant_id) <
             (SELECT expires_at, priority, created_at, grant_id FROM credit_grants WHERE grant_id = $3)
       LIMIT 1`,
      [config.orgId, stripeNos, lastGrantId]
    );
    if (earlierElsewhere.rowCount) return null;
  }

  return slices;
}

/** Apply a planned draw: decrement lots and the owning stripes. */
export async function applyDraw(lock: WalletLock, slices: DrawSlice[]): Promise<void> {
  const byStripe = new Map<number, number>();
  for (const slice of slices) {
    await lock.client.query(
      `UPDATE credit_lots
       SET remaining_credits = remaining_credits - $1, updated_at = NOW()
       WHERE grant_id = $2 AND stripe_no = $3`,
      [slice.credits, slice.grantId, slice.stripeNo]
    );
    byStripe.set(slice.stripeNo, (byStripe.get(slice.stripeNo) ?? 0) + slice.credits);
  }
  for (const [stripeNo, credits] of [...byStripe.entries()].sort((a, b) => a[0] - b[0])) {
    await adjustStripe(lock, stripeNo, { available: -credits });
  }
}

/** Apply deltas to a locked stripe (the CHECK constraints enforce the zero floors). */
export async function adjustStripe(
  lock: WalletLock,
  stripeNo: number,
  delta: { available?: number; reserved?: number }
): Promise<void> {
  const stripe = lock.stripes.get(stripeNo);
  if (!stripe) {
    throw new Error(`Stripe ${stripeNo} of ${lock.config.orgId} is not locked by this transaction`);
  }
  const available = delta.available ?? 0;
  const reserved = delta.reserved ?? 0;
  if (available === 0 && reserved === 0) return;
  await lock.client.query(
    `UPDATE org_wallet_stripes
     SET available_credits = available_credits + $1,
         reserved_credits = reserved_credits + $2,
         updated_at = NOW()
     WHERE org_id = $3 AND stripe_no = $4`,
    [available, reserved, lock.config.orgId, stripeNo]
  );
  stripe.availableCredits += available;
  stripe.reservedCredits += reserved;
}

/**
 * FULL mode only: move up to `amount` free (unreserved, unexpired) credits from other stripes into
 * `target`, lot by lot within the same grant, so org totals and per-grant totals are unchanged.
 */
export async function moveFreeCredits(lock: WalletLock, target: number, amount: number): Promise<number> {
  if (lock.mode !== 'FULL') throw new Error('moveFreeCredits requires a FULL wallet lock');
  let needed = amount;
  const donors = [...lock.stripes.values()]
    .filter((s) => s.stripeNo !== target && headroomOf(s) > 0)
    .sort((a, b) => headroomOf(b) - headroomOf(a) || a.stripeNo - b.stripeNo);

  for (const donor of donors) {
    if (needed <= 0) break;
    let budget = Math.min(headroomOf(donor), needed);
    const lots = await lock.client.query<{ grant_id: string; remaining_credits: string | number }>(
      `SELECT l.grant_id, l.remaining_credits
       FROM credit_lots l
       JOIN credit_grants g ON g.grant_id = l.grant_id
       WHERE l.org_id = $1 AND l.stripe_no = $2
         AND l.remaining_credits > 0 AND g.status = 'ACTIVE' AND g.expires_at > NOW()
       ORDER BY g.expires_at ASC, g.priority ASC, g.created_at ASC, g.grant_id ASC`,
      [lock.config.orgId, donor.stripeNo]
    );
    let movedFromDonor = 0;
    for (const lot of lots.rows) {
      if (budget <= 0) break;
      const take = Math.min(Number(lot.remaining_credits), budget);
      await lock.client.query(
        `UPDATE credit_lots SET remaining_credits = remaining_credits - $1, updated_at = NOW()
         WHERE grant_id = $2 AND stripe_no = $3`,
        [take, lot.grant_id, donor.stripeNo]
      );
      await lock.client.query(
        `INSERT INTO credit_lots (grant_id, stripe_no, org_id, remaining_credits)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (grant_id, stripe_no) DO UPDATE
         SET remaining_credits = credit_lots.remaining_credits + EXCLUDED.remaining_credits,
             updated_at = NOW()`,
        [lot.grant_id, target, lock.config.orgId, take]
      );
      budget -= take;
      movedFromDonor += take;
    }
    if (movedFromDonor > 0) {
      await adjustStripe(lock, donor.stripeNo, { available: -movedFromDonor });
      await adjustStripe(lock, target, { available: movedFromDonor });
      needed -= movedFromDonor;
    }
  }
  return amount - needed;
}

/**
 * FULL mode only: move free credits into stripes whose reservations exceed their effective credits,
 * so reservations stay servable from their own stripe whenever the org as a whole can back them.
 */
export async function rebalanceReservations(lock: WalletLock): Promise<void> {
  for (const stripe of [...lock.stripes.values()].sort((a, b) => a.stripeNo - b.stripeNo)) {
    const deficit = -headroomOf(stripe);
    if (deficit > 0) {
      await moveFreeCredits(lock, stripe.stripeNo, deficit);
    }
  }
}

/** Split `total` credits evenly over `n` stripes (remainder to the lowest stripes). */
export function splitAcrossStripes(total: number, n: number): number[] {
  const base = Math.floor(total / n);
  const extra = total % n;
  return Array.from({ length: n }, (_, i) => base + (i < extra ? 1 : 0));
}

/** FULL mode only: create the per-stripe lots of a freshly inserted grant and credit the stripes. */
export async function creditNewGrant(lock: WalletLock, grantId: string, credits: number): Promise<void> {
  if (lock.mode !== 'FULL') throw new Error('creditNewGrant requires a FULL wallet lock');
  const portions = splitAcrossStripes(credits, lock.config.stripeCount);
  for (const [stripeNo, portion] of portions.entries()) {
    if (portion === 0) continue;
    await lock.client.query(
      `INSERT INTO credit_lots (grant_id, stripe_no, org_id, remaining_credits) VALUES ($1, $2, $3, $4)`,
      [grantId, stripeNo, lock.config.orgId, portion]
    );
    await adjustStripe(lock, stripeNo, { available: portion });
  }
}
