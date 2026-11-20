import type { PoolClient } from 'pg';
import { InsufficientCreditsError } from '../types/credits';
import {
  adjustStripe,
  applyDraw,
  candidateStripes,
  clientQuery,
  DrawSlice,
  headroomOf,
  lockFullWallet,
  lockIdleStripe,
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

/*
 * Operations along an org chain [consumer, parent, grandparent, ...].
 *
 * Each level is locked only when it is reached, always consumer first and then ancestors in order
 * (strictly decreasing depth), which is the canonical lock order; within a level the STRIPE -> FULL
 * escalation only rolls back that level's locks. So cascading draws and reservations never wait on
 * a lock that sorts before one they hold.
 */

export interface ChainDraw {
  locks: Map<string, WalletLock>;
  snapshots: Map<string, OrgWalletSnapshot>;
  slices: DrawSlice[];
  charged: number;
  shortfall: number;
}

export interface ChainDrawHooks {
  /** Stripes that must be locked at an org (hold home stripe, portion stripes). */
  pinnedStripes?: (org: OrgConfig) => number[];
  /** Reserved credits of the hold being settled, per stripe of an org: usable by this draw. */
  portionCredits?: (org: OrgConfig) => Map<number, number>;
  /** Runs right after an org's locks are taken (and again after an escalation of that org). */
  onLocked?: (org: OrgConfig, lock: WalletLock, level: number) => Promise<void>;
  /** Highest chain level that must be locked even when nothing is left to draw. */
  lastRequiredLevel?: () => number;
}

const sumPortions = (portions: Map<number, number>, stripeNos: Iterable<number>) => {
  let total = 0;
  for (const stripeNo of stripeNos) total += portions.get(stripeNo) ?? 0;
  return total;
};

async function snapshotOf(
  client: PoolClient,
  org: OrgConfig,
  snapshots: Map<string, OrgWalletSnapshot>
): Promise<OrgWalletSnapshot> {
  let snapshot = snapshots.get(org.orgId);
  if (!snapshot) {
    snapshot = await readOrgWallet(clientQuery(client), org.orgId, org);
    snapshots.set(org.orgId, snapshot);
  }
  return snapshot;
}

/**
 * Draw `amount` credits own-pool-first along the chain, FIFO within each org. An org contributes its
 * free credits (effective minus reservations) plus the settling hold's own portions on it; the rest
 * cascades to the next ancestor. Throws InsufficientCreditsError (before the final level writes
 * anything) when the chain cannot cover the amount, unless `allowShortfall`.
 */
export async function drawAlongChain(
  client: PoolClient,
  chain: OrgConfig[],
  amount: number,
  affinityKey: string,
  hooks: ChainDrawHooks = {},
  allowShortfall = false
): Promise<ChainDraw> {
  const result: ChainDraw = { locks: new Map(), snapshots: new Map(), slices: [], charged: 0, shortfall: 0 };
  let remaining = amount;

  for (let level = 0; level < chain.length; level++) {
    const org = chain[level];
    const required = level === 0 || level <= (hooks.lastRequiredLevel?.() ?? -1);
    if (remaining <= 0 && !required) break;

    const need = remaining;
    const isLast = level === chain.length - 1;
    const pinned = () => hooks.pinnedStripes?.(org) ?? [];
    if (need <= 0 && pinned().length === 0) continue; // nothing to draw or release at this org
    const portions = () => hooks.portionCredits?.(org) ?? new Map<number, number>();

    const levelDraw = await withStripeOrFullLock(
      client,
      async () => {
        let lock: WalletLock;
        const pinnedStripes = pinned();
        if (pinnedStripes.length > 0) {
          lock = await lockStripes(client, org, pinnedStripes);
        } else {
          const candidates = candidateStripes(await snapshotOf(client, org, result.snapshots), need);
          if (candidates.length === 0) return null;
          lock = await lockIdleStripe(client, org, candidates, affinityKey);
        }
        await hooks.onLocked?.(org, lock, level);
        const budget =
          [...lock.stripes.values()].reduce((sum, s) => sum + headroomOf(s), 0) +
          sumPortions(portions(), lock.stripes.keys());
        if (need > budget) return null; // only a FULL lock knows the org's exact budget
        const slices = await planDraw(lock, need);
        if (!slices) return null;
        return async () => {
          await applyDraw(lock, slices);
          return { lock, slices, taken: need };
        };
      },
      async () => {
        const lock = await lockFullWallet(client, org.orgId, org);
        await hooks.onLocked?.(org, lock, level);
        const totals = totalsOf(lock.stripes.values());
        const budget = Math.max(0, totals.netSpendableCredits + sumPortions(portions(), lock.stripes.keys()));
        const taken = Math.min(need, budget);
        if (isLast && taken < need && !allowShortfall) {
          throw new InsufficientCreditsError(chain[0].orgId, amount, amount - need + taken);
        }
        const slices = await planDraw(lock, taken);
        if (!slices) {
          // Stripe counters claim more than the lots hold (drift): nothing written at this level.
          throw new InsufficientCreditsError(chain[0].orgId, amount, amount - need);
        }
        await applyDraw(lock, slices);
        return { lock, slices, taken };
      }
    );

    result.locks.set(org.orgId, levelDraw.lock);
    result.slices.push(...levelDraw.slices);
    remaining -= levelDraw.taken;
  }

  if (remaining > 0 && !allowShortfall) {
    throw new InsufficientCreditsError(chain[0].orgId, amount, amount - remaining);
  }
  result.charged = amount - remaining;
  result.shortfall = remaining;
  return result;
}

export interface ChainReservation {
  locks: Map<string, WalletLock>;
  snapshots: Map<string, OrgWalletSnapshot>;
  /** Reserved portions in acquisition order (consumer first). */
  portions: Array<{ orgId: string; stripeNo: number; credits: number }>;
  /** Consumer stripe that owns the hold row. */
  homeStripe: number;
}

/**
 * Reserve `amount` credits own-pool-first along the chain. Each org reserves what its free credits
 * allow on a single stripe (gathering free credits onto it under a FULL lock if needed); the deficit
 * cascades to the next ancestor. With `homeStripe`, the consumer's portion must land on that stripe
 * (extending an existing hold). Throws InsufficientCreditsError when the chain cannot cover it.
 */
export async function reserveAlongChain(
  client: PoolClient,
  chain: OrgConfig[],
  amount: number,
  affinityKey: string,
  options: {
    homeStripe?: number;
    onLocked?: (org: OrgConfig, lock: WalletLock, level: number) => Promise<void>;
    /** How to report a shortfall when `chain` is the ancestor part of a larger request. */
    error?: { orgId: string; requested: number; covered: number };
  } = {}
): Promise<ChainReservation> {
  const insufficient = (reserved: number) =>
    options.error
      ? new InsufficientCreditsError(options.error.orgId, options.error.requested, options.error.covered + reserved)
      : new InsufficientCreditsError(chain[0].orgId, amount, reserved);
  const result: ChainReservation = {
    locks: new Map(),
    snapshots: new Map(),
    portions: [],
    homeStripe: options.homeStripe ?? -1,
  };
  let remaining = amount;

  for (let level = 0; level < chain.length && (remaining > 0 || level === 0); level++) {
    const org = chain[level];
    const need = remaining;
    const isLast = level === chain.length - 1;
    const fixedStripe = level === 0 ? options.homeStripe : undefined;

    const levelResult = await withStripeOrFullLock(
      client,
      async () => {
        let lock: WalletLock;
        if (fixedStripe !== undefined) {
          lock = await lockStripes(client, org, [fixedStripe]);
        } else {
          const candidates = candidateStripes(await snapshotOf(client, org, result.snapshots), need);
          if (candidates.length === 0) return null;
          lock = await lockIdleStripe(client, org, candidates, affinityKey);
        }
        await options.onLocked?.(org, lock, level);
        const [stripe] = lock.stripes.values();
        if (headroomOf(stripe) < need) return null;
        return async () => {
          if (need > 0) await adjustStripe(lock, stripe.stripeNo, { reserved: need });
          return { lock, stripeNo: stripe.stripeNo, taken: need };
        };
      },
      async () => {
        const lock = await lockFullWallet(client, org.orgId, org);
        await options.onLocked?.(org, lock, level);
        const taken = Math.min(need, Math.max(0, totalsOf(lock.stripes.values()).netSpendableCredits));
        if (isLast && taken < need) {
          throw insufficient(amount - need + taken);
        }
        const target =
          fixedStripe ??
          [...lock.stripes.values()].sort((a, b) => headroomOf(b) - headroomOf(a) || a.stripeNo - b.stripeNo)[0]
            .stripeNo;
        if (taken > 0) {
          const missing = taken - headroomOf(lock.stripes.get(target)!);
          if (missing > 0) await moveFreeCredits(lock, target, missing);
          await adjustStripe(lock, target, { reserved: taken });
        }
        return { lock, stripeNo: target, taken };
      }
    );

    result.locks.set(org.orgId, levelResult.lock);
    if (level === 0 && result.homeStripe < 0) result.homeStripe = levelResult.stripeNo;
    if (levelResult.taken > 0) {
      result.portions.push({ orgId: org.orgId, stripeNo: levelResult.stripeNo, credits: levelResult.taken });
    }
    remaining -= levelResult.taken;
  }

  if (remaining > 0) {
    throw insufficient(amount - remaining);
  }
  return result;
}

/**
 * Estimated net spendable credits of an org after this transaction's writes: exact for a FULL lock,
 * otherwise the locked stripes' current values plus the lock-free snapshot of the other stripes.
 */
export function estimatedNetSpendable(
  lock: WalletLock,
  snapshot: OrgWalletSnapshot | undefined
): number {
  if (lock.mode === 'FULL' || !snapshot) {
    return totalsOf(lock.stripes.values()).netSpendableCredits;
  }
  return totalsOf([
    ...snapshot.stripes.filter((s) => !lock.stripes.has(s.stripeNo)),
    ...lock.stripes.values(),
  ]).netSpendableCredits;
}
