import { expect } from 'vitest';
import { query } from '../../src/db';

export interface WalletState {
  /** Sum of available credits over all wallet stripes. */
  cached: number;
  /** Sum of reserved credits over all wallet stripes. */
  reserved: number;
  activeLotCredits: number;
  activeHoldCredits: number;
  usageCharged: number;
  allocated: number;
  rolledUp: number;
}

export async function getWalletState(orgId: string): Promise<WalletState> {
  const res = await query<Record<keyof WalletState, string>>(
    `SELECT
       (SELECT COALESCE(SUM(available_credits), 0) FROM org_wallet_stripes WHERE org_id = $1)::text AS cached,
       (SELECT COALESCE(SUM(reserved_credits), 0) FROM org_wallet_stripes WHERE org_id = $1)::text AS reserved,
       (SELECT COALESCE(SUM(l.remaining_credits), 0) FROM credit_lots l
         JOIN credit_grants g ON g.grant_id = l.grant_id
         WHERE l.org_id = $1 AND g.status = 'ACTIVE')::text AS "activeLotCredits",
       (SELECT COALESCE(SUM(p.reserved_credits), 0) FROM credit_hold_portions p
         JOIN credit_holds h ON h.hold_id = p.hold_id
         WHERE p.org_id = $1 AND h.status = 'ACTIVE')::text AS "activeHoldCredits",
       (SELECT COALESCE(SUM(credits_charged), 0) FROM usage_events WHERE org_id = $1)::text AS "usageCharged",
       (SELECT COALESCE(SUM(a.credits_deducted), 0) FROM credit_lot_allocations a
         JOIN usage_events u ON u.usage_id = a.usage_id WHERE u.org_id = $1)::text AS allocated,
       ((SELECT COALESCE(SUM(credits), 0) FROM usage_daily_rollups WHERE org_id = $1)
        + (SELECT COALESCE(SUM(credits), 0) FROM usage_rollup_deltas WHERE org_id = $1))::text AS "rolledUp"`,
    [orgId]
  );
  const row = res.rows[0];
  return Object.fromEntries(
    Object.entries(row).map(([k, v]) => [k, Number(v)])
  ) as unknown as WalletState;
}

/**
 * Wallet invariants that every committed state must satisfy, per stripe and therefore in total:
 *  - a stripe's available credits equal the credits left in its lots of ACTIVE grants,
 *  - a stripe's reserved credits equal the ACTIVE hold portions reserved on it (own holds and
 *    sub-orgs' cascaded portions), and every ACTIVE hold's portions add up to its estimate,
 *  - every charged credit is backed by a FIFO lot allocation and counted in the daily rollup,
 *  - nothing is negative (also enforced by CHECK constraints).
 */
export async function expectWalletInvariants(orgId: string): Promise<WalletState> {
  const mismatches = await query<{ stripe_no: number; available: string; lots: string; reserved: string; holds: string }>(
    `SELECT s.stripe_no,
            s.available_credits::text AS available,
            COALESCE((SELECT SUM(l.remaining_credits) FROM credit_lots l
                      JOIN credit_grants g ON g.grant_id = l.grant_id
                      WHERE l.org_id = s.org_id AND l.stripe_no = s.stripe_no AND g.status = 'ACTIVE'), 0)::text AS lots,
            s.reserved_credits::text AS reserved,
            COALESCE((SELECT SUM(p.reserved_credits) FROM credit_hold_portions p
                      JOIN credit_holds h ON h.hold_id = p.hold_id
                      WHERE p.org_id = s.org_id AND p.stripe_no = s.stripe_no AND h.status = 'ACTIVE'), 0)::text AS holds
     FROM org_wallet_stripes s
     WHERE s.org_id = $1
     ORDER BY s.stripe_no`,
    [orgId]
  );
  for (const stripe of mismatches.rows) {
    expect(Number(stripe.available), `stripe ${stripe.stripe_no}: available == lots`).toBe(Number(stripe.lots));
    expect(Number(stripe.reserved), `stripe ${stripe.stripe_no}: reserved == holds`).toBe(Number(stripe.holds));
    expect(Number(stripe.available)).toBeGreaterThanOrEqual(0);
    expect(Number(stripe.reserved)).toBeGreaterThanOrEqual(0);
  }

  const holdTotals = await query<{ hold_id: string; estimated: string; portions: string }>(
    `SELECT h.hold_id, h.estimated_credits::text AS estimated,
            COALESCE((SELECT SUM(p.reserved_credits) FROM credit_hold_portions p WHERE p.hold_id = h.hold_id), 0)::text AS portions
     FROM credit_holds h WHERE h.org_id = $1 AND h.status = 'ACTIVE'`,
    [orgId]
  );
  for (const hold of holdTotals.rows) {
    expect(Number(hold.portions), `hold ${hold.hold_id}: portions == estimate`).toBe(Number(hold.estimated));
  }

  const state = await getWalletState(orgId);
  expect(state.cached, 'available == active lot credits').toBe(state.activeLotCredits);
  expect(state.reserved, 'reserved == active hold estimates').toBe(state.activeHoldCredits);
  expect(state.usageCharged, 'charged == allocated').toBe(state.allocated);
  expect(state.rolledUp, 'daily rollup (totals + pending deltas) == allocated').toBe(state.allocated);
  return state;
}

export async function expireLot(grantId: string): Promise<void> {
  await query(
    `UPDATE credit_grants SET expires_at = NOW() - INTERVAL '1 minute' WHERE grant_id = $1`,
    [grantId]
  );
}

/** Remaining credits and derived status (ACTIVE / DEPLETED / EXPIRED / REVOKED) of a grant. */
export async function getGrant(grantId: string): Promise<{ status: string; remaining: number }> {
  const res = await query<{ status: string; remaining_credits: string }>(
    `SELECT status, remaining_credits FROM credit_grant_balances WHERE grant_id = $1`,
    [grantId]
  );
  return { status: res.rows[0].status, remaining: Number(res.rows[0].remaining_credits) };
}

/** Overwrite the stripe counters of an org so that they sum to `total` (simulates counter drift). */
export async function setAvailableTotal(orgId: string, total: number): Promise<void> {
  const stripes = await query<{ n: string }>(
    `SELECT COUNT(*)::text AS n FROM org_wallet_stripes WHERE org_id = $1`,
    [orgId]
  );
  const n = Number(stripes.rows[0].n);
  await query(
    `UPDATE org_wallet_stripes
     SET available_credits = $2::bigint / $3::int + CASE WHEN stripe_no < $2::bigint % $3::int THEN 1 ELSE 0 END
     WHERE org_id = $1`,
    [orgId, total, n]
  );
}

export async function getAvailableTotal(orgId: string): Promise<number> {
  const res = await query<{ total: string }>(
    `SELECT COALESCE(SUM(available_credits), 0)::text AS total FROM org_wallet_stripes WHERE org_id = $1`,
    [orgId]
  );
  return Number(res.rows[0].total);
}
