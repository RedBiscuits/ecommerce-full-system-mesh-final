import crypto from 'crypto';
import type { PoolClient } from 'pg';
import { config } from '../config/env';
import { query, withTransaction } from '../db';
import {
  creditNewGrant,
  lockFullWallet,
  OrgConfig,
  rebalanceReservations,
  totalsOf,
  WalletLock,
} from './OrgWallet';
import { ConfigureAutoRechargeInput, ResourceNotFoundError } from '../types/credits';

export interface AutoRechargeTriggerResult {
  triggered: boolean;
  rechargeId?: string;
  grantId?: string;
  creditsAdded?: number;
  reason?: string;
}

export class AutoRechargeService {
  /**
   * Update organization auto-recharge configuration.
   */
  async configureAutoRecharge(
    orgId: string,
    input: ConfigureAutoRechargeInput
  ): Promise<{
    orgId: string;
    enabled: boolean;
    thresholdCredits: number;
    amountCredits: number;
    monthlyCapCredits: number;
  }> {
    const res = await query(
      `UPDATE org_balances
       SET auto_recharge_enabled = $1,
           auto_recharge_threshold_credits = $2,
           auto_recharge_amount_credits = $3,
           monthly_recharge_cap_credits = $4,
           updated_at = NOW()
       WHERE org_id = $5`,
      [
        input.enabled,
        input.thresholdCredits,
        input.amountCredits,
        input.monthlyCapCredits,
        orgId,
      ]
    );

    if (res.rowCount === 0) {
      throw new ResourceNotFoundError('Organization', orgId);
    }

    return {
      orgId,
      enabled: input.enabled,
      thresholdCredits: input.thresholdCredits,
      amountCredits: input.amountCredits,
      monthlyCapCredits: input.monthlyCapCredits,
    };
  }

  /**
   * Evaluate whether an organization's net spendable balance has fallen at or below its
   * auto-recharge threshold and, if eligible (monthly cap), issue a top-up grant - synchronously,
   * under a FULL wallet lock. The exact org-wide balance is used (lots past expiry excluded) and
   * concurrent evaluations serialize, so only the first one observing the low balance tops up.
   *
   * This is the relay's evaluation step; deductions never call it (they enqueue outbox intents).
   * `intentId` ties the order to the outbox intent that requested it (unique: one order per intent).
   */
  async evaluateAndTriggerIfNeeded(
    orgId: string,
    wallet?: WalletLock,
    intentId?: string
  ): Promise<AutoRechargeTriggerResult> {
    if (wallet) {
      return this.evaluateLocked(wallet, intentId ?? null);
    }
    return withTransaction(async (client) =>
      this.evaluateLocked(await lockFullWallet(client, orgId), intentId ?? null)
    );
  }

  private async evaluateLocked(lock: WalletLock, intentId: string | null): Promise<AutoRechargeTriggerResult> {
    if (lock.mode !== 'FULL') {
      throw new Error('Auto-recharge evaluation requires a FULL wallet lock');
    }
    const { client, config } = lock;
    const orgId = config.orgId;
    if (!config.autoRecharge.enabled) {
      return { triggered: false, reason: 'AUTO_RECHARGE_DISABLED' };
    }

    const netSpendable = totalsOf(lock.stripes.values()).netSpendableCredits;
    const threshold = config.autoRecharge.thresholdCredits;
    const rechargeAmount = config.autoRecharge.amountCredits;
    const monthlyCap = config.autoRecharge.monthlyCapCredits;

    if (netSpendable > threshold) {
      return { triggered: false, reason: 'ABOVE_THRESHOLD' };
    }

    const monthUsageRes = await client.query<{ total: string }>(
      `SELECT COALESCE(SUM(credits_added), 0)::text AS total
       FROM auto_recharge_orders
       WHERE org_id = $1
         AND status = 'COMPLETED'
         AND created_at >= date_trunc('month', NOW())`,
      [orgId]
    );

    const monthRechargedSoFar = Number(monthUsageRes.rows[0].total);
    if (monthRechargedSoFar + rechargeAmount > monthlyCap) {
      return { triggered: false, reason: 'MONTHLY_CAP_REACHED' };
    }

    const grantId = `grant_auto_${crypto.randomUUID()}`;
    const rechargeId = `ar_${crypto.randomUUID()}`;
    const invoiceId = `inv_auto_${crypto.randomUUID()}`;
    const expiresAt = new Date(Date.now() + 365 * 24 * 60 * 60 * 1000);

    await client.query(
      `INSERT INTO credit_grants (grant_id, org_id, source, invoice_id, initial_credits, priority, status, expires_at)
       VALUES ($1, $2, 'AUTO_RECHARGE', $3, $4, 20, 'ACTIVE', $5)`,
      [grantId, orgId, invoiceId, rechargeAmount, expiresAt]
    );
    await creditNewGrant(lock, grantId, rechargeAmount);
    await rebalanceReservations(lock);

    await client.query(
      `INSERT INTO auto_recharge_orders (
         recharge_id, org_id, intent_id, grant_id, invoice_id, credits_added, trigger_balance_credits, status
       )
       VALUES ($1, $2, $3, $4, $5, $6, $7, 'COMPLETED')`,
      [rechargeId, orgId, intentId, grantId, invoiceId, rechargeAmount, netSpendable]
    );

    return {
      triggered: true,
      rechargeId,
      grantId,
      creditsAdded: rechargeAmount,
    };
  }

  /* ----------------------------------------------------------------------------------------------
   * Transactional outbox
   * -------------------------------------------------------------------------------------------- */

  /**
   * Called inside a deduction / reservation transaction after it changed `org`'s balance. When the
   * observed balance is at or below the threshold, commit an outbox intent together with the
   * deduction - a plain INSERT (no unique index, no lock on a shared row), so it never blocks and
   * never waits for a top-up. If an intent is already pending it is reused. Duplicate intents from
   * racing transactions are harmless: the relay coalesces an org's pending intents into at most one
   * order after re-checking the balance exactly.
   */
  async enqueueIntentIfNeeded(
    client: PoolClient,
    org: OrgConfig,
    observedSpendableCredits: number,
    usageId: string | null
  ): Promise<string | null> {
    if (!org.autoRecharge.enabled || observedSpendableCredits > org.autoRecharge.thresholdCredits) {
      return null;
    }
    const pending = await client.query<{ intent_id: string }>(
      `SELECT intent_id FROM auto_recharge_outbox
       WHERE org_id = $1 AND status = 'PENDING'
       ORDER BY created_at ASC
       LIMIT 1`,
      [org.orgId]
    );
    if (pending.rowCount) return pending.rows[0].intent_id;

    const intentId = `ari_${crypto.randomUUID()}`;
    await client.query(
      `INSERT INTO auto_recharge_outbox (intent_id, org_id, triggered_by_usage_id, observed_spendable_credits)
       VALUES ($1, $2, $3, $4)`,
      [intentId, org.orgId, usageId, observedSpendableCredits]
    );
    return intentId;
  }

  /**
   * Outbox relay: for every org with pending intents (or the given orgs), one transaction takes a
   * FULL wallet lock, claims the org's pending intents, evaluates the threshold exactly and creates
   * at most one order; all claimed intents are marked COMPLETED (with the order) or SKIPPED.
   * A failure leaves the intents pending (attempts++) for the next run; the deductions that
   * enqueued them are long committed and unaffected.
   */
  async processOutbox(orgIds?: string[]): Promise<OutboxRunResult> {
    const targets =
      orgIds ??
      (
        await query<{ org_id: string }>(
          `SELECT DISTINCT org_id FROM auto_recharge_outbox WHERE status = 'PENDING' ORDER BY org_id`
        )
      ).rows.map((r) => r.org_id);

    const summary: OutboxRunResult = { ordersCreated: 0, intentsCompleted: 0, intentsSkipped: 0, failures: 0 };
    for (const orgId of [...new Set(targets)]) {
      try {
        const outcome = await withTransaction(async (client) => {
          const lock = await lockFullWallet(client, orgId);
          const intents = await client.query<{ intent_id: string }>(
            `SELECT intent_id FROM auto_recharge_outbox
             WHERE org_id = $1 AND status = 'PENDING'
             ORDER BY created_at ASC, intent_id ASC
             FOR UPDATE SKIP LOCKED`,
            [orgId]
          );
          if (intents.rowCount === 0) return null;
          const ids = intents.rows.map((r) => r.intent_id);
          const result = await this.evaluateAndTriggerIfNeeded(orgId, lock, ids[0]);
          await client.query(
            `UPDATE auto_recharge_outbox
             SET status = $1, recharge_id = $2, reason = $3, processed_at = NOW()
             WHERE intent_id = ANY($4::varchar[])`,
            [result.triggered ? 'COMPLETED' : 'SKIPPED', result.rechargeId ?? null, result.reason ?? null, ids]
          );
          return { result, count: ids.length };
        });
        if (!outcome) continue;
        if (outcome.result.triggered) {
          summary.ordersCreated += 1;
          summary.intentsCompleted += outcome.count;
        } else {
          summary.intentsSkipped += outcome.count;
        }
      } catch (err) {
        summary.failures += 1;
        console.error(`[AutoRechargeOutbox] processing ${orgId} failed:`, err);
        await query(
          `UPDATE auto_recharge_outbox SET attempts = attempts + 1, last_error = $2
           WHERE org_id = $1 AND status = 'PENDING'`,
          [orgId, (err as Error).message]
        ).catch(() => undefined);
      }
    }
    return summary;
  }

  private readonly inFlight = new Set<Promise<unknown>>();

  /**
   * Fire-and-forget relay run for orgs that just enqueued intents, started after the enqueuing
   * transaction committed. The caller does not wait for it (no synchronous top-up on the request
   * path); the maintenance worker picks up anything a kick misses.
   */
  kickOutbox(orgIds: string[]): void {
    if (orgIds.length === 0 || !this.autoKick) return;
    const run: Promise<unknown> = Promise.resolve()
      .then(() => this.processOutbox(orgIds))
      .catch((err) => console.error('[AutoRechargeOutbox] kick failed:', err))
      .finally(() => this.inFlight.delete(run));
    this.inFlight.add(run);
  }

  /** Resolves once every kicked relay run has finished (tests, graceful shutdown). */
  async whenOutboxIdle(): Promise<void> {
    while (this.inFlight.size > 0) {
      await Promise.allSettled([...this.inFlight]);
    }
  }

  /** Kicks can be turned off (e.g. AUTO_RECHARGE_OUTBOX_KICK=false) to rely on the worker only. */
  autoKick = config.autoRechargeOutboxKick;
}

export interface OutboxRunResult {
  ordersCreated: number;
  intentsCompleted: number;
  intentsSkipped: number;
  failures: number;
}

export const autoRechargeService = new AutoRechargeService();
