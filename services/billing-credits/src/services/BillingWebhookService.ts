import type { PoolClient } from 'pg';
import { withTransaction } from '../db';
import { balanceCacheKey, redis } from '../redis';
import { ResourceNotFoundError, StripeWebhookInput } from '../types/credits';
import { CreditLedgerService, creditLedgerService } from './CreditLedgerService';
import {
  adjustStripe,
  ensureOrgWallet,
  lockFullWallet,
  rebalanceReservations,
  totalsOf,
} from './OrgWallet';

export type StripeWebhookOutcome =
  | 'GRANTED'
  | 'REVOKED'
  | 'PARTIALLY_REVOKED'
  | 'DISCREPANCY_LOGGED'
  | 'DUPLICATE_IGNORED';

export interface StripeWebhookResult {
  eventId: string;
  invoiceId: string;
  outcome: StripeWebhookOutcome;
  grantId?: string;
  /** charge.refunded: credits refunded by this event (capped at what is still refundable). */
  refundedCredits?: number;
  /** charge.refunded: unspent credits clawed back from the wallet. */
  revokedCredits?: number;
  /** charge.refunded: refunded credits already consumed from the grant that cannot be clawed back. */
  unrecoveredCredits?: number;
  /** charge.refunded: held credits left without backing by the clawback (logged as a discrepancy). */
  unbackedHoldCredits?: number;
}

export class BillingWebhookService {
  constructor(private readonly ledger: CreditLedgerService = creditLedgerService) {}

  /**
   * Handle Stripe billing webhooks (`invoice.paid` for credit top-ups and `charge.refunded` for revocations).
   *
   * Each delivery is processed in one transaction that starts by claiming the event id with
   * INSERT ... ON CONFLICT DO NOTHING. A redelivery (sequential or concurrent) finds the claim and
   * becomes a no-op; a concurrent redelivery blocks on the uncommitted claim until the first
   * delivery commits (-> duplicate) or rolls back (-> it processes the event itself).
   */
  async handleStripeWebhook(input: StripeWebhookInput): Promise<StripeWebhookResult> {
    const { eventId, eventType, orgId, invoiceId } = input;

    const result = await withTransaction(async (client): Promise<StripeWebhookResult> => {
      const claim = await client.query(
        `INSERT INTO stripe_webhook_events (event_id, event_type, org_id, invoice_id, outcome)
         VALUES ($1, $2, $3, $4, 'PROCESSING')
         ON CONFLICT (event_id) DO NOTHING
         RETURNING event_id`,
        [eventId, eventType, orgId, invoiceId]
      );

      if (claim.rowCount === 0) {
        const prior = await client.query<{ grant_id: string | null }>(
          `SELECT grant_id FROM stripe_webhook_events WHERE event_id = $1`,
          [eventId]
        );
        return {
          eventId,
          invoiceId,
          outcome: 'DUPLICATE_IGNORED',
          grantId: prior.rows[0]?.grant_id ?? undefined,
        };
      }

      const applied =
        eventType === 'invoice.paid'
          ? await this.applyInvoicePaid(client, input)
          : await this.applyChargeRefunded(client, input);

      await client.query(
        `UPDATE stripe_webhook_events
         SET outcome = $1,
             grant_id = $2
         WHERE event_id = $3`,
        [applied.outcome, applied.grantId ?? null, eventId]
      );

      return { eventId, invoiceId, ...applied };
    });

    if (result.outcome !== 'DUPLICATE_IGNORED') {
      await redis.del(balanceCacheKey(orgId));
    }
    return result;
  }

  private async applyInvoicePaid(
    client: PoolClient,
    input: StripeWebhookInput
  ): Promise<Omit<StripeWebhookResult, 'eventId' | 'invoiceId'>> {
    const { orgId, invoiceId, credits } = input;
    const expiresInDays = input.expiresInDays ?? 365;

    // Paid invoices may be the first thing we hear about an org; make sure its wallet exists,
    // then take a FULL wallet lock (the grant is split across every stripe).
    await ensureOrgWallet(client, orgId);
    const wallet = await lockFullWallet(client, orgId);

    // Second line of defence behind event-id dedupe: never grant the same invoice twice.
    const existing = await client.query<{ grant_id: string }>(
      `SELECT grant_id FROM credit_grants
       WHERE org_id = $1 AND invoice_id = $2
       ORDER BY created_at ASC
       LIMIT 1`,
      [orgId, invoiceId]
    );
    if (existing.rowCount) {
      return { outcome: 'DUPLICATE_IGNORED', grantId: existing.rows[0].grant_id };
    }

    const expiresAt = new Date(Date.now() + expiresInDays * 24 * 60 * 60 * 1000).toISOString();
    const grant = await this.ledger.createGrant(
      { orgId, source: 'PREPAID_INVOICE', credits, expiresAt, invoiceId },
      { wallet }
    );

    return { outcome: 'GRANTED', grantId: grant.grantId };
  }

  /**
   * Claw back a refunded invoice's grant under a FULL wallet lock (all stripes, then the grant row):
   *   - refunds accumulate in `refunded_credits` (capped at the grant size); a fully refunded grant
   *     becomes REVOKED, a partial refund leaves it ACTIVE with fewer credits,
   *   - unspent credits are removed from the grant's per-stripe lots and the owning stripes,
   *   - refunded credits that were already consumed (per the FIFO allocations) and cannot be clawed
   *     back are logged once as REFUND_UNRECOVERABLE,
   *   - if the clawback leaves active holds without backing, the new shortfall is logged as
   *     REVOKED_DURING_ACTIVE_HOLD.
   */
  private async applyChargeRefunded(
    client: PoolClient,
    input: StripeWebhookInput
  ): Promise<Omit<StripeWebhookResult, 'eventId' | 'invoiceId'>> {
    const { orgId, invoiceId, originalEventId } = input;

    const wallet = await lockFullWallet(client, orgId);

    let targetGrantId: string | null = null;
    if (originalEventId) {
      const original = await client.query<{ grant_id: string | null }>(
        `SELECT grant_id FROM stripe_webhook_events
         WHERE event_id = $1 AND event_type = 'invoice.paid' AND org_id = $2 AND invoice_id = $3`,
        [originalEventId, orgId, invoiceId]
      );
      targetGrantId = original.rows[0]?.grant_id ?? null;
      if (!targetGrantId) {
        throw new ResourceNotFoundError(
          'CreditGrant',
          `for original event ${originalEventId} (org ${orgId}, invoice ${invoiceId})`
        );
      }
    }

    const grantColumns = `grant_id, status, initial_credits, refunded_credits, (expires_at <= NOW()) AS is_expired`;
    const grantRes = await client.query<{
      grant_id: string;
      status: string;
      initial_credits: string | number;
      refunded_credits: string | number;
      is_expired: boolean;
    }>(
      targetGrantId
        ? `SELECT ${grantColumns}
           FROM credit_grants
           WHERE grant_id = $3 AND org_id = $1 AND invoice_id = $2
           FOR UPDATE`
        : `SELECT ${grantColumns}
           FROM credit_grants
           WHERE org_id = $1 AND invoice_id = $2
           ORDER BY (status = 'REVOKED') ASC, created_at ASC
           LIMIT 1
           FOR UPDATE`,
      targetGrantId ? [orgId, invoiceId, targetGrantId] : [orgId, invoiceId]
    );

    if (grantRes.rowCount === 0) {
      throw new ResourceNotFoundError('CreditGrant', `for invoice ${invoiceId} (org ${orgId})`);
    }

    const grant = grantRes.rows[0];
    const initialCredits = Number(grant.initial_credits);
    const alreadyRefunded = Number(grant.refunded_credits);
    const refundedCredits = Math.min(input.credits, initialCredits - alreadyRefunded);
    if (grant.status === 'REVOKED' || refundedCredits <= 0) {
      // A different refund event for an already fully clawed-back grant.
      return { outcome: 'DUPLICATE_IGNORED', grantId: grant.grant_id };
    }

    const effectiveBefore = totalsOf(wallet.stripes.values()).effectiveAvailableCredits;
    const fullyRefunded = alreadyRefunded + refundedCredits >= initialCredits;

    // Claw back unspent credits from the grant's lots (only an ACTIVE grant still has any).
    const lotsRes = await client.query<{ stripe_no: number; remaining_credits: string | number }>(
      `SELECT stripe_no, remaining_credits FROM credit_lots
       WHERE grant_id = $1 AND remaining_credits > 0
       ORDER BY stripe_no`,
      [grant.grant_id]
    );
    let toClaw = fullyRefunded ? Number.MAX_SAFE_INTEGER : refundedCredits;
    let revokedCredits = 0;
    for (const lot of lotsRes.rows) {
      if (toClaw <= 0) break;
      const stripeNo = Number(lot.stripe_no);
      const take = Math.min(Number(lot.remaining_credits), toClaw);
      await client.query(
        `UPDATE credit_lots SET remaining_credits = remaining_credits - $1, updated_at = NOW()
         WHERE grant_id = $2 AND stripe_no = $3`,
        [take, grant.grant_id, stripeNo]
      );
      await adjustStripe(wallet, stripeNo, { available: -take });
      if (grant.is_expired) {
        // These credits were already excluded from the effective balance.
        wallet.stripes.get(stripeNo)!.unsweptExpiredCredits -= take;
      }
      toClaw -= take;
      revokedCredits += take;
    }

    await client.query(
      `UPDATE credit_grants
       SET refunded_credits = refunded_credits + $1,
           status = CASE WHEN $2::boolean THEN 'REVOKED' ELSE status END,
           updated_at = NOW()
       WHERE grant_id = $3`,
      [refundedCredits, fullyRefunded, grant.grant_id]
    );

    // Refunded credits that could not be clawed back came from consumption (logged) or expiry.
    const consumedRes = await client.query<{ consumed: string; logged: string }>(
      `SELECT
         (SELECT COALESCE(SUM(credits_deducted), 0) FROM credit_lot_allocations WHERE grant_id = $1)::text AS consumed,
         (SELECT COALESCE(SUM(unrecovered_credits), 0) FROM billing_discrepancies
           WHERE grant_id = $1 AND discrepancy_type = 'REFUND_UNRECOVERABLE')::text AS logged`,
      [grant.grant_id]
    );
    const unloggedConsumption = Math.max(
      0,
      Number(consumedRes.rows[0].consumed) - Number(consumedRes.rows[0].logged)
    );
    const unrecoveredCredits = Math.min(
      Math.max(0, refundedCredits - Math.min(revokedCredits, refundedCredits)),
      unloggedConsumption
    );

    if (unrecoveredCredits > 0) {
      await this.ledger.logDiscrepancy(wallet, {
        type: 'REFUND_UNRECOVERABLE',
        invoiceId,
        grantId: grant.grant_id,
        unrecovered: unrecoveredCredits,
        reason:
          `Refunded ${refundedCredits} credits of invoice ${invoiceId}: ${unrecoveredCredits} were already ` +
          `consumed from the grant and cannot be clawed back (${revokedCredits} unspent credits revoked)`,
      });
    }

    const totalsAfter = totalsOf(wallet.stripes.values());
    const uncoveredBefore = Math.max(0, totalsAfter.reservedCredits - effectiveBefore);
    const uncoveredAfter = Math.max(0, totalsAfter.reservedCredits - totalsAfter.effectiveAvailableCredits);
    const unbackedHoldCredits = uncoveredAfter - uncoveredBefore;
    if (unbackedHoldCredits > 0) {
      await this.ledger.logDiscrepancy(wallet, {
        type: 'REVOKED_DURING_ACTIVE_HOLD',
        invoiceId,
        grantId: grant.grant_id,
        unrecovered: unbackedHoldCredits,
        reason:
          `Refund clawback of ${revokedCredits} credits left active holds reserving ` +
          `${totalsAfter.reservedCredits} credits backed by only ${totalsAfter.effectiveAvailableCredits}. ` +
          `Active holds: ${await this.ledger.activeHoldIds(wallet)}`,
      });
    }

    await rebalanceReservations(wallet);

    return {
      outcome: unrecoveredCredits > 0 ? 'DISCREPANCY_LOGGED' : fullyRefunded ? 'REVOKED' : 'PARTIALLY_REVOKED',
      grantId: grant.grant_id,
      refundedCredits,
      revokedCredits,
      ...(unrecoveredCredits > 0 ? { unrecoveredCredits } : {}),
      ...(unbackedHoldCredits > 0 ? { unbackedHoldCredits } : {}),
    };
  }
}

export const billingWebhookService = new BillingWebhookService();
