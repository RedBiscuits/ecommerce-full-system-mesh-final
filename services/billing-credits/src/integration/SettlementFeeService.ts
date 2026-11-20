import type { PoolClient } from 'pg';
import { query, withTransaction } from '../db';
import {
  SettlementFeeError,
  SettlementFeeRuleSchema,
  calculateSettlementFee,
  type FeeQuote,
  type SettlementFeeRule,
} from '../services/SettlementFeeCalculator';
import {
  CONSUMERS,
  EVENT_TYPES,
  EXCHANGES,
  PRODUCERS,
  ROUTING_KEYS,
  buildIntegrationEvent,
  deterministicEventId,
  type MerchantSettlementCollectedEvent,
  type SettlementFee,
  type SettlementFeeCalculatedPayload,
} from '../../../../shared/contracts';
import {
  NonRetryableError,
  claimCheckpoint,
  completeCheckpoint,
  enqueueIntegrationEvent,
  listCheckpoints,
  type Checkpoint,
} from '../../../../shared/messaging';

export const BILLING_SETTLEMENT_FEE_CONSUMER = CONSUMERS.billingSettlementFees;

export type SettlementFeeOutcome = 'PRICED' | 'ALREADY_PRICED' | 'DUPLICATE_IGNORED';

export interface SettlementFeeResult {
  outcome: SettlementFeeOutcome;
  batchId: string;
  fee?: SettlementFee;
  payoutHalalas?: number;
  /** eventId of the billing.settlement-fee.calculated event (deterministic per batch). */
  feeEventId?: string;
}

export interface SettlementFeeQuoteRecord {
  batchId: string;
  merchantId: string;
  currency: string;
  itemCount: number;
  grossHalalas: number;
  fee: SettlementFee;
  payoutHalalas: number;
  sourceEventId: string;
  feeEventId: string;
  createdAt: string;
}

interface RuleRow {
  rule_id: string;
  version: number;
  merchant_id: string | null;
  tiers: unknown;
  per_transaction_halalas: string | number;
  minimum_fee_halalas: string | number;
  vat_rate_bps: number;
  active: boolean;
}

interface QuoteRow {
  batch_id: string;
  merchant_id: string;
  currency: string;
  item_count: string | number;
  gross_halalas: string | number;
  fee: SettlementFee;
  payout_halalas: string | number;
  source_event_id: string;
  fee_event_id: string;
  created_at: Date | string;
}

export interface NewFeeRuleInput {
  ruleId: string;
  merchantId: string | null;
  tiers: Array<{ upToHalalas: number | null; bps: number }>;
  perTransactionHalalas: number;
  minimumFeeHalalas: number;
}

function toRule(row: RuleRow): SettlementFeeRule {
  return SettlementFeeRuleSchema.parse({
    ruleId: row.rule_id,
    version: Number(row.version),
    merchantId: row.merchant_id,
    tiers: row.tiers,
    perTransactionHalalas: Number(row.per_transaction_halalas),
    minimumFeeHalalas: Number(row.minimum_fee_halalas),
    vatRateBps: Number(row.vat_rate_bps),
  });
}

function toQuote(row: QuoteRow): SettlementFeeQuoteRecord {
  return {
    batchId: row.batch_id,
    merchantId: row.merchant_id,
    currency: row.currency,
    itemCount: Number(row.item_count),
    grossHalalas: Number(row.gross_halalas),
    fee: row.fee,
    payoutHalalas: Number(row.payout_halalas),
    sourceEventId: row.source_event_id,
    feeEventId: row.fee_event_id,
    createdAt: new Date(row.created_at).toISOString(),
  };
}

/**
 * Billing's part of the merchant settlement workflow: price the processing fee of a collected
 * settlement batch with the merchant's tiered pricing rule (or the platform default).
 *
 * Exactly once, in ONE transaction:
 *   consumer checkpoint (billing.settlement-fees, eventId, 'done')
 *   + settlement_fee_quotes row (primary key = batch id: a batch is priced once, ever — a
 *     replay after a rule change still gets the original fee)
 *   + billing.settlement-fee.calculated outbox row (deterministic event id per batch)
 */
export class SettlementFeeService {
  async handleSettlementCollected(event: MerchantSettlementCollectedEvent): Promise<SettlementFeeResult> {
    const p = event.payload;
    return withTransaction(async (client): Promise<SettlementFeeResult> => {
      const key = { consumer: BILLING_SETTLEMENT_FEE_CONSUMER, eventId: event.eventId, step: 'done' };
      if (!(await claimCheckpoint(client, { ...key, correlationId: p.batchId }))) {
        return { outcome: 'DUPLICATE_IGNORED', batchId: p.batchId };
      }

      const existing = await client.query<QuoteRow>(`SELECT * FROM settlement_fee_quotes WHERE batch_id = $1 FOR UPDATE`, [p.batchId]);
      let quote: SettlementFeeQuoteRecord;
      let outcome: SettlementFeeOutcome;
      if (existing.rows.length > 0) {
        quote = toQuote(existing.rows[0]);
        if (quote.merchantId !== p.merchantId || quote.grossHalalas !== p.grossHalalas || quote.itemCount !== p.itemCount) {
          throw new NonRetryableError(
            'BATCH_MISMATCH',
            `Batch ${p.batchId} was priced for ${quote.merchantId} (${quote.itemCount} items, ${quote.grossHalalas}), ` +
              `now announced for ${p.merchantId} (${p.itemCount} items, ${p.grossHalalas})`
          );
        }
        outcome = 'ALREADY_PRICED';
      } else {
        const rule = await this.activeRuleFor(p.merchantId, client);
        const priced = this.price(rule, p.grossHalalas, p.itemCount);
        const feeEventId = deterministicEventId(EVENT_TYPES.settlementFeeCalculated, p.batchId);
        const inserted = await client.query<QuoteRow>(
          `INSERT INTO settlement_fee_quotes
             (batch_id, merchant_id, currency, item_count, gross_halalas, rule_id, rule_version, fee,
              fee_net_halalas, fee_vat_halalas, fee_total_halalas, payout_halalas, source_event_id, fee_event_id)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10, $11, $12, $13, $14)
           RETURNING *`,
          [
            p.batchId,
            p.merchantId,
            p.currency,
            p.itemCount,
            p.grossHalalas,
            rule.ruleId,
            rule.version,
            JSON.stringify(priced.fee),
            priced.fee.netHalalas,
            priced.fee.vatHalalas,
            priced.fee.totalHalalas,
            priced.payoutHalalas,
            event.eventId,
            feeEventId,
          ]
        );
        quote = toQuote(inserted.rows[0]);
        outcome = 'PRICED';
      }

      // Announced in the same transaction (a re-announcement of an already priced batch is a no-op:
      // the outbox refuses the same event id twice).
      const payload: SettlementFeeCalculatedPayload = {
        batchId: quote.batchId,
        merchantId: quote.merchantId,
        currency: 'SAR',
        itemCount: quote.itemCount,
        grossHalalas: quote.grossHalalas,
        fee: quote.fee,
        payoutHalalas: quote.payoutHalalas,
        calculatedAt: quote.createdAt,
      };
      await enqueueIntegrationEvent(client, {
        exchange: EXCHANGES.billing.name,
        routingKey: ROUTING_KEYS.settlementFeeCalculated,
        event: buildIntegrationEvent({
          eventId: quote.feeEventId,
          eventType: EVENT_TYPES.settlementFeeCalculated,
          producer: PRODUCERS.billing,
          correlationId: p.batchId,
          causationId: event.eventId,
          aggregateId: p.batchId,
          payload,
        }),
      });
      await completeCheckpoint(client, {
        ...key,
        outcome,
        detail: {
          ruleId: quote.fee.ruleId,
          ruleVersion: quote.fee.ruleVersion,
          feeNetHalalas: quote.fee.netHalalas,
          feeVatHalalas: quote.fee.vatHalalas,
          feeTotalHalalas: quote.fee.totalHalalas,
          payoutHalalas: quote.payoutHalalas,
          emittedEventId: quote.feeEventId,
        },
      });
      return { outcome, batchId: p.batchId, fee: quote.fee, payoutHalalas: quote.payoutHalalas, feeEventId: quote.feeEventId };
    });
  }

  /** Preview the fee a settlement would be charged now (nothing is stored). */
  async quote(merchantId: string, grossHalalas: number, itemCount: number): Promise<FeeQuote> {
    return this.price(await this.activeRuleFor(merchantId), grossHalalas, itemCount);
  }

  /** The merchant's active rule, else the platform default. */
  async activeRuleFor(merchantId: string, client?: PoolClient): Promise<SettlementFeeRule> {
    const sql = `SELECT * FROM settlement_fee_rules
       WHERE active AND (merchant_id = $1 OR merchant_id IS NULL)
       ORDER BY merchant_id NULLS LAST
       LIMIT 1`;
    const res = client ? await client.query<RuleRow>(sql, [merchantId]) : await query<RuleRow>(sql, [merchantId]);
    if (res.rows.length === 0) {
      throw new NonRetryableError('NO_FEE_RULE', `No active settlement fee rule for ${merchantId} and no platform default`);
    }
    try {
      return toRule(res.rows[0]);
    } catch (err) {
      throw new NonRetryableError('INVALID_FEE_RULE', `Fee rule ${res.rows[0].rule_id} is invalid: ${(err as Error).message}`);
    }
  }

  async listRules(): Promise<Array<SettlementFeeRule & { active: boolean }>> {
    const res = await query<RuleRow>(`SELECT * FROM settlement_fee_rules ORDER BY merchant_id NULLS FIRST, rule_id, version`);
    return res.rows.map((r) => ({ ...toRule(r), active: r.active }));
  }

  /**
   * Publish a new rule version for a merchant (or the default, merchantId null): the previous
   * active rule is retired in the same transaction. Batches already priced keep their fee.
   */
  async publishRule(input: NewFeeRuleInput): Promise<SettlementFeeRule> {
    return withTransaction(async (client) => {
      const prev = await client.query<{ version: number }>(
        `SELECT COALESCE(MAX(version), 0) AS version FROM settlement_fee_rules WHERE rule_id = $1`,
        [input.ruleId]
      );
      const rule = SettlementFeeRuleSchema.parse({
        ruleId: input.ruleId,
        version: Number(prev.rows[0].version) + 1,
        merchantId: input.merchantId,
        tiers: input.tiers,
        perTransactionHalalas: input.perTransactionHalalas,
        minimumFeeHalalas: input.minimumFeeHalalas,
        vatRateBps: 1500,
      });
      await client.query(
        `UPDATE settlement_fee_rules SET active = FALSE
         WHERE active AND COALESCE(merchant_id, '*') = COALESCE($1::varchar, '*')`,
        [input.merchantId]
      );
      await client.query(
        `INSERT INTO settlement_fee_rules
           (rule_id, version, merchant_id, currency, tiers, per_transaction_halalas, minimum_fee_halalas, vat_rate_bps, active)
         VALUES ($1, $2, $3, 'SAR', $4::jsonb, $5, $6, $7, TRUE)`,
        [rule.ruleId, rule.version, rule.merchantId, JSON.stringify(rule.tiers), rule.perTransactionHalalas, rule.minimumFeeHalalas, rule.vatRateBps]
      );
      return rule;
    });
  }

  /** What billing did for a settlement batch: the frozen fee and the consumer checkpoints. */
  async getBatchFee(batchId: string): Promise<{ quote: SettlementFeeQuoteRecord | null; checkpoints: Checkpoint[] }> {
    const res = await query<QuoteRow>(`SELECT * FROM settlement_fee_quotes WHERE batch_id = $1`, [batchId]);
    return {
      quote: res.rows[0] ? toQuote(res.rows[0]) : null,
      checkpoints: await listCheckpoints({ query: (text, params) => query(text, params) }, { correlationId: batchId }),
    };
  }

  private price(rule: SettlementFeeRule, grossHalalas: number, itemCount: number): FeeQuote {
    try {
      return calculateSettlementFee(rule, grossHalalas, itemCount);
    } catch (err) {
      if (err instanceof SettlementFeeError) throw new NonRetryableError(err.reason, err.message);
      throw err;
    }
  }
}

export const settlementFeeService = new SettlementFeeService();
