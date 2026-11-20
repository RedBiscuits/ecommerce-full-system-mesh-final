import { z } from 'zod';

export type PlanTier = 'STARTER' | 'GROWTH' | 'ENTERPRISE';

export type GrantSource =
  | 'PROMOTIONAL'
  | 'PREPAID_INVOICE'
  | 'ENTERPRISE_COMMIT'
  | 'AUTO_RECHARGE';

export type GrantStatus = 'ACTIVE' | 'DEPLETED' | 'EXPIRED' | 'REVOKED';
export type DiscrepancyType =
  | 'REFUND_UNRECOVERABLE'
  | 'EXPIRED_DURING_ACTIVE_HOLD'
  | 'REVOKED_DURING_ACTIVE_HOLD'
  | 'SETTLEMENT_SHORTFALL';

/** How usage was funded, for reporting: promotional, purchased (invoice/commit) or auto-recharge. */
export type FundingSource = 'PROMO' | 'PURCHASE' | 'AUTO_RECHARGE';

export function fundingSourceFor(source: GrantSource): FundingSource {
  if (source === 'PROMOTIONAL') return 'PROMO';
  if (source === 'AUTO_RECHARGE') return 'AUTO_RECHARGE';
  return 'PURCHASE';
}
export type HoldStatus = 'ACTIVE' | 'SETTLED' | 'RELEASED' | 'EXPIRED';

export interface BalanceSummary {
  orgId: string;
  planTier: PlanTier;
  totalGrantedCredits: number;
  availableCredits: number;
  reservedCredits: number;
  netSpendableCredits: number;
  parentOrgId: string | null;
  /** Credits the org could still cascade into its ancestors' shared pools. */
  poolSpendableCredits: number;
  walletStripes: Array<{ stripeNo: number; availableCredits: number; reservedCredits: number }>;
  autoRecharge: {
    enabled: boolean;
    thresholdCredits: number;
    amountCredits: number;
    monthlyCapCredits: number;
  };
  activeLots: Array<{
    grantId: string;
    source: GrantSource;
    remainingCredits: number;
    priority: number;
    expiresAt: string;
  }>;
}

export interface UsageHistoryItem {
  usageId: string;
  orgId: string;
  requestId: string;
  holdId: string | null;
  modelName: string;
  inputTokens: number;
  outputTokens: number;
  cachedInputTokens: number;
  creditsCharged: number;
  createdAt: string;
  allocations: Array<{
    grantId: string;
    creditsDeducted: number;
  }>;
}

export const CreateGrantSchema = z.object({
  orgId: z.string().min(1),
  source: z.enum(['PROMOTIONAL', 'PREPAID_INVOICE', 'ENTERPRISE_COMMIT', 'AUTO_RECHARGE']),
  credits: z.number().int().positive(),
  expiresAt: z.string().datetime(),
  invoiceId: z.string().min(1).optional(),
  priority: z.number().int().min(1).max(100).optional(),
});

export type CreateGrantInput = z.infer<typeof CreateGrantSchema>;

export const DeductUsageSchema = z
  .object({
    orgId: z.string().min(1),
    requestId: z.string().min(1),
    modelName: z.string().min(1),
    credits: z.number().int().positive().optional(),
    inputTokens: z.number().int().nonnegative().optional(),
    outputTokens: z.number().int().nonnegative().optional(),
    cachedInputTokens: z.number().int().nonnegative().optional(),
    idempotencyKey: z.string().min(1).optional(),
  })
  .refine(
    (data) =>
      typeof data.credits === 'number' ||
      typeof data.inputTokens === 'number' ||
      typeof data.outputTokens === 'number',
    {
      message: 'Either explicit credits or token counts (inputTokens/outputTokens) must be provided',
    }
  );

export type DeductUsageInput = z.infer<typeof DeductUsageSchema>;

export const ConfigureAutoRechargeSchema = z.object({
  enabled: z.boolean(),
  thresholdCredits: z.number().int().nonnegative(),
  amountCredits: z.number().int().positive(),
  monthlyCapCredits: z.number().int().positive(),
});

export type ConfigureAutoRechargeInput = z.infer<typeof ConfigureAutoRechargeSchema>;

export const CreateStreamHoldSchema = z.object({
  orgId: z.string().min(1),
  requestId: z.string().min(1),
  modelName: z.string().min(1),
  estimatedCredits: z.number().int().positive(),
  ttlSeconds: z.number().int().positive().optional(),
});

export type CreateStreamHoldInput = z.infer<typeof CreateStreamHoldSchema>;

export const ExtendStreamHoldSchema = z.object({
  holdId: z.string().min(1),
  additionalTtlSeconds: z.number().int().positive(),
  additionalCredits: z.number().int().nonnegative().optional(),
});

export type ExtendStreamHoldInput = z.infer<typeof ExtendStreamHoldSchema>;

export const SettleStreamHoldSchema = z.object({
  holdId: z.string().min(1),
  actualCredits: z.number().int().nonnegative().optional(),
  inputTokens: z.number().int().nonnegative().optional(),
  outputTokens: z.number().int().nonnegative().optional(),
  cachedInputTokens: z.number().int().nonnegative().optional(),
});

export type SettleStreamHoldInput = z.infer<typeof SettleStreamHoldSchema>;

export const ReleaseStreamHoldSchema = z.object({
  holdId: z.string().min(1),
  reason: z.string().min(1).optional(),
});

export type ReleaseStreamHoldInput = z.infer<typeof ReleaseStreamHoldSchema>;

export const PriceQuoteSchema = z.object({
  modelName: z.string().min(1),
  inputTokens: z.number().int().nonnegative(),
  outputTokens: z.number().int().nonnegative(),
  cachedInputTokens: z.number().int().nonnegative().optional(),
  planTier: z.enum(['STARTER', 'GROWTH', 'ENTERPRISE']).optional(),
});

export type PriceQuoteInput = z.infer<typeof PriceQuoteSchema>;

export const StripeWebhookSchema = z.object({
  eventId: z.string().min(1),
  eventType: z.enum(['invoice.paid', 'charge.refunded']),
  orgId: z.string().min(1),
  invoiceId: z.string().min(1),
  credits: z.number().int().positive(),
  expiresInDays: z.number().int().positive().optional(),
  /**
   * charge.refunded only: id of the original invoice.paid event that issued the grant.
   * When present the refund targets exactly that grant (it must still match orgId/invoiceId).
   */
  originalEventId: z.string().min(1).optional(),
});

export type StripeWebhookInput = z.infer<typeof StripeWebhookSchema>;

export class InsufficientCreditsError extends Error {
  public readonly statusCode = 402;
  constructor(orgId: string, requested: number, spendable: number) {
    super(
      `Organization ${orgId} has insufficient spendable credits: requested ${requested}, available ${spendable}`
    );
    this.name = 'InsufficientCreditsError';
  }
}

export class ResourceNotFoundError extends Error {
  public readonly statusCode = 404;
  constructor(resource: string, id: string) {
    super(`${resource} ${id} not found`);
    this.name = 'ResourceNotFoundError';
  }
}

export class HoldStateConflictError extends Error {
  public readonly statusCode = 409;
  constructor(holdId: string, message: string) {
    super(`Hold ${holdId} conflict: ${message}`);
    this.name = 'HoldStateConflictError';
  }
}

export class UnsupportedModelError extends Error {
  public readonly statusCode = 400;
  constructor(modelName: string) {
    super(`Unsupported model for metering: ${modelName}`);
    this.name = 'UnsupportedModelError';
  }
}

export class IdempotencyKeyConflictError extends Error {
  public readonly statusCode = 409;
  constructor(idempotencyKey: string) {
    super(
      `Idempotency key ${idempotencyKey} was already used for a different usage deduction request`
    );
    this.name = 'IdempotencyKeyConflictError';
  }
}

export class RateLimitExceededError extends Error {
  public readonly statusCode = 429;
  constructor(
    orgId: string,
    requested: number,
    limit: number,
    public readonly retryAfterSeconds: number
  ) {
    super(
      `Organization ${orgId} exceeded its credit burst limit: requested ${requested} credits, ` +
        `limit ${limit} credits per rolling 60s window`
    );
    this.name = 'RateLimitExceededError';
  }
}

const ISO_DATE = /^\d{4}-\d{2}-\d{2}$/;

export const DailyUsageQuerySchema = z
  .object({
    from: z.string().regex(ISO_DATE, 'from must be YYYY-MM-DD').optional(),
    to: z.string().regex(ISO_DATE, 'to must be YYYY-MM-DD').optional(),
  })
  .transform((q, ctx) => {
    const to = q.to ?? new Date().toISOString().slice(0, 10);
    const toMs = Date.parse(`${to}T00:00:00Z`);
    const from = q.from ?? new Date(toMs - 29 * 86_400_000).toISOString().slice(0, 10);
    const fromMs = Date.parse(`${from}T00:00:00Z`);
    if (Number.isNaN(fromMs) || Number.isNaN(toMs)) {
      ctx.addIssue({ code: z.ZodIssueCode.custom, message: 'Invalid calendar date' });
      return z.NEVER;
    }
    if (fromMs > toMs) {
      ctx.addIssue({ code: z.ZodIssueCode.custom, message: 'from must not be after to' });
      return z.NEVER;
    }
    if ((toMs - fromMs) / 86_400_000 > 366) {
      ctx.addIssue({ code: z.ZodIssueCode.custom, message: 'Date range must not exceed 366 days' });
      return z.NEVER;
    }
    return { from, to };
  });

export type DailyUsageQuery = z.infer<typeof DailyUsageQuerySchema>;

export type FundingBreakdown = Record<FundingSource, number>;

export interface DailyUsageReport {
  orgId: string;
  from: string;
  to: string;
  totalCredits: number;
  bySource: FundingBreakdown;
  /** Credits funded by ancestor pools rather than the org's own grants. */
  pooledCredits: number;
  days: Array<{
    date: string;
    totalCredits: number;
    bySource: FundingBreakdown;
    /** Credits by the org whose pool funded them (the org itself or an ancestor). */
    byFundingOrg: Record<string, number>;
    models: Array<{
      modelName: string;
      totalCredits: number;
      pooledCredits: number;
      bySource: FundingBreakdown;
      /** Usage events that drew on each funding source (an event funded by two sources counts in both). */
      eventsBySource: FundingBreakdown;
    }>;
  }>;
}

export const SetParentOrgSchema = z.object({
  parentOrgId: z.string().min(1).nullable(),
});

export type SetParentOrgInput = z.infer<typeof SetParentOrgSchema>;

export class PoolConfigurationError extends Error {
  public readonly statusCode = 409;
  constructor(message: string) {
    super(message);
    this.name = 'PoolConfigurationError';
  }
}
