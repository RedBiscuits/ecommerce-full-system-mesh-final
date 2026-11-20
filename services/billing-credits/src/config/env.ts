import dotenv from 'dotenv';

dotenv.config();

export const config = {
  port: parseInt(process.env.PORT || '3000', 10),
  nodeEnv: process.env.NODE_ENV || 'development',
  defaultHoldTtlSeconds: parseInt(process.env.DEFAULT_HOLD_TTL_SECONDS || '300', 10),
  /** Start an outbox relay run right after a deduction commits an auto-recharge intent. */
  autoRechargeOutboxKick: (process.env.AUTO_RECHARGE_OUTBOX_KICK || 'true') !== 'false',
  orderCredits: {
    /**
     * Value of one credit when it is applied to an order as a discount, in SAR halalas
     * (default 100: one credit = 1.00 SAR). Captured credits become the order's
     * document-level allowance on the ledger journal and the ZATCA invoice.
     */
    halalasPerCredit: parseInt(process.env.ORDER_CREDIT_HALALAS_PER_CREDIT || '100', 10),
    /** Lifetime of an order credit hold; it must outlive the checkout's payment window. */
    holdTtlSeconds: parseInt(process.env.ORDER_CREDIT_HOLD_TTL_SECONDS || '86400', 10),
    /** Credits restored by a refund expire after this many days when the consumed grants already expired. */
    restoreTtlDays: parseInt(process.env.ORDER_CREDIT_RESTORE_TTL_DAYS || '90', 10),
  },
  creditBurst: {
    enabled: (process.env.CREDIT_BURST_LIMITER_ENABLED || 'true') !== 'false',
    /** Length of the sliding window. */
    windowMs: parseInt(process.env.CREDIT_BURST_WINDOW_MS || '60000', 10),
    /** Redis budget for a limiter call; slower (or failing) Redis fails open. */
    redisTimeoutMs: parseInt(process.env.CREDIT_BURST_REDIS_TIMEOUT_MS || '50', 10),
    /** Default credits per rolling 60s window by plan tier (overridable per org). */
    limitsByTier: {
      STARTER: parseInt(process.env.CREDIT_BURST_LIMIT_STARTER || '10000', 10),
      GROWTH: parseInt(process.env.CREDIT_BURST_LIMIT_GROWTH || '50000', 10),
      ENTERPRISE: parseInt(process.env.CREDIT_BURST_LIMIT_ENTERPRISE || '250000', 10),
    } as Record<string, number>,
  },
};
