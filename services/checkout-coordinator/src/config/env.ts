import dotenv from 'dotenv';

dotenv.config();

function int(name: string, fallback: number): number {
  const raw = process.env[name];
  const value = raw === undefined || raw.trim() === '' ? fallback : Number(raw);
  if (!Number.isInteger(value) || value < 0) throw new Error(`[checkout-coordinator] ${name} must be a non-negative integer`);
  return value;
}

export const config = {
  port: int('PORT', 3105),
  nodeEnv: process.env.NODE_ENV || 'development',
  defaultMerchantId: process.env.COMMERCE_DEFAULT_MERCHANT_ID || 'txp_riyadh_retail',
  /** Value of one promotional credit applied to an order, in halalas (must equal billing's). */
  halalasPerCredit: int('ORDER_CREDIT_HALALAS_PER_CREDIT', 100),
  /** How long both reservations of a checkout are held before the checkout expires (and is released). */
  reservationTtlSeconds: int('CHECKOUT_RESERVATION_TTL_SECONDS', 900),
  /** The credit hold outlives the checkout by this margin, so billing never expires it first. */
  creditHoldTtlMarginSeconds: int('CHECKOUT_CREDIT_HOLD_TTL_MARGIN_SECONDS', 300),
  downstream: {
    /** Calls to billing / the ledger: attempts on unavailability (5xx, timeouts), jittered backoff. */
    maxAttempts: int('CHECKOUT_DOWNSTREAM_MAX_ATTEMPTS', 4),
    baseDelayMs: int('CHECKOUT_DOWNSTREAM_BASE_DELAY_MS', 100),
    maxDelayMs: int('CHECKOUT_DOWNSTREAM_MAX_DELAY_MS', 2_000),
    timeoutMs: int('CHECKOUT_DOWNSTREAM_TIMEOUT_MS', 5_000),
  },
  /** A saga step left by a crashed instance is resumed once its lease expired. */
  leaseMs: int('CHECKOUT_LEASE_MS', 30_000),
  /** Expiry / recovery sweep interval (0 disables the timer). */
  maintenanceIntervalMs: int('CHECKOUT_MAINTENANCE_INTERVAL_MS', 10_000),
  billingUrl: process.env.BILLING_CREDITS_URL || 'http://localhost:3103',
  walletUrl: process.env.WALLET_LEDGER_URL || 'http://localhost:3102',
  /** Bearer token of the `checkout_coordinator` service principal at the wallet ledger. */
  walletServiceToken: process.env.WALLET_SERVICE_TOKEN || '',
};

export type CoordinatorConfig = typeof config;
