-- Split-tender checkouts: the saga state of the two pre-checkout reservations
-- (promotional credits at billing, the payable remainder at the wallet ledger).
CREATE TABLE IF NOT EXISTS checkouts (
  checkout_id VARCHAR(100) PRIMARY KEY,
  -- sha256 of the canonical request: the same checkout id with another request is refused.
  request_fingerprint CHAR(64) NOT NULL,
  order_id VARCHAR(100) NOT NULL,
  merchant_id VARCHAR(64) NOT NULL,
  request JSONB NOT NULL,
  promotion_org_id VARCHAR(64),
  promotion_credits BIGINT NOT NULL DEFAULT 0 CHECK (promotion_credits >= 0),
  tender VARCHAR(8) NOT NULL CHECK (tender IN ('CARD', 'WALLET')),
  source_wallet_id VARCHAR(64),
  allowance_halalas BIGINT NOT NULL DEFAULT 0 CHECK (allowance_halalas >= 0),
  -- What the remainder tender pays (lines − promotion + VAT), and the full breakdown.
  total_halalas BIGINT NOT NULL CHECK (total_halalas > 0),
  totals JSONB NOT NULL,
  credit_hold_key VARCHAR(160) NOT NULL UNIQUE,
  credit_hold_id VARCHAR(64),
  credit_hold_status VARCHAR(16) NOT NULL DEFAULT 'PENDING'
    CHECK (credit_hold_status IN ('PENDING', 'NOT_REQUIRED', 'PLACED', 'RELEASED', 'CAPTURED')),
  remainder_hold_key VARCHAR(160) NOT NULL UNIQUE,
  remainder_hold_id VARCHAR(64),
  remainder_hold_status VARCHAR(16) NOT NULL DEFAULT 'PENDING'
    CHECK (remainder_hold_status IN ('PENDING', 'PLACED', 'RELEASED', 'CAPTURED')),
  status VARCHAR(20) NOT NULL
    CHECK (status IN ('RESERVING', 'RESERVED', 'CONFIRMED', 'COMPLETED', 'COMPENSATING', 'FAILED', 'CANCELLED', 'EXPIRED', 'PAYMENT_FAILED')),
  -- Final status a COMPENSATING checkout moves to once both reservations are released.
  compensation_target VARCHAR(20) CHECK (compensation_target IN ('FAILED', 'CANCELLED', 'EXPIRED')),
  failure_code VARCHAR(60),
  failure_message TEXT,
  payment_id VARCHAR(100),
  lease_owner VARCHAR(120),
  lease_until TIMESTAMPTZ,
  expires_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK ((status = 'COMPENSATING') = (compensation_target IS NOT NULL)),
  CHECK ((tender = 'WALLET') = (source_wallet_id IS NOT NULL))
);

-- An order has at most one open (or completed) checkout; a failed / cancelled / expired one may be retried.
CREATE UNIQUE INDEX IF NOT EXISTS uq_checkouts_open_order
  ON checkouts(order_id) WHERE status IN ('RESERVING', 'RESERVED', 'CONFIRMED', 'COMPENSATING', 'COMPLETED');
CREATE INDEX IF NOT EXISTS idx_checkouts_status_expiry ON checkouts(status, expires_at);

-- Audit trail of every saga step (reservations, compensations, confirmations, outcomes).
CREATE TABLE IF NOT EXISTS checkout_steps (
  seq BIGSERIAL PRIMARY KEY,
  checkout_id VARCHAR(100) NOT NULL REFERENCES checkouts(checkout_id),
  step VARCHAR(40) NOT NULL,
  outcome VARCHAR(40) NOT NULL,
  detail JSONB NOT NULL DEFAULT '{}'::jsonb,
  at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_checkout_steps_checkout ON checkout_steps(checkout_id, seq);
