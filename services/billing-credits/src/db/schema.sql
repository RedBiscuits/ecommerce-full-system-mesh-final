DROP VIEW IF EXISTS org_wallet_totals;
DROP TABLE IF EXISTS order_credit_refunds CASCADE;
DROP TABLE IF EXISTS settlement_fee_quotes CASCADE;
DROP TABLE IF EXISTS settlement_fee_rules CASCADE;
DROP TABLE IF EXISTS order_credit_settlements CASCADE;
DROP TABLE IF EXISTS order_credit_hold_keys CASCADE;
DROP VIEW IF EXISTS credit_grant_balances;
DROP TABLE IF EXISTS usage_rollup_deltas CASCADE;
DROP TABLE IF EXISTS usage_daily_rollups CASCADE;
DROP TABLE IF EXISTS auto_recharge_outbox CASCADE;
DROP TABLE IF EXISTS credit_hold_portions CASCADE;
DROP TABLE IF EXISTS auto_recharge_orders CASCADE;
DROP TABLE IF EXISTS billing_discrepancies CASCADE;
DROP TABLE IF EXISTS stripe_webhook_events CASCADE;
DROP TABLE IF EXISTS credit_lot_allocations CASCADE;
DROP TABLE IF EXISTS usage_events CASCADE;
DROP TABLE IF EXISTS credit_holds CASCADE;
DROP TABLE IF EXISTS credit_lots CASCADE;
DROP TABLE IF EXISTS credit_grants CASCADE;
DROP TABLE IF EXISTS org_wallet_stripes CASCADE;
DROP TABLE IF EXISTS org_balances CASCADE;

-- Organization configuration. Balances live in org_wallet_stripes (no per-org hot row).
CREATE TABLE org_balances (
    org_id VARCHAR(64) PRIMARY KEY,
    -- Hierarchical credit pooling: a sub-org draws on its own grants first, then cascades any
    -- remaining deduction / reservation deficit up to its ancestors' pools.
    parent_org_id VARCHAR(64) REFERENCES org_balances(org_id) CHECK (parent_org_id <> org_id),
    plan_tier VARCHAR(32) NOT NULL DEFAULT 'GROWTH', -- STARTER, GROWTH, ENTERPRISE
    auto_recharge_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    auto_recharge_threshold_credits BIGINT NOT NULL DEFAULT 250,
    auto_recharge_amount_credits BIGINT NOT NULL DEFAULT 2000,
    monthly_recharge_cap_credits BIGINT NOT NULL DEFAULT 10000,
    -- Per-org override of the plan-tier credit burst limit (credits per rolling 60s window).
    burst_limit_credits_per_minute BIGINT CHECK (burst_limit_credits_per_minute > 0),
    -- Number of wallet stripes; fixed at org creation.
    wallet_stripe_count SMALLINT NOT NULL DEFAULT 4 CHECK (wallet_stripe_count BETWEEN 1 AND 64),
    version INT NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Striped wallet: an org's balance and reservations are sharded across stripes so independent
-- deductions / holds lock different rows and run in parallel. Zero floors are enforced per stripe,
-- which makes the org-wide totals exact and non-negative as well.
CREATE TABLE org_wallet_stripes (
    org_id VARCHAR(64) NOT NULL REFERENCES org_balances(org_id),
    stripe_no SMALLINT NOT NULL CHECK (stripe_no >= 0),
    available_credits BIGINT NOT NULL DEFAULT 0 CHECK (available_credits >= 0),
    reserved_credits BIGINT NOT NULL DEFAULT 0 CHECK (reserved_credits >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (org_id, stripe_no)
);

-- Grant definition and lifecycle. Remaining credits live in credit_lots (one lot per stripe).
CREATE TABLE credit_grants (
    grant_id VARCHAR(64) PRIMARY KEY,
    org_id VARCHAR(64) NOT NULL REFERENCES org_balances(org_id),
    source VARCHAR(32) NOT NULL, -- PROMOTIONAL, PREPAID_INVOICE, ENTERPRISE_COMMIT, AUTO_RECHARGE
    invoice_id VARCHAR(100),
    initial_credits BIGINT NOT NULL CHECK (initial_credits > 0),
    refunded_credits BIGINT NOT NULL DEFAULT 0,
    priority INT NOT NULL DEFAULT 10,
    status VARCHAR(24) NOT NULL DEFAULT 'ACTIVE', -- ACTIVE, EXPIRED, REVOKED (DEPLETED is derived)
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (refunded_credits BETWEEN 0 AND initial_credits)
);

CREATE INDEX idx_credit_grants_org_expiry
    ON credit_grants (org_id, status, expires_at ASC, priority ASC, created_at ASC);

CREATE TABLE credit_lots (
    grant_id VARCHAR(64) NOT NULL REFERENCES credit_grants(grant_id),
    stripe_no SMALLINT NOT NULL,
    org_id VARCHAR(64) NOT NULL,
    remaining_credits BIGINT NOT NULL CHECK (remaining_credits >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (grant_id, stripe_no),
    FOREIGN KEY (org_id, stripe_no) REFERENCES org_wallet_stripes(org_id, stripe_no)
);

CREATE INDEX idx_credit_lots_org_stripe ON credit_lots (org_id, stripe_no);

CREATE TABLE credit_holds (
    hold_id VARCHAR(64) PRIMARY KEY,
    org_id VARCHAR(64) NOT NULL REFERENCES org_balances(org_id),
    -- Consumer-org stripe that owns the hold row (and carries the consumer's own portion). Immutable.
    home_stripe_no SMALLINT NOT NULL,
    request_id VARCHAR(100) NOT NULL,
    model_name VARCHAR(80) NOT NULL,
    estimated_credits BIGINT NOT NULL CHECK (estimated_credits > 0),
    actual_credits BIGINT,
    charged_credits BIGINT,
    status VARCHAR(24) NOT NULL DEFAULT 'ACTIVE', -- ACTIVE, SETTLED, RELEASED, EXPIRED
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_credit_holds_org_status
    ON credit_holds (org_id, status, expires_at);

-- An order is funded by at most one ACTIVE credit hold, so placing it twice (a retried checkout)
-- can never reserve twice, and the payment outcome captures / releases exactly that hold.
CREATE UNIQUE INDEX uq_credit_holds_active_order
    ON credit_holds (request_id)
    WHERE status = 'ACTIVE' AND model_name = 'order-credit';
CREATE INDEX idx_credit_holds_request ON credit_holds (request_id);

-- Where a hold's reservation lives: the consumer's own portion first (seq 0, on its home stripe),
-- then portions cascaded to ancestor pools, in acquisition order. Released LIFO.
CREATE TABLE credit_hold_portions (
    hold_id VARCHAR(64) NOT NULL REFERENCES credit_holds(hold_id),
    seq SMALLINT NOT NULL,
    org_id VARCHAR(64) NOT NULL,
    stripe_no SMALLINT NOT NULL,
    reserved_credits BIGINT NOT NULL CHECK (reserved_credits > 0),
    PRIMARY KEY (hold_id, seq),
    FOREIGN KEY (org_id, stripe_no) REFERENCES org_wallet_stripes(org_id, stripe_no)
);

CREATE INDEX idx_credit_hold_portions_org_stripe ON credit_hold_portions (org_id, stripe_no);

CREATE TABLE usage_events (
    usage_id VARCHAR(64) PRIMARY KEY,
    org_id VARCHAR(64) NOT NULL REFERENCES org_balances(org_id),
    request_id VARCHAR(100) NOT NULL,
    hold_id VARCHAR(64) REFERENCES credit_holds(hold_id),
    model_name VARCHAR(80) NOT NULL,
    input_tokens INT NOT NULL DEFAULT 0,
    output_tokens INT NOT NULL DEFAULT 0,
    cached_input_tokens INT NOT NULL DEFAULT 0,
    credits_charged BIGINT NOT NULL CHECK (credits_charged >= 0),
    idempotency_key VARCHAR(120),
    -- Snapshot of the original deduction response so idempotent replays return identical results.
    spendable_after_credits BIGINT,
    auto_recharge_intent_id VARCHAR(64),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_usage_events_org_created
    ON usage_events (org_id, created_at DESC);

-- Hard guarantee that an idempotency key can only ever produce one charge per organization.
CREATE UNIQUE INDEX uq_usage_events_org_idempotency_key
    ON usage_events (org_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE TABLE credit_lot_allocations (
    allocation_id VARCHAR(64) PRIMARY KEY,
    usage_id VARCHAR(64) NOT NULL REFERENCES usage_events(usage_id),
    grant_id VARCHAR(64) NOT NULL REFERENCES credit_grants(grant_id),
    stripe_no SMALLINT NOT NULL,
    credits_deducted BIGINT NOT NULL CHECK (credits_deducted > 0),
    -- FIFO position within the usage event (all allocations of one deduction share created_at).
    allocation_order INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_credit_lot_allocations_usage
    ON credit_lot_allocations (usage_id, allocation_order);

CREATE INDEX idx_credit_lot_allocations_grant
    ON credit_lot_allocations (grant_id);

-- UTC daily usage rollup, in two tiers so that deductions never contend on a shared row:
--   * usage_rollup_deltas: append-only, one row per (usage event, funding source, funding org),
--     inserted by the deduction transaction (plain INSERT: no upsert, no row lock on a shared row);
--   * usage_daily_rollups: compacted totals, written only by the compactor, which atomically moves
--     deltas into totals (DELETE ... RETURNING -> INSERT ... ON CONFLICT in one statement).
-- Readers sum both tiers in one query.
CREATE TABLE usage_rollup_deltas (
    delta_id VARCHAR(64) PRIMARY KEY,
    org_id VARCHAR(64) NOT NULL REFERENCES org_balances(org_id), -- consuming org
    usage_date DATE NOT NULL,
    model_name VARCHAR(80) NOT NULL,
    funding_source VARCHAR(20) NOT NULL, -- PROMO, PURCHASE, AUTO_RECHARGE
    funding_org_id VARCHAR(64) NOT NULL, -- org whose grants paid (self, or an ancestor pool)
    credits BIGINT NOT NULL CHECK (credits > 0),
    event_count INT NOT NULL DEFAULT 1 CHECK (event_count >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_usage_rollup_deltas_org_date ON usage_rollup_deltas (org_id, usage_date);
CREATE INDEX idx_usage_rollup_deltas_created ON usage_rollup_deltas (created_at, delta_id);

CREATE TABLE usage_daily_rollups (
    org_id VARCHAR(64) NOT NULL REFERENCES org_balances(org_id),
    usage_date DATE NOT NULL,
    model_name VARCHAR(80) NOT NULL,
    funding_source VARCHAR(20) NOT NULL,
    funding_org_id VARCHAR(64) NOT NULL,
    credits BIGINT NOT NULL DEFAULT 0 CHECK (credits >= 0),
    event_count INT NOT NULL DEFAULT 0 CHECK (event_count >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (org_id, usage_date, model_name, funding_source, funding_org_id)
);

CREATE TABLE stripe_webhook_events (
    event_id VARCHAR(100) PRIMARY KEY,
    event_type VARCHAR(50) NOT NULL,
    org_id VARCHAR(64) NOT NULL,
    invoice_id VARCHAR(100) NOT NULL,
    outcome VARCHAR(40) NOT NULL, -- PROCESSING (in-flight, never committed), GRANTED, REVOKED, PARTIALLY_REVOKED, DISCREPANCY_LOGGED, DUPLICATE_IGNORED
    grant_id VARCHAR(64) REFERENCES credit_grants(grant_id), -- grant created or revoked by this event
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE billing_discrepancies (
    discrepancy_id VARCHAR(64) PRIMARY KEY,
    org_id VARCHAR(64) NOT NULL REFERENCES org_balances(org_id),
    -- REFUND_UNRECOVERABLE, EXPIRED_DURING_ACTIVE_HOLD, REVOKED_DURING_ACTIVE_HOLD, SETTLEMENT_SHORTFALL
    discrepancy_type VARCHAR(40) NOT NULL,
    invoice_id VARCHAR(100), -- NULL for lots not backed by an invoice (e.g. PROMOTIONAL)
    grant_id VARCHAR(64) REFERENCES credit_grants(grant_id),
    hold_id VARCHAR(64) REFERENCES credit_holds(hold_id),
    usage_id VARCHAR(64) REFERENCES usage_events(usage_id),
    unrecovered_credits BIGINT NOT NULL CHECK (unrecovered_credits > 0),
    reason TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (grant_id IS NOT NULL OR hold_id IS NOT NULL)
);

CREATE INDEX idx_billing_discrepancies_org_type
    ON billing_discrepancies (org_id, discrepancy_type, created_at DESC);

-- Transactional outbox: a deduction / reservation that leaves an org at or below its auto-recharge
-- threshold commits an intent in its own transaction (plain insert, never blocks). The relay
-- evaluates intents exactly under a FULL wallet lock and coalesces all pending intents of an org
-- into at most one order.
CREATE TABLE auto_recharge_outbox (
    intent_id VARCHAR(64) PRIMARY KEY,
    org_id VARCHAR(64) NOT NULL REFERENCES org_balances(org_id),
    status VARCHAR(16) NOT NULL DEFAULT 'PENDING', -- PENDING, COMPLETED, SKIPPED
    triggered_by_usage_id VARCHAR(64),
    observed_spendable_credits BIGINT NOT NULL,
    recharge_id VARCHAR(64),
    reason VARCHAR(40),
    attempts INT NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ
);

CREATE INDEX idx_auto_recharge_outbox_pending ON auto_recharge_outbox (status, org_id, created_at);

CREATE TABLE auto_recharge_orders (
    recharge_id VARCHAR(64) PRIMARY KEY,
    org_id VARCHAR(64) NOT NULL REFERENCES org_balances(org_id),
    -- The outbox intent that produced this order: an intent can never yield two orders.
    intent_id VARCHAR(64) UNIQUE REFERENCES auto_recharge_outbox(intent_id),
    grant_id VARCHAR(64) REFERENCES credit_grants(grant_id),
    invoice_id VARCHAR(100) NOT NULL,
    credits_added BIGINT NOT NULL CHECK (credits_added > 0),
    trigger_balance_credits BIGINT NOT NULL,
    status VARCHAR(24) NOT NULL DEFAULT 'COMPLETED', -- COMPLETED, FAILED
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE usage_events
    ADD CONSTRAINT fk_usage_events_auto_recharge_intent
    FOREIGN KEY (auto_recharge_intent_id) REFERENCES auto_recharge_outbox(intent_id);

-- Reservation keys of order credit holds placed by the checkout coordinator (idempotency +
-- tombstones): one hold per key; a released / cancelled key can never place a hold again, so a
-- placement that arrives after its checkout was compensated cannot leave an orphan hold.
CREATE TABLE order_credit_hold_keys (
    hold_key VARCHAR(160) PRIMARY KEY,
    order_id VARCHAR(100) NOT NULL,
    org_id VARCHAR(64),
    credits BIGINT,
    hold_id VARCHAR(64) REFERENCES credit_holds(hold_id),
    status VARCHAR(16) NOT NULL CHECK (status IN ('PLACED', 'CANCELLED')),
    reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (status = 'CANCELLED' OR hold_id IS NOT NULL)
);

-- Outcome of a payment for the credits of its order (captured on SUCCESS, released on FAILED),
-- written in the same transaction as the hold transition, the consumer checkpoint and the
-- outbox event. One row per payment; at most one successful capture per order.
CREATE TABLE order_credit_settlements (
    payment_id VARCHAR(100) PRIMARY KEY,
    order_id VARCHAR(100) NOT NULL,
    org_id VARCHAR(64),
    payment_status VARCHAR(16) NOT NULL CHECK (payment_status IN ('SUCCESS', 'FAILED')),
    outcome VARCHAR(16) NOT NULL CHECK (outcome IN ('CAPTURED', 'RELEASED', 'NONE')),
    hold_id VARCHAR(64) REFERENCES credit_holds(hold_id),
    captured_credits BIGINT NOT NULL DEFAULT 0 CHECK (captured_credits >= 0),
    released_credits BIGINT NOT NULL DEFAULT 0 CHECK (released_credits >= 0),
    allowance_halalas BIGINT NOT NULL DEFAULT 0 CHECK (allowance_halalas >= 0),
    source_event_id VARCHAR(100) NOT NULL,
    settlement_event_id VARCHAR(100) NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX uq_order_credit_settlements_one_capture
    ON order_credit_settlements (order_id)
    WHERE payment_status = 'SUCCESS';
CREATE INDEX idx_order_credit_settlements_order ON order_credit_settlements (order_id);

-- Merchant settlement processing fees: tiered pricing rules. Marginal tiers in basis points of the
-- settled gross (`tiers`: [{ "upToHalalas": n | null, "bps": n }], ascending, the last one
-- unbounded), plus a fixed fee per settled transaction and a minimum fee; VAT is charged on top.
-- merchant_id NULL is the platform default; at most one ACTIVE rule per merchant (and one default).
CREATE TABLE settlement_fee_rules (
    rule_id VARCHAR(64) NOT NULL,
    version INT NOT NULL CHECK (version > 0),
    merchant_id VARCHAR(64),
    currency VARCHAR(3) NOT NULL DEFAULT 'SAR',
    tiers JSONB NOT NULL,
    per_transaction_halalas BIGINT NOT NULL DEFAULT 0 CHECK (per_transaction_halalas >= 0),
    minimum_fee_halalas BIGINT NOT NULL DEFAULT 0 CHECK (minimum_fee_halalas >= 0),
    vat_rate_bps INT NOT NULL DEFAULT 1500 CHECK (vat_rate_bps BETWEEN 0 AND 10000),
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (rule_id, version)
);

CREATE UNIQUE INDEX uq_settlement_fee_rules_active
    ON settlement_fee_rules ((COALESCE(merchant_id, '*')))
    WHERE active;

-- The fee of one settlement batch, frozen the first time the batch is priced (in the same
-- transaction as the consumer checkpoint and the outbox event): a redelivered or replayed event
-- gets the SAME fee even if the pricing rules changed in between.
CREATE TABLE settlement_fee_quotes (
    batch_id VARCHAR(64) PRIMARY KEY,
    merchant_id VARCHAR(64) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    item_count BIGINT NOT NULL CHECK (item_count > 0),
    gross_halalas BIGINT NOT NULL CHECK (gross_halalas > 0),
    rule_id VARCHAR(64) NOT NULL,
    rule_version INT NOT NULL,
    fee JSONB NOT NULL,
    fee_net_halalas BIGINT NOT NULL CHECK (fee_net_halalas >= 0),
    fee_vat_halalas BIGINT NOT NULL CHECK (fee_vat_halalas >= 0),
    fee_total_halalas BIGINT NOT NULL CHECK (fee_total_halalas >= 0),
    payout_halalas BIGINT NOT NULL CHECK (payout_halalas >= 0),
    source_event_id VARCHAR(100) NOT NULL,
    fee_event_id VARCHAR(100) NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (rule_id, rule_version) REFERENCES settlement_fee_rules (rule_id, version),
    CHECK (fee_total_halalas = fee_net_halalas + fee_vat_halalas),
    CHECK (gross_halalas = fee_total_halalas + payout_halalas)
);
CREATE INDEX idx_settlement_fee_quotes_merchant ON settlement_fee_quotes (merchant_id, created_at);

-- Customer refunds of captured orders: the promotional credits restored pro rata. One row per
-- refund id (idempotent), written with the consumer checkpoint, the restoring grant and the
-- billing.order-credits.restored outbox event in one transaction. Σ restored per order never
-- exceeds the credits captured for it.
CREATE TABLE order_credit_refunds (
    refund_id VARCHAR(100) PRIMARY KEY,
    order_id VARCHAR(100) NOT NULL,
    payment_id VARCHAR(100) NOT NULL,
    org_id VARCHAR(64),
    amount_halalas BIGINT NOT NULL CHECK (amount_halalas > 0),
    captured_halalas BIGINT NOT NULL CHECK (captured_halalas > 0),
    refunded_total_halalas BIGINT NOT NULL CHECK (refunded_total_halalas > 0 AND refunded_total_halalas <= captured_halalas),
    captured_credits BIGINT NOT NULL DEFAULT 0 CHECK (captured_credits >= 0),
    restored_credits BIGINT NOT NULL DEFAULT 0 CHECK (restored_credits >= 0),
    cumulative_restored_credits BIGINT NOT NULL DEFAULT 0 CHECK (cumulative_restored_credits >= 0 AND cumulative_restored_credits <= captured_credits),
    restore_grant_id VARCHAR(64) REFERENCES credit_grants(grant_id),
    source_event_id VARCHAR(100) NOT NULL,
    restored_event_id VARCHAR(100) NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_order_credit_refunds_order ON order_credit_refunds (order_id, created_at);

-- Grants with their remaining credits summed over stripes; DEPLETED is derived.
CREATE VIEW credit_grant_balances AS
SELECT
    g.grant_id,
    g.org_id,
    g.source,
    g.invoice_id,
    g.initial_credits,
    g.refunded_credits,
    g.priority,
    CASE WHEN g.status = 'ACTIVE' AND COALESCE(l.remaining, 0) = 0 THEN 'DEPLETED' ELSE g.status END AS status,
    g.status AS lifecycle_status,
    COALESCE(l.remaining, 0) AS remaining_credits,
    g.expires_at,
    g.created_at
FROM credit_grants g
LEFT JOIN (
    SELECT grant_id, SUM(remaining_credits) AS remaining
    FROM credit_lots
    GROUP BY grant_id
) l ON l.grant_id = g.grant_id;

-- Org-wide wallet totals (sum over stripes).
CREATE VIEW org_wallet_totals AS
SELECT
    o.org_id,
    COALESCE(SUM(s.available_credits), 0) AS available_credits,
    COALESCE(SUM(s.reserved_credits), 0) AS reserved_credits
FROM org_balances o
LEFT JOIN org_wallet_stripes s ON s.org_id = o.org_id
GROUP BY o.org_id;
