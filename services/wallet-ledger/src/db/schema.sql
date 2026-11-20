DROP TABLE IF EXISTS order_refunds CASCADE;
DROP TABLE IF EXISTS order_settlements CASCADE;
DROP TABLE IF EXISTS merchant_settlement_batches CASCADE;
DROP TABLE IF EXISTS payment_holds CASCADE;
DROP TABLE IF EXISTS merchant_accounts CASCADE;
DROP TABLE IF EXISTS reconciliation_log CASCADE;
DROP TABLE IF EXISTS payout_webhook_events CASCADE;
DROP TABLE IF EXISTS payouts CASCADE;
DROP TABLE IF EXISTS ledger_entries CASCADE;
DROP TABLE IF EXISTS transfer_saga_steps CASCADE;
DROP TABLE IF EXISTS transfers CASCADE;
DROP TABLE IF EXISTS fx_rates CASCADE;
DROP TABLE IF EXISTS wallet_balance_shards CASCADE;
DROP TABLE IF EXISTS wallets CASCADE;
DROP TABLE IF EXISTS currencies CASCADE;

-- ISO 4217 currencies and their minor-unit exponents. Must mirror
-- src/money/currencies.ts (enforced by a test).
CREATE TABLE currencies (
    code VARCHAR(3) PRIMARY KEY,
    minor_units SMALLINT NOT NULL CHECK (minor_units BETWEEN 0 AND 4)
);

-- All money columns are BIGINT minor units ("_cents" is historical naming;
-- for JPY the minor unit is 1 yen).
--
-- Balance maintenance by account type:
--   USER, ESCROW        app-maintained: balance_cents changes via version-guarded
--                       compare-and-swap under SELECT ... FOR UPDATE. Must be >= 0.
--   FEE_TREASURY        ledger-maintained: balance_cents moves with every ledger
--                       entry inside the O(1) delta trigger. Must be >= 0.
--   FX_POOL, CLEARING   ledger-maintained, and MAY go negative (the platform's FX
--                       position / the external-world mirror).
--
-- Unsharded wallets (USER, ESCROW; shard_count = 0): ledger_balance_cents /
-- ledger_version are a precalculated running total and entry counter on the
-- wallet row, maintained incrementally (one delta per ledger row, O(1)) by
-- trg_ledger_entries_apply_delta. Invariant at every COMMIT:
--   balance_cents = ledger_balance_cents
-- A wallet violating it is "drifted" and frozen pending reconciliation.
--
-- Sharded wallets (FEE_TREASURY, FX_POOL, CLEARING; shard_count > 0): these are
-- touched by almost every transfer in their currency, so a single balance row
-- would serialize all of them. Their money lives in wallet_balance_shards
-- (striped buckets); the wallets row itself is static (balance columns pinned
-- to 0 by CHECK) and is never a lock target. Live balance = SUM(buckets).
CREATE TABLE wallets (
    wallet_id VARCHAR(64) PRIMARY KEY,
    owner_id VARCHAR(100) NOT NULL,
    currency VARCHAR(3) NOT NULL REFERENCES currencies(code),
    account_type VARCHAR(16) NOT NULL DEFAULT 'USER'
        CHECK (account_type IN ('USER', 'ESCROW', 'FEE_TREASURY', 'FX_POOL', 'CLEARING')),
    balance_cents BIGINT NOT NULL DEFAULT 0,
    ledger_balance_cents BIGINT NOT NULL DEFAULT 0,
    ledger_version BIGINT NOT NULL DEFAULT 0,
    -- Number of striped balance buckets (0 = unsharded). Fixed at creation.
    shard_count SMALLINT NOT NULL DEFAULT 0 CHECK (shard_count BETWEEN 0 AND 256),
    version INT NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (wallet_id, currency),
    CONSTRAINT wallets_shard_layout
        CHECK ((account_type IN ('FEE_TREASURY', 'FX_POOL', 'CLEARING')) = (shard_count > 0)),
    CONSTRAINT wallets_sharded_row_static
        CHECK (shard_count = 0 OR (balance_cents = 0 AND ledger_balance_cents = 0 AND ledger_version = 0)),
    CONSTRAINT wallets_balance_non_negative
        CHECK (balance_cents >= 0 OR account_type IN ('FX_POOL', 'CLEARING')),
    CONSTRAINT wallets_ledger_balance_non_negative
        CHECK (ledger_balance_cents >= 0 OR account_type IN ('FX_POOL', 'CLEARING')),
    CONSTRAINT wallets_ledger_version_non_negative CHECK (ledger_version >= 0)
);

-- Striped balance buckets for sharded (ledger-maintained) accounts. Each ledger
-- row updates ONE bucket, chosen per transaction, so concurrent transfers in
-- the same currency lock different rows instead of queueing on one. Buckets may
-- individually go negative; the account-level sign rule applies to the SUM.
CREATE TABLE wallet_balance_shards (
    wallet_id VARCHAR(64) NOT NULL REFERENCES wallets(wallet_id),
    shard_no SMALLINT NOT NULL CHECK (shard_no >= 0),
    balance_cents BIGINT NOT NULL DEFAULT 0,
    entry_count BIGINT NOT NULL DEFAULT 0 CHECK (entry_count >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (wallet_id, shard_no)
);

-- Directional scaled-integer FX rates: rate_scaled = rate * 10^8 where rate is
-- QUOTE units per 1 BASE unit. No implicit inversion (that would add rounding).
CREATE TABLE fx_rates (
    base_currency VARCHAR(3) NOT NULL REFERENCES currencies(code),
    quote_currency VARCHAR(3) NOT NULL REFERENCES currencies(code),
    rate_scaled BIGINT NOT NULL CHECK (rate_scaled > 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (base_currency, quote_currency),
    CHECK (base_currency <> quote_currency)
);

CREATE TABLE transfers (
    transfer_id VARCHAR(64) PRIMARY KEY,
    from_wallet_id VARCHAR(64) NOT NULL REFERENCES wallets(wallet_id),
    to_wallet_id VARCHAR(64) NOT NULL REFERENCES wallets(wallet_id),
    amount_cents BIGINT NOT NULL CHECK (amount_cents > 0),
    reference_id VARCHAR(120),
    status VARCHAR(24) NOT NULL DEFAULT 'COMPLETED', -- COMPLETED | PENDING_REVIEW | REVERSED
    reviewed_by VARCHAR(100),
    -- Locked-in quote (deterministic: an AML release uses exactly these numbers).
    source_currency VARCHAR(3) NOT NULL REFERENCES currencies(code),
    dest_currency VARCHAR(3) NOT NULL REFERENCES currencies(code),
    fee_bps INT NOT NULL DEFAULT 0 CHECK (fee_bps BETWEEN 0 AND 10000),
    fee_cents BIGINT NOT NULL DEFAULT 0 CHECK (fee_cents >= 0),
    total_debit_cents BIGINT NOT NULL,
    fx_rate_scaled BIGINT NOT NULL CHECK (fx_rate_scaled > 0),
    credited_amount_cents BIGINT NOT NULL CHECK (credited_amount_cents > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (total_debit_cents = amount_cents + fee_cents)
);

-- Append-only audit log of every saga step executed for a transfer, including
-- compensation steps. `seq` gives a total order.
CREATE TABLE transfer_saga_steps (
    seq BIGSERIAL PRIMARY KEY,
    transfer_id VARCHAR(64) NOT NULL REFERENCES transfers(transfer_id),
    step VARCHAR(48) NOT NULL,
    detail TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE ledger_entries (
    entry_id VARCHAR(64) PRIMARY KEY,
    transaction_ref VARCHAR(64) NOT NULL,
    wallet_id VARCHAR(64) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    direction VARCHAR(10) NOT NULL CHECK (direction IN ('DEBIT', 'CREDIT')),
    signed_amount_cents BIGINT NOT NULL, -- negative for DEBIT, positive for CREDIT
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- A ledger line can only be booked in its account's own currency.
    FOREIGN KEY (wallet_id, currency) REFERENCES wallets(wallet_id, currency),
    CHECK (
        (direction = 'DEBIT' AND signed_amount_cents < 0)
        OR (direction = 'CREDIT' AND signed_amount_cents > 0)
    )
);

CREATE INDEX idx_ledger_entries_wallet ON ledger_entries (wallet_id);
CREATE INDEX idx_ledger_entries_journal ON ledger_entries (transaction_ref, currency);

CREATE TABLE payouts (
    payout_id VARCHAR(64) PRIMARY KEY,
    wallet_id VARCHAR(64) NOT NULL REFERENCES wallets(wallet_id),
    currency VARCHAR(3) NOT NULL REFERENCES currencies(code),
    amount_cents BIGINT NOT NULL CHECK (amount_cents > 0),
    bank_account_ref VARCHAR(120) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'PROCESSING', -- PROCESSING, SETTLED, REVERSED, RECONCILIATION_REQUIRED
    idempotency_key VARCHAR(120),
    reconciliation_note TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE payout_webhook_events (
    event_id VARCHAR(100) PRIMARY KEY,
    payout_id VARCHAR(64) NOT NULL REFERENCES payouts(payout_id),
    event_type VARCHAR(40) NOT NULL,
    outcome VARCHAR(40) NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Audit trail of automatic balance corrections made by the reconciliation
-- worker. event_id is UNIQUE so a redelivered event can never double-correct.
CREATE TABLE reconciliation_log (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(64) NOT NULL UNIQUE,
    wallet_id VARCHAR(64) NOT NULL REFERENCES wallets(wallet_id),
    currency VARCHAR(3) NOT NULL REFERENCES currencies(code),
    previous_balance_cents BIGINT NOT NULL,
    previous_ledger_balance_cents BIGINT NOT NULL,
    previous_ledger_version BIGINT NOT NULL,
    corrected_balance_cents BIGINT NOT NULL,
    corrected_ledger_version BIGINT NOT NULL,
    source_of_truth VARCHAR(16) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================================
-- Card payments (order checkout via the payment gateway)
-- ============================================================================
-- Merchants selling through the platform and the USER wallet their revenue is
-- credited to. merchant_id is the seller's id across the platform (it is also
-- the ZATCA taxpayer id of the seller).
CREATE TABLE merchant_accounts (
    merchant_id VARCHAR(64) PRIMARY KEY,
    revenue_wallet_id VARCHAR(64) NOT NULL UNIQUE REFERENCES wallets(wallet_id),
    currency VARCHAR(3) NOT NULL REFERENCES currencies(code),
    display_name VARCHAR(200) NOT NULL,
    -- Where settlements credit the merchant's next payout (revenue minus the platform's fee).
    payout_wallet_id VARCHAR(64) UNIQUE REFERENCES wallets(wallet_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (revenue_wallet_id, currency) REFERENCES wallets(wallet_id, currency),
    CHECK (payout_wallet_id IS NULL OR payout_wallet_id <> revenue_wallet_id)
);

-- Merchant settlement batches. A batch sweeps the merchant's settled order revenue in a time window
-- (window_from NULL = since the beginning) in bounded keyset chunks, then (once billing priced the
-- processing fee) posts ONE balanced settlement journal:
--   merchant revenue DEBIT gross | platform fee treasury CREDIT fee | merchant payout CREDIT payout
--   COLLECTING → COLLECTED → POSTED → COMPLETED (ZATCA documents reported), or EMPTY.
-- Every order settlement is claimed by exactly one batch (order_settlements.settlement_batch_id,
-- set once, under the unswept partial index), so no revenue is ever settled twice.
CREATE TABLE merchant_settlement_batches (
    batch_id VARCHAR(64) PRIMARY KEY,
    merchant_id VARCHAR(64) NOT NULL REFERENCES merchant_accounts(merchant_id),
    currency VARCHAR(3) NOT NULL REFERENCES currencies(code),
    idempotency_key VARCHAR(160),
    window_from TIMESTAMPTZ,
    window_to TIMESTAMPTZ NOT NULL,
    max_items INT NOT NULL CHECK (max_items > 0),
    status VARCHAR(16) NOT NULL DEFAULT 'COLLECTING'
        CHECK (status IN ('COLLECTING', 'COLLECTED', 'EMPTY', 'POSTED', 'COMPLETED')),
    item_count BIGINT NOT NULL DEFAULT 0 CHECK (item_count >= 0),
    gross_cents BIGINT NOT NULL DEFAULT 0 CHECK (gross_cents >= 0),
    chunks INT NOT NULL DEFAULT 0,
    first_settled_at TIMESTAMPTZ,
    last_settled_at TIMESTAMPTZ,
    has_more BOOLEAN NOT NULL DEFAULT FALSE,
    fee_rule_id VARCHAR(64),
    fee_rule_version INT,
    fee JSONB,
    fee_net_cents BIGINT CHECK (fee_net_cents >= 0),
    fee_vat_cents BIGINT CHECK (fee_vat_cents >= 0),
    fee_total_cents BIGINT CHECK (fee_total_cents >= 0),
    payout_cents BIGINT CHECK (payout_cents >= 0),
    journal_ref VARCHAR(64) UNIQUE,
    collected_event_id VARCHAR(100) UNIQUE,
    posted_event_id VARCHAR(100) UNIQUE,
    tax_documents JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    collected_at TIMESTAMPTZ,
    posted_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (merchant_id, idempotency_key),
    CHECK (window_from IS NULL OR window_from < window_to),
    CHECK ((status IN ('POSTED', 'COMPLETED')) = (journal_ref IS NOT NULL)),
    CHECK (status NOT IN ('POSTED', 'COMPLETED') OR gross_cents = fee_total_cents + payout_cents),
    CHECK (status NOT IN ('POSTED', 'COMPLETED') OR fee_total_cents = fee_net_cents + fee_vat_cents)
);

-- One batch collecting per merchant at a time (sweeps of one merchant never interleave).
CREATE UNIQUE INDEX uq_settlement_batches_one_collecting
    ON merchant_settlement_batches (merchant_id) WHERE status = 'COLLECTING';
CREATE INDEX idx_settlement_batches_merchant ON merchant_settlement_batches (merchant_id, created_at);

-- Two-phase hold of the payable remainder of an order, by tender:
--   CARD    an authorization at the gateway: HELD books nothing; CAPTURED posts the 3-leg
--           journal from the gateway clearing account; RELEASED books nothing.
--   WALLET  the customer's SAR wallet: HELD moves the amount into escrow (reserve journal);
--           CAPTURED pays merchant revenue + VAT out of escrow; RELEASED refunds the customer.
-- hold_key is the idempotency key (`payment:<paymentId>` for card payments announced by the
-- gateway, `checkout:<checkoutId>:remainder` for split-tender checkouts): one hold per key. A key
-- released before any hold existed is recorded as a RELEASED tombstone, so a placement that
-- arrives late can never create an orphan hold.
CREATE TABLE payment_holds (
    hold_id VARCHAR(64) PRIMARY KEY,
    hold_key VARCHAR(160) NOT NULL UNIQUE,
    payment_id VARCHAR(100) UNIQUE,
    checkout_id VARCHAR(100),
    order_id VARCHAR(100) NOT NULL,
    merchant_id VARCHAR(64) NOT NULL REFERENCES merchant_accounts(merchant_id),
    currency VARCHAR(3) NOT NULL REFERENCES currencies(code),
    tender VARCHAR(8) NOT NULL DEFAULT 'CARD' CHECK (tender IN ('CARD', 'WALLET')),
    -- Validated at placement (not a foreign key: a tombstone may name a wallet that does not exist).
    source_wallet_id VARCHAR(64),
    amount_cents BIGINT NOT NULL CHECK (amount_cents > 0), -- gross held (halalas for SAR)
    status VARCHAR(16) NOT NULL DEFAULT 'HELD' CHECK (status IN ('HELD', 'CAPTURED', 'RELEASED')),
    reserve_journal_ref VARCHAR(64) UNIQUE,
    journal_ref VARCHAR(64) UNIQUE,
    release_journal_ref VARCHAR(64) UNIQUE,
    release_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK ((status = 'CAPTURED') = (journal_ref IS NOT NULL)),
    CHECK (status <> 'CAPTURED' OR payment_id IS NOT NULL),
    CHECK ((tender = 'WALLET') = (source_wallet_id IS NOT NULL)),
    CHECK (tender = 'WALLET' OR (reserve_journal_ref IS NULL AND release_journal_ref IS NULL))
);

CREATE INDEX idx_payment_holds_order ON payment_holds (order_id);

-- The captured settlement of an order: exactly one per order (primary key), so
-- an order's revenue and VAT can never be booked twice, whatever is redelivered.
CREATE TABLE order_settlements (
    order_id VARCHAR(100) PRIMARY KEY,
    payment_id VARCHAR(100) NOT NULL UNIQUE REFERENCES payment_holds(payment_id),
    hold_id VARCHAR(64) NOT NULL UNIQUE REFERENCES payment_holds(hold_id),
    merchant_id VARCHAR(64) NOT NULL REFERENCES merchant_accounts(merchant_id),
    currency VARCHAR(3) NOT NULL REFERENCES currencies(code),
    journal_ref VARCHAR(64) NOT NULL UNIQUE,
    gross_cents BIGINT NOT NULL CHECK (gross_cents > 0),
    net_revenue_cents BIGINT NOT NULL CHECK (net_revenue_cents >= 0),
    vat_cents BIGINT NOT NULL CHECK (vat_cents >= 0),
    allowance_cents BIGINT NOT NULL DEFAULT 0 CHECK (allowance_cents >= 0),
    captured_credits BIGINT NOT NULL DEFAULT 0 CHECK (captured_credits >= 0),
    source_event_id VARCHAR(100) NOT NULL,
    settlement_event_id VARCHAR(100) NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- The merchant settlement batch that swept this revenue (set once; never cleared), and the
    -- revenue it claimed (net revenue minus the refunds reversed before the sweep).
    settlement_batch_id VARCHAR(64) REFERENCES merchant_settlement_batches(batch_id),
    settled_net_cents BIGINT CHECK (settled_net_cents >= 0),
    -- The order as captured (lines + allowance): the refund split is computed from what was booked.
    order_context JSONB,
    -- Customer refunds reversed so far (Σ order_refunds), never more than what was captured.
    refunded_gross_cents BIGINT NOT NULL DEFAULT 0 CHECK (refunded_gross_cents >= 0),
    refunded_net_cents BIGINT NOT NULL DEFAULT 0 CHECK (refunded_net_cents >= 0),
    refunded_vat_cents BIGINT NOT NULL DEFAULT 0 CHECK (refunded_vat_cents >= 0),
    -- Per VAT category: what was captured and what was refunded (the pro-rata basis of the next refund).
    refund_state JSONB,
    CHECK (gross_cents = net_revenue_cents + vat_cents),
    CHECK (refunded_gross_cents = refunded_net_cents + refunded_vat_cents),
    CHECK (refunded_gross_cents <= gross_cents),
    CHECK (refunded_net_cents <= net_revenue_cents),
    CHECK (refunded_vat_cents <= vat_cents)
);

-- Customer refunds of captured orders: one row per refund id (idempotent), written with the
-- consumer checkpoint, the reversal journal (journal_ref UNIQUE: never posted twice) and the
-- ledger.payment.refunded outbox event in ONE transaction.
CREATE TABLE order_refunds (
    refund_id VARCHAR(100) PRIMARY KEY,
    order_id VARCHAR(100) NOT NULL REFERENCES order_settlements(order_id),
    payment_id VARCHAR(100) NOT NULL,
    merchant_id VARCHAR(64) NOT NULL REFERENCES merchant_accounts(merchant_id),
    currency VARCHAR(3) NOT NULL REFERENCES currencies(code),
    amount_cents BIGINT NOT NULL CHECK (amount_cents > 0),
    net_cents BIGINT NOT NULL CHECK (net_cents >= 0),
    vat_cents BIGINT NOT NULL CHECK (vat_cents >= 0),
    split JSONB NOT NULL,
    -- REVENUE: the revenue was still in the merchant's revenue wallet; PAYOUT: it had been settled
    -- into the payout wallet (the merchant's next payout is reduced).
    merchant_account VARCHAR(8) NOT NULL CHECK (merchant_account IN ('REVENUE', 'PAYOUT')),
    refunded_to_wallet_id VARCHAR(64) NOT NULL REFERENCES wallets(wallet_id),
    journal_ref VARCHAR(64) NOT NULL UNIQUE,
    restored_credits BIGINT NOT NULL DEFAULT 0 CHECK (restored_credits >= 0),
    source_event_id VARCHAR(100) NOT NULL,
    refund_event_id VARCHAR(100) NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (amount_cents = net_cents + vat_cents)
);
CREATE INDEX idx_order_refunds_order ON order_refunds (order_id, created_at);

-- Sweep path: only UNSWEPT revenue is in this index, in keyset order. A claimed entry leaves it,
-- so later sweeps never read it again (each entry is scanned once, whatever the window). What a
-- sweep claims is the revenue still held: net_revenue_cents − refunded_net_cents.
CREATE INDEX idx_order_settlements_unswept
    ON order_settlements (merchant_id, created_at, order_id)
    WHERE settlement_batch_id IS NULL;
CREATE INDEX idx_order_settlements_batch
    ON order_settlements (settlement_batch_id, created_at, order_id)
    WHERE settlement_batch_id IS NOT NULL;

-- A swept entry can never be moved to another batch or un-swept.
CREATE OR REPLACE FUNCTION guard_order_settlement_batch() RETURNS TRIGGER AS $$
BEGIN
  IF OLD.settlement_batch_id IS NOT NULL AND NEW.settlement_batch_id IS DISTINCT FROM OLD.settlement_batch_id THEN
    RAISE EXCEPTION 'SETTLEMENT_ENTRY_ALREADY_SWEPT: order % belongs to batch %', OLD.order_id, OLD.settlement_batch_id
      USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_order_settlements_batch_once
BEFORE UPDATE OF settlement_batch_id ON order_settlements
FOR EACH ROW EXECUTE FUNCTION guard_order_settlement_batch();

-- ============================================================================
-- RULE 1a (O(1) incremental delta tracking): every ledger row applies its delta
-- either to its wallet's precalculated running total (unsharded) or to one
-- striped bucket (sharded fee / FX pool / clearing accounts, whose live balance
-- is the SUM of their buckets). Constant work per row, independent of history
-- length. Sign rules: CHECK constraints on unsharded rows; an under-full-lock
-- total check for debits of sharded non-negative (fee treasury) accounts.
-- ============================================================================
CREATE OR REPLACE FUNCTION apply_ledger_delta() RETURNS TRIGGER AS $$
DECLARE
  acct_type VARCHAR(16);
  shards SMALLINT;
  target_shard INT;
  shard_total NUMERIC;
BEGIN
  -- account_type and shard_count are immutable: a plain read, no row lock.
  SELECT account_type, shard_count INTO acct_type, shards
  FROM wallets WHERE wallet_id = NEW.wallet_id;

  IF shards = 0 THEN
    -- Unsharded (USER / ESCROW): O(1) running total on the wallet row, which
    -- the application has already locked FOR UPDATE in this transaction.
    UPDATE wallets
    SET ledger_balance_cents = ledger_balance_cents + NEW.signed_amount_cents,
        ledger_version = ledger_version + 1,
        updated_at = NOW()
    WHERE wallet_id = NEW.wallet_id;
    RETURN NULL;
  END IF;

  -- Sharded: one bucket per transaction. Concurrent transactions hold distinct
  -- (typically consecutive) xids, so they spread round-robin over the buckets;
  -- every line a transaction writes to this account lands in the same bucket,
  -- keeping the lock order deterministic: (wallet_id, shard_no).
  target_shard := (txid_current() % shards)::INT;

  IF NEW.signed_amount_cents < 0 AND acct_type = 'FEE_TREASURY' THEN
    -- A debit of a NON-NEGATIVE sharded account must see the true total.
    -- Lock every bucket in shard order first: this serializes debits against
    -- each other and against in-flight credits (no write skew across buckets).
    -- Credits, the hot path, never take this branch and stay concurrent.
    FOR i IN 0 .. shards - 1 LOOP
      PERFORM 1 FROM wallet_balance_shards
      WHERE wallet_id = NEW.wallet_id AND shard_no = i
      FOR UPDATE;
    END LOOP;
    SELECT COALESCE(SUM(balance_cents), 0) INTO shard_total
    FROM wallet_balance_shards WHERE wallet_id = NEW.wallet_id;
    IF shard_total + NEW.signed_amount_cents < 0 THEN
      RAISE EXCEPTION 'SHARDED_BALANCE_NON_NEGATIVE: % would go to %',
        NEW.wallet_id, shard_total + NEW.signed_amount_cents
        USING ERRCODE = '23514';
    END IF;
  END IF;

  UPDATE wallet_balance_shards
  SET balance_cents = balance_cents + NEW.signed_amount_cents,
      entry_count = entry_count + 1,
      updated_at = NOW()
  WHERE wallet_id = NEW.wallet_id AND shard_no = target_shard;

  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_ledger_entries_apply_delta
AFTER INSERT ON ledger_entries
FOR EACH ROW EXECUTE FUNCTION apply_ledger_delta();

-- A new wallet starts with an empty ledger: its precalculated totals can't be
-- seeded with arbitrary values.
CREATE OR REPLACE FUNCTION reset_new_wallet_ledger_totals() RETURNS TRIGGER AS $$
BEGIN
  NEW.ledger_balance_cents := 0;
  NEW.ledger_version := 0;
  -- Ledger-maintained account types are sharded (16 buckets unless specified).
  IF NEW.account_type IN ('FEE_TREASURY', 'FX_POOL', 'CLEARING') AND NEW.shard_count = 0 THEN
    NEW.shard_count := 16;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_wallets_reset_ledger_totals
BEFORE INSERT ON wallets
FOR EACH ROW EXECUTE FUNCTION reset_new_wallet_ledger_totals();

-- Pre-create a sharded account's buckets, so the delta trigger only ever
-- UPDATEs an existing row (a plain row lock; no insert races).
CREATE OR REPLACE FUNCTION create_wallet_balance_shards() RETURNS TRIGGER AS $$
BEGIN
  IF NEW.shard_count > 0 THEN
    INSERT INTO wallet_balance_shards (wallet_id, shard_no)
    SELECT NEW.wallet_id, g FROM generate_series(0, NEW.shard_count - 1) AS g;
  END IF;
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_wallets_create_balance_shards
AFTER INSERT ON wallets
FOR EACH ROW EXECUTE FUNCTION create_wallet_balance_shards();

-- ============================================================================
-- RULE 1b: balance_cents = ledger_balance_cents for every wallet at COMMIT.
-- O(1): a primary-key lookup of two precalculated columns, never a SUM over
-- history. Deferred, so a transaction may update the balance and post the
-- journal in either order. Every ledger row updates its wallet (RULE 1a), so
-- this single wallets trigger also covers ledger inserts. The violating
-- wallet id is reported in DETAIL so the app can map it to a clean 409.
-- ============================================================================
CREATE OR REPLACE FUNCTION assert_wallet_ledger_sync() RETURNS TRIGGER AS $$
DECLARE
  current_balance BIGINT;
  current_ledger_balance BIGINT;
BEGIN
  SELECT balance_cents, ledger_balance_cents INTO current_balance, current_ledger_balance
  FROM wallets
  WHERE wallet_id = NEW.wallet_id;

  IF FOUND AND current_balance <> current_ledger_balance THEN
    RAISE EXCEPTION 'LEDGER_OUT_OF_SYNC: wallet % has balance_cents=% but ledger_balance_cents=%',
      NEW.wallet_id, current_balance, current_ledger_balance
      USING ERRCODE = '23514', DETAIL = NEW.wallet_id;
  END IF;

  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER trg_wallets_ledger_sync
AFTER INSERT OR UPDATE ON wallets
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION assert_wallet_ledger_sync();

-- ============================================================================
-- RULE 2: every journal (transaction_ref) must balance to exactly zero in each
-- currency at COMMIT. This is what makes the global per-currency zero-sum
-- invariant hold by construction. Cost is O(lines in this journal) via the
-- (transaction_ref, currency) index: bounded (<= 5 lines), not history-sized.
-- ============================================================================
CREATE OR REPLACE FUNCTION assert_journal_balanced() RETURNS TRIGGER AS $$
DECLARE
  journal_total NUMERIC;
BEGIN
  SELECT COALESCE(SUM(signed_amount_cents), 0) INTO journal_total
  FROM ledger_entries
  WHERE transaction_ref = NEW.transaction_ref
    AND currency = NEW.currency;

  IF journal_total <> 0 THEN
    RAISE EXCEPTION 'UNBALANCED_JOURNAL: % does not balance in % (off by %)',
      NEW.transaction_ref, NEW.currency, journal_total
      USING ERRCODE = '23514';
  END IF;

  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER trg_ledger_entries_journal_balanced
AFTER INSERT ON ledger_entries
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION assert_journal_balanced();

-- ============================================================================
-- RULE 3: the ledger is append-only. Corrections are new balancing journals,
-- never edits. (The auditor exists for when these guards are bypassed, e.g.
-- triggers disabled during an ops intervention or a bad restore.)
-- ============================================================================
CREATE OR REPLACE FUNCTION reject_ledger_mutation() RETURNS TRIGGER AS $$
BEGIN
  RAISE EXCEPTION 'LEDGER_APPEND_ONLY: ledger_entries cannot be updated or deleted'
    USING ERRCODE = '42501';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_ledger_entries_append_only
BEFORE UPDATE OR DELETE ON ledger_entries
FOR EACH ROW EXECUTE FUNCTION reject_ledger_mutation();

-- ============================================================================
-- Seed data
-- ============================================================================
INSERT INTO currencies (code, minor_units) VALUES
    ('USD', 2), ('EUR', 2), ('GBP', 2), ('JPY', 0), ('SAR', 2);

-- Every wallet starts at zero; opening balances are posted as a balanced seed
-- journal by resetAndSeedDatabase() so balances and ledger agree from day one.
INSERT INTO wallets (wallet_id, owner_id, currency, account_type)
VALUES
    -- Users
    ('wallet_alice', 'user_alice', 'USD', 'USER'),
    ('wallet_bob',   'user_bob',   'USD', 'USER'),
    ('wallet_carol', 'user_carol', 'EUR', 'USER'),
    ('wallet_dave',  'user_dave',  'JPY', 'USER'),
    -- USD system accounts (legacy IDs kept for wallet_treasury / wallet_escrow)
    ('wallet_treasury',         'system_treasury',     'USD', 'CLEARING'),
    ('wallet_escrow',           'system_aml_escrow',   'USD', 'ESCROW'),
    ('wallet_fee_treasury_usd', 'system_fee_treasury', 'USD', 'FEE_TREASURY'),
    ('wallet_fx_pool_usd',      'system_fx_pool',      'USD', 'FX_POOL'),
    -- EUR system accounts
    ('wallet_treasury_eur',     'system_treasury',     'EUR', 'CLEARING'),
    ('wallet_escrow_eur',       'system_aml_escrow',   'EUR', 'ESCROW'),
    ('wallet_fee_treasury_eur', 'system_fee_treasury', 'EUR', 'FEE_TREASURY'),
    ('wallet_fx_pool_eur',      'system_fx_pool',      'EUR', 'FX_POOL'),
    -- GBP system accounts
    ('wallet_treasury_gbp',     'system_treasury',     'GBP', 'CLEARING'),
    ('wallet_escrow_gbp',       'system_aml_escrow',   'GBP', 'ESCROW'),
    ('wallet_fee_treasury_gbp', 'system_fee_treasury', 'GBP', 'FEE_TREASURY'),
    ('wallet_fx_pool_gbp',      'system_fx_pool',      'GBP', 'FX_POOL'),
    -- JPY system accounts
    ('wallet_treasury_jpy',     'system_treasury',     'JPY', 'CLEARING'),
    ('wallet_escrow_jpy',       'system_aml_escrow',   'JPY', 'ESCROW'),
    ('wallet_fee_treasury_jpy', 'system_fee_treasury', 'JPY', 'FEE_TREASURY'),
    ('wallet_fx_pool_jpy',      'system_fx_pool',      'JPY', 'FX_POOL'),
    -- SAR system accounts (wallet_treasury_sar also mirrors the card gateway)
    ('wallet_treasury_sar',     'system_treasury',     'SAR', 'CLEARING'),
    ('wallet_escrow_sar',       'system_aml_escrow',   'SAR', 'ESCROW'),
    ('wallet_fee_treasury_sar', 'system_fee_treasury', 'SAR', 'FEE_TREASURY'),
    ('wallet_fx_pool_sar',      'system_fx_pool',      'SAR', 'FX_POOL'),
    -- Output VAT owed to ZATCA (liability to an external party)
    ('wallet_vat_payable_sar',  'system_zatca_vat',    'SAR', 'CLEARING'),
    -- Merchant revenue (seller of the sandbox storefront; ZATCA taxpayer txp_riyadh_retail)
    ('wallet_merchant_riyadh_retail', 'merchant_txp_riyadh_retail', 'SAR', 'USER'),
    -- The merchant's payout account: settlements credit it with revenue minus the platform's fee
    ('wallet_payout_riyadh_retail', 'merchant_txp_riyadh_retail', 'SAR', 'USER'),
    -- A storefront customer paying from her SAR wallet (split-tender checkouts)
    ('wallet_noura_sar', 'user_noura', 'SAR', 'USER');

INSERT INTO merchant_accounts (merchant_id, revenue_wallet_id, currency, display_name, payout_wallet_id) VALUES
    ('txp_riyadh_retail', 'wallet_merchant_riyadh_retail', 'SAR', 'Riyadh Orbit E-Commerce Co.', 'wallet_payout_riyadh_retail');

-- Rates: QUOTE per 1 BASE, scaled by 10^8.
INSERT INTO fx_rates (base_currency, quote_currency, rate_scaled) VALUES
    ('USD', 'EUR',    92000000),  -- 0.92
    ('EUR', 'USD',   108700000),  -- 1.087
    ('USD', 'GBP',    79000000),  -- 0.79
    ('GBP', 'USD',   126500000),  -- 1.265
    ('USD', 'JPY', 15125000000),  -- 151.25
    ('JPY', 'USD',      660000),  -- 0.0066
    ('EUR', 'GBP',    85800000),  -- 0.858
    ('GBP', 'EUR',   116500000),  -- 1.165
    ('EUR', 'JPY', 16440000000),  -- 164.40
    ('JPY', 'EUR',      608000),  -- 0.00608
    ('GBP', 'JPY', 19150000000),  -- 191.50
    ('JPY', 'GBP',      522000),  -- 0.00522
    ('USD', 'SAR',   375000000),  -- 3.75 (SAMA peg)
    ('SAR', 'USD',    26666667);  -- 0.26666667
