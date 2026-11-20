# Billing Credits Service

Prepaid API credit ledger, token pricing calculator, streaming pre-authorization hold manager, and Stripe billing reconciliation microservice.

## Tech Stack
- **Runtime:** Node.js (v20+) & TypeScript
- **Web Framework:** Fastify
- **Database:** PostgreSQL (`pg` / PGlite)
- **Cache:** Redis (`ioredis`)
- **Validation:** Zod
- **Linter & Test Runner:** ESLint & Vitest

## Development Commands

```bash
npm install          # Install dependencies
npm run db:seed      # Seed initial organizations (org_acme, org_globex) and credit grants
npm run lint         # Run ESLint across src/ and __tests__/
npm run typecheck    # Run TypeScript compiler typecheck (tsc --noEmit)
npm run build        # Compile TypeScript to dist/
npm test             # Run Vitest suite (embedded PGlite)
TEST_DATABASE_URL=postgres://user@host:5432/db npm run test:pg
                     # Opt-in stress test against a real PostgreSQL server (row locks / deadlocks)
```

Set `DATABASE_URL` to run the service against a real PostgreSQL server; otherwise an embedded PGlite
instance is used. That sandbox database is in-memory and per process, so the standalone server installs
and seeds its schema at start (`DB_SEED_ON_START=false` to skip; never done when `DATABASE_URL` is set).

## API Surface

### Credits & Ledger
- `GET /credits/organizations/:orgId/balance` — Credit summary: spendable balance (lots past expiry excluded), reservations, wallet stripes, parent pool and `poolSpendableCredits`, auto-recharge config and active FIFO lots.
- `GET /credits/organizations/:orgId/history` — Recent metered usage events and their credit lot allocations.
- `GET /credits/organizations/:orgId/usage/daily?from=YYYY-MM-DD&to=YYYY-MM-DD` — Credits consumed per UTC day by model and funding source (`PROMO`, `PURCHASE`, `AUTO_RECHARGE`), with `pooledCredits` / `byFundingOrg` for usage funded by ancestor pools. One query over compacted daily totals plus pending deltas (default range: last 30 days, max 366 days).
- `PUT /credits/organizations/:orgId/auto-recharge` — Update threshold-based auto-recharge settings.
- `PUT /credits/organizations/:orgId/parent` — Attach a sub-org to a parent pool (`{ "parentOrgId": "org_x" | null }`). Rejects cycles, hierarchies deeper than 4 levels, and re-parenting while holds of the subtree are reserved in a parent pool (`409 POOL_CONFIGURATION_CONFLICT`).
- `POST /credits/grants` — Issue a new credit grant lot (`PROMOTIONAL`, `PREPAID_INVOICE`, `ENTERPRISE_COMMIT`, `AUTO_RECHARGE`).
- `POST /credits/deduct` — Deduct metered usage (explicit `credits` or `inputTokens`/`outputTokens`/`cachedInputTokens`) own-pool-first in FIFO order, cascading any remainder to ancestor pools. Supports `Idempotency-Key` (replays are free; a reused key with a different payload returns `409`). Subject to the credit burst limit (`429 CREDIT_BURST_LIMIT_EXCEEDED` + `Retry-After`). `autoRechargeQueued` reports a committed auto-recharge intent.

### Streaming Pre-Auth Holds
- `POST /credits/holds` — Reserve estimated credits before a streaming response (own pool first, deficit cascaded to ancestor pools; counts against the burst limit).
- `POST /credits/holds/extend` — Extend TTL and/or reserved credits (additional credits count against the burst limit).
- `POST /credits/holds/release` — Release an aborted stream hold without charging credits.
- `POST /credits/holds/settle` — Charge actual usage: the hold's own reservation and free credits first, ancestor pools only for the excess; never other holds' reservations. Unfundable usage is reported as `shortfallCredits` and logged as a `SETTLEMENT_SHORTFALL` billing discrepancy.
- Extend/release/settle return `409 HOLD_STATE_CONFLICT` for holds that are settled, released or expired (a hold past its TTL is expired on the spot).

### Pricing & Webhooks
- `GET /credits/pricing/models` — List model rate cards (`claude-3-7-sonnet`, `claude-3-5-haiku`, `gpt-4o`, `gemini-2.5-pro`).
- `POST /credits/pricing/quote` — Calculate billable credits from token counts and plan tier discount.
- `POST /webhooks/stripe` — Reconcile Stripe `invoice.paid` and `charge.refunded` events. Deliveries are deduplicated by event id (and invoice). Refunds claw back up to `credits` from the grant (partial refunds accumulate; a fully refunded grant is `REVOKED`), can target the grant of an `originalEventId`, and log already-consumed credits as `REFUND_UNRECOVERABLE` and newly unbacked holds as `REVOKED_DURING_ACTIVE_HOLD`.

## Concurrency Model

### Striped wallet
An organization's balance is sharded across `wallet_stripe_count` rows of `org_wallet_stripes`
(default 4) and every grant is split into one credit lot per stripe (`credit_lots`). Per stripe, the
database enforces `available_credits >= 0` and `reserved_credits >= 0`; the service keeps
`available = remaining credits of the stripe's lots` and `reserved = hold portions on the stripe`,
so org-wide totals keep an exact zero floor and exact reservation totals.

- **Fast path (STRIPE lock):** a deduction or hold locks one stripe, so independent writes of the
  same org run in parallel. A single-stripe deduction is only accepted when it is indistinguishable
  from a global FIFO draw.
- **FULL lock:** when one stripe cannot serve an operation exactly, the transaction rolls back to a
  savepoint (releasing the stripe) and locks every stripe of the org in ascending order. Grants,
  refunds, expiry sweeps and auto-recharges always use it.
- **Deterministic acquisition (no polling):** one `FOR UPDATE SKIP LOCKED` statement takes the first
  idle stripe of a rotation derived from `hash(operation key)`; if every eligible stripe is busy the
  operation queues on its rotation's home stripe in PostgreSQL's FIFO lock queue. Critical sections
  never wait for another wallet lock once their stripes are held, so that wait is bounded. Size
  `wallet_stripe_count` to the expected number of concurrent writers per org.

### Hierarchical credit pools
`org_balances.parent_org_id` links a sub-org to a parent (up to 4 levels). Deductions and hold
reservations use the sub-org's own grants first (FIFO) and cascade any remaining amount or deficit
up the ancestor chain. A hold records where its reservation lives as ordered portions
(`credit_hold_portions`): settlement charges the sub-org's own reservation and free credits first and
only the excess from ancestor pools; portions are released LIFO. Credits reserved by other holds are
never consumed, in any pool.

### Canonical lock order
Every transaction acquires wallet stripe locks in strictly increasing `(org depth DESC, org_id,
stripe_no)` order — descendants before ancestors, ascending stripes within an org — after at most
one `stripe_webhook_events` idempotency row. Holds, portions, lots, grants and outbox rows are only
touched while their owning stripe(s) are held. A per-transaction `LockLedger` enforces the order at
runtime (`LockOrderViolationError`), so parent deductions, sub-orgs cascading holds into the parent,
LIFO settlements of cascaded holds and org-wide maintenance cannot form a deadlock cycle. See
`src/services/OrgWallet.ts` and `src/services/WalletChain.ts`.

### Auto-recharge transactional outbox
A deduction, reservation or settlement that leaves a drawn pool at or below its auto-recharge
threshold commits an intent into `auto_recharge_outbox` in the same transaction (a plain insert — it
never blocks and never waits for a top-up). A relay (kicked right after commit, and run by the
maintenance worker) takes a FULL lock per org, re-checks the threshold and monthly cap exactly, and
coalesces all pending intents into at most one order (`auto_recharge_orders.intent_id` is unique).
Failed relay runs leave intents pending for retry; the deductions are unaffected.

### Daily usage rollups
The write path appends immutable rows to `usage_rollup_deltas` (no upsert, no shared row). The
maintenance worker's compactor moves deltas into `usage_daily_rollups` with one atomic statement per
batch; reports read totals plus pending deltas in a single query.

### Credit burst limiter
Metered deductions, hold reservations and hold extensions are admitted through a Redis sliding
window per org (`credits:burst:<orgId>`, atomic Lua script). Each admitted spend leaves the window
exactly `CREDIT_BURST_WINDOW_MS` after its admission, the key's expiry follows the newest entry, and
time is taken from the Redis server clock so all instances agree. Limits come from the plan tier
(`CREDIT_BURST_LIMIT_*`) or `org_balances.burst_limit_credits_per_minute`. Idempotent replays are
free, failed operations give their budget back, and the limiter fails open on Redis errors/timeouts.

### Wallet invariants
- per stripe: `available_credits` = remaining credits of its lots of ACTIVE grants, never negative
- per stripe: `reserved_credits` = portions of ACTIVE holds reserved on it, never negative; every
  ACTIVE hold's portions add up to its estimate
- every charged credit is backed by a FIFO lot allocation and counted in the daily rollup
- lots past `expires_at` are never spent or counted as available, even before the sweep retires them

### Order credits (platform order flow)
- `POST /credits/orders/:orderId/hold` — reserve credits for an order at checkout (`{ orgId, credits, ttlSeconds? }`). Same reservation mechanics as stream holds (own pool first, ancestors, burst limit); at most one ACTIVE hold per order (unique index), so re-placing it returns the existing hold (`200`).
- `src/integration/OrderCreditSettlementService.ts` consumes HyperPay's `PAYMENT_COMPLETED` (queue `billing.payment-results`): SUCCESS captures the order's hold (credits charged FIFO, model `order-credit`), FAILED releases it. The captured credits × `ORDER_CREDIT_HALALAS_PER_CREDIT` (default 100) become the order's allowance in the `billing.order-credits.settled` event. Checkpoint, hold transition (joined into the same transaction), `order_credit_settlements` row (one per payment, one successful capture per order) and outbox event commit atomically.
- `GET /credits/orders/:orderId/settlement` — what billing did for an order, with its checkpoints.
- **Keyed holds (split-tender checkouts)**: `POST /credits/orders/:orderId/hold` accepts a `holdKey`. The key is claimed in `order_credit_hold_keys` inside the reservation transaction, so a retried call returns the same hold. The response includes `allowanceHalalas` and `halalasPerCredit`. `POST /credits/orders/:orderId/hold/release` `{ holdKey, reason }` returns `RELEASED` / `ALREADY_RELEASED` / `TOMBSTONED` (the key was never placed: any later placement with it is refused with `409 HOLD_CANCELLED`). It returns `409` once the hold is settled.
- The consumer normalises both HyperPay's `PAYMENT_COMPLETED` and the coordinator's `checkout.payment.completed` (exchange `checkout.events`) to one `PaymentOutcome` (`handlePaymentOutcome`). The tender (`CARD` | `WALLET`) is passed on to the ledger in `billing.order-credits.settled`.

### Merchant settlement fees (platform settlement workflow)
- `src/services/SettlementFeeCalculator.ts`: tiered pricing in integer halalas. Marginal tiers in basis points of the settled gross, a fixed fee per settled transaction and a minimum fee. The percentage is rounded once with banker's rounding, and the per-tier shares add up to it exactly. VAT on the fee is computed exactly as ZATCA's engine invoices it. Fee + VAT never exceed the gross (capped).
- Rules live in `settlement_fee_rules`: versioned, one active per merchant, with `merchant_id NULL` as the platform default. The seed is `std-tiered` v1: up to 50,000 SAR at 2.50 %, up to 250,000 SAR at 2.00 %, above that 1.50 %, plus 0.50 SAR per transaction.
- `src/integration/SettlementFeeService.ts` consumes `ledger.merchant-settlement.collected` (queue `billing.settlement-fees`). The checkpoint, the `settlement_fee_quotes` row (primary key = batch: priced once, and frozen even if the rules change later) and `billing.settlement-fee.calculated` in the outbox commit in one transaction.
- API: `GET /billing/settlement-fees/rules`, `POST /billing/settlement-fees/rules` (publishes a new version and retires the previous one), `POST /billing/settlement-fees/quote` (preview), `GET /billing/settlement-fees/batches/:batchId`.

### Event authentication

Every event this service publishes is signed with its stream's shared key (HMAC-SHA256,
`x-key-id` / `x-event-stream` / `x-event-issuer` / `x-event-audience` / `x-signature`), and every
delivery it consumes is verified against the authorization matrix before the body is parsed: wrong
key, wrong issuer, wrong audience or a tampered byte means the message is dead-lettered at once
(`UNTRUSTED_EVENT`). See `shared/security/streams.ts` and the root README.

### Customer refunds
- `src/integration/OrderCreditRefundService.ts` consumes `REFUND_PROCESSED` (queue `billing.refunds`) and restores the captured promotional credits pro rata: after a refund bringing the refunded total to R on a payment of P, the org holds ⌊C × R / P⌋ of the C captured credits back (a new PROMOTIONAL grant, priority 1, expiring when the consumed grants would have or after `ORDER_CREDIT_RESTORE_TTL_DAYS`). One row per refund id in `order_credit_refunds`, written with the checkpoint, the grant and the `billing.order-credits.restored` outbox event. `GET /credits/orders/:orderId/refunds`.
