# Business Rules

This is the authoritative description of what the wallet & ledger engine guarantees. Every rule here is enforced in code, and most also in the database, and each is covered by the test suite (the test file is named next to each section).

---

## 1. Money representation

- **Integer minor units only.** Every amount is an integer number of the currency's minor unit (cents for USD/EUR/GBP, whole yen for JPY). Field names end in `Cents` for historical reasons; for JPY they mean yen.
- **BIGINT everywhere.** All balance, ledger, fee, quote and rate columns are PostgreSQL `BIGINT`. Internally the service computes with JS `bigint`; a `number` only appears at the API boundary and is range-checked in both directions, so precision is never silently lost.
- **API amounts** must be positive, safe integers (`≤ 2^53 − 1`); anything else is a `400`.

*Tests: `money-engine.test.ts`, `multicurrency-transfer.test.ts` (BIGINT schema check).*

## 2. Currencies and accounts

Supported currencies: **USD, EUR, GBP** (2 decimals) and **JPY** (0 decimals). The `currencies` table and the code registry must match (this is tested).

Every wallet has exactly one currency, and a ledger line can only be booked in its wallet's currency (composite foreign key).

| Account type | Purpose | Balance maintained by | May go negative | Storage |
|---|---|---|---|---|
| `USER` | customer wallet | the app: version-guarded compare-and-swap under a row lock | **no** | one row |
| `ESCROW` | AML-held funds (one per currency) | the app | **no** | one row |
| `FEE_TREASURY` | platform fee revenue (one per currency) | the ledger trigger | **no** | 16 striped buckets |
| `FX_POOL` | platform FX position (one per currency) | the ledger trigger | yes | 16 striped buckets |
| `CLEARING` | mirror of the external banking world (payout settlement) | the ledger trigger | yes | 16 striped buckets |

- **System accounts** (`ESCROW`, `FEE_TREASURY`, `FX_POOL`, `CLEARING`) can never be the source of a withdrawal or a party to a transfer (`422 SYSTEM_ACCOUNT_NOT_ALLOWED`).
- **Seed data:** `wallet_alice` and `wallet_bob` (USD 100.00 each), `wallet_carol` (EUR 100.00), `wallet_dave` (JPY 15,000), plus one clearing, escrow, fee treasury and FX pool account per currency. The legacy IDs `wallet_treasury` and `wallet_escrow` are kept for the USD clearing and escrow accounts.

*Tests: `multicurrency-transfer.test.ts`, `frozen-wallets-and-balances.test.ts`.*

## 3. Fees

- The platform fee is `PLATFORM_FEE_BPS` basis points (integer, 0 to 10,000), read at call time. The default is 0.
- `fee = round_half_even(principal × bps / 10,000)`.
- The fee is charged **on top**: the sender is debited exactly `principal + fee`, and the recipient receives the principal (converted, if cross-currency).
- The fee goes to the **fee treasury of the source currency**. A fee that rounds to 0 writes no ledger line.
- Funds checks use `principal + fee`.

## 4. FX conversion

- Rates are directional rows in `fx_rates`: `rate_scaled = rate × 10^8`, meaning units of quote currency per 1 unit of base currency. A rate is never inverted implicitly, because that would introduce a second rounding.
- `converted = round_half_even(amount × rate_scaled × 10^dstDecimals / (10^8 × 10^srcDecimals))`. This is computed with exact integers, and that final division is the only rounding step.
- A missing pair gives `422 FX_RATE_UNAVAILABLE`. A conversion that would credit 0 gives `422 AMOUNT_TOO_SMALL`.
- **Rounding:** banker's rounding (round half to even) is the only rounding mode in the system.
- **No remainder loss:** cross-currency principal flows `sender → FX pool (source currency)` and `FX pool (destination currency) → recipient`. Each currency's journal sums to exactly zero. The sub-unit rounding residual becomes part of the platform's FX position; it is never created or destroyed.

*Tests: `money-engine.test.ts` (thousands of seeded property checks of the half-even definition), `multicurrency-transfer.test.ts`.*

## 5. Transfers (P2P) and the AML saga

- `fromWalletId` must be owned by the caller (§11). The recipient may be any user wallet.
- Both wallets must exist and be `USER` accounts. Validation happens **before any write**, so a failed transfer leaves no ledger trace.
- Self-transfers are rejected (`400`). An optional `currency` asserts the sender's currency (`422 CURRENCY_MISMATCH`).
- **AML threshold:** if the principal's **USD equivalent** exceeds `AML_REVIEW_THRESHOLD_CENTS` (default 50,000 = $500.00), the transfer is **held**. If no USD rate exists, it is held as well (fail closed).

| Path | Saga steps | HTTP |
|---|---|---|
| Instant (≤ threshold) | `SENDER_DEBITED → [FEE_COLLECTED] → [FX_CONVERTED] → RECIPIENT_CREDITED → TRANSFER_COMPLETED`, one local transaction | `201 COMPLETED` |
| Held (> threshold) | `SENDER_DEBITED → ESCROW_HELD → AML_REVIEW_PENDING`: principal + fee moved to the source currency's escrow | `202 PENDING_REVIEW` |
| Review: APPROVE | `ESCROW_RELEASED → [FEE_COLLECTED] → [FX_CONVERTED] → RECIPIENT_CREDITED → TRANSFER_COMPLETED` | `200` |
| Review: REJECT (compensation) | `ESCROW_RELEASED → COMPENSATION_SENDER_REFUNDED → TRANSFER_REVERSED`: full refund, fee included | `200` |

- **Quote locking:** the fee, rate and credited amount are stored on the transfer at initiation. A release uses exactly those numbers even if rates or fees changed during review.
- **Review rules:** only admins may review. A decision is final (`409 TRANSFER_STATE_CONFLICT` afterwards). Concurrent reviews serialize on the transfer row. The audit trail records the **verified token subject** as the reviewer, never a name from the request body.
- **Optimistic concurrency:** an optional `expectedVersion` (the sender's wallet version) gives `409 WALLET_VERSION_CONFLICT` if the wallet changed.

*Tests: `transfer-saga.test.ts`, `multicurrency-transfer.test.ts`, `auth.test.ts`.*

## 6. Withdrawals and payouts

- The source wallet must be owned by the caller, be a `USER` account, and not be frozen.
- It is debited in one transaction, with a `PROCESSING` payout and a journal against the **clearing account of the wallet's currency**.
- **Bank webhooks** (`payout_webhook` role only) drive a state machine:

| Current | `payout.settled` | `payout.failed` |
|---|---|---|
| `PROCESSING` | → `SETTLED` | → `REVERSED` + refund (compensation), exactly once |
| `SETTLED` | ignored | → `RECONCILIATION_REQUIRED` (no money moves) |
| `REVERSED` | → `RECONCILIATION_REQUIRED` (refund is never clawed back) | ignored |
| `RECONCILIATION_REQUIRED` | ignored | ignored |

- Replays of the same `eventId` are acknowledged and ignored.
- A refund into a frozen wallet returns `409` **without** recording the event, so the bank's retry is processed after the wallet is repaired.

*Tests: `withdraw-funds.test.ts`, `payout-webhook.test.ts`.*

## 7. Concurrency rules

- **Single transaction:** every money movement is one database transaction: balance changes, payout/transfer rows, saga steps and a balanced journal, all-or-nothing.
- **Pessimistic and optimistic locking together:** wallets are locked `FOR UPDATE`, and every balance change is also a version-guarded compare-and-swap.
- **Deadlock-free lock hierarchy**, always acquired in this order:
  1. the saga or payout row;
  2. user and escrow wallets, sorted by `wallet_id`;
  3. sharded system-account buckets, in `(wallet_id, shard_no)` order.

  Journal lines are posted sorted by `wallet_id`, and each transaction writes one bucket per sharded account. Opposite-direction transfers (A→B and B→A, USD→EUR and EUR→USD) therefore cannot deadlock.

*Tests: `transfer-saga.test.ts` (lock order), `frozen-wallets-and-balances.test.ts` (journal order).*

## 8. Ledger integrity (enforced at COMMIT by PostgreSQL)

1. **Sync, O(1):** for every unsharded wallet, `balance_cents = ledger_balance_cents`. The running total and `ledger_version` are updated incrementally, one delta per ledger row; history is never re-summed on the write path.
2. **Journal balance:** every `transaction_ref` sums to 0 **per currency**, so each currency's ledger sums to zero globally, by construction.
3. **Append-only:** ledger rows cannot be updated or deleted. DEBIT lines are negative and CREDIT lines positive.
4. **Sign rules:**
   - `USER` and `ESCROW` rows must stay ≥ 0 (CHECK).
   - `FEE_TREASURY` must stay ≥ 0 **in total**. A debit locks all its buckets and checks the true sum, which prevents write skew.
   - `FX_POOL` and `CLEARING` are unrestricted.
5. **New wallets** start with an empty ledger: their precalculated totals are forced to 0, and an unbacked opening balance cannot commit.

## 9. Sharded balances (hot accounts)

- **Why:** fee, FX-pool and clearing accounts are touched by nearly every transfer in their currency. Each keeps its balance in `shard_count` (default 16) striped buckets in `wallet_balance_shards`. Its `wallets` row is static (a CHECK pins its balance columns to 0), so it is never written, and never locked, by a transfer.
- **Bucket choice:** a transaction updates bucket `txid % shard_count`. Concurrent transactions therefore spread round-robin across the buckets instead of queueing on one row lock.
- **Live balance** = `SUM(buckets)` (O(shard_count), a small constant). It is served by `GET /wallets/:id` together with `ledgerVersion = SUM(entry_count)`.
- A sharded account's balance **is** its ledger total; there is no separate projection. Sharded accounts are therefore never frozen. Bucket corruption is found by the audit and rebuilt by reconciliation.

*Tests: `frozen-wallets-and-balances.test.ts` (32 concurrent transfers use all 16 buckets evenly and never write the account row; true-total debit check).*

## 10. Drift, freezing and reconciliation

- **Drift:** an unsharded wallet whose `balance_cents ≠ ledger_balance_cents`. This can only happen through an out-of-band change (for example an ops script run with triggers disabled, or a bad restore).
- **Frozen:**
  - Any withdraw, transfer, review or refund touching a drifted wallet returns `409 WALLET_FROZEN_PENDING_RECONCILIATION` (with `walletId`). The check runs under the wallet's row lock, so it is race-free, and fails before any write.
  - A commit-time backstop maps the same condition to the same 409, but only if the committed state is confirmed drifted; a genuine bug stays a 500.
- **Self-healing:** every freeze publishes a `WALLET_DRIFT` event (deduplicated per wallet via Redis for 30 s), so repair starts without waiting for an audit.
- **Audit** (`POST /admin/ledger/audit`, admins only): the only place history is re-summed, and it runs offline with keyset pagination. It verifies:
  - per-currency zero-sum;
  - per-journal balance;
  - non-negative balances;
  - per wallet, that `balance`, running total, entry count and bucket layout all match a full recount.

  Each page is fully delivered to the broker before the next is read. When the queue is full the audit waits for consumers; if nothing drains within the deadline it reports `completed: false` with an `unpublished` count. It never drops events silently.
- **Reconciliation worker:**
  1. Locks the wallet row, and for sharded accounts every bucket, `FOR UPDATE`.
  2. Verifies the ledger as the source of truth: every journal touching the wallet balances, and the sum is ≥ 0 where required.
  3. Rebuilds the cached columns or buckets from the recount, bumps `version`, and records the correction in `reconciliation_log`.

  If the ledger itself is untrustworthy, nothing is changed and the event goes to the dead-letter queue for manual review. Global violations (zero-sum, unbalanced journal, negative balance) are re-verified and escalated to the DLQ.

*Tests: `ledger-audit.test.ts`, `frozen-wallets-and-balances.test.ts`.*

## 11. Authentication and authorization

- **Authentication:** every route except `/health` requires `Authorization: Bearer <JWT>`. The token must be signed with the configured key and algorithm, carry the right `iss`/`aud`, a `sub`, and an unexpired `exp`. Anything else is a `401`.
- **Roles** (`roles` claim): `user`, `admin`, `payout_webhook`.

| Capability | user (owner) | user (not owner) | admin | payout_webhook |
|---|---|---|---|---|
| Read a wallet | ✅ | ❌ 404 | ✅ | ❌ 404 |
| Withdraw from / transfer out of a wallet | ✅ | ❌ 404 | ❌ 404 | ❌ 404 |
| Transfer into any user wallet | ✅ | ✅ | own wallets only | n/a |
| Read a transfer | sender or recipient owner | ❌ 404 | ✅ | ❌ 404 |
| Review AML holds | ❌ 403 | ❌ 403 | ✅ | ❌ 403 |
| Run the ledger audit | ❌ 403 | ❌ 403 | ✅ | ❌ 403 |
| Post payout webhooks | ❌ 403 | ❌ 403 | ❌ 403 | ✅ |

- **No IDOR:** ownership (`wallets.owner_id = token sub`) is enforced inside the service on the locked row. "Not yours" returns the same `404` as "doesn't exist", so wallet IDs can't be probed.
- **Least privilege:** admins can read and review, but cannot move customers' money.
- **Secure by construction:** every route must declare an access policy, or the app refuses to start. The app also refuses to start with a missing or short signing secret.
- **Backward compatibility:** `AUTH_MODE=legacy` serves tokenless clients exactly as before, with `Deprecation`/`Warning` headers. Presented tokens are still fully verified and authorized. The default is `enforce`.

*Tests: `auth.test.ts`.*

## 12. Messaging guarantees

- **Delivery:** at-least-once, end to end, and every consumer is idempotent.
- **Publishing:**
  - A publish resolves only after the broker **confirms** it. A broker NACK rejects the caller.
  - A confirm lost to a disconnect, or a publish made while disconnected, is kept in a bounded pending ledger (`EVENT_BUS_MAX_PENDING_PUBLISHES`) and **re-published** on the next session.
  - If reconnection permanently gives up, everything pending fails fast.
- **Reconnection is atomic:** a new connection becomes visible only after the whole session (topology, publish channel, every consumer, capacity listener) has been rebuilt on it. A failure part-way discards that connection and retries with exponential backoff and jitter.
- **Consuming:**
  - A delivery in flight during a drop is redelivered by the broker. The late ack on the dead channel is harmless.
  - Retry copies and DLQ copies are broker-confirmed before the original is acked.
  - If the broker refuses a copy (queue full), the original falls back to the broker's native dead-lettering.
- **No sleeps:** all waits are event-driven (broker confirms, session events, capacity signals). The only timers are cancellable deadlines and the reconnect backoff schedule. A lint rule forbids `setTimeout`/`setInterval` in source and tests.

*Tests: `broker-reconnect.test.ts`, `ledger-audit.test.ts`.*
