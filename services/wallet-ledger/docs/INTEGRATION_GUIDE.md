# Integration Guide

How to call the wallet & ledger API, integrate a bank gateway, consume ledger events, and operate the service. Business semantics are specified in [BUSINESS_RULES.md](./BUSINESS_RULES.md).

---

## 1. Authentication

Every endpoint except `GET /health` requires a bearer token:

```
Authorization: Bearer <JWT>
```

### Token requirements

| Claim | Requirement |
|---|---|
| signature | `AUTH_JWT_ALGORITHM` with `AUTH_JWT_SECRET` (HS256, ≥ 32 bytes) or `AUTH_JWT_PUBLIC_KEY` (RS256/ES256, PEM SPKI). Only the one configured algorithm is accepted: `alg: none` and algorithm confusion are rejected. |
| `iss` | must equal `AUTH_JWT_ISSUER` (default `wallet-ledger-auth`) |
| `aud` | must equal `AUTH_JWT_AUDIENCE` (default `wallet-ledger-api`) |
| `sub` | required. For customers, the **owner id** of their wallets (e.g. `user_alice`). |
| `exp` | required; `AUTH_CLOCK_TOLERANCE_SECONDS` (default 30) of skew is allowed |
| `roles` | array containing any of `user`, `admin`, `payout_webhook`. A token with no known role gets `403`. |

Example payload:

```json
{ "sub": "user_alice", "roles": ["user"], "iss": "wallet-ledger-auth", "aud": "wallet-ledger-api", "exp": 1767225600 }
```

### Getting a token

- **Production:** tokens are issued by your identity provider. Configure the service with the provider's public key (`AUTH_JWT_PUBLIC_KEY`, `AUTH_JWT_ALGORITHM=RS256`) and its issuer/audience.
- **Local development** (HS256 shared secret):

  ```bash
  export AUTH_JWT_SIGNING_MATERIAL=<configured-at-runtime>
  npm run token:mint -- --sub user_alice --roles user --ttl 15m
  npm run token:mint -- --sub ops_jane --roles admin
  npm run token:mint -- --sub svc_bank_gateway --roles payout_webhook
  ```

  The script refuses to run with `NODE_ENV=production`.

### Auth errors

| Status | Body `code` | When | `WWW-Authenticate` |
|---|---|---|---|
| 401 | `UNAUTHENTICATED` | no header | `Bearer realm="wallet-ledger", error="invalid_request"` |
| 401 | `UNAUTHENTICATED` | malformed header, or a bad, expired, wrong-issuer or wrong-audience token | `… error="invalid_token"` (no details are leaked) |
| 403 | `FORBIDDEN` | valid token, but the route needs a role it lacks | none |
| 404 | `WALLET_NOT_FOUND` / `TRANSFER_NOT_FOUND` | the resource exists but is not yours (the same response as "doesn't exist") | none |

### Migrating existing (pre-auth) clients

The API contract (routes, request bodies, response bodies, success status codes) is unchanged. To migrate without downtime:

1. Deploy with `AUTH_MODE=legacy`. Tokenless requests keep working and are tagged with `Deprecation: true` and a `Warning` header. Any token that **is** sent is fully verified and authorized.
2. Update clients to send tokens; monitor for responses still carrying `Deprecation`.
3. Switch to `AUTH_MODE=enforce` (the default). The mode is read per request.

Behaviour change to note: for AML reviews, the recorded `reviewedBy` is now the verified token subject; a body-supplied `reviewedBy` is ignored for authenticated callers.

---

## 2. Endpoints

All amounts are integers in the **minor units of the relevant wallet's currency**.

### `GET /health`
Public. `200 {"status":"ok","service":"wallet-ledger-engine"}`.

### `GET /wallets/:walletId`
**Access:** the owner, or an admin.

```json
{
  "walletId": "wallet_alice", "ownerId": "user_alice", "currency": "USD", "accountType": "USER",
  "balanceCents": 10000, "ledgerBalanceCents": 10000, "ledgerVersion": 1,
  "shardCount": 0, "version": 1, "frozenPendingReconciliation": false, "updatedAt": "…"
}
```

For sharded system accounts, `balanceCents` is the live sum over buckets (it may be negative for `FX_POOL`/`CLEARING`), and `ledgerVersion` counts their entries.

### `POST /wallets/withdraw`
**Access:** the owner of `walletId`.

```json
{ "walletId": "wallet_alice", "amountCents": 2500, "bankAccountRef": "DE89…", "idempotencyKey": "opt", "expectedVersion": 3 }
```

`201`:

```json
{ "payoutId": "po_…", "walletId": "wallet_alice", "currency": "USD", "amountCents": 2500, "remainingBalanceCents": 7500, "version": 4, "status": "PROCESSING" }
```

### `POST /wallets/transfer`
**Access:** the owner of `fromWalletId`. Any user wallet may be the recipient.

```json
{ "fromWalletId": "wallet_alice", "toWalletId": "wallet_carol", "amountCents": 5000, "currency": "USD", "referenceId": "inv-42", "expectedVersion": 3 }
```

- `201` (completed) or `202` (held for AML review).
- The response body includes the locked quote: `feeBps`, `feeCents`, `totalDebitCents`, `fxRateScaled` (× 10^8), `creditedAmountCents`, `sourceCurrency`, `destCurrency`, plus `senderRemainingBalanceCents`, `senderVersion` and `amlReviewRequired`.

### `GET /transfers/:transferId`
**Access:** the sender's owner, the recipient's owner, or an admin. Returns the transfer, its locked quote, and `sagaSteps` in order.

### `POST /transfers/:transferId/review`
**Access:** admin.

```json
{ "decision": "APPROVE", "reviewNote": "docs verified" }
```

`200` returns `status` `COMPLETED` with outcome `RELEASED_TO_RECIPIENT`, or `REVERSED` with outcome `COMPENSATED_REFUNDED_TO_SENDER`.

### `POST /payouts/webhook`
**Access:** the `payout_webhook` role (see §3).

### `POST /admin/ledger/audit`
**Access:** admin.

```json
{ "publish": true }
```

Returns the audit report: `ok`, `completed`, `incompleteReason?`, and per-check `{ violations, sample }`, plus `events: { published, unpublished, backpressureWaits }`.

### Error catalogue

| Status | `code` | Meaning / client action |
|---|---|---|
| 400 | `Validation failed` (`details`) | Fix the request. Covers non-integer, unsafe or non-positive amounts, and self-transfers. |
| 401 / 403 | `UNAUTHENTICATED` / `FORBIDDEN` | See §1. |
| 404 | `WALLET_NOT_FOUND`, `TRANSFER_NOT_FOUND`, `PAYOUT_NOT_FOUND` | Unknown, or not yours. |
| 409 | `INSUFFICIENT_FUNDS` | Balance can't cover `principal + fee`. |
| 409 | `WALLET_VERSION_CONFLICT` | Your `expectedVersion` is stale: re-read the wallet and retry. |
| 409 | `TRANSFER_STATE_CONFLICT` | The transfer was already reviewed. |
| 409 | `WALLET_FROZEN_PENDING_RECONCILIATION` (+ `walletId`) | A ledger inconsistency is being repaired automatically. Retry with backoff, typically within seconds. |
| 422 | `SYSTEM_ACCOUNT_NOT_ALLOWED`, `CURRENCY_MISMATCH`, `FX_RATE_UNAVAILABLE`, `AMOUNT_TOO_SMALL`, `UNSUPPORTED_CURRENCY` | Business rule refusal: don't retry unchanged. |

**Retries:** `409 WALLET_FROZEN_PENDING_RECONCILIATION` and `409 WALLET_VERSION_CONFLICT` are retryable. Withdrawals are not yet deduplicated by `idempotencyKey` (see the README's known limitations), so don't blindly retry a withdrawal that timed out: check the wallet and its payouts first.

---

## 3. Bank gateway (payout webhooks)

Authenticate as a service principal with the `payout_webhook` role. Send:

```json
{ "eventId": "evt_123", "payoutId": "po_…", "eventType": "payout.settled", "bankReference": "opt", "failureReason": "opt" }
```

`eventType` is `payout.settled` or `payout.failed`.

- **Deliver at least once, and reuse the same `eventId` on retries.** Replays are acknowledged (`200`, outcome `DUPLICATE_IGNORED`).
- `200` outcomes: `SETTLED`, `REVERSED_AND_REFUNDED`, `RECONCILIATION_REQUIRED` (conflicting or out-of-order events; a human resolves these, and money never moves automatically), or `DUPLICATE_IGNORED`.
- `409 WALLET_FROZEN_PENDING_RECONCILIATION`: the refund target is being repaired. The event was **not** recorded, so retry later.
- `404 PAYOUT_NOT_FOUND`: unknown payout. Don't retry.

---

## 4. Ledger events (RabbitMQ)

The service publishes invariant violations and consumes them itself (the reconciliation worker). Other systems may bind their own queues to the `ledger.events` topic exchange.

| Exchange / queue | Type | Purpose |
|---|---|---|
| `ledger.events` | topic, durable | events; routing key = event type |
| `ledger.reconciliation` | queue, `x-max-length`, `reject-publish` | worker queue (binding `ledger.invariant.#`) |
| `ledger.reconciliation.retry.<n>` | queue, `x-message-ttl` | broker-side retry backoff, back to the worker queue via `ledger.events.requeue` |
| `ledger.events.dlx` → `ledger.reconciliation.dlq` | topic / queue | dead letters for manual review |
| `ledger.events.capacity` | fanout, non-durable | "queue space freed" signals (internal backpressure) |

**Event types:** `ledger.invariant.wallet_drift`, `ledger.invariant.negative_balance`, `ledger.invariant.global_zero_sum`, `ledger.invariant.unbalanced_journal`.

Envelope (amounts are decimal **strings**, since they are bigints):

```json
{ "eventId": "evt_…", "type": "ledger.invariant.wallet_drift", "occurredAt": "…", "auditRunId": "audit_… | guard_…",
  "detectedBy": "AUDIT | WRITE_GUARD",
  "payload": { "walletId": "wallet_alice", "currency": "USD", "accountType": "USER",
               "balanceCents": "9000", "ledgerBalanceCents": "10000", "ledgerVersion": "1",
               "ledgerSumCents": "10000", "ledgerEntryCount": "1" } }
```

**Headers:** `x-attempt` (delivery attempt number), plus `x-failure-reason`, `x-failed-at` and `x-original-routing-key` on retried and dead-lettered copies.

**Delivery:** at least once. Consumers must be idempotent; key on `eventId` or re-verify state.

---

## 5. Operations

### Configuration

| Variable | Default | Notes |
|---|---|---|
| `PORT` | 3002 | |
| `DB_SEED_ON_START` | `true` | create + seed the schema at boot (required for the in-memory sandbox DB) |
| `AUTH_MODE` | `enforce` | `legacy` = migration mode (§1) |
| `AUTH_JWT_SECRET` | — | required for HS256 (≥ 32 bytes) |
| `AUTH_JWT_PUBLIC_KEY` / `AUTH_JWT_ALGORITHM` | — / HS256 | asymmetric verification (RS256 default when a key is set) |
| `AUTH_JWT_ISSUER` / `AUTH_JWT_AUDIENCE` | `wallet-ledger-auth` / `wallet-ledger-api` | |
| `AUTH_CLOCK_TOLERANCE_SECONDS` | 30 | |
| `PLATFORM_FEE_BPS` | 0 | integer, 0 to 10,000 |
| `AML_REVIEW_THRESHOLD_CENTS` | 50000 | USD minor units |
| `EVENT_BUS_DRIVER` / `RABBITMQ_URL` | `fake` / `amqp://localhost` | use `amqp` beyond local dev |
| `EVENT_BUS_RECONNECT_INITIAL_MS` / `EVENT_BUS_RECONNECT_MAX_MS` | 100 / 30000 | exponential backoff with jitter |
| `EVENT_BUS_MAX_PENDING_PUBLISHES` | 10000 | unconfirmed-publish buffer (memory bound) |
| `RECONCILIATION_PREFETCH` / `RECONCILIATION_MAX_ATTEMPTS` | 10 / 4 | |
| `RECONCILIATION_RETRY_DELAYS_MS` | `1000,5000,30000` | per-tier TTLs |
| `RECONCILIATION_MAX_QUEUE_LENGTH` / `RECONCILIATION_MAX_DLQ_LENGTH` | 10000 / 100000 | must exceed the prefetch |
| `LEDGER_AUDIT_PAGE_SIZE` / `LEDGER_AUDIT_SAMPLE_SIZE` | 500 / 20 | |
| `LEDGER_AUDIT_DRAIN_TIMEOUT_MS` | 60000 | audit gives up (INCOMPLETE) after this long without queue capacity |
| `FROZEN_WALLET_EVENT_DEDUPE_SECONDS` / `FROZEN_WALLET_PUBLISH_TIMEOUT_MS` | 30 / 2000 | |

### Runbook

- **Schedule the audit.** Call `POST /admin/ledger/audit` periodically. `ok: false` with events published means reconciliation is in progress; `completed: false` means the worker isn't draining (check the worker and broker).
- **DLQ non-empty.** Each message needs a human. `x-failure-reason` says why:
  - `SOURCE_OF_TRUTH_UNDETERMINED`: the wallet's ledger itself is inconsistent.
  - `MANUAL_REVIEW_*`: a global violation; post a correcting journal.
  - Anything else: attempts were exhausted on a transient error. Re-publish it once the cause is fixed.
- **Frequent `409 WALLET_FROZEN_PENDING_RECONCILIATION`.** Check that the reconciliation worker is consuming, then look at `reconciliation_log` for what was corrected.
- **Broker outage.** Publishes queue in memory (up to `EVENT_BUS_MAX_PENDING_PUBLISHES`, after which they fail fast) and are re-sent on reconnect. Consumers resume automatically.

### Local development

```bash
npm install
cp .env.example .env   # set AUTH_JWT_SECRET
npm run dev            # API + reconciliation worker; seeds the in-memory sandbox DB at boot
npm run check          # typecheck + lint + tests
```

The sandbox database is in-memory per process, which is why the server seeds itself (`DB_SEED_ON_START=true`). Set it to `false` when pointing the service at a persistent database. The server also fails fast (exit code 1) on unsafe auth configuration, and shuts down gracefully on SIGTERM/SIGINT: it drains in-flight deliveries and closes the broker session.
