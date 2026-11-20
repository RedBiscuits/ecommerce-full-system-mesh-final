# hyperpay-integration

> **Production-ready Node.js + TypeScript microservice** for HyperPay / OPPWA payment processing.  
> Consumes payment verification events from RabbitMQ, queries the OPPWA API for the authoritative status, and persists every outcome to a local JSON database.  
> Processing is **idempotent**: webhook retries, AMQP redeliveries and duplicate events all converge on exactly one record per payment.  
> Failed messages are **dead-lettered** to `hyperpay.dlq`, where operators can dump and replay them via an authenticated admin API.  
> Every outcome is announced downstream through a **transactional outbox**: the domain event (`PAYMENT_COMPLETED`, `TOKEN_REGISTERED`, `REFUND_PROCESSED`) is written in the same DB transaction as the state change and published from there with publisher confirms — no dual write, nothing lost to a crash or a broker outage.  
> A zero-dependency **circuit breaker** sits in front of the gateway client: an outage opens it, work arriving meanwhile is parked in the outbox instead of piling up or dead-lettering, and the first parked request is the half-open probe that brings it back.  
> **Recurring-charge batches** are executed by a worker pool under a Redis-backed **distributed lock** per customer (Redlock semantics, atomic Lua scripts, watchdog-extended TTLs) — at most one charge per customer at a time across every instance; a colliding item goes back to the queue with jittered backoff.  
> The webhook is **HMAC-signed**, the broker connection **self-heals** (backoff reconnect, consumer re-attach, queue migration), tokenised card data lives in an **AES-256-GCM vault** (never in clear on disk), operators drive the **full transaction lifecycle** (charge, partial capture, partial refund, reverse, void) through a rule-checked ledger, a **reconciler** keeps local state in step with the gateway (on a schedule and on demand), a **remediation** command corrects outcomes the provider disagrees with — and the whole thing is covered by 886 unit / feature / integration / chaos tests.

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                 hyperpay-integration                │
│                                                     │
│  ┌──────────────┐      ┌──────────────────────────┐ │
│  │  HTTP Ingress│      │   RabbitMQ Consumer      │ │
│  │  POST        │      │   queue: hyperpay.       │ │
│  │  /api/webhook│      │         payments         │ │
│  │  /hyperpay   │      └────────────┬─────────────┘ │
│  └──────┬───────┘                   │               │
│         │          both call        │               │
│         └──────────────────────────►               │
│                        ┌────────────▼─────────────┐ │
│                        ┌────────────▼─────────────┐ │
│                        │   paymentProcessor.ts    │ │
│                        │  idempotency key lookup  │ │
│                        │  fetchPaymentStatus()    │ │
│                        │  ONE DB TRANSACTION:     │ │
│                        │   vault · payments ·     │ │
│                        │   outbox_events          │ │
│                        └────────────┬─────────────┘ │
│                                     │               │
│              ┌──────────────────────┤               │
│              │                      │               │
│    ┌─────────▼──────────┐  ┌───────▼─────────────┐ │
│    │  circuit breaker   │  │    db.json (local)  │ │
│    │  CLOSED/OPEN/HALF  │  │    JSON flat-file   │ │
│    │  ┌──────────────┐  │  │    database         │ │
│    │  │ OPPWA HTTP   │  │  │                     │ │
│    │  │ API          │  │  │  open → requests    │ │
│    │  └──────────────┘  │  │  DEFERRED here,     │ │
│    │  open: refuse fast │  │  replayed as the    │ │
│    └────────────────────┘  │  half-open probe    │ │
│                            └───────┬─────────────┘ │
│                                    │ outbox_events │
│                     ┌──────────────▼─────────────┐ │
│                     │   outboxPublisher.ts       │ │
│                     │  claim (lease) → publish   │ │
│                     │  → confirm → PUBLISHED     │ │
│                     └──────────────┬─────────────┘ │
│                                    │               │
│   hyperpay.results (payment results, legacy)  ◄────┤
│   hyperpay.events  (every domain event)       ◄────┘
│                                                     │
│  NACK ──► hyperpay.dlx ──► hyperpay.dlq (parked)    │
│           ▲                    │                    │
│           └── POST /api/admin/replay-dlq ───────────┘
└─────────────────────────────────────────────────────┘
```

### Key Design Principles

| Principle | Implementation |
|---|---|
| **Single processor** | Both HTTP and queue paths call `paymentProcessor.ts` — no duplicated logic |
| **Never trust the payload** | Webhook/queue payload is treated as a hint only; OPPWA is always re-queried |
| **Always ACK fast** | HTTP returns 200 immediately; queue ACK happens after the DB transaction (record + outbox event) commits |
| **No dual writes** | Nothing on a request path publishes to RabbitMQ. Domain events are rows in `outbox_events`, written in the same atomic DB transaction as the state they describe, and delivered by a separate publisher with confirms, leases and retries — see [Transactional outbox](#transactional-outbox) |
| **One charge per customer, cluster-wide** | Recurring charges run in a worker pool that takes `lock:charge:<customerId>` on Redis before calling the gateway (Redlock semantics, Lua compare-and-set/extend/delete, watchdog). Held elsewhere → back to the queue with full-jitter backoff; no lock store → nothing is charged — see [Distributed locks & the charge worker pool](#distributed-locks--the-charge-worker-pool) |
| **Fail fast, recover from the outbox** | A circuit breaker wraps every gateway call. During an outage calls are refused at once instead of queueing against a dead gateway; verify requests are parked in `outbox_events` (`DEFERRED`) and the oldest one is the half-open probe; a failed probe doubles the open period — see [Gateway circuit breaker](#gateway-circuit-breaker) |
| **Idempotent processing** | Any number of calls for the same payment yield one DB record — see [Idempotency](#idempotency) |
| **NACK without requeue** | Processing failures → `hyperpay.dlx` → `hyperpay.dlq`, not infinite retry loops |
| **Operator-driven replay** | Parked messages are dumped / replayed through `/api/admin/*`, guarded by a shared secret |
| **Config-driven** | All credentials and topology via `.env` — zero code changes between test/prod |
| **Signed webhooks** | `POST /api/webhook/hyperpay` requires an HMAC-SHA256 signature over the raw body + timestamp; forged and replayed deliveries get `401` |
| **Vaulted tokenisation** | Registration tokens + card data are sealed with AES-256-GCM into a separate collection; the payment record holds only a `vaultId`, and the type system forbids persisting the raw card block |
| **Token-free money movement** | Admin DB/PA/CP/RF/RV operations unseal the token in-process and hand it straight to the gateway; it never appears in responses, logs, error messages or records. Every operation requires an `Idempotency-Key` |
| **Ledger-checked lifecycle** | Partial captures / refunds, reverse and void are validated against a read model of the transaction and its children *before* the gateway is called (`422` with the exact reason) |
| **Reconciled, remediated** | PENDING records are re-verified on a schedule; FAILED records the provider now approves are corrected through the normal pipeline and re-announced downstream |
| **Self-healing AMQP** | Topology is reconciled on every connect (legacy queues migrated without message loss); connection *or* channel loss triggers exponential-backoff reconnect with the consumer re-attached |

---

## Prerequisites

- **Node.js ≥ 18**
- **RabbitMQ** running locally or reachable (default: `amqp://localhost:5672`)
- HyperPay sandbox account ([developer.hyperpay.com](https://developer.hyperpay.com))

---

## Quick Start

```bash
# 1. Clone and install
cd hyperpay-integration
npm install

# 2. Configure
cp .env.example .env
# Edit .env with your HyperPay credentials and RabbitMQ URL

# 3. Run in development (hot-reload)
npm run dev

# 4. Or build and run in production
npm run build
npm start
```

The service starts two listeners:

| Listener | Address |
|---|---|
| HTTP server | `http://localhost:3000` |
| RabbitMQ consumer | `hyperpay.payments` queue |

---

## Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `PORT` | No | `3000` | HTTP server port |
| `NODE_ENV` | No | `development` | Deployment environment |
| `HYPERPAY_HOST` | **Yes** | — | OPPWA base URL (`https://eu-test.oppwa.com` or live) |
| `HYPERPAY_USER_ID` | **Yes** | — | Merchant user ID |
| `HYPERPAY_PASSWORD` | **Yes** | — | Merchant password |
| `HYPERPAY_ENTITY_ID` | **Yes** | — | Payment channel entity ID |
| `RABBITMQ_URL` | No | `amqp://localhost:5672` | RabbitMQ connection URL |
| `RABBITMQ_EXCHANGE` | No | `hyperpay` | Direct exchange name |
| `RABBITMQ_QUEUE` | No | `hyperpay.payments` | Inbound queue |
| `RABBITMQ_ROUTING_KEY` | No | `payment.verify` | Inbound routing key |
| `RABBITMQ_RESULTS_QUEUE` | No | `hyperpay.results` | Outbound results queue (payment results only — unchanged for existing consumers) |
| `RABBITMQ_EVENTS_QUEUE` | No | `hyperpay.events` | Domain-event stream: bound to every event routing key |
| `RABBITMQ_TOKEN_REGISTERED_ROUTING_KEY` | No | `token.registered` | Routing key of `TOKEN_REGISTERED` |
| `RABBITMQ_CHARGES_QUEUE` / `RABBITMQ_CHARGE_ROUTING_KEY` | No | `hyperpay.charges` / `charge.execute` | Recurring-charge items (dead-lettered like the main queue) |
| `REDIS_URL` / `REDIS_NODES` | No | — | Lock store: one URL, or a comma list of independent masters for quorum. **Unset = charge workers disabled** |
| `REDIS_CONNECT_TIMEOUT_MS` / `REDIS_COMMAND_TIMEOUT_MS` | No | `5000` / `2000` | Fail-fast client timeouts |
| `REDIS_RETRY_DELAY_MS` / `REDIS_MAX_RETRY_DELAY_MS` | No | `500` / `10000` | Reconnect backoff (forever) |
| `LOCK_TTL_MS` | No | `10000` | Lock lifetime without its watchdog |
| `LOCK_DRIFT_FACTOR` | No | `0.01` | Redlock clock-drift allowance |
| `LOCK_WATCHDOG_INTERVAL_MS` | No | `0` (= TTL/3) | Heartbeat that extends held locks |
| `CHARGE_WORKER_CONCURRENCY` | No | `8` | Parallel charges per instance (= charges-queue prefetch) |
| `CHARGE_DEFER_BASE_MS` / `CHARGE_DEFER_MAX_MS` / `CHARGE_DEFER_MIN_MS` | No | `250` / `30000` / `50` | Full-jitter deferral window |
| `CHARGE_MAX_DEFERRALS` | No | `10` | Deferrals before an item is dead-lettered |
| `RABBITMQ_REFUND_PROCESSED_ROUTING_KEY` | No | `refund.processed` | Routing key of `REFUND_PROCESSED` |
| `RABBITMQ_RESULTS_ROUTING_KEY` | No | `payment.result` | Key result events are published with; the results queue is bound to it |
| `RABBITMQ_RETRY_DELAY_MS` | No | `5000` | Base reconnect delay; doubles per consecutive failure |
| `RABBITMQ_MAX_RETRY_DELAY_MS` | No | `30000` | Cap for the reconnect backoff |
| `RABBITMQ_TOPOLOGY_MIGRATION` | No | `recreate` | `recreate` migrates mismatched queues automatically (see below); `fail` refuses to start |
| `RABBITMQ_DLX` | No | `hyperpay.dlx` | Dead-letter exchange (direct, durable) |
| `RABBITMQ_DLQ` | No | `hyperpay.dlq` | Dead-letter queue (durable) |
| `RABBITMQ_DL_ROUTING_KEY` | No | `payment.dead` | Routing key used both as the main queue's `x-dead-letter-routing-key` and the DLQ binding |
| `WEBHOOK_SECRET` | **In production** | — | HMAC-SHA256 secret for webhook signatures. Startup fails in production without it; unsigned deliveries are accepted (with a warning) elsewhere |
| `WEBHOOK_SIGNATURE_TOLERANCE_SEC` | No | `300` | Max age of a signature timestamp (replay window) |
| `VAULT_MASTER_KEY` | **In production** | — | 32-byte AES-256-GCM key (base64/hex). Unset = vault disabled, tokens **discarded** (never stored in clear) |
| `VAULT_MASTER_KEY_ID` | No | `v1` | Label recorded on every envelope; bump when rotating |
| `VAULT_RETIRED_KEYS` | No | — | Decrypt-only previous keys, `id=key,id=key`, for rotation |
| `RECONCILE_INTERVAL_MS` | No | `300000` | Background reconciliation period; `0` disables the job |
| `RECONCILE_BATCH_LIMIT` | No | `200` | Max records per run |
| `GATEWAY_BREAKER_WINDOW_MS` | No | `30000` | Sliding window over which gateway failures are counted |
| `GATEWAY_BREAKER_FAILURE_THRESHOLD` | No | `5` | Outage-class failures inside the window that open the circuit |
| `GATEWAY_BREAKER_OPEN_MS` | No | `60000` | How long the circuit stays open; doubles after every failed probe |
| `GATEWAY_BREAKER_MAX_OPEN_MS` | No | `600000` | Cap for the doubling |
| `GATEWAY_BREAKER_HALF_OPEN_CALLS` | No | `1` | Trial calls allowed at a time while half-open |
| `GATEWAY_RECOVERY_INTERVAL_MS` | No | `30000` | Periodic sweep of deferred verifications while closed; `0` = state changes only |
| `OUTBOX_POLL_INTERVAL_MS` | No | `1000` | Outbox publisher poll interval (commits and reconnects wake it immediately). `0` = timer off, publish on demand |
| `OUTBOX_BATCH_SIZE` | No | `50` | Events leased per pass |
| `OUTBOX_LEASE_MS` | No | `30000` | How long a leased event is invisible to other publishers; an expired lease is reclaimed |
| `OUTBOX_MAX_ATTEMPTS` | No | `10` | Failed attempts (nack / timeout) before an event is parked as `FAILED` |
| `OUTBOX_RETRY_DELAY_MS` / `OUTBOX_MAX_RETRY_DELAY_MS` | No | `1000` / `60000` | Exponential backoff between attempts |
| `OUTBOX_PUBLISH_TIMEOUT_MS` | No | `10000` | Max wait for a publisher confirm |
| `OUTBOX_RETENTION_MS` | No | `86400000` | `PUBLISHED` events older than this are pruned; `0` keeps them |
| `ADMIN_API_KEY` | No | — | Shared secret for `/api/admin/*`. **Unset = admin API disabled (503)** |
| `DB_PATH` | No | `./db.json` | Path to the JSON flat-file database |

---

## API Reference

### `GET /health`
Liveness probe. Returns `200` if the process is running.

```json
{
  "status": "ok",
  "service": "hyperpay-integration",
  "version": "2.0.0",
  "environment": "production",
  "uptime": 3600.12,
  "timestamp": "2026-10-28T10:00:00.000Z"
}
```

### `GET /status/payments`
Returns a summary and the last 100 payment records from the DB.

```json
{
  "summary": { "total": 42, "success": 38, "failed": 3, "pending": 1 },
  "recent": [{ "transactionId": "...", "status": "SUCCESS", ... }]
}
```

### `POST /api/webhook/hyperpay`
HTTP ingress for webhook delivery. Responds `200 { status: "ACK" }` immediately for a validly signed request; processing is asynchronous.

**Body** (one or both):
```json
{ "resourcePath": "/v1/checkouts/ABC123/payment", "id": "ABC123" }
```

**Signature** (required whenever `WEBHOOK_SECRET` is set):

```
X-Webhook-Signature: t=1730000000,v1=5f1c…e9a0
```

- `t` — unix seconds when the sender signed the request.
- `v1` — hex `HMAC-SHA256(secret, "<t>." + <raw request body>)`. Several `v1=` values may be sent while rotating secrets.
- The MAC is verified over the **raw bytes** received, so the sender must sign exactly what it transmits.
- Deliveries whose `t` is more than `WEBHOOK_SIGNATURE_TOLERANCE_SEC` from the server clock are rejected (`timestamp_out_of_tolerance`) — a captured request cannot be replayed later.

Failures answer `401 { error: { message: "Invalid webhook signature (<reason>)" } }` with reason `missing` | `malformed` | `timestamp_out_of_tolerance` | `mismatch`, and nothing is processed.

Reference signer (Node):
```ts
import { sign } from './src/security/webhookSignature';
const body = JSON.stringify(payload);
headers['X-Webhook-Signature'] = sign(process.env.WEBHOOK_SECRET, body, Math.floor(Date.now() / 1000));
```

> **HyperPay specifics:** HyperPay's own dashboard notifications are *encrypted* (AES-256-GCM with `X-Initialization-Vector` / `X-Authentication-Tag` headers) rather than HMAC-signed. This endpoint authenticates callers that implement the scheme above — typically your own gateway adapter. If you point the HyperPay dashboard at it directly, put a small decrypt-and-re-sign adapter in front, or extend the guard with the AES-GCM variant.

### Admin API — `/api/admin/*`

All admin routes require the shared secret from `ADMIN_API_KEY`, sent as either header:

```
X-Admin-Key: <key>
Authorization: Bearer <key>
```

| Response | Meaning |
|---|---|
| `401` | Key missing or wrong (constant-time comparison) |
| `503` | `ADMIN_API_KEY` not configured, or RabbitMQ not connected |
| `409` | Another DLQ operation is already running |
| `400` | Invalid `limit` (must be a positive integer) |

#### `GET /api/admin/dlq?limit=100`
Non-destructive dump of parked messages (head of queue first). Messages are peeked on a throw-away channel and requeued untouched.

```json
{
  "queue": "hyperpay.dlq",
  "messageCount": 2,
  "consumerCount": 0,
  "messages": [
    {
      "correlationId": "your-unique-id",
      "messageId": null,
      "body": { "correlationId": "your-unique-id", "resourcePath": "/v1/checkouts/ABC123/payment" },
      "originalRoutingKey": "payment.verify",
      "deathCount": 2,
      "lastReason": "rejected",
      "lastDeadLetteredAt": "2026-10-28T10:00:00.000Z",
      "replayCount": 1
    }
  ]
}
```

#### `POST /api/admin/replay-dlq`
Pulls parked messages off `hyperpay.dlq` and republishes them to the main exchange with the main routing key, so the consumer processes them again. Optional body `{ "limit": 10 }` caps the number of messages.

```json
{
  "requested": 2,
  "replayed": 2,
  "remaining": 0,
  "messages": [ { "correlationId": "your-unique-id", "deathCount": 2, "replayCount": 2, "...": "..." } ]
}
```

```bash
curl -X POST http://localhost:3000/api/admin/replay-dlq -H "X-Admin-Key: $ADMIN_API_KEY"
```

#### `GET /api/admin/vault?customer=<email>`
Masked payment methods stored for a customer (matched by a keyed fingerprint of the e-mail on the payment):

```json
{ "methods": [ { "vaultId": "…", "paymentBrand": "VISA", "last4Digits": "5556", "expiryMonth": "05", "expiryYear": "2028", "transactionId": "8ac7…", "createdAt": "…" } ] }
```

#### `GET /api/admin/vault/:vaultId`
One masked method (same shape). `404` if unknown.

#### `POST /api/admin/vault/rotate-keys`
Re-seals every record not yet under `VAULT_MASTER_KEY_ID` with the active key. Returns `{ activeKeyId, scanned, reEncrypted }`.

All vault routes answer `503` when `VAULT_MASTER_KEY` is not configured. **There is no route that returns the registration token** — to use a stored card, charge it through the service (below).

### Money movement — `/api/admin/vault/:vaultId/charge`, `/api/admin/payments/:paymentId/*`

Every mutating call requires **two headers**: `X-Admin-Key` and an **`Idempotency-Key`** (1–255 chars, chosen by the caller — e.g. an order id or a UUID). The first call with a key executes; any repeat with the same key and the same request returns the stored outcome (`200`, `Idempotent-Replayed: true`) without touching the gateway, so a retry after a timeout can never charge twice. The same key with a *different* request is refused with `409`.

| Route | Body | OPPWA | Ledger rule |
|---|---|---|---|
| `POST /vault/:vaultId/charge` | `{ paymentType: "DB"\|"PA", amount, currency, merchantTransactionId?, standingInstructionType?: "UNSCHEDULED"\|"RECURRING" }` | `POST /v1/registrations/{token}/payments` as a merchant-initiated transaction (`standingInstruction.mode=REPEATED`, `source=MIT`) | — |
| `POST /payments/:id/capture` | `{ amount, currency }` | `POST /v1/payments/{id}` `CP` | `:id` is a successful PA; `amount ≤ authorised − captured − pending`. **Repeatable** for partial captures |
| `POST /payments/:id/refund` | `{ amount, currency }` | `POST /v1/payments/{id}` `RF` | `:id` is a successful DB **or a capture id**; `amount ≤ that transaction's settled − refunded − pending`. **Repeatable** for partial refunds. A PA root cannot be refunded — refund its captures |
| `POST /payments/:id/reverse` | — | `POST /v1/payments/{id}` `RV` | `:id` is a PA with no captures (not even pending) — releases the hold |
| `POST /payments/:id/void` | — | `POST /v1/payments/{id}` `RV` | `:id` is a DB or a capture with no refunds — cancels before settlement |
| `GET /operations/:id` | — | — | — |

`amount` is a positive decimal string with ≤ 2 decimals (`"49.90"`); `currency` an ISO 4217 code and must match the transaction's. Rule violations answer **`422`** with the exact reason (e.g. `Capture of 25.01 exceeds the 25.00 still available (authorised 100.00, captured 75.00, pending 0.00)`) and the gateway is never called. A gateway **decline is an outcome, not an error**: it is recorded and returned as `201` with `status: "FAILED"` and the OPPWA `resultCode`. Only transport failures (network, 5xx, non-payment replies) produce a `500` — and the same key can then be retried.

### Transaction ledger — `GET /api/admin/transactions[/:id]`

A read model over `payments` (checkout-originated) and `operations` (admin-originated). `:id` may be a root transaction **or any child id** (a capture, refund, …) — it resolves to the root's view. `?lifecycle=PARTIALLY_CAPTURED&limit=50` filters the list.

```json
{
  "transactionId": "pa-1", "origin": "admin", "paymentType": "PA", "amount": "100.00", "currency": "SAR",
  "status": "SUCCESS", "lifecycle": "PARTIALLY_REFUNDED",
  "authorizedAmount": "100.00", "settledAmount": "40.00", "capturedAmount": "75.00", "refundedAmount": "10.00",
  "reserved":  { "capture": "0.00", "refund": "0.00" },
  "available": { "capture": "25.00", "refund": "30.00", "reverse": false, "void": false },
  "operations": [
    { "operation": "CP", "transactionId": "cp-1", "amount": "40.00", "status": "SUCCESS", "…": "…" },
    { "operation": "CP", "transactionId": "cp-2", "amount": "35.00", "status": "SUCCESS" },
    { "operation": "RF", "transactionId": "rf-1", "referencedPaymentId": "cp-1", "amount": "10.00", "status": "SUCCESS" },
    { "operation": "RV", "transactionId": "rv-1", "referencedPaymentId": "cp-2", "status": "SUCCESS" }
  ],
  "vaultId": "…", "createdAt": "…"
}
```

Lifecycle states: `PENDING` → `FAILED` | `AUTHORIZED` → `PARTIALLY_CAPTURED` → `CAPTURED` | `DEBITED` → `PARTIALLY_REFUNDED` → `REFUNDED` | `REVERSED` (PA released) | `VOIDED` (DB / all captures cancelled). PENDING child operations are **reserved**: they reduce availability and block a reversal, but do not move the lifecycle until the gateway settles them.

### Reconciliation & remediation

| Route | What it does |
|---|---|
| `POST /api/admin/reconcile` `{ transactionIds?, since?, includeOperations?=true, dryRun?, limit? }` | Re-verifies every **PENDING** payment and operation against the gateway. Changed payments are updated through the normal processing pipeline (idempotency, vaulting, redaction all apply) and a `PAYMENT_COMPLETED` with `source: "reconciliation"` is queued in the outbox; changed operations are updated in place and announce themselves too (a settled capture → `PAYMENT_COMPLETED`, a settled refund → `REFUND_PROCESSED`). |
| `GET /api/admin/reconcile/status` | Background reconciler: `{ enabled, intervalMs, running, runs, lastRun: { startedAt, finishedAt, summary \| error }, nextRunAt }` |
| `POST /api/admin/remediate` `{ transactionIds?, since?, includePending?=true, republishUnchanged?, replayDlq?, dryRun?, limit? }` | The transaction-level sibling of the DLQ replay. Re-verifies payments we recorded as **FAILED** (and PENDING): where the provider now reports a different outcome — typically *approved* — the record is corrected through the pipeline and announced downstream with `source: "remediation"`. `replayDlq: true` additionally replays the dead-letter queue in the same call, covering messages that never produced a record at all. |

### Outbox — `/api/admin/outbox`

| Route | What it does |
|---|---|
| `GET /api/admin/outbox?status=PENDING\|PUBLISHED\|DEFERRED\|RESTORED\|FAILED&limit=100` | `{ stats: { pending, inFlight, published, deferred, restored, failed, oldestPendingAgeMs, oldestDeferredAgeMs }, publisher: { enabled, pollIntervalMs, publisherId, running, cycles, lastCycle, nextPollAt }, events: [...] }`. Alert on `stats.failed > 0`, on a growing `oldestPendingAgeMs` and on `stats.deferred` (work waiting for the gateway circuit). |
| `POST /api/admin/outbox/publish` | Run a publisher cycle now; answers `{ claimed, published, failed, released, pruned, skipped?, error? }`. |
| `POST /api/admin/outbox/retry` `{ eventIds? }` | Put `FAILED` (parked) rows back in their queue with a fresh attempt budget — all of them, or the given ids. Events go back to `PENDING`, deferred verifications to `DEFERRED`. |

### Circuit breaker — `/api/admin/circuit`

| Route | What it does |
|---|---|
| `GET /api/admin/circuit` | `{ state, metrics, log, recovery }` — see [Gateway circuit breaker](#gateway-circuit-breaker) for the shapes. |
| `POST /api/admin/circuit/trip` | Open the circuit now (planned gateway maintenance). |
| `POST /api/admin/circuit/reset` | Close it now, clear the failure window and the doubled open period. |
| `POST /api/admin/circuit/recover` | Run the recovery now: probe if half-open, drain deferred verifications if closed. |

While the circuit is open, money-movement routes answer **`503`** with a `Retry-After` header (the same `Idempotency-Key` can be retried once it closes), and `POST /api/admin/reconcile` / `remediate` answer `503` without touching anything.

### Recurring charges — `/api/admin/charges`

| Route | What it does |
|---|---|
| `POST /api/admin/charges/batch` `{ items: [{ chargeId, customerId, vaultId, amount, currency, merchantTransactionId?, standingInstructionType? }] }` | Validates every item (`400` names the first bad one, nothing is published), publishes each to `hyperpay.charges` with publisher confirms (`messageId` = `chargeId`), answers `202 { accepted, queue }`. Up to 1000 items. `503` without a broker, or when locking is disabled. |
| `GET /api/admin/charges/status` | `{ enabled, pool: { concurrency, active, waiting, deferred, stopped, totals }, locks: { nodes, quorum, available, held, totals } }` |

`chargeId` is the charge's idempotency key — a re-submitted batch charges nothing twice. `customerId` is what the per-customer lock is keyed on. The charge itself is a `DB` on the vaulted card (`standingInstructionType` defaults to `RECURRING`) through the same `chargeStoredCard` path as the manual route: recorded in `operations`, announced as `PAYMENT_COMPLETED` with `correlationId` = `chargeId`.

Both answer a summary — `{ scanned, updated, unchanged, errors, queued, truncated, items: [{ kind, recordId, transactionId, before, after, action: updated|unchanged|would-update|error, queued }] }` — and share one lock (`409` if a run is in progress). A single bad record never aborts a run. `dryRun` reports `would-update` without writing or queueing. `queued` counts the domain events written to the outbox by the run (they are published by the outbox publisher, not by the run itself).

The **background reconciler** runs `reconcile` every `RECONCILE_INTERVAL_MS` (timeout chain, so runs never overlap; a failed run is recorded and the chain continues; stopped on shutdown). It is the safety net for PENDING transactions whose webhook never arrived.

```bash
# Debit a stored card
curl -X POST http://localhost:3000/api/admin/vault/$VAULT_ID/charge \
  -H "X-Admin-Key: $ADMIN_API_KEY" -H "Idempotency-Key: order-1234" \
  -H "Content-Type: application/json" \
  -d '{"paymentType":"DB","amount":"49.90","currency":"SAR","merchantTransactionId":"order-1234"}'

# Pre-authorise, capture, refund
curl -X POST …/vault/$VAULT_ID/charge       -H … -H "Idempotency-Key: order-1234-pa" -d '{"paymentType":"PA","amount":"100.00","currency":"SAR"}'
curl -X POST …/payments/$PA_ID/capture      -H … -H "Idempotency-Key: order-1234-cp" -d '{"amount":"100.00","currency":"SAR"}'
curl -X POST …/payments/$CP_ID/refund       -H … -H "Idempotency-Key: order-1234-rf" -d '{"amount":"40.00","currency":"SAR"}'
```

Response (`201`, or `200` on replay):
```json
{
  "_id": "…", "idempotencyKey": "order-1234", "operation": "DB", "vaultId": "…",
  "amount": "49.90", "currency": "SAR", "status": "SUCCESS",
  "transactionId": "8ac7a4a2…", "resultCode": "000.100.110", "resultDescription": "Request successfully processed",
  "raw": { "…": "OPPWA resource, card + registrationId stripped" }
}
```

---

## RabbitMQ Integration

### Inbound — `PaymentVerifyEvent`
Publish to exchange `hyperpay` with routing key `payment.verify`:

```json
{
  "correlationId": "your-unique-id",
  "resourcePath": "/v1/checkouts/ABC123/payment"
}
```
or with `checkoutId` instead of `resourcePath`.

### Outbound — domain events

Every outbound message is a domain event that went through the [transactional outbox](#transactional-outbox). Three types exist:

| Event | Routing key | Queues | When |
|---|---|---|---|
| `PAYMENT_COMPLETED` | `payment.result` | `hyperpay.results`, `hyperpay.events` | A payment verification (queue / webhook / reconciliation / remediation) or an admin charge / capture (DB / PA / CP) reached an outcome — `SUCCESS`, `FAILED` or still `PENDING` |
| `TOKEN_REGISTERED` | `token.registered` | `hyperpay.events` | A *new* registration token was sealed in the vault (a replay, or the same card on a later payment, does not raise it again) |
| `REFUND_PROCESSED` | `refund.processed` | `hyperpay.events` | An RF operation was recorded (any outcome), and again whenever reconciliation moves a `PENDING` refund to its final status |

Reverse / void (RV) operations have no event type of their own (add one in `services/domainEvents.ts` if you need it).

Every body carries an **envelope** — `eventId` (unique per event, also the AMQP `messageId`), `eventType`, `occurredAt` — plus `correlationId` and `source` (`verify` | `webhook` | `admin` | `reconciliation` | `remediation`). AMQP properties: `messageId`, `type`, `correlationId`, `timestamp`, `persistent`, and headers `x-aggregate-type`, `x-aggregate-id`, `x-source`, `x-attempt`.

**`PAYMENT_COMPLETED`** — on `hyperpay.results` this is the legacy `PaymentResultEvent` plus additive fields, so existing consumers keep working unchanged:

```json
{
  "eventId": "0f3c…", "eventType": "PAYMENT_COMPLETED", "occurredAt": "2026-10-28T10:00:01.200Z",
  "correlationId": "your-unique-id",
  "source": "verify",
  "origin": "checkout",
  "transactionId": "8ac7a4a2...",
  "status": "SUCCESS",
  "amount": "99.00",
  "currency": "SAR",
  "resultCode": "000.100.110",
  "dbRecordId": "550e8400-e29b-41d4-a716-446655440000",
  "vaultId": "7c9e6679-…"
}
```

Admin-originated payments have `origin: "admin"`, `operation: "DB" | "PA" | "CP"`, `referencedPaymentId` (captures) and `correlationId` = the operator's `Idempotency-Key`.

**`TOKEN_REGISTERED`** — references only. The token never travels over the bus, and because outbox rows sit in clear in the DB file, neither do the masked card details (last4, expiry) or the customer e-mail — the vault's rule is that card data exists on disk only as ciphertext. Fetch the masked view via `GET /api/admin/vault/:vaultId`.

```json
{ "eventId": "…", "eventType": "TOKEN_REGISTERED", "occurredAt": "…", "correlationId": "your-unique-id", "source": "verify",
  "vaultId": "7c9e6679-…", "paymentBrand": "VISA", "transactionId": "8ac7a4a2...", "dbRecordId": "550e8400-…" }
```

**`REFUND_PROCESSED`**

```json
{ "eventId": "…", "eventType": "REFUND_PROCESSED", "occurredAt": "…", "correlationId": "order-1234-rf", "source": "admin",
  "transactionId": "8ac7…rf", "referencedPaymentId": "8ac7…cp", "amount": "40.00", "currency": "SAR",
  "status": "SUCCESS", "resultCode": "000.100.110", "dbRecordId": "…", "idempotencyKey": "order-1234-rf" }
```

**Reply semantics.** A `PAYMENT_COMPLETED` is queued for every inbound `PaymentVerifyEvent` — including retries and redeliveries — so an upstream caller that re-sends under the same `correlationId` always gets its reply; retries carry the same `dbRecordId`. Webhook deliveries, reconciliation and remediation announce **changes** only (a webhook retry that changes nothing queues nothing; `republishUnchanged` overrides this for remediation).

**Delivery is at-least-once.** De-duplicate on `eventId` / `messageId`. Events are published in insertion order per process; a retried event may arrive after later ones.

---

## Transactional outbox

The dual write this replaces looked like: write the record → `channel.publish(...)` → ACK. A crash, a closed channel or an unreachable broker between the first two steps left a record that downstream never heard about (or, on the webhook path, no announcement at all; reconciliation simply logged "not published"). Now:

```
request path                                    background
────────────────────────────────────────────    ─────────────────────────────────────────────
gateway call (outside any lock)                 outboxPublisher (poll, woken by commits/reconnects)
db.transaction(tx => {                            claim(batch)        lease: lockedBy, lockedUntil, attempts+1
  vault.store(…, tx)        payment_methods       publish             own confirm channel, never the consumer's
  tx.insert/update          payments|operations   waitForConfirms     timeout → failed attempt
  outbox.enqueue(tx, …)     outbox_events         markPublished       PUBLISHED, publishedAt
})                          ← ONE atomic write     markFailed          PENDING + backoff, FAILED after maxAttempts
ACK / 201                                         release             channel died mid-batch: leases back, no attempt charged
                                                  prune               PUBLISHED rows older than OUTBOX_RETENTION_MS
```

### `outbox_events` row

```json
{
  "_id": "0f3c…",                       ← the eventId / messageId
  "eventType": "PAYMENT_COMPLETED",
  "aggregateType": "payment",            ← payment | operation | payment_method
  "aggregateId": "550e8400-…",           ← _id of the record the event is about
  "correlationId": "your-unique-id",
  "exchange": "hyperpay", "routingKey": "payment.result",
  "payload": { "…": "the wire body minus the envelope" },
  "occurredAt": "2026-10-28T10:00:01.200Z",
  "status": "PENDING",                   ← PENDING | PUBLISHED | FAILED
  "attempts": 0,
  "nextAttemptAt": "…",                  ← backoff (absent when claimable now)
  "lockedBy": "host:pid:abc", "lockedUntil": "…",   ← lease (absent when unlocked)
  "publishedAt": "…", "lastError": "message nacked"
}
```

### Deferred work

The same table parks *inbound* work the gateway circuit refused (see [Gateway circuit breaker](#gateway-circuit-breaker)): a verify request becomes a `DEFERRED` row (`eventType: "PAYMENT_VERIFICATION_DEFERRED"`, `aggregateId` = the idempotency key, `payload` = the processor input) that the publisher never sees. The recovery replays it — `claim({ status: 'DEFERRED' })` → `processPaymentEvent` → `RESTORED` — with the same lease, backoff and `FAILED` parking as outbound events.

### Transactions on a JSON file

`db.transaction(work)` loads the file once, gives `work` a handle (`tx.insert / update / remove / find*`) over that in-memory snapshot, and — only if `work` resolves — writes the whole snapshot back with one atomic temp-file + rename. Either every write in the transaction lands or none does; a throwing `work` discards the snapshot (rollback is free). `tx.onCommit(fn)` runs after the rename succeeded — the outbox uses it to wake the publisher, so events normally go out within milliseconds and polling is only the safety net. Calling the top-level `db.*` API inside a transaction is detected (`AsyncLocalStorage`) and throws instead of deadlocking on the mutex.

### Row-level locking

`outbox.claim()` is the flat-file equivalent of

```sql
UPDATE outbox_events SET locked_by = $me, locked_until = now() + $lease, attempts = attempts + 1
WHERE id IN (SELECT id FROM outbox_events
             WHERE status = 'PENDING'
               AND (locked_until IS NULL OR locked_until < now())
               AND (next_attempt_at IS NULL OR next_attempt_at <= now())
             ORDER BY id LIMIT $batch FOR UPDATE SKIP LOCKED)
RETURNING *;
```

Select-and-mark happens inside one `db.transaction` (mutex + atomic commit), so two claimants never get the same row; an expired lease (the claimant died mid-publish) is reclaimable; every later transition checks the lease owner, so a slow publisher whose lease expired cannot clobber the one that took over. Attempts are counted at claim time, so an attempt that crashed the publisher still counts and a poison row ends up parked rather than crash-looping.

### Guarantees

| Window | What happens |
|---|---|
| Crash after the DB commit, before anything is published | The event is in the DB. The next publisher (this process after restart, or another instance) delivers it. |
| Broker unreachable | Cycles are no-ops (`skipped: "no-connection"`); rows are not even leased. Webhooks, admin calls and reconciliation keep committing outcomes. The reconnect wakes the publisher and the backlog drains in order. |
| Channel closed mid-batch | The rest of the batch is released untouched (no attempt charged); the cycle ends; the next one reopens a channel on the live connection. |
| Broker nack / confirm timeout | Exponential backoff (`OUTBOX_RETRY_DELAY_MS` → `OUTBOX_MAX_RETRY_DELAY_MS`); parked as `FAILED` after `OUTBOX_MAX_ATTEMPTS`; other rows keep flowing. Operators see `lastError` and retry through the API. |
| Publisher dies after the confirm, before marking the row | The lease expires and the event is published **again with the same `eventId` / `messageId`** — at-least-once; consumers de-duplicate. |
| DB write fails mid-flow (e.g. tokenised payment) | Nothing is persisted — no orphan vault record, no event without its record. The queue message is NACKed to the DLQ and the replay starts clean. |
| Shutdown | `stop()` waits for the cycle in progress, hands back unpublished leases, closes its channel. Unconfirmed events are published by the next instance. |

> **Multiple instances on one `db.json`:** the mutex is in-process, so the lease protocol only protects publishers *within* one process. Replace `jsonDb.ts` with a real database before running more than one instance; the SQL above is the drop-in.

---

## Distributed locks & the charge worker pool

### `RedisLockManager` (`src/locking/redisLockManager.ts`)

Redlock semantics over one or more Redis masters (`ioredis`):

| Step | How |
|---|---|
| **Acquire** | One random 128-bit owner token per acquisition. On every node, atomically (Lua): set the key to the token with a TTL unless another token holds it (re-applying *our* token refreshes the TTL, so a retried acquire whose reply was lost still succeeds). A quorum (`⌊N/2⌋+1`) must have granted it, and the time that took — plus the drift allowance `ttl × LOCK_DRIFT_FACTOR + 2ms` — must leave the lock some validity. Otherwise every partial grant is released and the result is **contention** (`null`). |
| **Watchdog** | Every `TTL/3` the TTL is pushed out on every node — only where the key still carries our token (Lua compare-and-`PEXPIRE`). Losing quorum (key expired, Redis restarted, network gone) marks the lock **lost**: `isValid()` is false, `signal` aborts, `lost` is emitted. |
| **Release** | Lua compare-and-`DEL` on every node: a token that is not ours is never deleted, so nobody can release a lock they do not hold, and a lock that expired and was re-acquired elsewhere is left alone. Idempotent, never throws (the TTL is the fallback), always stops the watchdog. |
| **`withLock(resource, work)`** | acquire → `work(lock)` → `release()` in `finally`. The pool uses exactly this shape. |

**Redis failure modes.** The client is configured to fail fast (`enableOfflineQueue: false`, `maxRetriesPerRequest: 0`, `commandTimeout`) and to reconnect forever with backoff. A node that errors counts as "not granted"; when fewer nodes answer than a quorum needs, `acquire()` throws `LockUnavailableError` — distinguishable from contention — and callers **fail closed**: no lock, no side-effect, retry later. `isAvailable()` (a quorum of nodes `ready`) lets a caller back off without even trying. A lock lost mid-work is reported, not hidden.

**What it cannot do** (inherent to Redlock): it hands no fencing token to the protected resource, so a process paused for longer than the TTL that has already *started* its side-effect cannot be stopped — keep `LOCK_TTL_MS` well above one gateway round-trip, and rely on the charge's idempotency key for the rest; a Redis node restarting without persistence forgets the locks it held (run Redis with `appendonly yes`, or use several nodes).

`GET /api/admin/charges/status` exposes `locks: { nodes: [{ index, status }], quorum, available, held, totals: { acquired, contended, unavailable, extended, lost, released } }`; the manager also emits `acquired`, `released`, `lost`, `unavailable`.

### `ChargeWorkerPool` (`src/workers/chargeWorkerPool.ts`)

```
hyperpay.charges ──► consumer (prefetch = concurrency) ──► pool.process(item)
                                                              │ wait for a worker slot
                                                              ▼
                       lock store reachable? ──no──────────► defer
                       acquire lock:charge:<customerId>
                         held elsewhere / store unreachable ► defer
                         ours ──► chargeStoredCard(chargeId, vaultId, amount, RECURRING)
                                   approved / declined / replayed ──► charged  → ACK
                                   circuit open / gateway outage ───► defer
                                   anything else ───────────────────► failed   → NACK (DLQ)
                                   finally: lock.release()   ← always, only our token
                       defer: sleep random(min, min(max, base × 2^n)), re-enter the queue
                              n = maxDeferrals ──► exhausted → NACK (DLQ)
```

- **Jitter, not a fixed delay.** The workers that collided on a customer would collide again after a fixed backoff; full jitter spreads them out (`utils/backoff.ts`).
- **Deferred items stay unacked** while they sleep: if the instance dies, the broker redelivers them — here or on another instance. `stop()` interrupts sleeping items (left to the broker) and lets running charges finish.
- **Fail closed.** No quorum connected → defer without touching Redis; `LockUnavailableError` → defer; lock lost before the charge started → defer. Nothing is ever charged without a lock.
- **Across instances.** Two instances consuming the same queue and locking on the same Redis never run two charges for one customer at once — `recurringCharges.test.ts` asserts that no two gateway calls for a customer overlap.
- **Dead-lettered charges go home.** The DLQ replay republishes every message under the routing key it originally carried (`x-death`), so a replayed charge returns to `hyperpay.charges`, not to the verify queue.

> **Scaling note.** The lock manager is the cross-instance primitive; the JSON flat-file DB is still single-process (its mutex is in-process). Running several instances of the *whole* service needs a real database behind `jsonDb.ts` — the locks, the outbox claim protocol and the charge pool are all written for that.

---

## Gateway circuit breaker

`src/resilience/circuitBreaker.ts` is a generic, zero-dependency breaker (Node's `events` only); `services/gatewayBreaker.ts` is the instance in front of the OPPWA client, and every call in `hyperpayService.ts` goes through it.

```
CLOSED ──failures in window ≥ threshold──► OPEN
  ▲                                         │ open period elapses (timer, or lazily on the next call)
  │ probe succeeded                         ▼
  └────────────────────────────────── HALF_OPEN
                                            │ probe failed
                                            └──► OPEN, period × 2 (capped)
```

| State | Behaviour |
|---|---|
| `CLOSED` | Every call goes through. Failures are counted in a **sliding time window** (`GATEWAY_BREAKER_WINDOW_MS`, ten buckets, constant memory). Reaching `GATEWAY_BREAKER_FAILURE_THRESHOLD` failures inside the window opens the circuit — "5 per second" is `WINDOW_MS=1000, THRESHOLD=5`; "500 per 30 s" is `WINDOW_MS=30000, THRESHOLD=500`. |
| `OPEN` | Every call is refused immediately with `CircuitOpenError` (`retryAfterMs`); the gateway is not touched. Lasts `GATEWAY_BREAKER_OPEN_MS`. |
| `HALF_OPEN` | `GATEWAY_BREAKER_HALF_OPEN_CALLS` trial calls pass, the rest are refused. A successful trial closes the circuit and resets the window and the open period; a failed one re-opens it for **twice as long**, up to `GATEWAY_BREAKER_MAX_OPEN_MS`. |

**What counts as a failure.** Only outage-class errors: no reply (connection refused, DNS, timeout), `5xx`, `429`. A declined payment is a *result*, not an error, and never touches the breaker; a `401` / `404` / `400` is a completed call — retrying later would not help and the gateway is evidently reachable (`services/gatewayError.ts`). The error type carries the gateway's reply as `gatewayStatus`, never `status`, so the HTTP error handler cannot relay an upstream `502` to our own clients.

**What happens to work while the circuit is open**

| Caller | Behaviour |
|---|---|
| Queue consumer | The verify request is written to `outbox_events` as `DEFERRED` and the message is **ACKed** — an outage says nothing about the message, so it does not belong in the DLQ. If even the deferral cannot be written, the message is NACKed as before. |
| Webhook | Same deferral (HyperPay already has its `200`). |
| Admin charge / capture / refund | `503` + `Retry-After`; retry with the same `Idempotency-Key` later. |
| Reconciliation / remediation | `503`, nothing touched (every item would fail at once, and the deferred queue must not be flooded). |
| Replays from the DB | Unaffected — a terminal record is answered without the gateway, circuit or no circuit. |

**Recovery (`services/gatewayRecovery.ts`).** Listens to the breaker's `stateChange` events:
- `HALF_OPEN` → lease the **oldest deferred row** and replay it through `processPaymentEvent`. That call *is* the breaker's trial call. Success → the circuit closes; an outage → the circuit re-opens for twice as long and the row is handed back untouched (no attempt charged). A row answered from the DB never reached the gateway, so the next one is tried. No deferred rows → the next live call is the probe.
- `CLOSED` (and every `GATEWAY_RECOVERY_INTERVAL_MS` while closed, and at startup) → drain: replay every deferred row oldest first; stop and hand the rest back the moment the circuit opens again. A row failing for a reason of its own (bad identifiers, processing error) backs off and is parked as `FAILED` after `OUTBOX_MAX_ATTEMPTS`, exactly like an outbound event — operators retry it through `POST /api/admin/outbox/retry`.
- Restored verifications produce their `PAYMENT_COMPLETED` events under the **original correlation ids**, so upstream callers get the replies they were owed.

**Observability.** `GET /api/admin/circuit`:

```json
{
  "state": "OPEN",
  "metrics": {
    "name": "oppwa", "state": "OPEN", "stateSince": "…", "failureThreshold": 5,
    "window": { "windowMs": 30000, "calls": 5, "successes": 0, "failures": 5, "rejected": 12 },
    "openDurationMs": 120000, "openUntil": "…",
    "halfOpen": { "inFlight": 0, "maxCalls": 1 },
    "totals": { "calls": 1402, "successes": 1397, "failures": 5, "rejected": 12, "opens": 2, "probes": 1 },
    "lastFailure": { "at": "…", "message": "OPPWA GET /v1/checkouts/…/payment failed: network error – …" }
  },
  "log": [
    { "from": "CLOSED", "to": "OPEN", "at": "…", "reason": "5 failure(s) in 30000ms (threshold 5) — open for 60000ms" },
    { "from": "OPEN", "to": "HALF_OPEN", "at": "…", "reason": "open period of 60000ms elapsed" },
    { "from": "HALF_OPEN", "to": "OPEN", "at": "…", "reason": "probe failed: OPPWA GET … network error — open for 120000ms" }
  ],
  "recovery": { "enabled": true, "intervalMs": 30000, "recoveryId": "…", "running": false, "runs": 3,
                "lastRun": { "trigger": "probe", "circuit": "HALF_OPEN", "claimed": 1, "restored": 0, "failed": 0, "released": 1, "…": "…" } }
}
```

The class itself: `execute(fn)`, `getState()`, `getMetrics()`, `getStateLog()` (last 100 changes), `trip()`, `reset()`, `stop()`, and a `stateChange` event `{ from, to, at, reason }`. Every transition is also logged as `[Circuit:oppwa] OPEN → HALF_OPEN: …`.

---

## AMQP connection lifecycle

`messaging/amqpClient.ts` owns the connection for the life of the process:

```
idle ──start()──► connecting ──ok──► connected ──loss──► reconnecting ─┐
                      ▲                                                │
                      └──────────────────── ok ◄───────────────────────┘
any ──stop()──► stopped
```

On every (re)connect, in order: open the connection → **reconcile topology** on a scratch channel → open the consumer channel with `prefetch(1)` → **attach the consumer** → publish the connection to the registry used by the admin API.

| Event | Behaviour |
|---|---|
| Broker unreachable at boot | HTTP ingress serves immediately; AMQP retries with backoff `RETRY_DELAY × 2ⁿ`, capped at `MAX_RETRY_DELAY`, **forever**. Admin DLQ routes answer `503` meanwhile. |
| Connection drops | Registry cleared (admin → `503`), reconnect loop starts, consumer re-attached on the new connection. Backoff resets after each success. |
| Consumer channel closed by the broker | The whole connection is recycled through the same path — no half-alive state. |
| Message in flight during a drop | Its ACK fails; the broker redelivers on reconnect; `processPaymentEvent` is idempotent so nothing is duplicated. The NACK path tolerates a dead channel, so this can never become an unhandled rejection. |
| Outbox events during a drop | Stay in `outbox_events`; the publisher's cycles are no-ops until the registry has a connection again, then it is woken and drains the backlog on a fresh confirm channel. |
| Topology / consumer attach fails | Counts as a failed attempt — the connection is closed and retried. |
| `SIGTERM` / `SIGINT` | `stop()`: cancel the consumer (in-flight message finishes), close channel + connection, **never reconnect**. Idempotent; also aborts an in-progress backoff sleep. |

Every channel this service opens has an `'error'` listener attached before use (`openChannel` / `openConfirmChannel`). amqplib emits `'error'` on a channel when the **server** closes it (406, 404, …); without a listener that is an uncaught exception.

---

## Dead-letter queue & replay

### Topology

Declared in `messaging/topology.ts` and reconciled on every connect (`ensureTopology`):

```
exchange  hyperpay       direct, durable
  queue   hyperpay.payments   durable   ← binding: payment.verify
            x-dead-letter-exchange    = hyperpay.dlx
            x-dead-letter-routing-key = payment.dead
  queue   hyperpay.charges    durable   ← binding: charge.execute
            x-dead-letter-exchange    = hyperpay.dlx
            x-dead-letter-routing-key = payment.dead
  queue   hyperpay.results    durable   ← binding: payment.result
  queue   hyperpay.events     durable   ← bindings: payment.result, token.registered, refund.processed

exchange  hyperpay.dlx   direct, durable
  queue   hyperpay.dlq        durable   ← binding: payment.dead
```

The DLQ binding uses exactly the exchange and routing key the main queue dead-letters with — the test suite asserts this pairing. The dead-letter side is declared *before* the main queue so the `x-dead-letter-exchange` reference always resolves.

### Migrating existing queues

RabbitMQ refuses to re-declare a queue whose arguments changed (`406 PRECONDITION_FAILED`) — which is exactly what happens the first time this version meets a `hyperpay.payments` created before dead-lettering existed. `ensureTopology` handles it, governed by `RABBITMQ_TOPOLOGY_MIGRATION`:

**`recreate`** (default) — for the offending queue only:
1. park every message in a durable `<queue>.migrating` queue (moved with publisher confirms — ACKed on the source only after the broker confirmed the copy);
2. `deleteQueue(ifEmpty)` — if a producer slipped a message in meanwhile this fails, the attempt is retried by the reconnect loop, and the parked messages are picked up again;
3. re-declare the queue with the new arguments and bindings;
4. restore everything in the parking queue (including leftovers from an earlier interrupted run) and delete it.

Scratch channels only — the consumer's channel is never exposed to the 406.

**`fail`** — throw a `TopologyMismatchError` naming the queue and the manual fixes (`rabbitmqadmin delete queue name=…` while empty, or a policy). Use this if another team owns the broker.

### Lifecycle

1. `handleMessage` fails (OPPWA down, bad payload, DB error) → `channel.nack(msg, false, false)`.
2. The **broker** moves the message to `hyperpay.dlq`, attaching its standard `x-death` history (`count`, `reason`, `queue`, `time`) and `x-first-death-*` headers.
3. An operator inspects with `GET /api/admin/dlq` and, once the cause is fixed, calls `POST /api/admin/replay-dlq`.
4. Each message is republished with **all original properties and headers intact** plus two markers: `x-replay-count` (incremented) and `x-replayed-at` — under the routing key it originally carried (from `x-death`), so charges go back to `hyperpay.charges` and verify events to `hyperpay.payments`.
5. The consumer processes it again. Because `processPaymentEvent` is idempotent, a replay of a payment that was meanwhile processed through another path (e.g. a webhook retry) just replays the existing record — no duplicate, no OPPWA call. If it fails again it is re-parked with `deathCount` and `replayCount` both incremented.

### Replay guarantees

| Guarantee | How |
|---|---|
| **Bounded** | At most *queue depth at start* messages are pulled, so a message that fails again immediately and lands back on the DLQ mid-pass is not replayed twice in the same pass. |
| **No message loss** | Republish happens on a **confirm channel**; the DLQ copy is ACKed only after the broker confirms. A crash in between duplicates the message (safe — processing is idempotent) rather than losing it. |
| **Isolated** | DLQ work runs on a dedicated channel opened per call and closed after. A protocol error (e.g. queue missing) can never kill the consumer's channel. |
| **Serialised** | One DLQ operation at a time; concurrent attempts get `409`. |
| **Auditable** | `x-death` history travels with the message; every replay is logged with correlation ID, death count and replay number. |


---

## Tokenisation & the vault

When a checkout is created with `createRegistration=true`, the OPPWA payment resource carries a **`registrationId`** — a token that can charge the same card again — alongside the card's `paymentBrand`, `last4Digits`, `expiryMonth`/`expiryYear` and `holder`. None of that is allowed to sit in `payments` in clear.

### Flow

```
OPPWA payment resource
   │
   ├─ paymentMethodFrom()  ──► { registrationId, brand, last4, expiry, holder }
   │                               │
   │                               ▼  AES-256-GCM (fresh IV, AAD = record identity)
   │                        payment_methods: { tokenFingerprint, customerKey, envelope{keyId,iv,authTag,ciphertext}, transactionId }
   │                               │
   └─ redactPaymentResource() ─────┴──► payments: { …, vaultId, raw: <resource WITHOUT card / registrationId> }
```

1. **Vault first.** The token is sealed *before* the payment record is written, so the record can reference `vaultId` and a vault failure leaves no half-persisted payment (the message is NACKed and replayed later).
2. **Redact everywhere.** `PaymentRecord.raw` is typed `SafeOppwaPaymentResource = Omit<…, 'card' | 'registrationId'>` — the compiler will not let card data into the persisted record. The `result` handed back to callers (and the `PaymentResultEvent` on the bus) is redacted the same way; upstream gets a `vaultId`, never the token.
3. **Idempotent per token.** The vault keys records by an HMAC fingerprint of the token. Webhook retries, redeliveries, or the same card used on a second payment all resolve to the *same* vault record (the newer transaction id is recorded as `lastTransactionId`).
4. **Plaintext columns are lookup-only.** `tokenFingerprint` and `customerKey` are HMAC-SHA256 under a key derived (HKDF) from the master key — not plain hashes, so they cannot be matched offline by someone who only has the DB file.

### Where the token is allowed to exist

The registration token has exactly one job: to sit in the URL of a gateway call. Everything else is designed so it cannot be observed:

| Surface | Guarantee | Enforced by |
|---|---|---|
| `payments` / `operations` records | `raw` is `SafeOppwaPaymentResource` — `card` and `registrationId` cannot be present | type system + `redactPaymentResource()` |
| HTTP responses | vault routes return masked data; operation routes return records; **no reveal route exists** | `vaultAdmin` / `chargeStoredCard` tests assert every surface |
| Log lines | vault unseal and charge logs name the `vaultId`, amount and brand only | every integration test captures all console levels and fails on the token |
| Error messages | gateway errors mask `/v1/registrations/<token>/…` *and* any echo of the token in the reply body before the error is thrown — so the global error handler and the `500` body are clean | `security/redact.ts`, `hyperpayService` tests |
| Result events on the bus | carry `vaultId` only | `tokenization` tests |
| `outbox_events` rows | `TOKEN_REGISTERED` carries `vaultId`, brand and transaction ids only — no token, no masked card data, no e-mail (the row is in clear on disk) | `tokenization` / `outbox` tests scan every event body and the DB file |

`vault.unseal()` is the single decryption path and is not reachable over HTTP; its only caller is `paymentOperations.ts`, which passes the value straight to `chargeRegistration()` and drops it.

### Cryptography

| Property | How |
|---|---|
| Confidentiality + integrity | AES-256-GCM, 96-bit random IV per record, 128-bit tag. Any change to ciphertext, tag, IV or AAD makes decryption fail. |
| Records can't be swapped | AAD = `hyperpay-vault:v1:<tokenFingerprint>` — an envelope copied onto another record will not open. |
| Rotation without downtime | Each envelope records its `keyId`. Set the new `VAULT_MASTER_KEY` + `_ID`, move the old key to `VAULT_RETIRED_KEYS`, deploy, call `POST /api/admin/vault/rotate-keys`, then drop the retired key. |
| Fail closed | Production refuses to start without `VAULT_MASTER_KEY`. Outside production a missing key *discards* tokens with a warning — it never falls back to clear text. |

Generate a key: `node -e "console.log(require('crypto').randomBytes(32).toString('base64'))"`.

> **What stays in clear, and why:** `paymentBrand` and `customerEmail` remain on the payment record — they are transaction metadata OPPWA returns at the top level (not card data under PCI DSS) and the status dashboard needs them. If your data-classification policy treats them as sensitive, they are two lines in `toPaymentRecord()` to remove; the vault already holds the brand.

> **Replacing the JSON DB:** keep `payment_methods` in a separate table with its own access policy; add unique indexes on `tokenFingerprint` and an index on `customerKey`. Consider moving the master key to a KMS and using it to wrap a per-record data key — the envelope format already leaves room for that via `keyId`.

---

## Idempotency

The same payment can legitimately reach the service many times:

| Scenario | Cause |
|---|---|
| HyperPay re-sends the webhook | Our `200 ACK` was lost on the network |
| RabbitMQ redelivers the message | Connection dropped after the DB write but before the ACK was flushed |
| Duplicate `payment.verify` events | Upstream retried after a timeout on its side |
| Simultaneous webhook + queue event | Both ingress paths fired for one checkout |

All of these are safe. `processPaymentEvent` — the single code path both transports use — guarantees:

1. **One record per payment.** Identity is established in two layers:
   - `idempotencyKey` — the canonical OPPWA resource path (`/v1/checkouts/{id}/payment`), derived *before* any network call. `resourcePath` and `checkoutId` for the same checkout produce the same key.
   - `transactionId` — OPPWA's own ID, known *after* the fetch. Catches the same payment arriving under a different path (e.g. `/v1/payments/{id}`).
2. **Terminal outcomes are replayed, not re-fetched.** If a `SUCCESS` or `FAILED` record exists, it is returned straight from the DB — no OPPWA call. A retry therefore succeeds even while OPPWA is down.
3. **`PENDING` is not terminal.** A repeat call re-queries OPPWA and updates the existing record *in place* (`_updatedAt` is stamped), so the status can progress to `SUCCESS`/`FAILED` without ever creating a second record.
4. **Concurrent calls coalesce.** Simultaneous requests for the same key share one in-flight operation: a single OPPWA call, a single DB write.
5. **Failures leave no partial state.** If OPPWA is unreachable nothing is written, the in-flight slot is released, and the next retry starts clean.

Every call reports what it did via `outcome`, which is also included in the log line:

| `outcome` | Meaning |
|---|---|
| `created` | First time this payment was seen — record inserted |
| `updated` | A non-terminal record existed — OPPWA re-queried, record refreshed in place |
| `replayed` | A terminal record existed (or an identical call was in flight) — result returned with no new side-effects |

> **Production note:** the in-flight coalescing is per process. `jsonDb` serialises all operations under a mutex and writes atomically (temp file + rename), so the check-then-write sequence is safe within one instance. When you swap in a real database for a multi-instance deployment, add a unique index on `payments.idempotencyKey` (and `payments.transactionId`) so the same guarantee holds across instances.

---

## Database

Records are persisted in `db.json` under the `payments` collection — exactly one per payment. Each record:

```json
{
  "idempotencyKey": "/v1/checkouts/ABC123/payment",
  "transactionId": "8ac7a4a2...",
  "status": "SUCCESS",
  "amount": "99.00",
  "currency": "SAR",
  "paymentBrand": "VISA",
  "resultCode": "000.100.110",
  "resultDescription": "Request successfully processed",
  "customerEmail": "user_at_example.com",
  "rawTimestamp": "2026-10-28T10:00:00Z",
  "vaultId": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
  "raw": { "...": "OPPWA payment resource as last fetched, card + registrationId stripped" },
  "_id": "550e8400-e29b-41d4-a716-446655440000",
  "_createdAt": "2026-10-28T10:00:01.123Z",
  "_updatedAt": "2026-10-28T10:05:42.001Z"
}
```

- `idempotencyKey` / `transactionId` — the two identity layers used for de-duplication.
- `vaultId` — present when the payment was tokenised; points at the encrypted method in `payment_methods` (see [Tokenisation & the vault](#tokenisation--the-vault)).
- `raw` — the gateway response **minus `card` and `registrationId`**, so a replayed request can return a complete result without contacting OPPWA and without ever holding card data.
- `_updatedAt` — present only on records that were re-verified (e.g. `PENDING` → `SUCCESS`).

The `jsonDb` service runs every operation under an async mutex and writes atomically (temp file + rename), so concurrent webhooks can't lose writes and a crash mid-write can't corrupt the file. `db.transaction(work)` extends this to several writes at once — see [Transactions on a JSON file](#transactions-on-a-json-file).

Other collections: `operations` (admin money movement), `payment_methods` (the vault), `outbox_events` (the [transactional outbox](#transactional-outbox)).

> **Production note:** Replace `jsonDb.ts` with a real database (PostgreSQL, MongoDB, etc.) by keeping the same `db.insert` / `db.update` / `db.findOne` / `db.findAll` / `db.transaction` interface, and add unique indexes on `idempotencyKey` and `transactionId`. The outbox needs an index on `(status, locked_until, next_attempt_at)` and a monotonic id for ordering.

---

## Testing & Reviewer Verification

### Running the Test Suite
All tests use dependency injection, filesystem isolation (temporary files for `jsonDb`), and network mocks (`axios-mock-adapter` or `nock` for OPPWA, an in-memory RabbitMQ, `ioredis-mock` running the real Lua scripts). No running RabbitMQ, Redis or live HyperPay credentials are required to execute the tests. The suite is free of open handles (`npx jest --detectOpenHandles` reports none).

```bash
# Run all 886 tests across unit, feature, integration and chaos suites
npm test

# Run specific suites
npm run test:unit
npm run test:feature
npm run test:integration

# Run coverage analysis
npm run test:coverage

# Static checks (both are clean: zero errors, zero warnings, no `any`)
npm run typecheck   # tsc over src/ and tests/
npm run lint        # eslint with type-aware rules over src/ and tests/
```

### Chaos & resilience suite — `tests/integration/chaos.test.ts`

The closing suite runs the service under catastrophic conditions and asserts fault tolerance and **data integrity**, not happy paths. The wire is as real as a test allows: OPPWA is intercepted at the HTTP layer with **nock** under `nock.disableNetConnect()` (a request no interceptor answers is refused *and* recorded — a leaked call cannot go unnoticed), HTTP ingress goes through **supertest**, RabbitMQ is the in-memory broker (connections dropped, channels killed, dead-lettering), Redis is `ioredis-mock` with the real Lua, and the database is the real JSON file.

| Scenario | What is asserted |
|---|---|
| **OPPWA outage** | Three refused connections open the circuit; from then on the queue path defers, the webhook ACKs and defers, admin charges answer `503` + `Retry-After`, reconciliation answers `503`, and twenty direct processor calls are refused in well under a second — with **the request counter frozen at three**: not one socket opens after the breaker trips. A terminal record is still served from the DB ("local execution"). |
| **Recovery** | With the gateway back, the log reads `OPEN → HALF_OPEN → CLOSED`; the **oldest deferred request is the single probe**, every deferred request is replayed **exactly once** (the gateway sees `w1 … w5`, in order, once each), queue callers get their replies under their correlation ids, the webhook outcome is announced. |
| **DB commit fails after the gateway answered** | Nothing is persisted — no payment, no vault record, no event. The retry yields exactly one record, one vault entry and one `TOKEN_REGISTERED` + `PAYMENT_COMPLETED`. |
| **DB fails after the broker confirmed** | The `PUBLISHED` write fails → a failed attempt with the error recorded, not a stuck lease; the next cycle delivers the **same `eventId`** (`x-attempt: 2`) and nothing more. Never lost, never a new id. |
| **RabbitMQ dies mid-batch** | Every recorded event is delivered after the reconnect; duplicates exist **only by `eventId`**, at most the one confirmed-then-lost; every row ends `PUBLISHED`. |
| **RabbitMQ dies with a charge in flight** | The operation is committed once; the lost ACK causes a redelivery that is **replayed from the DB** — the gateway is called once. |
| **Lock contention on one registration id** | Fifteen workers on three instances charge the same card simultaneously: **no two gateway calls overlap**, fifteen charges, fifteen distinct operations, the loser of every collision is deferred with `lock_held`, the lock key is free at the end. The same `chargeId` raced by all three instances → **one** gateway call (one charge, two idempotent replays). A worker that died holding the lock expires after the TTL and the next worker proceeds. |
| **Production readiness** | Every timer the service arms — AMQP reconnect backoff, outbox poll, recovery sweep, reconciler, breaker half-open, lock watchdog, pool deferral sleep — is **`unref()`'d** (`hasRef() === false`), and after a full stop in `server.ts` order no timer is live and no breaker listener remains. |

Every scenario ends with the integrity invariants: each recorded operation announced exactly once, no outbox row leased or parked, no lock held, nothing unacked at the broker, no unmatched outbound HTTP request.

### Reviewer Check: Confirming Tests Can Fail
To verify that the test suite genuinely catches regressions and enforces correctness:
1. Open `src/services/hyperpayService.ts`.
2. Locate `SUCCESS_PATTERN` (around line 25):
   ```ts
   export const SUCCESS_PATTERN = /^(000\.000\.|000\.100\.1|000\.400\.)/;
   ```
3. Intentionally break the pattern (e.g. change `000\.000\.` to `999\.999\.`).
4. Run `npm test`.
5. Observe that unit tests in `hyperpayService.test.ts` and integration tests in `fullFlow.test.ts` fail.
6. Revert the line back to normal.

To verify the ledger rules are enforced by tests:
1. Open `src/services/transactionLedger.ts`, `buildView`, the `reverse:` availability line.
2. Change `isPA && rootActive && noCaptureActivity` to `isPA && rootActive`.
3. Run `npm test` — 4 tests fail across `transactionLedger.test.ts` and `transactionLifecycle.test.ts` ("reverse refused once captured").
4. Revert.

To verify the token-exposure invariant is enforced by tests:
1. Open `src/services/paymentOperations.ts`, `chargeStoredCard`, the `console.log` before `chargeRegistration(...)`.
2. Append `token=${method.registrationId}` to the log line.
3. Run `npm test` — `paymentOperations.test.ts` ("never writes the registration token to any log line") and **every** test in `chargeStoredCard.test.ts` fail: its `afterEach` scans all captured console output for the token.
4. Revert.

To verify the vault's redaction is enforced by tests (not just by the type system):
1. Open `src/services/paymentProcessor.ts`, `toSafeResult`.
2. Change `raw: redactPaymentResource(result.raw)` to `raw: { ...redactPaymentResource(result.raw), ...result.raw }` (re-adds the card block; still compiles).
3. Run `npm test` — 6 tests fail across `paymentProcessor.test.ts`, `fullFlow.test.ts` and `tokenization.test.ts` ("no card data on disk", "status endpoint never exposes vault contents", …).
4. Revert.

To verify the reconnect logic is enforced by tests:
1. Open `src/messaging/amqpClient.ts`, `handleConnectionClosed`.
2. Delete the line `if (this.state === 'stopped') return;`.
3. Run `npm test` — `amqpClient.test.ts` ("does not reconnect after stop") and `lifecycle.test.ts` ("never reconnects") fail.
4. Revert.

To verify the dead-letter wiring is enforced by tests:
1. Open `src/messaging/connection.ts`, `assertTopology`.
2. Remove the `'x-dead-letter-routing-key'` argument from the main queue (or change the DLQ `bindQueue` key).
3. Run `npm test` — `connection.test.ts` fails on the DLX/DLQ pairing assertions.
4. Revert.

To verify the idempotency guarantee is enforced by tests and not by accident:
1. Open `src/services/paymentProcessor.ts`.
2. In `verifyAndPersist`, invert the replay guard: `if (byKey && isTerminal(byKey))` → `if (byKey && !isTerminal(byKey))`.
3. Run `npm test` — 14 tests fail: the replay / re-verify tests in `paymentProcessor.test.ts` and the `Idempotency —` suites in `fullFlow.test.ts` (second OPPWA call, stale PENDING, duplicate records).
4. Revert.

To verify the outbox is written in the *same* transaction as the record (no dual write):
1. Open `src/services/paymentProcessor.ts`, the "Domain events, same commit as the record" step.
2. Move the `PAYMENT_COMPLETED` write out of the transaction: replace `events.push(await recordPaymentCompleted(tx, record, origin))` with `tx.onCommit(() => void db.transaction((t2) => recordPaymentCompleted(t2, record, origin)))` (a second, separate write — still compiles, still "works").
3. Run `npm test` — 11 tests fail: `fullFlow.test.ts` ("records the result event in the same DB write as the payment record"), the transaction-boundary assertions in `paymentProcessor.test.ts`, and the reconciliation suites that count `queued` events.
4. Revert.

To verify the breaker only trips on outages (and never on declines / 4xx):
1. Open `src/services/gatewayError.ts`, `isGatewayOutage`.
2. Change `gatewayStatus >= 500 || gatewayStatus === 429` to `gatewayStatus >= 400`.
3. Run `npm test` — 5 tests fail in `gatewayError.test.ts` (classification, and the client raising a 401 / 404 as "not an outage") — the `circuitBreaker.test.ts` decline run still passes because declines are results, not errors.
4. Revert.

To verify a failed half-open probe hands the deferred row back rather than charging it:
1. Open `src/services/gatewayRecovery.ts`, `replay()`.
2. Change `if (isCircuitOpen(err) || isGatewayOutage(err))` to `if (isCircuitOpen(err) && !isGatewayOutage(err))`.
3. Run `npm test` — 3 tests fail: the probe-outage and drain-interrupted cases in `gatewayRecovery.test.ts`, and "a failed probe re-opens the circuit for twice as long and leaves the row untouched" in `circuitBreaker.test.ts` (the row now carries an attempt and a `lastError`).
4. Revert.

To verify only the owner's token can release a lock (the Lua compare-and-delete):
1. Open `src/locking/redisLockManager.ts`, `RELEASE_SCRIPT`.
2. Replace the body with `return redis.call("del", KEYS[1])` (delete regardless of owner).
3. Run `npm test` — 9 tests fail: the ownership tests in `redisLockManager.test.ts` (another owner's key now gets deleted, so exclusion breaks) and, because of that, the one-per-customer tests in `chargeWorkerPool.test.ts` / `recurringCharges.test.ts`.
4. Revert.

To verify the pool never charges without a lock:
1. Open `src/workers/chargeWorkerPool.ts`, `attempt()`.
2. Change `if (!lock) return { kind: 'defer', reason: 'lock_held' };` to proceed with a fake lock, e.g. `if (!lock) lock = await this.locks.acquire(\`${this.lockKey(item)}:${Math.random()}\`) as Lock;`.
3. Run `npm test` — 8 tests fail across `chargeWorkerPool.test.ts` and `recurringCharges.test.ts`: overlapping gateway calls for one customer, and items charged while Redis was down.
4. Revert.

To verify the "no timer can keep the process alive" contract:
1. Open `src/workers/chargeWorkerPool.ts`, `sleep()`, and delete `entry.timer.unref();`.
2. Run `npx jest tests/integration/chaos.test.ts -t unref --forceExit` — "every timer the service arms is unref'd" fails (a live timer with `hasRef() === true`). Without `--forceExit` the run itself hangs on the ref'd 5-second deferral timer — which is the point.
3. Revert.

To verify a row is only marked published after the broker's confirm:
1. Open `src/messaging/outboxPublisher.ts`, the per-row `try` block.
2. Swap the two lines so `outbox.markPublished(...)` runs before `publish(...)`.
3. Run `npm test` — 6 tests fail across `outboxPublisher.test.ts`, `outbox.test.ts` (integration) and `lifecycle.test.ts`: nacked / timed-out / crashed publishes are now recorded as delivered and never retried.
4. Revert.

---

## Project Structure

```
src/
  app.ts                        Express application & middleware definition
  config/env.ts                 Validated environment configuration
  types/
    hyperpay.ts                 OPPWA API TypeScript interfaces
    rabbitmq.ts                 RabbitMQ event envelope types (inbound verify event, legacy result body)
    events.ts                   Domain events: PAYMENT_COMPLETED / TOKEN_REGISTERED / REFUND_PROCESSED wire bodies
  services/
    hyperpayService.ts          OPPWA HTTP client: status fetch, registration charges, back-office ops (redacted errors), through the breaker
    gatewayError.ts             GatewayError (+ gatewayStatus) and the outage classification the breaker uses
    gatewayBreaker.ts           The process-wide breaker in front of the gateway, wired to config
    gatewayRecovery.ts          Defers refused verifications to the outbox; probes with them at half-open, drains on close
    jsonDb.ts                   Generic JSON flat-file database (mutex, atomic writes, transactions with commit hooks)
    outbox.ts                   Transactional outbox store: enqueue (in the caller's tx), lease-based claim, mark/release/retry/prune/stats
    domainEvents.ts             Builds PAYMENT_COMPLETED / TOKEN_REGISTERED / REFUND_PROCESSED from records, into the outbox
    paymentProcessor.ts         Core payment verification logic + idempotency + vault hand-off/redaction; one tx for vault + record + events
    vault.ts                    Encrypted payment-method storage: store / describe / unseal (in-process only) / rotate
    paymentOperations.ts        DB / PA / capture / refund / reverse / void with idempotency keys, ledger validation
    transactionLedger.ts        Lifecycle read model + availability + rule validation (422 reasons)
    reconciliation.ts           Engine: re-verify PENDING/FAILED records, queue corrections in the outbox; reconcile() / remediate()
    reconciler.ts               Background scheduler (non-overlapping timeout chain, state for /reconcile/status)
    backgroundReconciler.ts     Process-wide reconciler wired to config
    collections.ts              JSON DB collection names
  resilience/
    circuitBreaker.ts           Zero-dependency circuit breaker: sliding window, CLOSED/OPEN/HALF_OPEN, doubling, metrics, log, events
  locking/
    redisLockManager.ts         Redlock-style distributed locks: Lua acquire/extend/release, quorum, drift, watchdog, loss detection
    redisClient.ts              Fail-fast ioredis nodes from config + the process-wide lock manager
  workers/
    chargeWorkerPool.ts         Concurrent recurring-charge workers: per-customer lock, jittered deferral, exhaustion, stop
    chargeWorker.ts             Process-wide pool wired to config (enabled only with Redis)
  security/
    webhookSignature.ts         HMAC-SHA256 sign / parse / verify (pure)
    vaultCrypto.ts              AES-256-GCM envelopes, keyring with rotation, keyed fingerprints (pure)
    redact.ts                   Masks registration tokens in free text (error messages, gateway replies)
  messaging/
    amqpClient.ts               Connection lifecycle: backoff reconnect, channel recycling, clean stop
    topology.ts                 Declarative exchanges/queues/bindings + 406 migration (recreate / fail)
    amqpUtils.ts                Channel opening with error listeners, property carry-over, message moving
    connection.ts               Production-wired client singleton + active-connection registry (+ change listeners)
    consumer.ts                 Queue consumer + handleMessage (NACK tolerant of a dead channel); publishes nothing
    publisher.ts                Puts one outbox row on the wire (envelope, AMQP properties) and waits for the confirm
    outboxPublisher.ts          The relay: poll/wake → claim → publish → confirm → mark; backoff, parking, release, prune
    chargeConsumer.ts           hyperpay.charges consumer: item validation, outcome → ACK / NACK / leave unacked
    deadLetter.ts               DLQ inspect (dump) + replay
  controllers/
    paymentController.ts        HTTP webhook ingress
    healthController.ts         Health + status endpoints
    adminController.ts          DLQ dump / replay endpoints
    vaultController.ts          Vault lookup / describe / rotate endpoints (no reveal)
    operationsController.ts     Charge / capture / refund / reverse / void / operation lookup, input validation
    transactionsController.ts   Ledger views
    reconciliationController.ts reconcile / reconcile status / remediate (+ DLQ replay composition)
    outboxController.ts         Outbox stats / events, publish now, retry parked events
    circuitController.ts        Circuit state / metrics / log / recovery, trip, reset, recover
    chargesController.ts        Charge batch submission (validated, confirmed publish) + pool / lock status
  middleware/
    errorHandler.ts             Global Express error handler
    requestLogger.ts            Request/response logging
    adminAuth.ts                Shared-secret guard for /api/admin
    webhookSignature.ts         Raw-body capture + HMAC guard for the webhook route
  routes/
    paymentRoutes.ts            /api/webhook/hyperpay
    healthRoutes.ts             /health, /status/payments
    adminRoutes.ts              /api/admin/dlq, /api/admin/replay-dlq, /api/admin/vault/*, /api/admin/outbox/*, /api/admin/circuit/*
  utils/
    httpError.ts                Error type carrying an HTTP status
    money.ts                    Exact decimal-string arithmetic in minor units
    backoff.ts                  Full-jitter retry delays
  server.ts                     Entry point — HTTP + AMQP bootstrap

tests/
  helpers/
    jestSetup.ts                Env vars + per-file temp DB_PATH, injected before every test file
    jestAfterEnv.ts             Resets the process-wide gateway breaker before every test
    fakeRedis.ts                ioredis-mock nodes (real Lua) that can be taken down, slowed or wiped
    mockFactory.ts              Shared typed mock builders
    fakeBroker.ts               In-memory RabbitMQ: connections/channels with close+error events, queue
                                arguments & 406, round-robin consumers, ack/nack, DLX routing, x-death, confirms, drain
    webhook.ts                  Signed webhook request helpers
    waitFor.ts                  Polling helpers for push-driven flows
    testDb.ts                   Temp-file DB read/teardown helpers
    outbox.ts                   Read recorded events back; run a publisher cycle over a (fake) connection
  unit/config/
    env.test.ts                 Required vars, defaults, validation, production hardening
  unit/resilience/
    circuitBreaker.test.ts      Sliding window, every transition, doubling/cap, probes, isFailure, metrics, log, events, timer
  unit/locking/
    redisLockManager.test.ts    Exclusion across instances, owner-only release/extend, watchdog, loss detection, 3-node quorum,
                                unavailability vs contention, validity/drift, withLock finally, availability, metrics
  unit/workers/
    chargeWorkerPool.test.ts    Concurrency bound, one-per-customer across two pools, every deferral reason, jitter formula,
                                exhaustion, permanent failure + release in finally, lock lost, stop()
  unit/security/
    webhookSignature.test.ts    HMAC scheme: signing, parsing, tolerance window, rotation, raw-body binding
    vaultCrypto.test.ts         Key parsing, envelope round-trip, tamper/AAD detection, keyring rotation, fingerprints
    redact.test.ts              Registration-path and known-value masking
  unit/services/
    hyperpayService.test.ts     OPPWA HTTP client, status resolution, resource-path resolver
    jsonDb.test.ts              JSON DB CRUD, update, mutex ordering, atomic writes, transactions (atomicity, rollback, hooks, re-entrancy guard)
    outbox.test.ts              Outbox store: enqueue-in-tx, lease semantics (FIFO, bounded, expiry, ownership), backoff/parking, retry, prune, stats, deferred rows
    gatewayError.test.ts        Outage classification, and that the client raises errors that way
    gatewayRecovery.test.ts     Deferral, half-open probe from the outbox (success / outage / DB-answered / busy), drain, parking, lifecycle
    paymentProcessor.test.ts    Core processing logic + idempotency (replay/update/coalesce) + vault hand-off/redaction
    vault.test.ts               Vault over the real file DB: nothing sensitive on disk, idempotency, masked/unseal, rotation, disabled mode
    paymentOperations.test.ts   Charge/back-office flow, ledger hand-off, idempotency replay/409/coalescing, token-free records and logs
    transactionLedger.test.ts   Lifecycle derivation, views, availability, every capture/refund/reverse/void rule
    reconciliation.test.ts      Selection, per-kind verification, dry run, which events get queued, error isolation, locking
    reconciler.test.ts          Scheduler timing, non-overlap, error capture, stop
  unit/messaging/
    amqpClient.test.ts          State machine: backoff, reconnect, channel recycling, stop semantics
    topology.test.ts            Declarations (durable DLX/DLQ, matching bindings) + 406 migration paths
    amqpUtils.test.ts           Error listeners, error classification, property carry-over, moveMessages
    deadLetter.test.ts          DLQ inspect/replay protocol contract (channels, confirms, bounds, locking)
    publisher.test.ts           Wire format (envelope, AMQP properties) and the confirm handshake (nack / close / timeout)
    outboxPublisher.test.ts     Relay loop: skip without connection, drain, backoff → park, channel loss → release, wake coalescing, stop
  unit/utils/
    money.test.ts               Minor-unit conversion and arithmetic
    backoff.test.ts             Full-jitter delays
  unit/middleware/
    adminAuth.test.ts           Admin key guard (header forms, 401, disabled → 503)
    webhookSignature.test.ts    Webhook guard through Express (raw-body semantics, 401 reasons, dev mode)
    errorHandler.test.ts        Status mapping and production message masking
  feature/
    consumer.test.ts            Message consumption, ACK/NACK, redelivery, dead-channel tolerance, startConsumer
    webhook.test.ts             Signature enforcement, ACK semantics, retry pass-through, health, 404s
    admin.test.ts               Admin routes: auth, 503 without broker, validation, 409/500 mapping
    vaultAdmin.test.ts          Vault admin routes: auth, masked data, no reveal route, 404/503 mapping, rotate-keys routing
    operationsAdmin.test.ts     Money-movement routes: auth, Idempotency-Key, validation matrix, replay header, 422 mapping
    lifecycleAdmin.test.ts      Ledger / reconcile / remediate routes: auth, option parsing, status, DLQ composition
    outboxAdmin.test.ts         Outbox routes: auth, status/limit validation, publish now, retry ids
    circuitAdmin.test.ts        Circuit routes: auth, snapshot, trip/reset/recover; 503 + Retry-After for refused gateway calls
    chargeConsumer.test.ts      Charge item validation; outcome → ACK / NACK / unacked
    chargesAdmin.test.ts        Batch routes: auth, validation (nothing published), message contract, 503s, status
  integration/
    fullFlow.test.ts            Queue + signed webhook → OPPWA → DB, cross-transport idempotency
    dlqReplay.test.ts           NACK → DLQ → dump → replay → reprocess, via the fake broker
    lifecycle.test.ts           Boot on a legacy broker (migration), outage → reconnect → consumer re-attach,
                                lost-ACK redelivery, outbox backlog held through an outage and drained on reconnect,
                                DLQ across reconnects, graceful stop
    tokenization.test.ts        Tokenised payment (queue + webhook) → vault → redacted disk → admin lookup/rotate
    chargeStoredCard.test.ts    Vault → DB/PA → CP/RF/RV → idempotency; token absent from every response, log line, disk and error
    transactionLifecycle.test.ts PA → partial captures → refund/void of captures, reverse, DB refunds/void, every 422 at the API
    reconciliation.test.ts      Pending payment + capture settled by reconcile; FAILED→approved remediated through the pipeline;
                                dry run; remediate + DLQ replay; background reconciler on a timer (events read via the publisher)
    recurringCharges.test.ts    Batch → queue → two instances on one Redis → OPPWA → outbox: no overlapping charge per customer,
                                jittered deferral, Redis outage mid-batch (fail closed, nothing lost), exhaustion → DLQ → replay
                                routes home, duplicate chargeId charged once, instance stop leaves the item to the broker
    chaos.test.ts               Chaos & resilience (nock + supertest + fake broker + ioredis-mock): OPPWA outage with zero
                                leaked calls and local execution, recovery probes once / replays once; DB and RabbitMQ drops
                                mid-transaction (nothing partial, same eventId re-delivered, replay from DB); lock contention
                                on one registration id across three instances; every timer unref'd, clean full stop
    circuitBreaker.test.ts      Outage end to end: threshold → OPEN → deferral (queue + webhook), 503s; half-open probe from the
                                outbox → CLOSED → everything restored under the original correlation ids; failed probe doubles;
                                declines never trip it; admin snapshot
    outbox.test.ts              Crash after commit → delivered later; publisher crash after confirm → same eventId again;
                                nacks → backoff → parked → operator retry; confirm timeout; failed commit leaves no partial
                                state (vault + payment + events) and dead-letters; refund → REFUND_PROCESSED twice; wire contract

tsconfig.json                   Build config (src/ only)
tsconfig.eslint.json            Type-check config for src/ + tests/ (used by eslint & typecheck)
```

---

## Production readiness

- **Nothing the service schedules can keep the process alive.** Every timer — AMQP reconnect backoff, outbox poll, recovery sweep, reconciler, breaker half-open, lock watchdog, pool deferral sleep, publisher confirm timeout — is `unref()`'d, so the only foreground handles are the HTTP listener, the AMQP socket and the Redis sockets. `stop()` on every component clears its timers and detaches its listeners; the chaos suite asserts both. (Corollary: the HTTP listener in `server.ts` is what anchors the process during a broker outage; a headless worker build would need to `ref()` the AMQP reconnect timer.)
- **Fail closed, never silently.** No lock → no charge; gateway circuit open → defer or `503`, never a retry storm; Redis unreachable → defer with jitter; DB write fails → nothing partial, the message is dead-lettered or retried.
- **At-least-once, de-duplicated.** Every outbound event carries `eventId`/`messageId`; every charge carries `chargeId`; every verification is keyed by its OPPWA resource path. Consumers de-duplicate on those.
- **Observable.** `/api/admin/outbox`, `/api/admin/circuit`, `/api/admin/charges/status`, `/api/admin/reconcile/status`, `/api/admin/dlq` expose every queue, state machine and counter described in this document; every state transition is logged.
- **Known limits**, stated where they apply: the JSON file DB is single-process (the locks and the outbox protocol are ready for a real DB); Redlock hands no fencing token to OPPWA; a Redis node without persistence forgets its locks on restart.

---

## Graceful Shutdown

The process handles `SIGTERM` and `SIGINT`:
1. Stops accepting new HTTP connections.
2. Stops the background reconciler.
3. Stops the gateway recovery (an in-flight replay finishes) and the breaker's timer.
4. Stops the outbox publisher: waits for the cycle in progress, releases leases it had not published, closes its channel. Events not yet confirmed stay in the DB for the next instance.
5. Cancels the AMQP consumers (in-flight messages finish first) and closes the channels and connection cleanly.
6. Stops the charge worker pool (sleeping deferrals are left unacked for another instance, running charges finish), releases every lock this instance holds, and closes the Redis connections.

---

## Platform order flow (this repository)

In this monorepo the gateway is the entry point of the order-to-invoice flow (see the root README):

- `POST /api/webhook/hyperpay` verifies the HMAC-SHA256 signature over the **raw request bytes** (`401` otherwise), validates the OPPWA payment resource (`400`), and in ONE JSON-DB transaction records the payment (card block and registration token stripped; the token is sealed in the vault when it is enabled) and a `PAYMENT_COMPLETED` outbox row carrying the order context (`merchantTransactionId`, `customParameters.merchantId` / `creditOrgId`, customer, `cart.items` in halalas) — `src/services/paymentWebhook.ts`.
- Idempotency: one record per OPPWA payment id. A redelivery answers `200 DUPLICATE` with the original event id; PENDING → SUCCESS/FAILED emits one new event (`202 STATUS_UPDATED`); a contradicting final outcome is recorded under `statusConflicts` and never announced (`200 CONFLICT`).
- `src/messaging/runtime.ts` starts the existing self-healing AMQP client and the outbox publisher; events go to `hyperpay` / `payment.result` (also consumed by `billing.payment-results`).
- Components described above that are not in this repository (OPPWA client, payment processor, consumers, admin API) are not used; the signed notification is trusted instead of re-querying OPPWA.

### Event authentication

Every event this service publishes is signed with its stream's shared key (HMAC-SHA256,
`x-key-id` / `x-event-stream` / `x-event-issuer` / `x-event-audience` / `x-signature`), and every
delivery it consumes is verified against the authorization matrix before the body is parsed: wrong
key, wrong issuer, wrong audience or a tampered byte means the message is dead-lettered at once
(`UNTRUSTED_EVENT`). See `shared/security/streams.ts` and the root README.

### Customer refunds

`src/services/refundWebhook.ts`: a signed RF notification (`paymentType: "RF"`, `referencedId` = the captured payment) is recorded once per refund id and announced as `REFUND_PROCESSED` on `hyperpay / refund.processed` (stream `hyperpay.refund.processed`, audience: billing). It is refused — `409 ORIGINAL_PAYMENT_UNKNOWN`, `422 ORIGINAL_PAYMENT_NOT_CAPTURED | CURRENCY_MISMATCH | REFUND_EXCEEDS_REFUNDABLE` — before anything is recorded when it does not fit what is still refundable (captured − successful and pending refunds). Duplicates, PENDING → final progressions and contradictions behave like payments. `GET /api/payments/:paymentId/refunds` lists a payment's refunds.
