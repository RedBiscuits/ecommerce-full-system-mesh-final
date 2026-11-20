# Multi-Currency Wallet & Double-Entry Ledger Engine

A transactional, multi-currency digital wallet on a double-entry ledger: P2P transfers with integer fee and FX pricing, an AML escrow saga, bank withdrawals with webhook reconciliation, database-enforced ledger integrity, and an automated audit and self-healing reconciliation pipeline over RabbitMQ. It is protected by verified bearer tokens, with per-wallet authorization.

- **[Business rules](docs/BUSINESS_RULES.md):** what the system guarantees, and which tests prove each guarantee.
- **[Integration guide](docs/INTEGRATION_GUIDE.md):** authentication, endpoints, error codes, bank webhooks, ledger events, configuration, runbook.

## Tech stack

- **Runtime:** Node.js (≥ 20.19 or ≥ 22.12) and TypeScript
- **Web framework:** Fastify
- **Validation:** Zod
- **Auth:** `jose` (JWT verification)
- **Database:** PostgreSQL, via the embedded PGlite sandbox engine (`pg`-compatible)
- **Cache / dedupe:** Redis (`ioredis`; sandbox uses `ioredis-mock`)
- **Messaging:** RabbitMQ via `amqplib` (sandbox uses the in-process `@onify/fake-amqplib`)
- **Tooling:** Vitest, ESLint with typescript-eslint

## Quick start

```bash
npm install
cp .env.example .env             # set AUTH_JWT_SECRET (>= 32 bytes)
npm run dev                      # API + worker; seeds the in-memory sandbox DB at boot:
                                 # alice/bob USD 100, carol EUR 100, dave JPY 15,000 + system accounts

TOKEN=$(npm run -s token:mint -- --sub user_alice --roles user)
curl -s localhost:3002/wallets/wallet_alice -H "Authorization: Bearer $TOKEN"
```

| Script | Purpose |
|---|---|
| `npm test` | full test suite |
| `npm run lint` / `npm run typecheck` | static checks |
| `npm run check` | typecheck, then lint, then tests |
| `npm run token:mint -- --sub <owner> --roles user,admin,payout_webhook` | development token (refuses in production) |

## Architecture at a glance

```
HTTP (Fastify) -- installAuth: every route declares public | authenticated | admin | payout_webhook
   |
   +- WalletService / TransferSagaService / PayoutWebhookService   (one DB transaction per operation)
   |     +- money engine (src/money): bigint fees (bps), scaled-integer FX, banker's rounding, balanced journals
   |     +- locks: saga/payout row -> user+escrow wallets (sorted, FOR UPDATE) -> sharded buckets (sorted)
   |     +- ownership (IDOR) checks on the locked row; frozen-wallet guard -> 409 + self-healing event
   |
   +- PostgreSQL, enforced at COMMIT:
   |     O(1) balance = running-total sync; per-currency journal balance; append-only ledger;
   |     sign rules; sharded buckets for fee / FX pool / clearing accounts (no hot rows)
   |
   +- LedgerAuditService (admin): paged full recount -> events (queue-aware backpressure)
         |
         v  RabbitMQ: confirms, bounded pending ledger, atomic reconnect, TTL retry tiers, DLQ
      LedgerReconciliationService (worker): lock -> verify ledger -> rebuild caches -> log
```

## Project layout

```
src/
  api/          routes, each with an explicit access policy
  auth/         principal and roles, JWT verification, route policy hooks
  money/        integer money engine (pure functions)
  services/     wallet, transfer saga, webhooks, audit, reconciliation, ledger writer
  messaging/    AmqpEventBus (confirms, re-publishing), RecoveringConnection (atomic reconnect)
  db/           schema.sql (tables, triggers, seed), PGlite sandbox pool
  scripts/      mint-token
__tests__/      one suite per concern, plus helpers/ (auth, broker harness, event waits)
docs/           BUSINESS_RULES.md, INTEGRATION_GUIDE.md
```

## Sandbox notes

- **The sandbox database is in-memory, per process.** The server creates and seeds its own schema at boot (`DB_SEED_ON_START`, default `true`). `npm run db:seed` only exercises the seed in a throwaway instance; it cannot populate a running server.
- **PGlite is single-connection.** The sandbox pool serializes transactions, so row-lock contention and deadlocks cannot physically occur here. Lock ordering is therefore verified structurally (the order of `FOR UPDATE` statements, sorted journal lines, the bucket distribution), and production behaviour relies on real PostgreSQL honouring those locks.
- **The fake broker differs from RabbitMQ in three ways:**
  - On overflow it evicts the oldest message instead of honouring `x-overflow=reject-publish`, and counts unacked deliveries toward `x-max-length`.
  - It expires TTL messages lazily.
  - It drops all state when its last connection closes.

  The audit's conservative headroom keeps it from ever overflowing the queue, and the tests model RabbitMQ's behaviour explicitly where those differences matter. Use `EVENT_BUS_DRIVER=amqp` with a real RabbitMQ beyond local development.

## Known limitations

- **Idempotency keys:** `idempotencyKey` on withdrawals is stored but not enforced. A client retry can create a second payout.
- **Event durability:** the pending-publish ledger lives in memory. Events lost in a process crash are recovered by detection: the write guard re-fires on the next request, and the periodic audit re-detects. A transactional outbox table would make them durable.
- **Identity provider:** none is included. Tokens are verified, not issued (apart from the development mint script).
- **Shard count:** it is fixed when an account is created.

## Card payments (platform order flow)

- **SAR** is a supported currency (halalas): `wallet_treasury_sar` (clearing; also mirrors the card gateway), escrow / fee / FX accounts, `wallet_vat_payable_sar` (a CLEARING account: output VAT owed to ZATCA) and the merchant revenue wallet `wallet_merchant_riyadh_retail` (`merchant_accounts`). USD↔SAR rates use the 3.75 peg.
- `PaymentSettlementService` (`src/services/PaymentSettlementService.ts`): a card payment is a two-phase hold (`payment_holds`: HELD → CAPTURED | RELEASED). Capture posts ONE balanced 3-leg journal — clearing DEBIT gross, merchant CREDIT net, VAT payable CREDIT 15 % — whose split is computed with ZATCA's exact arithmetic from the order lines and the captured-credit allowance and must reproduce the amount charged (otherwise `AMOUNT_MISMATCH`, dead-lettered, nothing posted). `order_settlements` has the order id as primary key: an order's revenue can never be booked twice.
- `src/integration/runtime.ts`: consumes `billing.events / billing.order-credits.settled` (queue `ledger.order-settlements`), publishes `ledger.payment.settled` / `ledger.payment.released` through a transactional outbox (`integration_outbox`). Checkpoint, hold transition, journal, settlement row and outbox event commit in one transaction.
- Admin API: `POST /payments/holds` (record an authorization), `GET /payments/orders/:orderId/settlement`.
- The self-healing `RecoveringConnection` moved to `shared/messaging` (re-exported from `src/messaging`).

### Tender holds (split-tender checkouts)

- `payment_holds` is keyed by `hold_key` (UNIQUE). `payment_id` is nullable and UNIQUE, and is bound when the gateway payment arrives. A hold also records `checkout_id`, `tender` (`CARD` | `WALLET`), `source_wallet_id` and the journal references of its reserve / capture / release.
- **CARD**: the hold records the authorization; the capture posts the usual 3-leg journal from the clearing account.
- **WALLET**: placing the hold moves the remainder from the customer's wallet into `wallet_escrow_sar` straight away (reserve journal). `INSUFFICIENT_FUNDS`, unknown or frozen wallets roll back and no row is written. The capture then posts the 3-leg journal from escrow (escrow DEBIT gross, merchant CREDIT net, VAT payable CREDIT). A release refunds escrow to the customer.
- `POST /payments/holds` — the legacy card authorization, or a keyed tender hold `{ holdKey, orderId, merchantId, tender, sourceWalletId?, amountHalalas, checkoutId? }`. Returns `201` created, `200` replay of the same key, `409` insufficient funds / released key / mismatch.
- `POST /payments/holds/release` `{ holdKey, reason }` — idempotent. If the key is unknown it is **tombstoned**, so a late placement is refused. Returns `409` if the hold was already captured. `GET /payments/holds/:holdKey` returns the hold.
- Role `checkout_coordinator` (access policy `payment_holds`): the coordinator's service principal.
- Seed: `wallet_noura_sar` (user `user_noura`) starts with 500.00 SAR, used by the demo and the split-tender tests.

## Merchant settlement batches

`src/services/MerchantSettlementService.ts`, API in `src/api/settlements.routes.ts` (admin role):

- **Sweep.** `POST /settlements/merchants/:merchantId/batches` `{ from?, to?, maxItems?, idempotencyKey?, wait? }` opens a batch: `202`, collected in the background (`wait: true` → `201` once collected). `from` omitted means since the beginning. The merchant's settled, unswept `order_settlements` in `[from, to)` are claimed in keyset chunks (`SETTLEMENT_SWEEP_CHUNK_SIZE`, default 500). Each chunk is one short transaction: `UPDATE … SET settlement_batch_id` under `FOR UPDATE SKIP LOCKED`, aggregated by PostgreSQL, so the app holds one row per chunk. The event loop is yielded between chunks, with an optional pause (`SETTLEMENT_SWEEP_PAUSE_MS`). A batch holds at most `maxItems` entries (`SETTLEMENT_MAX_ITEMS_PER_BATCH`, default 100,000); `hasMore` marks a capped batch. Only unswept entries are in the sweep's partial index, so an entry is scanned and settled once. A trigger forbids moving it to another batch. One batch collects per merchant at a time (`409 SETTLEMENT_IN_PROGRESS`). A batch with no revenue ends `EMPTY`. Otherwise it ends `COLLECTED` and `ledger.merchant-settlement.collected` goes to the outbox in the same transaction.
- **Post** (queue `ledger.settlement-fees`, on billing's `billing.settlement-fee.calculated`). One transaction holds the checkpoint, a cross-check of the batch against the capture journals it claimed, the wallet locks, and the balanced 3-leg journal `stl_<batch>`: merchant revenue DEBIT gross, `wallet_fee_treasury_sar` CREDIT fee incl. VAT, the merchant's payout wallet (`merchant_accounts.payout_wallet_id`, seeded `wallet_payout_riyadh_retail`) CREDIT gross − fee. It also moves the batch to `POSTED` (`journal_ref` UNIQUE) and writes `ledger.merchant-settlement.posted` to the outbox. The following are refused and dead-lettered: `BATCH_MISMATCH`, `FEE_ARITHMETIC_MISMATCH`, `SETTLEMENT_LEDGER_MISMATCH`, `INSUFFICIENT_MERCHANT_REVENUE`.
- **Complete** (queue `ledger.settlement-documents`, on ZATCA's `zatca.settlement-documents.reported`). The batch moves to `COMPLETED` with the document references.
- **Reads.** `GET /settlements/batches/:batchId` returns the batch, its journal and checkpoints. `GET /settlements/batches/:batchId/items?after=&limit=` returns keyset pages. `GET /settlements/merchants/:merchantId/batches` lists a merchant's batches. `POST /settlements/maintenance` resumes sweeps left `COLLECTING`.

### Event authentication

Every event this service publishes is signed with its stream's shared key (HMAC-SHA256,
`x-key-id` / `x-event-stream` / `x-event-issuer` / `x-event-audience` / `x-signature`), and every
delivery it consumes is verified against the authorization matrix before the body is parsed: wrong
key, wrong issuer, wrong audience or a tampered byte means the message is dead-lettered at once
(`UNTRUSTED_EVENT`). See `shared/security/streams.ts` and the root README.

### Customer refunds

`src/services/OrderRefundService.ts` consumes `billing.order-credits.restored` (queue `ledger.order-refunds`) and reverses the capture pro rata: the refund is split over what is still refundable of every component (taxable amount and VAT per VAT category; `proRataRefundSplit`, exact to the halala, a final refund reverses exactly what remains) and posted as ONE balanced 3-leg journal `rfd_<refund>` — merchant revenue (unsettled) or payout wallet (settled) DEBIT net share, `wallet_vat_payable_sar` DEBIT VAT share, gateway clearing (card) or the customer's wallet (wallet tender) CREDIT the refund. Running totals on `order_settlements` (`refunded_*`, per-category `refund_state`) are CHECKed against the capture; `order_refunds` has one row per refund id. A refund of an order inside a batch being priced / posted is retried (`SETTLEMENT_IN_FLIGHT`); settlement sweeps claim `net − refunded` (`settled_net_cents`). `GET /payments/orders/:orderId/refunds` (admin).
