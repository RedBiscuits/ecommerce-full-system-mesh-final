# KSA Fintech Commerce & ZATCA Phase 2 Tax Compliance Suite

A Node.js 20+ / TypeScript monorepo of Saudi (KSA) commerce services: HyperPay payments, a double-entry halala ledger, prepaid credits, ZATCA Phase 2 (Fatoora) e-invoicing and a tiered file cache, wired into **one order-to-invoice flow over RabbitMQ**. A sixth service, the checkout coordinator, handles **split-tender checkouts** (promotional credits + card or wallet). A **merchant settlement batch workflow** and a **customer refund flow** (partial or full) run across the existing services. The ledger sweeps a merchant's settled revenue, billing prices the tiered processing fee, the ledger posts the settlement journal, ZATCA documents it, and the file cache keeps the merchant's statement.

Everything runs in-process without Docker: embedded PostgreSQL (`@electric-sql/pglite`), an in-process RabbitMQ (`@onify/fake-amqplib`) and `ioredis-mock`. Set `PLATFORM_BROKER_DRIVER=amqp` + `RABBITMQ_URL` to run against a real RabbitMQ, and `DATABASE_URL` to run billing on a real PostgreSQL.

```bash
npm install
npm test            # every test of every service + the platform, in parallel shards (also: npm run tests)
npm run platform:demo   # a signed checkout, a split-tender checkout and a merchant settlement, end to end
```

---

## The order flow

A signed HyperPay webhook enters the gateway; from there every hop is an event on the broker, published through a **transactional outbox** and consumed by a **reliable, idempotent consumer** that owns its own queue, retry tiers and dead-letter queue.

```
 signed webhook (HMAC over raw bytes)
        │
        ▼
 hyperpay-gateway ── payment + PAYMENT_COMPLETED in ONE commit ──outbox──► hyperpay (direct) / payment.result
                                                                               │ queue billing.payment-results
 billing-credits  ◄────────────────────────────────────────────────────────────┘
   SUCCESS → capture the order's credit hold (credits charged FIFO)
   FAILED  → release it (reservation back to the pool)
   captured credits × 100 halalas = the order's allowance ─────outbox──► billing.events / billing.order-credits.settled
                                                                               │ queue ledger.order-settlements
 wallet-ledger    ◄────────────────────────────────────────────────────────────┘
   FAILED  → release the payment hold (nothing is booked)
   SUCCESS → capture the hold and post ONE balanced 3-leg SAR journal:
               wallet_treasury_sar            DEBIT   gross     (money in from the gateway)
               wallet_merchant_riyadh_retail  CREDIT  net       (revenue after the credit allowance)
               wallet_vat_payable_sar         CREDIT  VAT 15 %  (owed to ZATCA)      ──outbox──► ledger.events / ledger.payment.settled
                                                                               │ queue zatca.ledger-settlements
 zatca-tax        ◄────────────────────────────────────────────────────────────┘
   step 'issued': Simplified Tax Invoice 388/0200000 with the credit allowance,
                  next ICV, PIH = previous hash, ECDSA stamp, TLV QR, UBL XML
   step 'done'  : reported to Fatoora                                ──outbox──► zatca.events / zatca.invoice.reported
                                                                               │ queue file-cache.invoice-pdfs
 file-cache-engine ◄───────────────────────────────────────────────────────────┘
   render the invoice PDF from the event and cache it (key = taxpayer/invoice, sha256 + invoice hash + ICV as attributes)
```

Example (`npm run platform:demo`): 2 × 100.00 SAR, 20 credits held → **207.00 SAR** charged = 180.00 revenue + 27.00 VAT, invoice ICV 1 with a 20.00 SAR credit allowance, PDF cached.

### Guarantees

| Concern | How |
|---|---|
| No lost events | Every producer writes its event in the **same DB transaction** as the state change (transactional outbox: hyperpay's JSON-DB outbox, `integration_outbox` in billing / wallet / ZATCA) and a relay publishes it with **publisher confirms**. A broker outage only delays delivery. |
| Duplicate webhooks | One payment record per OPPWA payment id: a retry is answered `200 DUPLICATE` with the original event id, a PENDING→final update emits one new event, a contradicting final outcome is recorded as a conflict and never announced. |
| Nothing done twice | Per-consumer **checkpoints** (`integration_checkpoints`: consumer, event, step) committed atomically with each step, plus **business keys** as a second line (one order-credit capture per order, one payment hold per payment, one ledger settlement per order — its primary key —, one invoice per order, unique ICV / PIH / invoice hash per taxpayer) and **deterministic event ids** for every downstream event, so even a re-announcement under a new id changes nothing. |
| Checkpoints per event | Each consumer has its own queue, so a retry or a DLQ replay reaches **only the consumer that failed**. Multi-step work resumes after the last completed step (a Fatoora outage retries the report without burning another ICV). `traceOrder()` / the `…/settlement` and `…/invoicing` endpoints show every checkpoint of an order. |
| Retries | Transient failures are retried with **full-jitter exponential backoff** (`random(min, min(max, base·2^(n−1)))`) through one TTL queue per attempt (`<queue>.retry.<n>`), up to `maxAttempts` (default 5). |
| DLQ | Malformed JSON / schema-invalid messages and `NonRetryableError`s (business rules: amount mismatch, order already paid, Fatoora rejection, …) go to `<queue>.dlq` at once; exhausted retries too, with the reason in `x-failure-reason`. `consumer.inspectDeadLetters()` / `replayDeadLetters()` per consumer. |
| Ledger integrity | Never double-posted (checkpoint + hold state machine + `order_settlements` PK), balanced per currency (app check + deferred DB trigger), VAT split computed with ZATCA's exact arithmetic (pinned by a 5,000-order parity property test) and refused (DLQ) if it does not reproduce the amount charged. |
| ZATCA chain | ICVs assigned under the taxpayer row lock; UNIQUE (taxpayer, ICV), (taxpayer, PIH), (taxpayer, hash) in the database; `verifyHashChain()` / `GET /api/v1/taxpayers/:id/hash-chain` re-verifies the whole chain (links, recomputed hashes, ECDSA stamps, head). |
| Only the right service reads an event | Every event is signed with its stream's key (HMAC-SHA256) and names its audience; a service that is not in the audience does not hold the key and dead-letters the delivery (`UNTRUSTED_EVENT`) before parsing it. See [Authenticated service-to-service events](#authenticated-service-to-service-events-authorization-matrix--hmac-sha256). |
| Eventual consensus | `verifyOrderConsistency(orderId)` checks that what HyperPay charged, billing captured, the ledger booked, ZATCA invoiced and the file cache holds describe the same sale; `npm run cluster:doctor` reports DLQs, parked outbox rows and hash-chain breaks. |

---

## Split-tender checkouts (credits + card / wallet)

`services/checkout-coordinator` ([README](services/checkout-coordinator/README.md)) reserves the two
tenders of a checkout as one unit. The promotional credits are held at billing and the remainder
at the ledger (card authorization, or the customer's wallet moved into escrow). If either is
refused, both are released and both tenders report the same failure code. After that the order
runs through the **same** RabbitMQ pipeline:

```
POST /checkouts (coordinator) ── credits hold (billing, key chk:<id>:credits)
                              └─ remainder hold (ledger, key chk:<id>:remainder; WALLET: customer → escrow)
  CARD   → customer pays at HyperPay with customParameters.checkoutId → PAYMENT_COMPLETED
  WALLET → POST /checkouts/:id/confirm → outbox → checkout.events / checkout.payment.completed
        → billing captures the credit hold (allowance) → ledger captures the remainder hold
        → ONE 3-leg journal (clearing or escrow DEBIT gross / merchant CREDIT net / VAT CREDIT)
        → ZATCA invoice: promotion as an allowance per VAT category → PDF cached
        → ledger.payment.settled / released → checkout COMPLETED / PAYMENT_FAILED
```

Example (`npm run platform:demo`, step 5): lines S 100 + Z 100 + E 50 SAR, 25 credits (25.00 SAR),
remainder from the wallet. The allowance is split 10 / 10 / 5 across the categories. The customer
pays **238.50 SAR** from escrow: 225.00 revenue + 13.50 VAT. The invoice carries the allowance
"Promotional credit (25 credits)" and a VAT breakdown per category.

| Concern | How |
|---|---|
| Both or neither | The coordinator runs a saga (credits, then remainder). A refusal or an unavailable service triggers compensation, which releases whatever was already reserved and fails the checkout with the refusal's code (`INSUFFICIENT_FUNDS`, …). |
| Idempotent retries | `checkoutId` / `Idempotency-Key` at the coordinator, plus deterministic **hold keys** at billing and the ledger. A call that timed out after it committed is retried with the same key and returns the same hold: one debit, one reservation. |
| No orphans | A release for a key that was never placed writes a **tombstone**, so a late placement is refused (`HOLD_CANCELLED`). Leases plus a recovery sweep resume sagas left behind by a crashed instance, and unpaid checkouts expire and are released. `verifyOrderConsistency` checks for active credit holds or held ledger holds left behind, and that every released wallet reservation was refunded. |
| Exact money | Integer halalas throughout. The allowance is allocated across VAT categories with BigInt largest remainder (`shared/contracts/money.ts`), so the shares always add up exactly. The per-category VAT reduction uses banker's rounding (half-even) and matches ZATCA's engine byte for byte (parity tests). A property test checks that every halala and every credit is conserved across many mixed-category checkouts. |
| Fixed along the way | ZATCA's engine used to take the whole allowance off the standard-rated base. For a mixed S/Z/E basket that under-collected VAT (11.25 instead of 13.50 SAR in the example above). |

---

## Merchant settlement batches

The workflow uses only the existing services. Every hop is a transactional outbox on the producer side and a checkpointed consumer with its own queue, retry tiers and DLQ on the consumer side.

```
wallet-ledger   POST /settlements/merchants/:merchantId/batches  { from?, to?, maxItems?, idempotencyKey? }
  sweep: claims the merchant's settled, unswept order revenue in [from, to)
         (from omitted = since the beginning), in keyset chunks of SETTLEMENT_SWEEP_CHUNK_SIZE
  ──outbox──► ledger.events / ledger.merchant-settlement.collected                 queue billing.settlement-fees
billing         tiered fee (marginal tiers in bps + a fee per transaction + a minimum), VAT 15 % on the fee,
                frozen per batch
  ──outbox──► billing.events / billing.settlement-fee.calculated                   queue ledger.settlement-fees
wallet-ledger   ONE balanced 3-leg settlement journal, atomic with the batch → POSTED:
                  wallet_merchant_riyadh_retail  DEBIT   gross            (settled revenue)
                  wallet_fee_treasury_sar        CREDIT  fee incl. VAT    (the platform)
                  wallet_payout_riyadh_retail    CREDIT  gross − fee      (the merchant's next payout)
  ──outbox──► ledger.events / ledger.merchant-settlement.posted
                ├─ queue file-cache.settlement-statements: statement JSON, PENDING_TAX_REPORTING, kept 72 h
                └─ queue zatca.merchant-settlements: the platform (its own taxpayer, ICV / PIH chain)
                     388 tax invoice for the fee: taxable = the platform's net fee revenue, VAT 15 %
                     381 credit note for the payout (references the fee invoice; O / VATEX-SA-OOS, no VAT)
                     B2B (the merchant is VAT-registered) → standard 0100000, CLEARED by Fatoora
  ──outbox──► zatca.events / zatca.settlement-documents.reported
                ├─ queue file-cache.settlement-statements: both PDFs + UBL XML cached, statement → FINAL
                └─ queue ledger.settlement-documents: batch → COMPLETED
```

Example (`npm run platform:demo`, step 6): 405.00 SAR of revenue from 2 orders. The fee is 2.50 % = 10.125, rounded half-even to 10.12, plus 2 × 0.50 = 11.12 SAR, and 1.67 SAR VAT, for a total of 12.79 SAR. The payout is 392.21 SAR. Fee invoice ICV 1, credit note ICV 2, statement FINAL.

| Concern | How |
|---|---|
| Each entry settled once | An order settlement is claimed by setting `order_settlements.settlement_batch_id` under `FOR UPDATE SKIP LOCKED`. The sweep reads only a partial index over unswept rows, so a claimed entry is never scanned again by this or any later sweep. A trigger forbids moving or un-sweeping it. One batch collects per merchant at a time. |
| No OOM, no CPU hogging | Each chunk is one short transaction that claims up to N rows with `UPDATE … RETURNING` and aggregates them inside PostgreSQL, so the app receives one row per chunk. The event loop is yielded between chunks, with an optional pause (`SETTLEMENT_SWEEP_PAUSE_MS`), and other requests are served in between. A batch is capped at `maxItems` (`hasMore` tells you the window has more). The posting cross-check, the item listing (keyset pages) and the documents are bounded too: the invoice and credit note carry one line each, however large the batch. A 3,000-entry sweep in the tests never blocks the process for longer than one chunk. |
| Resumable | A sweep that crashed or was aborted leaves the batch `COLLECTING`. `POST /settlements/maintenance` (or `resumeCollecting()`) continues it, and chunks are idempotent. |
| Money is exact | Integer halalas and BigInt throughout. The percentage fee is rounded once with banker's rounding, and the per-tier shares add up to it exactly. The fee VAT uses the ZATCA engine's own arithmetic, so the fee invoice reproduces it. The fee never exceeds the gross. The ledger refuses a fee whose totals don't match what it collected (`BATCH_MISMATCH`), checks the batch against the capture journals it booked (`SETTLEMENT_LEDGER_MISMATCH`), and will not overdraw the merchant's revenue wallet. |
| Never posted twice | Consumer checkpoint, `journal_ref` UNIQUE (`stl_<batch>`), a batch state machine and deterministic event ids. Billing freezes the fee per batch, so a replay after a rule change keeps the original fee. ZATCA allows one fee invoice and one credit note per batch (unique index), and a Fatoora outage retries only the submission, so no ICV is burned. |
| Pending until reported | The statement is `PENDING_TAX_REPORTING` (no `tax` section) until ZATCA cleared or reported both documents. It then becomes `FINAL` and lists the invoice numbers, ICVs, hashes and PDF / XML keys. It only moves forward (events can arrive in any order) and keeps the 72 h from its first write. |
| Stampede-free reads | `GET /settlements/:merchantId/:batchId` on the file cache (port 3106): concurrent fetches of one statement share one read (single flight), and the hotness tracker (the TinyLFU frequency sketch) pins frequently fetched statements in memory for 1 s. A write drops the pin and starts a new read generation, so nobody gets a stale PENDING. In the tests, 500 concurrent fetches make 1 store read. |
| Consensus | `platform.verifySettlementConsistency(batchId)` checks that the claimed entries, billing's fee, the journal legs, the ZATCA fee invoice and credit note, the hash chain and the file cache statement and documents agree, and that every consumer checkpointed. `platform.traceSettlement(batchId)` shows all of it. |

APIs: wallet (admin): `POST /settlements/merchants/:merchantId/batches`, `GET /settlements/merchants/:merchantId/batches`, `GET /settlements/batches/:batchId`, `GET /settlements/batches/:batchId/items`, `POST /settlements/maintenance`. Billing: `GET|POST /billing/settlement-fees/rules`, `POST /billing/settlement-fees/quote`, `GET /billing/settlement-fees/batches/:batchId`. ZATCA: `GET /api/v1/settlements/:batchId/documents`, `POST /api/v1/invoices/:id/clearance`. File cache: `GET /settlements/:merchantId/:batchId`, `GET /documents/:taxpayerId/:invoiceId.pdf|xml`.

Judgment calls, worth a look by finance / tax:
- **The fee leg carries the fee including VAT**, to keep the journal at the three legs requested. The fee invoice splits it into net fee (the platform's revenue) and VAT. To book that VAT as a liability in the ledger, add a fourth leg (fee treasury → `wallet_vat_payable_sar`).
- **The payout credit note.** A ZATCA credit note normally reduces an earlier supply. Here it documents the payout credited to the merchant and references the fee invoice. Its line is out of scope (O / VATEX-SA-OOS), so it carries no VAT and changes no VAT return. If your tax advisor prefers a non-fiscal remittance advice, turning the credit note off is a small change in `SettlementInvoicingService`. The rest of the flow already copes with a missing credit note, which is what happens when the payout is zero.
- **Standard (B2B) documents are cleared, not reported**, since the merchant is VAT-registered. An unregistered merchant gets simplified documents, which are reported. The UBL carries the buyer's VAT number; the buyer's postal address is not stored.

---

## RabbitMQ: where each service stood, and what was added

| Service | Before | Now |
|---|---|---|
| `hyperpay-gateway` | Complete messaging layer (self-healing AMQP client, topology with DLX/DLQ, outbox + confirm publisher, DLQ replay) — **but never started**, and the webhook only checked the signature (over a re-serialised body) and returned 202: nothing was recorded or published. | Webhook verifies the HMAC over the **raw bytes**, records the payment (card data redacted, tokens sealed in the vault) and `PAYMENT_COMPLETED` in one commit; `startHyperPayMessaging()` runs the existing client + outbox publisher. |
| `wallet-ledger` | **Properly set up** for its own invariant / reconciliation events (confirms, atomic reconnect, TTL retry tiers, DLQ) — no payment flow, no SAR. | Adds SAR (halalas), card payment holds, the 3-leg capture journal, an outbox relay and the `ledger.order-settlements` consumer. Its reconnect logic now lives in `shared/messaging` and is reused by every service. |
| `billing-credits` | No RabbitMQ (an internal auto-recharge outbox table only). | Order credit holds, capture / release on the payment outcome, outbox relay, `billing.payment-results` consumer. |
| `zatca-tax` | No RabbitMQ. | Invoicing of settled payments with per-step checkpoints, outbox relay, `zatca.ledger-settlements` consumer, DB-enforced chain uniqueness, chain verifier. |
| `file-cache-engine` | No RabbitMQ (a library). | `file-cache.invoice-pdfs` worker: renders and caches invoice PDFs. |

Fidelity fix for the in-process broker: `@onify/fake-amqplib`'s engine load-balanced **direct** exchanges (one matching queue per message) instead of delivering a copy to every bound queue as RabbitMQ does — with three queues bound to `hyperpay / payment.result` two out of three payments would silently never reach billing. `shared/messaging/connect.ts` restores AMQP semantics for the fake broker (tested in `shared/__tests__/broker-fidelity.test.ts`).

---

## Customer refunds (partial or full)

A refund starts where the money is: a signed OPPWA **RF** notification hits the gateway webhook,
referencing the captured payment. From there it crosses the same signed queues as everything else.

```
hyperpay-gateway  POST /api/webhook/hyperpay  { paymentType: "RF", referencedId, amount, … }
  one record per refund id; refused (422) when amount > captured − refunds already recorded
  ──outbox──► hyperpay / refund.processed                           queue billing.refunds
billing-credits   restores ⌊credits × refundedTotal / paid⌋ − already restored, as a new
                  PROMOTIONAL grant (so a full refund gives every credit back)
  ──outbox──► billing.events / billing.order-credits.restored        queue ledger.order-refunds
wallet-ledger     ONE balanced 3-leg reversal rfd_<refund>:
                    merchant revenue (or payout, once settled)  DEBIT   net share
                    wallet_vat_payable_sar                      DEBIT   VAT share
                    gateway clearing / customer wallet          CREDIT  the refund
  ──outbox──► ledger.events / ledger.payment.refunded               queue zatca.ledger-refunds
zatca-tax         credit note 381 against the order's invoice, one line per VAT category with
                  the pro-rata VAT, reported (or cleared), next ICV in the merchant's chain
  ──outbox──► zatca.events / zatca.credit-note.reported             queue file-cache.credit-notes
file-cache        caches the credit note PDF + XML; invalidates the invoice's cached PDF and
                  re-renders it with the credit notes applied
```

| Concern | How |
|---|---|
| Never more than what was paid | The gateway refuses a refund beyond `captured − (successful + pending refunds)` before recording anything. The ledger refuses anything beyond `captured − refunded` (and CHECK constraints keep the running totals inside the capture). ZATCA refuses credit notes beyond the invoice total. |
| Exact, pro-rata split | `proRataRefundSplit` (shared) allocates the amount over what is still refundable of every component — the taxable amount and the VAT of each VAT category — with largest-remainder allocation: exact to the halala, no part beyond what is left, and a final refund reverses exactly what remains (the tests check a mixed S/Z/E order to zero). |
| The credit note carries the VAT actually returned | Its lines state the pro-rata VAT explicitly; the engine accepts it within the rounding the invoice itself accumulated (per line and allowance). The ledger's reversal, the credit note and the money returned therefore agree to the halala. |
| Promotion restored | Billing restores the pro-rata share of the captured credits as a new PROMOTIONAL grant that expires when the consumed grants would have (else `ORDER_CREDIT_RESTORE_TTL_DAYS`). The FIFO history of the capture is left intact. |
| Settled merchants | Before the sweep, the reversal debits the revenue wallet and the sweep later claims `net − refunded`. After the batch is posted, it debits the merchant's **payout** wallet (the next payout shrinks). While a batch is being priced or posted, the refund waits (`SETTLEMENT_IN_FLIGHT`, retried with jitter). If the money already left the platform, the delivery parks (`MERCHANT_BALANCE_INSUFFICIENT`) for an operator. |
| Idempotent | One record per refund id at the gateway (duplicate webhooks → `200 DUPLICATE`), checkpoints + `refund_id` primary keys at billing and the ledger, `rfd_<refund>` journal ref UNIQUE, one credit note per refund (unique index), deterministic event ids; the file cache re-renders only when the credited total changed. |
| Invariants checked by the doctor | `npm run cluster:doctor` → `refunds`: refunded totals equal the refund records and never exceed the capture, every reversal balanced and recorded, restored credits never beyond captured, one credit note per refund of the same amount, Σ credit notes never beyond the invoice. `verifyOrderConsistency(orderId)` adds the per-order refund checks (including the re-rendered PDF). |

APIs: `GET /api/payments/:paymentId/refunds` (gateway), `GET /credits/orders/:orderId/refunds` (billing), `GET /payments/orders/:orderId/refunds` (wallet, admin), `GET /api/v1/orders/:orderId/credit-notes` (ZATCA).

Not covered (on purpose): refunds of wallet-paid split-tender orders have no entry point yet — the ledger reverses them to the customer's wallet when asked (tested), but nothing asks; a refund request API on the checkout coordinator would complete that.

---

## Authenticated service-to-service events (authorization matrix + HMAC-SHA256)

Nothing on the broker is trusted because it arrived on the right queue. Every integration event
belongs to a **stream** with one **issuer**, a fixed **audience**, and a **shared secret key** that
only those services hold (`shared/security/streams.ts`):

| stream | issuer | audience (holders of the key) |
|---|---|---|
| `hyperpay.payment.result` | hyperpay-gateway | billing-credits |
| `checkout.payment.completed` | checkout-coordinator | billing-credits |
| `billing.order-credits.settled` | billing-credits | wallet-ledger |
| `billing.settlement-fee.calculated` | billing-credits | wallet-ledger |
| `ledger.payment.settled` | wallet-ledger | zatca-tax, checkout-coordinator |
| `ledger.payment.released` | wallet-ledger | checkout-coordinator |
| `ledger.merchant-settlement.collected` | wallet-ledger | billing-credits |
| `ledger.merchant-settlement.posted` | wallet-ledger | zatca-tax, file-cache-engine |
| `zatca.invoice.reported` | zatca-tax | file-cache-engine |
| `zatca.settlement-documents.reported` | zatca-tax | file-cache-engine, wallet-ledger |
| `hyperpay.refund.processed` | hyperpay-gateway | billing-credits |
| `billing.order-credits.restored` | billing-credits | wallet-ledger |
| `ledger.payment.refunded` | wallet-ledger | zatca-tax |
| `zatca.credit-note.reported` | zatca-tax | file-cache-engine |
| `ledger.invariant` | wallet-ledger | wallet-ledger (its own reconciliation bus) |

Every publish (the outbox relays, HyperPay's own outbox publisher and the ledger's invariant bus)
carries the signature headers:

```
x-signature-alg   HMAC-SHA256
x-key-id          ledger.payment.settled@v1          which shared key signed it
x-event-stream    ledger.payment.settled
x-event-issuer    wallet-ledger
x-event-audience  checkout-coordinator,zatca-tax     who may consume it
x-signed-at       2026-11-15T18:00:00.000Z
x-signature       base64url(HMAC-SHA256(key, canonical))
```

The canonical string is `version, algorithm, key id, stream, issuer, audience, event type, event id,
signed-at, sha256(body)`, so no header above and no byte of the body can change in flight.

A consumer accepts a delivery only if: it is signed; the key id names a key **this** service holds;
that key belongs to the stream in the headers and the event type really belongs to that stream; the
issuer is the stream's issuer; this service is in the audience header **and** in the matrix; the
HMAC verifies (constant-time); and the signature is not dated into the future beyond the allowed
skew (and not older than `EVENT_SIGNATURE_MAX_AGE_MS`, if one is set). Otherwise the delivery is
dead-lettered immediately — no retries burned — with a reason like `UNTRUSTED_EVENT: UNKNOWN_KEY_ID`,
`UNSIGNED_EVENT`, `SIGNATURE_INVALID`, `AUDIENCE_DENIED`, `EVENT_TYPE_NOT_IN_STREAM`. The checks run
**before** the body is parsed, so a forged message never reaches a handler. `npm run cluster:doctor`
lists anything parked that way, and the topology report (`npm run cluster:topology`, also printed by
the supervisor) prints the whole matrix with its key ids.

- **Adding a consumer to a stream is two steps**: put the service in the stream's audience, and hand
  it that stream's key. Without the key it cannot accept the event even if someone binds its queue
  to the exchange (there is a test for exactly that).
- **Key distribution.** In production each service gets only its own keys, as JSON in
  `EVENT_SIGNING_KEYS` (`[{ "keyId": "ledger.payment.settled@v2", "streamId": "…", "secret": "…" }]`),
  typically from a secrets manager. In the sandbox, keys are derived deterministically from one
  master secret (`EVENT_SIGNING_SECRET`), and each service still derives only the keys the matrix
  entitles it to.
- **Rotation.** Key ids are `<stream>@v<n>`. Verifiers accept every version they hold, issuers sign
  with the newest: ship `@v2` everywhere, then let the issuer flip to it, then drop `@v1`.
- **Rollout.** `EVENT_SIGNATURE_MODE=permissive` accepts unsigned events (and still rejects bad
  signatures) while producers are being upgraded; `enforce` (default) requires a signature; `off`
  disables the scheme.
- **Retries and DLQ replays keep the headers**, so a message replayed days later still verifies
  (the age check is off by default for that reason).

---

## Repository layout

- **`shared/`** — `security/`: the event authorization matrix (`streams.ts`), the per-service keyrings (`keys.ts`) and the HMAC-SHA256 signer / verifier (`eventAuth.ts`). `messaging/`: `ReliableConsumer` (per-consumer work queue, jittered retry tiers, DLQ, inspect / replay, reconnect), `OutboxRelay` + `SqlOutboxStore`, checkpoints, `RecoveringConnection`, broker driver. `contracts/`: event envelope, routing keys, exchanges, consumer bindings, payload schemas (zod), order context, VAT / totals arithmetic.
- **`services/hyperpay-gateway`** — Fastify webhook & vault gateway (`src/server.ts`, `src/services/paymentWebhook.ts`), AES-256-GCM vault (`src/services/vault.ts`), HMAC webhook signatures (`src/security/webhookSignature.ts`), circuit breaker, JSON-DB outbox (`src/services/outbox.ts`, `src/messaging/outboxPublisher.ts`), self-healing topology (`src/messaging/topology.ts`), runtime (`src/messaging/runtime.ts`).
- **`services/wallet-ledger`** — double-entry integer ledger with sharded system accounts and DB-enforced invariants (`src/db/schema.sql`), transfer saga / payouts / audit / reconciliation, card payment settlement (`src/services/PaymentSettlementService.ts`), merchant settlement batches (`src/services/MerchantSettlementService.ts`), integration runtime (`src/integration/runtime.ts`), JWT RBAC (`src/auth/`).
- **`services/billing-credits`** — striped FIFO credit wallet, hierarchical pools, stream holds (`src/services/StreamHoldService.ts`), order credit settlement and settlement fees (`src/integration/`, tiered pricing in `src/services/SettlementFeeCalculator.ts`), Stripe webhooks, auto-recharge outbox.
- **`services/zatca-tax`** — invoices, integer halala VAT engine, UBL 2.1, ECDSA + ICV/PIH chain, Fatoora reporting and clearance, PDF (`src/services/`), order invoicing and settlement documents (`src/integration/`).
- **`services/checkout-coordinator`** — split-tender saga (`src/services/CheckoutCoordinator.ts`), HTTP clients with retries and jitter (`src/clients/`), checkout API, outbox + ledger-outcome consumer (`src/integration/runtime.ts`).
- **`services/file-cache-engine`** — SLRU + TinyLFU memory tier over a crash-safe filesystem store, resilience, rate limiting, telemetry, invoice PDF worker (`src/integration/invoice-pdf-worker.ts`), settlement statements and documents with the stampede-free read API (`src/integration/settlement-statements.ts`, `settlement-http.ts`).
- **`scripts/`** — the bootstrap: `platform.ts` (boots and wires every service on one broker; `settle`, `traceOrder`, `verifyOrderConsistency`, `traceSettlement`, `verifySettlementConsistency`), `supervisor.ts`, `topology-bootstrap.ts`, `cluster-doctor.ts`, `platform-demo.ts`, `invoice-pdf-worker.ts`.
- **`__tests__/`** — platform tests (end-to-end order flow, split tender, merchant settlement, customer refunds, event security, tooling, VAT parity) and the PostgreSQL global setup. `scripts/run-tests.ts` is the parallel test runner.

---

## Commands

```bash
npm test                  # ALL tests (every service, shared, platform) in parallel shards; npm run tests is the same
npm run test:serial       # the same tests in one vitest run (slower; handy with vitest flags)
npm run test:platform     # end-to-end flows, tooling, contracts, shared messaging
npm run test:hyperpay | test:wallet | test:billing | test:zatca | test:file-cache | test:coordinator | test:shared
npm run test:pg           # the real-PostgreSQL suite only
npm run check             # typecheck + lint + all tests
npm run lint
npm run typecheck
npm run build             # tsc + schema files into dist/

npm run platform:demo                 # checkout, split tender and merchant settlement end to end, with consistency reports
npm run cluster:supervise             # boot the wired cluster, print health + topology
npm run cluster:supervise -- --serve  # …and serve the HTTP APIs (ports in .env.example)
npm run cluster:topology              # declare / print the whole broker topology + the event authorization matrix
npm run cluster:doctor
npm run worker:invoice-pdfs           # standalone invoice PDF worker (multi-process deployments)
```

**How `npm test` runs.** `scripts/run-tests.ts` collects every test file (about 103 files and 1,350 tests) and groups them per service. It packs the groups into shards balanced by their measured durations (cached in `node_modules/.cache/ksa-test-timings.json`) and runs the shards as parallel vitest processes, `TEST_CONCURRENCY` at a time (default: the number of CPUs). PostgreSQL is provisioned once, with one database per shard. A file that fails is re-run once on its own: if it passes, it is reported as flaky and the run still succeeds. Set `TEST_NO_RERUN=1` to make any failure final. At the end it prints a summary per service. On 4 vCPUs the run takes about 2–2.5 minutes; `npm run test:serial` takes 5–9 minutes. With arguments (`npm test -- services/zatca-tax`), it runs a plain `vitest run` with those arguments instead.

**Real PostgreSQL in the test run.** `billing-credits` has opt-in suites that need a real PostgreSQL (row locks, deadlocks, concurrent deliveries). `npm test` provides one automatically: `TEST_DATABASE_URL` if set, otherwise a throwaway cluster started with the installed `initdb` / `pg_ctl` (removed afterwards). Without PostgreSQL those suites are skipped; `TEST_PG=off` skips them deliberately.

### Operating the flow

- `GET /credits/orders/:orderId/settlement` (billing), `GET /payments/orders/:orderId/settlement` (wallet, admin), `GET /api/v1/orders/:orderId/invoicing` (ZATCA): what each service did for an order, with its checkpoints. For a settlement batch, use `GET /settlements/batches/:batchId` (wallet), `GET /billing/settlement-fees/batches/:batchId` and `GET /api/v1/settlements/:batchId/documents`.
- A parked message: `consumer.inspectDeadLetters()` shows the reason; fix the cause and `consumer.replayDeadLetters()` — the message returns to that consumer only, with a fresh attempt budget; every other service has already done its part and is not involved.
- Business-rule DLQs are deliberate stops for a human (e.g. `ORDER_ALREADY_CAPTURED` / `ORDER_ALREADY_SETTLED`: a second successful payment for a paid order needs a refund; `AMOUNT_MISMATCH`: the checkout charged a price that does not match the cart and the captured credits). Upstream steps that already completed are not compensated automatically.

### Notes and limits

- The HyperPay README describes components that are not part of this repository (OPPWA client, payment processor, consumers, admin API). The webhook therefore trusts the **signed** notification's payment resource instead of re-querying OPPWA.
- The hyperpay outbox lives in a JSON file database (single process). Billing, wallet and ZATCA outbox relays use `FOR UPDATE SKIP LOCKED` leases and are safe to run in several instances.
- Per-message TTL retry tiers: RabbitMQ expires messages at the head of a queue; one tier per attempt keeps delays within a tier within the jitter window.
