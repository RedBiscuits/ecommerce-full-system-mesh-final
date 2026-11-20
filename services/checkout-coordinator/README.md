# Checkout Coordinator (`checkout-coordinator`)

Split-tender checkouts: part of an order is paid with **promotional credits** (billing-credits), the
remainder with the customer's **card** (HyperPay) or **wallet** (wallet-ledger). The coordinator
reserves both tenders as one unit. Either both reservations exist, or neither does.

```
POST /checkouts ─► RESERVING ─┬─ credits held at billing   (hold key chk:<id>:credits)
                              └─ remainder held at ledger  (hold key chk:<id>:remainder)
                                   │ both placed                     │ any refusal / unavailability
                                   ▼                                 ▼
                               RESERVED                         COMPENSATING ─► FAILED (same failure code)
          CARD: pay at HyperPay with customParameters.checkoutId     (both released; keys tombstoned)
          WALLET: POST /checkouts/:id/confirm ─► CONFIRMED
                                   │ order pipeline (RabbitMQ)
                                   ▼
          billing captures the credits → ledger captures the remainder and posts the 3-leg journal
          → ZATCA invoice with the promotion as a per-VAT-category allowance → cached PDF
                                   │ ledger.payment.settled / released
                                   ▼
                      COMPLETED  |  PAYMENT_FAILED (both released by the pipeline)

cancel → CANCELLED · not paid before expiresAt → EXPIRED · both release every reservation
```

## How it holds together

| Concern | How |
|---|---|
| Idempotency | `checkoutId` (or the `Idempotency-Key` header) is the key. A retry with the same request returns the same checkout (`200`). Reusing the key for a different request returns `409 IDEMPOTENCY_KEY_REUSED`. Each downstream reservation has a **deterministic hold key** (`checkoutHoldKey(id, 'credits' | 'remainder')`), so a call that timed out after it committed can be retried. The service recognises the key and does not reserve twice. |
| Atomicity | It is a saga: reserve credits, then reserve the remainder. If either is refused, the coordinator releases whatever was already reserved. The checkout is then marked `FAILED` and both tenders report the failure code of the refusal (e.g. `INSUFFICIENT_FUNDS`). |
| No orphaned reservations | A release for a key that was never placed writes a **tombstone** (billing: `order_credit_hold_keys`; ledger: `payment_holds` in RELEASED). If the placement arrives late, it is refused (`HOLD_CANCELLED`), so a slow request cannot bring a reservation back to life after compensation. |
| Crashes | Every transition is written to `checkouts` / `checkout_steps` before the next call. Work runs under a lease, and the recovery sweep (`recoverStuck`, on a timer and `POST /checkouts/maintenance`) resumes `RESERVING` / `COMPENSATING` checkouts whose lease has expired. Releases are retried until both are confirmed. |
| Expiry | Unpaid checkouts expire after `CHECKOUT_RESERVATION_TTL_SECONDS` and both reservations are released. The credit hold outlives the checkout by `CHECKOUT_CREDIT_HOLD_TTL_MARGIN_SECONDS`, so billing never expires it first. |
| Race with the capture | If a cancellation reaches a reservation that was already captured, it backs off. The checkout is marked `CONFIRMED` with `CANCELLED_TOO_LATE` and then completes normally. |
| Exact amounts | The remainder is priced with the shared `computeOrderTotals`, the same integer arithmetic ZATCA uses. The allowance is split across VAT categories by the largest-remainder method, and the per-category VAT reduction uses banker's rounding. The coordinator refuses (`ALLOWANCE_MISMATCH`) if billing values the credits differently. |
| Hand-off | Confirming a WALLET checkout writes `checkout.payment.completed` (exchange `checkout.events`) to the coordinator's transactional outbox. Billing consumes it exactly as it consumes a HyperPay `PAYMENT_COMPLETED`. The coordinator closes checkouts from `ledger.payment.settled / released` (queue `checkout.ledger-outcomes`, checkpointed). |

## API (port 3105)

| Method | Path | |
|---|---|---|
| `POST` | `/checkouts` | `{ checkoutId, orderId, merchantId?, customer, lines, promotion?: { orgId, credits }, remainder: { tender: 'CARD' } \| { tender: 'WALLET', walletId } }` → `201` reserved · `200` replay · `422` refused (both released) · `503` a service was unavailable · `409` key reused / order has an open checkout |
| `GET` | `/checkouts/:id` | The checkout, its holds and every step |
| `POST` | `/checkouts/:id/confirm` | WALLET remainder: hand the checkout to the order pipeline (`202`). A CARD remainder is paid at HyperPay instead (`409 PAY_REMAINDER_AT_GATEWAY`). |
| `POST` | `/checkouts/:id/cancel` | Release both reservations (idempotent) |
| `POST` | `/checkouts/maintenance` | Run the expiry and recovery sweeps now |

## Configuration

`PORT` (3105), `BILLING_CREDITS_URL`, `WALLET_LEDGER_URL`, `WALLET_SERVICE_TOKEN` (a JWT for the
`checkout_coordinator` role at the wallet ledger), `ORDER_CREDIT_HALALAS_PER_CREDIT` (must match
billing's value), `CHECKOUT_RESERVATION_TTL_SECONDS`, `CHECKOUT_CREDIT_HOLD_TTL_MARGIN_SECONDS`,
`CHECKOUT_DOWNSTREAM_MAX_ATTEMPTS` / `_BASE_DELAY_MS` / `_MAX_DELAY_MS` / `_TIMEOUT_MS` (jittered
retries of unavailable services), `CHECKOUT_LEASE_MS`, `CHECKOUT_MAINTENANCE_INTERVAL_MS`. See the
root `.env.example`.

In the platform (`scripts/platform.ts`), the coordinator calls billing and the ledger through
Fastify `inject` with a service JWT it mints itself. `npm run cluster:supervise -- --serve` serves it
on 3105.

## Tests

`npm run test:coordinator` runs the unit tests: saga, compensation, idempotency, crash recovery,
expiry, races and the HTTP mapping, all against fake ports. `__tests__/platform-split-tender.test.ts`
at the root runs end to end through every service, covering wallet / card / declined / insufficient
funds / concurrent retries / timeouts / cancel / expiry, plus a conservation property over many
mixed-VAT checkouts.

### Event authentication

Every event this service publishes is signed with its stream's shared key (HMAC-SHA256,
`x-key-id` / `x-event-stream` / `x-event-issuer` / `x-event-audience` / `x-signature`), and every
delivery it consumes is verified against the authorization matrix before the body is parsed: wrong
key, wrong issuer, wrong audience or a tampered byte means the message is dead-lettered at once
(`UNTRUSTED_EVENT`). See `shared/security/streams.ts` and the root README.
