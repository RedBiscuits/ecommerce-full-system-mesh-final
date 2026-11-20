/**
 * src/types/hyperpay.ts
 *
 * Strict TypeScript interfaces that mirror the HyperPay / OPPWA API contracts.
 * Keep these in sync with the official OPPWA documentation:
 * https://developer.hyperpay.com/reference
 */

// ─── OPPWA Response ───────────────────────────────────────────────────────────

export interface OppwaResult {
  /** Numeric result code, e.g. "000.100.110" */
  code: string;
  /** Human-readable description of the result code */
  description: string;
}

export interface OppwaCard {
  bin: string;
  last4Digits: string;
  holder: string;
  expiryMonth: string;
  expiryYear: string;
  type: string;
}

export interface OppwaCustomer {
  ip?: string;
  email?: string;
  givenName?: string;
  surname?: string;
}

/** Full payment resource returned by GET /v1/checkouts/{checkoutId}/payment */
export interface OppwaPaymentResource {
  id: string;
  paymentType: string;
  paymentBrand?: string;
  amount: string;
  currency: string;
  descriptor?: string;
  merchantTransactionId?: string;
  result: OppwaResult;
  card?: OppwaCard;
  customer?: OppwaCustomer;
  /**
   * Tokenisation: present when the checkout was created with
   * `createRegistration=true`. This token can charge the stored card again
   * (POST /v1/registrations/{registrationId}/payments) — treat it as a secret.
   */
  registrationId?: string;
  /** For back-office operations (CP/RF/RV): the payment this one references. */
  referencedId?: string;
  /** ISO-8601 timestamp */
  timestamp: string;
  ndc: string;
}

// ─── Operations on stored cards / previous payments ──────────────────────────

/** Operations that charge a vaulted card through its registration token. */
export type RegistrationOperation = 'DB' | 'PA';
/** Operations that reference an earlier payment id. */
export type ReferencedOperation = 'CP' | 'RF' | 'RV';
export type PaymentOperation = RegistrationOperation | ReferencedOperation;

export const REGISTRATION_OPERATIONS: readonly RegistrationOperation[] = ['DB', 'PA'];
export const REFERENCED_OPERATIONS: readonly ReferencedOperation[] = ['CP', 'RF', 'RV'];

/** OPPWA standing-instruction type for merchant-initiated charges on a stored card. */
export type StandingInstructionType = 'UNSCHEDULED' | 'RECURRING';

// ─── Transaction lifecycle ────────────────────────────────────────────────────

/**
 * Where a root transaction stands, derived from it and its child operations:
 *
 *   PENDING              gateway has not settled the root yet
 *   FAILED               root was declined / rejected
 *   AUTHORIZED           PA approved, nothing captured yet
 *   PARTIALLY_CAPTURED   PA with some, but not all, of the amount captured
 *   CAPTURED             PA fully captured
 *   DEBITED              DB approved (money taken in one step)
 *   PARTIALLY_REFUNDED   some of the settled amount refunded
 *   REFUNDED             all of the settled amount refunded
 *   REVERSED             PA reversed (hold released) before any capture
 *   VOIDED               DB / capture cancelled before settlement
 */
export type TransactionLifecycle =
  | 'PENDING'
  | 'FAILED'
  | 'AUTHORIZED'
  | 'PARTIALLY_CAPTURED'
  | 'CAPTURED'
  | 'DEBITED'
  | 'PARTIALLY_REFUNDED'
  | 'REFUNDED'
  | 'REVERSED'
  | 'VOIDED';

/**
 * The payment resource with everything that belongs in the vault removed.
 * This is the only shape that may be persisted or returned to callers —
 * the compiler enforces that `card` and `registrationId` cannot be present.
 */
export type SafeOppwaPaymentResource = Omit<OppwaPaymentResource, 'card' | 'registrationId'>;

// ─── Tokenisation / vault ─────────────────────────────────────────────────────

/**
 * Cardholder data that is only ever stored encrypted.
 * Card fields are optional: some brands (wallets, bank transfers) tokenise
 * without a card block, and the token must be vaulted regardless.
 */
export interface VaultedPaymentMethod {
  /** OPPWA registration token — can initiate new charges. */
  registrationId: string;
  paymentBrand: string;
  last4Digits?: string;
  expiryMonth?: string;
  expiryYear?: string;
  /** Cardholder name, when OPPWA returns it. */
  holder?: string;
}

/** What the vault exposes without revealing the token. */
export interface MaskedPaymentMethod {
  vaultId: string;
  paymentBrand: string;
  last4Digits?: string;
  expiryMonth?: string;
  expiryYear?: string;
  /** Transaction that first produced the token. */
  transactionId: string;
  createdAt: string;
}

// ─── Webhook Payload ──────────────────────────────────────────────────────────

/**
 * Minimal shape of the JSON body HyperPay POSTs to your webhook endpoint.
 * HyperPay sends either `resourcePath` or `id` (or both).
 */
export interface HyperPayWebhookPayload {
  /** e.g. "/v1/checkouts/ABC123/payment" */
  resourcePath?: string;
  /** Payment / checkout ID */
  id?: string;
  /** Raw result code included in some webhook flavours */
  result?: OppwaResult;
  [key: string]: unknown;
}

// ─── Internal service results ─────────────────────────────────────────────────

export type TransactionStatus = 'SUCCESS' | 'PENDING' | 'FAILED';

export interface TransactionResult {
  status: TransactionStatus;
  transactionId: string;
  amount: string;
  currency: string;
  resultCode: string;
  resultDescription: string;
  raw: OppwaPaymentResource;
}

/** TransactionResult as handed out by the payment processor — card data already vaulted. */
export type SafeTransactionResult = Omit<TransactionResult, 'raw'> & { raw: SafeOppwaPaymentResource };

// ─── Persisted Payment Record ─────────────────────────────────────────────────

/**
 * Shape stored in the JSON DB under the "payments" collection.
 *
 * Exactly one record exists per payment. Repeated verification requests for
 * the same payment (webhook retries, AMQP redeliveries, duplicate publishes)
 * are de-duplicated via `idempotencyKey` and `transactionId` — see
 * `services/paymentProcessor.ts`.
 */
export interface PaymentRecord {
  /**
   * Canonical OPPWA resource path the verification was requested for,
   * e.g. "/v1/checkouts/ABC123/payment". Deterministic for a given payment,
   * so any retry of the same event maps to the same record.
   */
  idempotencyKey: string;
  transactionId: string;
  status: TransactionStatus;
  amount: string;
  currency: string;
  paymentBrand?: string;
  resultCode: string;
  resultDescription: string;
  /** Email from OPPWA customer object, if present */
  customerEmail?: string;
  /** OPPWA-reported transaction timestamp (ISO-8601), for auditing */
  rawTimestamp: string;
  /**
   * Reference to the encrypted payment method in the vault, when the payment
   * carried a registration token. The token itself is never stored here.
   */
  vaultId?: string;
  /**
   * The OPPWA payment resource as last fetched, with card data and the
   * registration token stripped. Lets a replayed request return a complete
   * result without re-querying the gateway.
   */
  raw: SafeOppwaPaymentResource;
  /** Merchant order id (OPPWA `merchantTransactionId`) for checkout payments. */
  orderId?: string;
  /** eventId of the last PAYMENT_COMPLETED event recorded for this payment. */
  lastEventId?: string;
  /**
   * A later delivery reported a different FINAL outcome than the one recorded
   * (e.g. FAILED after SUCCESS). The recorded outcome is kept and no event is
   * emitted: such a contradiction needs a human (refund / reconciliation).
   */
  statusConflicts?: Array<{ reportedStatus: TransactionStatus; resultCode: string; receivedAt: string }>;
}
