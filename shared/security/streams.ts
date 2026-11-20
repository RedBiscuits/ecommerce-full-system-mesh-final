/**
 * Service-to-service authorization matrix for the platform's broker traffic.
 *
 * Every integration event belongs to exactly one **stream**. A stream names its
 * **issuer** (the only service allowed to publish it) and its **audience** (the only
 * services allowed to consume it), and it is protected by a **shared secret key**: the issuer
 * signs with it, and only the services in the audience hold it, so a service that is not in the
 * audience cannot verify (and therefore cannot accept) the event even if a misconfigured binding
 * delivers it. Adding a consumer to a stream is therefore two steps: put it in the audience here,
 * and hand it the stream's key.
 *
 *   stream                                  issuer                audience
 *   hyperpay.payment.result                 hyperpay-gateway      billing-credits
 *   checkout.payment.completed              checkout-coordinator  billing-credits
 *   billing.order-credits.settled           billing-credits       wallet-ledger
 *   billing.settlement-fee.calculated       billing-credits       wallet-ledger
 *   ledger.payment.settled                  wallet-ledger         zatca-tax, checkout-coordinator
 *   ledger.payment.released                 wallet-ledger         checkout-coordinator
 *   ledger.merchant-settlement.collected    wallet-ledger         billing-credits
 *   ledger.merchant-settlement.posted       wallet-ledger         zatca-tax, file-cache-engine
 *   zatca.invoice.reported                  zatca-tax             file-cache-engine
 *   zatca.settlement-documents.reported     zatca-tax             file-cache-engine, wallet-ledger
 *   hyperpay.refund.processed               hyperpay-gateway      billing-credits
 *   billing.order-credits.restored          billing-credits       wallet-ledger
 *   ledger.payment.refunded                 wallet-ledger         zatca-tax
 *   zatca.credit-note.reported              zatca-tax             file-cache-engine
 *   ledger.invariant                        wallet-ledger         wallet-ledger (its own reconciliation bus)
 */
import { CONSUMERS, EVENT_TYPES, EXCHANGES, PRODUCERS, ROUTING_KEYS } from '../contracts';

export const SERVICES = {
  hyperpay: PRODUCERS.hyperpay,
  billing: PRODUCERS.billing,
  ledger: PRODUCERS.ledger,
  zatca: PRODUCERS.zatca,
  checkout: PRODUCERS.checkout,
  fileCache: 'file-cache-engine',
} as const;

export type ServiceId = (typeof SERVICES)[keyof typeof SERVICES];

export interface EventStream {
  /** Stable id; also the base of its key ids (`<stream>@v<n>`). */
  id: string;
  /** The only service allowed to publish (and sign) events of this stream. */
  issuer: ServiceId;
  /** The only services allowed to consume it; they are the ones holding the key. */
  audience: readonly ServiceId[];
  /** Event types carried by the stream (exact match), and/or a routing-key prefix. */
  eventTypes: readonly string[];
  prefix?: string;
  exchange: string;
  routingKeys: readonly string[];
}

/** Routing key of the ledger's internal invariant events (wallet drift, …). */
export const LEDGER_INVARIANT_PREFIX = 'ledger.invariant.';

export const EVENT_STREAMS: Record<string, EventStream> = {
  'hyperpay.payment.result': {
    id: 'hyperpay.payment.result',
    issuer: SERVICES.hyperpay,
    audience: [SERVICES.billing],
    eventTypes: [EVENT_TYPES.paymentCompleted],
    exchange: EXCHANGES.hyperpay.name,
    routingKeys: [ROUTING_KEYS.paymentResult],
  },
  'checkout.payment.completed': {
    id: 'checkout.payment.completed',
    issuer: SERVICES.checkout,
    audience: [SERVICES.billing],
    eventTypes: [EVENT_TYPES.checkoutPaymentCompleted],
    exchange: EXCHANGES.checkout.name,
    routingKeys: [ROUTING_KEYS.checkoutPaymentCompleted],
  },
  'billing.order-credits.settled': {
    id: 'billing.order-credits.settled',
    issuer: SERVICES.billing,
    audience: [SERVICES.ledger],
    eventTypes: [EVENT_TYPES.orderCreditsSettled],
    exchange: EXCHANGES.billing.name,
    routingKeys: [ROUTING_KEYS.orderCreditsSettled],
  },
  'billing.settlement-fee.calculated': {
    id: 'billing.settlement-fee.calculated',
    issuer: SERVICES.billing,
    audience: [SERVICES.ledger],
    eventTypes: [EVENT_TYPES.settlementFeeCalculated],
    exchange: EXCHANGES.billing.name,
    routingKeys: [ROUTING_KEYS.settlementFeeCalculated],
  },
  'ledger.payment.settled': {
    id: 'ledger.payment.settled',
    issuer: SERVICES.ledger,
    audience: [SERVICES.zatca, SERVICES.checkout],
    eventTypes: [EVENT_TYPES.ledgerPaymentSettled],
    exchange: EXCHANGES.ledger.name,
    routingKeys: [ROUTING_KEYS.ledgerPaymentSettled],
  },
  'ledger.payment.released': {
    id: 'ledger.payment.released',
    issuer: SERVICES.ledger,
    audience: [SERVICES.checkout],
    eventTypes: [EVENT_TYPES.ledgerPaymentReleased],
    exchange: EXCHANGES.ledger.name,
    routingKeys: [ROUTING_KEYS.ledgerPaymentReleased],
  },
  'ledger.merchant-settlement.collected': {
    id: 'ledger.merchant-settlement.collected',
    issuer: SERVICES.ledger,
    audience: [SERVICES.billing],
    eventTypes: [EVENT_TYPES.merchantSettlementCollected],
    exchange: EXCHANGES.ledger.name,
    routingKeys: [ROUTING_KEYS.merchantSettlementCollected],
  },
  'ledger.merchant-settlement.posted': {
    id: 'ledger.merchant-settlement.posted',
    issuer: SERVICES.ledger,
    audience: [SERVICES.zatca, SERVICES.fileCache],
    eventTypes: [EVENT_TYPES.merchantSettlementPosted],
    exchange: EXCHANGES.ledger.name,
    routingKeys: [ROUTING_KEYS.merchantSettlementPosted],
  },
  'zatca.invoice.reported': {
    id: 'zatca.invoice.reported',
    issuer: SERVICES.zatca,
    audience: [SERVICES.fileCache],
    eventTypes: [EVENT_TYPES.invoiceReported],
    exchange: EXCHANGES.zatca.name,
    routingKeys: [ROUTING_KEYS.invoiceReported],
  },
  'zatca.settlement-documents.reported': {
    id: 'zatca.settlement-documents.reported',
    issuer: SERVICES.zatca,
    audience: [SERVICES.fileCache, SERVICES.ledger],
    eventTypes: [EVENT_TYPES.settlementDocumentsReported],
    exchange: EXCHANGES.zatca.name,
    routingKeys: [ROUTING_KEYS.settlementDocumentsReported],
  },
  'hyperpay.refund.processed': {
    id: 'hyperpay.refund.processed',
    issuer: SERVICES.hyperpay,
    audience: [SERVICES.billing],
    eventTypes: [EVENT_TYPES.refundProcessed],
    exchange: EXCHANGES.hyperpay.name,
    routingKeys: [ROUTING_KEYS.refundProcessed],
  },
  'billing.order-credits.restored': {
    id: 'billing.order-credits.restored',
    issuer: SERVICES.billing,
    audience: [SERVICES.ledger],
    eventTypes: [EVENT_TYPES.orderCreditsRestored],
    exchange: EXCHANGES.billing.name,
    routingKeys: [ROUTING_KEYS.orderCreditsRestored],
  },
  'ledger.payment.refunded': {
    id: 'ledger.payment.refunded',
    issuer: SERVICES.ledger,
    audience: [SERVICES.zatca],
    eventTypes: [EVENT_TYPES.ledgerPaymentRefunded],
    exchange: EXCHANGES.ledger.name,
    routingKeys: [ROUTING_KEYS.ledgerPaymentRefunded],
  },
  'zatca.credit-note.reported': {
    id: 'zatca.credit-note.reported',
    issuer: SERVICES.zatca,
    audience: [SERVICES.fileCache],
    eventTypes: [EVENT_TYPES.creditNoteReported],
    exchange: EXCHANGES.zatca.name,
    routingKeys: [ROUTING_KEYS.creditNoteReported],
  },
  'ledger.invariant': {
    id: 'ledger.invariant',
    issuer: SERVICES.ledger,
    audience: [SERVICES.ledger],
    eventTypes: [],
    prefix: LEDGER_INVARIANT_PREFIX,
    exchange: EXCHANGES.ledger.name,
    routingKeys: [`${LEDGER_INVARIANT_PREFIX}#`],
  },
};

/** Which service owns (and therefore verifies for) each integration consumer queue. */
export const CONSUMER_SERVICE: Record<string, ServiceId> = {
  [CONSUMERS.billingPaymentResults]: SERVICES.billing,
  [CONSUMERS.billingSettlementFees]: SERVICES.billing,
  [CONSUMERS.ledgerOrderSettlements]: SERVICES.ledger,
  [CONSUMERS.ledgerSettlementFees]: SERVICES.ledger,
  [CONSUMERS.ledgerSettlementDocuments]: SERVICES.ledger,
  [CONSUMERS.zatcaLedgerSettlements]: SERVICES.zatca,
  [CONSUMERS.zatcaMerchantSettlements]: SERVICES.zatca,
  [CONSUMERS.fileCacheInvoicePdfs]: SERVICES.fileCache,
  [CONSUMERS.fileCacheSettlementStatements]: SERVICES.fileCache,
  [CONSUMERS.checkoutLedgerOutcomes]: SERVICES.checkout,
  [CONSUMERS.billingRefunds]: SERVICES.billing,
  [CONSUMERS.ledgerOrderRefunds]: SERVICES.ledger,
  [CONSUMERS.zatcaLedgerRefunds]: SERVICES.zatca,
  [CONSUMERS.fileCacheCreditNotes]: SERVICES.fileCache,
};

/** The stream an event type belongs to (exact match first, then the prefix streams). */
export function streamForEventType(eventType: string): EventStream | undefined {
  for (const stream of Object.values(EVENT_STREAMS)) {
    if (stream.eventTypes.includes(eventType)) return stream;
  }
  return Object.values(EVENT_STREAMS).find((s) => s.prefix !== undefined && eventType.startsWith(s.prefix));
}

export function streamById(streamId: string): EventStream | undefined {
  return EVENT_STREAMS[streamId];
}

/** Streams a service may publish. */
export function streamsIssuedBy(service: ServiceId): EventStream[] {
  return Object.values(EVENT_STREAMS).filter((s) => s.issuer === service);
}

/** Streams a service may consume (and therefore holds the key of). */
export function streamsAudiencedBy(service: ServiceId): EventStream[] {
  return Object.values(EVENT_STREAMS).filter((s) => s.audience.includes(service));
}

/** Every stream whose key a service holds (its own, plus the ones addressed to it). */
export function streamsFor(service: ServiceId): EventStream[] {
  const streams = new Map<string, EventStream>();
  for (const stream of [...streamsIssuedBy(service), ...streamsAudiencedBy(service)]) streams.set(stream.id, stream);
  return [...streams.values()];
}

export function isAudience(stream: EventStream, service: string): boolean {
  return stream.audience.includes(service as ServiceId);
}
