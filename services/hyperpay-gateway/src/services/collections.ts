/**
 * src/services/collections.ts
 *
 * Names of the JSON DB collections, in one place so modules that read
 * several of them (the ledger, reconciliation) don't have to import each
 * owning service.
 */

export const PAYMENTS_COLLECTION = 'payments';
export const OPERATIONS_COLLECTION = 'operations';
/** Customer refunds (OPPWA RF transactions) of captured payments — see services/refundWebhook.ts. */
export const REFUNDS_COLLECTION = 'refunds';
export const VAULT_COLLECTION = 'payment_methods';
/** Transactional outbox — domain events awaiting publication (see services/outbox.ts). */
export const OUTBOX_COLLECTION = 'outbox_events';
