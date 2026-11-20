/**
 * The self-healing broker connection is shared by every service on the
 * platform (wallet event bus, integration consumers, outbox relays), so it
 * lives in `shared/messaging`. Re-exported here to keep this module's public
 * surface unchanged.
 */
export * from '../../../../shared/messaging/RecoveringConnection';
