/** Roles carried in the verified token's `roles` claim. */
/**
 * `checkout_coordinator`: the split-tender checkout service; may place and release payment holds
 * (including reserving a customer's wallet funds into escrow for a checkout), nothing else.
 */
export const ROLES = ['user', 'admin', 'payout_webhook', 'checkout_coordinator'] as const;
export type Role = (typeof ROLES)[number];

/**
 * Who is calling.
 *  - 'token':  a verified bearer token; `subject` is the token's `sub`, which
 *              is matched against wallets.owner_id for ownership checks.
 *  - 'legacy': an unauthenticated request accepted only in AUTH_MODE=legacy
 *              (pre-auth behaviour, kept for backward compatibility).
 */
export interface Principal {
  kind: 'token' | 'legacy';
  subject: string;
  roles: ReadonlySet<Role>;
}

export const LEGACY_PRINCIPAL: Principal = Object.freeze({
  kind: 'legacy',
  subject: 'legacy-unauthenticated',
  roles: new Set<Role>(ROLES),
});

export function hasRole(principal: Principal | undefined, role: Role): boolean {
  return principal?.roles.has(role) ?? false;
}

/**
 * IDOR guard for acting on a wallet (moving its money). A token principal may
 * only act on wallets it owns; admins are NOT exempt (least privilege: staff
 * review and audit, they don't move customers' money). `undefined` means a
 * trusted internal caller (workers, scripts, tests); legacy keeps the
 * pre-auth behaviour.
 */
export function ownsWallet(principal: Principal | undefined, ownerId: string): boolean {
  return !principal || principal.kind === 'legacy' || principal.subject === ownerId;
}

/** Read access: owners, plus admins (support/audit). */
export function canReadWallet(principal: Principal | undefined, ownerId: string): boolean {
  return ownsWallet(principal, ownerId) || hasRole(principal, 'admin');
}

/** Caller context threaded from routes into services. */
export interface ActorContext {
  actor?: Principal;
}
