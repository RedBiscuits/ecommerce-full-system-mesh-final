import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';
import { config } from '../config/env';
import { LEGACY_PRINCIPAL, Principal, hasRole } from './principal';
import { AuthenticationError, assertAuthConfigured, verifyAccessToken } from './tokens';

/**
 * Access policy every route MUST declare in `config.access`:
 *  - public:          no token needed (health checks)
 *  - authenticated:   any verified token carrying at least one known role;
 *                     resource ownership is then enforced by the service
 *  - admin:           token with the `admin` role
 *  - payout_webhook:  token with the `payout_webhook` role (bank gateway)
 *  - payment_holds:   `admin` or the `checkout_coordinator` service role (payment holds)
 */
export type AccessPolicy = 'public' | 'authenticated' | 'admin' | 'payout_webhook' | 'payment_holds';

declare module 'fastify' {
  interface FastifyRequest {
    principal?: Principal;
  }
  interface FastifyContextConfig {
    access?: AccessPolicy;
  }
}

/**
 * Install authentication + coarse authorization on the root instance. Must be
 * called BEFORE any route is registered: the onRoute hook makes an undeclared
 * access policy a startup error, so an endpoint can never ship unprotected.
 */
export function installAuth(app: FastifyInstance): void {
  assertAuthConfigured();
  app.decorateRequest('principal', undefined);

  app.addHook('onRoute', (route) => {
    const access = (route.config as { access?: AccessPolicy } | undefined)?.access;
    if (!access) {
      throw new Error(`Route ${String(route.method)} ${route.url} must declare config.access`);
    }
  });

  app.addHook('onRequest', async (request, reply) => {
    const access = request.routeOptions.config.access;
    if (!access || access === 'public') return; // unmatched routes fall through to 404

    const header = request.headers.authorization;
    if (!header) {
      if (config.auth.mode === 'legacy') {
        request.principal = LEGACY_PRINCIPAL;
        reply.header('Deprecation', 'true');
        reply.header('Warning', '299 - "Unauthenticated access is deprecated; send an Authorization: Bearer token"');
        return;
      }
      return sendUnauthorized(reply, new AuthenticationError('Authentication required', 'invalid_request'));
    }

    const match = /^Bearer\s+([A-Za-z0-9\-_]+\.[A-Za-z0-9\-_]+\.[A-Za-z0-9\-_]*)$/i.exec(header.trim());
    if (!match) {
      return sendUnauthorized(reply, new AuthenticationError('Malformed Authorization header', 'invalid_request'));
    }

    let principal: Principal;
    try {
      principal = await verifyAccessToken(match[1]);
    } catch (err) {
      request.log.info({ reason: err instanceof Error ? err.message : String(err) }, 'rejected bearer token');
      return sendUnauthorized(reply, err instanceof AuthenticationError ? err : new AuthenticationError('invalid token'));
    }

    if (!isAllowed(access, principal)) {
      return reply.status(403).send({ error: 'Insufficient role for this operation', code: 'FORBIDDEN' });
    }
    request.principal = principal;
  });
}

function isAllowed(access: Exclude<AccessPolicy, 'public'>, principal: Principal): boolean {
  switch (access) {
    case 'authenticated':
      return principal.roles.size > 0;
    case 'admin':
      return hasRole(principal, 'admin');
    case 'payout_webhook':
      return hasRole(principal, 'payout_webhook');
    case 'payment_holds':
      return hasRole(principal, 'admin') || hasRole(principal, 'checkout_coordinator');
  }
}

function sendUnauthorized(reply: FastifyReply, err: AuthenticationError): FastifyReply {
  return reply
    .status(401)
    .header('WWW-Authenticate', `Bearer realm="wallet-ledger", error="${err.bearerError}"`)
    .send({ error: 'Authentication required', code: err.code });
}

/** The authenticated caller (set by the onRequest hook for non-public routes). */
export function principalOf(request: FastifyRequest): Principal | undefined {
  return request.principal;
}
