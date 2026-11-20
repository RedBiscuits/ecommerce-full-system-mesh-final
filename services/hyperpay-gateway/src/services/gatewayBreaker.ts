/**
 * src/services/gatewayBreaker.ts
 *
 * The process-wide circuit breaker in front of the OPPWA client, wired to
 * config. `hyperpayService.ts` runs every gateway call through it; the
 * consumer and the webhook defer work to the outbox while it is open, and
 * `gatewayRecovery.ts` replays that work as the breaker's half-open probe.
 */

import { config } from '../config/env';
import { CircuitBreaker } from '../resilience/circuitBreaker';
import { isGatewayOutage } from './gatewayError';

export const gatewayBreaker = new CircuitBreaker({
  name: 'oppwa',
  ...config.gateway.breaker,
  isFailure: isGatewayOutage,
});
