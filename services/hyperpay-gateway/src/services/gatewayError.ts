/**
 * src/services/gatewayError.ts
 *
 * The error the OPPWA client throws, and the one question the circuit
 * breaker asks of it: "is this an outage?"
 *
 * Outage-class (counts towards opening the circuit):
 *   - no response at all: connection refused, DNS, timeout (`status` undefined)
 *   - 5xx — the gateway is up but failing
 *   - 429 — the gateway is shedding load
 *
 * Not an outage (the call completed; the circuit stays closed):
 *   - any other 4xx: wrong credentials, unknown resource, malformed request —
 *     retrying later would not help and the gateway is evidently reachable
 *   - a reply that is not a payment resource (a protocol surprise, not a fault)
 */

export class GatewayError extends Error {
  /**
   * HTTP status of the gateway's reply; absent when there was no reply.
   * Deliberately not called `status`: the Express error handler would read
   * that as *our* response status and relay the gateway's 401 / 502 to callers.
   */
  public readonly gatewayStatus?: number;

  constructor(message: string, gatewayStatus?: number) {
    super(message);
    this.name = 'GatewayError';
    if (gatewayStatus !== undefined) this.gatewayStatus = gatewayStatus;
  }
}

export function isGatewayOutage(error: unknown): boolean {
  if (!(error instanceof GatewayError)) return false;
  const { gatewayStatus } = error;
  return gatewayStatus === undefined || gatewayStatus >= 500 || gatewayStatus === 429;
}
