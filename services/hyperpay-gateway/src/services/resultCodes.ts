/**
 * src/services/resultCodes.ts
 *
 * OPPWA result codes → the service's normalised status, using the patterns
 * published in the OPPWA result-code reference:
 *
 *   SUCCESS  000.000.* , 000.100.1* , 000.3* , 000.6*           (processed)
 *            000.400.0* (except 000.400.03*) , 000.400.100       (processed, review recommended)
 *   PENDING  000.200.*                                           (transaction pending)
 *            800.400.5* , 100.400.500                            (waiting for confirmation / external)
 *   FAILED   everything else (rejections, risk, validation, system errors)
 */

import type { TransactionStatus } from '../types/hyperpay';

const SUCCESS = [/^(000\.000\.|000\.100\.1|000\.[36])/, /^(000\.400\.0[^3]|000\.400\.100)/];
const PENDING = [/^(000\.200)/, /^(800\.400\.5|100\.400\.500)/];

export function statusFromResultCode(code: string): TransactionStatus {
  const trimmed = code.trim();
  if (SUCCESS.some((re) => re.test(trimmed))) return 'SUCCESS';
  if (PENDING.some((re) => re.test(trimmed))) return 'PENDING';
  return 'FAILED';
}
