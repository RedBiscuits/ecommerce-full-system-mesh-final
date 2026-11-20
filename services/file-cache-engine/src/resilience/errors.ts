import { CacheError, CacheIntegrityError } from '../cache/errors';

/**
 * Coarse classification of a failure, used to pick a recovery strategy:
 *
 * - `transient`   – worth retrying after a short backoff (EMFILE, EBUSY, …)
 * - `capacity`    – disk or quota exhausted; reclaim space, then retry once
 * - `permission`  – read-only or forbidden; retrying will not help
 * - `missing`     – a path vanished underneath us; recreate and retry
 * - `corruption`  – stored data failed validation; quarantine it
 * - `unknown`     – anything else; treated as non-retryable
 */
export type FailureKind =
  | 'transient'
  | 'capacity'
  | 'permission'
  | 'missing'
  | 'corruption'
  | 'unknown';

const TRANSIENT_CODES: ReadonlySet<string> = new Set([
  'EAGAIN',
  'EWOULDBLOCK',
  'EBUSY',
  'EMFILE',
  'ENFILE',
  'EINTR',
  'ETIMEDOUT',
  'ETXTBSY',
  'ESTALE',
  'ENOMEM',
]);
const CAPACITY_CODES: ReadonlySet<string> = new Set(['ENOSPC', 'EDQUOT']);
const PERMISSION_CODES: ReadonlySet<string> = new Set([
  'EACCES',
  'EPERM',
  'EROFS',
]);
const MISSING_CODES: ReadonlySet<string> = new Set(['ENOENT', 'ENOTDIR']);

/** Extracts the `code` of a Node system error, if any. */
export function errnoOf(error: unknown): string | undefined {
  if (
    typeof error === 'object' &&
    error !== null &&
    'code' in error &&
    typeof error.code === 'string'
  ) {
    return error.code;
  }
  return undefined;
}

export function isErrno(error: unknown, ...codes: readonly string[]): boolean {
  const code = errnoOf(error);
  return code !== undefined && codes.includes(code);
}

export function classifyFailure(error: unknown): FailureKind {
  if (error instanceof CacheIntegrityError) {
    return 'corruption';
  }
  const code = errnoOf(error);
  if (code === undefined) {
    return 'unknown';
  }
  if (TRANSIENT_CODES.has(code)) {
    return 'transient';
  }
  if (CAPACITY_CODES.has(code)) {
    return 'capacity';
  }
  if (PERMISSION_CODES.has(code)) {
    return 'permission';
  }
  if (MISSING_CODES.has(code)) {
    return 'missing';
  }
  return 'unknown';
}

/**
 * Whether `error` says the storage itself is unhealthy (EIO, ENOSPC, EROFS,
 * exhausted retries, an open lower-level circuit), as opposed to a problem
 * with one call: an invalid key, an oversized entry, or one corrupt file.
 * Only the former should trip a circuit that cuts the storage off for
 * everyone. Follows the `cause` chain, since decorators wrap errors.
 */
export function isStorageFailure(error: unknown): boolean {
  let current: unknown = error;
  for (let depth = 0; depth < 8 && current !== undefined; depth++) {
    if (current instanceof CacheIntegrityError) {
      return false;
    }
    if (
      current instanceof StoreUnavailableError ||
      current instanceof CircuitOpenError ||
      errnoOf(current) !== undefined
    ) {
      return true;
    }
    current = current instanceof Error ? current.cause : undefined;
  }
  return false;
}

/** Raised when a store operation failed after every recovery strategy. */
export class StoreUnavailableError extends CacheError {
  constructor(
    readonly operation: string,
    readonly kind: FailureKind,
    readonly attempts: number,
    options?: ErrorOptions,
  ) {
    super(
      `Cache store operation "${operation}" failed (${kind}) after ${attempts} attempt(s)`,
      options,
    );
  }
}

/** Raised when a call is rejected because the circuit is open. */
export class CircuitOpenError extends CacheError {
  constructor(
    readonly circuit: string,
    readonly retryAfterMs: number,
  ) {
    super(
      `Circuit "${circuit}" is open; retry in ~${Math.ceil(retryAfterMs)} ms`,
    );
  }
}

/** Raised when a lock could not be obtained in time. */
export class LockTimeoutError extends CacheError {
  constructor(
    readonly key: string,
    readonly timeoutMs: number,
  ) {
    super(`Timed out after ${timeoutMs} ms waiting for lock "${key}"`);
  }
}

/** Raised when a held lock was broken by another party (stale-lock recovery). */
export class LockLostError extends CacheError {
  constructor(readonly path: string) {
    super(`Lock "${path}" is no longer held by this process`);
  }
}

/** Raised when a loader did not settle within its time budget. */
export class LoaderTimeoutError extends CacheError {
  constructor(
    readonly key: string,
    readonly timeoutMs: number,
  ) {
    super(`Loader for "${key}" did not complete within ${timeoutMs} ms`);
  }
}
