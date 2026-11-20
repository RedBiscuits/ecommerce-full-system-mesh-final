export type { FailureKind } from './errors';
export {
  CircuitOpenError,
  LoaderTimeoutError,
  LockLostError,
  LockTimeoutError,
  StoreUnavailableError,
  classifyFailure,
  errnoOf,
  isErrno,
  isStorageFailure,
} from './errors';
export { DEFAULT_RETRY_POLICY, backoffDelay, withRetry } from './retry';
export type { RetryOptions, RetryPolicy } from './retry';
export { CircuitBreaker } from './circuit-breaker';
export type {
  CircuitBreakerOptions,
  CircuitSnapshot,
  CircuitState,
} from './circuit-breaker';
export { FileLock } from './file-lock';
export type { FileLockOptions } from './file-lock';
export { ResilientCacheStore } from './resilient-store';
export type {
  ResilienceEvent,
  ResilientStoreOptions,
  StoreHealth,
  StoreOperation,
} from './resilient-store';
