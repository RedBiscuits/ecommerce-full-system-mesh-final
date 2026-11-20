import { createHash } from 'node:crypto';

import { InvalidCacheKeyError } from './errors';

/**
 * Upper bound on key length. Keys are hashed before touching the file system,
 * so this only guards against abuse (unbounded memory in headers).
 */
export const MAX_KEY_LENGTH = 8_192;

/** Throws {@link InvalidCacheKeyError} if `key` is unusable. */
export function assertValidKey(key: string): void {
  if (typeof key !== 'string') {
    throw new InvalidCacheKeyError('key must be a string');
  }
  if (key.length === 0) {
    throw new InvalidCacheKeyError('key must not be empty');
  }
  if (key.length > MAX_KEY_LENGTH) {
    throw new InvalidCacheKeyError(
      `key length ${key.length} exceeds ${MAX_KEY_LENGTH}`,
    );
  }
}

/**
 * Maps an arbitrary key to a fixed-length, file-system-safe identifier.
 *
 * Hashing (rather than sanitising) means any string – including URLs, paths
 * with `..`, or unicode – becomes a plain hex name, which closes off path
 * traversal by construction.
 */
export function hashKey(key: string): string {
  assertValidKey(key);
  return createHash('sha256').update(key, 'utf8').digest('hex');
}

/** Computes the payload digest persisted in an entry header. */
export function checksumOf(payload: Uint8Array): string {
  return `sha256:${createHash('sha256').update(payload).digest('hex')}`;
}
