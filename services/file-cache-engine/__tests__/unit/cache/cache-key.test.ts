import { describe, expect, it } from 'vitest';

import {
  MAX_KEY_LENGTH,
  assertValidKey,
  hashKey,
} from '../../../src/cache/cache-key';
import { InvalidCacheKeyError } from '../../../src/cache/errors';

describe('cache-key', () => {
  it('maps any key to a fixed-length hex name (no path traversal possible)', () => {
    const hostile = '../../../etc/passwd';
    const hash = hashKey(hostile);
    expect(hash).toMatch(/^[0-9a-f]{64}$/);
    expect(hashKey('https://example.com/a?b=c#d')).toMatch(/^[0-9a-f]{64}$/);
    expect(hashKey('\u0000weird\\..\\key')).toMatch(/^[0-9a-f]{64}$/);
  });

  it('is deterministic and collision-free for distinct inputs', () => {
    expect(hashKey('a')).toBe(hashKey('a'));
    expect(hashKey('a')).not.toBe(hashKey('b'));
  });

  it('rejects empty and oversized keys', () => {
    expect(() => assertValidKey('')).toThrow(InvalidCacheKeyError);
    expect(() => assertValidKey('x'.repeat(MAX_KEY_LENGTH + 1))).toThrow(
      InvalidCacheKeyError,
    );
    expect(() => assertValidKey('x'.repeat(MAX_KEY_LENGTH))).not.toThrow();
  });
});
