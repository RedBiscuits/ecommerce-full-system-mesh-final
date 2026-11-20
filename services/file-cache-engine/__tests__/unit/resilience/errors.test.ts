import { describe, expect, it } from 'vitest';

import {
  CacheIntegrityError,
  EntryTooLargeError,
  InvalidCacheKeyError,
} from '../../../src/cache/errors';
import {
  classifyFailure,
  errnoOf,
  isErrno,
  CircuitOpenError,
  StoreUnavailableError,
  isStorageFailure,
} from '../../../src/resilience/errors';
import { errno } from '../../helpers/faulty-store';

describe('resilience/errors', () => {
  it.each([
    ['EMFILE', 'transient'],
    ['EBUSY', 'transient'],
    ['EAGAIN', 'transient'],
    ['ENOSPC', 'capacity'],
    ['EDQUOT', 'capacity'],
    ['EROFS', 'permission'],
    ['EACCES', 'permission'],
    ['ENOENT', 'missing'],
    ['EIO', 'unknown'],
  ] as const)('classifies %s as %s', (code, kind) => {
    expect(classifyFailure(errno(code))).toBe(kind);
  });

  it('classifies integrity errors and plain errors', () => {
    expect(classifyFailure(new CacheIntegrityError('/p', 'bad'))).toBe(
      'corruption',
    );
    expect(classifyFailure(new Error('boom'))).toBe('unknown');
    expect(classifyFailure('string')).toBe('unknown');
    expect(classifyFailure(undefined)).toBe('unknown');
  });

  it('extracts errno codes defensively', () => {
    expect(errnoOf(errno('EIO'))).toBe('EIO');
    expect(errnoOf({ code: 42 })).toBeUndefined();
    expect(errnoOf(null)).toBeUndefined();
    expect(isErrno(errno('EIO'), 'ENOENT', 'EIO')).toBe(true);
    expect(isErrno(new Error('x'), 'EIO')).toBe(false);
  });
});

describe('isStorageFailure', () => {
  it('counts disk errors, exhausted retries and an open lower circuit', () => {
    expect(isStorageFailure(errno('EIO'))).toBe(true);
    expect(isStorageFailure(errno('ENOSPC'))).toBe(true);
    expect(isStorageFailure(errno('EROFS'))).toBe(true);
    expect(
      isStorageFailure(new StoreUnavailableError('write', 'unknown', 3)),
    ).toBe(true);
    expect(isStorageFailure(new CircuitOpenError('cache-store', 10))).toBe(
      true,
    );
  });

  it('follows the cause chain through decorators', () => {
    const wrapped = new Error('outer', {
      cause: new Error('middle', { cause: errno('EIO') }),
    });
    expect(isStorageFailure(wrapped)).toBe(true);
  });

  it('ignores problems with one call: bad input, one corrupt file, plain errors', () => {
    expect(isStorageFailure(new InvalidCacheKeyError('empty'))).toBe(false);
    expect(isStorageFailure(new EntryTooLargeError('k', 10, 5))).toBe(false);
    expect(isStorageFailure(new CacheIntegrityError('/x', 'bad magic'))).toBe(
      false,
    );
    expect(
      isStorageFailure(
        new Error('x', { cause: new CacheIntegrityError('/x', 'y') }),
      ),
    ).toBe(false);
    expect(isStorageFailure(new Error('boom'))).toBe(false);
    expect(isStorageFailure(undefined)).toBe(false);
  });
});
