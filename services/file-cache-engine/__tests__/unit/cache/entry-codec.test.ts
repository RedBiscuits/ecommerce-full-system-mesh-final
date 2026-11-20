import { describe, expect, it } from 'vitest';

import {
  MAX_HEADER_LENGTH,
  PREAMBLE_LENGTH,
  decodeHeader,
  decodePreamble,
  encodeHeader,
} from '../../../src/cache/entry-codec';
import { CacheIntegrityError } from '../../../src/cache/errors';
import type { EntryMetadata } from '../../../src/cache/types';

const metadata: EntryMetadata = {
  key: 'https://example.com/file.bin',
  createdAt: 1000,
  expiresAt: 2000,
  staleUntil: 3000,
  size: 5,
  checksum: 'sha256:abc',
  attributes: { 'content-type': 'application/octet-stream' },
};

describe('entry-codec', () => {
  it('round-trips metadata through the preamble and header', () => {
    const encoded = encodeHeader(metadata);
    const headerLength = decodePreamble(encoded, '/p');
    expect(headerLength).toBe(encoded.length - PREAMBLE_LENGTH);

    const decoded = decodeHeader(encoded.subarray(PREAMBLE_LENGTH), '/p');
    expect(decoded).toEqual(metadata);
    expect(Object.isFrozen(decoded.attributes)).toBe(true);
  });

  it('rejects bad magic, short files and absurd header lengths', () => {
    expect(() => decodePreamble(Buffer.from('NF'), '/p')).toThrow(
      CacheIntegrityError,
    );
    const badMagic = Buffer.alloc(PREAMBLE_LENGTH);
    badMagic.write('XXXX');
    badMagic.writeUInt32BE(10, 4);
    expect(() => decodePreamble(badMagic, '/p')).toThrow(/bad magic/);

    const huge = Buffer.alloc(PREAMBLE_LENGTH);
    huge.write('NFC1');
    huge.writeUInt32BE(MAX_HEADER_LENGTH + 1, 4);
    expect(() => decodePreamble(huge, '/p')).toThrow(/bad header length/);
  });

  it.each([
    ['not json', Buffer.from('{nope')],
    ['array', Buffer.from('[]')],
    ['wrong version', Buffer.from(JSON.stringify({ ...metadata, v: 2 }))],
    [
      'missing key',
      Buffer.from(JSON.stringify({ ...metadata, v: 1, key: '' })),
    ],
    [
      'non-numeric timestamp',
      Buffer.from(JSON.stringify({ ...metadata, v: 1, expiresAt: 'soon' })),
    ],
    [
      'negative size',
      Buffer.from(JSON.stringify({ ...metadata, v: 1, size: -1 })),
    ],
    [
      'foreign checksum algorithm',
      Buffer.from(JSON.stringify({ ...metadata, v: 1, checksum: 'md5:x' })),
    ],
    [
      'non-string attribute',
      Buffer.from(JSON.stringify({ ...metadata, v: 1, attributes: { n: 1 } })),
    ],
  ])('rejects a malformed header: %s', (_label, header) => {
    expect(() => decodeHeader(header, '/p')).toThrow(CacheIntegrityError);
  });
});
