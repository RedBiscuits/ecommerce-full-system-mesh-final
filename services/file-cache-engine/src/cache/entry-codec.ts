import { CacheIntegrityError } from './errors';
import type { EntryAttributes, EntryMetadata } from './types';

/**
 * On-disk layout of a cache entry (single file, so one `rename` publishes
 * header and payload together):
 *
 * ```
 * ┌────────────┬───────────────┬──────────────────┬─────────────┐
 * │ magic (4)  │ headerLen (4) │ header JSON      │ payload     │
 * │ "NFC1"     │ uint32 BE     │ utf-8, headerLen │ size bytes  │
 * └────────────┴───────────────┴──────────────────┴─────────────┘
 * ```
 */
export const MAGIC = Buffer.from('NFC1', 'ascii');
export const PREAMBLE_LENGTH = MAGIC.length + 4;

/** Defensive cap so a corrupt length field cannot trigger a huge allocation. */
export const MAX_HEADER_LENGTH = 1_048_576;

const FORMAT_VERSION = 1;

interface HeaderRecord {
  readonly v: number;
  readonly key: string;
  readonly createdAt: number;
  readonly expiresAt: number;
  readonly staleUntil: number;
  readonly size: number;
  readonly checksum: string;
  readonly attributes: EntryAttributes;
}

/** Serialises the preamble + header for `metadata`. */
export function encodeHeader(metadata: EntryMetadata): Buffer {
  const record: HeaderRecord = {
    v: FORMAT_VERSION,
    key: metadata.key,
    createdAt: metadata.createdAt,
    expiresAt: metadata.expiresAt,
    staleUntil: metadata.staleUntil,
    size: metadata.size,
    checksum: metadata.checksum,
    attributes: metadata.attributes,
  };
  const json = Buffer.from(JSON.stringify(record), 'utf8');
  if (json.length > MAX_HEADER_LENGTH) {
    throw new RangeError(
      `Entry header is ${json.length} bytes which exceeds ${MAX_HEADER_LENGTH}`,
    );
  }

  const preamble = Buffer.alloc(PREAMBLE_LENGTH);
  MAGIC.copy(preamble, 0);
  preamble.writeUInt32BE(json.length, MAGIC.length);
  return Buffer.concat([preamble, json]);
}

/**
 * Validates the preamble and returns the header length.
 * @throws {CacheIntegrityError} when the magic bytes or length are invalid.
 */
export function decodePreamble(preamble: Buffer, path: string): number {
  if (preamble.length < PREAMBLE_LENGTH) {
    throw new CacheIntegrityError(path, 'file is shorter than the preamble');
  }
  if (!preamble.subarray(0, MAGIC.length).equals(MAGIC)) {
    throw new CacheIntegrityError(path, 'bad magic bytes');
  }
  const headerLength = preamble.readUInt32BE(MAGIC.length);
  if (headerLength === 0 || headerLength > MAX_HEADER_LENGTH) {
    throw new CacheIntegrityError(path, `bad header length ${headerLength}`);
  }
  return headerLength;
}

/**
 * Parses and validates the JSON header.
 * @throws {CacheIntegrityError} when the header is malformed.
 */
export function decodeHeader(header: Buffer, path: string): EntryMetadata {
  let parsed: unknown;
  try {
    parsed = JSON.parse(header.toString('utf8'));
  } catch (error) {
    throw new CacheIntegrityError(path, 'header is not valid JSON', {
      cause: error,
    });
  }

  if (!isRecord(parsed)) {
    throw new CacheIntegrityError(path, 'header is not an object');
  }
  if (parsed['v'] !== FORMAT_VERSION) {
    throw new CacheIntegrityError(
      path,
      `unsupported format version ${String(parsed['v'])}`,
    );
  }

  const key = parsed['key'];
  const createdAt = parsed['createdAt'];
  const expiresAt = parsed['expiresAt'];
  const staleUntil = parsed['staleUntil'];
  const size = parsed['size'];
  const checksum = parsed['checksum'];
  const attributes = parsed['attributes'];

  if (typeof key !== 'string' || key.length === 0) {
    throw new CacheIntegrityError(path, 'header key is invalid');
  }
  if (
    !isFiniteNumber(createdAt) ||
    !isFiniteNumber(expiresAt) ||
    !isFiniteNumber(staleUntil)
  ) {
    throw new CacheIntegrityError(path, 'header timestamps are invalid');
  }
  if (!isFiniteNumber(size) || !Number.isSafeInteger(size) || size < 0) {
    throw new CacheIntegrityError(path, 'header size is invalid');
  }
  if (typeof checksum !== 'string' || !checksum.startsWith('sha256:')) {
    throw new CacheIntegrityError(path, 'header checksum is invalid');
  }
  if (!isStringRecord(attributes)) {
    throw new CacheIntegrityError(path, 'header attributes are invalid');
  }

  return {
    key,
    createdAt,
    expiresAt,
    staleUntil,
    size,
    checksum,
    attributes: Object.freeze({ ...attributes }),
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function isStringRecord(value: unknown): value is Record<string, string> {
  return (
    isRecord(value) && Object.values(value).every((v) => typeof v === 'string')
  );
}

function isFiniteNumber(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value);
}
