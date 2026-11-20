/** Base class for every error raised by the cache layer. */
export class CacheError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = new.target.name;
  }
}

/** The supplied key cannot be used (empty, too long, ...). */
export class InvalidCacheKeyError extends CacheError {
  constructor(reason: string) {
    super(`Invalid cache key: ${reason}`);
  }
}

/** A stored file failed structural or checksum validation. */
export class CacheIntegrityError extends CacheError {
  constructor(
    readonly path: string,
    reason: string,
    options?: ErrorOptions,
  ) {
    super(`Cache entry at "${path}" is corrupt: ${reason}`, options);
  }
}

/** A payload exceeds the configured per-entry size limit. */
export class EntryTooLargeError extends CacheError {
  constructor(
    readonly key: string,
    readonly size: number,
    readonly limit: number,
  ) {
    super(
      `Entry "${key}" is ${size} bytes which exceeds the limit of ${limit} bytes`,
    );
  }
}

/** The cache was closed; no further operations are accepted. */
export class CacheClosedError extends CacheError {
  constructor() {
    super('Cache is closed');
  }
}
