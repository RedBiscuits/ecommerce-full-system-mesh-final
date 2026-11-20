import type { OutgoingHttpHeader, OutgoingHttpHeaders } from 'node:http';

import type { ServerResponse } from './types';

export interface CapturedResponse {
  readonly status: number;
  readonly headers: OutgoingHttpHeaders;
  readonly body: Buffer;
}

export type CaptureOutcome =
  | { readonly kind: 'complete'; readonly response: CapturedResponse }
  /** Body exceeded `maxBytes`; buffering stopped but the response streamed on. */
  | { readonly kind: 'oversized' }
  /** Connection closed before the response finished. */
  | { readonly kind: 'aborted' };

export interface CaptureOptions {
  /** Stop buffering beyond this many bytes. */
  readonly maxBytes: number;
  /**
   * Detach early: restores the patched methods, releases the buffered
   * chunks and settles with `aborted`. The response itself keeps streaming.
   */
  readonly signal?: AbortSignal;
}

/**
 * Tees everything written to `res` into memory while letting it stream to the
 * client untouched. Only the *instance* is patched — prototypes are never
 * modified — and the patch is removed once the response settles.
 *
 * The returned promise resolves when the response finishes (or the socket
 * closes); it never rejects.
 */
export function captureResponse(
  res: ServerResponse,
  options: CaptureOptions,
): Promise<CaptureOutcome> {
  const chunks: Buffer[] = [];
  let size = 0;
  let oversized = false;
  let explicitStatus: number | undefined;
  let explicitHeaders: OutgoingHttpHeaders = {};

  const originalWrite = res.write;
  const originalEnd = res.end;
  const originalWriteHead = res.writeHead;

  const record = (chunk: unknown, encoding: unknown): void => {
    if (oversized || chunk === undefined || chunk === null) {
      return;
    }
    const buffer = toBuffer(chunk, encoding);
    if (buffer === undefined) {
      return;
    }
    size += buffer.length;
    if (size > options.maxBytes) {
      oversized = true;
      chunks.length = 0;
      return;
    }
    chunks.push(buffer);
  };

  const restore = (): void => {
    res.write = originalWrite;
    res.end = originalEnd;
    res.writeHead = originalWriteHead;
  };

  // The patched methods are declared with the same overloads Node exposes so
  // they are assignable without casts; dispatch to the originals is dynamic.
  function write(
    this: ServerResponse,
    chunk: unknown,
    callback?: (error: Error | null | undefined) => void,
  ): boolean;
  function write(
    this: ServerResponse,
    chunk: unknown,
    encoding: BufferEncoding,
    callback?: (error: Error | null | undefined) => void,
  ): boolean;
  function write(this: ServerResponse, ...args: unknown[]): boolean {
    record(args[0], args[1]);
    return Reflect.apply(originalWrite, this, args);
  }

  function end(this: ServerResponse, callback?: () => void): ServerResponse;
  function end(
    this: ServerResponse,
    chunk: unknown,
    callback?: () => void,
  ): ServerResponse;
  function end(
    this: ServerResponse,
    chunk: unknown,
    encoding: BufferEncoding,
    callback?: () => void,
  ): ServerResponse;
  function end(this: ServerResponse, ...args: unknown[]): ServerResponse {
    if (typeof args[0] !== 'function') {
      record(args[0], args[1]);
    }
    return Reflect.apply(originalEnd, this, args);
  }

  function writeHead(
    this: ServerResponse,
    statusCode: number,
    statusMessage?: string,
    headers?: OutgoingHttpHeaders | OutgoingHttpHeader[],
  ): ServerResponse;
  function writeHead(
    this: ServerResponse,
    statusCode: number,
    headers?: OutgoingHttpHeaders | OutgoingHttpHeader[],
  ): ServerResponse;
  function writeHead(
    this: ServerResponse,
    statusCode: number,
    ...rest: unknown[]
  ): ServerResponse {
    explicitStatus = statusCode;
    const headers = typeof rest[0] === 'string' ? rest[1] : rest[0];
    if (isHeaderObject(headers)) {
      explicitHeaders = { ...explicitHeaders, ...headers };
    }
    return Reflect.apply(originalWriteHead, this, [statusCode, ...rest]);
  }

  res.write = write;
  res.end = end;
  res.writeHead = writeHead;

  const { signal } = options;
  return new Promise<CaptureOutcome>((resolve) => {
    const settle = (outcome: CaptureOutcome): void => {
      restore();
      chunks.length = 0;
      res.off('finish', onFinish);
      res.off('close', onClose);
      signal?.removeEventListener('abort', onAbort);
      resolve(outcome);
    };
    const onAbort = (): void => settle({ kind: 'aborted' });
    const onFinish = (): void => {
      if (oversized) {
        settle({ kind: 'oversized' });
        return;
      }
      const body = Buffer.concat(chunks);
      settle({
        kind: 'complete',
        response: {
          status: explicitStatus ?? res.statusCode,
          headers: { ...res.getHeaders(), ...explicitHeaders },
          body,
        },
      });
    };
    const onClose = (): void => {
      if (!res.writableFinished) {
        settle({ kind: 'aborted' });
      }
    };
    if (signal?.aborted) {
      settle({ kind: 'aborted' });
      return;
    }
    res.once('finish', onFinish);
    res.once('close', onClose);
    signal?.addEventListener('abort', onAbort, { once: true });
  });
}

function toBuffer(chunk: unknown, encoding: unknown): Buffer | undefined {
  if (typeof chunk === 'string') {
    return Buffer.from(chunk, isEncoding(encoding) ? encoding : 'utf8');
  }
  if (Buffer.isBuffer(chunk)) {
    return chunk;
  }
  if (chunk instanceof Uint8Array) {
    return Buffer.from(chunk.buffer, chunk.byteOffset, chunk.byteLength);
  }
  return undefined;
}

function isEncoding(value: unknown): value is BufferEncoding {
  return typeof value === 'string' && Buffer.isEncoding(value);
}

function isHeaderObject(value: unknown): value is OutgoingHttpHeaders {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}
