import { describe, expect, it } from 'vitest';

import {
  directiveMs,
  headerValue,
  isNotModified,
  notModifiedHeaders,
  parseCacheControl,
  storableHeaders,
} from '../../../src/http/headers';

describe('http/headers', () => {
  it('headerValue returns the first trimmed value or undefined', () => {
    expect(headerValue({ 'x-a': ' v ' }, 'X-A')).toBe('v');
    expect(headerValue({ 'x-a': ['1', '2'] }, 'x-a')).toBe('1');
    expect(headerValue({ 'x-a': '  ' }, 'x-a')).toBeUndefined();
    expect(headerValue({}, 'x-a')).toBeUndefined();
  });

  it('parses Cache-Control directives case-insensitively with values', () => {
    const control = parseCacheControl(
      'Public, max-age=60, s-maxage="120", stale-while-revalidate=30',
    );
    expect(control.has('public')).toBe(true);
    expect(directiveMs(control, 'max-age')).toBe(60_000);
    expect(directiveMs(control, 's-maxage')).toBe(120_000);
    expect(directiveMs(control, 'stale-while-revalidate')).toBe(30_000);
    expect(directiveMs(control, 'missing')).toBeUndefined();
    expect(
      directiveMs(parseCacheControl('max-age=abc'), 'max-age'),
    ).toBeUndefined();
    expect(parseCacheControl(undefined).size).toBe(0);
  });

  it('strips hop-by-hop, cookie and length headers before storing', () => {
    expect(
      storableHeaders({
        'Content-Type': 'text/plain',
        'Set-Cookie': ['a=1'],
        Connection: 'keep-alive',
        'Transfer-Encoding': 'chunked',
        'Content-Length': 5,
        ETag: '"x"',
        Missing: undefined,
      }),
    ).toEqual({ 'content-type': 'text/plain', etag: '"x"' });
  });

  it('selects the headers allowed on a 304', () => {
    expect(
      notModifiedHeaders({
        etag: '"x"',
        'cache-control': 'max-age=1',
        'content-type': 'text/plain',
        'last-modified': 'Wed, 21 Oct 2015 07:28:00 GMT',
      }),
    ).toEqual({
      etag: '"x"',
      'cache-control': 'max-age=1',
      'last-modified': 'Wed, 21 Oct 2015 07:28:00 GMT',
    });
  });

  describe('isNotModified', () => {
    const stored = {
      etag: 'W/"abc"',
      'last-modified': 'Wed, 21 Oct 2015 07:28:00 GMT',
    };

    it('matches If-None-Match weakly, including lists and *', () => {
      expect(isNotModified({ 'if-none-match': '"abc"' }, stored)).toBe(true);
      expect(isNotModified({ 'if-none-match': 'W/"abc"' }, stored)).toBe(true);
      expect(isNotModified({ 'if-none-match': '"x", "abc"' }, stored)).toBe(
        true,
      );
      expect(isNotModified({ 'if-none-match': '*' }, stored)).toBe(true);
      expect(isNotModified({ 'if-none-match': '"nope"' }, stored)).toBe(false);
      expect(isNotModified({ 'if-none-match': '"abc"' }, {})).toBe(false);
    });

    it('falls back to If-Modified-Since only when If-None-Match is absent', () => {
      expect(
        isNotModified(
          { 'if-modified-since': 'Thu, 22 Oct 2015 07:28:00 GMT' },
          stored,
        ),
      ).toBe(true);
      expect(
        isNotModified(
          { 'if-modified-since': 'Tue, 20 Oct 2015 07:28:00 GMT' },
          stored,
        ),
      ).toBe(false);
      expect(
        isNotModified(
          {
            'if-none-match': '"nope"',
            'if-modified-since': 'Thu, 22 Oct 2015 07:28:00 GMT',
          },
          stored,
        ),
      ).toBe(false);
      expect(isNotModified({ 'if-modified-since': 'garbage' }, stored)).toBe(
        false,
      );
      expect(isNotModified({}, stored)).toBe(false);
    });
  });
});
