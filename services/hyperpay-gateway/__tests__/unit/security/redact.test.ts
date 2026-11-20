/* eslint-disable @typescript-eslint/no-unused-vars */
import { describe, it, test, expect, beforeEach, afterEach, beforeAll, afterAll, vi } from 'vitest';
/**
 * tests/unit/security/redact.test.ts
 */

import { REDACTED, redactRegistrationPaths, redactSecrets, redactValue } from '../../../src/security/redact';

const TOKEN = '8ac7a4a08f1b2c3d018f1b2c3d4e0001';

describe('redactRegistrationPaths()', () => {
  it('masks the token segment of a registration URL', () => {
    expect(redactRegistrationPaths(`OPPWA POST /v1/registrations/${TOKEN}/payments failed`)).toBe(
      `OPPWA POST /v1/registrations/${REDACTED}/payments failed`,
    );
  });

  it('masks every occurrence, with or without a trailing path, inside JSON and quotes', () => {
    const text = `{"url":"/v1/registrations/${TOKEN}","next":"/v1/registrations/abc/payments?x=1"} and '/v1/registrations/${TOKEN}'`;
    const out = redactRegistrationPaths(text);
    expect(out).not.toContain(TOKEN);
    expect(out).not.toContain('/v1/registrations/abc');
    expect(out).toContain(`/v1/registrations/${REDACTED}/payments?x=1`);
  });

  it('leaves other paths alone', () => {
    const text = '/v1/checkouts/abc/payment and /v1/payments/xyz';
    expect(redactRegistrationPaths(text)).toBe(text);
  });
});

describe('redactValue()', () => {
  it('masks every occurrence of a known secret', () => {
    expect(redactValue(`a ${TOKEN} b ${TOKEN}`, TOKEN)).toBe(`a ${REDACTED} b ${REDACTED}`);
  });

  it('is a no-op for undefined or empty secrets', () => {
    expect(redactValue('unchanged', undefined)).toBe('unchanged');
    expect(redactValue('unchanged', '')).toBe('unchanged');
  });
});

describe('redactSecrets()', () => {
  it('applies both strategies', () => {
    const text = `POST /v1/registrations/${TOKEN}/payments: {"echo":"${TOKEN}"}`;
    const out = redactSecrets(text, TOKEN);
    expect(out).not.toContain(TOKEN);
    expect(out).toBe(`POST /v1/registrations/${REDACTED}/payments: {"echo":"${REDACTED}"}`);
  });

  it('handles several secrets, some undefined', () => {
    expect(redactSecrets('x=1 y=2', 'x=1', undefined, 'y=2')).toBe(`${REDACTED} ${REDACTED}`);
  });
});
