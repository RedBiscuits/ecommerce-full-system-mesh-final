/**
 * src/security/redact.ts
 *
 * Keeps registration tokens out of anything that can be read later: log
 * lines, error messages, HTTP responses, persisted records.
 *
 * Two complementary strategies:
 *   - pattern-based: mask the token segment of OPPWA registration URLs
 *     (`/v1/registrations/<token>/…`) wherever they appear in free text;
 *   - value-based: mask a *known* secret wherever it appears, for text we
 *     did not produce ourselves (gateway error bodies, axios messages).
 */

export const REDACTED = '[REDACTED]';

const REGISTRATION_PATH = /(\/v1\/registrations\/)([^/\s"'?]+)/g;

/** Mask the token segment of any OPPWA registration path in `text`. */
export function redactRegistrationPaths(text: string): string {
  return text.replace(REGISTRATION_PATH, `$1${REDACTED}`);
}

/** Mask every occurrence of `secret` in `text` (no-op for empty secrets). */
export function redactValue(text: string, secret: string | undefined): string {
  if (!secret) return text;
  return text.split(secret).join(REDACTED);
}

/** Both of the above — the one to use on anything that might carry a token. */
export function redactSecrets(text: string, ...secrets: Array<string | undefined>): string {
  return secrets.reduce<string>((acc, secret) => redactValue(acc, secret), redactRegistrationPaths(text));
}
