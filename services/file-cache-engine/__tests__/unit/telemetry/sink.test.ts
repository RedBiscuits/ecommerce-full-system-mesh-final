import { describe, expect, it, vi } from 'vitest';

import type { TelemetryEvent } from '../../../src/telemetry/events';
import { describeError } from '../../../src/telemetry/events';
import {
  CompositeSink,
  ConsoleSink,
  noopSink,
  safeSink,
} from '../../../src/telemetry/sink';

const event: TelemetryEvent = {
  type: 'cache.lookup',
  at: 1,
  operation: 'get',
  key: 'https://x/private?token=abc',
  outcome: 'hit',
  freshness: 'fresh',
  durationMs: 1.5,
};

describe('telemetry sinks', () => {
  it('ConsoleSink writes JSON lines and redacts keys by default', () => {
    const lines: string[] = [];
    new ConsoleSink({ write: (l): number => lines.push(l) }).emit(event);
    const parsed: unknown = JSON.parse(lines[0] ?? '');
    expect(parsed).toMatchObject({
      type: 'cache.lookup',
      outcome: 'hit',
      key: '[redacted]',
    });
    expect(lines[0]).not.toContain('token=abc');

    const verbose: string[] = [];
    new ConsoleSink({
      write: (l): number => verbose.push(l),
      includeKeys: true,
    }).emit(event);
    expect(verbose[0]).toContain('token=abc');
  });

  it('ConsoleSink can filter by event type', () => {
    const lines: string[] = [];
    const sink = new ConsoleSink({
      write: (l): number => lines.push(l),
      types: ['http.request'],
    });
    sink.emit(event);
    expect(lines).toEqual([]);
  });

  it('CompositeSink fans out and isolates a throwing sink', () => {
    const good = vi.fn<(e: TelemetryEvent) => void>();
    const bad = {
      emit: (): void => {
        throw new Error('observer bug');
      },
    };
    const composite = new CompositeSink([bad, undefined, { emit: good }]);
    expect(() => composite.emit(event)).not.toThrow();
    expect(good).toHaveBeenCalledWith(event);
  });

  it('safeSink never throws and defaults to noop', () => {
    expect(() =>
      safeSink({
        emit: () => {
          throw new Error('x');
        },
      }).emit(event),
    ).not.toThrow();
    expect(safeSink(undefined)).toBe(noopSink);
    expect(() => noopSink.emit(event)).not.toThrow();
  });

  it('describeError renders errors, errno codes and non-errors', () => {
    const e = Object.assign(new Error('disk'), { code: 'EIO' });
    expect(describeError(e)).toBe('Error: disk [EIO]');
    expect(describeError(new RangeError('bad'))).toBe('RangeError: bad');
    expect(describeError('plain')).toBe('plain');
  });
});
