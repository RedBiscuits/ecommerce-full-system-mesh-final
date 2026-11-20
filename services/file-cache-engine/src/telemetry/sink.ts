import type { TelemetryEvent, TelemetryEventType } from './events';

/**
 * Destination for telemetry. `emit` must be synchronous and cheap — it runs
 * on the hot path. It is wrapped so a throwing sink can never break a
 * request; see {@link safeSink}.
 */
export interface TelemetrySink {
  emit(event: TelemetryEvent): void;
}

/** Discards everything. */
export const noopSink: TelemetrySink = {
  emit(): void {
    // intentionally empty
  },
};

/** Fans one event out to several sinks, isolating each from the others. */
export class CompositeSink implements TelemetrySink {
  readonly #sinks: readonly TelemetrySink[];

  constructor(sinks: readonly (TelemetrySink | undefined)[]) {
    this.#sinks = sinks.filter((s): s is TelemetrySink => s !== undefined);
  }

  emit(event: TelemetryEvent): void {
    for (const sink of this.#sinks) {
      try {
        sink.emit(event);
      } catch {
        // A faulty observer must never affect the observed system.
      }
    }
  }
}

export interface ConsoleSinkOptions {
  /** Where lines go. Default: `console.log`. */
  readonly write?: (line: string) => void;
  /**
   * Cache keys often embed URLs with query strings or tokens. They are
   * omitted from log lines unless explicitly enabled. Default: `false`.
   */
  readonly includeKeys?: boolean;
  /** Only log these event types. Default: everything. */
  readonly types?: readonly TelemetryEventType[];
}

/** Structured JSON-lines logger. */
export class ConsoleSink implements TelemetrySink {
  readonly #write: (line: string) => void;
  readonly #includeKeys: boolean;
  readonly #types: ReadonlySet<TelemetryEventType> | undefined;

  constructor(options: ConsoleSinkOptions = {}) {
    this.#write = options.write ?? ((line): void => console.log(line));
    this.#includeKeys = options.includeKeys ?? false;
    this.#types =
      options.types === undefined ? undefined : new Set(options.types);
  }

  emit(event: TelemetryEvent): void {
    if (this.#types !== undefined && !this.#types.has(event.type)) {
      return;
    }
    const record: Record<string, unknown> = { ...event };
    if (!this.#includeKeys && 'key' in record) {
      record['key'] = '[redacted]';
    }
    this.#write(JSON.stringify(record));
  }
}

/** Wraps a sink so that `emit` can never throw. */
export function safeSink(sink: TelemetrySink | undefined): TelemetrySink {
  if (sink === undefined) {
    return noopSink;
  }
  return {
    emit(event): void {
      try {
        sink.emit(event);
      } catch {
        // swallowed by design
      }
    },
  };
}
