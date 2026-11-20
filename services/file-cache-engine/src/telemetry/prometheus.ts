import { Counter, Gauge, Histogram, Registry } from 'prom-client';

import type { TieredCacheStore } from '../cache/tiered-cache-store';
import type { CircuitState } from '../resilience/circuit-breaker';
import type { TelemetryEvent } from './events';
import type { TelemetrySink } from './sink';

export interface PrometheusTelemetryOptions {
  /**
   * Registry the metrics are registered in. Default: a new private
   * `Registry` (see `registry`), never prom-client's global one, so
   * creating several caches or test instances cannot collide. Pass your
   * app's registry to serve everything from one `/metrics`. Several caches
   * may share a registry; the `cache` label tells them apart.
   */
  readonly registry?: Registry;
  /** Metric name prefix. Default: `file_cache_`. */
  readonly prefix?: string;
  /** Value of the `cache` label on every series. Default: `default`. */
  readonly cacheName?: string;
  /** Buckets (seconds) of the flush-latency histogram. */
  readonly flushBuckets?: readonly number[];
}

/** 0.5 ms … 5 s: a local-disk write is sub-ms, a sick one takes seconds. */
export const DEFAULT_FLUSH_BUCKETS: readonly number[] = [
  0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5,
];

const CIRCUIT_STATES: readonly CircuitState[] = ['closed', 'half-open', 'open'];

/**
 * Prometheus exporter for the tiered cache, built on `prom-client`.
 *
 * It is a {@link TelemetrySink}, so the store never depends on prom-client.
 * Event-driven series (counters, the flush histogram) are updated from
 * `tier.*` events. Level series (queue length, L1 bytes, circuit state) are
 * read from `TieredCacheStore.stats()` at scrape time, so they cost nothing
 * between scrapes. Call {@link observe} to attach a store.
 *
 * Series (prefix `file_cache_`, all labelled `cache`):
 * - `hits_total{tier="l1"|"write_buffer"|"l2"}`, `misses_total{reason}`
 * - `coalesced_reads_total`, `promotions_total{result}`
 * - `writes_total{mode}`, `flushes_total{outcome}`
 * - `flush_duration_seconds{outcome}` (histogram)
 * - `write_behind_queue_length`, `write_behind_queue_bytes`,
 *   `write_behind_queue_capacity`
 * - `l1_bytes{segment}`, `l1_entries`
 * - `circuit_state{state}` (1 for the current state),
 *   `circuit_consecutive_failures`, `circuit_transitions_total{state}`
 */
export class PrometheusTelemetry implements TelemetrySink {
  readonly registry: Registry;
  readonly #cache: string;
  readonly #hits: Counter<'cache' | 'tier'>;
  readonly #misses: Counter<'cache' | 'reason'>;
  readonly #coalesced: Counter<'cache'>;
  readonly #promotions: Counter<'cache' | 'result'>;
  readonly #writes: Counter<'cache' | 'mode'>;
  readonly #flushes: Counter<'cache' | 'outcome'>;
  readonly #flushSeconds: Histogram<'cache' | 'outcome'>;
  readonly #transitions: Counter<'cache' | 'state'>;
  readonly #gauges: Array<SharedGauge>;
  #detach: (() => void) | undefined;

  constructor(options: PrometheusTelemetryOptions = {}) {
    this.registry = options.registry ?? new Registry();
    this.#cache = options.cacheName ?? 'default';
    const p = options.prefix ?? 'file_cache_';
    const r = this.registry;

    this.#hits = counter(r, `${p}hits_total`, 'Reads answered by a tier.', [
      'cache',
      'tier',
    ]);
    this.#misses = counter(
      r,
      `${p}misses_total`,
      'Reads found in no tier (short_circuited: L2 skipped, circuit not closed).',
      ['cache', 'reason'],
    );
    this.#coalesced = counter(
      r,
      `${p}coalesced_reads_total`,
      'Reads that joined an L2 read already in flight.',
      ['cache'],
    );
    this.#promotions = counter(
      r,
      `${p}promotions_total`,
      'L2 hits offered to L1, by TinyLFU admission result.',
      ['cache', 'result'],
    );
    this.#writes = counter(
      r,
      `${p}writes_total`,
      'Writes by how they were accepted.',
      ['cache', 'mode'],
    );
    this.#flushes = counter(
      r,
      `${p}flushes_total`,
      'Attempts to write a buffered write to L2, by outcome.',
      ['cache', 'outcome'],
    );
    this.#flushSeconds = histogram(
      r,
      `${p}flush_duration_seconds`,
      'Latency of writing one buffered write to L2.',
      ['cache', 'outcome'],
      options.flushBuckets ?? DEFAULT_FLUSH_BUCKETS,
    );
    this.#transitions = counter(
      r,
      `${p}circuit_transitions_total`,
      'L2 circuit state changes, by the state entered.',
      ['cache', 'state'],
    );
    this.#gauges = [
      sharedGauge(
        r,
        `${p}write_behind_queue_length`,
        'Writes accepted but not yet confirmed by L2.',
        ['cache'],
      ),
      sharedGauge(
        r,
        `${p}write_behind_queue_bytes`,
        'Payload bytes held by those writes.',
        ['cache'],
      ),
      sharedGauge(
        r,
        `${p}write_behind_queue_capacity`,
        'Write-behind ring buffer capacity.',
        ['cache'],
      ),
      sharedGauge(r, `${p}l1_bytes`, 'Bytes charged to L1, per segment.', [
        'cache',
        'segment',
      ]),
      sharedGauge(r, `${p}l1_entries`, 'Entries in L1.', ['cache']),
      sharedGauge(
        r,
        `${p}circuit_state`,
        'L2 circuit state: 1 for the current state, 0 otherwise.',
        ['cache', 'state'],
      ),
      sharedGauge(
        r,
        `${p}circuit_consecutive_failures`,
        'Disk failures in the current streak (within the sliding window).',
        ['cache'],
      ),
    ];
  }

  /**
   * Reads level gauges from `store` at scrape time. Replaces any store
   * observed before. Returns (and {@link dispose} calls) the detach function.
   */
  observe(store: TieredCacheStore): () => void {
    this.#detach?.();
    const cache = this.#cache;
    const [length, bytes, capacity, l1Bytes, l1Entries, state, failures] =
      this.#gauges as [
        SharedGauge,
        SharedGauge,
        SharedGauge,
        SharedGauge,
        SharedGauge,
        SharedGauge,
        SharedGauge,
      ];
    const update = (): void => {
      const stats = store.stats();
      const l1 = store.l1.stats();
      length.gauge.set({ cache }, stats.pendingWrites);
      bytes.gauge.set({ cache }, stats.pendingBytes);
      capacity.gauge.set({ cache }, store.maxPendingWrites);
      l1Bytes.gauge.set({ cache, segment: 'probation' }, l1.probationBytes);
      l1Bytes.gauge.set({ cache, segment: 'protected' }, l1.protectedBytes);
      l1Entries.gauge.set({ cache }, l1.entries);
      const current = stats.circuit?.state ?? 'closed';
      for (const s of CIRCUIT_STATES) {
        state.gauge.set({ cache, state: s }, s === current ? 1 : 0);
      }
      failures.gauge.set({ cache }, stats.circuit?.consecutiveFailures ?? 0);
    };
    // Registered on every level gauge, so reading any one of them alone
    // (not just a full scrape, whatever the order) sees fresh values. A
    // scrape runs it a few times: cheap, and only at scrape time.
    for (const shared of this.#gauges) {
      shared.updaters.set(cache, update);
    }
    const detach = (): void => {
      for (const shared of this.#gauges) {
        if (shared.updaters.get(cache) === update) {
          shared.updaters.delete(cache);
        }
      }
      if (this.#detach === detach) {
        this.#detach = undefined;
      }
    };
    this.#detach = detach;
    return detach;
  }

  /** Stops reading from the observed store (its series keep their last value). */
  dispose(): void {
    this.#detach?.();
  }

  /** The exposition text for a `/metrics` endpoint. */
  metrics(): Promise<string> {
    return this.registry.metrics();
  }

  /** `Content-Type` for {@link metrics}. */
  get contentType(): string {
    return this.registry.contentType;
  }

  emit(event: TelemetryEvent): void {
    const cache = this.#cache;
    switch (event.type) {
      case 'tier.read':
        if (event.coalesced) {
          this.#coalesced.inc({ cache });
        }
        if (event.source === 'miss') {
          this.#misses.inc({
            cache,
            reason: event.shortCircuited ? 'short_circuited' : 'not_found',
          });
        } else {
          this.#hits.inc({ cache, tier: event.source.replace('-', '_') });
        }
        break;
      case 'tier.promotion':
        this.#promotions.inc({
          cache,
          result: event.admitted ? 'admitted' : 'rejected',
        });
        break;
      case 'tier.write':
        this.#writes.inc({ cache, mode: event.mode.replace('-', '_') });
        break;
      case 'tier.flush':
        this.#flushes.inc({ cache, outcome: event.outcome });
        this.#flushSeconds.observe(
          { cache, outcome: event.outcome },
          event.durationMs / 1000,
        );
        break;
      case 'tier.circuit':
        this.#transitions.inc({ cache, state: event.state });
        break;
      default:
        // Other events are covered by MetricsCollector.
        break;
    }
  }
}

// --- get-or-create helpers ------------------------------------------------------

interface SharedGauge {
  readonly gauge: Gauge<string>;
  /** Per-cache refresh functions, run at scrape time. */
  readonly updaters: Map<string, () => void>;
}

const gaugeUpdaters = new WeakMap<Gauge<string>, Map<string, () => void>>();

function existing<T>(
  registry: Registry,
  name: string,
  type: abstract new (...args: never[]) => T,
): T | undefined {
  const metric = registry.getSingleMetric(name);
  if (metric === undefined) {
    return undefined;
  }
  if (!(metric instanceof type)) {
    throw new TypeError(
      `Metric "${name}" is already registered with a different type`,
    );
  }
  return metric;
}

function counter<L extends string>(
  registry: Registry,
  name: string,
  help: string,
  labelNames: readonly L[],
): Counter<L> {
  return (
    (existing(registry, name, Counter) as Counter<L> | undefined) ??
    new Counter({ name, help, labelNames, registers: [registry] })
  );
}

function histogram<L extends string>(
  registry: Registry,
  name: string,
  help: string,
  labelNames: readonly L[],
  buckets: readonly number[],
): Histogram<L> {
  return (
    (existing(registry, name, Histogram) as Histogram<L> | undefined) ??
    new Histogram({
      name,
      help,
      labelNames,
      buckets: [...buckets],
      registers: [registry],
    })
  );
}

function sharedGauge(
  registry: Registry,
  name: string,
  help: string,
  labelNames: readonly string[],
): SharedGauge {
  const found = existing(registry, name, Gauge) as Gauge<string> | undefined;
  if (found !== undefined) {
    const updaters = gaugeUpdaters.get(found);
    if (updaters === undefined) {
      throw new TypeError(
        `Gauge "${name}" was not registered by this exporter`,
      );
    }
    return { gauge: found, updaters };
  }
  const updaters = new Map<string, () => void>();
  const gauge = new Gauge({
    name,
    help,
    labelNames,
    registers: [registry],
    collect(): void {
      for (const update of updaters.values()) {
        update();
      }
    },
  });
  gaugeUpdaters.set(gauge, updaters);
  return { gauge, updaters };
}
