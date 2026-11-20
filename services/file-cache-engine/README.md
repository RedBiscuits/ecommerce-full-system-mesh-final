# File Cache Engine (`file-cache-engine`)

A production-grade, high-throughput, resilient file-backed caching and HTTP proxy engine for Node.js and TypeScript.

The engine provides an end-to-end caching layer designed for high-concurrency backend services, microservices, and reverse proxies. It isolates downstream storage and origin services using single-flight request coalescing, circuit breakers, token-bucket rate limiting, atomic crash-safe writes, and automated cold-entry garbage collection.

---

## Architecture Overview

```
┌────────────────────────────────────────────────────────────────────────┐
│                          createFileCache()                             │
│                  (Unified Composition Root / Factory)                  │
└───────┬──────────────┬──────────────┬──────────────┬──────────────┬────┘
        │              │              │              │              │
┌───────▼──────┐┌──────▼──────┐┌──────▼──────┐┌──────▼──────┐┌──────▼──────┐
│ Core Storage ││ Resilience  ││ Rate Limit  ││ HTTP Stack  ││ Observability│
│  Subsystem   ││  Decorator  ││ & Concurr.  ││ Middlewares ││  & Health   │
│(FsCacheStore)││ (Circ. Brk, ││(TokenBucket,││ (ETag, RFC  ││  (Metrics,  │
│(CacheManager)││ Retries,    ││SingleFlight,││  Cache,     ││  Telemetry, │
│(Exp.Sweeper) ││ File Locks) ││ KeyedMutex) ││ Client-ID)  ││  Diagnostics│
└──────────────┘└─────────────┘└─────────────┘└─────────────┘└─────────────┘
```

### Core Subsystems

1. **Storage Layer (`src/cache/`)**:
   - `FsCacheStore`: Filesystem key-value store with deterministic SHA-256 key sharding, atomic writes (write-to-unique-temp + fsync + atomic rename), entry header checksum verification, and corruption isolation.
   - `CacheManager`: Policy orchestrator supporting configurable TTL with random jitter, `stale-while-revalidate`, `stale-if-error`, and loader delegation.
   - `ExpirySweeper`: Background garbage collection worker that purges expired cache files on a scheduled interval. Uses a cross-process lock (`sweep.lock`) so that only one worker runs sweeps in a multi-instance cluster.
   - `TieredCacheStore` (opt-in via `tiering`): an in-memory L1 (`SlruMemoryStore`) in front of the disk. Memory is budgeted in bytes. It is a segmented LRU (20 % probationary / 80 % protected) with TinyLFU admission (`FrequencySketch`), so scans and one-hit wonders cannot evict entries that are re-read. Concurrent misses share one disk read. With `writeBehind`, writes are acknowledged from memory and flushed to disk in coalesced batches, with bounded backpressure and a strict drain on `close()`.

     ```ts
     const fileCache = await createFileCache({
       directory: './cache',
       tiering: { memoryBytes: 64 * 1024 * 1024, writeBehind: true },
     });
     // ...
     await fileCache.close(); // flushes pending writes before closing the disk store
     ```

     **Disk circuit breaker (L1-only mode).** 5 consecutive disk failures (EIO, ENOSPC, EROFS, …) within a 10 s sliding window open the circuit. The cache then serves from memory only: reads never touch the disk, writes are buffered for replay, and deletes/clears are refused (they can't be made durable). A `tier.circuit` event with `retryAt` announces the next probe: 30 s plus up to 20 % jitter, so instances don't probe in lock-step. A successful probe closes the circuit and drains the buffer. Reads can trip the circuit but can't reset it, so a disk that reads fine but is full still trips. An open circuit makes health `degraded` (still ready), never `down`. Tune or disable it with `tiering.circuit`.

     **Prometheus** (`prometheus: true`, or `{ registry, cacheName, prefix }`) uses prom-client and a private registry by default (`fileCache.prometheus.metrics()` / `.contentType` for a `/metrics` route). Series, all labelled `cache`:
     `file_cache_hits_total{tier="l1|write_buffer|l2"}`, `file_cache_misses_total{reason}`, `file_cache_coalesced_reads_total`, `file_cache_promotions_total{result}`, `file_cache_writes_total{mode}`, `file_cache_flushes_total{outcome}`, `file_cache_flush_duration_seconds` (histogram), `file_cache_write_behind_queue_{length,bytes,capacity}`, `file_cache_l1_bytes{segment}`, `file_cache_l1_entries`, `file_cache_circuit_state{state}`, `file_cache_circuit_consecutive_failures`, `file_cache_circuit_transitions_total{state}`.

2. **Resilience & Fault Tolerance (`src/resilience/`)**:
   - `ResilientCacheStore`: Fault-tolerant decorator wrapping the raw storage layer.
   - `CircuitBreaker`: Three-state breaker (Closed, Open, Half-Open) tripping on storage I/O failures to prevent thread starvation during degraded disk states.
   - `Retry`: Exponential backoff with full jitter for transient I/O faults.
   - `FileLock`: Inter-process mutex using filesystem lock files with stale lock detection.
   - **Emergency Disk Recovery**: Detects disk exhaustion (`ENOSPC`) and triggers an immediate on-demand sweep before reattempting the write.

3. **Concurrency & Traffic Shaping (`src/rate-limit/` & `src/shared/`)**:
   - `SingleFlight`: Prevents thundering herds by coalescing concurrent in-flight requests for the same cache key into a single backend fetch.
   - `KeyedMutex`: Granular per-key serialization.
   - `TokenBucketRateLimiter`: Leaky token bucket rate limiter to protect backends from cache miss storms.

4. **HTTP Middlewares (`src/http/`)**:
   - `cache-middleware`: Transparent RFC-compliant HTTP `GET` caching, handling `Cache-Control`, `ETag`, `If-None-Match` (304 Not Modified), and response stream capture.
   - `rate-limit-middleware`: Client quota enforcement with standard `429 Too Many Requests` and `Retry-After` headers.
   - `client-identity`: Robust client identifier resolver (IP, API tokens, authorization headers).

5. **Telemetry & Diagnostics (`src/telemetry/` & `src/diagnostics/`)**:
   - `TelemetrySink` & `CompositeSink`: Structured event pipeline emitting cache hits, misses, corruptions, and sweeps.
   - `MetricsCollector`: In-memory metrics tracking throughput, latencies, and space reclaimed.
   - `Diagnostics`: Offline CLI tool (`bin/diagnose.ts`) and HTTP `/health` and `/ready` endpoints verifying filesystem read/write latency, lock states, and disk utilization.

---

## Quality Gates & Verification

Run the verification commands from the project root:

```bash
# 1. Type verification
npm run check-types

# 2. Static analysis
npm run lint

# 3. Unit, Resilience, and Integration Test Suite
npm test

# 4. Storage Diagnostic Inspection
npm run diagnose
```

---

## Invoice PDFs (platform order flow)

`src/integration/invoice-pdf-worker.ts` consumes `zatca.invoice.reported` (queue `file-cache.invoice-pdfs`), renders the invoice PDF with an injected renderer (the ZATCA renderer, from the event's document snapshot) and stores it under `zatca/invoices/<taxpayer>/<invoice>.pdf` with `content-type`, `x-content-sha256`, `x-invoice-hash` and `x-icv` attributes. The cache entry is the checkpoint: a redelivery finds a fresh entry for the same invoice hash and is acknowledged without rendering again.

## Merchant settlement statements (platform settlement workflow)

`src/integration/settlement-statements.ts` consumes `ledger.merchant-settlement.posted` and `zatca.settlement-documents.reported`. Both arrive on one queue, `file-cache.settlement-statements`, in any order and any number of times.

- **On posted:** the statement JSON `settlements/<merchant>/<batch>.json` is written with gross, fee (net, VAT, total, tiers, rule), net payout, the journal transaction id and legs, window and period. Its status is `PENDING_TAX_REPORTING` and it is kept for **72 h** (no TTL jitter).
- **On reported:** both documents' PDFs (rendered from the snapshot) and UBL XML are cached under `zatca/invoices/<taxpayer>/<invoice>.pdf|xml`. The statement then becomes `FINAL`, lists them (numbers, ICVs, hashes, keys, SHA-256) and keeps its original expiry.
- **Monotonic:** the status only moves from absent to PENDING to FINAL. Writes are serialized per statement, in-process with `KeyedMutex` and across processes with a `FileLock` in the cache directory.
- **Reads** (`SettlementStatementStore.fetch`): concurrent fetches of one statement share one store read and parse (`SingleFlight`). The hotness tracker (a `FrequencySketch`, TinyLFU) pins statements fetched at least `hotThreshold` times in memory for `hotTtlMs`. The pinned set is bounded and the coldest entry is dropped first. A write invalidates the pin and starts a new read generation, so stale copies are never pinned.
- **HTTP** (`src/integration/settlement-http.ts`, a `node:http` handler, port 3106 in the supervisor): `GET /settlements/:merchantId/:batchId` (strong ETag, `If-None-Match` → 304, `X-Settlement-Status`), `GET /documents/:taxpayerId/:invoiceId.pdf|xml`, `GET /health` (with the read stats).

### Event authentication

Every event this service publishes is signed with its stream's shared key (HMAC-SHA256,
`x-key-id` / `x-event-stream` / `x-event-issuer` / `x-event-audience` / `x-signature`), and every
delivery it consumes is verified against the authorization matrix before the body is parsed: wrong
key, wrong issuer, wrong audience or a tampered byte means the message is dead-lettered at once
(`UNTRUSTED_EVENT`). See `shared/security/streams.ts` and the root README.

## Customer refunds (credit notes)

`src/integration/credit-note-worker.ts` consumes `zatca.credit-note.reported` (queue `file-cache.credit-notes`): caches the credit note's PDF and UBL XML, then invalidates the credited invoice's cached PDF and replaces it with a fresh render listing the credit notes applied and the amount still standing (`x-credited-halalas`, `x-credit-notes` attributes). A redelivery changes nothing.
