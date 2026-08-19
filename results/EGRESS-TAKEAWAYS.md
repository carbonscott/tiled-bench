# Egress benchmark — high-level takeaways

One page. Full evidence: `EGRESS-FINDINGS.md`; deck: `docs/egress-findings-slides.html`;
interactive map: `docs/egress-heatmap.html`. Campaign: Jul 20 – Aug 12 2026, five retrieval
methods × four workloads × three HDF5 layouts, local SQLite + shared tiled-test, across
four server deployments (every CSV row is stamped with its `server_version`/population).

## For users pulling data

1. **Register with the broker mimetype, always.** Stock HDF5 registration is 26× slower on
   remote per-entity reads and outright breaks (504s) multi-GB exports. This is the single
   largest factor measured.
2. **Serially, `container_export` wins almost everything** (~35 ms/entity at 1 MB). The one
   exception: fetching a small artifact out of a large mixed entity — there use
   `artifact_read` (export would move 259× the bytes you asked for).
3. **Fetching a large fraction of a shared file? Take the whole file** (`asset_bytes` /
   `raw_export` — same speed). Crossover rule of thumb: ~40% of the file for 1 MB
   single-artifact entities, ~2/3 for ≥16 MB entities, ~1/4 for multi-artifact entities.
   Crossing the wrong way costs up to 13×.
4. **One Python process caps at ~190 MB/s (GIL).** Use ~8 threads, then fan out processes.
   `raw_export` cannot run in threads at all (upstream SIGINT-handler bug) — processes only.
5. **Present the server ≈20 concurrent catalog requests, no more.** That's where peak
   throughput lives (~230 req/s); beyond it requests only queue (or, raw-wire, get shed as
   fast 500s that the tiled client silently retries).
6. **Never parallelize multi-GB exports.** Container export builds its whole output in server
   RAM; ~2 concurrent 4.3 GB exports OOM-kill a worker. Above a few GB, `asset_bytes` is
   faster *and* memory-flat.
7. **On-cluster ML pipelines should use Mode A (`h5py_direct`)**: 1.2–1.5 GB/s warm — ~10×
   the single-process HTTP path. But small files kill it (~30 MB/s at 64 KB/file): batch or
   group your layout.

## For the server admin

- **The ~20-in-flight ceiling is the knob worth finding.** It survived 8→16 workers (only
  1.6× gain) and the pooler repair. Suspects: Postgres query capacity, per-worker event
  loop, ingress. Wire/streaming has headroom (1.2 GB/s clean; `/healthz` fabric 6300 req/s).
- **Pooling:** session-mode PgBouncer must fit tiled's persistent pools
  (workers × (pool+overflow) ≈ 240 connections — this is what's deployed now, pass-through).
  Transaction mode requires PgBouncer ≥ 1.21 + `max_prepared_statements`, else instant
  prepared-statement collisions. Run `tiled catalog init`/migrations direct, never pooled.
- **Memory:** per-pod limit ≈ 2 concurrent whole-dataset exports (~9–17 GB). Failure is
  contained (those downloads die; health stays green), confirmed by the 2026-08-12 00:12 UTC
  OOMKilled event.
- **Storage is never the bottleneck**: one client node pulls 2+ GB/s cold from Weka — every
  HTTP-path ceiling sits ≥4× below it.

## For anyone running the next benchmark

- **Stamp every row with the server version/population** — the deployment changed 4× under
  this campaign; unstamped numbers would have been garbage.
- **Guardrail every row** (entity counts, zero errors, byte totals): fast-but-wrong is a
  failed run. Keep error rows in the CSV, exclude them from claims.
- **Probe the raw wire beside the client library** — tiled's silent retries masked a 35%
  server-side 500 rate.
- **Register at pool=4, verify per-artifact** — the registration race at pool=8 breaks up to
  5% of multi-artifact entities, invisibly to `len(dataset)`.
