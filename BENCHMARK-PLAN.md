# Tiled Registration Benchmark — Plan

Finalized design for profiling and benchmarking the Tiled HTTP registration path. Supersedes
the open decisions in `REGISTRATION-BENCHMARK-HANDOFF.md` §4. Domain language is in
`CONTEXT.md`; the import decision is `docs/adr/0001-import-tcb-for-registration-benchmark.md`.

## Goals

1. **Optimization profile** — decompose where registration time goes (the five layers in
   `CONTEXT.md`) and quantify the achievable speedup.
2. **Scaling curve** — time-to-register vs `n_entities` per layout, and whether per-node cost
   stays flat or degrades as the catalog fills.

> **Regression detection comes free from the profile, not from a standing gate.** Keep the
> profile CSVs (`app;dur` p50 at a fixed config); "did a Tiled/TCB bump slow registration?" is
> answered by re-running the profile and diffing the CSV — which you do anyway. No committed
> baseline JSON to rot, no fixed-multiplier timing threshold to tune, no exit-nonzero gate on a
> local-SQLite proxy that nobody keeps quiesced. A committed CI gate here was cut deliberately.

## Approach (locked)

- **Import TCB** (ADR 0001): use `tcb generate` for manifests and `register_dataset_http` for the
  real path. Keep a **native raw-client** variant as a control. `real_path − native_path` = the
  client-side HDF5 probe (layer 1).
- **Two servers:** local SQLite (`tiled serve` subprocess, RTT≈0, py-spy-able, profiling home)
  and remote Postgres (existing test host, real numbers). Pollution on remote is acceptable.
- **Two write-paths:** stock (`application/x-hdf5`) and broker (`application/x-hdf5-broker`).
- **Three layouts:** per_entity, batched, grouped (synthetic only for grouped — no real dataset).
- **Data:** a small synthetic-HDF5 writer (~new code) emits 1-element arrays in each layout →
  `tcb generate` → manifests. Lets a 10k-entity dataset be KB on disk. Plus real datasets for the
  headline.

## Instruments

- **`Server-Timing: app;dur` header** — the server self-reports per-call work time. Works on
  **local and remote**. This is the primary white-box instrument and the layer-3/4 splitter:
  `server_work = app;dur`, `client+network = wall − app;dur`.
- **py-spy** on the local server PID — flamegraph for finer server-internal attribution.
- **Black-box differentials** — containers-only vs +artifacts; closure-trigger on/off via
  `DROP TRIGGER` on the SQLite file (real, not modeled — `bulk_register.py:249`).
- **Per-call samples** — one run = thousands of `create` calls, so p50/p95/p99 come from *within*
  a single run; full-run reps only measure run-to-run variance.

## Suites

### 1. Scale sweep  (layout = full axis)
`{per_entity, batched, grouped} × n_entities[10,100,1k,10k] × {local, remote}`, workers=8, stock.
Reps: 5 @≤100, 3 @1k, 1 @10k. → throughput vs scale; per-node cost by layout.
*If local-10k exceeds ~10 min, demote it to a one-off.*

### 2. Concurrency sweep
`max_workers[1,2,4,8,16,32]` @ n=500, `{local, remote}`, layouts = **per_entity + batched**
(grouped is path-equivalent to batched on the asset axis). 3 reps. → optimal workers; how
concurrency hides RTT; batched's shared-asset row may show contention per_entity won't.

### 3. White-box internals  (local only)
py-spy flamegraph; differentials per layout (containers-only vs +artifacts; trigger on/off).
Optional deepening: in-process direct `create_node` + cProfile, only if py-spy is too coarse.

### 4. Write-path check
stock vs broker at one config **per layout** — confirm mimetype doesn't move registration cost.

## Optimization experiments

Identified from the baseline. Run **both** isolated and stacked (fixes interact — parallelizing
hides RTT, which shifts per_entity's bottleneck further onto the HDF5 scan):

| Fix | Layer | Scope |
|---|---|---|
| Shape/dtype in manifest (kill HDF5 scan) | 1 | TCB-side — **bench before/after** |
| Skip existence-GET on fresh load (always a 404) | 2 | TCB-side — **bench before/after** |
| Parallelize (`max_workers` > 1) | 4 | TCB-side — **bench before/after** |
| Array-children → single table node (fewer nodes) | 3 | measure potential, **file upstream** |
| Closure trigger / double-commit / batch-create API | 3 | measure, **file upstream — don't fix** |

- **Isolated** — toggle each TCB-side fix independently from baseline @ per_entity n=1k (local + remote).
- **Stacked** — all three TCB-side fixes on = the "after" config for the headline.
- Layer-3 items: white-box differentials quantify their cost; the number goes into the filed issue.

## RTT isolation

Primary: `wall − app;dur` per call (un-confounded, works remote). Cross-checks (expected to
agree): a pure-RTT probe (trivial GET, serial, post-warmup, p50) and `remote − local`. Layer 5
(Postgres vs SQLite) = `app;dur(remote) − app;dur(local)` for the same op.

## Regression detection (no standing gate)

Cut the committed `--check` gate. Regression detection falls out of the profile for free: keep
the profile CSVs and, on a Tiled/TCB bump, re-run `regbench scale` at a fixed config (e.g.
`per_entity`/`batched` n=1k, local, fresh DB) and diff `app_dur_p50_ms` against the prior CSV.
The CSV already records the Tiled version + TCB SHA per row, so the diff is self-documenting.
No `regression_baseline.json` to re-baseline, no fixed-multiplier threshold to tune, no
exit-nonzero gate on a local-SQLite proxy that nobody keeps quiesced.

## Headline run

Re-register real datasets into **fresh timestamped keys** on remote (clean timing + full layer
decomposition), framed as **current vs optimized**:

| Dataset | Layout | Before | After |
|---|---|---|---|
| nips3_multimodal | per_entity, 7,616 | subsample ~1k + project (avoid 6h) | full run (optimized) |
| sam_klein_v2 | batched, 10k | full (~63 min) | full run (optimized) |

The story: "registering real 7,616-entity nips3 went from a projected ~6h to X min."

## Outputs

- CSV, one row per (config × rep): axes + wall-clock, entities/s, artifacts/s, nodes/s,
  per-call p50/p95/p99, `app;dur` p50/p95.
- py-spy flamegraph SVGs for white-box runs.
- marimo notebook for plots (matches `bandwidth_benchmark.py`).
- Findings writeup → feeds TCB fixes (PR4) + the filed upstream issues.

## Out of scope (filed, not fixed)

Tiled-internal/upstream changes: batch-create API, closure-trigger redesign, the double commit per
artifact. Measured here for evidence; filed as issues per handoff §2.8.
