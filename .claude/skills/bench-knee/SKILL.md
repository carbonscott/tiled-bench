---
name: bench-knee
description: Find where the Tiled catalog path saturates under concurrent requests and what holds a request while it waits. Use when asked about the request-rate ceiling, the "20-slot" limit, Little's law, or why req/s stops rising with more clients.
---

# Where is the knee, and what holds the slot?

**Question.** At what measured in-flight does catalog-backed request throughput stop
rising, and which part of the request (token, ACL, catalog read, pack, database) is
holding the time?

**The one metric.** `req_per_s` against `in_flight` (app-seconds per wall-second, the
Little's-law quantity, computed by `Run.summary()`), per rung of a concurrency ladder.
Alongside it, the per-segment p50s (`tok`, `acl`, `read`, `pack`, `app`) from the same
calls, and the database's `active` backend count sampled during the rung.

**What we already know** (`results/EGRESS-FINDINGS.md`, steps 7–7f): peak ~230 req/s
with 16 workers, ~135 with 8; a doubling ladder put the peak at P=8 × 8 threads where
in-flight happened to be ~20; in-flight grows linearly with offered load (11, 20, 40,
65, 80), so the knee is somewhere between 11 and 40 and was never resolved. `/healthz`
does 6,300 req/s, so the ingress is not it.

## Protocol

1. Dataset: `EGRESS_PE_64K_BROKER` (1000 entities, one 64 KB artifact `signal`). Two
   work functions: `reads.artifact_read` (2 nav GETs + 1 data GET) and
   `reads.metadata_get` (catalog only, no client library).
2. Fixed `procs=8`; thread ladder `1, 2, 3, 4, 6, 8, 12, 16`. That is offered 8..128 in
   steps fine enough to see the knee. Two reps per rung. Stop escalating when a rung
   has >5% failed units.
3. Around every rung: `probes.tiled_metrics` before and after (pool checkouts,
   overflow and at-max counters). `probes.PgSampler` at 1 Hz for the whole ladder if a
   `psql` command is available; `probes.pg_connections` once before and once after.
4. Every rung is a `load.run(...)`; `assert_clean()` it; `rd.save(run, "<work>-t<threads>")`.
5. One job on an exclusive compute node, the same node class as before (milano). Keep the
   load generator out of the picture: 8 processes × 16 threads is far below one
   process's own ceiling.

`rounds/2026-11-02-knee/run.py` is this protocol written down; copy it for a re-run.

## Reading it

- Knee = first rung where `req_per_s` stops rising while `app_p50_ms` keeps rising.
  Report `in_flight` at that rung, not the offered concurrency.
- If `read_ms` carries the growth and the sampler shows `active` backends flat at some
  N with nothing waiting: a fixed pool somewhere between tiled and Postgres.
- If `active` tracks offered load with empty `wait_event`: the database is CPU-bound;
  ask the admin for the CNPG pod's CPU limit and usage over the window.
- If `app_ms` grows but no segment does: time is spent between segments, i.e. the
  worker's event loop or a container CPU quota. Ask for the tiled pod's CPU limit and
  `container_cpu_cfs_throttled_periods_total` over the window.
- `pg_connections` after minus before says how many connections the pools ever opened:
  16 workers × 2 means no worker ever needed a third concurrent query.

## Hazards

- The Tiled client retries silently; only `metadata_get` sees raw 500s. Report both.
- Remote is a shared server. Note the wall-clock window in FINDINGS.md so the admin can
  correlate dashboards (2-minute bins).
- Credentials: `source` the env file in the same shell as the run; never print it.
