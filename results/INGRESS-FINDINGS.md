# Ingress (registration) benchmark — findings (2026-08-19)

First execution of the campaign designed in `BENCHMARK-PLAN.md` /
`docs/agent-runbook-http-register-benchmark.md` (the regbench harness existed; no results
had ever been committed). Before/after comparison of two pre-identified TCB optimizations,
measured local and remote from milano exclusive nodes. Raw rows:
`results/ingress/{before,after,remote_before,remote_after}.csv`; HTML reports:
`results/ingress/http_register_before_after{,_remote}.html`.

**Code states.** before = TCB `9e36bcd` (branch `refactor/cleanup-inspect-bulk`, the
editable install). after = `d0ab7f1` on branch `optimize/http-register-perf`
(worktree `../tiled-catalog-broker/.worktrees/optimize-http-register-perf`, injected via
`PYTHONPATH` so the main checkout is untouched):

- **Fix A** — `register_dataset_http(assume_new=True)` skips the per-entity existence GET
  (a guaranteed-404 round-trip on fresh keys).
- **Fix B** — `tcb generate` records per-entity `shape`/`dtype` in the artifact manifest;
  `create_data_source` prefers them over opening each HDF5 (`get_artifact_info`), with
  fallback for old manifests. Read-back verified: array returns `(1,) float64` and
  metadata matches (also fixes the metadata dtype string, previously
  `"<class 'numpy.float64'>"`).
- **Fix C** (added mid-campaign, see below) — resume on 409 container/artifact collisions
  instead of aborting the entity.

**Protocol.** `regbench scale`: layouts per_entity + batched, sizes 10/100/1000, workers 8,
stock write-path, fresh local server+DB per (layout, size); reps 5/5/3. Guardrail on every
row (`entities == artifacts == n_entities`, 0 failures); rows stamped with `tcb_sha`,
`tiled_version` (0.2.13 client) and — new this campaign — `server_version`.

## Local (SQLite): fixes are safe but the server is the bottleneck

| layout, n | wall before → after | speedup |
|---|---|---|
| per_entity 1000 | 89.7 s → 76.0 s | **1.18×** |
| batched 1000 | 80.6 s → 77.0 s | 1.05× |
| per_entity 100 | 8.2 s → 7.7 s | 1.07× |
| batched 100 | 8.4 s → 7.1 s | 1.18× |

`call_p50 ≈ app_dur_p50` (135–190 ms per write POST) throughout: wall time is the server
serializing SQLite writes at ~12–14 ent/s (~25 nodes/s). The removed GET doesn't hold the
write lock and the removed HDF5 opens are client-side, so neither moves the serialized
server work much. n=10 cells are sub-second warmup noise. Per-node cost is **flat with
catalog fill**: a 10k-entity registration ran at 12.7 ent/s vs 13.2 at n=1000
(BENCHMARK-PLAN goal 2 answered, at least to 10k on SQLite).

## Remote (tiled-test, Postgres, server 0.2.15b1.dev14+b7aafcd1): 1.25–1.44×

| layout, n | wall before → after | speedup | ent/s before → after |
|---|---|---|---|
| per_entity 1000 | 30.7 s → 23.5 s | **1.31×** | 32.5 → 42.6 |
| batched 1000 | 30.1 s → 24.0 s | **1.25×** | 33.3 → 41.7 |
| per_entity 100 | 3.4 s → 2.5 s | 1.38× | 29.4 → 40.6 |
| batched 100 | 3.3 s → 2.6 s | 1.29× | 30.2 → 38.9 |

The remote path is round-trip-bound; the existence GET was one of ~3 serial round-trips
per entity, so removing it is worth ~1.3× uniformly across layouts. Two context points:

- **Remote registers ~2.5× faster than local SQLite even before the fixes** (33 vs 13
  ent/s at n=1000) — Postgres absorbs the 8 concurrent writers in parallel. The local
  numbers under-state the production benefit of the fixes.
- Extrapolation: at the after-rate (~42 ent/s), NiPS3-scale per_entity (~7.6k) ≈ 3 min,
  batched 10k ≈ 4 min. The write path runs ~85 calls/s at 8 workers — well under the
  server's ~230 req/s ceiling; client concurrency is the next lever, not the server.

## Discovery: Fix A exposes the registration race locally; Fix C resolves it

The first after-sweep crashed: SQLite `database is locked` → 500 → the tiled client's
transparent retry → **409 Conflict on the already-committed container POST** → TCB aborted
the entity. This is the *same* 500→retry→409 race the egress campaign documented on the
shared server — and the abort-after-container-landed behavior is precisely what produces
its "structure-less entities" (container present, artifacts missing, `len(dataset)`
correct). Fix A's higher write pressure made it reproducible locally for the first time.

Fix C changes `_register_one_entity` to **resume** on a 409 container collision (fetch the
existing container, continue registering its artifacts) and to count artifact-409s as
landed. Under identical pressure the re-run passed 26/26 rows clean. This converts the
documented registration-race damage from silent data corruption into a self-healing retry
and is the strongest candidate of the three fixes for merging regardless of performance.

## Harness notes (for the next campaign)

- `regbench` builds asset URIs as `file://localhost` + `base_dir`; a **relative
  `--workspace` yields malformed URIs** that the server refuses at read time (registration
  succeeds silently). Always pass absolute workspace paths. The sweep CSVs are unaffected
  (registration-only), and the runbook's `--workspace "_bench_ws"` example has this trap.
- Remote registration needs `server_base_dir` (pod sees `/prjmaiqmag01/`, host sees
  `/sdf/.../results/`); the runner now derives it from
  `TILED_HOST_DATA_ROOT`/`TILED_SERVER_DATA_ROOT` (.env.test) when `location=remote`, and
  the workspace must live under `data-source/` (used `data-source/regbench_ws/`;
  read-probe confirmed `readable_storage` covers it).
- Runner now survives a rep that dies mid-registration (row kept, flagged by the
  entity-count guardrail, excluded from claims — egress convention); `server_version`
  column added to `RunResult`.
- Remote cleanup: all 52 throwaway `regbench_*` datasets deleted from tiled-test post-run
  (`client.delete_contents(key, recursive=True)`; note `delete_tree(k)` misparses the key
  as the `recursive` flag) and verified gone.

## Caveats

- Local before-rows ran on job 35320024, after-rows on 35321642 (different day-of-night,
  same node type, exclusive) — control deltas well under the claimed effects.
- Remote rows share the proxy with real users; medians over 3–5 reps.
- The after-branch's assume_new is exercised by the harness (fresh keys); real `tcb
  register` runs on existing datasets still pay the existence GET by default — the flag is
  opt-in by design (re-running with it on an existing dataset raises key conflicts, which
  Fix C then absorbs as skips-with-artifact-checks).
