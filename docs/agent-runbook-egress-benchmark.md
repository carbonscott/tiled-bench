# Agent Runbook — Egress (read-path) benchmark

**You are an agent exploring Tiled egress performance with the `egbench` harness.** The
goal: characterize throughput across **data size × layout × concurrency × retrieval
method**, and find where the ceiling is — server software (`app_frac → 1`), the wire
(`wire_mbps` plateaus while `app_frac` falls), or the client (decode/nav dominate).
Domain language is in `CONTEXT.md` (entity/artifact/layout definitions apply here too).

---

## 0. Environment

- **Interpreter (always):** `PY=/sdf/data/lcls/ds/prj/prjmaiqmag01/results/cfitussi/tiled-catalog-broker/.venv/bin/python`
  (has `tiled`, `tiled_catalog_broker` editable, `h5py`, `pandas`).
- **cwd:** `/sdf/data/lcls/ds/prj/prjmaiqmag01/results/ajshack/tiled-bench` — `egbench`
  and `regbench` import from here.
- **Local server state:** `_egbench_ws/` (catalog.db + `server.json`). Unlike regbench,
  the egress catalog is **persistent** — datasets are registered once and reused.
- **Remote:** `https://lcls-data-portal.slac.stanford.edu/tiled-test` via
  `TILED_URL` / `TILED_API_KEY` (or `--url` / `--api-key`).
- **Results:** append-only CSVs under `results/` (columns in `egbench/config.py`).

## 1. Datasets (the size × layout axis)

`"$PY" -m egbench.cli datasets` lists the registry + on-disk status. MB = 1e6 bytes.

| Key | Layout | Entities × MB/ent | Origin |
|---|---|---|---|
| `EGRESS_PE_64K`  | per_entity | 1000 × 0.066 | synthesized |
| `EGRESS_PE_1M`   | per_entity | 1000 × 1.049 | synthesized |
| `EGRESS_PE_16M`  | per_entity | 256 × 16.777 | synthesized |
| `EGRESS_BAT_1M`  | batched    | 1000 × 1.049 | synthesized |
| `EGRESS_BAT_16M` | batched    | 256 × 16.777 | synthesized |
| `EGRESS_GRP_1M`  | grouped    | 1000 × 1.049 | synthesized |
| `EGRESS_BAT_1M_BROKER` | batched | 50 × 1.049 | same file as BAT_1M, broker mimetype — **remote only** |
| `BENCH_LARGE`    | per_entity | 1000 × 1.049 (`powder`; 4 artifacts, ~3.7 MB/ent total) | pre-existing — never regenerate |
| `VERY_LARGE_DATASET_BENCHMARK` | per_entity | 200 × 5.04 (`spectral_cube`) | pre-existing — never regenerate |

### One-off generation recipe (already done; repeat only for a new size/layout point)

Synthesis and registration are **agentic one-offs, not harness code**. The synthetic
family lives under `data-source/egress_bench/<name>/{<name>.yml, data/*.h5}`:

- Single artifact `signal`, float64, filled from `np.random.default_rng(seed)` —
  **random so the payload is incompressible** (gzip would fake wire numbers).
- Per-entity params `p0 = i`, `p1 = 0.5·i + 1` (distinct → unique content-addressed UIDs);
  layout conventions exactly as `regbench/synth.py` writers (root scalars / `/params`
  arrays / `samples/sample_NNNNNN` groups).
- Batched files are written in ≤64 MB row-chunks so 4 GB never sits in RAM.
- YAML mirrors `bench_large.yml`: `data_type: benchmark`, `layout`, and
  `server_base_dir: /prjmaiqmag01/data-source/egress_bench/<name>/data` for the shared
  server.

Registration (per dataset, per server): `tcb generate`-style manifests then
`register_dataset_http` with `mimetype = application/x-hdf5` (stock) and stable
`dataset_key` from the registry; pass `server_base_dir` **only for remote**. Verify
`len(client[KEY]) == n_entities` after.

## 2. Harness commands

```bash
PY=/sdf/data/lcls/ds/prj/prjmaiqmag01/results/cfitussi/tiled-catalog-broker/.venv/bin/python

"$PY" -m egbench.cli serve &                  # persistent local server → _egbench_ws/server.json
"$PY" -m egbench.cli datasets                 # registry + on-disk status
"$PY" -m egbench.cli run   --dataset EGRESS_PE_1M --method export_hdf5 \
      --concurrency 4 --batch 20 --n 200 --reps 3 --out results/egress.csv
"$PY" -m egbench.cli sweep --datasets EGRESS_PE_1M,EGRESS_BAT_1M \
      --methods export_hdf5,raw_read --concurrency 1,2,4,8,16 --location remote ...
```

- `--location local` reads `server.json`; `remote` uses env/flags.
- `batch` applies to `export_hdf5` only (sweep pins it to 1 elsewhere). Exports are
  chunked ≤25 keys/request regardless (proxy 414 limit) — a batch of 100 = 4 requests.
- Methods: `export_hdf5` (container re-encode path), `raw_read` (octet-stream per
  array), `h5py_direct` (filesystem floor, no HTTP; local page cache makes repeat runs
  warm — note which you're reporting).

> **Known hazards (measured 2026-07-21, see `results/EGRESS-FINDINGS.md`):** batched+stock
> costs a full file read per entity server-side — keep remote batched runs tiny (n≤8), and
> `EGRESS_BAT_16M` container export 504s through the proxy entirely. A single client
> process caps at ~165 MB/s (GIL): to probe server/wire ceilings, launch several
> `egbench run` processes concurrently and sum the rates. Export sweeps need
> units = n/batch ≫ concurrency or the ladder is unit-capped, not concurrency-limited.

## 3. Measurement protocol

1. **Layer decomposition first, at `concurrency=1`** — sweep method × dataset. The
   app/net/nav/decode sums are only cleanly interpretable serially.
2. **Concurrency ladder** (1,2,4,8,16,32) on the throughput question — big payloads
   (`EGRESS_PE_16M`, `EGRESS_BAT_16M`) × best method. Watch `wire_mbps` for the plateau
   and `app_frac` for who's saturated; `req_p95/p99` for queueing onset.
3. **Ceiling claim** needs three numbers at the plateau: remote `wire_mbps` (the wire),
   local `wire_mbps` (software ceiling, loopback), `h5py_direct` `payload_mbps`
   (storage floor). The smallest one that binds is the ceiling; say which.
4. Reps: 3 (default) — enough for medians; per-request percentiles come from within a
   run. Vary nothing else mid-sweep; the CSV records tiled_version + url per row.

## 4. Correctness guardrail (a fast-but-wrong row is a failed run)

Every valid row must have `entities == n` (requested), `errors == 0`, and
`payload_mb ≈ entities × mb_per_entity` from the registry (±1%). Rows with `[warn]`
prints, unit errors, or a machine suspend mid-run (absurd `wall_s`) are discarded and
re-run, not averaged in. Remote rows with retried chunks are valid (retries are inside
transfer time) — rows whose units *failed* after retries are not.

## 5. Reading the CSV

Key columns: `payload_mbps` (useful-data rate — the headline), `wire_mbps`
(bytes-on-socket rate), `app_frac` (server-work share; →1 server-bound, →0 wire/client-
bound), `nav_sum_s` (raw_read's metadata tax), `decode_s` (export's h5py tax),
`req_p50/p95/p99_ms` (queueing/tail). `wall = ` fan-out of units; at c=1,
`wall ≈ app_sum + net_sum + nav_sum + decode`.

**Visualization:** `docs/egress-heatmap.html` is a self-contained interactive heatmap of
every valid run (open directly in a browser; axes/color/filters are switchable, hover a
cell for the runs behind it). After appending rows to `results/*.csv`, refresh the
embedded data with `python docs/egress-heatmap.build.py`.
