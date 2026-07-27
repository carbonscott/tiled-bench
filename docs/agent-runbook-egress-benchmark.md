# Agent Runbook — Egress (read-path) benchmark

**You are an agent characterizing Tiled egress performance with the `egbench` harness.**

The deliverable is **not** "what is the max throughput." It is a recommendation table a
user can act on:

> *Given what you are trying to do, and how your data was registered, which retrieval
> method should you use — and what does choosing wrong cost you?*

The four user workloads that table must answer:

| # | Workload | What matters |
|---|---|---|
| 1 | Plot one artifact, interactively | latency of a single fetch |
| 2 | Get one whole entity (all artifacts), cache it | per-entity cost including all artifacts |
| 3 | Get a batch of k entities, cache, feed an ML pipeline | throughput, and how it scales with k |
| 4 | Get the whole dataset | bulk transfer rate |

Domain language is in `CONTEXT.md` (entity / artifact / layout / dataset definitions).

---

## 0. Environment

- **Interpreter (always):** `PY=/sdf/data/lcls/ds/prj/prjmaiqmag01/results/cfitussi/tiled-catalog-broker/.venv/bin/python`
  (has `tiled`, `tiled_catalog_broker` editable, `h5py`, `pandas`).
- **cwd:** `/sdf/data/lcls/ds/prj/prjmaiqmag01/results/ajshack/tiled-bench` — `egbench`
  and `regbench` import from here.
- **Local server state:** `_egbench_ws/` (catalog.db + `server.json`). The egress catalog
  is **persistent** — datasets are registered once and reused across restarts.
- **Remote:** `https://lcls-data-portal.slac.stanford.edu/tiled-test` via
  `TILED_URL` / `TILED_API_KEY` (or `--url` / `--api-key`).

The harness has **no dataset registry**. It measures what you point it at and prints only
what it measured. Everything else — layout, artifact names, entity counts, mimetype — you
read off the server and record yourself (§5).

---

## 1. Datasets to register before testing

Register **all of these with the tcb default mimetype**, `application/x-hdf5-broker`
(leave the mimetype unset and `bulk_register.py:216` / `http_register.py:111` supply it).
The local server can serve it since `8b5db40` wired `adapters_by_mimetype` into the
generated config.

### 1a. Single-artifact family — size × layout (already synthesized; re-register broker)

| Key | Layout | Artifacts | MB/artifact | MB/entity | Entities |
|---|---|---|---|---|---|
| `EGRESS_PE_64K`  | per_entity | 1 | 0.065536 | 0.065536 | 1000 |
| `EGRESS_PE_1M`   | per_entity | 1 | 1.048576 | 1.048576 | 1000 |
| `EGRESS_PE_16M`  | per_entity | 1 | 16.777216 | 16.777216 | 256 |
| `EGRESS_BAT_1M`  | batched    | 1 | 1.048576 | 1.048576 | 1000 |
| `EGRESS_BAT_16M` | batched    | 1 | 16.777216 | 16.777216 | 256 |
| `EGRESS_GRP_1M`  | grouped    | 1 | 1.048576 | 1.048576 | 1000 |

These answer size scaling and layout, and nothing about artifact count — every entity has
exactly one artifact, so "fetch one artifact" and "fetch the whole entity" are the same
measurement. That is the gap 1b closes.

### 1b. Artifact-count ladder — NEW, required for workloads 1 and 2

**Hold MB/entity constant at 1.048576 and vary only the artifact count.** Any difference
measured across this ladder is caused by artifact count alone, not payload size.

| Key | Layout | Artifacts | MB/artifact | MB/entity | Entities |
|---|---|---|---|---|---|
| `EGRESS_PE_4X256K`  | per_entity | 4  | 0.262144 | 1.048576 | 1000 |
| `EGRESS_PE_16X64K`  | per_entity | 16 | 0.065536 | 1.048576 | 1000 |
| `EGRESS_BAT_4X256K` | batched    | 4  | 0.262144 | 1.048576 | 1000 |
| `EGRESS_GRP_4X256K` | grouped    | 4  | 0.262144 | 1.048576 | 1000 |

`EGRESS_PE_1M` (1 artifact) is the bottom rung of the per_entity ladder — do not
re-synthesize it. Name artifacts `art_00 … art_NN` so a fetch-one-artifact run can always
name `art_00`.

### 1c. Realistic mixed entity — NEW, one big artifact plus sidecars

Real detector entities are one large array plus small companions. This is where "the
server sent me four and I needed one" costs the most.

| Key | Layout | Artifacts | Composition | MB/entity | Entities |
|---|---|---|---|---|---|
| `EGRESS_PE_MIXED`  | per_entity | 4 | `primary` 16.777216 + 3 × `aux_N` 0.065536 | 16.973824 | 256 |
| `EGRESS_BAT_MIXED` | batched    | 4 | same | 16.973824 | 256 |

### 1d. Stock control — keep one, for the regression claim

| Key | Layout | Mimetype | Entities | Why |
|---|---|---|---|---|
| `EGRESS_BAT_1M_STOCK` | batched | `application/x-hdf5` (stock) | **50** | Preserves the 33× broker-vs-stock comparison. Same files as `EGRESS_BAT_1M` |

Keep it at 50 entities — stock batched costs a full file read per entity (§6), so a
1000-entity run would take ~25 minutes and hammer a shared server for no extra insight.

### Synthesis conventions (unchanged)

Single artifact per file family lives under
`data-source/egress_bench/<name>/{<name>.yml, data/*.h5}`:

- float64 from `np.random.default_rng(seed)` — **random so the payload is
  incompressible** (gzip would fake the wire numbers).
- Per-entity params `p0 = i`, `p1 = 0.5·i + 1` (distinct → unique content-addressed UIDs).
- Layout conventions exactly as the `regbench/synth.py` writers (root scalars /
  `/params` arrays / `samples/sample_NNNNNN` groups).
- Batched files written in ≤64 MB row-chunks so multi-GB arrays never sit in RAM.
- YAML mirrors `bench_large.yml`: `data_type: benchmark`, `layout`, and
  `server_base_dir: /prjmaiqmag01/data-source/egress_bench/<name>/data` for the shared
  server.

Registration: `tcb generate`-style manifests then `register_dataset_http`; pass
`server_base_dir` **only for remote**. Verify `len(client[KEY]) == entities` and
`len(client[KEY][first_key]) == artifacts` after — the second check is new and catches a
multi-artifact dataset that registered only its first artifact.

### Pre-existing sets — never regenerate

`BENCH_LARGE` (per_entity, 4 artifacts, `powder` + others, ~3.7 MB/entity, 1000) and
`VERY_LARGE_DATASET_BENCHMARK` (per_entity, `spectral_cube`, 5.04 MB/entity, 200). Both
are stock-registered. `BENCH_LARGE` is a useful real-world multi-artifact cross-check
against the synthetic ladder.

---

## 2. The five retrieval methods

Named for the Tiled API each exercises.

| Method | Tiled API | Endpoint | Requests for k entities |
|---|---|---|---|
| `container_export` | `container.export(buf, fields=[…], format="application/x-hdf5")` | `GET {container}/full?field=…` | `ceil(k/25)` |
| `artifact_read` | `node[key][artifact].read()` | `GET {array}/full`, octet-stream | k data + ~2k metadata (× artifacts if `--artifact` omitted) |
| `raw_export` | `node[key][artifact].raw_export(dest)` | `GET /asset/bytes` | **1** if batched/grouped, **k** if per_entity |
| `asset_bytes` | none — the endpoint under `raw_export` | `GET /asset/bytes` | same as `raw_export` |
| `h5py_direct` | none — filesystem | — | 0 |

What each pays:

- **`container_export`** — server reads every artifact of every requested entity and
  re-encodes into one HDF5 container; client pays an h5py parse (`decode_s`). **The wire
  carries all artifacts whether you wanted them or not.** `--artifact` controls only what
  gets decoded, not what gets transferred: omit it for workload 2, name one to measure
  workload 1's waste.
- **`artifact_read`** — one request per artifact, plus a per-entity navigation tax
  (~105 ms, 2 metadata GETs). Omit `--artifact` to fetch every artifact of the entity,
  which is workload 2 paid the expensive way.
- **`raw_export`** — the public download API. Materializes the whole file in RAM
  (default) or on disk (`--download-dir`). Nothing is decoded, so `payload_mb` is 0 and
  `wire_mb` is the number that means something.
- **`asset_bytes`** — same requests and same wire bytes as `raw_export`, streamed and
  discarded. **The gap between the two is the client library's buffering cost**, which is
  a per-chunk `BytesIO`/disk write plus a `rich` progress update on the receiving thread
  (`download.py:86-88`). Run both wherever you run either; the delta is a finding.
- **`h5py_direct`** — local filesystem read. Measures neither server nor wire, so it is a
  **reference line, not a candidate method**: measure it once per dataset to say "HTTP
  costs you N×," and do not sweep it across concurrency or selectivity. The exception is
  workload 3 on the cluster, where it is genuinely the recommendation.

---

## 3. Harness commands

```bash
PY=/sdf/data/lcls/ds/prj/prjmaiqmag01/results/cfitussi/tiled-catalog-broker/.venv/bin/python

# persistent local server → _egbench_ws/server.json
"$PY" -m egbench.cli serve --read-path /sdf/.../data-source &

# one invocation = one measurement, printed as JSON on stdout
"$PY" -m egbench.cli run --dataset EGRESS_PE_4X256K --method container_export \
      --concurrency 4 --export-batch 20 --n 200
{"wall_s": 8.31, "entities": 200, "expected_entities": 200, "errors": 0, ...}

# whole entity, the expensive way (no --artifact → every artifact)
"$PY" -m egbench.cli run --dataset EGRESS_PE_4X256K --method artifact_read --n 200

# one artifact only
"$PY" -m egbench.cli run --dataset EGRESS_PE_4X256K --method artifact_read \
      --artifact art_00 --n 200

# bulk file download; --download-dir is required for files too big to buffer
"$PY" -m egbench.cli run --dataset EGRESS_BAT_1M --method asset_bytes \
      --artifact art_00 --layout batched --n 1000

# reference line, no server
"$PY" -m egbench.cli run --method h5py_direct --layout batched --n 1000 \
      --data-dir /sdf/.../egress_bench/egress_bat_1m/data --h5-path signal
```

- `--location local` reads `server.json`; `remote` uses env/flags.
- **There is no `sweep`, no `--reps`, no `--out`** — loop in the shell so you control
  exactly what varies per row, and you own the CSV.
- `--export-batch` sets the work-unit size for `container_export`; requests still chunk at
  25 keys regardless (proxy 414 limit), so a batch of 100 is 4 requests.
- `--n` is optional for per_entity `h5py_direct` (counted from files on disk) and
  **required** for batched/grouped, where the count lives inside the shared file.
- `--layout` is required for `raw_export`, `asset_bytes` and `h5py_direct`. For the first
  two it sets the request count; for the third it sets how entities are indexed.

---

## 4. Measurement protocol — run in this order

**Step 0 — reference lines.** One `h5py_direct` run per dataset, `concurrency=1`. Record
and set aside. Everything later is quoted as a fraction of this.

**Step 1 — layer decomposition at `concurrency=1`.** Every method × every dataset, n
fixed (200 is plenty; 8 for anything stock-batched). The app/net/nav/decode split is only
cleanly interpretable serially. This is what tells you *why* a method is slow, not just
that it is.

**Step 2 — the artifact-count question (workloads 1 and 2).** On the 1b ladder
(1 → 4 → 16 artifacts at constant 1.048576 MB/entity), `concurrency=1`, n=200:

- `container_export` with `--artifact` omitted vs `artifact_read` with `--artifact`
  omitted → **workload 2**: whole entity, both ways.
- `container_export --artifact art_00` vs `artifact_read --artifact art_00` →
  **workload 1**: one artifact. Watch `wire_mb` on the export side — it carries all
  artifacts either way, so `wire_mb / payload_mb` is the waste factor, and it should grow
  with the artifact count.

Then repeat on `EGRESS_PE_MIXED`, where one artifact is 99% of the entity — the waste
factor should be near 1.0 when you want `primary` and near 256× when you want an `aux_N`.

**Step 3 — the selectivity question (workloads 3 and 4).** Fix one dataset, sweep
`--n` = 1, 2, 5, 10, 25, 50, 100, 250, all across `container_export`, `artifact_read` and
`asset_bytes`, `concurrency=1`. Do this on a **batched** and a **per_entity** dataset.

This draws three curves against "how much of the dataset do you want," and **where they
cross is the user guidance.** Expect `asset_bytes` on batched to be flat (it moves the
whole file regardless of k) and the other two to rise with k — so `asset_bytes` loses
badly at k=1 and wins at k=N. Find the crossover; it is the headline number.

**Step 4 — client overhead.** `raw_export` vs `asset_bytes`, same dataset, same n, both
concurrencies 1 and 8. If they match, quote `raw_export` to users. If `raw_export` is
materially slower, that is an upstream bug report — name the per-chunk progress update
and `BytesIO` regrowth as suspects.

**Step 5 — concurrency ladder.** 1, 2, 4, 8, 16, 32 on the winning method per workload,
big payloads (`EGRESS_PE_16M`, `EGRESS_BAT_16M`). Watch `wire_mbps` for the plateau,
`app_frac` for who saturates, `req_p95/p99` for queueing onset. Note that `--concurrency`
is **inert** for `raw_export`/`asset_bytes` on batched/grouped — one asset means one work
unit, so `app_frac` divides by workers that aren't working. Do not sweep it there.

**Step 6 — the stock regression.** `EGRESS_BAT_1M_STOCK` vs `EGRESS_BAT_1M`,
`artifact_read`, n=8, `concurrency=1`. Confirms the broker fix still holds and quantifies
what registration mimetype is worth.

**Reps: 3** for every point. Run the same command three times, write three rows with a
`rep` column. Vary nothing else mid-sweep.

---

## 5. Storing results — what to write and what matters

`egbench run` prints **only what it measured**. You compose the row. One CSV row per
(config × rep).

### Columns you fill in

| Column | Source |
|---|---|
| `dataset_key`, `method`, `concurrency`, `export_batch`, `n_entities`, `rep` | the command you ran |
| `artifact` | what you passed, or `ALL` when omitted — **never leave blank**, blank is ambiguous between "one" and "all" |
| `location` | `local` \| `remote` |
| `layout` | `per_entity` \| `batched` \| `grouped` — from `parameters["slice"]` present ⟺ batched |
| `n_artifacts` | `len(client[KEY][first_key])` |
| `mb_per_artifact`, `mb_per_entity` | `shape × itemsize / 1e6`, summed over artifacts |
| `stack_size` | registration batch: `max(slice)+1`, or `index_<artifact>` on entity metadata. Blank unless batched |
| `mimetype` | `data_sources()[0].mimetype` — **required on every row** |
| `tiled_version`, `url` | from the client |

### Columns copied verbatim from the JSON

`wall_s`, `entities`, `expected_entities`, `errors`, `payload_mb`, `wire_mb`,
`req_p50_ms`, `req_p95_ms`, `req_p99_ms`, `app_p50_ms`, `app_sum_s`, `net_sum_s`,
`nav_sum_s`, `decode_s`.

Copy the JSON, do not retype the numbers.

### Do NOT store — derive at analysis time

`payload_mbps = payload_mb / wall_s` · `wire_mbps` · `ent_per_s = entities / wall_s` ·
`app_frac = app_sum_s / (wall_s × concurrency)` · `waste = wire_mb / payload_mb`

Stored rates go stale the moment a count is recomputed. Derive them in pandas.

### File layout

```
results/
  stock-2026-07/…                 # frozen; stock mimetype, do not append
  broker/step1-layers.csv
  broker/step2-artifacts.csv
  broker/step3-selectivity.csv
  …
```

**Never append broker rows to a stock-era file.** They are different populations —
batched differs by ~33× — and averaging across them produces a meaningless number.

### What actually matters when reading the results

- **`payload_mb` means different things by method.** For `container_export` /
  `artifact_read` / `h5py_direct` it is decoded array bytes. For `raw_export` /
  `asset_bytes` **nothing is decoded, so it is 0 by construction** — use `wire_mb` there.
  Never compute `payload_mbps` for those two.
- **`wire_mb / payload_mb` is the waste factor** and it is the headline for workload 1.
  1.000 means you transferred exactly what you needed.
- **`app_frac` → 1** means the server is the ceiling; **→ 0** means the wire or client is.
  It is meaningless when concurrency is inert (see Step 5).
- **`nav_sum_s`** is `artifact_read`'s metadata tax and it multiplies by artifact count.
- **`decode_s`** is `container_export`'s client-side parse cost. It is real for a user but
  it is *client* work — separate it from server+wire when arguing about the server.

---

## 6. Correctness guardrails — a fast-but-wrong row is a failed run

Every valid row must satisfy:

1. `entities == expected_entities`
2. `errors == 0`
3. `payload_mb / entities` matches the entity size you recorded (±1%) — **except** for
   `raw_export` / `asset_bytes`, where instead `wire_mb` must match the on-disk file size
   (`ls -l`) times the number of downloads
4. `n_artifacts` recorded matches what the dataset was registered with

Discard and re-run: rows with `[unit error]` prints, or an absurd `wall_s` (machine
suspend). Remote rows with *retried* chunks are valid — retries are inside transfer time.
Rows whose units **failed** after retries are not.

---

## 7. Known hazards

- **Stock + batched is pathological.** The stock adapter reads the *entire* file per
  entity (`lazy_hdf5.py:5-8`): 1.4 s/entity on a 1 GB file, 5.2 s/entity on 4.3 GB,
  `app_frac` 96–98%, flat under concurrency. Container export through the proxy 504s
  outright. Broker fixes it — 95 ms vs 3,141 ms per entity, **33×**. This is why
  everything is registered broker and why the stock control is capped at 50 entities.
- **A single client process caps at ~165 MB/s** (GIL + decode), local and remote alike.
  To probe server/wire ceilings, launch several `egbench run` processes and sum the rates;
  scale by process, not threads, beyond c≈8.
- **`asset_bytes` on batched moves the whole file regardless of `--n`.** Asking for one
  entity out of `EGRESS_BAT_16M` transfers 4.3 GB. That is the measurement, not a bug.
- **`raw_export` in RAM needs the whole file.** `EGRESS_BAT_16M` is 4.3 GB — use
  `--download-dir` or skip it, and record which.
- Export sweeps need units = n/`--export-batch` ≫ concurrency, or the ladder is
  unit-capped rather than concurrency-limited.
- Remote is a shared server. Tail latencies show occasional 1.5–2.6 s stalls; the
  >0.5 GB/s probe pulled 17 GB.
- `h5py_direct` is page-cache-warm on repeat runs. Note cold vs warm in the row.

---

## 8. Deliverable

A recommendation table with one row per (workload × layout), naming the method to use,
the measured advantage over the runner-up, and the cost of choosing wrong. Plus the
crossover from Step 3 stated as a rule of thumb — "below k ≈ X entities use A, above it
use B."

Findings prose goes in `results/EGRESS-FINDINGS.md`. The interactive heatmap is
`docs/egress-heatmap.html`; refresh its embedded data with
`python docs/egress-heatmap.build.py` after appending rows.
