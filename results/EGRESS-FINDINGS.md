# Egress benchmark — findings (2026-07-27, broker campaign)

Measured with the rewritten `egbench` harness (`fee61ee`) per
`docs/agent-runbook-egress-benchmark.md`. Raw rows: `results/broker/step{0..6}-*.csv`
(one row per config × rep; rates derived at analysis time, never stored). The 2026-07-21
stock-era campaign is frozen in `results/stock-2026-07/` — **different population, do not
mix**. MB = 1e6 bytes. Medians of 3 reps unless noted.

**Platform (re-baselined, not inherited):** client = tiled **0.2.13** in an isolated venv
(both broker venvs actually hold **0.2.9** — see "Version drift" below); local server =
tiled 0.2.9, SQLite, loopback (`egbench serve`, broker adapter wired); remote = tiled-test
via the lcls-data-portal proxy, server **0.2.10b5.dev3**. Every synthetic dataset is
registered with the broker mimetype (`application/x-hdf5-broker`) on both servers; the
stock rows in `step6-stock.csv` are the deliberate control.

---

## The recommendation table

One row per workload × layout. "Cost of wrong" = median wall-clock of the runner-up ÷
winner at the measured config, remote, c=1 unless stated.

| # | Workload | Layout | Use this | Runner-up costs you | Notes |
|---|---|---|---|---|---|
| 1 | Plot one artifact, interactively | any, artifact ≈ entity (single/uniform) | `container_export` naming the artifact | `artifact_read` 3.5–6× slower (~190 ms vs ~35–55 ms/fetch) | export wire-waste = artifact count (§step 2): fine at 1–4 artifacts, think twice at 16 |
| 1 | Plot one **small** artifact of a mixed entity (16.7 MB primary + 64 KB aux) | any | `artifact_read` | `container_export` 1.7× slower and moves **259×** the bytes | the only serial cell artifact_read wins |
| 2 | Whole entity, cache it | per_entity / batched / grouped | `container_export` (omit `--artifact`) | `artifact_read` 9–13×, growing with artifact count | 1 art: 9.4×; 4: 12×; 16: 13× |
| 3 | Batch of k entities → ML pipeline | batched | k ≲ 45% of file: `container_export`; above: `asset_bytes` whole file | crossing wrong way costs up to 13× (k=1000: 170 s read vs 12.7 s file) | **crossover k ≈ 430/1000 vs export, k ≈ 67/1000 vs artifact_read** (1 MB entities, 1 GB file) |
| 3 | Batch of k | per_entity | `container_export`; scale with concurrency c≈8–16, then processes | `artifact_read` ~6× at c=1, converges at c≥8 (both plateau ~140 MB/s) | on-cluster: `h5py_direct` is 8–10× the whole-process HTTP plateau |
| 4 | Whole dataset | batched / grouped | `asset_bytes` / `raw_export` (one file = one GET; 83–90 MB/s single stream) | `container_export` 2.1–2.7× slower serially | `raw_export` ≈ `asset_bytes` (+5% at c=1, nil across processes) |
| 4 | Whole dataset | per_entity | `container_export` at c=8 (140 MB/s), or N processes × raw download (8 procs → 224 MB/s) | serial anything: 3–7×; thread scaling caps at ~145 MB/s/process (GIL) | `raw_export` cannot thread at all (upstream bug, below) — scale by process |

Rules of thumb a user can carry:

- **Serially, `container_export` wins everything** except tiny-artifact-of-huge-entity.
  Its per-entity cost is ~35 ms at 1 MB (25-key chunked requests amortize per-request
  overhead); `artifact_read` pays a flat ~190 ms/entity navigation tax (2 metadata GETs +
  1 data GET), `asset_bytes`/`raw_export` on per_entity pay ~250 ms (3 nav requests + GET).
- **Batched crossover:** fetching a subset of a batched file by entity costs
  ~27–190 ms/entity; pulling the whole backing file moves everything at wire speed.
  With 1 MB entities in a 1 GB file the lines cross at **k ≈ 430 (~45% of the file)**
  against `container_export`, k ≈ 67 against `artifact_read`. The fraction, not the
  absolute k, is the portable number: whole-file wins when you want ≳ 45% of the file
  (and sooner if you'd otherwise use `artifact_read`).
- **One Python process tops out ~145 MB/s** (c=8–16 plateau, `app_frac` ≤ 0.13 — the
  client is the ceiling, not the server). Need more: fan out processes (8 → 224 MB/s
  measured on raw downloads; the stock-era campaign sustained ~580 MB/s at 8 procs).
- **On the cluster, Mode A (`h5py_direct`) is the real bulk answer**: 1.2–1.5 GB/s warm
  against big contiguous arrays — ~10× the single-process HTTP plateau. Small files
  (64 KB) drop it to ~30 MB/s: file-open cost dominates, so layout matters more than
  protocol there.

## Step 2 — the artifact-count question (workloads 1–2)

Constant 1.048576 MB/entity, artifact count 1 → 4 → 16 (per_entity, remote, n=200):

| arts | `container_export` ALL | `artifact_read` ALL | ratio | `artifact_read` one art |
|---|---|---|---|---|
| 1 | 6.6 s | 62 s | 9.4× | 40 s |
| 4 | 11.6 s | 140 s | 12× | 38 s |
| 16 | 35.5 s | 461 s | 13× | 37 s |

- `artifact_read`'s whole-entity cost is linear in artifact count (one request + nav per
  artifact); fetching **one** artifact is flat (~38 s / 200 = 190 ms) regardless of count.
  **Null result:** artifact count does not change single-artifact latency at all.
- `container_export` rises sub-linearly (6.6 → 35.5 s for 16× the artifacts).
- Waste factor (`wire_mb/payload_mb`) when exporting one artifact is exactly the artifact
  count: 4.0 and 16.3 measured — the wire carries every artifact regardless. On the MIXED
  datasets the extremes hold as predicted: exporting `primary` wastes nothing (1.01×),
  exporting one `aux` moves **259×** the bytes you asked for — and that is the one place
  `artifact_read` wins serially (9.0 s vs 15.2 s, n=50).
- Layout cross-check at 4 artifacts: batched ≈ grouped ≈ per_entity for both methods
  (11.4–12.5 s export ALL; 139–142 s read ALL) — with the broker mimetype, layout no
  longer moves method cost at this size. (Another useful null.)
- Local (loopback) cross-check agrees everywhere except two wire-sensitive cells:
  (a) the mixed-aux `artifact_read` win vanishes locally (8.4 s vs 8.3 s — waste costs
  nothing when the wire is free), so that row of the table is a *remote* recommendation;
  (b) at 16 artifacts, one-artifact `container_export` loses even serially on loopback
  (47 s vs 32 s) — the server's 16-artifact encode, not the wire, is what it pays there.

## Step 3 — selectivity and the crossover (workloads 3–4)

`EGRESS_BAT_1M_BROKER` (1000 × 1 MB in one 1.05 GB file) and `EGRESS_PE_1M_BROKER`,
remote, c=1, k = 1 … 1000 (medians, seconds):

| k | BAT export | BAT read | BAT asset_bytes | PE export | PE read | PE asset_bytes |
|---|---|---|---|---|---|---|
| 1 | 0.08 | 0.19 | 12.8 | 0.08 | 0.18 | 0.22 |
| 50 | 1.68 | 9.5 | 12.5 | 1.54 | 9.6 | 11.5 |
| 250 | 8.0 | 48.5 | — | 7.9 | 47.6 | 59.7 |
| 1000 | 27.2 | 170.5 | 12.7 | 34.5 | 195.7 | 236.3 |

- `asset_bytes` on batched is **flat** (whole 1.05 GB file at ~83 MB/s regardless of k) —
  measured at k ∈ {1, 50, 1000} only, deliberately: the flatness is structural.
- Interpolated crossovers: **k ≈ 433 vs `container_export`** (the operative one),
  k ≈ 67 vs `artifact_read`.
- On per_entity there is no crossover: every method scales with k and `container_export`
  leads at every point (export ~34 ms/ent; whole-file download of per-entity files is the
  worst of all — nav per file plus no amortization).

## Step 4 — `raw_export` vs `asset_bytes` (the client-library question)

Same skeleton (main-thread one-off for both), `EGRESS_PE_16M_BROKER`, n=48, remote:

| | `asset_bytes` | `raw_export` (RAM) | penalty |
|---|---|---|---|
| 1 process | 21.1 s | 22.2 s | **+5.4%** |
| 8 processes (6 ents each) | 3.76 s | 3.55 s | −5.6% (noise) |

The materialization cost (per-chunk `BytesIO` write + rich progress update) is ~5% at
16 MB files and vanishes under process parallelism: **quote `raw_export` to users** — the
public API is not meaningfully slower than its own transport. Both moved byte-identical
wire totals (805.4 MB, = 48 × on-disk file size).

**But an upstream bug bars threaded use entirely:** `tiled.client.download.download()`
installs a SIGINT handler unconditionally (`download.py:149` in 0.2.13, `:98` in 0.2.9);
`signal.signal` raises `ValueError` off the main thread, so any `raw_export` driven from a
worker pool (egbench's included) dies immediately. Scale `raw_export` by **process**, not
thread. Worth filing upstream: guard with `threading.current_thread() is main_thread()`.
Second, smaller item: the in-RAM `MutableMapping` destination only exists from ≥0.2.10 —
0.2.9 clients are path-only, so the harness's RAM mode is version-gated.

## Step 5 — concurrency (who is the ceiling?)

`EGRESS_PE_16M_BROKER`, remote, n=32, payload MB/s (median):

| c | export (batch=1) | artifact_read | app_frac (read) |
|---|---|---|---|
| 1 | 51 | 37 | 0.20 |
| 4 | 123 | 106 | 0.16 |
| 8 | 140 | 134 | 0.12 |
| 16 | 132 | **147** | 0.08 |
| 32 | 129 | 144 | 0.07 |

Plateau ~140–147 MB/s per client process with `app_frac` → 0.07: the **client process is
the ceiling** (GIL + memcpy), not tiled-test. Broker-batched `artifact_read` at c=8 hits
the same 141 MB/s — the lazy-slicing fix scales under concurrency. Export declines past
c=8 (decode contention on the receiving process). This re-baselines the stock-era
"~165 MB/s process cap" to ~145 MB/s on this client stack; same conclusion, scale by
process beyond c≈8 (8 procs → 224 MB/s in step 4; ~580 MB/s was reached at 8 procs in the
stock-era campaign and was not re-run here).

## Step 6 — the stock-mimetype regression control

`artifact_read`, batched 1 MB × n=8, c=1 (same files, only registration mimetype differs):

| | stock | broker | ratio |
|---|---|---|---|
| remote | 4.82 s/ent | 0.186 s/ent | **26×** |
| local | 1.38 s/ent | 0.193 s/ent | 7× |

The stock adapter still reads the whole backing file per entity slice; the broker fix
holds (was 33× on the 0.2.9-era measurement — same order, re-baselined). Broker also
un-breaks what used to fail outright: `container_export` of `EGRESS_BAT_16M` through the
proxy, which 504'd on every chunk stock, now delivers 839 MB in ~16 s. Stock also taxes
**per_entity** container export: 60 ms/ent stock vs 36 ms/ent broker remote (~1.7×) —
mimetype is not a batched-only concern.

## Storage floor (step 0, context for all of the above)

Warm `h5py_direct`, local: 1.2–1.5 GB/s for ≥16 MB contiguous reads; ~0.4–0.5 GB/s for
1 MB-per-file; ~30 MB/s at 64 KB-per-file (open cost dominates). Cold-cache floors were
not measurable (no root; every file had been touched this session) — all quoted floors
are warm and therefore *upper* bounds on storage, which only strengthens the "client is
the ceiling" attribution.

## Version drift (unknown #3, resolved)

The runbook said "the venv is now 0.2.13"; **both** broker venvs actually pin tiled 0.2.9
(`uv.lock` included), and 0.2.9's `raw_export` API cannot run the harness's code (dict
destination + thread use). The campaign used a scratch 0.2.13 client venv so the harness
ran as written; per-row `tiled_version`/`server_version` columns record the real stack.
If the shared venvs get upgraded, the raw_export RAM path starts working on them — the
SIGINT/thread bug does not go away at any released version through 0.2.14.

## Registration race — new scaling evidence (side finding)

The known 500→retry→409 race (structure-less nodes that 500 on every later touch) scales
with per-entity artifact count and shared-asset contention at pool=8:
PE 16-artifact: **51/1000 entities broken (5.1%)**; batched 4-artifact: 13+3/1000;
grouped 4-artifact: 1+1/1000; per-entity 4-artifact: 0 local, 0 remote; single-artifact
datasets historically ~0.1%. At pool=4 the re-registrations produced zero 500s.
Detection subtlety that cost this campaign an hour: a single-shot 500 probe
**false-positives under load** (transient read 500s heal on retry) — a broken-node scan
must retry each 500 several times before flagging. Verification must check more than
entity counts: `len(ds)` was correct in every damaged dataset; the damage is only visible
per-artifact (`len(entity)` + a read). Both harness-level guardrails (`entities ==
expected`, `errors == 0`) caught the damage downstream, which is what they are for.

## Caveats

- Remote rows share the proxy with real users; `req_p95` shows occasional 1.5–2.6 s
  stalls. `EGRESS_BAT_16M` bulk cells are single-rep by deliberate budget (4.3 GB/pull);
  16M-entity cells ran n=50 remote (vs n=200) for the same reason — noted per row.
- Local step-1/2 rows ran while a remote sweep shared the client host (different server,
  c=1 each; CPU contention negligible on this node).
- `payload_mb` is 0 by construction for `asset_bytes`/`raw_export` (nothing decoded);
  their numbers are `wire_mb`-based throughout. Never compute payload rates for them.
- h5py_direct floors are warm-cache; treat as storage upper bounds.
