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
| 3 | Batch of k entities → ML pipeline | batched / grouped | below the crossover fraction: `container_export`; above: `asset_bytes` whole file. **Crossover ≈ 23–66% of the file depending on entity size and artifact count** (table in step 3) | crossing wrong way costs up to 13× (k=1000: 170 s read vs 12.7 s file) | 1 MB×1 art: 43%; 16.8 MB: 66%; 1 MB×4 arts: 23%; grouped ≈ batched (41%). vs `artifact_read` the whole file wins from 2–7% on multi-artifact/1 MB files |
| 3 | Batch of k | per_entity | `container_export`; scale with concurrency c≈8–16, then processes | `artifact_read` ~6× at c=1, converges at c≥8 (both plateau ~140 MB/s) | on-cluster: `h5py_direct` is 8–10× the whole-process HTTP plateau |
| 4 | Whole dataset | batched / grouped | `asset_bytes` / `raw_export` (one file = one GET; 83–90 MB/s single stream) | `container_export` 2.1–2.7× slower serially | `raw_export` ≈ `asset_bytes` (+5% at c=1, nil across processes) |
| 4 | Whole dataset | per_entity | `container_export` at c=8 (140 MB/s), or N processes × raw download (8 procs → 224 MB/s) | serial anything: 3–7×; thread scaling caps at ~145 MB/s/process (GIL) | `raw_export` cannot thread at all (upstream bug, below) — scale by process |

Rules of thumb a user can carry:

- **Serially, `container_export` wins everything** except tiny-artifact-of-huge-entity.
  Its per-entity cost is ~35 ms at 1 MB (25-key chunked requests amortize per-request
  overhead); `artifact_read` pays a flat ~190 ms/entity navigation tax (2 metadata GETs +
  1 data GET), `asset_bytes`/`raw_export` on per_entity pay ~250 ms (3 nav requests + GET).
- **Shared-file crossover (batched and grouped alike):** fetching a subset by entity
  costs per-entity time; pulling the whole backing file moves everything at wire speed.
  The crossover *fraction* is the portable number, and it moves with two knobs:
  **entity size pushes it up** (export amortizes overhead better on big entities:
  66% at 16.8 MB/entity vs 43% at 1 MB), **artifact count pulls it down** (23% at
  4 artifacts — export pays per artifact, the file doesn't). Working rule: single-artifact
  ~1 MB entities → whole file above ~40%; big (≥16 MB) entities → export until ~2/3;
  multi-artifact → whole file already above ~1/4. If your fallback is `artifact_read`
  rather than export, the whole file wins from 2–7% on 1 MB/multi-artifact files
  (45% at 16.8 MB).
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
  measured at 2–3 k-points per dataset, deliberately: the flatness is structural.
- On per_entity there is no crossover: every method scales with k and `container_export`
  leads at every point (export ~34 ms/ent; whole-file download of per-entity files is the
  worst of all — nav per file plus no amortization).

The full crossover picture (2026-07-28 ladders; every shared-file dataset, interpolated
between measured k-points; high-k points 1–2 reps by transfer budget, noted per row):

| Dataset | File | `asset_bytes` flat | crossover vs `container_export` | vs `artifact_read` |
|---|---|---|---|---|
| `EGRESS_BAT_1M_BROKER` (1000 × 1 MB, 1 art) | 1.05 GB | 12.7 s | k ≈ 433 (**43%**) | k ≈ 67 (7%) |
| `EGRESS_BAT_16M_BROKER` (256 × 16.8 MB, 1 art) | 4.3 GB | 52.9 s | k ≈ 170 (**66%**) | k ≈ 116 (45%) |
| `EGRESS_BAT_4X256K` (1000 × 1 MB, 4 arts) | 1.05 GB | 13.0 s | k ≈ 232 (**23%**) | k ≈ 19 (2%) |
| `EGRESS_GRP_1M_BROKER` (1000 × 1 MB, grouped) | 1.05 GB | 13.7 s | k ≈ 406 (**41%**) | k ≈ 67 (7%) |

- **Grouped ≈ batched** (41% vs 43%, and matching per-entity costs) — the whole
  crossover story transfers to grouped layout unchanged.
- **PE_MIXED bulk ranking** (per_entity 17 MB mixed entities, whole-entity fetch):
  `container_export` < `asset_bytes`-per-file < `artifact_read` at every k
  (k=256: 90 s vs 118 s vs 257 s). Downloading the backing files is a respectable
  second (1.3× export) and 2.2× better than per-artifact reads — but on per_entity
  layout the export stays the recommendation at any k.

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

## Step 7 — ceilings from a dedicated client node (2026-07-28)

Everything above was measured from a busy interactive node. Step 7 re-ran the reference
cells and pushed for the server's actual ceilings from an exclusive milano node
(sdfmilan015, 120 cores, `--exclusive`, job 33461448). Rows:
`step7-control.csv`, `step7-ceiling.csv`; flamegraphs: `results/profiles/*.svg`.

**Control — were the interactive-node numbers depressed?** Modestly, and more under
concurrency: c=1 cells improved 5–10% (PE_1M export 7.26→6.56 s), but the c=16 PE_16M
points improved 22–33% (artifact_read 3.67→2.85 s, export 4.07→2.72 s). **The
single-process plateau re-baselines from ~145 to ~190 MB/s** on a quiet node — the
prior cap was part client-host contention. Serial conclusions and every ranking in the
recommendation table are unchanged (deltas ≪ the 2–13× method gaps).

**Three distinct ceilings found:**

| Ceiling | Value | Evidence |
|---|---|---|
| Request path | **~135 req/s (~44 ent/s)** aggregate, regardless of parallelism | artifact_read on 64 KB entities: flat from 32 → 256 client streams while server-side `app;dur` p50 inflates 142→329 ms and p95 hits 6.9 s — requests queue *inside* the server |
| Bulk egress (wire + server streaming) | **≥ 940 MB/s, not yet saturated** | 16 processes streaming the same cache-hot 1.05 GB file: 316→514→857–941 MB/s at P=4→8→16, near-linear |
| One client process | ~190 MB/s (quiet node) | c=16 control points; GIL, see profiles |

**The three ceilings are one resource: ~19 concurrent request-processing slots.**
Little's law at the request ladder's onset: 135.8 req/s × 142 ms = **19.3 in flight** —
past that, offered streams only queue (in-flight grows to ~35 at 256 streams, `app;dur`
inflates, completions/s *fall*). "135 req/s" is just what 19 slots deliver when each
request holds a slot ~140 ms. Every workload's ceiling = 19 ÷ (slot-seconds per unit):

- 64 KB `artifact_read` (3 requests ≈ 0.42 slot-s/entity) → ~45 ent/s ≈ 135 req/s;
- 16.8 MB per-entity `asset_bytes` (2 nav + 1 streaming GET ≈ 0.6 slot-s/entity)
  → ~30 ent/s ≈ **the ~470–485 MB/s "bandwidth" plateau** — slot-limited, not wire-limited
  (P=64 rung single-rep, 485 MB/s; second rep lost a shard and was discarded);
- 1.05 GB whole-file streams (1 slot held ~20 s at ~54–79 MB/s each, only 0.8 req/s)
  → 16 × 59 ≈ **941 MB/s with ~3 slots to spare**; model ceiling ≈ 19 × per-stream rate
  ≈ 1.0–1.1 GB/s, consistent with no plateau observed.

The ~19 matches the stock-era "~18 concurrent worker threads" observation — likely
uvicorn/anyio workers or a DB/adapter thread pool. Raising that one deployment knob
lifts every ceiling except the wire; the Prometheus dashboard showing single-digit
utilization is consistent (a saturated thread pool, not CPU). For capacity planning: tiled-test as configured serves ~135 interactive
requests/s total, shared among all users — worth a look at server worker/DB-pool
configuration before concluding hardware is the limit.

**Client-side profiles (py-spy, on-node):** the c=16 `artifact_read` plateau is
GIL-bound in response-body handling — 54% of wall samples in
`tiled/client/array.py:_get_slice → httpx Response.read`, and the `--gil` profile shows
~35% of GIL-held time in that same stack (SSL decrypt + chunk assembly hold the GIL;
socket *waits* release it, which is why threads help up to c≈8 and then stop).
`container_export` at c=8 is the same shape (71% in `iter_bytes`/httpcore read; h5py
decode is minor). Two upstream-worthy observations: (1) `.read()` on an array client
routes through a full dask task graph even for a whole-array fetch — pure overhead
visible as a 54%-deep stack; (2) response streaming could release the GIL more (e.g.
`recv_into` a preallocated buffer) — but the practical client answer remains: scale by
process, ~190 MB/s each.

## Step 7b — after the worker scale-up (2026-08-03): faster aggregate, worse behavior

The deployment was scaled 8 → 16 workers — and, it turns out, **also upgraded to tiled
0.2.14.dev18** (was 0.2.10b5.dev3), so the two axes changed together and nothing below
can be attributed to worker count alone. Same milano-exclusive job (34091273); rows in
`step7-{control,ceiling}-workers16.csv`. Rows with `errors > 0` are **kept** in the CSV
(the errors are the finding) but excluded from every rate quoted here.

| Measure | workers 8 / 0.2.10b5 | workers 16 / 0.2.14.dev18 |
|---|---|---|
| Request-path ceiling | ~135 req/s, flat under any overload | **~195 req/s peak (1.45×)** at 64 streams — then *collapses* to ~112 at 256 streams |
| Overload behavior | graceful: latency inflates, zero errors | **sheds load: HTTP 500s** on `/asset/bytes` and metadata from ~16 concurrent streams up; 42% throughput collapse; P=64 rung aborted |
| Whole-file streams | 941 MB/s @ 16 streams, clean | 583 @ 8 clean; **1.2–1.5 GB/s @ 32** but every ≥16-stream rep dropped ~1 stream to 500s |
| Distinct-file (per-entity) ladder | clean to P=64, ~470–485 MB/s | **no clean rung at P ≥ 8** — every rep lost 1–14 entities to 500s |
| Serial controls | — | **7–24% slower** (e.g. PE_1M export 6.56→8.06 s); the 8-proc control +150%, inflated by the tiled client's transparent retry-with-backoff against intermittent nav 500s |

Reading:

- The throughput gain is real but sub-linear (1.45×, not 2×), and peak effective
  in-flight only moved ~19 → ~21: the serialized resource was never just worker count —
  something behind the workers (DB connection pool, event loop, catalog) still binds.
- The reliability regression is the headline. The 500s were reproduced and captured:
  server-side `500 Internal Server Error` on `/asset/bytes` under ~16 concurrent
  streams (13/14 healed by client retries, which is why serial users see latency, not
  errors). Correlation IDs are in the server logs — worth pulling the actual exception;
  16 workers × connection-pool size vs Postgres limits is the first suspect, the
  0.2.14.dev18 upgrade itself the second.
- Cold-start note: the first rung after the pod restart was an outlier (95 MB/s /
  7 ent/s first reps) — fresh caches; medians absorb it.
- Net for users right now: single-client work is somewhat *worse* than before the
  scale-up; aggregate multi-process work is faster but must tolerate retries. The
  pre-scale-up recommendation table is unchanged (method rankings are unaffected).

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
