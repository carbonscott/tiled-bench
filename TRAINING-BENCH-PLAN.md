# Training-shaped retrieval benchmark — campaign plan (2026-09-19)

Successor campaign to egress/ingress/data-model. Goal: headline numbers that are
representative for **ML engineers**, with throughput decomposed so each ceiling is
attributed to a named resource. Designed in a grill-me session; decisions below are
settled, protocol details are defaults an executor may refine.

## Motivating critique (what the prior campaigns did not show)

- No shuffled / random-access workload — everything was sequential or select-k.
- No baseline the ML engineer already uses (plain h5py over a folder).
- No end-to-end samples/sec; component numbers were never assembled into a training-shaped claim.
- The storage ceiling was never reached: warm 1.2–1.5 GB/s measures RAM+client, and the
  cold Weka probes (2.0–2.3 GB/s) were explicit lower bounds. "HTTP matches direct at
  scale" is true only for whole-file streaming (1.32 GB/s @ 32 streams), false for
  per-sample access (~20 server slots, 224–580 MB/s) — the two were never on one chart.
- Numbers measured old TCB SHAs, not shipped TileWright.

## Headline artifact

One chart per dataset: **samples/sec (y) vs worker processes (x = 1,2,4,8,16,32)** for a
fully shuffled epoch. Secondary y-axis in MB/s. Every plateau labeled with its cause
(GIL / server slots / NIC / Weka / open-tax). Warm variant solid, cold dashed.

## Workloads (both; opposite failure modes)

- **EDRIXS** — `datasets/sam_klein_v2.yml`, key `BENCH_BATCHED_5F`. 10,000 samples,
  batched layout (5 files × 2,000). Sample = (151,40) f64 spectrum ≈ 48 KB + 12 params.
  Stresses: random small slices from big shared files.
- **NiPS3** — `datasets/nips3_multimodal_v2.yml`, key `BENCH_PER_ENTITY`. 7,616 samples,
  per-entity layout. Sample = 6 artifacts (~2–3.5 MB total) + 9 params, stacked X + Θ.
  Stresses: file open per sample, multi-artifact assembly.

"Sample" = what SBI training consumes: X = spectra artifact(s), Θ = parameter vector.
Epoch = every sample exactly once, order from a seeded full per-sample shuffle.

## Lines (per dataset)

1. **plain h5py** — hand-rolled file/slice index, direct h5py reads. The user's world
   today; its cold plateau is the storage/NIC ceiling.
2. **TileWright Mode A** — shipped `tilewright.client.query_catalog` +
   `load_artifacts` (or `locate`/`load`), stamped with the TileWright SHA. Claim: sits on
   line 1 (zero training-time overhead). Note: `load_artifacts` iterates rows and may
   reopen files on batched layout — if line 2 sags below line 1 on EDRIXS, that is a
   reportable finding, not a benchmark bug.
3. **HTTP per-sample** — tiled client array read per sample against the shared server
   (EDRIXS: artifact_read of the slice; NiPS3: container_export per entity, per egress
   rankings). Expected to wall at ~20 in-flight slots; the chart shows that honestly.
4. *(annotation only)* **download-then-train** — one-time asset_bytes fetch cost quoted
   from the egress campaign; afterwards the user lives on line 1.

## Cache-state protocol

- **Warm**: one untimed priming epoch, then timed epochs. Steady-state training number
  (both datasets fit in a 376 GB node's page cache).
- **Cold**: `os.posix_fadvise(POSIX_FADV_DONTNEED)` over every file before each rep,
  cross-checked at least once per dataset against never-before-read fresh copies (Weka
  client cache may survive fadvise; if the two disagree, fresh copies are the truth and
  fadvise reps are discarded).
- Advice is stated conditionally: fits-in-RAM → solid line after epoch 1; larger than
  RAM → dashed line throughout.

## Harness

Bare multiprocess loop (no torch in the measured path): N processes, disjoint shuffled
index shards, samples/sec aggregated. A torch DataLoader wrapper was deliberately
deferred — add later as a single confirmation point if an ML audience asks. GPU
utilization is not measured; state the composition rule instead ("loader delivers X
samples/s; if your step time consumes fewer, the GPU never waits").

## Protocol invariants (carried over from prior campaigns)

- Stamp every row: tilewright SHA, tiled client + server versions, node, dataset key,
  cache state, worker count, population.
- Guardrail every row: sample count, byte totals, zero errors, per-sample spot checksums
  vs a sequential reference pass. Fast-but-wrong = failed run; keep row, exclude from claims.
- Reps: ≥5 for small cells, interleave paths within one session (kills the
  different-nights confound the critique flagged); large/cold cells ≥3.
- Runs from milano exclusive nodes; HTTP line hits the shared tiled-test deployment.
- Contingency: if cold plain-h5py has not plateaued at 32 procs on one node, extend the
  ladder across 2–4 nodes to actually find the Weka/NIC ceiling.

## Bundled credibility items (byproducts)

- **Timed, stamped registration** of both datasets with shipped TileWright = the
  never-run BENCHMARK-PLAN.md headline ("real 7,616-entity registration in X min"),
  measured not extrapolated, and links numbers to a TileWright version.
- **Catalog DB size** measured after registration (tests the documented "~5 MB per 10k
  entities" claim).
- **Pool-default race check**: register at workers=8 with shipped TileWright
  (`_DEFAULT_MAX_WORKERS = 8`), verify every artifact of every entity, and determine
  whether resume-on-409 self-heals the known registration race. Outcome: either evidence
  the shipped default is safe, or a default change to 4 + doc note.

## Explicitly out of scope (deferred, with reasons)

- **JSONB/data-model 2×2 harness** (ADR-0004 reproducibility): own campaign; defends an
  already-made design decision.
- **Format shootout** (Zarr/WebDataset/HF datasets): plain h5py is the baseline users
  actually have; HDF5 is fixed upstream by the simulation outputs.
- **torch DataLoader / GPU-util measurements**: see Harness.
