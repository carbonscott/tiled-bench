# Training-shaped retrieval benchmark — findings (2026-09-20)

First campaign designed for an ML audience (plan: `TRAINING-BENCH-PLAN.md`): one fully
shuffled epoch over a real training dataset, N worker processes, samples/sec — per
retrieval path, warm and cold, every plateau attributed to a named resource. Charts:
`results/trainbench/{edrixs,nips3}_saturation.png`. Raw rows:
`results/trainbench/{edrixs,nips3,registration}.csv` (harness: `trainbench/`).

**Stamps.** TileWright `b268731` (shipped code, first campaign to measure it), tiled
client 0.2.18, server `0.2.15b1.dev14+b7aafcd1` (tiled-test), milano exclusive nodes
(direct paths + HTTP client side). Every row carries version stamps and guardrails
(sample count, byte totals, zero errors, cross-path spot sha1 vs an h5py reference).
**430 ladder rows, 0 guardrail failures**; every payload byte-identical across
transports.

**Workloads.** EDRIXS `BENCH_BATCHED_5F`: 10,000 samples, 48 KB each, batched in 5
gzip-4 HDF5 files, chunks (125,19,5). NiPS3 `BENCH_PER_ENTITY`: 7,616 samples,
~2.6 MB across 6 artifacts, one uncompressed file per sample. Sample = what SBI
training consumes (X = artifacts, Θ = params). Disclosed caps: EDRIXS default-loader
cells 2,000 samples/epoch; HTTP cells 2,000 (EDRIXS) / 1,000 (NiPS3); all other direct
cells full epochs.

## Headline numbers (samples/sec, median)

| | w=1 | w=32 | ceiling |
|---|---|---|---|
| **NiPS3 warm** (h5py = Mode A cached) | 470 | 7,200–8,800 | RAM bandwidth, 24–29 GB/s |
| **NiPS3 cold** | 61 | 1,636 | Weka ≥5.4 GB/s/node, **not yet saturated at 32 workers** |
| **NiPS3 HTTP per-sample** | 5.4 | 60 | server request wall |
| **EDRIXS default loader** (all direct paths) | 40 | ~1,100 | gzip band decompress, ~25 ms/sample/worker |
| **EDRIXS tuned loader** (hoisted handles + 256 MB rdcc) | ~3,000 | ~4,400 | epoch fixed costs; decompress-once |
| **EDRIXS HTTP per-sample** | 5.1 | 72 | server request wall |

## What this settles

1. **TileWright costs nothing at training time.** Mode A with cached handles is
   indistinguishable from hand-rolled h5py at every worker count, warm and cold, both
   datasets. The locator indirection is free; adopt the catalog for discovery without
   paying anything in the epoch loop.

2. **The old "1.2–1.5 GB/s" reference was never the storage limit.** Cold shuffled
   NiPS3 reads reach **5.4 GB/s aggregate on one node at 32 workers and are still
   climbing** — the prior single-process warm number was a client-side bound. The true
   per-node Weka ceiling remains above 5.4 GB/s (multi-node extension would be needed
   to find it; not run).

3. **Do not train over HTTP.** Per-sample HTTP tops out at 60–72 samples/s per *server*
   regardless of client parallelism — 16–27× below one node's direct cold reads, and it
   is a shared resource. The wall is consistent with the egress campaign's ~20-slot
   model: EDRIXS artifact_read = 2 requests/sample → 72 samples/s ≈ 144 req/s; NiPS3
   container_export = 1 heavy request → 60/s. HTTP remains the right tool for
   *download-then-train* (19.4 GB at egress-measured wire rates ≈ 15 s–4 min, once) and
   for browsing/visualization.

4. **On gzip-chunked batched files, the loader — not the storage — is the entire
   game.** Every shuffled 48 KB read decompresses a ~6 MB chunk band: a naive loader
   (or any loader that reopens per sample) is pinned at ~40 samples/s/worker, warm or
   cold. Two lines of loader code — hoist `f[name]` out of the loop and open with
   `rdcc_nbytes=256 MB` — give **~70× at one worker** (40 → ~3,000/s). The HDF5 chunk
   cache belongs to the *dataset handle*: a fresh `f[dset]` per sample starts empty,
   which is why `rdcc` alone did nothing until the handle was hoisted (the first
   ladder's rdcc256 rows record that trap; `notes=dsid_cache_fix` rows supersede them).

5. **Cache state matters only when I/O is the bottleneck.** NiPS3: warm/cold is
   7.7× at w=1 (RAM vs Weka). EDRIXS: warm ≈ cold everywhere — CPU decompression hides
   the storage entirely. "Fits in RAM" training lives on the warm lines from epoch 2.

## Discovery (time to a training set)

| route | EDRIXS (batched) | NiPS3 (per-entity) |
|---|---|---|
| plain user (glob + harvest params from HDF5) | **0.02 s** (params are 5 vectors) | 40 s warm on-node; 64 min cold on a contended login node |
| TileWright `query_catalog` sweep | 25.7 s | **15.7 s** |
| Parquet manifest (data-model campaign) | ~5 ms | ~5 ms |

Per-layout honesty: for batched layouts the plain user already has cheap discovery;
the catalog's win is per-entity layouts (and, everywhere, the pre-assembled Θ +
locators + queryability). The manifest file remains the floor by ~3 orders.

## Registration byproducts (results/trainbench/registration.csv)

- **Real datasets, shipped TileWright, timed:** EDRIXS 10,000 entities in **351 s**
  (28.5 ent/s); NiPS3 7,616 entities × 7 nodes in **799 s** (13.3 min, ~67 nodes/s).
  The BENCHMARK-PLAN "projected ~6 h" headline is settled by measurement: **minutes.**
- **Pool-8 race check: clean.** workers=8 (the shipped default), 60k+ artifact
  registrations, per-entity + spot per-artifact read-back verification: **0 broken
  entities** on both datasets (server 0.2.15b1.dev14). The egress-era recommendation to
  register at pool=4 is not supported on this server version; resume-on-409 was never
  exercised because nothing broke.
- **Catalog size: the "~5 MB per 10k entities" doc claim is ~6× off.** Measured
  (fresh local SQLite): 10k-entity/20k-node EDRIXS catalog = **29.2 MB**; both datasets
  (73k nodes) = 90.2 MB. ≈1.2–1.5 KB/node. Still small; docs should be corrected.
- **Manifest generation is Weka-latency-bound:** NiPS3's 7,616-file probe took 82 min
  cold on a contended login node vs **160 s warm** — same work, page cache is the
  entire difference.

## TileWright code findings (shipped `b268731`)

1. **`client.load_artifacts` reopens the HDF5 file for every row** (and per artifact
   type). Cost: 1.9× on warm NiPS3 (247 vs 470 samples/s at w=1; bulk 157 vs 470); on
   gzip-batched EDRIXS it caps *any* consumer at the 40/s decompress floor because the
   chunk cache dies with each handle. Recommendation: cache open files/datasets across
   rows (or document the hoisted-handle + `rdcc_nbytes` pattern for training loops).
2. **A missing `server_base_dir` is silent at registration and fatal at read time.**
   Registration succeeds, `len(dataset)` is right, and every array read 500s (the
   server mounts the project space at `/prjmaiqmag01/...`). The attempt-1 rows in
   `registration.csv` record this failure mode. Recommendation: `tilewright register`
   should read back one artifact per dataset and fail loudly.
3. **Data-producer guidance for batched files:** gzip-4 with chunks (125,19,5) makes
   shuffled access pay 125× read amplification. Chunk per-sample — e.g. (1,151,40) —
   or store uncompressed; EDRIXS-style data compresses the wrong axis for training.

## Protocol notes

- Cold = `posix_fadvise(DONTNEED)` per file per rep (client page cache evicted;
  verified against never-read files). Weka keeps its own backend tiers: post-fadvise
  streams ~0.9–2 GB/s, while months-untouched (tiered-out) data read at **44 MB/s** —
  a storage-tiering caveat, not a benchmark axis. HTTP rows are `ambient` (the shared
  server's cache is not ours to control).
- Paths interleaved within every (workers, cache, rep) cell in one session; per-rep
  shuffle seeds; reps 5 warm / 3 cold / 3 HTTP.
- Deliberately not measured (plan §out-of-scope): torch DataLoader integration, GPU
  utilization (composition rule: if your training step consumes fewer samples/s than
  the loader's line, the GPU never waits), multi-node ceiling hunt, Zarr/WebDataset
  shootouts, the ADR-0004 JSONB harness.
