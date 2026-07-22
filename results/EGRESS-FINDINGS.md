# Egress benchmark — findings (2026-07-21)

Measured with the `egbench` harness (see `docs/agent-runbook-egress-benchmark.md`).
Raw rows: `local_c1.csv`, `local_conc.csv`, `remote_c1.csv`, `remote_conc.csv`,
`mp_*` (multi-process probes). Local = fresh `tiled serve catalog` (SQLite, loopback)
on sdfiana004; remote = tiled-test via the lcls-data-portal proxy. Tiled 0.2.9,
stock `application/x-hdf5` mimetype except where noted. MB = 1e6 bytes.

## Where the ceiling is

| Ceiling | Value | Evidence |
|---|---|---|
| Storage floor (`h5py_direct`) | ~450 MB/s per-file, **1.1 GB/s** batched/16M files | local c=1, warm cache |
| Single client process (any method) | **~165 MB/s** | local and remote plateau at the same number with server mostly idle (app_frac ≤ 0.17); Python GIL + decode |
| Local server, container export | **~150–200 MB/s** | 4-process probe: per-request `app;dur` inflates under load, aggregate stalls while storage is 5× faster — HDF5 re-encode is the bottleneck |
| Remote aggregate (the wire question) | **~550–600 MB/s (~4.5 Gbps)** | 8 processes × c=8 raw_read: 578 MB/s sum of overlapped rates, sublinear 4→8 scaling (480→578), server showing ~18 concurrent worker-threads |

So: no single consumer will ever see the wire — the per-process software cap (~165 MB/s)
binds first, then the server's encode/thread capacity. The wire itself carries ≥ 0.5 GB/s.

## Layout is the dominant axis (stock mimetype)

Per-entity server cost of serving one slice of a **batched** (axis-0-stacked) dataset
equals a **full read of the whole file**: 1.4 s/entity for the 1 GB file, 5.2 s/entity for
the 4.3 GB file (≈ file_size ÷ 800 MB/s), app_frac 96–98%, flat under concurrency —
requests just queue. Consequences at 16 MB/entity: 3.2 MB/s egress locally (350× below
the storage floor), and container export **fails outright through the proxy** (504 on
every chunk — server can't finish inside the gateway timeout; 0 entities delivered).

**per_entity** and **grouped** are healthy and within ~25% of each other — the problem is
specifically index-sliced stacked datasets, not shared files.

**The broker mimetype fixes it**: `application/x-hdf5-broker` (lazy slicing) on the same
batched file, same server (remote): **95 ms/entity vs 3,141 ms/entity stock — 33×**,
i.e. batched becomes equivalent to per_entity. Registered as `EGRESS_BAT_1M_BROKER`.
Note the inversion: batched is the *cheapest* layout to register (asset dedup) and the
*fastest* for direct h5py access, but unusable for stock Tiled egress.

## Method choice (per_entity, remote)

- **Container export** amortizes per-request overhead: at 1 MB/entity it beats raw_read
  4.6× serially (22 vs 4.8 MB/s). Its cost is server encode (app-bound at small sizes)
  plus client h5py decode (which is what caps the client process at high concurrency).
- **raw_read** pays a ~105 ms/entity **navigation tax** (2 metadata GETs per entity,
  ~40% of it server-side work) that dominates below ~4 MB/entity serially, but it skips
  the re-encode entirely — at 16 MB/entity with concurrency it is the fastest path
  (167 MB/s single process, and it is what reached the 580 MB/s aggregate).
- Practical guidance: **many small entities → batch via export; big arrays → raw reads
  with concurrency; bulk ML ingest on the cluster → h5py_direct (Mode A), which is
  5–40× faster than any HTTP path.**
- Navigation could be trimmed by resolving handles via one `search` call instead of
  per-entity metadata GETs — untested, worth filing as a client-side optimization.

## Size scaling (per_entity, export, c=1)

| MB/entity | local MB/s | remote MB/s |
|---|---|---|
| 0.066 | 1.7 | 2.0 |
| 1.05 | 24 | 22 |
| 16.8 | 91 | 51 |

Fixed ~40 ms/entity server overhead amortizes with payload; local and remote track each
other until the payload is big enough for the WAN to matter.

## Registration bug found along the way

A transient 500 during artifact create, retried by TCB, lands on a 409 and leaves a
**structure-less array node** that 500s on every subsequent touch (`structure is None` in
`links_for_array`). Hit 6 entities across local + remote runs (~0.1% rate at pool=8).
Repair: HTTP DELETE the artifact node and entity node, re-register that uid from the
manifests. Worth filing upstream: the create should be atomic, and TCB's retry should
handle 409-after-500 as possible-success and verify.

## Caveats

- Local numbers from one warm node (sdfiana004); h5py_direct is page-cache-warm.
- Remote is a shared server — batched runs were capped at n=8 and the >0.5 GB/s probe
  pulled 17 GB; tail latencies (`req_p95`) on remote showed occasional 1.5–2.6 s stalls.
- Client-process cap is a property of one Python process (GIL); scale consumers by
  process, not threads, beyond c≈8.
