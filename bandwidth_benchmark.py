# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "marimo",
#     "tiled[server]",
#     "h5py",
#     "numpy",
#     "matplotlib",
# ]
# ///

import marimo

__generated_with = "0.23.8"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import os
    import threading
    import time
    import statistics
    from io import BytesIO
    from concurrent.futures import ThreadPoolExecutor

    import h5py
    import numpy as np
    import matplotlib.pyplot as plt

    from tiled.client import from_uri

    # Reuse the ingress benchmark's Server-Timing capture (stdlib-only) so egress
    # gets the same wall = app;dur + (network + client) decomposition. Requires the
    # notebook to run from the repo root so `regbench` is importable.
    from regbench.instruments import CallCollector, percentile

    return BytesIO, CallCollector, ThreadPoolExecutor, from_uri, h5py, mo, np, os, percentile, plt, threading, time


@app.cell
def _(from_uri, mo, os):
    TILED_URL = os.environ.get(
        "TILED_URL", "https://lcls-data-portal.slac.stanford.edu/tiled-test"
    )
    API_KEY  = os.environ.get("TILED_API_KEY", "")
    DATASET  = "SYNTHETIC_BENCHMARK"
    ARTIFACT = "rixs_spectrum"
    TEST_MODE = "test" in mo.cli_args()

    print(f"Connecting to {TILED_URL} ...")
    client   = from_uri(TILED_URL, api_key=API_KEY)
    ds       = client[DATASET]
    print(f"Fetching entity list for '{DATASET}' ...")
    all_keys = list(ds.keys())
    if TEST_MODE:
        all_keys = all_keys[:30]
        print(f"Test mode — capped to {len(all_keys)} entities")
    else:
        print(f"Connected — {len(all_keys)} entities found in '{DATASET}'")

    mo.md(
        f"Connected to `{TILED_URL}` — "
        f"**{len(all_keys):,}** entities in `{DATASET}`"
        + (" _(test mode)_" if TEST_MODE else "")
    )
    return API_KEY, ARTIFACT, DATASET, TEST_MODE, TILED_URL, all_keys, client, ds


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## §1 — Batch-size sweep

    We sweep `batch_size` over a grid and issue `N_REPS` independent bulk-export
    calls per size: `ds.export(..., format="application/x-hdf5")`.

    **Decomposition (new).** A single wall-clock number can't tell you *why* egress is
    slow, so — mirroring the ingress benchmark — every export is split into layers:

    | Layer | How it's measured |
    |-------|-------------------|
    | `app` — server read + HDF5 re-encode | `Server-Timing: app;dur` header (via `CallCollector`) |
    | `net` — TLS + network + receive | transfer time − `app;dur` |
    | `decode` — client HDF5 parse + `np.stack` | timed separately from transfer |

    Two byte figures are reported so they can't be confused: **wire** MB/s (bytes on the
    HTTP socket, `_buf.tell()`) over transfer time, and **decoded** MB/s (`nbytes` of the
    stacked array) over wall time. If the server gzips the container these diverge.

    **Key question:** as batch size grows, which layer dominates? If `app` dominates, the
    ceiling is server-side encode and parallelism won't reach GB/s; if `net` dominates on a
    single stream, it's bandwidth-delay-product-limited and only concurrency helps; if
    `decode` dominates, the client CPU is the wall.
    """)
    return


@app.cell
def _(ARTIFACT, BytesIO, CallCollector, TEST_MODE, all_keys, client, ds, h5py, mo, np, time):
    # ── Configuration ──────────────────────────────────────────────────────────
    BATCH_SIZES = [1, 5, 10] if TEST_MODE else [1, 5, 10, 20, 40, 80, 100, 200, 220, 230, 240]
    N_REPS      = 2 if TEST_MODE else 5      # repetitions per batch size (for IQR)
    MAX_WAIT_S  = 120.0  # hard abort: skip batch sizes slower than this

    # Server-Timing capture attaches to the Tiled client's shared httpx.Client and reads
    # `app;dur` off every export response — the layer-splitter from the ingress benchmark.
    _coll = CallCollector()

    def _median(xs):
        return float(np.median(xs)) if xs else float("nan")

    # ── Warmup: prime TCP connection and server-side caches ────────────────────
    # No guard: a warmup failure means the connection/key is broken, so fail loud here
    # rather than limp into a sweep that would error on every batch.
    print("Warmup call ...")
    _warmup_buf = BytesIO()
    ds.export(_warmup_buf, fields=all_keys[:1], format="application/x-hdf5")
    print("Warmup done.")

    # ── Sweep ──────────────────────────────────────────────────────────────────
    print(f"\nStarting batch-size sweep: {BATCH_SIZES} ({N_REPS} reps each) ...")
    batch_results = []

    for _bs in BATCH_SIZES:
        if _bs > len(all_keys):
            print(f"  skip batch_size={_bs}: only {len(all_keys)} entities available")
            continue

        _xfer, _decode, _wall, _app = [], [], [], []
        _wire_bytes = _dec_bytes = None
        _had_error = False

        for _rep in range(N_REPS):
            # Different window each rep — avoids server-side caching artefacts
            _off  = (_rep * _bs) % max(1, len(all_keys) - _bs)
            _keys = all_keys[_off : _off + _bs]

            with _coll.attached(client):
                # transfer = server work + network + receive (buffer filled)
                _t0 = time.perf_counter()
                _buf = BytesIO()
                ds.export(_buf, fields=_keys, format="application/x-hdf5")
                _t_xfer = time.perf_counter() - _t0
                _nwire = _buf.tell()  # actual bytes transferred over HTTP

                # decode = client HDF5 parse + np.stack, timed on its own
                _buf.seek(0)
                _t1 = time.perf_counter()
                with h5py.File(_buf, "r") as _f:
                    _arrs = [
                        np.asarray(_f[k][ARTIFACT])
                        for k in _keys
                        if k in _f and ARTIFACT in _f[k]
                    ]
                    if not _arrs:
                        print(f"  batch={_bs} rep={_rep}: no arrays found — check ARTIFACT key '{ARTIFACT}'")
                        _had_error = True
                        break
                    _X = np.stack(_arrs)
                _t_dec = time.perf_counter() - _t1

            _wall_rep = _t_xfer + _t_dec
            if _wall_rep > MAX_WAIT_S:
                print(f"  batch={_bs} rep={_rep}: {_wall_rep:.1f}s > limit — aborting this size")
                _had_error = True
                break

            # Sum app;dur across the GET(s) this export issued (ms). Missing header → 0.
            _app_ms = sum(s.app_ms for s in _coll.filter(method="GET") if s.app_ms is not None)
            _xfer.append(_t_xfer)
            _decode.append(_t_dec)
            _wall.append(_wall_rep)
            _app.append(_app_ms / 1000.0)  # → seconds, same clock domain as _xfer
            if _wire_bytes is None:
                _wire_bytes = _nwire
                _dec_bytes = int(_X.nbytes)

        if not _wall:
            continue

        _p50      = float(np.percentile(_wall, 50))
        _xfer_p50 = _median(_xfer)
        _dec_p50  = _median(_decode)
        _app_p50  = _median(_app)
        # network+recv on the wire = transfer − server-side app work
        _net_p50  = max(0.0, _xfer_p50 - _app_p50)
        _mb_wire  = (_wire_bytes or 0) / 1e6
        _mb_dec   = (_dec_bytes or 0) / 1e6

        batch_results.append(
            {
                "batch_size":       _bs,
                "n_reps":           len(_wall),
                "p50_s":            _p50,
                "p95_s":            float(np.percentile(_wall, 95)),
                "p99_s":            float(np.percentile(_wall, 99)),
                "app_s":            _app_p50,
                "net_s":            _net_p50,
                "decode_s":         _dec_p50,
                "entities_per_s":   _bs / _p50,
                "mb_wire_per_s":    _mb_wire / _xfer_p50 if _xfer_p50 else float("nan"),
                "mb_decoded_per_s": _mb_dec / _p50,
                "mb_wire":          _mb_wire,
                "mb_decoded":       _mb_dec,
                "had_error":        _had_error,
            }
        )

        print(
            f"  batch={_bs:4d}  wall={_p50:.2f}s  "
            f"app={_app_p50:.2f}s  net={_net_p50:.2f}s  decode={_dec_p50:.2f}s  "
            f"wireMB/s={(_mb_wire/_xfer_p50 if _xfer_p50 else float('nan')):.1f}  "
            f"decMB/s={_mb_dec/_p50:.1f}"
        )

    print(f"\nSweep complete — {len(batch_results)} batch sizes measured.")
    mo.md(f"Sweep complete — **{len(batch_results)}** batch sizes measured.")
    return (batch_results,)


@app.cell
def _(batch_results, mo):
    if not batch_results:
        mo.md("_No results yet._")
    else:
        _rows = "\n".join(
            f"| {r['batch_size']} "
            f"| {r['p50_s']:.2f} "
            f"| {r['app_s']:.2f} "
            f"| {r['net_s']:.2f} "
            f"| {r['decode_s']:.2f} "
            f"| {r['entities_per_s']:.1f} "
            f"| {r['mb_wire_per_s']:.1f} "
            f"| {r['mb_decoded_per_s']:.1f} |"
            for r in batch_results
        )
        mo.md(
            f"""### Batch-size sweep results

    `wall = app + net + decode`.  wire MB/s is over transfer time; decoded MB/s over wall.

    | Batch | wall p50 (s) | app (s) | net (s) | decode (s) | ent/s | wire MB/s | dec MB/s |
    |------:|-------------:|--------:|--------:|-----------:|------:|----------:|---------:|
    {_rows}
    """
        )
    return


@app.cell
def _(batch_results, mo, plt):
    if not batch_results:
        _out = mo.md("_Run the sweep cell to generate plots._")
    else:
        _bs_ax   = [r["batch_size"]       for r in batch_results]
        _ep      = [r["entities_per_s"]   for r in batch_results]
        _mb_wire = [r["mb_wire_per_s"]    for r in batch_results]
        _mb_dec  = [r["mb_decoded_per_s"] for r in batch_results]
        _app     = [r["app_s"]            for r in batch_results]
        _net     = [r["net_s"]            for r in batch_results]
        _decode  = [r["decode_s"]         for r in batch_results]
        _p50     = [r["p50_s"]            for r in batch_results]
        _p95     = [r["p95_s"]            for r in batch_results]
        _p99     = [r["p99_s"]            for r in batch_results]

        _fig, _axes = plt.subplots(2, 2, figsize=(14, 8))

        _axes[0, 0].plot(_bs_ax, _ep, "o-", color="steelblue")
        _axes[0, 0].set_xlabel("Batch size")
        _axes[0, 0].set_ylabel("entities / s")
        _axes[0, 0].set_title("Throughput — entities/s")
        _axes[0, 0].set_xscale("log")
        _axes[0, 0].grid(True, alpha=0.3)

        _axes[0, 1].plot(_bs_ax, _mb_wire, "o-", color="darkorange", label="wire (transfer)")
        _axes[0, 1].plot(_bs_ax, _mb_dec, "s--", color="firebrick", label="decoded (wall)")
        _axes[0, 1].set_xlabel("Batch size")
        _axes[0, 1].set_ylabel("MB / s")
        _axes[0, 1].set_title("Throughput — MB/s")
        _axes[0, 1].set_xscale("log")
        _axes[0, 1].legend()
        _axes[0, 1].grid(True, alpha=0.3)

        # The money plot: where does wall time actually go, layer by layer?
        _axes[1, 0].stackplot(
            _bs_ax, _app, _net, _decode,
            labels=["app (server)", "net", "decode (client)"],
            colors=["#4c72b0", "#dd8452", "#55a868"], alpha=0.85,
        )
        _axes[1, 0].set_xlabel("Batch size")
        _axes[1, 0].set_ylabel("Wall time p50 (s)")
        _axes[1, 0].set_title("Layer decomposition — app + net + decode")
        _axes[1, 0].set_xscale("log")
        _axes[1, 0].legend(loc="upper left")
        _axes[1, 0].grid(True, alpha=0.3)

        _axes[1, 1].plot(_bs_ax, _p50, "o-", label="p50")
        _axes[1, 1].plot(_bs_ax, _p95, "s--", label="p95")
        _axes[1, 1].plot(_bs_ax, _p99, "^:", label="p99")
        _axes[1, 1].set_xlabel("Batch size")
        _axes[1, 1].set_ylabel("Batch latency (s)")
        _axes[1, 1].set_title("Batch latency — absolute")
        _axes[1, 1].set_xscale("log")
        _axes[1, 1].legend()
        _axes[1, 1].grid(True, alpha=0.3)

        plt.tight_layout()
        plt.savefig("batch_sweep.png", dpi=150, bbox_inches="tight")
        print("Saved: batch_sweep.png")
        _out = _fig
    _out
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## §1b — Raw-path probe  (array-level octet-stream vs container HDF5)

    §1 measures the container→HDF5 export path, which forces the server to walk each
    child, `read()` it from storage, and **re-encode it into an HDF5 container** — and the
    client to **parse HDF5** back out. Reading at the *array* level
    (`ds[key][artifact].read()`) transfers a raw `application/octet-stream` numpy buffer:
    **no h5py on either end.**

    This cell reads the same `RAW_PROBE_N` entities **both ways** and diffs the layers:

    | Quantity | Meaning |
    |----------|---------|
    | `Δapp = app(hdf5) − app(raw)` | server-side HDF5 re-encode cost |
    | `decode(hdf5)` | client-side h5py parse cost the raw path skips |
    | `net(raw)` | inflated by *N* round-trips (raw = N requests vs HDF5's one); concurrency (§2) hides this |

    Array navigation (`ds[key][artifact]`) is resolved **before** timing, so only data
    transfer is measured. The raw path is run serially here for a clean per-layer diff — it
    pays N×RTT that a concurrent client would amortize.
    """)
    return


@app.cell
def _(ARTIFACT, BytesIO, CallCollector, TEST_MODE, all_keys, client, ds, h5py, mo, np, time):
    RAW_PROBE_N = 5 if TEST_MODE else 40
    RAW_REPS    = 2 if TEST_MODE else 5

    _coll = CallCollector()
    _probe_keys = all_keys[:RAW_PROBE_N]

    # Resolve array handles once, outside the timed region — excludes node navigation so
    # only the data transfer is measured. Assumes every probe entity has ARTIFACT (uniform
    # synthetic dataset); a missing key surfaces loudly rather than being silently skipped.
    _handles = [ds[_k][ARTIFACT] for _k in _probe_keys]

    def _app_s(coll):
        # Sum Server-Timing app;dur (ms→s) across every GET this path issued.
        return sum(s.app_ms for s in coll.filter(method="GET") if s.app_ms is not None) / 1000.0

    _h_app, _h_net, _h_dec, _h_mb = [], [], [], []
    _r_app, _r_net, _r_mb         = [], [], []

    print(f"Raw-path probe: {RAW_PROBE_N} entities x {RAW_REPS} reps, both paths ...")
    for _rep in range(RAW_REPS):
        # ── Path A: container → HDF5 (one request for all N entities) ──
        with _coll.attached(client):
            _t0 = time.perf_counter()
            _buf = BytesIO()
            ds.export(_buf, fields=_probe_keys, format="application/x-hdf5")
            _t_x = time.perf_counter() - _t0
            _buf.seek(0)
            _t1 = time.perf_counter()
            with h5py.File(_buf, "r") as _f:
                _X = np.stack([
                    np.asarray(_f[_k][ARTIFACT])
                    for _k in _probe_keys
                    if _k in _f and ARTIFACT in _f[_k]
                ])
            _t_d = time.perf_counter() - _t1
        _a = _app_s(_coll)
        _h_app.append(_a)
        _h_net.append(max(0.0, _t_x - _a))
        _h_dec.append(_t_d)
        _h_mb.append(_X.nbytes / 1e6)

        # ── Path B: N array reads (octet-stream, serial). read() returns a materialized
        #    numpy array, so client decode is a trivial buffer copy folded into transfer. ──
        with _coll.attached(client):
            _t0 = time.perf_counter()
            _arrs = [_h.read() for _h in _handles]
            _t_x = time.perf_counter() - _t0
        _a = _app_s(_coll)
        _r_app.append(_a)
        _r_net.append(max(0.0, _t_x - _a))
        _r_mb.append(sum(a.nbytes for a in _arrs) / 1e6)

    _N = RAW_PROBE_N
    _h_tot = float(np.median(_h_app) + np.median(_h_net) + np.median(_h_dec))
    _r_tot = float(np.median(_r_app) + np.median(_r_net))
    rawpath_results = {
        "n": _N,
        "hdf5": {
            "app_ms":    1000 * float(np.median(_h_app)) / _N,
            "net_ms":    1000 * float(np.median(_h_net)) / _N,
            "decode_ms": 1000 * float(np.median(_h_dec)) / _N,
            "mb_per_s":  float(np.median(_h_mb)) / _h_tot if _h_tot else float("nan"),
        },
        "raw": {
            "app_ms":    1000 * float(np.median(_r_app)) / _N,
            "net_ms":    1000 * float(np.median(_r_net)) / _N,
            "decode_ms": 0.0,
            "mb_per_s":  float(np.median(_r_mb)) / _r_tot if _r_tot else float("nan"),
        },
    }
    rawpath_results["reencode_ms_per_entity"] = (
        rawpath_results["hdf5"]["app_ms"] - rawpath_results["raw"]["app_ms"]
    )

    _hd, _rw = rawpath_results["hdf5"], rawpath_results["raw"]
    print(f"  HDF5  app={_hd['app_ms']:.1f}ms/ent  net={_hd['net_ms']:.1f}  "
          f"decode={_hd['decode_ms']:.1f}  → {_hd['mb_per_s']:.1f} MB/s")
    print(f"  RAW   app={_rw['app_ms']:.1f}ms/ent  net={_rw['net_ms']:.1f}  "
          f"decode=0.0  → {_rw['mb_per_s']:.1f} MB/s")
    print(f"  Δapp (HDF5 re-encode) ≈ {rawpath_results['reencode_ms_per_entity']:.1f} ms/entity")

    mo.md(
        f"Raw-path probe complete — HDF5 re-encode ≈ "
        f"**{rawpath_results['reencode_ms_per_entity']:.1f} ms/entity**, "
        f"client h5py parse ≈ **{_hd['decode_ms']:.1f} ms/entity**."
    )
    return (rawpath_results,)


@app.cell
def _(plt, rawpath_results):
    _r = rawpath_results
    _paths  = ["HDF5 export", "Raw array"]
    _app    = [_r["hdf5"]["app_ms"],    _r["raw"]["app_ms"]]
    _net    = [_r["hdf5"]["net_ms"],    _r["raw"]["net_ms"]]
    _decode = [_r["hdf5"]["decode_ms"], _r["raw"]["decode_ms"]]

    _fig, _ax = plt.subplots(figsize=(7, 4))
    _ax.bar(_paths, _app, label="app (server)", color="#4c72b0")
    _ax.bar(_paths, _net, bottom=_app, label="net", color="#dd8452")
    _ax.bar(
        _paths, _decode,
        bottom=[a + n for a, n in zip(_app, _net)],
        label="decode (client)", color="#55a868",
    )
    _ax.set_ylabel("Time per entity (ms)")
    _ax.set_title(f"Per-entity layer cost — HDF5 export vs raw array read (n={_r['n']})")
    _ax.legend()
    _ax.grid(True, axis="y", alpha=0.3)
    plt.tight_layout()
    plt.savefig("rawpath_probe.png", dpi=150, bbox_inches="tight")
    print("Saved: rawpath_probe.png")
    _fig
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## §2 — Concurrency sweep

    Fixed `batch_size = 20`.  We vary the number of parallel in-flight export
    calls from 1 to 8 using a `ThreadPoolExecutor`.

    **Goal:** determine whether the server can saturate multiple connections
    simultaneously.  If throughput scales linearly with concurrency, the
    bottleneck is the single-threaded client; if it plateaus, the server is
    the ceiling.
    """)
    return


@app.cell
def _(API_KEY, ARTIFACT, DATASET, BytesIO, ThreadPoolExecutor, TILED_URL, all_keys, from_uri, h5py, mo, np, threading, time):
    CONCURRENCY_LEVELS = [1, 2, 4, 8]
    FIXED_BATCH        = 20
    _N_TOTAL           = min(200, len(all_keys))  # 10 batches of FIXED_BATCH

    _batches = [
        all_keys[i : i + FIXED_BATCH]
        for i in range(0, _N_TOTAL, FIXED_BATCH)
    ]

    # One Tiled client per thread — the underlying httpx.Client is not thread-safe
    _local = threading.local()

    def _get_ds():
        if not hasattr(_local, "ds"):
            _local.ds = from_uri(TILED_URL, api_key=API_KEY)[DATASET]
        return _local.ds

    def _fetch(keys):
        _buf = BytesIO()
        _get_ds().export(_buf, fields=keys, format="application/x-hdf5")
        _buf.seek(0)
        with h5py.File(_buf, "r") as _f:
            _arrs = [
                np.asarray(_f[k][ARTIFACT])
                for k in keys
                if k in _f and ARTIFACT in _f[k]
            ]
            if not _arrs:
                return np.empty((0,))
            return np.stack(_arrs)

    conc_results = []

    for _c in CONCURRENCY_LEVELS:
        _t0 = time.perf_counter()
        with ThreadPoolExecutor(max_workers=_c) as _pool:
            _futs = [_pool.submit(_fetch, _bk) for _bk in _batches]
            _out  = [_f.result() for _f in _futs]
        _elapsed = time.perf_counter() - _t0

        _total_e  = sum(len(bk) for bk in _batches)
        _total_mb = sum(a.nbytes for a in _out) / 1e6

        conc_results.append(
            {
                "concurrency":    _c,
                "elapsed_s":      round(_elapsed, 2),
                "entities_per_s": round(_total_e  / _elapsed, 1),
                "mb_per_s":       round(_total_mb / _elapsed, 1),
            }
        )
        print(
            f"  concurrency={_c}  elapsed={_elapsed:.2f}s  "
            f"ent/s={_total_e/_elapsed:.1f}  MB/s={_total_mb/_elapsed:.1f}"
        )

    mo.md("Concurrency sweep complete.")
    return FIXED_BATCH, conc_results


@app.cell
def _(FIXED_BATCH, conc_results, mo, plt):
    if not conc_results:
        _out = mo.md("_No concurrency results yet._")
    else:
        _cc   = [r["concurrency"]    for r in conc_results]
        _ep   = [r["entities_per_s"] for r in conc_results]
        _mbps = [r["mb_per_s"]       for r in conc_results]

        _fig, _axes = plt.subplots(1, 2, figsize=(10, 4))

        _axes[0].plot(_cc, _ep, "o-", color="steelblue")
        _axes[0].set_xlabel("Concurrent workers")
        _axes[0].set_ylabel("entities / s")
        _axes[0].set_title(f"Throughput vs concurrency  (batch={FIXED_BATCH})")
        _axes[0].grid(True, alpha=0.3)

        _axes[1].plot(_cc, _mbps, "o-", color="darkorange")
        _axes[1].set_xlabel("Concurrent workers")
        _axes[1].set_ylabel("MB / s")
        _axes[1].set_title(f"MB/s vs concurrency  (batch={FIXED_BATCH})")
        _axes[1].grid(True, alpha=0.3)

        plt.tight_layout()
        plt.savefig("concurrency_sweep.png", dpi=150, bbox_inches="tight")
        print("Saved: concurrency_sweep.png")
        _out = _fig
    _out
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## §3 — Sustained throughput  (many batches, fixed size)

    §1 measures throughput over N_REPS short bursts.  Here we run a longer
    sustained pass — all available entities in batches of `SUSTAINED_BATCH` — to
    confirm that throughput doesn't decay over time (thermal throttling, connection
    pool exhaustion, server-side GC pauses).

    Per-batch timing is printed so you can eyeball drift.
    """)
    return


@app.cell
def _(ARTIFACT, BytesIO, all_keys, ds, h5py, mo, np, time):
    SUSTAINED_BATCH = 40
    _MAX_ENTITIES   = min(500, len(all_keys))  # cap so it doesn't run forever

    _keys_sustained = all_keys[:_MAX_ENTITIES]
    _batches_s = [
        _keys_sustained[i : i + SUSTAINED_BATCH]
        for i in range(0, len(_keys_sustained), SUSTAINED_BATCH)
    ]

    _t0_total  = time.perf_counter()
    _bytes_total = 0
    sustained_per_batch = []

    for _i, _bk in enumerate(_batches_s):
        _t0 = time.perf_counter()
        _buf = BytesIO()
        ds.export(_buf, fields=_bk, format="application/x-hdf5")
        _buf.seek(0)
        with h5py.File(_buf, "r") as _f:
            _arrs = [
                np.asarray(_f[k][ARTIFACT])
                for k in _bk
                if k in _f and ARTIFACT in _f[k]
            ]
            if not _arrs:
                print(f"  batch {_i}: no arrays found — check ARTIFACT key '{ARTIFACT}'")
                continue
            _X = np.stack(_arrs)
        _elapsed_b = time.perf_counter() - _t0
        _bytes_total += _X.nbytes
        sustained_per_batch.append(_elapsed_b)
        print(
            f"  batch {_i:3d}/{len(_batches_s)}  "
            f"{_elapsed_b:.2f}s  "
            f"{len(_bk)/_elapsed_b:.1f} ent/s  "
            f"{_X.nbytes/1e6/_elapsed_b:.1f} MB/s"
        )

    _dt    = time.perf_counter() - _t0_total
    _mb    = _bytes_total / 1e6
    _total = len(_keys_sustained)

    mo.md(
        f"**Sustained run:** {_total} entities in {_dt:.1f}s  "
        f"→ **{_total/_dt:.1f} ent/s** · **{_mb/_dt:.1f} MB/s**  "
        f"({_mb:.0f} MB total)"
    )
    return (sustained_per_batch,)


@app.cell
def _(mo, plt, sustained_per_batch):
    if not sustained_per_batch:
        _out = mo.md("_No sustained results yet._")
    else:
        _fig, _ax = plt.subplots(figsize=(10, 3))
        _ax.plot(sustained_per_batch, "o-", color="steelblue", markersize=3)
        _ax.axhline(
            sum(sustained_per_batch) / len(sustained_per_batch),
            color="red",
            linestyle="--",
            label="mean",
        )
        _ax.set_xlabel("Batch index")
        _ax.set_ylabel("Batch latency (s)")
        _ax.set_title("Per-batch latency — sustained run")
        _ax.legend()
        _ax.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig("sustained_run.png", dpi=150, bbox_inches="tight")
        print("Saved: sustained_run.png")
        print("Benchmark complete.")
        _out = _fig
    _out
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
 
    """)
    return


if __name__ == "__main__":
    app.run()
