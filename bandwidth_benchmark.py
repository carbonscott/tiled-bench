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

    return BytesIO, ThreadPoolExecutor, from_uri, h5py, mo, np, os, plt, threading, time


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
    return API_KEY, ARTIFACT, DATASET, TEST_MODE, TILED_URL, all_keys, ds


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## §1 — Batch-size sweep

    We sweep `batch_size` over a grid and issue `N_REPS` independent bulk-export
    calls per size: `ds.export(..., format="application/x-hdf5")`.  Wall-clock time
    includes the HTTP transfer **and** HDF5 parse.

    **Key question:** where does throughput plateau as batch size grows?  The three
    signatures to watch for:

    | Pattern | Meaning |
    |---------|---------|
    | Throughput still rising | Not yet saturated — push harder |
    | Throughput flat, latency stable | Bandwidth ceiling reached |
    | p99 diverges from p50 | Queue buildup — back off |
    """)
    return


@app.cell
def _(ARTIFACT, BytesIO, TEST_MODE, all_keys, ds, h5py, mo, np, time):
    # ── Configuration ──────────────────────────────────────────────────────────
    BATCH_SIZES = [1, 5, 10] if TEST_MODE else [1, 5, 10, 20, 40, 80, 100, 200, 220, 230, 240]
    N_REPS      = 2 if TEST_MODE else 5      # repetitions per batch size (for IQR)
    MAX_WAIT_S  = 120.0  # hard abort: skip batch sizes slower than this

    # ── Warmup: prime TCP connection and server-side caches ────────────────────
    print("Warmup call ...")
    try:
        _warmup_buf = BytesIO()
        ds.export(_warmup_buf, fields=all_keys[:1], format="application/x-hdf5")
        print("Warmup done.")
    except Exception as _e:
        print(f"Warmup failed (non-fatal): {_e}")

    # ── Sweep ──────────────────────────────────────────────────────────────────
    print(f"\nStarting batch-size sweep: {BATCH_SIZES} ({N_REPS} reps each) ...")
    batch_results = []

    for _bs in BATCH_SIZES:
        if _bs > len(all_keys):
            print(f"  skip batch_size={_bs}: only {len(all_keys)} entities available")
            continue

        _lats      = []
        _nbytes    = None
        _had_error = False

        for _rep in range(N_REPS):
            # Different window each rep — avoids server-side caching artefacts
            _off  = (_rep * _bs) % max(1, len(all_keys) - _bs)
            _keys = all_keys[_off : _off + _bs]

            _t0 = time.perf_counter()
            try:
                _buf = BytesIO()
                ds.export(_buf, fields=_keys, format="application/x-hdf5")
                _wire_bytes = _buf.tell()  # actual bytes transferred over HTTP
                _buf.seek(0)
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
                _elapsed = time.perf_counter() - _t0

                if _nbytes is None:
                    _nbytes = _wire_bytes  # wire bytes, not decompressed array size

                if _elapsed > MAX_WAIT_S:
                    print(
                        f"  batch={_bs} rep={_rep}: "
                        f"{_elapsed:.1f}s > limit — aborting this size"
                    )
                    _had_error = True
                    break

                _lats.append(_elapsed)

            except Exception as _e:
                print(f"  batch={_bs} rep={_rep}: ERROR — {_e}")
                _had_error = True
                break

        if not _lats:
            continue

        _arr = np.array(_lats)
        _p50 = float(np.percentile(_arr, 50))
        _p95 = float(np.percentile(_arr, 95))
        _p99 = float(np.percentile(_arr, 99))
        _mb  = (_nbytes or 0) / 1e6

        batch_results.append(
            {
                "batch_size":     _bs,
                "n_reps":         len(_lats),
                "p50_s":          _p50,
                "p95_s":          _p95,
                "p99_s":          _p99,
                "entities_per_s": _bs / _p50,
                "mb_per_s":       _mb / _p50,
                "mb_per_batch":   _mb,
                "had_error":      _had_error,
            }
        )

        print(
            f"  batch={_bs:4d}  "
            f"p50={_p50:.2f}s  p95={_p95:.2f}s  "
            f"p50/ent={_p50/_bs:.3f}s  p95/ent={_p95/_bs:.3f}s  "
            f"ent/s={_bs/_p50:.1f}  MB/s={_mb/_p50:.1f}"
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
            f"| {r['p95_s']:.2f} "
            f"| {r['p99_s']:.2f} "
            f"| {r['entities_per_s']:.1f} "
            f"| {r['mb_per_s']:.1f} "
            f"| {r['mb_per_batch']:.1f} |"
            for r in batch_results
        )
        mo.md(
            f"""### Batch-size sweep results

    | Batch | p50 (s) | p95 (s) | p99 (s) | ent/s | MB/s | MB/batch |
    |------:|--------:|--------:|--------:|------:|-----:|---------:|
    {_rows}
    """
        )
    return


@app.cell
def _(batch_results, mo, plt):
    if not batch_results:
        _out = mo.md("_Run the sweep cell to generate plots._")
    else:
        _bs_ax   = [r["batch_size"]     for r in batch_results]
        _ep      = [r["entities_per_s"] for r in batch_results]
        _mbps    = [r["mb_per_s"]       for r in batch_results]
        _p50     = [r["p50_s"]          for r in batch_results]
        _p95     = [r["p95_s"]          for r in batch_results]
        _p99     = [r["p99_s"]          for r in batch_results]
        _p50_ent = [p / bs for p, bs in zip(_p50, _bs_ax)]
        _p95_ent = [p / bs for p, bs in zip(_p95, _bs_ax)]
        _p99_ent = [p / bs for p, bs in zip(_p99, _bs_ax)]

        _fig, _axes = plt.subplots(2, 2, figsize=(14, 8))

        _axes[0, 0].plot(_bs_ax, _ep, "o-", color="steelblue")
        _axes[0, 0].set_xlabel("Batch size")
        _axes[0, 0].set_ylabel("entities / s")
        _axes[0, 0].set_title("Throughput — entities/s")
        _axes[0, 0].set_xscale("log")
        _axes[0, 0].grid(True, alpha=0.3)

        _axes[0, 1].plot(_bs_ax, _mbps, "o-", color="darkorange")
        _axes[0, 1].set_xlabel("Batch size")
        _axes[0, 1].set_ylabel("MB / s")
        _axes[0, 1].set_title("Throughput — MB/s")
        _axes[0, 1].set_xscale("log")
        _axes[0, 1].grid(True, alpha=0.3)

        _axes[1, 0].plot(_bs_ax, _p50, "o-", label="p50")
        _axes[1, 0].plot(_bs_ax, _p95, "s--", label="p95")
        _axes[1, 0].plot(_bs_ax, _p99, "^:", label="p99")
        _axes[1, 0].set_xlabel("Batch size")
        _axes[1, 0].set_ylabel("Batch latency (s)")
        _axes[1, 0].set_title("Batch latency — absolute")
        _axes[1, 0].set_xscale("log")
        _axes[1, 0].legend()
        _axes[1, 0].grid(True, alpha=0.3)

        _axes[1, 1].plot(_bs_ax, _p50_ent, "o-", label="p50/ent")
        _axes[1, 1].plot(_bs_ax, _p95_ent, "s--", label="p95/ent")
        _axes[1, 1].plot(_bs_ax, _p99_ent, "^:", label="p99/ent")
        _axes[1, 1].set_xlabel("Batch size")
        _axes[1, 1].set_ylabel("Latency per entity (s)")
        _axes[1, 1].set_title("Batch latency — normalised per entity")
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
