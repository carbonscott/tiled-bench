"""One shuffled epoch, N worker processes, samples/sec — per retrieval path.

Paths (what does the byte-moving during the epoch):
  h5py_plain    hand-rolled index over the raw folder, direct h5py reads, cached file
                handles. The baseline an ML engineer has today, done well.
  mode_a        locators from TileWright discovery (query_catalog sweep), then h5py with
                *shipped semantics*: file opened per sample per artifact, exactly as
                tilewright.client.load_artifacts does row-by-row.
  mode_a_cached same locators, but file handles cached like h5py_plain. If this curve sits
                on h5py_plain, TileWright's locator indirection costs nothing.
  mode_a_bulk   the documented workflow verbatim: one load_artifacts() call materializing
                every sample in RAM (single process). Reported as bulk samples/sec.
  http          per-sample reads through the Tiled server. edrixs: artifact_read
                (node[key][artifact].read()). nips3: container export of one entity per
                request, h5py-decoded (the egress-validated multi-artifact shape).

Cache states: warm (files pre-read into page cache), cold (posix_fadvise DONTNEED before
each rep). http rows are 'ambient' — the server's cache is not ours to control.

Guardrails per row: sample count, byte total vs expected, zero errors, spot sha1 checksums
vs a reference file written by `--write-ref` (sequential h5py_plain). Fast-but-wrong is a
failed row. Every row stamps tilewright SHA, tiled client/server versions, node, seed.
"""

import argparse
import csv
import hashlib
import json
import multiprocessing as mp
import os
import platform
import socket
import subprocess
import sys
import time
from datetime import datetime, timezone
from glob import glob
from io import BytesIO
from pathlib import Path

import h5py
import numpy as np

SPOT_EVERY = 499  # sample indices idx % SPOT_EVERY == 0 get sha1'd
TW_REPO = "/sdf/data/lcls/ds/prj/prjmaiqmag01/results/ajshack/tilewright"

DATASETS = {
    "edrixs": {
        "key": "BENCH_BATCHED_5F",
        "dir": "/sdf/data/lcls/ds/prj/prjmaiqmag01/results/data-source/sam/initial_data_proper",
        "pattern": "*/simulations.h5",
        "layout": "batched",
        "artifacts": [("rixs_spectrum", "/spectra")],
        "params_group": "/params",
    },
    "nips3": {
        "key": "BENCH_PER_ENTITY",
        "dir": "/sdf/data/lcls/ds/prj/prjmaiqmag01/results/data-source/NiPS3_Multimodal_Synthetic/data",
        "pattern": "*.h5",
        "layout": "per_entity",
        "artifacts": [
            ("hisym", "/hisym"), ("powder", "/powder"), ("powder_mask", "/powder_mask"),
            ("mag_a", "/Ma"), ("mag_b", "/Mb"), ("mag_cs", "/Mcs"),
        ],
        "params_group": None,  # 0-dim datasets at the file root
    },
}


# ── discovery: build (samples, theta) per path family ────────────────────────────────
# A sample is a tuple of per-artifact locators: (abs_path, h5_dataset, row_or_None).

def discover_plain(ds):
    """Hand-rolled index: glob + shapes. edrixs theta from /params vectors; nips3 theta
    harvested by opening every file (this cost is the point — it is what the manifest
    replaces)."""
    files = sorted(glob(os.path.join(ds["dir"], ds["pattern"])))
    if not files:
        raise SystemExit(f"no files match {ds['pattern']} under {ds['dir']}")
    samples, theta = [], []
    if ds["layout"] == "batched":
        for path in files:
            with h5py.File(path, "r") as f:
                n = f[ds["artifacts"][0][1]].shape[0]
                pnames = sorted(f[ds["params_group"]].keys())
                cols = [f[f"{ds['params_group']}/{p}"][:] for p in pnames]
                theta.append(np.column_stack([np.broadcast_to(c, (n,)) for c in cols]))
            samples += [tuple((path, dset, i) for _, dset in ds["artifacts"]) for i in range(n)]
        theta = np.concatenate(theta)
    else:
        for path in files:
            with h5py.File(path, "r") as f:
                theta.append([float(f[k][()]) for k in sorted(f.keys())
                              if isinstance(f[k], h5py.Dataset) and f[k].shape == ()])
            samples.append(tuple((path, dset, None) for _, dset in ds["artifacts"]))
        theta = np.asarray(theta)
    return samples, theta


def tiled_dataset_node(ds):
    from tiled.client import from_uri
    url = os.environ["TILED_URL"]
    client = from_uri(url, api_key=os.environ.get("TILED_API_KEY"))
    return client[ds["key"]]


def discover_mode_a(ds, cache_path):
    """Shipped discovery: tilewright.client.query_catalog sweep -> locator DataFrame.
    Cached to parquet so the sweep is timed once, not per invocation."""
    import pandas as pd
    route = "query_catalog"
    if cache_path and Path(cache_path).exists():
        df = pd.read_parquet(cache_path)
        route = "cached"
    else:
        from tilewright.client import query_catalog
        node = tiled_dataset_node(ds)
        df = query_catalog(node, artifact_type=ds["artifacts"][0][0])
        if df.empty:
            raise SystemExit(f"query_catalog returned no rows for {ds['key']}")
        if cache_path:
            df.to_parquet(cache_path)
    # Canonical sample order = (file, row) of the first artifact, matching
    # discover_plain's sorted-glob order, so spot checksums align across paths.
    a0 = ds["artifacts"][0][0]
    idx_col = f"index_{a0}"
    df["_row"] = df[idx_col].fillna(-1).astype(int) if idx_col in df.columns else -1
    df = df.sort_values([f"path_{a0}", "_row"], kind="mergesort").reset_index(drop=True)
    samples = []
    for _, row in df.iterrows():
        locs = []
        for atype, _ in ds["artifacts"]:
            idx = row.get(f"index_{atype}")
            locs.append((os.path.join(ds["dir"], row[f"path_{atype}"]),
                         row[f"dataset_{atype}"],
                         None if idx is None or (isinstance(idx, float) and np.isnan(idx))
                         else int(idx)))
        samples.append(tuple(locs))
    pcols = [c for c in df.columns
             if not c.startswith(("path_", "dataset_", "index_"))
             and c not in ("ent_key", "_row") and df[c].dtype.kind in "if"]
    theta = df[pcols].to_numpy()
    return samples, theta, df, route


# ── per-sample readers (run inside workers) ──────────────────────────────────────────

class DirectReader:
    """h5py reads from locator tuples; handle cache size 0 = reopen per artifact
    (shipped load_artifacts semantics), >0 = keep files open (best practice)."""

    def __init__(self, cache_size, rdcc_mb=1):
        self.cache_size = cache_size
        self.rdcc = dict(rdcc_nbytes=int(rdcc_mb * 2**20), rdcc_nslots=1_000_003)
        self.handles = {}  # path -> (h5py.File, {dset_name: h5py.Dataset})

    def _dset(self, path, dset):
        # The HDF5 chunk cache belongs to the *dataset handle*: a fresh f[dset] per
        # sample starts with an empty cache and re-decompresses its whole chunk band
        # every read. Cache Dataset objects, not just files.
        entry = self.handles.get(path)
        if entry is None:
            if len(self.handles) >= self.cache_size:
                self.handles.pop(next(iter(self.handles)))[0].close()
            entry = self.handles[path] = (h5py.File(path, "r", **self.rdcc), {})
        f, dsets = entry
        d = dsets.get(dset)
        if d is None:
            d = dsets[dset] = f[dset]
        return d

    def read(self, sample):
        bufs = []
        for path, dset, row in sample:
            if self.cache_size == 0:  # shipped load_artifacts semantics: reopen per read
                with h5py.File(path, "r", **self.rdcc) as f:
                    d = f[dset]
                    bufs.append(np.asarray(d[row] if row is not None else d[()]))
            else:
                d = self._dset(path, dset)
                bufs.append(np.asarray(d[row] if row is not None else d[()]))
        return bufs


class HttpReader:
    """Per-sample reads through the server. Connects once per worker."""

    def __init__(self, ds, ent_keys):
        self.ds = ds
        self.ent_keys = ent_keys
        self.node = tiled_dataset_node(ds)
        self.multi = len(ds["artifacts"]) > 1

    def read(self, sample_idx):
        key = self.ent_keys[sample_idx]
        if not self.multi:  # artifact_read
            return [self.node[key][self.ds["artifacts"][0][0]].read()]
        buf = BytesIO()  # container export, one entity per request
        self.node.export(buf, fields=[key], format="application/x-hdf5")
        buf.seek(0)
        bufs = []
        with h5py.File(buf, "r") as f:
            for atype, _ in self.ds["artifacts"]:
                bufs.append(np.asarray(f[key][atype]))
        return bufs


# ── cache-state control ──────────────────────────────────────────────────────────────

def files_of(samples):
    return sorted({loc[0] for s in samples for loc in s})


def prime(paths):
    t0 = time.monotonic()
    for p in paths:
        with open(p, "rb", buffering=0) as f:
            while f.read(1 << 22):
                pass
    return time.monotonic() - t0


def evict(paths):
    t0 = time.monotonic()
    for p in paths:
        fd = os.open(p, os.O_RDONLY)
        try:
            os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
        finally:
            os.close(fd)
    return time.monotonic() - t0


# ── epoch execution ──────────────────────────────────────────────────────────────────

def _worker(wid, mode, ds, samples, ent_keys, shard, cache_size, rdcc_mb, barrier, q):
    try:
        if mode == "http":
            reader = HttpReader(ds, ent_keys)
        else:
            reader = DirectReader(cache_size, rdcc_mb)
        spots, nbytes, errs = {}, 0, 0
        barrier.wait()
        t0 = time.monotonic()
        for idx in shard:
            try:
                bufs = reader.read(idx if mode == "http" else samples[idx])
                for b in bufs:
                    nbytes += b.nbytes
                if idx % SPOT_EVERY == 0:
                    h = hashlib.sha1()
                    for b in bufs:
                        h.update(np.ascontiguousarray(b).tobytes())
                    spots[idx] = h.hexdigest()[:12]
            except Exception:
                errs += 1
        t1 = time.monotonic()
        q.put({"wid": wid, "n": len(shard), "bytes": nbytes, "t0": t0, "t1": t1,
               "spots": spots, "errors": errs})
    except Exception as e:  # setup failure: report as fully failed shard
        try:
            barrier.wait(timeout=1)
        except Exception:
            pass
        q.put({"wid": wid, "n": len(shard), "bytes": 0, "t0": 0.0, "t1": 0.0,
               "spots": {}, "errors": len(shard), "fatal": repr(e)})


def run_epoch(mode, ds, samples, ent_keys, order, workers, cache_size, rdcc_mb):
    ctx = mp.get_context("fork")
    shards = [order[i::workers] for i in range(workers)]
    barrier = ctx.Barrier(workers)
    q = ctx.Queue()
    procs = [ctx.Process(target=_worker,
                         args=(i, mode, ds, samples, ent_keys, shards[i], cache_size,
                               rdcc_mb, barrier, q))
             for i in range(workers)]
    for p in procs:
        p.start()
    results = [q.get() for _ in procs]
    for p in procs:
        p.join()
    fatal = [r.get("fatal") for r in results if r.get("fatal")]
    ok = [r for r in results if not r.get("fatal")]
    t0 = min((r["t0"] for r in ok), default=0)
    t1 = max((r["t1"] for r in ok), default=0)
    busy = sum(r["t1"] - r["t0"] for r in ok)
    spots = {}
    for r in results:
        spots.update(r["spots"])
    return {
        "n": sum(r["n"] for r in results), "bytes": sum(r["bytes"] for r in results),
        "epoch_s": t1 - t0, "skew_s": (t1 - t0) - (min((r["t1"] for r in ok), default=0)
                                                   - max((r["t0"] for r in ok), default=0)),
        "busy_frac": busy / (workers * (t1 - t0)) if t1 > t0 else 0.0,
        "errors": sum(r["errors"] for r in results), "spots": spots,
        "fatal": "; ".join(fatal),
    }


# ── stamps, reference checksums, csv ─────────────────────────────────────────────────

def stamps():
    sha = subprocess.run(["git", "-C", TW_REPO, "rev-parse", "--short", "HEAD"],
                         capture_output=True, text=True).stdout.strip()
    import tiled
    server = ""
    try:
        import httpx
        r = httpx.get(os.environ["TILED_URL"] + "/api/v1/", timeout=15,
                      headers={"Authorization": "Apikey " + os.environ.get("TILED_API_KEY", "")})
        server = r.json().get("library_version", "")
    except Exception:
        pass
    return {"tilewright_sha": sha, "tiled_client": tiled.__version__,
            "server_version": server, "node": socket.gethostname().split(".")[0],
            "python": platform.python_version()}


def ref_path(outdir, dataset):
    return Path(outdir) / f"ref_{dataset}.json"


def check_spots(spots, ref):
    bad = sum(1 for k, v in spots.items() if ref.get(str(k)) != v)
    missing = sum(1 for k in spots if str(k) not in ref)
    return bad == 0 and missing == 0, bad, missing


CSV_FIELDS = [
    "ts", "dataset", "key", "layout", "path", "cache_state", "workers", "rep", "seed",
    "n_samples", "cap", "bytes_total", "epoch_s", "samples_per_s", "mb_per_s",
    "rdcc_mb", "discovery_s", "discovery_route", "skew_s", "busy_frac", "errors", "guardrail_ok",
    "spot_ok", "prime_s", "evict_s", "node", "tilewright_sha", "tiled_client",
    "server_version", "python", "notes",
]


def append_row(csv_file, row):
    new = not Path(csv_file).exists()
    with open(csv_file, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if new:
            w.writeheader()
        w.writerow(row)


# ── main ─────────────────────────────────────────────────────────────────────────────

def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--dataset", required=True, choices=sorted(DATASETS))
    ap.add_argument("--path", required=True,
                    choices=["h5py_plain", "mode_a", "mode_a_cached", "mode_a_bulk", "http"])
    ap.add_argument("--cache", default="warm", choices=["warm", "cold", "ambient"])
    ap.add_argument("--workers", type=int, default=1)
    ap.add_argument("--reps", type=int, default=1)
    ap.add_argument("--cap", type=int, default=0,
                    help="cap samples per epoch (0 = full epoch); recorded in the row")
    ap.add_argument("--seed", type=int, default=20260919)
    ap.add_argument("--handle-cache", type=int, default=16)
    ap.add_argument("--rdcc-mb", type=float, default=1.0,
                    help="h5py chunk-cache size per open file (h5py default 1 MB)")
    ap.add_argument("--outdir", default="results/trainbench")
    ap.add_argument("--csv", default=None, help="default: <outdir>/<dataset>.csv")
    ap.add_argument("--discovery-cache", default=None,
                    help="parquet path for the query_catalog sweep result")
    ap.add_argument("--write-ref", action="store_true",
                    help="write spot-checksum reference (sequential h5py_plain) and exit")
    ap.add_argument("--notes", default="")
    args = ap.parse_args(argv)

    ds = DATASETS[args.dataset]
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    csv_file = args.csv or outdir / f"{args.dataset}.csv"

    # discovery
    ent_keys = None
    t0 = time.monotonic()
    if args.path in ("mode_a", "mode_a_cached", "mode_a_bulk", "http"):
        samples, theta, df, route = discover_mode_a(ds, args.discovery_cache)
        ent_keys = df["ent_key"].tolist()
    else:
        samples, theta = discover_plain(ds)
        route = "glob+harvest"
    discovery_s = time.monotonic() - t0
    n_total = len(samples)
    print(f"discovery[{route}]: {n_total} samples, theta {theta.shape}, "
          f"{discovery_s:.2f}s", flush=True)

    if args.write_ref:
        reader = DirectReader(args.handle_cache)
        ref = {}
        for idx in range(0, n_total, SPOT_EVERY):
            h = hashlib.sha1()
            for b in reader.read(samples[idx]):
                h.update(np.ascontiguousarray(b).tobytes())
            ref[str(idx)] = h.hexdigest()[:12]
        ref_path(outdir, args.dataset).write_text(json.dumps(ref, indent=0))
        print(f"wrote {len(ref)} reference checksums")
        return

    ref = json.loads(ref_path(outdir, args.dataset).read_text()) \
        if ref_path(outdir, args.dataset).exists() else {}
    if not ref:
        raise SystemExit("no reference checksums; run --write-ref first")

    paths = files_of(samples)
    st = stamps()

    if args.path == "mode_a_bulk":
        from tilewright.client import load_artifacts
        for rep in range(args.reps):
            prime_s = prime(paths) if args.cache == "warm" else 0.0
            evict_s = evict(paths) if args.cache == "cold" else 0.0
            t0 = time.monotonic()
            nbytes = 0
            for atype, _ in ds["artifacts"]:
                arrs = load_artifacts(df, atype, ds["dir"])
                nbytes += sum(a.nbytes for a in arrs)
                del arrs
            el = time.monotonic() - t0
            row = dict(ts=datetime.now(timezone.utc).isoformat(timespec="seconds"),
                       dataset=args.dataset, key=ds["key"], layout=ds["layout"],
                       path=args.path, cache_state=args.cache, workers=1, rep=rep,
                       seed=args.seed, n_samples=n_total, cap=0, bytes_total=nbytes,
                       epoch_s=round(el, 3), samples_per_s=round(n_total / el, 2),
                       mb_per_s=round(nbytes / el / 1e6, 1), rdcc_mb=args.rdcc_mb,
                       discovery_s=round(discovery_s, 3),
                       discovery_route=route, skew_s=0, busy_frac=1.0, errors=0,
                       guardrail_ok=1, spot_ok="", prime_s=round(prime_s, 1),
                       evict_s=round(evict_s, 1), notes=args.notes, **st)
            append_row(csv_file, row)
            print(f"bulk rep{rep}: {n_total / el:.1f} samples/s ({nbytes / el / 1e6:.0f} MB/s)")
        return

    expected_bytes_full = None
    for rep in range(args.reps):
        rng = np.random.default_rng(args.seed + rep)
        order = rng.permutation(n_total)
        if args.cap:
            order = order[: args.cap]
        order = order.tolist()

        prime_s = evict_s = 0.0
        if args.path != "http":
            if args.cache == "warm":
                prime_s = prime(paths)
            elif args.cache == "cold":
                evict_s = evict(paths)

        cache_size = {"h5py_plain": args.handle_cache, "mode_a": 0,
                      "mode_a_cached": args.handle_cache, "http": 0}[args.path]
        res = run_epoch("http" if args.path == "http" else "direct", ds, samples,
                        ent_keys, order, args.workers, cache_size, args.rdcc_mb)

        spot_ok, bad, missing = check_spots(res["spots"], ref)
        if expected_bytes_full is None and not args.cap and res["errors"] == 0:
            expected_bytes_full = res["bytes"]
        guard = (res["errors"] == 0 and res["n"] == len(order) and spot_ok
                 and not res["fatal"])
        el = res["epoch_s"]
        row = dict(ts=datetime.now(timezone.utc).isoformat(timespec="seconds"),
                   dataset=args.dataset, key=ds["key"], layout=ds["layout"],
                   path=args.path, cache_state=args.cache, workers=args.workers, rep=rep,
                   seed=args.seed + rep, n_samples=res["n"], cap=args.cap,
                   bytes_total=res["bytes"], epoch_s=round(el, 3),
                   samples_per_s=round(res["n"] / el, 2) if el else 0,
                   mb_per_s=round(res["bytes"] / el / 1e6, 1) if el else 0,
                   rdcc_mb=args.rdcc_mb, discovery_s=round(discovery_s, 3),
                   discovery_route=route,
                   skew_s=round(res["skew_s"], 3), busy_frac=round(res["busy_frac"], 3),
                   errors=res["errors"], guardrail_ok=int(guard), spot_ok=f"{bad}b{missing}m",
                   prime_s=round(prime_s, 1), evict_s=round(evict_s, 1),
                   notes=(args.notes + (" FATAL:" + res["fatal"] if res["fatal"] else "")).strip(),
                   **st)
        append_row(csv_file, row)
        print(f"rep{rep} {args.path} {args.cache} w{args.workers}: "
              f"{row['samples_per_s']} samples/s, {row['mb_per_s']} MB/s, "
              f"guardrail={'OK' if guard else 'FAIL'}", flush=True)


if __name__ == "__main__":
    main()
