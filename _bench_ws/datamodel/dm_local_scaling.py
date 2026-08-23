"""Data-model probe, local: search/sweep latency vs catalog size (RTT-free).

Reuses the catalogs the regbench 'after' sweep left behind (per_entity n=10/100/1000,
each in its own workspace with catalog.db + synthetic HDF5), optionally adds a fresh
n=10000, and measures per catalog size:
  len(dataset) · full items() sweep (query_catalog pattern) · server-side search at
  ~50%/10%/1% selectivity (synth params: p0 = float(i), so thresholds are exact).

Usage: dm_local_scaling.py OUT.CSV WS_ROOT [--with-10k]
WS_ROOT = the regbench after-sweep workspace root (e.g. _bench_ws/after).
Run under PYTHONPATH of the after TCB worktree; cwd = tiled-bench.
"""
import csv
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from tiled.client import from_uri  # noqa: F401  (re-exported via make_client)
from tiled.queries import Key

from regbench.config import RunConfig
from regbench.manifests import build_manifest
from regbench.runner import make_client, run_registration
from regbench.server import local_server

OUT = sys.argv[1]
WS_ROOT = Path(sys.argv[2])
WITH_10K = "--with-10k" in sys.argv

rows = []
def rec(n_cat, measure, wall_s, n=None, note=""):
    rows.append({
        "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "catalog_entities": n_cat, "measure": measure,
        "wall_s": round(wall_s, 4), "n": n, "note": note,
    })
    print(f"[dm-local] n_cat={n_cat:<6} {measure:24s} {wall_s:8.3f}s  n={n} {note}")


def measure_catalog(client, ds_key, n_cat):
    ds = client[ds_key]
    for rep in range(3):
        t0 = time.perf_counter(); n = len(ds); rec(n_cat, "len(dataset)", time.perf_counter() - t0, n, f"rep{rep}")
    t0 = time.perf_counter()
    swept = sum(1 for _ in ds.items())
    rec(n_cat, "sweep_full_items", time.perf_counter() - t0, swept)
    # synth params: p0 = float(i) for i in 0..n-1 → exact selectivity thresholds
    for frac in (0.5, 0.1, 0.01):
        thr = n_cat * (1 - frac)
        t0 = time.perf_counter()
        hits = ds.search(Key("p0") >= thr)
        got = sum(1 for _ in hits.items())
        rec(n_cat, f"search_{int(frac*100)}pct", time.perf_counter() - t0, got, f"p0>={thr:g}")


# --- existing after-sweep catalogs ---
for n_cat in (10, 100, 1000):
    label = f"per_entity_n{n_cat}_local_w8"
    ws = WS_ROOT / label
    if not (ws / "catalog.db").exists():
        print(f"[dm-local] skip n={n_cat}: no catalog at {ws}")
        continue
    with local_server(workspace=ws, fresh_db=False) as server:
        client = make_client(server.uri, server.api_key)
        ds_keys = [k for k in client.keys() if k.startswith("regbench_")]
        # several reps registered several datasets; the last one is complete like the rest
        measure_catalog(client, sorted(ds_keys)[-1], n_cat)

# --- optional fresh 10k catalog (registered with the code under PYTHONPATH) ---
if WITH_10K:
    ws = WS_ROOT.parent / "dm10k"
    cfg = RunConfig(layout="per_entity", n_entities=10000, max_workers=8)
    manifest = build_manifest(cfg, root=ws / "datasets")
    with local_server(workspace=ws) as server:
        client = make_client(server.uri, server.api_key)
        res = run_registration(cfg, manifest, client, rep=0)
        print(f"[dm-local] 10k registration: {res.wall_s:.1f}s, {res.entities} entities "
              f"({res.entities_per_s:.1f} ent/s)")
        assert res.entities == 10000, res.entities
        measure_catalog(client, res.dataset_key, 10000)

new = not os.path.exists(OUT)
with open(OUT, "a", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
    if new:
        w.writeheader()
    w.writerows(rows)
print(f"[dm-local] {len(rows)} rows -> {OUT}")
