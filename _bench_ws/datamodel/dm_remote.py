"""Data-model probe, remote (tiled-test): discovery-route latency + navigation primitives.

Measures, on a real registered dataset:
  D2 primitives  — len(dataset), first-100 keys page, one entity metadata GET,
                   one artifact metadata GET (the "nav tax" as data-model cost).
  D1 discovery   — three routes to (params + locators) for a filtered entity subset:
                   route=search : server-side client.search(Key(p) >= t) + sweep of matches
                   route=sweep  : query_catalog-style full client.items() sweep, filter in pandas
                   route=parquet: read the dataset's artifacts/entities parquet manifest directly
                   at ~50% / ~10% / ~1% selectivity.

Env: TILED_URL, TILED_API_KEY. Usage: dm_remote.py OUT.CSV [DATASET_KEY]
Appends one CSV row per measurement; population stamped from server info.
"""
import csv
import os
import sys
import time
from datetime import datetime, timezone

import pandas as pd
from tiled.client import from_uri
from tiled.queries import Key

OUT = sys.argv[1]
PREFERRED = [sys.argv[2]] if len(sys.argv) > 2 else ["VDP", "EDRIXS", "EGRESS_PE_1M_BROKER"]

client = from_uri(os.environ["TILED_URL"], api_key=os.environ["TILED_API_KEY"])
server_version = (client.context.server_info.library_version
                  if hasattr(client.context.server_info, "library_version")
                  else dict(client.context.server_info).get("library_version", "?"))

root_keys = list(client.keys())
ds_key = next((k for k in PREFERRED if k in root_keys), None)
assert ds_key, f"none of {PREFERRED} on server; root has {root_keys[:20]}"
ds = client[ds_key]
print(f"[dm] server {server_version}, dataset {ds_key}")

rows = []
def rec(measure, wall_s, n=None, note=""):
    rows.append({
        "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "server_version": server_version, "dataset": ds_key,
        "measure": measure, "wall_s": round(wall_s, 4), "n": n, "note": note,
    })
    print(f"[dm] {measure:28s} {wall_s:8.3f}s  n={n} {note}")

# --- D2 primitives (3 reps each) ---
for rep in range(3):
    t0 = time.perf_counter(); n_ent = len(ds); rec("len(dataset)", time.perf_counter() - t0, n_ent, f"rep{rep}")
for rep in range(3):
    t0 = time.perf_counter(); keys = []
    for k in ds.keys():
        keys.append(k)
        if len(keys) >= 100:
            break
    rec("keys_first_100", time.perf_counter() - t0, len(keys), f"rep{rep}")
ent_key = keys[0]
for rep in range(3):
    t0 = time.perf_counter(); ent = ds[ent_key]; meta = dict(ent.metadata)
    rec("entity_metadata_get", time.perf_counter() - t0, len(meta), f"rep{rep}")
art_keys = list(ent.keys())
for rep in range(3):
    t0 = time.perf_counter(); art = ent[art_keys[0]]; _ = dict(art.metadata)
    rec("artifact_metadata_get", time.perf_counter() - t0, 1, f"rep{rep}")

# --- pick a numeric physics param + an artifact type from the sample entity ---
param = next((k for k, v in meta.items()
              if isinstance(v, (int, float)) and not isinstance(v, bool)
              and not k.startswith(("path_", "dataset_", "index_", "shape", "dtype", "file_"))), None)
assert param, f"no numeric param in {list(meta)[:20]}"
art_type = next((k[len("path_"):] for k in meta if k.startswith("path_")), art_keys[0])
print(f"[dm] filter param: {param!r}, artifact_type: {art_type!r}")

# --- D1 route=sweep: full items() sweep (query_catalog pattern), once ---
t0 = time.perf_counter()
sweep_rows = []
for k, node in ds.items():
    m = dict(node.metadata)
    m["ent_key"] = k
    sweep_rows.append(m)
sweep_df = pd.DataFrame(sweep_rows)
rec("route=sweep_full", time.perf_counter() - t0, len(sweep_df))

# thresholds for ~50/10/1% from the swept distribution (ground truth in hand)
vals = pd.to_numeric(sweep_df[param], errors="coerce").dropna()
for frac in (0.5, 0.1, 0.01):
    thr = float(vals.quantile(1 - frac))
    expected = int((vals >= thr).sum())

    # route=search: server-side filter, then sweep only the matches
    t0 = time.perf_counter()
    hits = ds.search(Key(param) >= thr)
    got = []
    for k, node in hits.items():
        m = dict(node.metadata)
        m["ent_key"] = k
        got.append(m)
    rec(f"route=search_{int(frac*100)}pct", time.perf_counter() - t0, len(got),
        f"{param}>={thr:.4g} expected~{expected}")

    # route=sweep: reuse the full sweep cost + pandas filter (filter cost ~0)
    t0 = time.perf_counter()
    _ = sweep_df[pd.to_numeric(sweep_df[param], errors="coerce") >= thr]
    rec(f"route=sweepfilter_{int(frac*100)}pct", time.perf_counter() - t0, len(_),
        "pandas filter only; add route=sweep_full for total")

# --- D1 route=parquet: the Mode A floor, if a manifest exists locally ---
man_root = os.environ.get("TCB_MANIFESTS",
    "/sdf/data/lcls/ds/prj/prjmaiqmag01/results/ajshack/tiled-catalog-broker/datasets/manifests")
cand = [p for p in (os.listdir(man_root) if os.path.isdir(man_root) else [])
        if ds_key.lower().replace("_", "") in p.lower().replace("_", "").replace(" ", "")]
if cand:
    ent_pq = os.path.join(man_root, cand[0], "entities.parquet")
    if os.path.exists(ent_pq):
        for rep in range(3):
            t0 = time.perf_counter()
            df = pd.read_parquet(ent_pq)
            _ = df[pd.to_numeric(df[param], errors="coerce") >= float(vals.quantile(0.5))] \
                if param in df.columns else df
            rec("route=parquet_read+filter", time.perf_counter() - t0, len(df), f"rep{rep} {cand[0]}")
else:
    print(f"[dm] no local manifest matching {ds_key} under {man_root} — parquet route skipped")

new = not os.path.exists(OUT)
with open(OUT, "a", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
    if new:
        w.writeheader()
    w.writerows(rows)
print(f"[dm] {len(rows)} rows -> {OUT}")
