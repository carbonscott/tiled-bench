"""Main-thread raw_export measurement — workaround for the upstream limitation.

tiled's download() installs a SIGINT handler unconditionally
(tiled/client/download.py:149 in 0.2.13; :98 in 0.2.9), which raises
ValueError off the main thread — so egbench's worker-pool cannot drive
raw_export. This one-off reproduces egbench's timed region exactly
(warmup outside, fan-out inside; here fan-out is serial on the main thread),
attaches the same CallCollector, and prints the same JSON as egbench.cli run.

Concurrency: always 1 in-process (main thread only). Scale by processes.

Usage:
  raw_export_main.py --dataset K --artifact A --layout L --n N --location loc
                     [--download-dir D | --ram]
"""
import argparse
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, "/sdf/data/lcls/ds/prj/prjmaiqmag01/results/ajshack/tiled-bench")

import httpx
from tiled.client import from_uri

from regbench.instruments import CallCollector, percentile

TIMEOUT = httpx.Timeout(5.0, connect=10.0, read=300.0)
DATA_PATHS = ("/full/", "/asset/bytes")

ap = argparse.ArgumentParser()
ap.add_argument("--dataset", required=True)
ap.add_argument("--artifact", required=True)
ap.add_argument("--layout", required=True,
                choices=("per_entity", "batched", "grouped"))
ap.add_argument("--n", type=int, default=0)
ap.add_argument("--offset", type=int, default=0,
                help="skip this many keys first (process-sharding)")
ap.add_argument("--location", default="local")
ap.add_argument("--download-dir", default=None)
ap.add_argument("--ram", action="store_true",
                help="use the 0.2.13 MutableMapping destination (in-RAM)")
ap.add_argument("--method", default="raw_export",
                choices=("raw_export", "asset_bytes"),
                help="asset_bytes = same skeleton, stream + discard")
args = ap.parse_args()

if args.location == "local":
    state = json.loads(Path("/sdf/data/lcls/ds/prj/prjmaiqmag01/results/ajshack/"
                            "tiled-bench/_egbench_ws/server.json").read_text())
    url, api_key = state["uri"], state["api_key"]
else:
    url = os.environ.get("TILED_URL")
    api_key = os.environ.get("TILED_API_KEY")

node = from_uri(url, api_key=api_key or None, timeout=TIMEOUT)[args.dataset]
keys = list(node.keys())
keys = keys[args.offset:]
keys = keys[: args.n] if args.n else keys

# warmup: metadata only (same as egbench — downloading would double a big run)
node[keys[0]][args.artifact].include_data_sources()

units = [[k] for k in keys] if args.layout == "per_entity" else [keys]


def run_unit(unit):
    art = node[unit[0]][args.artifact]
    if args.method == "asset_bytes":
        art = art.include_data_sources()
        asset = art.data_sources()[0].assets[0]
        u = art.item["links"]["self"].replace("/metadata", "/asset/bytes", 1)
        moved = 0
        with art.context.http_client.stream("GET", u,
                                            params={"id": asset.id}) as r:
            r.raise_for_status()
            for chunk in r.iter_bytes():
                moved += len(chunk)
        return len(unit), moved
    if args.ram:
        buffers = {}
        art.raw_export(buffers)
        return len(unit), sum(b.getbuffer().nbytes for b in buffers.values())
    dest = args.download_dir or "."
    return len(unit), sum(Path(p).stat().st_size for p in art.raw_export(dest))


collector = CallCollector()
found = wire = errors = 0
with collector.attached(node):
    t0 = time.perf_counter()
    for unit in units:
        try:
            f, w = run_unit(unit)
        except (httpx.HTTPStatusError, httpx.TransportError) as e:
            print(f"[unit error] {str(e).split('?', 1)[0][:160]}")
            errors += 1
            continue
        found += f
        wire += w
    wall_s = time.perf_counter() - t0


def is_data(s):
    return any(p in s.path for p in DATA_PATHS)


data = [s for s in collector.samples if is_data(s)]
nav = [s for s in collector.samples if not is_data(s)]
walls = [s.wall_ms for s in data if s.wall_ms is not None]
apps = [s.app_ms for s in data if s.app_ms is not None]
wire_hdr = sum(s.bytes for s in data if s.bytes)

print(json.dumps({
    "wall_s": wall_s,
    "entities": found,
    "expected_entities": len(keys),
    "errors": errors,
    "payload_mb": 0.0,
    "decode_s": 0.0,
    "wire_mb": (wire_hdr or wire) / 1e6,
    "req_p50_ms": percentile(walls, 50),
    "req_p95_ms": percentile(walls, 95),
    "req_p99_ms": percentile(walls, 99),
    "app_p50_ms": percentile(apps, 50),
    "app_sum_s": sum(apps) / 1000.0,
    "net_sum_s": sum(s.wall_ms - s.app_ms for s in data
                     if s.wall_ms is not None and s.app_ms is not None) / 1000.0,
    "nav_sum_s": sum(s.wall_ms for s in nav if s.wall_ms is not None) / 1000.0,
}))
