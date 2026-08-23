"""artifact_read on a disjoint key shard — for multi-process request-rate probes.

Mirrors egbench.runner.run_http's artifact_read path exactly (same warmup
exclusion, same per-worker clients, same CallCollector layer split), plus a
--offset so concurrent processes read disjoint entities. Read-only reuse of
egbench internals; prints the same JSON as `egbench run`.
"""
import argparse
import json
import os
import queue
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

BENCH = "/sdf/data/lcls/ds/prj/prjmaiqmag01/results/ajshack/tiled-bench"
sys.path.insert(0, BENCH)

import httpx
from tiled.client import from_uri

from egbench.methods import fetch_artifact_read
from egbench.runner import TIMEOUT, _attached, _layers
from regbench.instruments import CallCollector

ap = argparse.ArgumentParser()
ap.add_argument("--dataset", required=True)
ap.add_argument("--artifact", default=None)
ap.add_argument("--n", type=int, required=True)
ap.add_argument("--offset", type=int, default=0)
ap.add_argument("--concurrency", type=int, default=1)
args = ap.parse_args()

url = os.environ["TILED_URL"]
api_key = os.environ["TILED_API_KEY"]

nodes = [from_uri(url, api_key=api_key, timeout=TIMEOUT)[args.dataset]
         for _ in range(args.concurrency)]
keys = list(nodes[0].keys())[args.offset:args.offset + args.n]

fetch_artifact_read(nodes[0], keys[0], args.artifact)  # warmup, untimed

node_q = queue.Queue()
for nd in nodes:
    node_q.put(nd)
tl = threading.local()
counts = {"found": 0, "payload": 0, "errors": 0}
lock = threading.Lock()


def worker(key):
    if not hasattr(tl, "node"):
        tl.node = node_q.get_nowait()
    try:
        found, payload, _, _ = fetch_artifact_read(tl.node, key, args.artifact)
    except (httpx.HTTPStatusError, httpx.TransportError) as e:
        print(f"[unit error] {str(e).split('?', 1)[0][:160]}")
        with lock:
            counts["errors"] += 1
        return
    with lock:
        counts["found"] += found
        counts["payload"] += payload


collector = CallCollector()
with _attached(collector, nodes):
    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        list(pool.map(worker, keys))
    wall_s = time.perf_counter() - t0

out = {"wall_s": wall_s, "entities": counts["found"],
       "expected_entities": len(keys), "errors": counts["errors"],
       "payload_mb": counts["payload"] / 1e6, "decode_s": 0.0}
out.update(_layers(collector, fallback_wire=counts["payload"]))
print(json.dumps(out))
