"""What the benchmark measures: one run -> a dict of measured numbers.

The timed region is exactly the fan-out of work units over the thread pool. Deliberately
*outside* it:

  - building the Tiled clients (``from_uri`` does a handshake)
  - listing entity keys
  - warmup (one work unit, so TCP connections and server-side caches are primed)

Deliberately *inside* it: everything a real client pays to get the data, including the
artifact_read path's per-entity metadata navigation (reported separately as ``nav_sum_s``).
Thread-pool spin-up is inside too — sub-millisecond against multi-second walls.

One Tiled client per worker (the underlying ``httpx.Client`` is not thread-safe), all
feeding one thread-safe ``CallCollector`` so the layer split covers every request issued.

Returns only what this process actually measured. Axes and dataset provenance are the
caller's to record — see cli.py.
"""

import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path

import httpx
from tiled.client import from_uri

from regbench.instruments import CallCollector, percentile

from .methods import (
    fetch_artifact_read,
    fetch_asset_bytes,
    fetch_container_export,
    fetch_raw_export,
    read_direct,
)

# Large batches legitimately exceed Tiled's default 30 s read timeout.
TIMEOUT = httpx.Timeout(5.0, connect=10.0, read=300.0)

# Requests that carry data, as opposed to metadata navigation. /full/ is the array and
# container endpoints; /asset/bytes is raw_export's file download.
DATA_PATHS = ("/full/", "/asset/bytes")


@contextmanager
def _attached(collector, nodes):
    """Attach one collector to several clients (one per worker thread).

    regbench's ``CallCollector.attached`` handles a single client; here every worker has
    its own httpx client and all must feed the same collector.
    """
    collector.samples = []
    collector._starts = {}
    hooked = []
    for nd in nodes:
        hooks = nd.context.http_client.event_hooks
        hooks["request"].append(collector._on_request)
        hooks["response"].append(collector._on_response)
        hooked.append(hooks)
    try:
        yield collector
    finally:
        for hooks in hooked:
            hooks["request"].remove(collector._on_request)
            hooks["response"].remove(collector._on_response)


def run_http(args, url, api_key):
    """container_export / artifact_read / raw_export / asset_bytes against a server."""
    nodes = [from_uri(url, api_key=api_key or None, timeout=TIMEOUT)[args.dataset]
             for _ in range(args.concurrency)]
    keys = list(nodes[0].keys())
    keys = keys[: args.n] if args.n else keys

    if args.method == "container_export":
        fetch_container_export(nodes[0], keys[:1], args.artifact)  # warmup, untimed
        units = [keys[i : i + args.export_batch]
                 for i in range(0, len(keys), args.export_batch)]

        def run_unit(node, unit):
            return fetch_container_export(node, unit, args.artifact)

    elif args.method == "artifact_read":
        fetch_artifact_read(nodes[0], keys[0], args.artifact)      # warmup, untimed
        units = [[k] for k in keys]

        def run_unit(node, unit):
            return fetch_artifact_read(node, unit[0], args.artifact)

    else:  # raw_export | asset_bytes — one unit per *asset*, which layout decides
        # A shared file serves every entity in one download; per_entity needs one each.
        # Warmup is metadata only: downloading the asset to prime the connection would
        # double a multi-GB run.
        nodes[0][keys[0]][args.artifact].include_data_sources()
        units = [[k] for k in keys] if args.layout == "per_entity" else [keys]

        if args.method == "raw_export":
            def run_unit(node, unit):
                return fetch_raw_export(node, unit, args.artifact, args.download_dir)
        else:
            def run_unit(node, unit):
                return fetch_asset_bytes(node, unit, args.artifact)

    node_q = queue.Queue()
    for nd in nodes:
        node_q.put(nd)
    tl = threading.local()
    counts = {"found": 0, "payload": 0, "wire": 0, "decode": 0.0, "errors": 0}
    lock = threading.Lock()

    def worker(unit):
        if not hasattr(tl, "node"):
            tl.node = node_q.get_nowait()  # <=concurrency threads, one client each
        try:
            found, payload, wire, dec = run_unit(tl.node, unit)
        except (httpx.HTTPStatusError, httpx.TransportError) as e:
            # Expected on the shared proxy: tail stalls that survive the method-level
            # retries. Count the unit so the run reports partial coverage instead of
            # losing every measurement taken so far.
            print(f"[unit error] {str(e).split('?', 1)[0][:160]}")
            with lock:
                counts["errors"] += 1
            return
        with lock:
            counts["found"] += found
            counts["payload"] += payload
            counts["wire"] += wire
            counts["decode"] += dec

    collector = CallCollector()
    with _attached(collector, nodes):
        t0 = time.perf_counter()
        with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
            list(pool.map(worker, units))
        wall_s = time.perf_counter() - t0

    out = {
        "wall_s": wall_s,
        "entities": counts["found"],
        "expected_entities": len(keys),
        "errors": counts["errors"],
        "payload_mb": counts["payload"] / 1e6,
        "decode_s": counts["decode"],
    }
    out.update(_layers(collector, fallback_wire=counts["wire"] or counts["payload"]))
    return out


def run_direct(args):
    """h5py_direct: read straight from disk through the same thread pool."""
    files = sorted(Path(args.data_dir).glob("*.h5"))
    # per_entity has one file per entity, so the count is on disk. batched/grouped share
    # one file and the count only exists inside it — the agent passes it as --n.
    n = args.n or (len(files) if args.layout == "per_entity" else 0)

    read_direct(files, args.layout, args.h5_path, 0, args.group_fmt)  # warmup, untimed

    payload = 0
    lock = threading.Lock()

    def worker(i):
        nonlocal payload
        nbytes = read_direct(files, args.layout, args.h5_path, i, args.group_fmt)
        with lock:
            payload += nbytes

    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        list(pool.map(worker, range(n)))
    wall_s = time.perf_counter() - t0

    return {
        "wall_s": wall_s,
        "entities": n,
        "expected_entities": n,
        "errors": 0,
        "payload_mb": payload / 1e6,
        "wire_mb": 0.0,          # no HTTP on this path
        "decode_s": 0.0,
        "req_p50_ms": None, "req_p95_ms": None, "req_p99_ms": None,
        "app_p50_ms": None, "app_sum_s": 0.0, "net_sum_s": 0.0, "nav_sum_s": 0.0,
    }


def _layers(collector, fallback_wire):
    """Split collected requests into data vs navigation; sum the layers."""
    def is_data(s):
        return any(p in s.path for p in DATA_PATHS)

    data = [s for s in collector.samples if is_data(s)]
    nav = [s for s in collector.samples if not is_data(s)]

    walls = [s.wall_ms for s in data if s.wall_ms is not None]
    apps = [s.app_ms for s in data if s.app_ms is not None]

    # Wire bytes: Content-Length when the server sends it; chunked responses don't carry
    # one, so fall back to measured body bytes (container/raw payload).
    wire = sum(s.bytes for s in data if s.bytes)

    return {
        "wire_mb": (wire or fallback_wire) / 1e6,
        "req_p50_ms": percentile(walls, 50),
        "req_p95_ms": percentile(walls, 95),
        "req_p99_ms": percentile(walls, 99),
        "app_p50_ms": percentile(apps, 50),
        "app_sum_s": sum(apps) / 1000.0,
        "net_sum_s": sum(s.wall_ms - s.app_ms for s in data
                         if s.wall_ms is not None and s.app_ms is not None) / 1000.0,
        "nav_sum_s": sum(s.wall_ms for s in nav if s.wall_ms is not None) / 1000.0,
    }
