"""Run one :class:`FetchConfig` → :class:`FetchResult`.

Client setup, key listing, and warmup happen *before* the timed region; the timed region
is exactly the fan-out of work units over the thread pool. One Tiled client per worker
(the underlying ``httpx.Client`` is not thread-safe), all feeding one thread-safe
``CallCollector`` so the layer split (app / net / nav) covers every request issued.
"""

import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

import httpx
import tiled
from tiled.client import from_uri

from regbench.instruments import CallCollector, percentile

from .config import FetchConfig, FetchResult
from .datasets import REGISTRY
from .methods import fetch_export, fetch_raw, read_direct

# Large batches legitimately exceed Tiled's default 30 s read timeout.
TIMEOUT = httpx.Timeout(5.0, connect=10.0, read=300.0)


@contextmanager
def _attached_many(collector, nodes):
    """Attach one collector's hooks to several clients (one per worker thread).

    regbench's ``CallCollector.attached`` handles a single client; here each worker has
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


def run_fetch(cfg: FetchConfig, url: str, api_key: str, rep: int) -> FetchResult:
    """Fetch ``cfg.n_entities`` entities per ``cfg`` and return the measured row."""
    spec = REGISTRY[cfg.dataset_key]
    res = FetchResult.from_config(cfg, rep)
    res.layout = spec.layout
    res.tiled_version = tiled.__version__
    res.url = url or ""
    n = min(cfg.n_entities or spec.n_entities, spec.n_entities)

    if cfg.method == "h5py_direct":
        _run_direct(res, cfg, spec, n)
        return res.finalize_rates()

    nodes = [from_uri(url, api_key=api_key or None, timeout=TIMEOUT)[cfg.dataset_key]
             for _ in range(cfg.concurrency)]
    keys = list(nodes[0].keys())[:n]
    if len(keys) < n:
        print(f"[warn] {cfg.dataset_key}: only {len(keys)} of {n} requested entities exist")

    # Warmup: prime TCP connections and server-side caches, untimed. A warmup failure
    # means the dataset/key is broken — fail loud rather than limp into the sweep.
    if cfg.method == "export_hdf5":
        fetch_export(nodes[0], keys[:1], spec.artifact)
        units = [keys[i : i + cfg.batch_size] for i in range(0, len(keys), cfg.batch_size)]

        def run_unit(node, unit):
            return fetch_export(node, unit, spec.artifact)
    else:  # raw_read
        fetch_raw(nodes[0], keys[0], spec.artifact)
        units = [[k] for k in keys]

        def run_unit(node, unit):
            return 1, fetch_raw(node, unit[0], spec.artifact), 0, 0.0

    node_q = queue.Queue()
    for nd in nodes:
        node_q.put(nd)
    tl = threading.local()
    counters = {"found": 0, "payload": 0, "container": 0, "decode": 0.0, "errors": 0}
    lock = threading.Lock()

    def worker(unit):
        if not hasattr(tl, "node"):
            tl.node = node_q.get_nowait()  # ≤concurrency threads, one pre-built client each
        try:
            found, payload, container, dec = run_unit(tl.node, unit)
        except (httpx.HTTPStatusError, httpx.TransportError) as e:
            # Expected on the shared proxy: tail stalls that survive the method-level
            # retries. Count the unit so the row shows partial coverage instead of
            # killing a whole sweep.
            print(f"  [unit error] {str(e).split('?', 1)[0][:160]}")
            with lock:
                counters["errors"] += 1
            return
        with lock:
            counters["found"] += found
            counters["payload"] += payload
            counters["container"] += container
            counters["decode"] += dec

    collector = CallCollector()
    with _attached_many(collector, nodes):
        t0 = time.perf_counter()
        with ThreadPoolExecutor(max_workers=cfg.concurrency) as pool:
            list(pool.map(worker, units))
        res.wall_s = time.perf_counter() - t0

    res.entities = counters["found"]
    res.errors = counters["errors"]
    res.payload_mb = counters["payload"] / 1e6
    res.decode_s = counters["decode"]
    _fill_from_samples(res, collector,
                       fallback_wire=counters["container"] or counters["payload"])
    if res.entities != len(keys):
        print(f"[warn] fetched {res.entities} entities, expected {len(keys)}")
    return res.finalize_rates()


def _run_direct(res: FetchResult, cfg: FetchConfig, spec, n: int):
    """h5py_direct: read n entities straight from disk through the same thread pool."""
    files = spec.files()
    if not files:
        raise FileNotFoundError(f"no HDF5 files under {spec.data_dir}")
    read_direct(spec, files, 0)  # warmup: h5py import + first-open cost, untimed

    counters = {"payload": 0}
    lock = threading.Lock()

    def worker(i):
        nbytes = read_direct(spec, files, i)
        with lock:
            counters["payload"] += nbytes

    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=cfg.concurrency) as pool:
        list(pool.map(worker, range(n)))
    res.wall_s = time.perf_counter() - t0
    res.entities = n
    res.payload_mb = counters["payload"] / 1e6
    res.wire_mb = 0.0


def _fill_from_samples(res: FetchResult, collector: CallCollector, fallback_wire: int):
    """Split collected requests into data (/full/ endpoints) vs navigation, fill layers."""
    data = [s for s in collector.samples if "/full/" in s.path]
    nav = [s for s in collector.samples if "/full/" not in s.path]

    walls = [s.wall_ms for s in data if s.wall_ms is not None]
    apps = [s.app_ms for s in data if s.app_ms is not None]
    res.req_p50_ms = percentile(walls, 50)
    res.req_p95_ms = percentile(walls, 95)
    res.req_p99_ms = percentile(walls, 99)
    res.app_p50_ms = percentile(apps, 50)
    res.app_sum_s = sum(apps) / 1000.0
    res.net_sum_s = sum(
        s.wall_ms - s.app_ms for s in data
        if s.wall_ms is not None and s.app_ms is not None
    ) / 1000.0
    res.nav_sum_s = sum(s.wall_ms for s in nav if s.wall_ms is not None) / 1000.0

    # Wire bytes: Content-Length when the server sends it; chunked responses don't
    # carry one, so fall back to measured body bytes (container/raw payload).
    wire = sum(s.bytes for s in data if s.bytes)
    res.wire_mb = (wire or fallback_wire) / 1e6
