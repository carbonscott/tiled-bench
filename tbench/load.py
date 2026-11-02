"""Drive a read path under load and keep every observation.

    run = load.run(work, items, client, threads=8, procs=8)

``work(node, item) -> dict`` is one unit of work (any function in ``tbench.reads``, or your
own). ``client() -> node`` builds a fresh Tiled node; it is called once per thread, inside
the process that will use it, because an ``httpx.Client`` is neither thread-safe nor
fork-safe. ``items`` is sharded across processes (``items[p::procs]``) and pulled from a
queue by the threads of each process, so every item runs exactly once.

Scale by process, not only by thread: one Python process tops out near 540 req/s on
``/healthz`` and ~190 MB/s on data (GIL in TLS and body handling, see the 2026-07 profiles).

What the timed region contains, deliberately: everything a real client pays, including
navigation GETs. What it excludes: client construction, key listing, and one untimed
warm-up unit per process. All processes start together on a barrier.

The result is a ``Run``: one record per unit, one record per HTTP call (with every
Server-Timing segment), the summed counts, and the wall time. ``Run.summary()`` derives
rates, percentiles and the Little's-law in-flight; ``Run.assert_clean()`` is the guardrail
that a fast-but-wrong run is a failed run.
"""

import json
import multiprocessing as mp
import os
import queue
import socket
import statistics
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

from .timing import Collector

_NUMERIC = (int, float)


@dataclass
class Run:
    wall_s: float
    units: list            # one dict per unit: proc, thread, item, t0, t1, ok, error, counts...
    calls: list            # one dict per HTTP call, see tbench.timing.Collector
    totals: dict           # numeric counts summed over successful units
    procs: int
    threads: int
    label: str = ""
    meta: dict = field(default_factory=dict)

    # ---- derived ----
    def summary(self):
        """Rates, latency percentiles per Server-Timing segment, and in-flight.

        ``in_flight`` is the Little's-law quantity measured directly: total server app time
        divided by wall time, i.e. the time-average number of requests inside the app
        handler. ``offered`` is procs × threads, the closed-loop concurrency presented.
        """
        ok = [u for u in self.units if u["ok"]]
        calls_ok = [c for c in self.calls if 200 <= c["status"] < 300]
        segs = sorted({k[:-3] for c in self.calls for k in c if k.endswith("_ms")})
        s = {
            "label": self.label,
            "procs": self.procs,
            "threads": self.threads,
            "offered": self.procs * self.threads,
            "wall_s": self.wall_s,
            "units": len(self.units),
            "units_ok": len(ok),
            "units_err": len(self.units) - len(ok),
            "unit_per_s": len(ok) / self.wall_s if self.wall_s else None,
            "calls": len(self.calls),
            "calls_err": len(self.calls) - len(calls_ok),
            "req_per_s": len(calls_ok) / self.wall_s if self.wall_s else None,
        }
        for seg in segs:
            vals = [c[f"{seg}_ms"] for c in calls_ok if f"{seg}_ms" in c]
            if vals:
                s[f"{seg}_p50_ms"] = _pct(vals, 50)
                s[f"{seg}_p95_ms"] = _pct(vals, 95)
                s[f"{seg}_sum_s"] = sum(vals) / 1000.0
        if "app_sum_s" in s and self.wall_s:
            s["in_flight"] = s["app_sum_s"] / self.wall_s
        for k, v in self.totals.items():
            s[f"total_{k}"] = v
        wire = self.totals.get("wire") or sum(c["bytes"] or 0 for c in calls_ok)
        if wire and self.wall_s:
            s["wire_mb_per_s"] = wire / 1e6 / self.wall_s
        return s

    def assert_clean(self, expected_units=None):
        """A valid run has no failed units, no failed calls, and covered every item."""
        bad = [u for u in self.units if not u["ok"]]
        if bad:
            raise AssertionError(f"{len(bad)} failed units, first: {bad[0]['error']}")
        err = [c for c in self.calls if not 200 <= c["status"] < 300]
        if err:
            raise AssertionError(f"{len(err)} non-2xx calls, first: {err[0]}")
        if expected_units is not None and len(self.units) != expected_units:
            raise AssertionError(f"{len(self.units)} units, expected {expected_units}")
        return self

    def save(self, directory, name=None):
        """Write ``<name>.summary.json``, ``<name>.units.jsonl``, ``<name>.calls.jsonl``."""
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=True)
        name = name or self.label or "run"
        (directory / f"{name}.summary.json").write_text(
            json.dumps({**self.summary(), "meta": self.meta}, indent=1))
        with open(directory / f"{name}.units.jsonl", "w") as f:
            for u in self.units:
                f.write(json.dumps(u) + "\n")
        with open(directory / f"{name}.calls.jsonl", "w") as f:
            for c in self.calls:
                f.write(json.dumps(c) + "\n")
        return directory / f"{name}.summary.json"


def run(work, items, client, threads=1, procs=1, label="", warmup=True, meta=None):
    """Run ``work`` over ``items`` with ``procs`` processes × ``threads`` threads."""
    items = list(items)
    if not items:
        raise ValueError("no items")
    if procs == 1:
        barrier = threading.Barrier(1)
        res = [_shard(0, items, work, client, threads, warmup, barrier)]
    else:
        ctx = mp.get_context("fork")
        barrier = ctx.Barrier(procs)
        q = ctx.Queue()
        ps = [ctx.Process(target=_child, args=(q, p, items[p::procs], work, client, threads,
                                               warmup, barrier)) for p in range(procs)]
        for p in ps:
            p.start()
        res = [q.get() for _ in ps]
        for p in ps:
            p.join()
        fatal = [r for r in res if "fatal" in r]
        if fatal:
            raise RuntimeError("worker process failed: " + fatal[0]["fatal"])
    t0 = min(r["t0"] for r in res)
    t1 = max(r["t1"] for r in res)
    units, calls = [], []
    for r in res:
        off = r["t0"] - t0  # re-base each process's clock onto the earliest start
        for u in r["units"]:
            u["t0"] += off
            u["t1"] += off
        for c in r["calls"]:
            if c["t0"] is not None:
                c["t0"] += off
            c["t1"] += off
        units += r["units"]
        calls += r["calls"]
    units.sort(key=lambda u: u["t0"])
    calls.sort(key=lambda c: c["t1"])
    totals = {}
    for u in units:
        if u["ok"]:
            for k, v in u.items():
                if k not in _UNIT_KEYS and isinstance(v, _NUMERIC) and not isinstance(v, bool):
                    totals[k] = totals.get(k, 0) + v
    return Run(wall_s=t1 - t0, units=units, calls=calls, totals=totals, procs=procs,
               threads=threads, label=label, meta=meta or {})


_UNIT_KEYS = {"proc", "thread", "item", "t0", "t1", "ok", "error"}


def _child(q, proc, shard, work, client, threads, warmup, barrier):
    try:
        q.put(_shard(proc, shard, work, client, threads, warmup, barrier))
    except Exception as e:  # noqa: BLE001 - report, don't hang the parent
        try:
            barrier.abort()
        except Exception:
            pass
        q.put({"fatal": repr(e), "t0": 0.0, "t1": 0.0, "units": [], "calls": []})


def _shard(proc, shard, work, client, threads, warmup, barrier):
    """One process's share: build clients, warm up, wait for the others, go."""
    nodes = [client() for _ in range(threads)]
    if warmup and shard:
        try:
            work(nodes[0], shard[0])
        except Exception:
            pass  # the timed run will report it properly
    node_q = queue.Queue()
    for nd in nodes:
        node_q.put(nd)
    tl = threading.local()
    units = []
    lock = threading.Lock()
    collector = Collector()

    def unit(item):
        if not hasattr(tl, "node"):
            tl.node = node_q.get_nowait()
        rec = {"proc": proc, "thread": threading.current_thread().name, "item": str(item),
               "t0": time.perf_counter() - collector.epoch}
        try:
            counts = work(tl.node, item)
            rec.update(counts or {})
            rec["ok"] = True
            rec["error"] = None
        except Exception as e:  # noqa: BLE001 - a failed unit is data, not a crash
            rec["ok"] = False
            rec["error"] = f"{type(e).__name__}: {str(e).split('?', 1)[0][:200]}"
        rec["t1"] = time.perf_counter() - collector.epoch
        with lock:
            units.append(rec)

    barrier.wait()
    collector.epoch = time.perf_counter()
    with collector.attached(nodes):
        with ThreadPoolExecutor(max_workers=threads) as pool:
            list(pool.map(unit, shard))
    end = time.perf_counter()
    return {"t0": collector.epoch, "t1": end, "units": units, "calls": collector.records}


def _pct(vals, p):
    vals = sorted(vals)
    k = max(0, min(len(vals) - 1, int(round(p / 100 * (len(vals) - 1)))))
    return vals[k]


class Round:
    """One question, one directory: ``rounds/<date>-<slug>/`` with ``meta.json``.

        with load.Round("knee", url=URL, api_key=KEY) as rd:
            rd.save(run, "ladder-t08")

    ``meta.json`` records what was measured against: date, host, git sha of this repo,
    client tiled version, server version, and anything passed in ``extra``. Commit the
    round directory (script + raw records + FINDINGS.md) when the round is done.
    """

    def __init__(self, slug, url=None, api_key=None, root="rounds", extra=None, day=None):
        day = day or date.today().isoformat()
        self.dir = Path(root) / f"{day}-{slug}"
        self.url, self.api_key, self.extra = url, api_key, extra or {}

    def __enter__(self):
        self.dir.mkdir(parents=True, exist_ok=True)
        meta = {
            "date": date.today().isoformat(),
            "host": socket.gethostname(),
            "python": sys.version.split()[0],
            "git_sha": _git_sha(),
            "argv": sys.argv,
            "slurm_job": os.environ.get("SLURM_JOB_ID"),
        }
        try:
            import tiled
            meta["tiled_client"] = tiled.__version__
        except Exception:
            pass
        if self.url:
            from .probes import server_version
            meta["url"] = self.url
            try:
                meta["server_version"] = server_version(self.url, self.api_key)
            except Exception as e:  # noqa: BLE001
                meta["server_version"] = f"unavailable: {e!r}"
        meta.update(self.extra)
        self.meta = meta
        (self.dir / "meta.json").write_text(json.dumps(meta, indent=1))
        return self

    def __exit__(self, *exc):
        return False

    def save(self, run, name=None):
        run.meta = {**self.meta, **run.meta}
        name = name or run.label or "run"
        path = run.save(self.dir, name)
        s = run.summary()
        print(f"[{self.dir.name}] {name}: {s.get('req_per_s', 0) or 0:.1f} req/s  "
              f"app_p50={s.get('app_p50_ms', float('nan')):.0f} ms  "
              f"in_flight={s.get('in_flight', float('nan')):.1f}  "
              f"err={s['units_err']}/{s['units']}", flush=True)
        return path


def _git_sha():
    try:
        return subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True,
                              text=True, timeout=10).stdout.strip() or None
    except Exception:
        return None
