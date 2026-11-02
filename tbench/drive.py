"""Put load on the server two ways, and keep every call.

open_loop   requests arrive at a fixed rate whether or not earlier ones have returned.
            This is how capacity is measured: hold the arrival rate, watch latency. Below
            capacity latency is flat; at capacity it inflects; above it the backlog grows
            without bound. Little's law then reads directly: in-flight = rate x latency.
closed_loop N clients each wait for a reply before sending the next. This is what real
            dataloaders and the old thread ladders do. It cannot overload the server, so
            it finds a throughput plateau but not a capacity, and its "in-flight" is just
            N minus whoever is between requests. Use it to measure what N clients get, not
            what the server can do.

Both run on one asyncio loop in one process. One process drives a few thousand small
requests per second; ``procs`` forks more when a shape needs it (large bodies, TLS cost).
Every HTTP call is a record (see ``workload.call``); a ``Trace`` holds them with the
offered load and the wall clock, and summarises itself.
"""

import asyncio
import multiprocessing as mp
import time
from dataclasses import dataclass, field

import httpx

from .workload import auth


@dataclass
class Trace:
    mode: str              # "open" | "closed"
    offered: float         # open: arrival rate (req/s); closed: concurrency (clients)
    duration_s: float
    calls: list
    dropped: int = 0       # open loop only: arrivals skipped because max_inflight was hit
    meta: dict = field(default_factory=dict)

    def summary(self):
        ok = [c for c in self.calls if 200 <= c["status"] < 300]
        err = len(self.calls) - len(ok)
        s = {"mode": self.mode, "offered": self.offered, "duration_s": self.duration_s,
             "calls": len(self.calls), "ok": len(ok), "err": err, "dropped": self.dropped,
             "achieved": len(ok) / self.duration_s if self.duration_s else None,
             "err_rate": err / len(self.calls) if self.calls else None}
        segs = sorted({k[:-3] for c in ok for k in c if k.endswith("_ms")})
        for seg in segs:
            v = sorted(c[f"{seg}_ms"] for c in ok if f"{seg}_ms" in c)
            if v:
                s[f"{seg}_p50"] = _pct(v, 50)
                s[f"{seg}_p95"] = _pct(v, 95)
                s[f"{seg}_p99"] = _pct(v, 99)
                s[f"{seg}_mean"] = sum(v) / len(v)
        if "app_mean" in s and s["achieved"]:
            s["in_flight_app"] = s["achieved"] * s["app_mean"] / 1000.0   # Little, server side
            s["in_flight_wall"] = s["achieved"] * s["wall_mean"] / 1000.0  # Little, client side
        s["bytes"] = sum(c["bytes"] for c in ok)
        s["mb_per_s"] = s["bytes"] / 1e6 / self.duration_s if self.duration_s else None
        return s


def _pct(v, p):
    return v[max(0, min(len(v) - 1, int(round(p / 100 * (len(v) - 1)))))]


def _client(url, api_key, max_conn, timeout):
    return httpx.AsyncClient(base_url=url, headers=auth(api_key), http2=False,
                             timeout=httpx.Timeout(timeout, connect=10.0),
                             limits=httpx.Limits(max_connections=max_conn, max_keepalive_connections=max_conn))


async def _open(url, api_key, shape, targets, rate, duration, max_inflight, timeout, warmup):
    calls, dropped = [], 0
    async with _client(url, api_key, max_inflight, timeout) as c:
        if warmup:
            await shape(c, targets[0], time.perf_counter())
        sem = asyncio.Semaphore(max_inflight)
        tasks = set()
        clock0 = time.perf_counter()

        async def one(t):
            try:
                calls.extend(await shape(c, t, clock0))
            finally:
                sem.release()

        i = 0
        while True:
            due = i / rate
            now = time.perf_counter() - clock0
            if due >= duration:
                break
            if due > now:
                await asyncio.sleep(due - now)
            if sem.locked():
                dropped += 1          # the server is so far past capacity the client is full
            else:
                await sem.acquire()
                tasks.add(asyncio.create_task(one(targets[i % len(targets)])))
            i += 1
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        wall = time.perf_counter() - clock0
    return calls, dropped, wall


async def _closed(url, api_key, shape, targets, concurrency, duration, timeout, warmup):
    calls = []
    async with _client(url, api_key, concurrency, timeout) as c:
        if warmup:
            await shape(c, targets[0], time.perf_counter())
        clock0 = time.perf_counter()

        async def worker(w):
            i = w
            while time.perf_counter() - clock0 < duration:
                calls.extend(await shape(c, targets[i % len(targets)], clock0))
                i += concurrency

        await asyncio.gather(*(worker(w) for w in range(concurrency)))
        wall = time.perf_counter() - clock0
    return calls, 0, wall


def open_loop(url, api_key, shape, targets, rate, duration, procs=1, max_inflight=256,
              timeout=60.0, warmup=True):
    """Arrivals at ``rate`` req/s for ``duration`` s. With ``procs``>1 the rate is split."""
    parts = _fan(procs, _open, (url, api_key, shape, list(targets), rate / procs, duration,
                                max_inflight, timeout, warmup))
    calls = [c for p in parts for c in p[0]]
    return Trace("open", rate, max(p[2] for p in parts), sorted(calls, key=lambda c: c["t0"]),
                 dropped=sum(p[1] for p in parts), meta={"procs": procs})


def closed_loop(url, api_key, shape, targets, concurrency, duration, procs=1, timeout=60.0, warmup=True):
    """``concurrency`` clients in lock-step for ``duration`` s. With ``procs``>1 they are split."""
    per = max(1, concurrency // procs)
    parts = _fan(procs, _closed, (url, api_key, shape, list(targets), per, duration, timeout, warmup))
    calls = [c for p in parts for c in p[0]]
    return Trace("closed", per * procs, max(p[2] for p in parts), sorted(calls, key=lambda c: c["t0"]),
                 meta={"procs": procs})


def _fan(procs, coro, args):
    if procs == 1:
        return [asyncio.run(coro(*args))]
    ctx = mp.get_context("fork")
    q = ctx.Queue()

    def child():
        q.put(asyncio.run(coro(*args)))

    ps = [ctx.Process(target=child) for _ in range(procs)]
    for p in ps:
        p.start()
    out = [q.get() for _ in ps]
    for p in ps:
        p.join()
    return out
