"""Watch the server side while load is on. Every observer may be unreachable; say so.

An observer is ``snapshot() -> dict | None``. ``Sampler`` polls a set of them on a timer
into JSONL, and records which ones answered, so a claim can state what it was blind to.

    TiledMetrics   /api/v1/metrics: request histograms and SQLAlchemy pool gauges
                   (checked-out, first-overflow, at-max). Needs a key with the metrics scope.
    PgActivity     pg_stat_activity grouped by state and wait event, through any psql command
                   (``kubectl exec ... -- psql ...`` works). The database's own view of
                   concurrency: a fixed pool shows a flat count of active backends with
                   nothing waiting; a CPU-bound database shows active tracking offered load.
    Reported       nothing to poll; carries what the operator told us (pod CPU, throttling)
                   so a run's observe.jsonl can hold it next to the samples.
"""

import json
import re
import subprocess
import threading
import time
from pathlib import Path

import httpx

from .workload import auth

_PROM = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+([-+0-9.eE]+|NaN|[+-]Inf)')


class TiledMetrics:
    name = "tiled_metrics"

    def __init__(self, url, api_key=None, prefix=("db_pool", "tiled", "http")):
        self.url, self.api_key, self.prefix = url, api_key, prefix

    def snapshot(self):
        try:
            r = httpx.get(self.url.rstrip("/") + "/api/v1/metrics", headers=auth(self.api_key), timeout=30)
        except httpx.HTTPError:
            return None
        if r.status_code != 200:
            return None
        out = {}
        for line in r.text.splitlines():
            m = _PROM.match(line)
            if m and m.group(1).startswith(self.prefix):
                out[m.group(1) + (m.group(2) or "")] = float(m.group(3))
        return out


class PgActivity:
    name = "pg_activity"
    SQL = ("SELECT datname, state, coalesce(wait_event_type,''), coalesce(wait_event,''), count(*) "
           "FROM pg_stat_activity WHERE backend_type='client backend' GROUP BY 1,2,3,4")
    XACT = "SELECT datname, xact_commit, tup_returned FROM pg_stat_database WHERE datname IS NOT NULL"

    def __init__(self, psql_cmd):
        self.cmd = list(psql_cmd)

    def _q(self, sql):
        r = subprocess.run([*self.cmd, "-At", "-F", "\t", "-c", sql], capture_output=True, text=True, timeout=60)
        if r.returncode != 0:
            raise RuntimeError(r.stderr[-300:])
        return [line.split("\t") for line in r.stdout.splitlines() if line]

    def snapshot(self):
        try:
            return {"activity": self._q(self.SQL), "xact": self._q(self.XACT)}
        except Exception:  # noqa: BLE001
            return None


class Reported:
    name = "reported"

    def __init__(self, **fields):
        self.fields = fields

    def snapshot(self):
        return dict(self.fields)


class Sampler:
    """Poll observers at ``hz`` into ``out`` (JSONL) for the length of a ``with`` block."""

    def __init__(self, observers, out, hz=1.0):
        self.observers, self.out, self.period = list(observers), Path(out), 1.0 / hz
        self.answered = {o.name: 0 for o in self.observers}
        self.asked = 0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)

    def _loop(self):
        with open(self.out, "a") as f:
            while not self._stop.is_set():
                t = time.time()
                rec = {"t": t}
                for o in self.observers:
                    snap = o.snapshot()
                    rec[o.name] = snap
                    if snap is not None:
                        self.answered[o.name] += 1
                self.asked += 1
                f.write(json.dumps(rec) + "\n")
                f.flush()
                self._stop.wait(max(0.0, self.period - (time.time() - t)))

    def __enter__(self):
        self.out.parent.mkdir(parents=True, exist_ok=True)
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self._thread.join(timeout=self.period + 90)
        return False

    def coverage(self):
        """Which observers answered, as a fraction of polls; the 'blind to' statement."""
        return {k: (v / self.asked if self.asked else 0.0) for k, v in self.answered.items()}


def once(observers):
    return {o.name: o.snapshot() for o in observers}
