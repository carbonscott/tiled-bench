"""Look at the server side while a run is on: version, health, metrics, the database.

Nothing here needs code on the server. ``tiled_metrics`` reads Tiled's own Prometheus
endpoint (request histograms and the SQLAlchemy pool gauges: checked-out connections, the
first-overflow and at-max counters). ``PgSampler`` runs a ``psql`` command you supply on a
timer, so it works through ``kubectl exec`` or a port-forward alike.
"""

import json
import re
import subprocess
import threading
import time
from pathlib import Path

import httpx


def _headers(api_key):
    return {"Authorization": f"Apikey {api_key}"} if api_key else {}


def server_version(url, api_key=None):
    r = httpx.get(url.rstrip("/") + "/api/v1/", headers=_headers(api_key), timeout=30)
    r.raise_for_status()
    return r.json()["library_version"]


def healthz(url, n=1):
    """``n`` serial GETs of ``/healthz``: (status of last, p50 ms). The no-catalog floor."""
    walls = []
    status = None
    with httpx.Client(timeout=30) as c:
        for _ in range(n):
            t0 = time.perf_counter()
            status = c.get(url.rstrip("/") + "/healthz").status_code
            walls.append((time.perf_counter() - t0) * 1000)
    walls.sort()
    return status, walls[len(walls) // 2]


_PROM = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+([-+0-9.eE]+|NaN|[+-]Inf)')


def tiled_metrics(url, api_key=None, prefix=("db_pool", "tiled", "http")):
    """Tiled's ``/api/v1/metrics`` as ``{"name{labels}": value}`` for names in ``prefix``.

    The endpoint needs a key with the ``metrics`` scope. Without one it answers 401/403;
    then this returns ``{"_unavailable": <status>}`` so a round records the gap and goes on.
    """
    r = httpx.get(url.rstrip("/") + "/api/v1/metrics", headers=_headers(api_key), timeout=30)
    if r.status_code in (401, 403):
        return {"_unavailable": float(r.status_code)}
    r.raise_for_status()
    out = {}
    for line in r.text.splitlines():
        if line.startswith("#"):
            continue
        m = _PROM.match(line)
        if m and m.group(1).startswith(prefix):
            out[m.group(1) + (m.group(2) or "")] = float(m.group(3))
    return out


def metrics_delta(before, after):
    """Counters and gauges that moved between two ``tiled_metrics`` snapshots."""
    if "_unavailable" in after or "_unavailable" in before:
        return {"_unavailable": after.get("_unavailable", before.get("_unavailable"))}
    return {k: after[k] - before.get(k, 0.0) for k in after
            if after[k] != before.get(k, 0.0)}


PG_ACTIVITY_SQL = (
    "SELECT now(), datname, state, wait_event_type, wait_event, count(*) "
    "FROM pg_stat_activity WHERE backend_type = 'client backend' "
    "GROUP BY 1,2,3,4,5 ORDER BY 6 DESC"
)
PG_XACT_SQL = "SELECT now(), datname, xact_commit, tup_returned FROM pg_stat_database WHERE datname IS NOT NULL"


def psql(cmd, sql):
    """Run ``sql`` through ``cmd`` (e.g. ``["psql", "-h", host, "-U", user, dbname]`` or a
    ``kubectl exec ... -- psql ...`` prefix). Rows as lists of strings."""
    r = subprocess.run([*cmd, "-At", "-F", "\t", "-c", sql], capture_output=True, text=True,
                       timeout=60)
    if r.returncode != 0:
        raise RuntimeError(r.stderr.strip()[-400:])
    return [line.split("\t") for line in r.stdout.splitlines() if line]


def pg_connections(cmd):
    """Per database / user / state connection counts, one shot."""
    return psql(cmd, "SELECT datname, usename, state, count(*) FROM pg_stat_activity "
                     "GROUP BY 1,2,3 ORDER BY 4 DESC")


class PgSampler:
    """Sample ``pg_stat_activity`` (and ``pg_stat_database`` counters) at ``hz`` into JSONL.

        with PgSampler(["psql", ...], out="rounds/<round>/pg.jsonl", hz=1):
            run = load.run(...)

    Each line is ``{"t": epoch_s, "activity": rows, "xact": rows}``. Read it next to the
    run's ``calls.jsonl``: a semaphore-shaped limit shows a fixed number of ``active``
    backends and nothing waiting; a CPU-bound database shows ``active`` tracking offered
    load with empty wait events; a lock shows ``wait_event_type`` = Lock or LWLock.
    """

    def __init__(self, cmd, out, hz=1.0):
        self.cmd, self.out, self.period = list(cmd), Path(out), 1.0 / hz
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, name="pg-sampler", daemon=True)
        self.errors = 0

    def _loop(self):
        with open(self.out, "a") as f:
            while not self._stop.is_set():
                t = time.time()
                try:
                    rec = {"t": t, "activity": psql(self.cmd, PG_ACTIVITY_SQL),
                           "xact": psql(self.cmd, PG_XACT_SQL)}
                except Exception as e:  # noqa: BLE001 - keep sampling
                    self.errors += 1
                    rec = {"t": t, "error": repr(e)[:200]}
                f.write(json.dumps(rec) + "\n")
                f.flush()
                self._stop.wait(max(0.0, self.period - (time.time() - t)))

    def __enter__(self):
        self.out.parent.mkdir(parents=True, exist_ok=True)
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self._thread.join(timeout=self.period + 60)
        return False
