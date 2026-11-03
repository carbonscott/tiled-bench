"""What the measurements were taken against.

Half of it can be observed from here (server version, client version, this host). The
other half only the operator knows: worker count, database backend, pooler and its mode,
pool sizes, CPU limits. Those arrive in conversation ("I scaled workers to 16") and are
recorded through ``ledger.intervene``; this module just merges the two halves and hashes
the fields that make results comparable into a short ``key``.

Two measurements are comparable only if their keys match. A claim made under one key is
stale under another.
"""

import hashlib
import json
import socket
import subprocess
import sys

import httpx

# Operator-reported fields that change what the server can do. Add here, never remove.
REPORTED_FIELDS = (
    "pods",             # replicas of the tiled deployment
    "workers_per_pod",  # uvicorn workers in each pod (image entrypoint / WEB_CONCURRENCY)
    "db",               # e.g. "postgres-cnpg-18", "sqlite"
    "pooler",           # e.g. "none", "pgbouncer-session", "pgbouncer-transaction"
    "db_max_connections",
    "catalog_pool_size",      # per process
    "catalog_max_overflow",   # per process
    "cpu_limit",        # tiled pod CPU limit, cores
    "db_cpu_limit",     # database primary CPU limit, cores
)


def observe(url, api_key=None):
    """The observable half. Never raises: an unreachable server is itself a fact."""
    out = {"url": url, "host": socket.gethostname(), "python": sys.version.split()[0]}
    try:
        import tiled
        out["client_version"] = tiled.__version__
    except Exception:  # noqa: BLE001
        out["client_version"] = None
    try:
        r = httpx.get(url.rstrip("/") + "/api/v1/",
                      headers={"Authorization": f"Apikey {api_key}"} if api_key else {},
                      timeout=30)
        r.raise_for_status()
        out["server_version"] = r.json().get("library_version")
    except Exception as e:  # noqa: BLE001
        out["server_version"] = None
        out["server_error"] = repr(e)[:200]
    try:
        out["git_sha"] = subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True,
                                        text=True, timeout=10).stdout.strip() or None
    except Exception:  # noqa: BLE001
        out["git_sha"] = None
    return out


def key(observed, reported):
    """Short hash of the fields that decide comparability."""
    stable = {"server_version": observed.get("server_version"),
              "url": observed.get("url"),
              **{f: reported.get(f) for f in REPORTED_FIELDS}}
    return hashlib.sha1(json.dumps(stable, sort_keys=True).encode()).hexdigest()[:10]


def merge(observed, reported):
    return {"key": key(observed, reported), "observed": observed, "reported": dict(reported)}
