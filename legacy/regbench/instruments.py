"""Per-call latency + server ``app;dur`` capture via an httpx event hook.

The Tiled client (``client.context.http_client``) is a single ``httpx.Client`` shared by
every thread in ``register_dataset_http``'s pool. Attaching request/response event hooks to
it captures *every* HTTP call the registration issues — with zero changes to TCB. That gives
us both halves of the layer-3/4 split from BENCHMARK-PLAN.md:

    app_ms   = Server-Timing `app;dur`  (server-side work; emitted on every response)
    wall_ms  = client-observed round trip
    wall_ms - app_ms  ≈  client prep + network RTT

The Tiled server's ``capture_metrics`` middleware always sets the header, e.g.
``Server-Timing: tok;dur=0.4, pack;dur=0.1, app;dur=17.4`` — so this works local *and* remote.
"""

import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass


@dataclass
class Sample:
    """One observed HTTP call."""

    method: str
    path: str
    status: int
    wall_ms: float | None   # client-observed round trip
    app_ms: float | None    # server Server-Timing app;dur
    bytes: int | None = None  # Content-Length — wire bytes (None when chunked/absent)


def _parse_server_timing(header: str | None) -> dict[str, float]:
    """Parse ``Server-Timing`` into ``{segment: dur_ms}``. Missing/blank → empty.

    Header grammar: comma-separated segments, each ``name;dur=NN[;extra=..]``. We only keep
    the ``dur`` of each named segment (``app``, ``tok``, ``pack``, ...).
    """
    out: dict[str, float] = {}
    if not header:
        return out
    for seg in header.split(","):
        parts = seg.strip().split(";")
        name = parts[0].strip()
        for kv in parts[1:]:
            if kv.strip().startswith("dur="):
                out[name] = float(kv.split("=", 1)[1])
    return out


class CallCollector:
    """Accumulates one :class:`Sample` per HTTP call while attached to a client.

    Thread-safe: ``register_dataset_http`` fires calls from a ThreadPoolExecutor, all sharing
    the client. ``list.append`` is atomic under the GIL; the start-time map is lock-guarded.
    """

    def __init__(self):
        self.samples: list[Sample] = []
        self._starts: dict[int, float] = {}
        self._lock = threading.Lock()

    # --- event hooks (httpx calls these; request before send, response after headers) ---
    def _on_request(self, request):
        with self._lock:
            self._starts[id(request)] = time.perf_counter()

    def _on_response(self, response):
        end = time.perf_counter()
        req = response.request
        with self._lock:
            start = self._starts.pop(id(req), None)
        wall_ms = (end - start) * 1000 if start is not None else None
        app_ms = _parse_server_timing(response.headers.get("server-timing")).get("app")
        clen = response.headers.get("content-length")
        self.samples.append(
            Sample(
                method=req.method,
                path=req.url.path,
                status=response.status_code,
                wall_ms=wall_ms,
                app_ms=app_ms,
                bytes=int(clen) if clen else None,
            )
        )

    @contextmanager
    def attached(self, client):
        """Attach hooks to a Tiled client for the duration of the block, then remove them.

        Resets ``samples`` on entry so each run measures only its own calls. Removes exactly
        the hooks it added so a client reused across runs never accumulates them.
        """
        hooks = client.context.http_client.event_hooks
        self.samples = []
        self._starts = {}
        hooks["request"].append(self._on_request)
        hooks["response"].append(self._on_response)
        try:
            yield self
        finally:
            hooks["request"].remove(self._on_request)
            hooks["response"].remove(self._on_response)

    # --- queries over the collected samples ---
    def filter(self, method=None, status_ok=None):
        """Samples matching a method and/or 2xx-status predicate."""
        out = self.samples
        if method is not None:
            out = [s for s in out if s.method == method]
        if status_ok is True:
            out = [s for s in out if 200 <= s.status < 300]
        elif status_ok is False:
            out = [s for s in out if not (200 <= s.status < 300)]
        return out


def percentile(values, p: float) -> float | None:
    """Nearest-rank percentile of ``values`` (0 ≤ p ≤ 100). None if empty."""
    vals = sorted(v for v in values if v is not None)
    if not vals:
        return None
    if len(vals) == 1:
        return vals[0]
    # nearest-rank: rank = ceil(p/100 * n), clamped to [1, n]
    rank = max(1, min(len(vals), int(-(-p / 100 * len(vals) // 1))))
    return vals[rank - 1]
