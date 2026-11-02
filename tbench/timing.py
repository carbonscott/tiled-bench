"""Capture every HTTP call a Tiled client makes, with the server's full Server-Timing.

Tiled's ``capture_metrics`` middleware sets ``Server-Timing`` on every response, e.g.
``tok;dur=0.4, acl;dur=0.2, read;dur=12.1, pack;dur=0.9, app;dur=17.4``. Keep all of it:
``app`` is the whole handler, and the others say where inside it the time went (token
lookup, access control, catalog/adapter read, serialisation). The old harness kept only
``app`` and threw the rest away; that is what left "where does a request wait" unanswered.

One ``Collector`` can watch many clients (one per worker thread) at once.
"""

import threading
import time
from contextlib import contextmanager


def parse_server_timing(header):
    """``"tok;dur=0.4, app;dur=17.4"`` -> ``{"tok": 0.4, "app": 17.4}`` (ms)."""
    out = {}
    if not header:
        return out
    for seg in header.split(","):
        parts = seg.strip().split(";")
        name = parts[0].strip()
        for kv in parts[1:]:
            kv = kv.strip()
            if kv.startswith("dur="):
                try:
                    out[name] = float(kv[4:])
                except ValueError:
                    pass
    return out


class Collector:
    """One record per HTTP call: when, what, status, bytes, and every timing segment.

    Records are plain dicts so they serialise as they are:
        t0, t1      seconds since ``epoch`` (client-observed send / headers-received)
        method, path, status, bytes (Content-Length, None when chunked)
        thread      the calling thread's name
        <seg>_ms    one key per Server-Timing segment (``app_ms``, ``tok_ms``, ...)
    """

    def __init__(self, epoch=None):
        self.epoch = epoch if epoch is not None else time.perf_counter()
        self.records = []
        self._starts = {}
        self._lock = threading.Lock()

    def _on_request(self, request):
        with self._lock:
            self._starts[id(request)] = time.perf_counter()

    def _on_response(self, response):
        end = time.perf_counter()
        req = response.request
        with self._lock:
            start = self._starts.pop(id(req), None)
        clen = response.headers.get("content-length")
        rec = {
            "t0": None if start is None else start - self.epoch,
            "t1": end - self.epoch,
            "method": req.method,
            "path": req.url.path,
            "status": response.status_code,
            "bytes": int(clen) if clen else None,
            "thread": threading.current_thread().name,
        }
        for name, ms in parse_server_timing(response.headers.get("server-timing")).items():
            rec[f"{name}_ms"] = ms
        if start is not None:
            rec["wall_ms"] = (end - start) * 1000.0
        self.records.append(rec)  # list.append is atomic under the GIL

    @contextmanager
    def attached(self, nodes):
        """Hook every client in ``nodes`` for the block; unhook exactly what was added."""
        hooked = []
        for nd in nodes:
            hooks = nd.context.http_client.event_hooks
            hooks["request"].append(self._on_request)
            hooks["response"].append(self._on_response)
            hooked.append(hooks)
        try:
            yield self
        finally:
            for hooks in hooked:
                hooks["request"].remove(self._on_request)
                hooks["response"].remove(self._on_response)
