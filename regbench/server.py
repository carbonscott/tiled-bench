"""Launch a local Tiled catalog server as a subprocess — the ``local`` location.

A fresh SQLite catalog per launch (``--init``), RTT≈0, and a PID we can hand to py-spy
later (see ``LocalTiledServer.pid``). This is the profiling home and the white-box target
from BENCHMARK-PLAN.md.

Launch line mirrors what the TCB tests assume a human runs:
    tiled serve catalog <db> --init --api-key secret --port <p> --read <workspace>

``--read <workspace>`` is required so the server will accept external HDF5 ``file://`` asset
URIs living under that root; every synthetic dataset is created beneath it.
"""

import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from contextlib import contextmanager
from pathlib import Path

DEFAULT_API_KEY = "secret"


def _free_port() -> int:
    """Grab an OS-assigned free port. Small TOCTOU window before the server binds it,
    acceptable for a single-user benchmark box."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _tiled_bin() -> str:
    """The ``tiled`` console script next to the active interpreter (the TCB venv)."""
    cand = Path(sys.executable).parent / "tiled"
    return str(cand) if cand.exists() else "tiled"


class LocalTiledServer:
    """Handle for a running local server. Use via :func:`local_server` (context manager)."""

    def __init__(self, uri: str, api_key: str, proc: subprocess.Popen, db_path: Path):
        self.uri = uri
        self.api_key = api_key
        self.proc = proc
        self.db_path = db_path

    @property
    def pid(self) -> int:
        """Server PID — the target for ``py-spy record --pid`` in the white-box suite."""
        return self.proc.pid


def _wait_until_up(uri: str, proc: subprocess.Popen, timeout: float = 30.0):
    """Poll until the server answers HTTP. Connection refused / resets are the expected
    state while it boots, so they're swallowed in the loop rather than guarded per-call."""
    deadline = time.time() + timeout
    probe = uri.rstrip("/") + "/api/v1/"
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(
                f"tiled server exited early (code {proc.returncode}) before serving"
            )
        try:
            with urllib.request.urlopen(probe, timeout=2) as r:
                r.read(1)
            return  # any HTTP response (even 401) means it's listening
        except urllib.error.HTTPError:
            return  # 401/403 etc. — server is up, just guarding the endpoint
        except (urllib.error.URLError, ConnectionError, socket.timeout):
            time.sleep(0.15)  # not up yet
    raise TimeoutError(f"tiled server did not come up at {uri} within {timeout}s")


@contextmanager
def local_server(workspace: Path, api_key: str = DEFAULT_API_KEY, port: int | None = None):
    """Run a fresh local Tiled catalog for the duration of the ``with`` block.

    Args:
        workspace: Root the server may read HDF5 assets from (``--read``); also where the
            catalog ``.db`` is created. Every dataset under test must live beneath it.
        api_key: Single-user API key.
        port: Bind port; a free one is chosen if omitted.

    Yields:
        LocalTiledServer with ``.uri``, ``.api_key``, ``.pid``.
    """
    workspace = Path(workspace)
    workspace.mkdir(parents=True, exist_ok=True)
    db_path = workspace / "catalog.db"
    if db_path.exists():
        db_path.unlink()  # --init refuses to clobber; guarantee a fresh DB per run

    port = port or _free_port()
    uri = f"http://127.0.0.1:{port}"

    cmd = [
        _tiled_bin(), "serve", "catalog", str(db_path),
        "--init",
        "--api-key", api_key,
        "--host", "127.0.0.1",
        "--port", str(port),
        "--read", str(workspace),
        "--write", str(workspace),
    ]
    # Route the server's per-request logging to a file in the workspace, not the console —
    # a single sweep is thousands of requests and would bury the harness output. The log stays
    # available for debugging a failed run.
    log_path = workspace / "tiled_server.log"
    log_file = open(log_path, "w")
    # Inherit env so the subprocess resolves the same tiled/tcb install as the harness.
    proc = subprocess.Popen(cmd, env=os.environ.copy(), stdout=log_file, stderr=subprocess.STDOUT)
    server = LocalTiledServer(uri, api_key, proc, db_path)
    try:
        _wait_until_up(uri, proc)
        yield server
    finally:
        proc.terminate()
        # Give it a moment to flush SQLite; escalate if it ignores SIGTERM.
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
        log_file.close()
