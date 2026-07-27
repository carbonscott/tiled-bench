"""Launch a local Tiled catalog server as a subprocess — the ``local`` location.

A fresh SQLite catalog per launch (``--init``), RTT≈0, and a PID we can hand to py-spy
later (see ``LocalTiledServer.pid``). This is the profiling home and the white-box target
from BENCHMARK-PLAN.md.

Launched via ``tiled serve config`` with a generated config file rather than the bare
``tiled serve catalog`` CLI: the config form is the only way to pass
``adapters_by_mimetype``, which maps the broker's private ``application/x-hdf5-broker``
mimetype to :class:`tiled_catalog_broker.adapters.LazyHDF5ArrayAdapter`. Without that
mapping, broker-mimetype nodes register fine but 500 on every read (no adapter).

``readable_storage`` must cover every HDF5 ``file://`` asset root; every synthetic
dataset is created beneath the workspace, and the egress benchmark adds ``data-source/``.
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

import yaml

DEFAULT_API_KEY = "secret"

# Private mimetype → adapter, mirroring the shared-server admin config. Keep in sync
# with tiled_catalog_broker.http_register (the registration-side default).
BROKER_ADAPTERS = {
    "application/x-hdf5-broker": "tiled_catalog_broker.adapters:LazyHDF5ArrayAdapter",
}


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


def _wait_until_up(uri: str, proc: subprocess.Popen, timeout: float = 600.0):
    # 600 s: a cold boot on S3DF's shared filesystem spends minutes stat()ing imports
    # (observed: `import tiled.server.app` = 4m23s wall, 7s CPU on a cold node).
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
def local_server(workspace: Path, api_key: str = DEFAULT_API_KEY, port: int | None = None,
                 read_paths=None, fresh_db: bool = True):
    """Run a local Tiled catalog for the duration of the ``with`` block.

    Args:
        workspace: Root the server may read HDF5 assets from (``--read``); also where the
            catalog ``.db`` is created. Every dataset under test must live beneath it.
        api_key: Single-user API key.
        port: Bind port; a free one is chosen if omitted.
        read_paths: Extra ``--read`` roots beyond the workspace (e.g. ``data-source/`` for
            the egress benchmark, whose HDF5 lives outside the workspace).
        fresh_db: Delete any existing catalog and ``--init`` a new one (regbench's default —
            registration timing needs an empty catalog). The egress benchmark passes False
            to keep its registered datasets across server restarts.

    Yields:
        LocalTiledServer with ``.uri``, ``.api_key``, ``.pid``.
    """
    workspace = Path(workspace)
    workspace.mkdir(parents=True, exist_ok=True)
    db_path = workspace / "catalog.db"
    if fresh_db and db_path.exists():
        db_path.unlink()  # --init refuses to clobber; guarantee a fresh DB per run

    port = port or _free_port()
    uri = f"http://127.0.0.1:{port}"

    config = {
        "trees": [{
            "path": "/",
            "tree": "catalog",
            "args": {
                "uri": f"sqlite:///{db_path}",
                "init_if_not_exists": True,
                "writable_storage": str(workspace),
                "readable_storage": [str(workspace)] + [str(rp) for rp in read_paths or ()],
                "adapters_by_mimetype": dict(BROKER_ADAPTERS),
            },
        }],
    }
    config_path = workspace / "server_config.yml"
    config_path.write_text(yaml.safe_dump(config))
    cmd = [
        _tiled_bin(), "serve", "config", str(config_path),
        "--api-key", api_key,
        "--host", "127.0.0.1",
        "--port", str(port),
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
