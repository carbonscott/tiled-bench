"""The core: run one registration and return a :class:`RunResult`.

Calls the *real* TCB path (``register_dataset_http``, per ADR 0001) against a Tiled client,
into a fresh timestamped ``dataset_key`` so timing is never polluted by existing-container
skips. Captures wall-clock + counts today; per-call and ``app;dur`` percentiles arrive when
the instruments land at the ``_INSTRUMENT SEAM`` below.
"""

import inspect
import os
import subprocess
import time
from pathlib import Path

import tiled
from tiled.client import from_uri
from tiled_catalog_broker.http_register import register_dataset_http
from tiled_catalog_broker.utils import get_artifact_info

from .config import WRITE_PATH_MIMETYPES, RunConfig, RunResult
from .instruments import CallCollector, percentile
from .manifests import Manifest


def make_client(uri: str, api_key: str):
    """Connect a Tiled client to a running server (local or remote)."""
    return from_uri(uri, api_key=api_key)


def _tcb_sha() -> str:
    """Short git SHA of the installed tiled-catalog-broker checkout (provenance for the
    profile-CSV provenance / version diffs). Returns '' if the package isn't a git checkout."""
    import tiled_catalog_broker

    repo = Path(tiled_catalog_broker.__file__).resolve().parents[2]
    proc = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "--short", "HEAD"],
        capture_output=True, text=True,
    )
    return proc.stdout.strip() if proc.returncode == 0 else ""


def _server_version(client) -> str:
    """The connected server's tiled version (About model or dict, version-dependent)."""
    info = client.context.server_info
    if hasattr(info, "library_version"):
        return str(info.library_version)
    return str(dict(info).get("library_version", ""))


def _fresh_dataset_key(cfg: RunConfig, rep: int) -> str:
    """A never-before-seen key so every entity does real INSERT work (no SELECT-hit skips)."""
    return f"regbench_{cfg.label()}_r{rep}_{int(time.time() * 1000)}"


def run_registration(cfg: RunConfig, manifest: Manifest, client, rep: int) -> RunResult:
    """Register ``manifest`` once under a fresh key; return the measured row.

    Args:
        cfg: The config being measured (axes copied into the result).
        manifest: Output of ``manifests.build_manifest`` — the DataFrames + metadata.
        client: A connected Tiled client (from ``make_client``).
        rep: Repetition index (0-based) for this config.
    """
    result = RunResult.from_config(cfg, rep)
    result.tiled_version = tiled.__version__
    result.tcb_sha = _tcb_sha()
    result.server_version = _server_version(client)
    result.dataset_key = _fresh_dataset_key(cfg, rep)

    # Remote servers see the filesystem at a different mount (K8s pod); asset
    # URIs must use the server-side path or every later read is refused. The
    # TILED_HOST_DATA_ROOT → TILED_SERVER_DATA_ROOT pair comes from .env.test
    # (same convention tcb inspect uses to fill data.server_base_dir).
    server_base_dir = None
    if cfg.location == "remote":
        host_root = os.environ.get("TILED_HOST_DATA_ROOT")
        server_root = os.environ.get("TILED_SERVER_DATA_ROOT")
        if host_root and server_root and manifest.base_dir.startswith(host_root):
            server_base_dir = server_root + manifest.base_dir[len(host_root):]

    # Clear TCB's per-file HDF5 shape/dtype cache so each run pays the layer-1 probe fresh
    # (a warm cache from a prior rep would hide the very cost we want to attribute). This
    # mirrors what TCB's own CLI does between datasets (cli.py:398).
    get_artifact_info.__defaults__[-1].clear()

    # Realize the write_path axis: stamp the mimetype column create_data_source reads.
    # Without this every artifact defaults to the broker adapter and 415s on a stock server.
    manifest.art_df["mimetype"] = WRITE_PATH_MIMETYPES[cfg.write_path]

    # A CallCollector attached to the client's shared httpx client records every HTTP call
    # register_dataset_http fires (all threads share one client), giving per-call wall latency
    # and the server's `Server-Timing: app;dur`. register_dataset_http needs no changes.
    # The dataset_key is always fresh (see _fresh_dataset_key), so the per-entity
    # existence GET is a guaranteed 404 round-trip. Optimized TCB branches expose
    # assume_new to skip it; pass it only when the installed TCB has the parameter
    # so this one runner drives both the before and after code states.
    kwargs = {}
    if "assume_new" in inspect.signature(register_dataset_http).parameters:
        kwargs["assume_new"] = True

    collector = CallCollector()
    with collector.attached(client):
        t0 = time.perf_counter()
        # A rep that dies mid-registration (e.g. the 500→retry→409 race) must
        # still produce a CSV row — the entity-count shortfall below flags it
        # for the guardrail, and analysis excludes flagged rows. Letting the
        # exception propagate would kill the whole sweep instead of one rep.
        try:
            register_dataset_http(
                client,
                manifest.ent_df,
                manifest.art_df,
                base_dir=manifest.base_dir,
                label=cfg.label(),
                dataset_key=result.dataset_key,
                dataset_metadata=manifest.dataset_metadata,
                server_base_dir=server_base_dir,
                max_workers=cfg.max_workers,
                **kwargs,
            )
        except Exception as e:
            print(f"[warn] rep failed mid-registration: {type(e).__name__}: {e}")
        result.wall_s = time.perf_counter() - t0

    _fill_call_metrics(result, collector)

    # Counts. register_dataset_http returns only a bool, so we confirm what actually landed:
    #   - entities: one cheap request — the dataset container's length on the server.
    #   - artifacts: expected count (per-entity confirmation is n_entities extra requests).
    # A gap between server entities and expected is a real failure, surfaced as a warning.
    result.entities = len(client[result.dataset_key])
    result.artifacts = len(manifest.art_df)
    if result.entities != len(manifest.ent_df):
        print(f"[warn] {result.entities} entities landed, expected {len(manifest.ent_df)} "
              f"— registration partially failed")
    result.finalize_rates()
    return result


def _fill_call_metrics(result: RunResult, collector: CallCollector):
    """Populate the per-call latency + app;dur percentiles from collected samples.

    Percentiles are over the successful *write* calls (2xx POSTs) — the container/datasource
    creates. GET existence-checks and any 4xx are excluded so the numbers reflect real
    registration work, not the wasted 404 probes (those are a separate optimization axis).
    """
    posts = collector.filter(method="POST", status_ok=True)
    walls = [s.wall_ms for s in posts]
    apps = [s.app_ms for s in posts]
    result.call_p50_ms = percentile(walls, 50)
    result.call_p95_ms = percentile(walls, 95)
    result.call_p99_ms = percentile(walls, 99)
    result.app_dur_p50_ms = percentile(apps, 50)
    result.app_dur_p95_ms = percentile(apps, 95)
