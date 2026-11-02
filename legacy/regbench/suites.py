"""Scale sweep — the scaling-curve driver (Suite 1 of BENCHMARK-PLAN.md).

Sweeps ``layouts × sizes``, one fresh-DB server launch per config (local) so a big run never
pollutes a small one, N reps each, into one CSV. Two curves fall out of the result columns:

    x = n_entities,  y = wall_s          →  "time to register N", per layout
    x = n_entities,  y = entities_per_s  →  does per-node cost stay flat, or degrade as the
                                            catalog fills? (closure-table + GIN index growth)

Doubles as the regression signal: keep the CSVs and diff ``app_dur_p50_ms`` at a fixed config
across Tiled/TCB versions — no committed baseline or timing gate to maintain.
"""

import statistics
from collections import defaultdict
from contextlib import contextmanager
from pathlib import Path

from .config import RunConfig
from .manifests import build_manifest
from .runner import make_client, run_registration
from .server import local_server


def default_reps(n: int) -> int:
    """Plan's rep schedule: cheap points averaged more, expensive points run once."""
    if n <= 100:
        return 5
    if n <= 1000:
        return 3
    return 1


@contextmanager
def _client_for(location, workspace, remote_uri, remote_api_key):
    """Yield a client for one config. local → fresh server (clean DB); remote → connect.

    Fresh-per-config on local is the whole point: each size measures against an empty catalog,
    so a 10k run can't inflate the 10-run that follows. Remote pollution is acceptable (the
    fresh timestamped dataset_key keeps runs from colliding).
    """
    if location == "remote":
        if not remote_uri:
            raise ValueError("location='remote' requires remote_uri")
        yield make_client(remote_uri, remote_api_key or "")
    else:
        with local_server(workspace=workspace) as server:
            yield make_client(server.uri, server.api_key)


def run_scale_sweep(*, layouts, sizes, root, reporter, location="local", workers=8,
                    reps_fn=default_reps, reps_override=None,
                    remote_uri=None, remote_api_key=None, log=print):
    """Run the full ``layouts × sizes`` sweep, append every row to ``reporter``.

    Returns the flat list of RunResults (also useful for :func:`summarize`).
    """
    results = []
    for layout in layouts:
        for n in sizes:
            cfg = RunConfig(layout=layout, n_entities=n, location=location, max_workers=workers)
            ws = Path(root) / cfg.label()
            manifest = build_manifest(cfg, root=ws / "datasets")
            reps = reps_override if reps_override is not None else reps_fn(n)
            log(f"[scale] {cfg.label()}: {reps} rep(s), "
                f"{len(manifest.ent_df)} ent / {len(manifest.art_df)} art")
            with _client_for(location, ws, remote_uri, remote_api_key) as client:
                for rep in range(reps):
                    res = run_registration(cfg, manifest, client, rep)
                    reporter.append(res)
                    results.append(res)
                    app = f"{res.app_dur_p50_ms:.0f}ms" if res.app_dur_p50_ms else "-"
                    log(f"[scale]   {layout} n={n} rep {rep}: {res.wall_s:6.2f}s  "
                        f"{res.entities_per_s:6.1f} ent/s  {res.nodes_per_s:6.1f} nodes/s  "
                        f"app;dur p50={app}")
    return results


def _median(vals):
    clean = [v for v in vals if v is not None]
    return statistics.median(clean) if clean else None


def summarize(results):
    """Collapse reps to one row per (layout, n): the scaling-curve points (medians)."""
    groups = defaultdict(list)
    for r in results:
        groups[(r.layout, r.n_entities)].append(r)
    rows = []
    for (layout, n), rs in sorted(groups.items()):
        rows.append({
            "layout": layout,
            "n": n,
            "reps": len(rs),
            "wall_s": _median([r.wall_s for r in rs]),
            "ent_per_s": _median([r.entities_per_s for r in rs]),
            "nodes_per_s": _median([r.nodes_per_s for r in rs]),
            "app_dur_p50_ms": _median([r.app_dur_p50_ms for r in rs]),
        })
    return rows


def format_summary(rows):
    """Fixed-width scaling table for the console."""
    head = f"{'layout':<11}{'N':>7}{'reps':>5}{'wall_s':>9}{'ent/s':>8}{'nodes/s':>9}{'app;dur_p50':>13}"
    lines = [head, "-" * len(head)]
    for r in rows:
        app = f"{r['app_dur_p50_ms']:.1f}" if r["app_dur_p50_ms"] is not None else "-"
        lines.append(
            f"{r['layout']:<11}{r['n']:>7}{r['reps']:>5}{r['wall_s']:>9.2f}"
            f"{r['ent_per_s']:>8.1f}{r['nodes_per_s']:>9.1f}{app:>13}"
        )
    return "\n".join(lines)
