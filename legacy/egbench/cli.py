"""egbench CLI — args in, measured JSON out.

One invocation = one measurement. Everything the harness can't measure (layout, stack
size, mimetype, entity count) is something you read off the Tiled server and pass as a
flag or record yourself; the harness never guesses. Reps and sweeps are a shell loop.

    PY=/sdf/data/lcls/ds/prj/prjmaiqmag01/results/cfitussi/tiled-catalog-broker/.venv/bin/python
    "$PY" -m egbench.cli serve --read-path /sdf/.../data-source &
    "$PY" -m egbench.cli run --dataset EGRESS_BAT_1M --artifact signal \\
          --method artifact_read --concurrency 4 --n 200
    {"wall_s": 12.41, "entities": 200, ...}
"""

import argparse
import json
import os
from pathlib import Path

from .methods import GROUP_FMT
from .runner import run_direct, run_http

DEFAULT_REMOTE_URL = "https://lcls-data-portal.slac.stanford.edu/tiled-test"


def _target(args):
    """(url, api_key) for the chosen location. local reads the serve state file."""
    if args.location == "local":
        state = json.loads((Path(args.workspace) / "server.json").read_text())
        return state["uri"], state["api_key"]
    return (args.url or os.environ.get("TILED_URL", DEFAULT_REMOTE_URL),
            args.api_key or os.environ.get("TILED_API_KEY", ""))


def _cmd_serve(args):
    from regbench.server import local_server

    ws = Path(args.workspace).resolve()
    with local_server(workspace=ws, read_paths=[Path(p) for p in args.read_path],
                      fresh_db=args.fresh, port=args.port) as server:
        (ws / "server.json").write_text(json.dumps(
            {"uri": server.uri, "api_key": server.api_key, "pid": server.pid}))
        print(f"[serve] up at {server.uri} (pid {server.pid}) — "
              f"state in {ws / 'server.json'}; serving until killed", flush=True)
        try:
            server.proc.wait()
        except KeyboardInterrupt:
            pass  # Ctrl-C is the normal shutdown; the context manager stops the server


def _cmd_run(args):
    if args.method == "h5py_direct":
        measured = run_direct(args)
    else:
        url, api_key = _target(args)
        measured = run_http(args, url, api_key)
    print(json.dumps(measured))


def build_parser():
    p = argparse.ArgumentParser(prog="egbench", description=__doc__)
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("serve", help="run a persistent local Tiled server (foreground)")
    s.add_argument("--workspace", default="./_egbench_ws")
    s.add_argument("--read-path", action="append", default=[],
                   help="extra --read root the server may load HDF5 from (repeatable)")
    s.add_argument("--port", type=int, default=None)
    s.add_argument("--fresh", action="store_true",
                   help="wipe the catalog DB (default: keep registered datasets)")
    s.set_defaults(func=_cmd_serve)

    r = sub.add_parser("run", help="measure one config, print measured JSON")
    r.add_argument("--method", required=True,
                   choices=("container_export", "artifact_read", "raw_export",
                            "asset_bytes", "h5py_direct"))
    r.add_argument("--concurrency", type=int, default=1)
    r.add_argument("--n", type=int, default=0,
                   help="entities to fetch; 0 = all (required for batched/grouped direct)")

    r.add_argument("--dataset", help="top-level Tiled container key (HTTP methods)")
    r.add_argument("--artifact",
                   help="artifact node name, e.g. signal. Required for artifact_read, "
                        "raw_export and asset_bytes. For container_export it is optional: "
                        "omit to decode every artifact of each entity ('I want the whole "
                        "entity'), name one to decode only that ('the server sent four, I "
                        "needed one') — the wire cost is the same either way")
    r.add_argument("--export-batch", type=int, default=20,
                   help="entity keys per export work-unit; container_export only. "
                        "Requests are chunked <=25 keys regardless (proxy 414 limit)")
    r.add_argument("--download-dir",
                   help="raw_export only: write the asset here instead of holding it in "
                        "RAM. Needed for files too big to buffer (EGRESS_BAT_16M is 4.3 GB)")

    r.add_argument("--location", default="local", choices=("local", "remote"))
    r.add_argument("--workspace", default="./_egbench_ws",
                   help="local-server state dir (serve writes server.json here)")
    r.add_argument("--url", default=None, help="remote Tiled URI (default: $TILED_URL)")
    r.add_argument("--api-key", default=None, help="remote key (default: $TILED_API_KEY)")

    r.add_argument("--data-dir", help="dir of *.h5 files (h5py_direct)")
    r.add_argument("--h5-path", help="HDF5 dataset path (h5py_direct, raw_export)")
    r.add_argument("--layout", choices=("per_entity", "batched", "grouped"),
                   help="how entities map onto files (h5py_direct, raw_export). For "
                        "raw_export this sets the request count: 1 for a shared file, "
                        "N for per_entity")
    r.add_argument("--group-fmt", default=GROUP_FMT,
                   help="entity group path template (grouped layout)")
    r.set_defaults(func=_cmd_run)
    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
