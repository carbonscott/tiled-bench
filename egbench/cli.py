"""egbench CLI — serve / datasets / run / sweep.

Data creation + registration are one-off agentic steps (recipe in
docs/agent-runbook-egress-benchmark.md), not subcommands. Run under the TCB venv from
the tiled-bench repo root so ``egbench`` and ``regbench`` import:

    PY=/sdf/data/lcls/ds/prj/prjmaiqmag01/results/cfitussi/tiled-catalog-broker/.venv/bin/python
    "$PY" -m egbench.cli serve &          # persistent local server → server.json
    "$PY" -m egbench.cli run --dataset EGRESS_PE_1M --method export_hdf5 --concurrency 4
"""

import argparse
import itertools
import json
import os
from pathlib import Path

from .config import FetchConfig
from .datasets import DATA_ROOT, DEFAULT_REMOTE_URL, REGISTRY
from .report import CsvReporter
from .runner import run_fetch


def _resolve_target(args):
    """(url, api_key) for the chosen location. local reads the serve state file."""
    if args.location == "local":
        state = json.loads((Path(args.workspace) / "server.json").read_text())
        return state["uri"], state["api_key"]
    url = args.url or os.environ.get("TILED_URL", DEFAULT_REMOTE_URL)
    return url, args.api_key or os.environ.get("TILED_API_KEY", "")


def _run_configs(cfgs, reps, args):
    url, api_key = _resolve_target(args)
    reporter = CsvReporter(args.out)
    for cfg in cfgs:
        for rep in range(reps):
            res = run_fetch(cfg, url, api_key, rep)
            reporter.append(res)
            app = f"{100 * res.app_frac:.0f}%" if res.app_frac is not None else "-"
            print(f"[run] {cfg.label()} rep{rep}: wall={res.wall_s:.2f}s  "
                  f"{res.ent_per_s:.1f} ent/s  payload={res.payload_mbps:.1f} MB/s  "
                  f"wire={res.wire_mbps:.1f} MB/s  app={app}  nav={res.nav_sum_s:.2f}s  "
                  f"dec={res.decode_s:.2f}s  err={res.errors}", flush=True)
    print(f"[done] appended to {args.out}")


def _cmd_serve(args):
    from regbench.server import local_server

    ws = Path(args.workspace).resolve()
    with local_server(workspace=ws, read_paths=[DATA_ROOT], fresh_db=args.fresh,
                      port=args.port) as server:
        (ws / "server.json").write_text(json.dumps(
            {"uri": server.uri, "api_key": server.api_key, "pid": server.pid}))
        print(f"[serve] up at {server.uri} (pid {server.pid}) — "
              f"state in {ws / 'server.json'}; serving until killed", flush=True)
        try:
            server.proc.wait()
        except KeyboardInterrupt:
            pass  # Ctrl-C is the normal shutdown; the context manager stops the server


def _cmd_datasets(args):
    for key, s in REGISTRY.items():
        nfiles = len(s.files())
        status = "on disk" if nfiles else "MISSING"
        print(f"{key:30s} {s.layout:10s} n={s.n_entities:<5d} "
              f"{s.mb_per_entity:9.3f} MB/ent  files={nfiles:<5d} {status:8s} {s.data_dir}")


def _cmd_run(args):
    cfg = FetchConfig(dataset_key=args.dataset, method=args.method, location=args.location,
                      concurrency=args.concurrency, batch_size=args.batch,
                      n_entities=args.n)
    _run_configs([cfg], args.reps, args)


def _cmd_sweep(args):
    datasets = [x.strip() for x in args.datasets.split(",") if x.strip()]
    methods = [x.strip() for x in args.methods.split(",") if x.strip()]
    concs = [int(x) for x in args.concurrency.split(",") if x.strip()]
    batches = [int(x) for x in args.batches.split(",") if x.strip()]
    # batch_size only exists on the export path — pin it to one value elsewhere so the
    # grid doesn't repeat identical raw_read/h5py_direct points per batch value.
    cfgs = list(dict.fromkeys(
        FetchConfig(dataset_key=d, method=m, location=args.location, concurrency=c,
                    batch_size=b if m == "export_hdf5" else 1, n_entities=args.n)
        for d, m, c, b in itertools.product(datasets, methods, concs, batches)
    ))
    print(f"[sweep] {len(cfgs)} configs × {args.reps} rep(s) → {args.out}")
    _run_configs(cfgs, args.reps, args)


def _add_target_args(p):
    p.add_argument("--location", default="local", choices=("local", "remote"))
    p.add_argument("--workspace", default="./_egbench_ws",
                   help="local-server state dir (serve writes server.json here)")
    p.add_argument("--url", default=None, help="remote Tiled URI (default: $TILED_URL)")
    p.add_argument("--api-key", default=None, help="remote key (default: $TILED_API_KEY)")
    p.add_argument("--out", default="./results/egress.csv", help="CSV to append rows to")
    p.add_argument("--n", type=int, default=0, help="entities per run (0 = all)")
    p.add_argument("--reps", type=int, default=3)


def build_parser():
    p = argparse.ArgumentParser(prog="egbench", description=__doc__)
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("serve", help="run a persistent local Tiled server (foreground)")
    s.add_argument("--workspace", default="./_egbench_ws")
    s.add_argument("--port", type=int, default=None)
    s.add_argument("--fresh", action="store_true",
                   help="wipe the catalog DB (default: keep registered datasets)")
    s.set_defaults(func=_cmd_serve)

    d = sub.add_parser("datasets", help="list the dataset registry + on-disk status")
    d.set_defaults(func=_cmd_datasets)

    r = sub.add_parser("run", help="measure one config, append one CSV row per rep")
    r.add_argument("--dataset", required=True, choices=sorted(REGISTRY))
    r.add_argument("--method", required=True,
                   choices=("export_hdf5", "raw_read", "h5py_direct"))
    r.add_argument("--concurrency", type=int, default=1)
    r.add_argument("--batch", type=int, default=20, help="keys per export work-unit")
    _add_target_args(r)
    r.set_defaults(func=_cmd_run)

    w = sub.add_parser("sweep", help="grid over datasets × methods × concurrency × batch")
    w.add_argument("--datasets", required=True, help="comma-separated dataset keys")
    w.add_argument("--methods", default="export_hdf5,raw_read")
    w.add_argument("--concurrency", default="1")
    w.add_argument("--batches", default="20")
    _add_target_args(w)
    w.set_defaults(func=_cmd_sweep)
    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
