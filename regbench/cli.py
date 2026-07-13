"""Entry point wiring the scaffold together.

Today it exposes one subcommand, ``smoke``: stand up a local server, synthesize a small
dataset, register it once, write a CSV row, and print it. That proves the whole loop —
server ⇄ manifests ⇄ runner ⇄ report — end to end. The sweep/suite drivers from
BENCHMARK-PLAN.md (scale, concurrency, white-box, write-path) become sibling subcommands
built on the same four modules.

Run under the TCB venv so ``tiled_catalog_broker`` imports:
    /home/ajshack/tiled-catalog-broker/.venv/bin/python -m regbench.cli smoke
"""

import argparse
from dataclasses import asdict
from pathlib import Path

from .config import RunConfig
from .manifests import build_manifest
from .report import CsvReporter
from .runner import make_client, run_registration
from .server import local_server
from .suites import format_summary, run_scale_sweep, summarize


def _cmd_smoke(args):
    workspace = Path(args.workspace).resolve()
    cfg = RunConfig(
        layout=args.layout,
        n_entities=args.n,
        location="local",
        max_workers=args.workers,
    )
    reporter = CsvReporter(args.out)

    print(f"[smoke] workspace={workspace}")
    print(f"[smoke] config={cfg.label()}")

    # Manifests can be built before the server exists; the data must live under the
    # workspace so the server's --read root covers the asset file:// URIs.
    manifest = build_manifest(cfg, root=workspace / "datasets")
    print(f"[smoke] manifest: {len(manifest.ent_df)} entities, "
          f"{len(manifest.art_df)} artifacts, base_dir={manifest.base_dir}")

    with local_server(workspace=workspace) as server:
        print(f"[smoke] server up at {server.uri} (pid {server.pid})")
        client = make_client(server.uri, server.api_key)
        for rep in range(args.reps):
            result = run_registration(cfg, manifest, client, rep=rep)
            reporter.append(result)
            print(f"[smoke] rep {rep}: {result.entities} ent in "
                  f"{result.wall_s:.2f}s = {result.entities_per_s:.1f} ent/s")

    print(f"[smoke] wrote {args.out}")


def _cmd_scale(args):
    layouts = [x.strip() for x in args.layouts.split(",") if x.strip()]
    sizes = [int(x) for x in args.sizes.split(",") if x.strip()]
    reporter = CsvReporter(args.out)

    print(f"[scale] location={args.location} layouts={layouts} sizes={sizes} workers={args.workers}")
    results = run_scale_sweep(
        layouts=layouts,
        sizes=sizes,
        root=Path(args.workspace).resolve(),
        reporter=reporter,
        location=args.location,
        workers=args.workers,
        reps_override=args.reps,          # None → plan's per-size schedule
        remote_uri=args.remote_uri,
        remote_api_key=args.remote_api_key,
    )
    print(f"\n[scale] wrote {args.out}\n")
    print(format_summary(summarize(results)))


def build_parser():
    p = argparse.ArgumentParser(prog="regbench", description=__doc__)
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("smoke", help="prove the server⇄manifests⇄runner⇄report loop")
    s.add_argument("--workspace", default="./_regbench_ws",
                   help="scratch root for datasets + catalog.db + --read")
    s.add_argument("--out", default="./regbench_smoke.csv", help="CSV output path")
    s.add_argument("--layout", default="per_entity",
                   choices=("per_entity", "batched", "grouped"))
    s.add_argument("--n", type=int, default=20, help="entities to synthesize")
    s.add_argument("--workers", type=int, default=8, help="register_dataset_http max_workers")
    s.add_argument("--reps", type=int, default=1, help="registration repetitions")
    s.set_defaults(func=_cmd_smoke)

    sc = sub.add_parser("scale", help="scale sweep → the scaling curve (Suite 1)")
    sc.add_argument("--workspace", default="./_regbench_ws",
                    help="scratch root; each config gets its own subdir + fresh DB")
    sc.add_argument("--out", default="./regbench_scale.csv", help="CSV output path")
    sc.add_argument("--layouts", default="per_entity,batched,grouped",
                    help="comma-separated layouts")
    sc.add_argument("--sizes", default="10,100,1000",
                    help="comma-separated n_entities points")
    sc.add_argument("--workers", type=int, default=8, help="register_dataset_http max_workers")
    sc.add_argument("--location", default="local", choices=("local", "remote"))
    sc.add_argument("--reps", type=int, default=None,
                    help="override reps (default: plan schedule 5@≤100, 3@≤1k, 1@else)")
    sc.add_argument("--remote-uri", default=None, help="remote Tiled URI (location=remote)")
    sc.add_argument("--remote-api-key", default=None, help="remote API key (location=remote)")
    sc.set_defaults(func=_cmd_scale)
    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
