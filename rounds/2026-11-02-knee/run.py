"""Where does the catalog path saturate, and what holds a request while it waits?

The protocol in .claude/skills/bench-knee/SKILL.md, written down. Fixed processes, a
fine thread ladder, both the client read path and the raw catalog GET, Tiled's pool
gauges around every rung, and pg_stat_activity sampled underneath if psql is reachable.

    source ../tiled-catalog-broker/.env.test
    .venv/bin/python rounds/2026-11-02-knee/run.py                   # the full ladder
    .venv/bin/python rounds/2026-11-02-knee/run.py --procs 1 --threads 2 --n 20   # smoke
    PSQL="kubectl exec -i cnpg-1 -- psql -U tiled-test tiled-test" ... # adds pg.jsonl
"""

import argparse
import os
import shlex
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tiled.client import from_uri  # noqa: E402

from tbench import load, probes, reads  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("--dataset", default="EGRESS_PE_64K_BROKER")
ap.add_argument("--artifact", default="signal")
ap.add_argument("--n", type=int, default=1000)
ap.add_argument("--procs", type=int, default=8)
ap.add_argument("--threads", type=int, nargs="+", default=[1, 2, 3, 4, 6, 8, 12, 16])
ap.add_argument("--reps", type=int, default=2)
ap.add_argument("--max-err", type=float, default=0.05, help="stop the ladder past this unit error rate")
args = ap.parse_args()

URL, KEY = os.environ["TILED_URL"], os.environ["TILED_API_KEY"]


def client():
    return from_uri(URL, api_key=KEY)[args.dataset]


keys = list(client().keys())[: args.n]
WORK = {
    "arread": lambda node, k: reads.artifact_read(node, k, args.artifact),
    "metaget": lambda node, k: reads.metadata_get(node, k),
}
psql = shlex.split(os.environ["PSQL"]) if os.environ.get("PSQL") else None

with load.Round("knee", url=URL, api_key=KEY,
                extra={"dataset": args.dataset, "n": len(keys), "procs": args.procs}) as rd:
    print("healthz:", probes.healthz(URL, n=5), flush=True)
    if psql:
        (rd.dir / "pg_connections_before.txt").write_text(
            "\n".join("\t".join(r) for r in probes.pg_connections(psql)))
    sampler = probes.PgSampler(psql, rd.dir / "pg.jsonl") if psql else None
    if sampler:
        sampler.__enter__()
    try:
        for name, work in WORK.items():
            for threads in args.threads:
                for rep in range(1, args.reps + 1):
                    before = probes.tiled_metrics(URL, KEY)
                    run = load.run(work, keys, client, threads=threads, procs=args.procs,
                                   label=f"{name}-t{threads:02d}-r{rep}")
                    run.meta["metrics_delta"] = probes.metrics_delta(before, probes.tiled_metrics(URL, KEY))
                    rd.save(run)
                    s = run.summary()
                    if s["units_err"] / s["units"] > args.max_err:
                        print(f"[stop] {name}: {s['units_err']}/{s['units']} failed at t={threads}", flush=True)
                        break
                else:
                    continue
                break
    finally:
        if sampler:
            sampler.__exit__(None, None, None)
        if psql:
            (rd.dir / "pg_connections_after.txt").write_text(
                "\n".join("\t".join(r) for r in probes.pg_connections(psql)))
