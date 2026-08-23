"""Run one egbench measurement and append a fully-composed CSV row (runbook §5).

The harness prints only what it measured; this wrapper owns the axes and the
dataset provenance registry. One invocation = one row.

Usage:
  runrow.py --out results/broker/step1-layers.csv --dataset EGRESS_PE_1M_BROKER \
            --method artifact_read --n 200 --rep 1 --location local [--artifact signal]
"""
import argparse
import csv
import json
import os
import subprocess
import sys
from pathlib import Path

_SP = Path(__file__).resolve().parent
PY = "/sdf/data/lcls/ds/prj/prjmaiqmag01/results/ajshack/tiled-bench/.venv/bin/python"
BENCH = "/sdf/data/lcls/ds/prj/prjmaiqmag01/results/ajshack/tiled-bench"
EB_ROOT = "/sdf/data/lcls/ds/prj/prjmaiqmag01/results/data-source/egress_bench"
CLIENT_TILED_VERSION = "0.2.13"
SERVER_VERSION = {"local": "0.2.9",
                  "remote": os.environ.get("EGRESS_REMOTE_SERVER_VERSION",
                                           "unknown")}

# key -> layout, n_entities, artifacts {name: mb}, mimetype, data subdir (h5py_direct)
REG = {}
def _add(key, layout, n, arts, mime, subdir, h5_prefix=""):
    REG[key] = {"layout": layout, "n_entities": n, "artifacts": arts,
                "mimetype": mime, "subdir": subdir, "h5_prefix": h5_prefix}

_STOCK = "application/x-hdf5"
_BROKER = "application/x-hdf5-broker"
for base, layout, n, arts, subdir in [
    ("EGRESS_PE_64K", "per_entity", 1000, {"signal": 0.065536}, "egress_pe_64k"),
    ("EGRESS_PE_1M", "per_entity", 1000, {"signal": 1.048576}, "egress_pe_1m"),
    ("EGRESS_PE_16M", "per_entity", 256, {"signal": 16.777216}, "egress_pe_16m"),
    ("EGRESS_BAT_1M", "batched", 1000, {"signal": 1.048576}, "egress_bat_1m"),
    ("EGRESS_BAT_16M", "batched", 256, {"signal": 16.777216}, "egress_bat_16m"),
    ("EGRESS_GRP_1M", "grouped", 1000, {"signal": 1.048576}, "egress_grp_1m"),
]:
    _add(base, layout, n, arts, _STOCK, subdir)
    _add(base + "_BROKER", layout, n, arts, _BROKER, subdir)

for key, layout, n, arts, subdir in [
    ("EGRESS_PE_4X256K", "per_entity", 1000,
     {f"art_{a:02d}": 0.262144 for a in range(4)}, "egress_pe_4x256k"),
    ("EGRESS_PE_16X64K", "per_entity", 1000,
     {f"art_{a:02d}": 0.065536 for a in range(16)}, "egress_pe_16x64k"),
    ("EGRESS_BAT_4X256K", "batched", 1000,
     {f"art_{a:02d}": 0.262144 for a in range(4)}, "egress_bat_4x256k"),
    ("EGRESS_GRP_4X256K", "grouped", 1000,
     {f"art_{a:02d}": 0.262144 for a in range(4)}, "egress_grp_4x256k"),
    ("EGRESS_PE_MIXED", "per_entity", 256,
     {"primary": 16.777216, "aux_0": 0.065536, "aux_1": 0.065536,
      "aux_2": 0.065536}, "egress_pe_mixed"),
    ("EGRESS_BAT_MIXED", "batched", 256,
     {"primary": 16.777216, "aux_0": 0.065536, "aux_1": 0.065536,
      "aux_2": 0.065536}, "egress_bat_mixed"),
]:
    _add(key, layout, n, arts, _BROKER, subdir)

JSON_COLS = ["wall_s", "entities", "expected_entities", "errors", "payload_mb",
             "wire_mb", "req_p50_ms", "req_p95_ms", "req_p99_ms", "app_p50_ms",
             "app_sum_s", "net_sum_s", "nav_sum_s", "decode_s"]
FILL_COLS = ["dataset_key", "method", "concurrency", "export_batch", "n_entities",
             "rep", "artifact", "location", "layout", "n_artifacts",
             "mb_per_artifact", "mb_per_entity", "stack_size", "mimetype",
             "tiled_version", "server_version", "url", "note"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--method", required=True)
    ap.add_argument("--concurrency", type=int, default=1)
    ap.add_argument("--n", type=int, default=0)
    ap.add_argument("--rep", type=int, required=True)
    ap.add_argument("--artifact", default=None)
    ap.add_argument("--export-batch", type=int, default=20)
    ap.add_argument("--download-dir", default=None)
    ap.add_argument("--location", default="local")
    ap.add_argument("--note", default="")
    args = ap.parse_args()

    reg = REG[args.dataset]
    layout = reg["layout"]

    if args.method == "raw_export":
        # upstream: tiled download() installs a SIGINT handler (illegal off the
        # main thread), so egbench's pool cannot drive raw_export — use the
        # main-thread one-off with the same timed region. Concurrency is 1.
        assert args.concurrency == 1, "raw_export scales by process, not threads"
        cmd = [PY, str(_SP / "raw_export_main.py"), "--dataset", args.dataset,
               "--artifact", args.artifact, "--layout", layout,
               "--location", args.location]
        if args.n:
            cmd += ["--n", str(args.n)]
        if args.download_dir:
            cmd += ["--download-dir", args.download_dir]
        else:
            cmd += ["--ram"]
        return finish(args, reg, layout, cmd)

    cmd = [PY, "-m", "egbench.cli", "run", "--method", args.method,
           "--concurrency", str(args.concurrency), "--location", args.location]
    if args.n:
        cmd += ["--n", str(args.n)]
    if args.method == "h5py_direct":
        data_dir = f"{EB_ROOT}/{reg['subdir']}/data"
        art = args.artifact or next(iter(reg["artifacts"]))
        h5_path = art if layout == "grouped" else f"/{art}"
        cmd += ["--data-dir", data_dir, "--h5-path", h5_path, "--layout", layout]
        if layout != "per_entity" and not args.n:
            cmd += ["--n", str(reg["n_entities"])]
    else:
        cmd += ["--dataset", args.dataset]
        if args.artifact:
            cmd += ["--artifact", args.artifact]
        if args.method == "container_export":
            cmd += ["--export-batch", str(args.export_batch)]
        if args.method == "asset_bytes":
            cmd += ["--layout", layout]
    return finish(args, reg, layout, cmd)


def finish(args, reg, layout, cmd):
    r = subprocess.run(cmd, cwd=BENCH, capture_output=True, text=True)
    stdout = r.stdout.strip().splitlines()
    warns = [l for l in stdout if not l.startswith("{")] + \
            [l for l in r.stderr.strip().splitlines() if l][-3:]
    if r.returncode != 0 or not stdout or not stdout[-1].startswith("{"):
        print(f"[runrow FAIL] rc={r.returncode} cmd={' '.join(cmd)}")
        print(r.stdout[-2000:])
        print(r.stderr[-2000:])
        sys.exit(1)
    measured = json.loads(stdout[-1])
    for w in warns:
        if "[unit error]" in w or "[warn]" in w:
            print(f"[runrow warn] {w}")

    if args.location == "local":
        state = json.loads((Path(BENCH) / "_egbench_ws/server.json").read_text())
        url = state["uri"]
    else:
        url = os.environ.get("TILED_URL",
                             "https://lcls-data-portal.slac.stanford.edu/tiled-test")

    arts = reg["artifacts"]
    uniform = len(set(arts.values())) == 1
    if args.artifact:
        mb_per_artifact = arts[args.artifact]
    else:
        mb_per_artifact = next(iter(arts.values())) if uniform else ""
    row = {
        "dataset_key": args.dataset,
        "method": args.method,
        "concurrency": args.concurrency,
        "export_batch": args.export_batch if args.method == "container_export" else "",
        "n_entities": args.n or reg["n_entities"],
        "rep": args.rep,
        "artifact": args.artifact or "ALL",
        "location": args.location,
        "layout": layout,
        "n_artifacts": len(arts),
        "mb_per_artifact": mb_per_artifact,
        "mb_per_entity": round(sum(arts.values()), 6),
        "stack_size": reg["n_entities"] if layout == "batched" else "",
        "mimetype": reg["mimetype"] if args.method != "h5py_direct" else "",
        "tiled_version": CLIENT_TILED_VERSION,
        "server_version": SERVER_VERSION[args.location],
        "url": url if args.method != "h5py_direct" else "",
        "note": args.note,
    }
    for c in JSON_COLS:
        row[c] = measured.get(c)

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    new = not out.exists()
    with open(out, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FILL_COLS + JSON_COLS)
        if new:
            w.writeheader()
        w.writerow(row)

    # one-line summary for the driving shell
    wall = measured["wall_s"]
    print(json.dumps({"dataset": args.dataset, "method": args.method,
                      "c": args.concurrency, "n": args.n, "rep": args.rep,
                      "artifact": args.artifact or "ALL",
                      "wall_s": round(wall, 3),
                      "entities": measured["entities"],
                      "expected": measured["expected_entities"],
                      "errors": measured["errors"],
                      "payload_mb": round(measured["payload_mb"], 3),
                      "wire_mb": round(measured["wire_mb"], 3)}))


if __name__ == "__main__":
    main()
