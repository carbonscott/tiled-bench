"""Milano --exclusive job driver: client-node control re-runs + server ceiling probe.

Phases (all remote/tiled-test):
  A. Control — re-run reference cells measured earlier from an interactive node,
     to test whether those numbers were client-node-limited.
  B. Bandwidth ceiling — asset_bytes process ladder: PE_16M shards (distinct
     files) and BAT_1M whole-file (same file, server-cache-hot).
  C. Request-rate ceiling — artifact_read on 64K entities, processes x threads,
     disjoint shards.
  D. py-spy profiles of the single-process plateau (GIL attribution).

Escalation stops early if a rung shows >5% unit errors. One CSV row per rung
(wall=max over procs, sums summed) with note '<P>-processes-aggregate ...' so
the heatmap build puts it on the process axis.
"""
import csv
import json
import subprocess
import sys
from pathlib import Path

import os

BENCH = Path("/sdf/data/lcls/ds/prj/prjmaiqmag01/results/ajshack/tiled-bench")
PY = str(BENCH / ".venv/bin/python")
HERE = Path(__file__).resolve().parent
# Server deployment config is a population axis: workers were 8 for the
# 2026-07-28 step-7 run, 16 from 2026-08-03. Separate files + note tag.
TAG = os.environ.get("CEILING_TAG", "w16pool250")
CONTROL_CSV = BENCH / f"results/broker/step7-control-{TAG}.csv"
CEILING_CSV = BENCH / f"results/broker/step7-ceiling-{TAG}.csv"
PROFILES = BENCH / "results/profiles"
PROFILES.mkdir(exist_ok=True)

# Stamp rows with the server actually running (hardcoding burned two runs).
import httpx as _hx
SERVER_VERSION = _hx.get(os.environ["TILED_URL"] + "/api/v1/",
                         headers={"Authorization": "Apikey " + os.environ["TILED_API_KEY"]},
                         timeout=30).json()["library_version"]
os.environ["EGRESS_REMOTE_SERVER_VERSION"] = SERVER_VERSION
print(f"[ceiling] remote server: {SERVER_VERSION}", flush=True)

FILL_COLS = ["dataset_key", "method", "concurrency", "export_batch", "n_entities",
             "rep", "artifact", "location", "layout", "n_artifacts",
             "mb_per_artifact", "mb_per_entity", "stack_size", "mimetype",
             "tiled_version", "server_version", "url", "note"]
JSON_COLS = ["wall_s", "entities", "expected_entities", "errors", "payload_mb",
             "wire_mb", "req_p50_ms", "req_p95_ms", "req_p99_ms", "app_p50_ms",
             "app_sum_s", "net_sum_s", "nav_sum_s", "decode_s"]

DS = {
    "EGRESS_PE_16M_BROKER": dict(layout="per_entity", mb=16.777216, n_all=256,
                                 art="signal", stack=""),
    "EGRESS_BAT_1M_BROKER": dict(layout="batched", mb=1.048576, n_all=1000,
                                 art="signal", stack=1000),
    "EGRESS_PE_64K_BROKER": dict(layout="per_entity", mb=0.065536, n_all=1000,
                                 art="signal", stack=""),
}


def log(*a):
    print(*a, flush=True)


def write_row(csv_path, ds, method, conc, n, rep, measured, note, nproc=1):
    note = f"{note} {TAG}".strip()
    d = DS[ds]
    row = {"dataset_key": ds, "method": method, "concurrency": conc,
           "export_batch": "", "n_entities": n, "rep": rep,
           "artifact": d["art"], "location": "remote", "layout": d["layout"],
           "n_artifacts": 1, "mb_per_artifact": d["mb"], "mb_per_entity": d["mb"],
           "stack_size": d["stack"], "mimetype": "application/x-hdf5-broker",
           "tiled_version": "0.2.13",
           "server_version": SERVER_VERSION,
           "url": "https://lcls-data-portal.slac.stanford.edu/tiled-test",
           "note": note}
    row.update({c: measured.get(c) for c in JSON_COLS})
    new = not csv_path.exists()
    with open(csv_path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FILL_COLS + JSON_COLS)
        if new:
            w.writeheader()
        w.writerow(row)
    log(f"[row] {ds} {method} c={conc} nproc={nproc} n={n} rep={rep} "
        f"wall={measured['wall_s']:.2f}s wire={measured['wire_mb']:.0f}MB "
        f"errors={measured['errors']}")


def run_json(cmd):
    r = subprocess.run(cmd, cwd=BENCH, capture_output=True, text=True)
    lines = [l for l in r.stdout.strip().splitlines() if l.startswith("{")]
    if r.returncode != 0 or not lines:
        log("[FAILED]", " ".join(map(str, cmd)), r.stderr[-400:])
        return None
    return json.loads(lines[-1])


def runrow(*extra):
    cmd = [PY, str(HERE / "runrow.py"), *map(str, extra)]
    r = subprocess.run(cmd, cwd=BENCH, capture_output=True, text=True)
    log(r.stdout.strip().splitlines()[-1] if r.stdout.strip() else "[no output]")
    if r.returncode != 0:
        log("[FAILED]", " ".join(map(str, cmd)), r.stderr[-300:])


def fanout(builder, P):
    """Launch P subprocesses, aggregate: wall=max, sums=sum. builder(i)->cmd."""
    procs = [subprocess.Popen(builder(i), cwd=BENCH, stdout=subprocess.PIPE,
                              text=True) for i in range(P)]
    outs = []
    for p in procs:
        out, _ = p.communicate()
        lines = [l for l in out.strip().splitlines() if l.startswith("{")]
        outs.append(json.loads(lines[-1]) if p.returncode == 0 and lines else None)
    good = [o for o in outs if o]
    if len(good) < P:
        log(f"[warn] {P - len(good)}/{P} procs died")
    if not good:
        return None
    agg = {"wall_s": max(o["wall_s"] for o in good),
           "entities": sum(o["entities"] for o in good),
           "expected_entities": sum(o["expected_entities"] for o in good),
           "errors": sum(o["errors"] for o in good) + (P - len(good)) * 1000,
           "payload_mb": sum(o["payload_mb"] for o in good),
           "wire_mb": sum(o["wire_mb"] for o in good),
           "req_p50_ms": sorted(o["req_p50_ms"] for o in good)[len(good) // 2],
           "req_p95_ms": max(o["req_p95_ms"] for o in good),
           "req_p99_ms": max(o["req_p99_ms"] for o in good),
           "app_p50_ms": sorted((o["app_p50_ms"] or 0) for o in good)[len(good) // 2],
           "app_sum_s": sum(o["app_sum_s"] for o in good),
           "net_sum_s": sum(o["net_sum_s"] for o in good),
           "nav_sum_s": sum(o["nav_sum_s"] for o in good),
           "decode_s": sum(o["decode_s"] for o in good)}
    return agg


# ---------------- Phase A: control re-runs ----------------
log("=== Phase A: control (milano-exclusive vs interactive-node numbers)")
for rep in (1, 2, 3):
    for m in ("container_export", "artifact_read", "asset_bytes"):
        runrow("--out", CONTROL_CSV, "--location", "remote",
               "--dataset", "EGRESS_PE_1M_BROKER", "--method", m,
               "--artifact", "signal", "--n", 200, "--rep", rep,
               "--note", f"milano-exclusive-control {TAG}")
    for c in (1, 8, 16):
        runrow("--out", CONTROL_CSV, "--location", "remote",
               "--dataset", "EGRESS_PE_16M_BROKER", "--method",
               "container_export", "--artifact", "signal", "--n", 32,
               "--concurrency", c, "--export-batch", 1, "--rep", rep,
               "--note", f"milano-exclusive-control {TAG}")
        runrow("--out", CONTROL_CSV, "--location", "remote",
               "--dataset", "EGRESS_PE_16M_BROKER", "--method",
               "artifact_read", "--artifact", "signal", "--n", 32,
               "--concurrency", c, "--rep", rep,
               "--note", f"milano-exclusive-control {TAG}")

# step4-style 8-proc control
def ab_shard(ds, n, off):
    return [PY, str(HERE / "raw_export_main.py"), "--dataset", ds,
            "--artifact", "signal", "--layout", "per_entity", "--n", str(n),
            "--offset", str(off), "--location", "remote",
            "--method", "asset_bytes"]

for rep in (1, 2, 3):
    agg = fanout(lambda i: ab_shard("EGRESS_PE_16M_BROKER", 6, i * 6), 8)
    if agg:
        write_row(CONTROL_CSV, "EGRESS_PE_16M_BROKER", "asset_bytes", 1, 48,
                  rep, agg, "8-processes-aggregate milano-exclusive-control",
                  nproc=8)

# ---------------- Phase B: bandwidth ceiling ----------------
log("=== Phase B: bandwidth ladder (asset_bytes processes)")
prev_rate = 0
for P in (8, 16, 32, 64, 128):
    n_each = 256 // P
    stop = False
    for rep in (1, 2):
        agg = fanout(lambda i: ab_shard("EGRESS_PE_16M_BROKER", n_each,
                                        i * n_each), P)
        if agg is None:
            stop = True
            break
        write_row(CEILING_CSV, "EGRESS_PE_16M_BROKER", "asset_bytes", 1,
                  P * n_each, rep, agg,
                  f"{P}-processes-aggregate bandwidth-ladder", nproc=P)
        rate = agg["wire_mb"] / agg["wall_s"]
        log(f"  P={P}: {rate:.0f} MB/s aggregate")
        if agg["errors"] > 0.05 * agg["expected_entities"]:
            log(f"[abort] error rate too high at P={P}")
            stop = True
            break
    if stop:
        break

def bat_stream(_i):
    return [PY, str(HERE / "raw_export_main.py"), "--dataset",
            "EGRESS_BAT_1M_BROKER", "--artifact", "signal", "--layout",
            "batched", "--location", "remote", "--method", "asset_bytes"]

log("=== Phase B2: whole-file streams (same 1.05 GB file x P)")
for P in (4, 8, 16, 32):
    for rep in (1, 2):
        agg = fanout(bat_stream, P)
        if agg is None:
            break
        write_row(CEILING_CSV, "EGRESS_BAT_1M_BROKER", "asset_bytes", 1, 1000,
                  rep, agg, f"{P}-processes-aggregate whole-file-streams",
                  nproc=P)
        log(f"  P={P}: {agg['wire_mb'] / agg['wall_s']:.0f} MB/s aggregate")

# ---------------- Phase C: request-rate ceiling ----------------
log("=== Phase C: request-rate ladder (artifact_read 64K, P procs x 8 threads)")
for P in (4, 8, 16, 32, 64):
    n_each = 1000 // P
    for rep in (1, 2):
        agg = fanout(lambda i: [PY, str(HERE / "arread_shard.py"), "--dataset",
                                "EGRESS_PE_64K_BROKER", "--artifact", "signal",
                                "--n", str(n_each), "--offset", str(i * n_each),
                                "--concurrency", "8"], P)
        if agg is None:
            break
        write_row(CEILING_CSV, "EGRESS_PE_64K_BROKER", "artifact_read", 8,
                  P * n_each, rep, agg,
                  f"{P}-processes-aggregate request-rate-ladder", nproc=P)
        eps = agg["entities"] / agg["wall_s"]
        log(f"  P={P}: {eps:.0f} ent/s ~ {3 * eps:.0f} req/s")
        if agg["errors"] > 0.05 * agg["expected_entities"]:
            log(f"[abort] error rate too high at P={P}")
            break

# ---------------- Phase D: py-spy profiles ----------------
if os.environ.get("CEILING_PROFILES") != "1":
    log("=== Phase D skipped (py-spy hang; workers8 profiles remain the reference)")
    log("CEILING JOB DONE")
    sys.exit(0)
log("=== Phase D: py-spy profiles (single-process plateau attribution)")
PROF = [
    ("arread_c16_pe16m", ["-m", "egbench.cli", "run", "--method",
     "artifact_read", "--dataset", "EGRESS_PE_16M_BROKER", "--artifact",
     "signal", "--n", "64", "--concurrency", "16", "--location", "remote"]),
    ("arread_c16_pe16m_gil", None),  # same run, --gil sampling
    ("export_c8_pe16m", ["-m", "egbench.cli", "run", "--method",
     "container_export", "--dataset", "EGRESS_PE_16M_BROKER", "--artifact",
     "signal", "--n", "64", "--concurrency", "8", "--export-batch", "1",
     "--location", "remote"]),
    ("assetbytes_c1_pe16m", ["-m", "egbench.cli", "run", "--method",
     "asset_bytes", "--dataset", "EGRESS_PE_16M_BROKER", "--artifact",
     "signal", "--layout", "per_entity", "--n", "32", "--location", "remote"]),
]
pyspy = str(BENCH / ".venv/bin/py-spy")
for name, args in PROF:
    gil = name.endswith("_gil")
    base = PROF[0][1] if gil else args
    cmd = [pyspy, "record", "--rate", "200", "--subprocesses",
           "-o", str(PROFILES / f"{name}_{TAG}.svg")] \
        + (["--gil"] if gil else []) + ["--", PY] + base
    r = subprocess.run(cmd, cwd=BENCH, capture_output=True, text=True)
    log(f"[profile] {name}: rc={r.returncode} "
        f"{(r.stderr or '').strip().splitlines()[-1] if r.stderr else ''}")

log("CEILING JOB DONE")
