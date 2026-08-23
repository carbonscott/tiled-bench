#!/bin/bash
#SBATCH --partition=milano
#SBATCH --account=lcls:data
#SBATCH --exclusive
#SBATCH --nodes=1
#SBATCH --time=03:00:00
#SBATCH --job-name=regbench-ba
#SBATCH --output=/sdf/data/lcls/ds/prj/prjmaiqmag01/results/ajshack/tiled-bench/_bench_ws/job/slurm-%j.log

# Registration (ingress) before/after campaign — local SQLite, exclusive node.
# before = TCB main checkout (editable install), after = optimize/http-register-perf
# worktree via PYTHONPATH shadowing. Protocol per docs/agent-runbook-http-register-benchmark.md.
set -u
cd /sdf/data/lcls/ds/prj/prjmaiqmag01/results/ajshack/tiled-bench
PY=.venv/bin/python
WT=/sdf/data/lcls/ds/prj/prjmaiqmag01/results/ajshack/tiled-catalog-broker/.worktrees/optimize-http-register-perf
mkdir -p results/ingress

echo "node: $(hostname)  cores: $(nproc)  start: $(date -Is)"
echo "== prewarm imports =="
time $PY -c "import tiled.server.app, tiled.catalog, tiled_catalog_broker, regbench.cli"

BEFORE_SHA=$(git -C ../tiled-catalog-broker rev-parse --short HEAD)
AFTER_SHA=$(git -C "$WT" rev-parse --short HEAD)
echo "before sha: $BEFORE_SHA   after sha: $AFTER_SHA"

echo "== BEFORE sweep ($BEFORE_SHA) =="
rm -rf _bench_ws/before results/ingress/before.csv
$PY -m regbench.cli scale --workspace _bench_ws/before --out results/ingress/before.csv \
    --layouts per_entity,batched --sizes 10,100,1000 --workers 8 --location local
$PY _bench_ws/job/verify_csv.py results/ingress/before.csv

echo "== AFTER sweep ($AFTER_SHA) =="
rm -rf _bench_ws/after results/ingress/after.csv
PYTHONPATH=$WT/src $PY -m regbench.cli scale --workspace _bench_ws/after --out results/ingress/after.csv \
    --layouts per_entity,batched --sizes 10,100,1000 --workers 8 --location local
PYTHONPATH=$WT/src $PY _bench_ws/job/verify_csv.py results/ingress/after.csv

echo "== read-back check (after code) =="
rm -rf _bench_ws/readback
# $PWD on PYTHONPATH: running a script by path puts the script's dir (not cwd)
# at sys.path[0], and regbench lives in the repo root.
PYTHONPATH=$WT/src:$PWD $PY _bench_ws/job/readback_check.py _bench_ws/readback

echo "end: $(date -Is)"
