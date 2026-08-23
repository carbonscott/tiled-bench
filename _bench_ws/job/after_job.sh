#!/bin/bash
#SBATCH --partition=milano
#SBATCH --account=lcls:data
#SBATCH --exclusive
#SBATCH --nodes=1
#SBATCH --time=02:00:00
#SBATCH --job-name=regbench-after
#SBATCH --output=/sdf/data/lcls/ds/prj/prjmaiqmag01/results/ajshack/tiled-bench/_bench_ws/job/slurm-%j.log

# AFTER-only rerun: the first after sweep (job 35320024) died on the
# 500(SQLite lock)→retry→409 race; the worktree now resumes on 409 (Fix C)
# and the runner survives failed reps. before.csv from 35320024 stands.
set -u
cd /sdf/data/lcls/ds/prj/prjmaiqmag01/results/ajshack/tiled-bench
PY=.venv/bin/python
WT=/sdf/data/lcls/ds/prj/prjmaiqmag01/results/ajshack/tiled-catalog-broker/.worktrees/optimize-http-register-perf

echo "node: $(hostname)  cores: $(nproc)  start: $(date -Is)"
echo "== prewarm imports =="
time $PY -c "import tiled.server.app, tiled.catalog, tiled_catalog_broker, regbench.cli"

AFTER_SHA=$(git -C "$WT" rev-parse --short HEAD)
echo "after sha: $AFTER_SHA"

echo "== AFTER sweep ($AFTER_SHA) =="
rm -rf _bench_ws/after results/ingress/after.csv
PYTHONPATH=$WT/src $PY -m regbench.cli scale --workspace _bench_ws/after --out results/ingress/after.csv \
    --layouts per_entity,batched --sizes 10,100,1000 --workers 8 --location local
PYTHONPATH=$WT/src $PY _bench_ws/job/verify_csv.py results/ingress/after.csv

echo "== read-back check (after code) =="
rm -rf _bench_ws/readback
PYTHONPATH=$WT/src:$PWD $PY _bench_ws/job/readback_check.py _bench_ws/readback

echo "end: $(date -Is)"
