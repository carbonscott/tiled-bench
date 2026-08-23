#!/bin/bash
#SBATCH --partition=milano
#SBATCH --account=lcls:data
#SBATCH --exclusive
#SBATCH --nodes=1
#SBATCH --time=02:00:00
#SBATCH --job-name=dm-bench
#SBATCH --output=/sdf/data/lcls/ds/prj/prjmaiqmag01/results/ajshack/tiled-bench/_bench_ws/datamodel/slurm-%j.log

# Data-model probes: local catalog-size scaling (reuses regbench after-sweep
# catalogs + fresh 10k) and remote discovery-route latency on tiled-test.
set -u
cd /sdf/data/lcls/ds/prj/prjmaiqmag01/results/ajshack/tiled-bench
source ../tiled-catalog-broker/.env.test
PY=.venv/bin/python
WT=/sdf/data/lcls/ds/prj/prjmaiqmag01/results/ajshack/tiled-catalog-broker/.worktrees/optimize-http-register-perf
mkdir -p results/datamodel

echo "node: $(hostname)  start: $(date -Is)"
echo "== prewarm =="
time $PY -c "import tiled.server.app, tiled.catalog, tiled_catalog_broker, regbench.cli, pandas"

echo "== local catalog-size scaling (after-code, incl. fresh 10k) =="
# $PWD on PYTHONPATH: script-by-path puts the script dir at sys.path[0]; regbench
# lives in the repo root.
PYTHONPATH=$WT/src:$PWD $PY _bench_ws/datamodel/dm_local_scaling.py \
    results/datamodel/local_scaling.csv _bench_ws/after --with-10k

echo "== remote discovery routes (tiled-test) =="
$PY _bench_ws/datamodel/dm_remote.py results/datamodel/remote_routes.csv

echo "end: $(date -Is)"
