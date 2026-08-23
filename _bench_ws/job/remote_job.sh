#!/bin/bash
#SBATCH --partition=milano
#SBATCH --account=lcls:data
#SBATCH --exclusive
#SBATCH --nodes=1
#SBATCH --time=03:00:00
#SBATCH --job-name=regbench-remote
#SBATCH --output=/sdf/data/lcls/ds/prj/prjmaiqmag01/results/ajshack/tiled-bench/_bench_ws/job/slurm-remote-%j.log

# Remote (tiled-test) registration before/after. Workspace lives under
# data-source/ so the synthetic HDF5 is inside the server's readable_storage;
# regbench derives server_base_dir from TILED_HOST_DATA_ROOT/TILED_SERVER_DATA_ROOT.
# All regbench_* keys are deleted from the server afterwards.
set -u
cd /sdf/data/lcls/ds/prj/prjmaiqmag01/results/ajshack/tiled-bench
source ../tiled-catalog-broker/.env.test
PY=.venv/bin/python
WT=/sdf/data/lcls/ds/prj/prjmaiqmag01/results/ajshack/tiled-catalog-broker/.worktrees/optimize-http-register-perf
WS=/sdf/data/lcls/ds/prj/prjmaiqmag01/results/data-source/regbench_ws
mkdir -p results/ingress "$WS"

echo "node: $(hostname)  start: $(date -Is)"
echo "== prewarm =="
time $PY -c "import tiled.server.app, tiled_catalog_broker, regbench.cli"

BEFORE_SHA=$(git -C ../tiled-catalog-broker rev-parse --short HEAD)
AFTER_SHA=$(git -C "$WT" rev-parse --short HEAD)
echo "before sha: $BEFORE_SHA   after sha: $AFTER_SHA   remote: $TILED_URL"

echo "== read-back check (after code, local server) =="
# Absolute workspace path: regbench builds asset URIs as file://localhost<base_dir>,
# so a relative workspace yields malformed URIs the server refuses to serve.
rm -rf _bench_ws/readback
PYTHONPATH=$WT/src:$PWD $PY _bench_ws/job/readback_check.py "$PWD/_bench_ws/readback"

echo "== remote BEFORE sweep ($BEFORE_SHA) =="
rm -rf "$WS/before" results/ingress/remote_before.csv
$PY -m regbench.cli scale --workspace "$WS/before" --out results/ingress/remote_before.csv \
    --layouts per_entity,batched --sizes 10,100,1000 --workers 8 \
    --location remote --remote-uri "$TILED_URL" --remote-api-key "$TILED_API_KEY"
$PY _bench_ws/job/verify_csv.py results/ingress/remote_before.csv

echo "== remote AFTER sweep ($AFTER_SHA) =="
rm -rf "$WS/after" results/ingress/remote_after.csv
PYTHONPATH=$WT/src $PY -m regbench.cli scale --workspace "$WS/after" --out results/ingress/remote_after.csv \
    --layouts per_entity,batched --sizes 10,100,1000 --workers 8 \
    --location remote --remote-uri "$TILED_URL" --remote-api-key "$TILED_API_KEY"
PYTHONPATH=$WT/src $PY _bench_ws/job/verify_csv.py results/ingress/remote_after.csv

echo "== remote read probe (readable_storage coverage of regbench_ws) =="
$PY - <<'EOF'
import os
from tiled.client import from_uri
c = from_uri(os.environ["TILED_URL"], api_key=os.environ["TILED_API_KEY"])
keys = sorted(k for k in c.keys() if k.startswith("regbench_"))
ds = c[keys[-1]]
ent = ds[sorted(ds.keys())[0]]
art = ent[sorted(ent.keys())[0]]
try:
    arr = art.read()
    print(f"[probe] remote read OK: {arr.shape} {arr.dtype} from {keys[-1]}")
except Exception as e:
    # Refusal is a live possibility: readable_storage may not cover the new
    # data-source/regbench_ws path. Timing rows are unaffected either way.
    print(f"[probe] remote read FAILED ({type(e).__name__}: {e}) — "
          f"readable_storage likely does not cover regbench_ws")
EOF

echo "== cleanup: delete regbench_* keys from remote =="
$PY _bench_ws/job/remote_cleanup.py

echo "end: $(date -Is)"
