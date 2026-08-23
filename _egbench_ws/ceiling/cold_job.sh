#!/bin/bash
#SBATCH --partition=milano
#SBATCH --account=lcls:data
#SBATCH --exclusive
#SBATCH --nodes=1
#SBATCH --time=00:20:00
#SBATCH --job-name=cold-storage
#SBATCH --output=/sdf/data/lcls/ds/prj/prjmaiqmag01/results/ajshack/tiled-bench/_egbench_ws/ceiling/cold-%j.log
cd /sdf/data/lcls/ds/prj/prjmaiqmag01/results/ajshack/tiled-bench
echo "node: $(hostname) start: $(date -Is)"
.venv/bin/python _egbench_ws/ceiling/cold_storage.py
echo "COLD DONE"
