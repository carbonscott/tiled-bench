#!/bin/bash
#SBATCH --partition=milano
#SBATCH --account=lcls:data
#SBATCH --exclusive
#SBATCH --nodes=1
#SBATCH --time=02:00:00
#SBATCH --job-name=egress-ceiling
#SBATCH --output=/sdf/data/lcls/ds/prj/prjmaiqmag01/results/ajshack/tiled-bench/_egbench_ws/ceiling/slurm-%j.log

set -u
cd /sdf/data/lcls/ds/prj/prjmaiqmag01/results/ajshack/tiled-bench
source ../tiled-catalog-broker/.env.test
# 2026-08-11 deployment: admin-side config fix (server unchanged 0.2.15b1.dev14).
# Exact topology (direct revert vs fixed pooler) per admin — tag is date-scoped.
export CEILING_TAG=w16fix0811
echo "node: $(hostname)  cores: $(nproc)  start: $(date -Is)"
.venv/bin/python _egbench_ws/ceiling/ceiling.py
echo "end: $(date -Is)"
