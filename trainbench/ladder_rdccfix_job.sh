#!/bin/bash
#SBATCH --partition=milano
#SBATCH --account=lcls:data
#SBATCH --exclusive
#SBATCH --nodes=1
#SBATCH --time=04:00:00
#SBATCH --job-name=tb-rdccfix
#SBATCH --output=/sdf/data/lcls/ds/prj/prjmaiqmag01/results/ajshack/tiled-bench/_bench_ws/trainbench/slurm-%j.log
# Re-measure the EDRIXS rdcc=256 cells with the dataset-handle cache fix (the first
# ladder re-derefed f[dset] per sample, so the chunk cache never engaged and rdcc256
# rows measured the same thrash as rdcc1). Full epochs, notes=dsid_cache_fix.
set -uo pipefail
BENCH=/sdf/data/lcls/ds/prj/prjmaiqmag01/results/ajshack/tiled-bench
PY=$BENCH/.venv-tw/bin/python
OUT=$BENCH/results/trainbench
DISC=$BENCH/_bench_ws/trainbench/discovery_edrixs.parquet
cd "$BENCH"
set -a; source /sdf/data/lcls/ds/prj/prjmaiqmag01/results/ajshack/tiled-catalog-broker/.env.test; set +a

run() { local path=$1 cache=$2 w=$3 rep=$4; shift 4
  $PY -m trainbench --dataset edrixs --path "$path" --cache "$cache" --workers "$w" \
      --reps 1 --seed $((20260919 + rep)) --outdir "$OUT" --discovery-cache "$DISC" \
      --rdcc-mb 256 --notes dsid_cache_fix "$@" \
      || echo "CELL FAILED: edrixs $path $cache w=$w rep=$rep"
}

for w in 1 2 4 8 16 32; do
  for cache in warm cold; do
    reps=5; [ "$cache" = cold ] && reps=3
    for rep in $(seq 1 $reps); do
      run h5py_plain    "$cache" "$w" "$rep"
      run mode_a_cached "$cache" "$w" "$rep"
    done
  done
done
echo "rdccfix ladder done"
