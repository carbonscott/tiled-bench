#!/bin/bash
#SBATCH --partition=milano
#SBATCH --account=lcls:data
#SBATCH --exclusive
#SBATCH --nodes=1
#SBATCH --time=08:00:00
#SBATCH --job-name=trainbench
#SBATCH --output=/sdf/data/lcls/ds/prj/prjmaiqmag01/results/ajshack/tiled-bench/_bench_ws/trainbench/slurm-%j.log
# Saturation ladder for one dataset: sbatch ladder_job.sh {edrixs|nips3}
# Interleaves paths within each (workers, cache, rep) cell per the campaign protocol.
set -uo pipefail

DATASET=${1:?usage: ladder_job.sh edrixs|nips3}
BENCH=/sdf/data/lcls/ds/prj/prjmaiqmag01/results/ajshack/tiled-bench
PY=$BENCH/.venv-tw/bin/python
OUT=$BENCH/results/trainbench
DISC=$BENCH/_bench_ws/trainbench/discovery_${DATASET}.parquet
cd "$BENCH"
set -a; source /sdf/data/lcls/ds/prj/prjmaiqmag01/results/ajshack/tiled-catalog-broker/.env.test; set +a

run() { # path cache workers rep extra...   (rep feeds the shuffle seed)
  local path=$1 cache=$2 w=$3 rep=$4; shift 4
  $PY -m trainbench --dataset "$DATASET" --path "$path" --cache "$cache" \
      --workers "$w" --reps 1 --seed $((20260919 + rep)) \
      --outdir "$OUT" --discovery-cache "$DISC" "$@" \
      || echo "CELL FAILED: $DATASET $path $cache w=$w rep=$rep $*"
}

# Discovery sweep timing (uncached query_catalog) — one dedicated tiny cell first.
if [ ! -f "$DISC" ]; then
  run mode_a warm 1 0 --cap 64 --notes discovery_sweep_probe
fi

WORKERS="1 2 4 8 16 32"

if [ "$DATASET" = "edrixs" ]; then
  # gzip-chunked: rdcc is an axis. rdcc=1 (h5py default) capped for runtime; rdcc=256
  # (the one-knob fix) runs the full epoch. mode_a reopens per sample (shipped
  # semantics) so a big chunk cache cannot help it — rdcc=1 only.
  for w in $WORKERS; do
    for cache in warm cold; do
      reps=5; [ "$cache" = cold ] && reps=3
      for rep in $(seq 1 $reps); do
        run h5py_plain    "$cache" "$w" "$rep" --rdcc-mb 1   --cap 2000
        run h5py_plain    "$cache" "$w" "$rep" --rdcc-mb 256
        run mode_a        "$cache" "$w" "$rep" --rdcc-mb 1   --cap 2000
        run mode_a_cached "$cache" "$w" "$rep" --rdcc-mb 1   --cap 2000
        run mode_a_cached "$cache" "$w" "$rep" --rdcc-mb 256
      done
    done
  done
  HTTP_CAP=2000
else
  for w in $WORKERS; do
    for cache in warm cold; do
      reps=5; [ "$cache" = cold ] && reps=3
      for rep in $(seq 1 $reps); do
        run h5py_plain    "$cache" "$w" "$rep"
        run mode_a        "$cache" "$w" "$rep"
        run mode_a_cached "$cache" "$w" "$rep"
      done
    done
  done
  HTTP_CAP=1000
fi

# HTTP per-sample ladder (server cache is ambient; cap logged in every row)
for w in $WORKERS; do
  for rep in 1 2 3; do
    run http ambient "$w" "$rep" --cap "$HTTP_CAP"
  done
done

# Documented bulk workflow (single process) — secondary rows
for rep in 1 2 3; do run mode_a_bulk warm 1 "$rep"; done
run mode_a_bulk cold 1 0

echo "ladder done: $DATASET"
