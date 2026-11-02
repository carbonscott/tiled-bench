#!/bin/bash
# Attempt 2 of the pre-flight registration: same protocol, after adding server_base_dir
# to both contracts (attempt 1 registered client-side paths -> every server read 500'd).
# EDRIXS manifest was regenerated; NiPS3 register is tried against the existing manifest
# first and regenerates only if TileWright refuses it.
set -uo pipefail

BENCH=/sdf/data/lcls/ds/prj/prjmaiqmag01/results/ajshack/tiled-bench
TW=$BENCH/.venv-tw/bin/tilewright
PY=$BENCH/.venv-tw/bin/python
OUT=$BENCH/results/trainbench
LOG=$OUT/registration.log
CSV=$OUT/registration.csv
set -a; source /sdf/data/lcls/ds/prj/prjmaiqmag01/results/ajshack/tiled-catalog-broker/.env.test; set +a

SHA=$(git -C /sdf/data/lcls/ds/prj/prjmaiqmag01/results/ajshack/tilewright rev-parse --short HEAD)
NODE=$(hostname -s)
SERVER=$($PY -c "import httpx,os;print(httpx.get(os.environ['TILED_URL']+'/api/v1/',timeout=15).json().get('library_version',''))" 2>/dev/null)

run_timed() { # key action workers n note cmd...
  local key=$1 action=$2 workers=$3 n=$4 note=$5; shift 5
  echo "== $key $action ($(date -u +%FT%TZ)) ==" | tee -a "$LOG"
  local t0=$SECONDS
  "$@" 2>&1 | tee -a "$LOG"
  local rc=${PIPESTATUS[0]} wall=$((SECONDS-t0))
  local eps=$($PY -c "print(round($n/max($wall,1),2))" 2>/dev/null || echo "")
  echo "$(date -u +%FT%TZ),$key,$action,$workers,$n,$wall,$eps,$rc,$SHA,$SERVER,$NODE,$note" >> "$CSV"
  echo "-- $key $action: ${wall}s rc=$rc" | tee -a "$LOG"
  return "$rc"
}

DS_E=$BENCH/_bench_ws/trainbench/datasets/sam_klein_v2.yml
DS_N=$BENCH/_bench_ws/trainbench/datasets/nips3_multimodal_v2.yml
ART_N=hisym,powder,powder_mask,mag_a,mag_b,mag_cs
SH_N=H,energies,qs,energies_powder,radii

run_timed BENCH_BATCHED_5F delete 8 10000 attempt2 "$TW" delete BENCH_BATCHED_5F -y
run_timed BENCH_PER_ENTITY delete 8 7616  attempt2 "$TW" delete BENCH_PER_ENTITY -y

run_timed BENCH_BATCHED_5F register 8 10000 server_base_dir_fix \
  "$TW" register "$DS_E" --max-workers 8
run_timed BENCH_BATCHED_5F verify - 10000 attempt2 \
  "$PY" "$BENCH/trainbench/verify_registration.py" BENCH_BATCHED_5F 10000 \
  rixs_spectrum --shared eloss,omega_bounds
VE=$?

if ! run_timed BENCH_PER_ENTITY register 8 7616 server_base_dir_fix \
     "$TW" register "$DS_N" --max-workers 8; then
  run_timed BENCH_PER_ENTITY manifest_regen - 7616 after_server_base_dir \
    "$TW" manifest "$DS_N"
  run_timed BENCH_PER_ENTITY register 8 7616 server_base_dir_fix_retry \
    "$TW" register "$DS_N" --max-workers 8
fi
run_timed BENCH_PER_ENTITY verify - 7616 attempt2 \
  "$PY" "$BENCH/trainbench/verify_registration.py" BENCH_PER_ENTITY 7616 \
  "$ART_N" --shared "$SH_N"
VN=$?

if [ "$VE" -ne 0 ]; then
  run_timed BENCH_BATCHED_5F resume 8 10000 attempt2 "$TW" register "$DS_E" --max-workers 8
  run_timed BENCH_BATCHED_5F verify - 10000 attempt2_after_resume \
    "$PY" "$BENCH/trainbench/verify_registration.py" BENCH_BATCHED_5F 10000 \
    rixs_spectrum --shared eloss,omega_bounds
fi
if [ "$VN" -ne 0 ]; then
  run_timed BENCH_PER_ENTITY resume 8 7616 attempt2 "$TW" register "$DS_N" --max-workers 8
  run_timed BENCH_PER_ENTITY verify - 7616 attempt2_after_resume \
    "$PY" "$BENCH/trainbench/verify_registration.py" BENCH_PER_ENTITY 7616 \
    "$ART_N" --shared "$SH_N"
fi
echo "attempt2 done"
