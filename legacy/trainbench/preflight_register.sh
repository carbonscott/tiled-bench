#!/bin/bash
# Pre-flight for the training-shaped campaign (TRAINING-BENCH-PLAN.md, task "registration
# byproducts"): delete the stale bench keys, register both datasets fresh with shipped
# TileWright at the SHIPPED default workers=8, verify every entity + spot artifacts
# (the pool-8 race check), and if broken, re-register to test resume/self-heal.
# Run from tiled-bench root. Appends to results/trainbench/registration.csv.
set -uo pipefail

BENCH=/sdf/data/lcls/ds/prj/prjmaiqmag01/results/ajshack/tiled-bench
TW=$BENCH/.venv-tw/bin/tilewright
PY=$BENCH/.venv-tw/bin/python
OUT=$BENCH/results/trainbench
LOG=$OUT/registration.log
CSV=$OUT/registration.csv
mkdir -p "$OUT"
set -a; source /sdf/data/lcls/ds/prj/prjmaiqmag01/results/ajshack/tiled-catalog-broker/.env.test; set +a

SHA=$(git -C /sdf/data/lcls/ds/prj/prjmaiqmag01/results/ajshack/tilewright rev-parse --short HEAD)
NODE=$(hostname -s)
SERVER=$($PY -c "import httpx,os;print(httpx.get(os.environ['TILED_URL']+'/api/v1/',timeout=15).json().get('library_version',''))" 2>/dev/null)

[ -f "$CSV" ] || echo "ts,key,action,workers,n_entities,wall_s,ent_per_s,exit,tilewright_sha,server_version,node,notes" >> "$CSV"

row() { # key action workers n wall exit notes
  local eps="";  [ "$5" != "" ] && [ "$4" -gt 0 ] && eps=$($PY -c "print(round($4/$5,2))")
  echo "$(date -u +%FT%TZ),$1,$2,$3,$4,$5,$eps,$6,$SHA,$SERVER,$NODE,$7" >> "$CSV"
}

run_timed() { # key action workers n cmd...
  local key=$1 action=$2 workers=$3 n=$4; shift 4
  echo "== $key $action ($(date -u +%FT%TZ)) ==" | tee -a "$LOG"
  local t0=$SECONDS
  "$@" 2>&1 | tee -a "$LOG"
  local rc=${PIPESTATUS[0]} wall=$((SECONDS-t0))
  row "$key" "$action" "$workers" "$n" "$wall" "$rc" ""
  echo "-- $key $action: ${wall}s rc=$rc" | tee -a "$LOG"
  return "$rc"
}

verify() { # key n artifacts shared
  run_timed "$1" verify - "$2" \
    "$PY" "$BENCH/trainbench/verify_registration.py" "$1" "$2" "$3" --shared "$4"
}

DS_E=$BENCH/_bench_ws/trainbench/datasets/sam_klein_v2.yml
DS_N=$BENCH/_bench_ws/trainbench/datasets/nips3_multimodal_v2.yml
ART_N=hisym,powder,powder_mask,mag_a,mag_b,mag_cs
SH_N=H,energies,qs,energies_powder,radii

# 1. delete stale keys (timed; deletion cost is a byproduct number too)
run_timed BENCH_BATCHED_5F delete 8 10000 "$TW" delete BENCH_BATCHED_5F -y
run_timed BENCH_PER_ENTITY delete 8 7616  "$TW" delete BENCH_PER_ENTITY -y

# 2. fresh registration at the SHIPPED default (workers=8) — the race-check condition
run_timed BENCH_BATCHED_5F register 8 10000 "$TW" register "$DS_E" --max-workers 8
run_timed BENCH_PER_ENTITY register 8 7616  "$TW" register "$DS_N" --max-workers 8

# 3. verify per-entity/per-artifact
verify BENCH_BATCHED_5F 10000 rixs_spectrum eloss,omega_bounds; VE=$?
verify BENCH_PER_ENTITY 7616 "$ART_N" "$SH_N"; VN=$?

# 4. resume/self-heal test only if broken
if [ "$VE" -ne 0 ]; then
  run_timed BENCH_BATCHED_5F resume 8 10000 "$TW" register "$DS_E" --max-workers 8
  verify BENCH_BATCHED_5F 10000 rixs_spectrum eloss,omega_bounds
fi
if [ "$VN" -ne 0 ]; then
  run_timed BENCH_PER_ENTITY resume 8 7616 "$TW" register "$DS_N" --max-workers 8
  verify BENCH_PER_ENTITY 7616 "$ART_N" "$SH_N"
fi

echo "preflight done; rows in $CSV"
