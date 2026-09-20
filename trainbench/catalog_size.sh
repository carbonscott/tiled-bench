#!/bin/bash
# Catalog-size byproduct: register both bench datasets into a fresh local SQLite
# catalog and measure the .db file — tests TileWright's documented "~5 MB per 10k
# entities" claim. Also yields local (SQLite) registration timing rows for free.
# Run from tiled-bench root; server lives and dies inside this script.
set -uo pipefail

BENCH=/sdf/data/lcls/ds/prj/prjmaiqmag01/results/ajshack/tiled-bench
PY=$BENCH/.venv-tw/bin/python
TW=$BENCH/.venv-tw/bin/tilewright
WS=$BENCH/_bench_ws/trainbench/catsize
OUT=$BENCH/results/trainbench
CSV=$OUT/registration.csv
PORT=8207
cd /  # never run rm -rf $WS with a CWD inside it (a prior shell may sit there)
rm -rf "$WS"; mkdir -p "$WS" "$OUT"; cd "$WS"

cat > "$WS/config.yml" <<EOF
uvicorn: {host: "127.0.0.1", port: $PORT}
trees:
  - path: /
    tree: catalog
    args:
      uri: "sqlite:///$WS/catalog.db"
      init_if_not_exists: true
      writable_storage: "$WS/storage"
      adapters_by_mimetype:
        application/x-hdf5-broker: "tilewright.adapter:LazyHDF5ArrayAdapter"
      readable_storage:
        - "/sdf/data/lcls/ds/prj/prjmaiqmag01/results/data-source/sam/initial_data_proper"
        - "/sdf/data/lcls/ds/prj/prjmaiqmag01/results/data-source/NiPS3_Multimodal_Synthetic/data"
EOF

"$BENCH/.venv-tw/bin/tiled" serve config "$WS/config.yml" --api-key localsecret \
  > "$WS/server.log" 2>&1 &
SRV=$!
trap 'kill $SRV 2>/dev/null' EXIT
for i in $(seq 1 240); do
  curl -sf "http://127.0.0.1:$PORT/api/v1/" >/dev/null 2>&1 && break; sleep 1
done
curl -sf "http://127.0.0.1:$PORT/api/v1/" >/dev/null || { echo "server never came up"; exit 1; }

export TILED_URL=http://127.0.0.1:$PORT TILED_API_KEY=localsecret
SHA=$(git -C /sdf/data/lcls/ds/prj/prjmaiqmag01/results/ajshack/tilewright rev-parse --short HEAD)
NODE=$(hostname -s)
[ -f "$CSV" ] || echo "ts,key,action,workers,n_entities,wall_s,ent_per_s,exit,tilewright_sha,server_version,node,notes" >> "$CSV"

reg() { # key yaml n
  local t0=$SECONDS
  "$TW" register "$2" --max-workers 8 >> "$WS/register.log" 2>&1
  local rc=$? wall=$((SECONDS-t0))
  echo "$(date -u +%FT%TZ),$1,register_local_sqlite,8,$3,$wall,$($PY -c "print(round($3/max($wall,1),2))"),$rc,$SHA,local-sqlite,$NODE,catalog_size_probe" >> "$CSV"
  echo "$1: ${wall}s rc=$rc"
}

reg BENCH_BATCHED_5F "$BENCH/_bench_ws/trainbench/datasets/sam_klein_v2.yml" 10000
S1=$(stat -c%s "$WS/catalog.db")
reg BENCH_PER_ENTITY "$BENCH/_bench_ws/trainbench/datasets/nips3_multimodal_v2.yml" 7616
S2=$(stat -c%s "$WS/catalog.db")

echo "$(date -u +%FT%TZ),BENCH_BATCHED_5F,catalog_db_bytes,,10000,$S1,,0,$SHA,local-sqlite,$NODE,db after 10k-entity dataset" >> "$CSV"
echo "$(date -u +%FT%TZ),BOTH,catalog_db_bytes,,17616,$S2,,0,$SHA,local-sqlite,$NODE,db after both datasets (17616 ents; 55616 artifact rows)" >> "$CSV"
echo "catalog.db: after EDRIXS(10k)=$S1 bytes; after both=$S2 bytes"
