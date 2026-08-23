"""True per-node storage rates from a cold-cache milano node.

Runs on a freshly allocated node (nothing in page cache), reads disjoint file
subsets per phase so later phases stay cold:

  A. 1 process, sequential h5py reads, 32 x 16.8 MB files      (cold single-stream)
  B. 8 processes x 16 files each, disjoint                     (cold 8-way)
  C. 16 processes x 6 files each, disjoint                     (cold 16-way)
  D. 1 process, re-read phase-A files                          (warm control)
  E. 1 process, sequential read of the 4.3 GB batched file     (cold big-stream)

Prints one JSON line per phase: {phase, procs, files, mb, wall_s, mbps}.
"""
import json
import subprocess
import sys
import time
from pathlib import Path

PE_DIR = Path("/sdf/data/lcls/ds/prj/prjmaiqmag01/results/data-source/"
               "egress_bench/egress_pe_16m/data")
BAT = Path("/sdf/data/lcls/ds/prj/prjmaiqmag01/results/data-source/"
           "egress_bench/egress_bat_16m/data/batch.h5")
PY = sys.executable

READER = r"""
import sys, time, h5py, numpy as np
files = sys.argv[1:]
t0 = time.perf_counter()
nbytes = 0
for f in files:
    with h5py.File(f, "r") as h:
        nbytes += np.asarray(h["signal"]).nbytes
print(nbytes, time.perf_counter() - t0)
"""


def read_files(shards):
    """One subprocess per shard; returns (total_mb, wall_s)."""
    t0 = time.perf_counter()
    procs = [subprocess.Popen([PY, "-c", READER, *map(str, s)],
                              stdout=subprocess.PIPE, text=True)
             for s in shards]
    total = 0
    for p in procs:
        out, _ = p.communicate()
        total += int(out.split()[0])
    return total / 1e6, time.perf_counter() - t0


files = sorted(PE_DIR.glob("*.h5"))
assert len(files) == 256, len(files)

phases = []
mb, wall = read_files([files[0:32]])
phases.append(("A_cold_1proc", 1, 32, mb, wall))
mb, wall = read_files([files[32 + i*16: 32 + (i+1)*16] for i in range(8)])
phases.append(("B_cold_8proc", 8, 128, mb, wall))
mb, wall = read_files([files[160 + i*6: 160 + (i+1)*6] for i in range(16)])
phases.append(("C_cold_16proc", 16, 96, mb, wall))
mb, wall = read_files([files[0:32]])
phases.append(("D_warm_1proc", 1, 32, mb, wall))

t0 = time.perf_counter()
import h5py
import numpy as np
with h5py.File(BAT, "r") as h:
    d = h["signal"]
    n = 0
    for i in range(0, d.shape[0], 16):  # 16 rows x 16.8 MB = 269 MB blocks
        n += np.asarray(d[i:i+16]).nbytes
wall = time.perf_counter() - t0
phases.append(("E_cold_bigfile_1proc", 1, 1, n / 1e6, wall))

for name, procs, nf, mb, wall in phases:
    print(json.dumps({"phase": name, "procs": procs, "files": nf,
                      "mb": round(mb, 1), "wall_s": round(wall, 2),
                      "mbps": round(mb / wall, 1)}))
