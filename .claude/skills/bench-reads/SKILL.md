---
name: bench-reads
description: Compare the ways of reading a dataset out of Tiled (container_export, artifact_read, raw_export, asset_bytes, h5py_direct) for a given workload. Use when asked which read path to recommend, or whether a read path regressed.
---

# Which read path for this workload?

**Question.** For one named workload (plot one artifact; whole entity; many entities;
whole dataset), which of the read paths in `tbench/reads.py` moves the data fastest
without moving data the user did not ask for?

**The one metric.** Seconds per entity at `threads=1`, and at `threads=8`, per read
path, with `total_wire` next to `total_payload` so wasted bytes are visible.

**Prior answer.** `results/EGRESS-TAKEAWAYS.md` has the recommendation table; this
skill re-measures a row of it, it does not redo the campaign.

## Protocol

1. Pick the dataset that matches the workload's entity shape (`results/` names the
   synthetic families: `EGRESS_PE_*`, `EGRESS_BAT_*`). Register it with tilewright if
   it is not on the server.
2. For each read path that applies: one `load.run` at `threads=1`, one at `threads=8`,
   `procs=1`, over the same keys. `container_export` takes key chunks as items;
   `artifact_read` takes single keys; `raw_export`/`asset_bytes` take one item per
   asset, which layout decides (per_entity: one per key; batched/grouped: one item of
   all keys).
3. `h5py_direct` on the same entities is the floor. Say whether the page cache was warm.
4. `assert_clean()` every run; additionally check `total_payload / units_ok` against the
   entity size you expect (±1%), or `total_wire` against the file size for the asset
   paths.

## Hazards

- Stock adapter + batched layout reads the whole file per entity; register broker.
- `asset_bytes` on a batched dataset moves the whole file regardless of key count.
- `container_export` builds the whole output in server RAM (step 7f: 4 concurrent 4 GB
  exports OOM-kill a worker). Keep export chunks small on shared servers.
- `raw_export` dies off the main thread (upstream SIGINT bug through 0.2.14): use
  `threads=1`, scale with `procs`.
