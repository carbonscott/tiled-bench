---
name: bench-epoch
description: Time one training-shaped epoch (shuffled sample order, N dataloader workers) through Tiled versus straight from the HDF5 files. Use when asked whether a training job should read through the server or from the locators, or how many workers saturate.
---

# How long is one epoch, through the server and from the files?

**Question.** For a real registered dataset and a shuffled sample order, how many
samples per second do `workers` processes achieve reading through Tiled versus from
the files via the `path_/dataset_/index_` locators, and where does each saturate?

**The one metric.** Samples per second per worker count, both paths, and
`busy fraction` (sum of per-worker busy time over workers × epoch wall) so a skewed
shard shows up.

**Prior answer.** `results/TRAINING-FINDINGS.md`.

## Protocol

1. `load.run(work, order, client, procs=workers, threads=1)` where `order` is the
   shuffled sample index list and `work` reads one sample. For the file path, `client`
   returns an object holding an open-file cache, not a Tiled node.
2. Every Nth sample, `work` returns a short hash of the bytes under a non-numeric key
   (`"spot": sha1[:12]`); non-numeric values stay on the unit record. Compare spots
   across paths and reps: same sample, same hash.
3. Prime or evict the page cache deliberately between reps and record which.
4. Ladder `workers` 1, 2, 4, 8, 16, 32 until samples/s stops rising.

## Hazards

- Metadata discovery (listing keys, resolving locators) is outside the timed epoch;
  report it once separately.
- Keep `rdcc` (h5py chunk cache) fixed across reps; it moved results before.
