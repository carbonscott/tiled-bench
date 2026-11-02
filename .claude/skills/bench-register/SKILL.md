---
name: bench-register
description: Measure what registering a dataset into Tiled costs per node and where the time goes (client, network, server, database). Use when asked how long registration takes, whether a layout registers cheaper, or whether a tilewright or server change moved registration cost.
---

# What does registration cost, and where?

**Question.** Per entity and per artifact, how long does `tilewright register` spend,
split into client prep, network, and server (`app`, with `tok`/`acl`/`read`/`pack`
inside it)?

**The one metric.** Server `app_ms` p50 per `POST` (one per node), and wall minus app
as the client+network remainder, grouped by request path (container vs array nodes).

**Prior answer.** `results/INGRESS-FINDINGS.md`. Language for layouts and the
asset-dedup path is in `CONTEXT.md`.

## Protocol

1. Synthesize or pick a dataset; `tbench.synth.write_synthetic_dataset` writes HDF5 +
   YAML for one layout (it still targets the old broker's YAML; port to the tilewright
   contract the first time a round needs it, and say so in FINDINGS.md).
2. Build the Tiled client tilewright will use, attach `timing.Collector().attached([client])`
   around the register call, and time the whole thing with `time.perf_counter()`.
3. Read back: entity count and one artifact per layout must match the manifest.
4. Save the collector's records as `calls.jsonl` and a hand-built summary; `load.Run`
   is for closed-loop reads and does not fit here.
5. Delete the dataset afterwards on a shared server.

## Hazards

- A fresh dataset key per rep; re-registering the same key measures dedup, not insert.
- Postgres with 16 workers: 8 registration threads saw ~0.1% 500s on shared assets;
  4 saw none. Record the thread count.
