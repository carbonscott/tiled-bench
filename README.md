# tiled-bench

Measurements of a [Tiled](https://blueskyproject.io/tiled/) server serving the MAIQMag
datasets. The shape is: a small library of functions that work well with Tiled, a skill
per question, and one directory per round of measurement.

```
tbench/            the library (~1,000 lines, 650 of them new; no CLI)
  reads.py         the read paths under test, one function each
  timing.py        every HTTP call with the full Server-Timing split (tok/acl/read/pack/app)
  load.py          run(work, items, client, threads, procs) -> Run; Round for the directory
  probes.py        server version, healthz, /metrics (pool gauges), pg_stat_activity sampler
  server.py        a local SQLite tiled as a context manager
  synth.py         synthetic datasets per layout
.claude/skills/    one skill per question: bench-knee, bench-reads, bench-register, bench-epoch
rounds/            one directory per round: the script that ran, raw records, FINDINGS.md
results/           the 2026-07/09 campaigns (findings + CSVs), unchanged
legacy/            the harnesses those campaigns used, kept so their results stay reproducible
CONTEXT.md         domain language (entity, artifact, layout, dataset)
```

## A round, end to end

```python
from tiled.client import from_uri
from tbench import load, reads, probes

URL, KEY = os.environ["TILED_URL"], os.environ["TILED_API_KEY"]
client = lambda: from_uri(URL, api_key=KEY)["EGRESS_PE_64K_BROKER"]
keys = list(client().keys())

with load.Round("knee", url=URL, api_key=KEY) as rd:
    for threads in (1, 2, 4, 8):
        before = probes.tiled_metrics(URL, KEY)
        run = load.run(lambda node, k: reads.artifact_read(node, k, "signal"),
                       keys, client, threads=threads, procs=8, label=f"t{threads:02d}")
        run.meta["metrics_delta"] = probes.metrics_delta(before, probes.tiled_metrics(URL, KEY))
        rd.save(run.assert_clean(expected_units=len(keys)))
```

That writes `rounds/<date>-knee/{meta.json, t01.summary.json, t01.units.jsonl,
t01.calls.jsonl, ...}`. `Run.summary()` gives req/s, per-segment p50/p95 and the
Little's-law in-flight (app-seconds per wall-second). The records are plain JSONL: load
them with pandas when reading the round.

The library does not decide what to measure. The skill does, in a page: the question, the
one metric that answers it, the protocol, the guardrails, how to read the result. An
agent reads the skill, writes a script like the one above, runs it, and writes
`FINDINGS.md` next to the records.

## Rules

- **Every round commits three things:** the script that ran, the raw records, and
  `FINDINGS.md`. The numbers in a findings file are reproducible from the script beside
  it or they do not go in.
- **A fast-but-wrong run is a failed run.** `Run.assert_clean()` before saving; the
  skills list the per-question checks on top of that.
- **No retries in the read paths.** Server errors are data. The Tiled client retries on
  its own; `reads.metadata_get` is the raw probe that does not.
- **Scale by process.** One Python client process caps near 540 req/s and ~190 MB/s.
- **Shared server etiquette.** Record the wall-clock window in `FINDINGS.md`; delete
  what you registered.

## Running

```bash
source ../tiled-catalog-broker/.env.test            # TILED_URL, TILED_API_KEY (never print it)
.venv/bin/python rounds/2026-11-02-knee/run.py --threads 1 2 4 --procs 2 --n 100   # smoke
.venv/bin/python tests/test_tbench.py     # offline tests
```

Heavy rounds go through SLURM on an exclusive node; the round's `run.py` is the job
script's only payload. Python imports are slow on a cold node of this filesystem
(minutes); that is the filesystem, not the library.
