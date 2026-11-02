# tiled-bench

Measure a [Tiled](https://blueskyproject.io/tiled/) server as a model with a few
parameters, keep every claim in a ledger with the conditions it was measured under, and
let an agent and an operator work the loop together in conversation.

```
tbench/
  fingerprint.py   observed (server version, client) + operator-reported (workers, db, pooler, limits)
  ledger.py        claims.jsonl, interventions.jsonl, reported.json, runs/<id>/
  workload.py      request shapes over bare httpx: healthz, metadata, array, asset
  drive.py         open_loop(rate) and closed_loop(concurrency); every call is a record
  observe.py       TiledMetrics, PgActivity, Reported; a Sampler that says what it was blind to
  model.py         little(), usl_fit(), knee(), Capacity.predict()
  experiments.py   rate_ladder, concurrency_ladder, compare, replay
questions/         one file per recurring question: hypotheses, prediction table, experiment, budget
ledger/            what we know (committed)
.claude/skills/ask the loop an agent runs for any question
legacy/, results/  the 2026-07..09 campaigns and their harnesses, frozen
```

## The idea

- **A claim, not a row.** An experiment ends with `ledger.claim("metadata.capacity_rate",
  228, fp, evidence=[run_id])`. Claims carry a fingerprint key.
- **The fingerprint is half yours.** The server version is observed. Worker count, database,
  pooler mode, pool sizes and CPU limits are what you tell the agent. When you change one
  ("scaled workers to 16"), the agent records `ledger.intervene("workers", 16)`, the key
  changes, and every earlier claim is stale without anyone remembering to mark it.
- **Predictions first.** A question is a table of hypotheses and what the cheapest experiment
  shows under each. `questions/knee.md` is the first.
- **Open loop for capacity.** Arrivals at a fixed rate; latency inflects at capacity;
  in-flight at capacity by Little's law is the slot count. Closed loop (N clients) is for
  "what do N clients get" and for the USL fit that separates contention from coherence.
- **Observers are optional and declared.** A run records which of `/metrics`,
  `pg_stat_activity` and operator-reported pod numbers it had.

## A session

```
you:    where does the catalog path saturate now?
agent:  fingerprint 3f9a…: server 0.2.10b2.dev493. I need workers, db, pooler, pool sizes.
you:    16 workers, postgres-cnpg, pgbouncer-session, pool 5+10, max_connections 250
agent:  recorded. No claims under this key; two stale ones from August (w16fix0811).
        Prediction table in questions/knee.md. Running compare(healthz, metadata),
        12 rungs × 20 s, ~60k requests, ~8 min. Window 14:02–14:10.
agent:  healthz flat to 400/s. metadata knees at 240 offered: capacity 221 req/s, floor 48 ms,
        in-flight 11.8; read_ms carried the growth; pg_activity shows active flat at 12.
        H1 survives. Claimed. The intervention H1 and H2 disagree on: raise the pooler pool
        from 12 to 24. If H1, capacity ≈ 440; if H2, unchanged.
you:    done, pool is 24.
agent:  recorded intervention; new key 81c0…; re-running metadata ladder.
```

## Running

```bash
source ../tiled-catalog-broker/.env.test     # TILED_URL, TILED_API_KEY; never print it
.venv/bin/python tests/test_tbench.py        # offline: fake server with a 4-slot semaphore
```

Experiments are run from a Python session or a short script the agent writes in the
conversation; parameters are saved in the run directory and `experiments.replay` reruns
them. Python imports are slow on a cold node of this filesystem; that is the filesystem.
