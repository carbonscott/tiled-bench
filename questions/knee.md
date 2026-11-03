# Where is the catalog path's capacity, and what sets it?

**Quantity.** `metadata.capacity_rate` (req/s), `metadata.latency_floor_ms`,
`metadata.in_flight_at_capacity` (the slot count), and which hypothesis sets it.

**Why it matters.** Every interactive and training workload is a mix of catalog lookups
and reads; the catalog lookup is the smallest unit and the one that saturated first in
every prior deployment (~135 req/s at 8 workers, ~230 at 16; see `legacy/` for how that
was measured, closed loop, which is why the slot count was never resolved).

## Hypotheses and what the experiment shows for each

| # | Hypothesis | rate_ladder(metadata) | pg_activity during the ladder | after intervention |
|---|---|---|---|---|
| H1 | Tiled's per-process catalog pools, summed: `pods × workers_per_pod × (catalog_pool_size + catalog_max_overflow)` | latency inflects where in-flight ≈ that sum; `read_ms` carries the growth | `active` flat at that sum, nothing waiting | raising `catalog_pool_size` moves the knee proportionally |
| H2 | Database CPU: one CNPG primary serves every query, `db_cpu_limit` cores | inflects where the primary is pegged; in-flight at knee has no relation to the pool sum | `active` tracks offered load past the pool sum, `wait_event` empty | pool changes do nothing; the DB CPU limit moves it |
| H3 | Tiled worker CPU or pod CPU quota | `app_ms` grows but no segment (`tok`,`read`,`pack`) grows with it | DB quiet, `active` small | more workers / higher quota moves it; pool changes do not |
| H4 | Ingress or TLS fabric | `healthz` ladder knees at the same rate as `metadata` | nothing | — |

H4 is cheap and goes first (`compare(["healthz", "metadata"])`). It was refuted under the
August fingerprint (6,300 req/s on healthz) but that fingerprint is dead.

## Experiment

Under the deployment read on 2026-11-03 (3 pods, catalog pool 3+3 per process, DB primary
limited to 1 core) H1 predicts in-flight at capacity of 18 × workers_per_pod; H2 predicts
a throughput cap set by the 1-core primary with in-flight well above that.

```python
fp = ledger.current(URL, KEY)                       # ask operator for reported fields first
keys = workload.entity_keys(URL, KEY, "EGRESS_PE_64K_BROKER", 1000)
obs = [observe.TiledMetrics(URL, KEY)] + ([observe.PgActivity(PSQL)] if PSQL else [])
rates = [10, 20, 40, 60, 80, 100, 130, 160, 200, 250, 300, 400]
res = experiments.compare(URL, KEY, "EGRESS_PE_64K_BROKER", "signal", ["healthz", "metadata"],
                          keys, rates, duration=20, observers=obs, fp=fp)
```

Budget: at most 12 rungs × 20 s × ≤400 req/s ≈ 60k requests of ~1 KB, about 8 minutes,
per shape. Run from a compute node, not a login node.

## Reading it

- `knee["capacity_rate"]` and `knee["in_flight_at_capacity"]` are the claims.
- The segment that grows between the floor rung and the knee rung names the layer.
- `observe.jsonl` → `pg_activity.activity` rows at the knee rung's wall-clock window:
  count `active` vs `idle`, and look at `wait_event_type`.
- Then, with the operator: the one intervention H1/H2/H3 disagree on, and the same ladder
  again under the new fingerprint.
