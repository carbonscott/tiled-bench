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
| H1 | A fixed pool between tiled and Postgres (pooler pool, or tiled's per-worker pools summed) | latency inflects at a rate where in-flight ≈ pool size; `read_ms` carries the growth | `active` flat at the pool size, `idle` pool holds the rest, no wait events | raising the pool moves the knee proportionally |
| H2 | Database CPU | inflects where the DB is pegged; in-flight at knee is whatever the arrival rate × latency happens to be | `active` tracks offered load, `wait_event` empty, DB CPU at limit | more tiled workers do nothing; DB CPU limit moves it |
| H3 | Tiled worker CPU or pod CPU quota | `app_ms` grows but no segment (`tok`,`read`,`pack`) grows with it | DB quiet, `active` small | more workers / higher quota moves it; pool changes do not |
| H4 | Ingress or TLS fabric | `healthz` ladder knees at the same rate as `metadata` | nothing | — |

H4 is cheap and goes first (`compare(["healthz", "metadata"])`). It was refuted under the
August fingerprint (6,300 req/s on healthz) but that fingerprint is dead.

## Experiment

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
