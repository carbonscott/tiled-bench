---
name: ask
description: Answer a performance question about a Tiled server by experiment, in conversation with the operator. Use for any "how fast / how many / where is the limit / did the change help" question about the server, the catalog, the database, or a read path. Reads the ledger first, runs the cheapest discriminating experiment, records a claim, and asks the operator for back-end changes or facts it cannot observe.
---

# ask: question → hypotheses → experiment → claim

The server is a model with a few parameters (service time per request shape, a
concurrency limit, a wire ceiling, a database cost per request). A question is answered
when the model predicts the measurement. The ledger (`ledger/`) holds every claim with
the fingerprint it was measured under; a back-end change makes earlier claims stale by
construction. You never remember what the server was; you read it.

## The loop

1. **Fingerprint.** `fp = ledger.current(URL, KEY)`. If `fp["reported"]` is missing any
   of `fingerprint.REPORTED_FIELDS` that the question depends on (workers, db, pooler,
   pool sizes, CPU limits), ask the operator for them now, in one message, and record each
   answer with `ledger.intervene(field, value, by="operator")`. Do not guess them.
2. **What is already known.** `ledger.known(fp)` lists claims valid right now;
   `ledger.stale(fp)` lists claims from other fingerprints and `ledger.interventions()`
   says what changed in between. A stale claim is a prior, not an answer.
3. **Hypotheses with predictions.** Write the competing explanations and, for each, what
   the cheapest experiment would show. The question files in `questions/` have these
   written for the recurring questions; for a new question, write the table in the
   conversation before running anything. No experiment without a prediction table.
4. **Run the discriminating experiment** from `tbench.experiments`, with every observer
   that is reachable (`observe.TiledMetrics`, `observe.PgActivity` if the operator gives
   a `psql` command). Budget: say how many requests, bytes and minutes it will cost the
   shared server before starting; stop rules are built in.
5. **Verdict.** Compare rungs, knee, USL fit and observer samples against the prediction
   table. State which hypothesis survived and which were refuted, with the numbers.
6. **Claim.** `ledger.claim(quantity, value, fp, evidence=[run_id], interval=..., method=...)`
   for each quantity measured, including refuted predictions (`status="refuted"`).
7. **If a hypothesis needs a back-end change** (scale workers, change the pool size,
   raise a limit), ask the operator for exactly that change, with the prediction for
   each outcome. Wait. When they report it done and give the new value, record it with
   `ledger.intervene(...)`, re-fingerprint, and re-run the same experiment. The before
   and after are two claims under two fingerprints with an intervention between them;
   that is the whole record of the change.

## Experiments and what they can say

- `experiments.rate_ladder` (open loop): capacity in req/s, latency floor, and
  **in-flight at capacity** by Little's law. This is the only way to measure "how many
  at once": a closed loop cannot overload the server and its in-flight is just the client
  count.
- `experiments.concurrency_ladder` (closed loop) + `model.usl_fit`: whether the limit is
  contention (`sigma`, a serialised resource; `1/sigma` is the slot count if it is a pure
  semaphore) or coherence (`kappa`, cost growing with N², throughput that falls).
- `experiments.compare`: the same ladder for two shapes (`healthz` vs `metadata` isolates
  the catalog; `metadata` vs `array` isolates the adapter read) or for one shape before
  and after an intervention.
- Observers: `pg_activity` says whether the database sees the concurrency (active
  backends flat at N → a pool in front of it; active tracking load with no wait events →
  database CPU); `tiled_metrics` pool gauges say whether tiled's own pools ever filled.

## Hygiene

- Never print the API key; `source` the env file in the same shell as the run.
- Report the wall-clock window of every run so the operator can correlate dashboards.
- `assert` coverage: if an observer was unreachable, the claim says so in `note`.
- Shapes make raw HTTP calls with no retries; errors are counted, never hidden.
