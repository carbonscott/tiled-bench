# Import tiled-catalog-broker rather than keep the registration benchmark standalone

The registration benchmark imports `tiled_catalog_broker` directly — using `tcb generate`
for manifests and `register_dataset_http` for the registration path — instead of vendoring a
copy of the write-path into tiled-bench. The only registration code original to tiled-bench is
a small synthetic-HDF5 writer for the three layouts.

## Context

The seeding handoff (REGISTRATION-BENCHMARK-HANDOFF.md §2.3–2.4) deliberately specified **no TCB
import**: drive the raw Tiled client and *copy* the broker write-path, so the bench was a
"native Tiled registration benchmark." That made sense when the goal was characterizing Tiled
itself.

Baseline measurements changed the goal. They showed the dominant, most-actionable costs live in
**TCB's client-side code**, not Tiled internals:
- per_entity registration spends **48% of wall-clock in `get_artifact_info`** (HDF5 shape/dtype
  reads, re-opening each file once per artifact because the cache key includes the dataset path);
- every entity pays a wasted **existence-check GET → 404** on fresh loads;
- registration is serial (`concurrency=1`), leaving the parallelism win on the table.

To benchmark fixes to that code, the bench must run that code. Reimplementing the manifest
contract and registration path just to stay "standalone" would duplicate ~hundreds of lines of
TCB and drift out of sync.

## Decision

Import TCB. Use `tcb generate` (authoritative manifest contract) and `register_dataset_http`
(the real registration path). Keep a **native raw-client variant** as a control: the difference
`real_path − native_path` isolates the client-side HDF5 probe (layer 1).

## Consequences

- tiled-bench now depends on tiled-catalog-broker being importable (it already is in this
  environment; both are sibling checkouts).
- The optimization experiments (HDF5 cache key, skip-existence-GET, parallelization) are run by
  mutating TCB's code and re-benchmarking — the bench and the PR4 fixes are intentionally coupled.
- Every profile CSV row records the TCB git SHA (and Tiled version), so a run is traceable to the
  exact code that produced it — which is what makes "diff the CSV across a version bump" a
  sufficient regression signal without a committed baseline (see BENCHMARK-PLAN.md).
