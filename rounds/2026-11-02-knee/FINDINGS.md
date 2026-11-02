# Where is the knee, and what holds the slot?

**Status: protocol written and smoke-tested; the ladder itself has not run.** `run.py`
is the protocol from `bench-knee`; the records go here when it runs on an exclusive node
against tiled-test.

**What it will settle.** The prior campaign (`results/EGRESS-FINDINGS.md`, steps 7–7f)
put peak throughput at ~230 req/s and called it a "~20-slot" ceiling, but in-flight
grew linearly with offered load on a doubling ladder (11, 20, 40, 65, 80), so the 20 was
where P=8 happened to land. This round resolves the knee with a thread ladder in steps
of 1–4, splits each request into `tok/acl/read/pack/app`, and watches the database.

**Smoke (2026-11-02, `smoke/`, interactive node sdfiana007, 2 procs × 1–2 threads,
20 entities, not a measurement).** The path works end to end: `meta.json` carries the
live server version (`0.2.10b2.dev493`, not the `0.2.15b1.dev14` of August; the
deployment has changed again and the ladder must be read against this one), every call
record carries `tok`, `read`, `pack` and `app` (no `acl` segment on this server
version), and `in_flight` tracks offered concurrency as expected (1.3 at 2, 3.2 at 4).
Two things to carry into the real run: this API key lacks the `metrics` scope, so the
pool gauges are `_unavailable` until the admin grants it or reads them off the
dashboard; and single-thread units on the shared server stalled for 2.6–5.7 s on 8 of
20 entities, the tail-stall hazard noted in 2026-07, which an exclusive node and
`n=1000` will average out but which `units.jsonl` will show per unit.

**Window:** —
**Answer:** —
**Open:** —
