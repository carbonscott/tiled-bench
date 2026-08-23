The combined fixes are safe but modest on the local stack: 1.05–1.38× by cell, with the
headline per_entity n=1000 at 1.18× (89.7 s → 76.0 s median) and the expected
gap-widens-with-N shape weakly present on per_entity (1.07× at n=100 → 1.18× at n=1000).
The n=10 cells are sub-second and dominated by fresh-server warmup; the apparent 0.79×
"regression" at per_entity n=10 is that noise, not a real cost.

Why the wins are small here: the local sweep is server-bound, not round-trip-bound.
app_dur_p50 is 135–190 ms per write POST and call_p50 ≈ app_dur_p50 throughout — nearly
all per-call wall time is the server serializing SQLite writes. Fix A removes a cheap
read (the existence GET does not hold the write lock) and Fix B removes client-side file
opens; neither shrinks the serialized server write work that sets the pace at ~12–14
entities/s (~25 nodes/s). The remote server is the environment these fixes were designed
for — there the existence GET costs a full WAN round-trip per entity — so the remote
before/after is the decisive measurement (results in remote_before/after.csv).

One real discovery came out of the first (crashed) after-run: the extra write pressure
from Fix A triggered SQLite "database is locked" 500s, whose transparent client retries
collided as 409s — the exact mechanism behind the registration race the egress campaign
documented (structure-less entities on the shared server). The branch now resumes on a
409 container collision instead of aborting (fetch the existing container, register its
artifacts), which converts that documented failure mode from silent data damage into a
self-healing retry; the re-run passed 26/26 rows clean under the same pressure.

Scaling with catalog fill: a separate probe registered 10,000 entities into one catalog
at 12.7 ent/s — statistically the same rate as n=1000 (13.2) — so per-node registration
cost stays flat at least to 10k on SQLite; a flat extrapolation to the real datasets
(NiPS3 ≈7.6k per_entity, batched ≈10k) is reasonable at this scale.
