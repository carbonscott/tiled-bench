Remote is where the fixes pay: 1.25–1.44× on every cell, with the large-n cells —
the ones that matter for real datasets — at 1.25× (batched n=1000, 30.1 s → 24.0 s)
and 1.31× (per_entity n=1000, 30.7 s → 23.5 s; 32.5 → 42.6 entities/s). The gain is
uniform across layouts because its dominant source is Fix A: the existence GET was one
of ~3 serial round-trips per entity, and on an RTT-bound path removing it is worth
~1.3× regardless of file layout. Fix B's HDF5-scan savings are secondary here (the
open is client-side and cheap next to the WAN round-trips), which is consistent with
per_entity gaining only slightly more than batched.

Two population notes. First, the remote server registers ~2.5× faster than the local
SQLite stack to begin with (33 vs 13 ent/s at n=1000 before): Postgres absorbs the 8
concurrent writers in parallel while SQLite serializes them — so the local sweep
under-states what these fixes do in production. Second, all remote rows are stamped
server_version 0.2.15b1.dev14+b7aafcd1 (the post-fix w16fix0811-era deployment); rows
from other deployments must not be pooled with these.

Extrapolation to the real datasets: at the measured after-rate (~42 ent/s), NiPS3-scale
per_entity (~7.6k entities) registers in ~3 minutes and a batched 10k in ~4 minutes,
versus ~4 and ~5 minutes before. The absolute ceiling is now the write-path
request rate (~85 write calls/s at call_p50 ≈ 60–70 ms and 8 workers), well under the
server's ~230 req/s read ceiling — worker count or higher client concurrency, not the
server, is the next lever if faster bulk loads are wanted.
