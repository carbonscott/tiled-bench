# ledger

What we know about the server, under which conditions, on what evidence. Written by
`tbench.ledger`, committed with the repo. Empty until the first real round; the smoke
runs during development used a scratch ledger on purpose.

- `claims.jsonl` — quantity, value, interval, fingerprint key, evidence run ids, status
- `interventions.jsonl` — every back-end change the operator reported, before/after, when
- `reported.json` — the operator-reported half of the current fingerprint
- `runs/<id>/` — params, fingerprint, per-rung summaries, every call, observer samples, coverage
