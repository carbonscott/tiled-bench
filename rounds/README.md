# rounds

One directory per round of measurement, named `<date>-<slug>`, where the slug is the
question (`knee`, `reads-whole-entity`, `register-batched`, `epoch-nips3`).

Each holds:

- `run.py` — the script that produced the records. Short, written against `tbench`,
  committed as run. If it needed a SLURM wrapper, that too.
- `meta.json` — written by `load.Round`: date, host, git sha, client and server version.
- `<label>.summary.json`, `<label>.units.jsonl`, `<label>.calls.jsonl` — one set per
  `load.run`. Optional `pg.jsonl` from `probes.PgSampler`.
- `FINDINGS.md` — the question in one sentence, the answer in one paragraph, the one
  table that shows it, the wall-clock window, what was left open.

A round answers one question. A second question is a second round, even on the same day.
