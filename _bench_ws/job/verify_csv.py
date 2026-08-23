"""Guardrail: every row must satisfy entities == n_entities == artifacts, no failures.

A faster-but-wrong row is a failed run (runbook §1). artifacts_per_entity is 1 in
this campaign, so artifacts must equal n_entities too.
"""
import sys

import pandas as pd

df = pd.read_csv(sys.argv[1])
bad = df[(df.entities != df.n_entities) | (df.artifacts != df.n_entities) | (df.art_failed > 0)]
print(f"[guardrail] {sys.argv[1]}: {len(df)} rows, {len(bad)} violations; "
      f"tcb_sha={sorted(df.tcb_sha.unique())}")
if len(bad):
    print(bad.to_string())

# Failed reps are kept in the CSV and excluded from claims; the run is only
# unusable if some (layout, size) config has no clean rep at all.
clean = df.drop(bad.index)
missing = (set(map(tuple, df[["layout", "n_entities"]].drop_duplicates().values))
           - set(map(tuple, clean[["layout", "n_entities"]].drop_duplicates().values)))
if missing:
    print(f"[guardrail] FATAL: configs with zero clean reps: {sorted(missing)}")
    sys.exit(1)
