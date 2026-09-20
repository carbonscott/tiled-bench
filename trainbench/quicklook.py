"""Quick-look aggregation of trainbench CSVs: median samples/sec per cell,
guardrail-filtered, printed as a ladder table. Not the findings chart — a sanity lens.

Usage: quicklook.py results/trainbench/edrixs.csv
"""

import sys

import pandas as pd

pd.set_option("display.width", 200)


def main(path):
    df = pd.read_csv(path)
    n_all = len(df)
    bad = df[df.guardrail_ok != 1]
    df = df[df.guardrail_ok == 1]
    print(f"{path}: {n_all} rows, {len(bad)} failed guardrails (excluded from claims)")
    if len(bad):
        print(bad[["ts", "path", "cache_state", "workers", "errors", "notes"]]
              .to_string(index=False))

    df["cell"] = (df.path + "/" + df.cache_state
                  + df.rdcc_mb.map(lambda r: f"/rdcc{r:g}" if r and r != 1.0 else ""))
    pivot = df.pivot_table(index="cell", columns="workers", values="samples_per_s",
                           aggfunc="median").round(1)
    print("\nmedian samples/sec by workers:")
    print(pivot.to_string())

    mb = df.pivot_table(index="cell", columns="workers", values="mb_per_s",
                        aggfunc="median").round(0)
    print("\nmedian MB/s by workers:")
    print(mb.to_string())

    disc = df[df.discovery_route != "cached"].groupby(["path", "discovery_route"])[
        "discovery_s"].median().round(2)
    if len(disc):
        print("\ndiscovery (uncached) seconds:")
        print(disc.to_string())

    reps = df.groupby("cell")["samples_per_s"].agg(["count", "median", "min", "max"])
    spread = ((reps["max"] - reps["min"]) / reps["median"]).round(2)
    wide = spread[spread > 0.3]
    if len(wide):
        print("\ncells with >30% rep spread (check before quoting):")
        print(wide.to_string())


if __name__ == "__main__":
    for p in sys.argv[1:]:
        main(p)
        print()
