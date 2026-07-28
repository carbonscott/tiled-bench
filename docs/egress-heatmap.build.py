"""Regenerate the data embedded in docs/egress-heatmap.html from results/*.csv.

Run after appending new rows to the results CSVs:

    python docs/egress-heatmap.build.py

The page is fully self-contained (data inlined as `const DATA = [...]`), so it
opens directly in a browser — no server needed.

Aggregation rules:
- Regular CSVs: one record per (dataset, method, location, concurrency, batch, n),
  median across reps. n_processes = 1. smoke.csv is skipped.
- mp_* probe files: per-process rows of one multi-process probe combine into a
  single record with n_processes = N and rates summed across processes (the
  sum-of-overlapped-rates aggregate reported in results/EGRESS-FINDINGS.md).
"""
import csv, json, re, statistics
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
RESULTS = REPO / "results" / "stock-2026-07"   # frozen stock-era CSVs
BROKER = REPO / "results" / "broker"           # 2026-07-27 broker campaign
PAGE = REPO / "docs" / "egress-heatmap.html"

PROBES = {  # probe name -> (glob, n_processes)
    "local 4-proc export probe": ("mp_probe_p*.csv", 4),
    "remote 4-proc raw probe": ("mp_remote_p?.csv", 4),
    "remote 8-proc raw probe": ("mp_remote8_p*.csv", 8),
}

# failed runs may not record size/layout; patch from the registry
SIZES = {"EGRESS_PE_64K": 0.065536, "EGRESS_PE_1M": 1.048576,
         "EGRESS_PE_16M": 16.777216, "EGRESS_BAT_1M": 1.048576,
         "EGRESS_BAT_16M": 16.777216, "EGRESS_GRP_1M": 1.048576}
LAYOUTS = {"EGRESS_BAT_16M": "batched", "EGRESS_BAT_1M": "batched",
           "EGRESS_GRP_1M": "grouped"}

NUM = ["wall_s", "entities", "errors", "payload_mb", "payload_mbps", "wire_mbps",
       "ent_per_s", "req_p50_ms", "req_p95_ms", "req_p99_ms", "app_p50_ms",
       "app_sum_s", "net_sum_s", "nav_sum_s", "decode_s", "app_frac"]


def read_rows(path):
    with open(path) as f:
        for r in csv.DictReader(f):
            for k in NUM:
                r[k] = float(r[k]) if r[k] not in ("", None) else 0.0
            r["concurrency"] = int(r["concurrency"])
            r["batch_size"] = int(r["batch_size"])
            r["n_entities"] = int(r["n_entities"])
            r["mb_per_entity"] = float(r["mb_per_entity"])
            yield r


def med(rows, k):
    return statistics.median(r[k] for r in rows)


def base_record(rows, src):
    r0 = rows[0]
    rec = {
        "dataset": r0["dataset_key"], "method": r0["method"],
        "location": r0["location"], "conc": r0["concurrency"],
        "batch": r0["batch_size"], "n": r0["n_entities"], "nproc": 1,
        "layout": r0["layout"], "mb_ent": r0["mb_per_entity"],
        "reps": len(rows), "src": src,
    }
    for k in NUM:
        rec[k] = round(med(rows, k), 4)
    rec["errors"] = max(r["errors"] for r in rows)
    rec["failed"] = rec["entities"] == 0 or rec["errors"] > 0
    if rec["mb_ent"] == 0:
        rec["mb_ent"] = SIZES.get(rec["dataset"], 0)
        rec["layout"] = LAYOUTS.get(rec["dataset"], rec["layout"])
    return rec


def read_broker_rows(path):
    """Broker-campaign CSVs (results/broker/step*.csv): axes + measured values
    only — rates are derived here, never stored (runbook §5)."""
    with open(path) as f:
        for r in csv.DictReader(f):
            wall = float(r["wall_s"] or 0)
            ents = float(r["entities"] or 0)
            conc = int(r["concurrency"] or 1)
            out = {
                "dataset_key": r["dataset_key"], "method": r["method"],
                "location": r["location"], "concurrency": conc,
                "batch_size": int(float(r["export_batch"] or 0)) or 1,
                "n_entities": int(float(r["n_entities"] or 0)),
                "layout": r.get("layout", ""),
                "mb_per_entity": float(r["mb_per_entity"] or 0),
            }
            for k in ("wall_s", "entities", "errors", "payload_mb",
                      "req_p50_ms", "req_p95_ms", "req_p99_ms", "app_p50_ms",
                      "app_sum_s", "net_sum_s", "nav_sum_s", "decode_s"):
                v = r.get(k)
                out[k] = float(v) if v not in ("", None) else 0.0
            wire = float(r["wire_mb"] or 0)
            out["payload_mbps"] = out["payload_mb"] / wall if wall else 0.0
            out["wire_mbps"] = wire / wall if wall else 0.0
            out["ent_per_s"] = ents / wall if wall else 0.0
            out["app_frac"] = out["app_sum_s"] / (wall * conc) if wall else 0.0
            yield out


def build_records():
    records = []
    for f in sorted(BROKER.glob("*.csv")):
        groups = {}
        for r in read_broker_rows(f):
            key = (r["dataset_key"], r["method"], r["location"],
                   r["concurrency"], r["batch_size"], r["n_entities"])
            groups.setdefault(key, []).append(r)
        for rows in groups.values():
            records.append(base_record(rows, "broker/" + f.stem))

    for f in sorted(RESULTS.glob("*.csv")):
        if f.name.startswith("mp_") or f.name == "smoke.csv":
            continue
        groups = {}
        for r in read_rows(f):
            key = (r["dataset_key"], r["method"], r["location"], r["concurrency"],
                   r["batch_size"], r["n_entities"])
            groups.setdefault(key, []).append(r)
        for rows in groups.values():
            records.append(base_record(rows, f.stem))

    for name, (pat, nproc) in PROBES.items():
        rows = [r for f in sorted(RESULTS.glob(pat)) for r in read_rows(f)]
        if not rows:
            continue
        rec = base_record(rows, name)
        rec["nproc"] = nproc
        rec["reps"] = 1
        for k in ("payload_mbps", "wire_mbps", "ent_per_s"):
            rec[k] = round(sum(r[k] for r in rows), 1)
        rec["entities"] = sum(r["entities"] for r in rows)
        rec["payload_mb"] = round(sum(r["payload_mb"] for r in rows), 1)
        rec["wall_s"] = round(max(r["wall_s"] for r in rows), 2)
        rec["failed"] = False
        records.append(rec)
    return records


def main():
    records = build_records()
    data = json.dumps(records, separators=(",", ":"))
    html = PAGE.read_text()
    new_html, n = re.subn(r"const DATA = \[.*?\];", f"const DATA = {data};",
                          html, count=1, flags=re.S)
    if n != 1:
        raise SystemExit("could not find `const DATA = [...];` in the page")
    PAGE.write_text(new_html)
    print(f"{len(records)} records embedded into {PAGE.relative_to(REPO)}")


if __name__ == "__main__":
    main()
