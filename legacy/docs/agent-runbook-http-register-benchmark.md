# Agent Runbook — Benchmark the HTTP-register optimizations (before vs after)

**You are an agent tasked with measuring the registration-path optimizations end to end.**
Run the local `regbench` harness against the *current* TCB code (baseline), make the
optimization changes on a new TCB branch, run the harness again, and publish a single
self-contained HTML report comparing the two.

Read `BENCHMARK-PLAN.md` and `docs/adr/0001-import-tcb-for-registration-benchmark.md` first
— they define the layers and the fixes you are measuring.

---

## 0. Environment (do not deviate)

- **Interpreter (always use this):** `/home/ajshack/tiled-catalog-broker/.venv/bin/python`
  — call it `PY`. It has `tiled`, `tiled_catalog_broker` (editable), `h5py`, `pandas`.
- **Harness repo / cwd:** `/home/ajshack/tiled-bench` (contains the `regbench` package).
- **TCB repo (the code under test):** `/home/ajshack/tiled-catalog-broker`.
  It is installed **editable**, so checking out a branch there changes what the harness
  imports — **no reinstall needed**.
- **Scratch/workspace root:** `/home/ajshack/tiled-bench/_bench_ws` (fresh dirs per run).
- **Results dir:** `/home/ajshack/tiled-bench/results/` (create it).

Two separate git repos are in play. **The optimization branch is created in TCB**, not in
tiled-bench. The harness stays on its current branch the whole time.

---

## 1. Measurement protocol (identical for before and after)

Run the **scale sweep** (`regbench scale`) so you capture a *curve*, not a single point —
time-to-register vs `n_entities` per layout, before and after. Two layouts, `local`, `stock`
write-path, `max_workers=8`:

| Layout | Why |
|---|---|
| `per_entity` | Maximizes both costs: one HDF5 file per entity (HDF5-scan heavy) + one existence-GET per entity. This is where the win shows, and where it should grow with N. |
| `batched` | Control: one shared file (scan cost near-zero). Isolates the existence-GET fix from the HDF5-scan fix. |

**Sizes `10,100,1000`.** The sweep launches a **fresh-DB server per (layout, size)** on its
own, so a big run never pollutes a small one — no manual per-rep looping needed. Reps follow
the plan schedule automatically (5 @≤100, 3 @≤1k). One command does the whole grid:

```bash
cd /home/ajshack/tiled-bench
PY=/home/ajshack/tiled-catalog-broker/.venv/bin/python
run_sweep () {   # $1 = out.csv
  "$PY" -m regbench.cli scale \
    --workspace "_bench_ws" --out "$1" \
    --layouts per_entity,batched --sizes 10,100,1000 --workers 8 --location local
}
```

It prints a scaling summary table at the end and appends one CSV row per (layout, size, rep)
with all metrics (throughput, `call_p50/p95/p99_ms`, `app_dur_p50/p95_ms`) **and** the
`tcb_sha` + `tiled_version` — so the CSV is self-documenting about which code produced it.

> **Runtime:** the `per_entity n=1000` point is ~200s/rep at the unoptimized baseline
> (~5 ent/s local), so the full before sweep is ~20–30 min; the after sweep is faster. If you
> need it quicker, drop `--sizes` to `10,100,500` or add `--reps 2`. **Do not** add `n=10000`
> — at baseline rates it's a multi-hour run (that's a one-off, not part of this before/after).
> If a rep spans a machine suspend, its `wall_s` may look absurd — discard and re-run it.

### Correctness guardrail (MANDATORY — a faster-but-wrong result is a failed task)

An optimization that skips work can "win" by registering fewer or malformed nodes. For
**every** run:

1. The harness prints `[warn] N entities landed, expected M` on any shortfall, and TCB
   prints `Artifact errors: K` per config. **Every config must show `entities == n`,
   `artifacts == n`, 0 errors, no `[warn]`.** If not, stop and report. (Verify from the CSV:
   every row's `entities` and `artifacts` must equal its `n_entities`.)
2. After the *after* runs, do one read-back sanity check (Fix B changes how shape/dtype are
   sourced): register one small dataset, then confirm an artifact reads back with the
   correct shape/dtype:
   ```bash
   "$PY" - <<'EOF'
   # connect to a just-registered dataset, pull one array, assert shape/dtype are sane
   # (see regbench/runner.py make_client + the dataset_key printed by smoke)
   EOF
   ```
   If the after-branch produces different shapes/dtypes than before, the speedup is invalid.

---

## 2. Baseline run ("before")

```bash
cd /home/ajshack/tiled-catalog-broker
git status                      # MUST be clean; if not, stop and report
git rev-parse --short HEAD      # record this — it is the baseline SHA
```

Then, from `/home/ajshack/tiled-bench`:

```bash
rm -rf _bench_ws results/before.csv
mkdir -p results
run_sweep results/before.csv
```

Verify every row satisfies the correctness guardrail (`entities == n_entities`,
`artifacts == n_entities`, no `[warn]`). Keep `results/before.csv`.

---

## 3. Branch + apply the optimizations (in TCB)

```bash
cd /home/ajshack/tiled-catalog-broker
git checkout -b optimize/http-register-perf
```

Apply **both** fixes below (this combined state is "after"). Follow the user's global code
guideline: **no `try/except` unless a failure is genuinely expected — and comment why.**

### Fix A — skip the existence-GET (layer 2, pure `http_register.py`)

`_register_one_entity` (around `src/tiled_catalog_broker/http_register.py:144`) does:

```python
if ent_key in parent_client:      # <-- issues an HTTP GET per entity; a 404 on fresh loads
    return (0, 0, 1, 0)
```

On a fresh load every entity pays a wasted round-trip. Add an `assume_new: bool = False`
parameter to **both** `register_dataset_http` and `_register_one_entity`; thread it through
the `executor.submit(...)` call. When `assume_new` is True, skip the `if ent_key in
parent_client` block entirely. The harness always uses fresh `dataset_key`s, so it is safe
to pass `assume_new=True`.

> To let the harness turn it on, add a `assume_new=True` kwarg where `runner.py` calls
> `register_dataset_http` — OR default it True on the branch for the benchmark. Note in the
> report which you chose.

### Fix B — kill the HDF5 scan (layer 1, `generate.py` + `http_register.py`)

`create_data_source` (`http_register.py:83`) calls `get_artifact_info(...)`, which opens the
HDF5 file with `h5py.File` to read shape/dtype. For `per_entity` that is **one file open per
entity** (the cache key includes the file path). The manifest can carry this instead.

1. **`tools/generate.py`** — when building each artifact row, also record `shape` (list of
   ints) and `dtype` (str). The generator already reads the HDF5 during manifest creation,
   so this is nearly free there.
2. **`create_data_source`** — prefer the manifest values, fall back to the scan:
   ```python
   if "shape" in art_row.index and pd.notna(art_row.get("shape")):
       data_shape = list(art_row["shape"])
       data_dtype = np.dtype(art_row["dtype"])
   else:
       # Fallback: manifest predates shape/dtype columns — read from HDF5 (old path).
       data_shape, _, _, _ = get_artifact_info(base_dir, h5_rel_path, dataset_path, index)
       data_dtype = np.float64
   ```
   Preserve the existing `index`-slicing semantics (`get_artifact_info` drops axis 0 when
   `index` is set — the manifest `shape` must match, i.e. store the **per-entity** shape).

The harness regenerates manifests for every run, so once `generate.py` writes the columns the
"after" runs will exercise the fast path automatically. `runner.py` clears the
`get_artifact_info` cache each run, so the removed `h5py.File` opens are a real, measured
delta — not a caching artifact.

Commit the changes:
```bash
git add -A && git commit -m "perf(register): skip existence-GET + source shape/dtype from manifest"
git rev-parse --short HEAD    # record — this is the after SHA
```

---

## 4. After run

From `/home/ajshack/tiled-bench` (TCB branch still checked out):

```bash
rm -rf _bench_ws results/after.csv
run_sweep results/after.csv
```

Same guardrail on every row (`entities == n_entities`, `artifacts == n_entities`, no warnings,
0 artifact errors). Then run the read-back sanity check from §1. Confirm `before.csv` and
`after.csv` carry **different `tcb_sha`** values (proves you measured two distinct code states).

---

## 5. HTML report

Write a **single self-contained** file to
`/home/ajshack/tiled-bench/results/http_register_before_after.html` (inline CSS; no external
assets). It must contain:

1. **Header / methodology** — date, `tiled_version`, before SHA, after SHA, the layouts,
   sizes, reps schedule, "local SQLite, fresh DB per (layout, size), stock write-path,
   workers=8", and a one-paragraph description of the two fixes with their layer numbers.
2. **Scaling curves — the centerpiece.** For each layout, a line chart of `wall_s` (median
   over reps) vs `n_entities`, with a **before** line and an **after** line on the same axes
   (inline SVG or `<canvas>` + inline `<script>` — no CDN). This is the "time to register N"
   story: the after-line should sit below the before-line, and the *gap should widen with N*
   for `per_entity` (the HDF5-scan cost is per-file, so it grows with entity count). Add a
   second pair of curves for `entities_per_s` vs `n_entities` to show whether throughput holds
   or degrades as the catalog fills.
3. **Per-size comparison table**, one section per layout, one row per size. Aggregate reps
   with the **median**; show before / after / absolute delta / **% change** for:
   `wall_s`, `entities_per_s`, `nodes_per_s`, `call_p50_ms`, `app_dur_p50_ms`. Color %
   improvements green, regressions red.
4. **Interpretation** — 3–5 sentences. Expected story: `per_entity` improves substantially and
   the gain *grows with N* (both fixes bite: file opens + existence-GETs, both per-entity);
   `batched` improves mostly from the existence-GET fix only (one shared file, little scan
   cost to remove). Note that on local the `call_p50 ≈ app_dur_p50` relationship means the win
   is real *server-side* work removed, not just client-side. Call out the extrapolation to the
   real datasets (per_entity nips3 ≈7.6k, batched ≈10k) with the caveat that per-node cost may
   creep as the catalog grows (closure-table + GIN), so a flat extrapolation is optimistic.
5. **Correctness footer** — state explicitly that every row matched (`entities`/`artifacts` ==
   `n_entities`, 0 errors) in both before and after, and that the read-back check passed.
   Without this the numbers are not trustworthy.

Build the tables/charts by reading the two CSVs with pandas in `$PY`; do not hand-transcribe
numbers.

---

## 6. Deliverables (report these back)

- `results/before.csv`, `results/after.csv` (raw rows).
- `results/http_register_before_after.html` (the report).
- The TCB branch `optimize/http-register-perf` with the committed changes.
- A short summary message: baseline SHA, after SHA, and the headline `per_entity` result at
  the largest size (n=1000) — `wall_s` and `entities/sec` before→after with the % gain, plus
  whether the gain widened with N — and confirmation the correctness guardrail passed.

**Do not** merge or push the TCB branch, and **do not** touch the tiled-bench branch. Leave
the TCB repo on the new branch when done.
```
