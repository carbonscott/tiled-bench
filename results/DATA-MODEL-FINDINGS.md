# Data model — query/discovery performance + design assessment (2026-08-19)

The Dataset → Entity → Artifact hierarchy with physics parameters as entity metadata,
evaluated on (a) how fast each discovery route answers "give me the entities where
p ≥ x, with locators", and (b) where the design helps or hurts. Raw rows:
`results/datamodel/{local_scaling,remote_routes}.csv`
(probes: `_bench_ws/datamodel/dm_{local_scaling,remote}.py`). Local = fresh SQLite
catalogs (synth entities, params p0/p1); remote = tiled-test
(server 0.2.15b1.dev14+b7aafcd1), `EGRESS_PE_1M_BROKER`, 1000 entities.

## The numbers

**Three discovery routes, same question (params + locators for a filtered subset):**

| Route | cost model | measured (1000-entity dataset, remote) |
|---|---|---|
| Parquet manifest (`pd.read_parquet` + filter) | O(file), ~ms | **4–5 ms** warm, 161 ms cold |
| Server-side `search(Key(p) >= x)` + sweep matches | **O(matches)** ~1.7–2.9 ms/match | 56 ms @ 10 · 167 ms @ 100 · 879 ms @ 500 |
| Full `items()` sweep, filter client-side (`query_catalog` pattern) | **O(catalog)** ~2.6 ms/entity | 2.63 s @ 1000 — identical local (2.66 s), so cost is server-side, not RTT |

**Catalog-size scaling (local, 10 → 10,000 entities):** the full sweep is dead-linear at
~2.6 ms/entity across three decades (26.6 s at 10k); `search()` cost tracks the *result*
size, not the catalog (0.24 s for 100 matches out of 10k); `len(dataset)` is one cheap
request (~0 ms after the first). The catalog's search indexes are doing their job —
selective physics-parameter queries stay fast as the catalog grows.

**Navigation primitives (remote):** first-100-keys page ~50 ms; entity metadata GET
66–176 ms; artifact metadata GET ~50 ms. Pagination amortizes per-entity metadata harvest
to ~2.6 ms/entity — 20–70× cheaper than the per-node GETs egress measured as the ~190 ms
"nav tax". Bulk metadata should always ride pagination, never per-node requests.

**Registration side (from the ingress campaign):** per-node registration cost is flat to
10k entities (12.7 vs 13.2 ent/s); the hierarchy does not degrade as it fills.

## Assessment

**What the design gets right**

- **Params-as-metadata makes physics queries first-class.** `search(Key("Ja_meV") >= x)`
  runs server-side, scales with matches, and needs no dataset-specific code — a direct
  payoff of the dataset-agnostic contract putting parameters at the top level of entity
  metadata rather than nesting them.
- **Locators-in-metadata make discovery single-pass.** Each entity's metadata carries
  `path_*/dataset_*/index_*`, so one paginated sweep (or one search) yields everything
  Mode A needs to open HDF5 directly — no per-entity follow-up requests. This is what
  keeps the harvest at ~2.6 ms/entity instead of ~190 ms.
- **The Parquet manifest is a 500× discovery floor.** For a known dataset it answers the
  same query in ~5 ms, and with the optimization branch recording `shape`/`dtype` it is
  fully self-describing — an ML pipeline can plan loads (shapes, dtypes, file paths)
  without touching either the server or the HDF5 files.
- **Flat registration and search scaling to 10k** means nothing in the model needs
  rethinking at current dataset sizes (7.6k–10k entities).

**Where it hurts, and what to do**

- **`query_catalog` unfiltered is O(catalog)** — 26 s on a 10k dataset, every time. The
  docstring already recommends pre-filtering with `search()`; measured, that is the
  difference between 26 s and sub-second for selective queries. Recommendation: make the
  search-first composition the documented default, and treat unfiltered `query_catalog`
  as an export tool, not a query tool.
- **Route selection is the data-model analogue of the egress crossover.** Rule of thumb:
  *known dataset, params-only question → parquet manifest (ms); selective cross-entity
  question or live catalog state → server search (∝ matches); full-catalog harvest →
  paginated sweep (~2.6 ms/entity), and consider just reading the manifest instead.*
- **The catalog and the manifest can drift.** Two sources of truth (parquet on disk,
  metadata in catalog) are the price of the 500× floor; `tcb register` is incremental and
  the manifest is the contract, so drift is bounded by re-registration discipline — but
  nothing *measures* drift today. A cheap `tcb verify` (row counts + spot metadata
  equality) would close the loop.
- **Per-node metadata GETs are expensive relative to their information content**
  (~50–170 ms each remote). Interactive tools should page; anything that loops
  `client[key]` over entities inherits the egress nav tax.

**Bottom line.** The two-mode design is validated by measurement: the catalog earns its
keep at *selective* discovery (search ∝ matches) and visualization (Mode B), while the
parquet manifest is the right backbone for ML-scale harvesting — and the fastest "query
engine" in the system by two orders of magnitude. The one behavior worth changing is
unfiltered full-catalog sweeps in library code.
