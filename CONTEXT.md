# Tiled Registration Benchmark

Characterization and optimization profiling of the **HTTP registration path** into a
Tiled catalog server. Lives in `tiled-bench` alongside the reading benchmark
(`bandwidth_benchmark.py`). Drives the raw Tiled client; copies (does not import) the
broker write-path setup from `tiled-catalog-broker`.

## Language

**Registration**:
Creating catalog nodes in a running Tiled server via the HTTP client — one
`create_container` per entity plus one `.new()` per artifact. The write path under test.
_Avoid_: ingest, upload.

**Entity**:
A scientific sample/simulation instance. Becomes one Tiled **container** node holding its
parameters as metadata.

**Artifact**:
A named array belonging to an entity (e.g. a spectrum). Becomes one Tiled **array** node
with an external `DataSource` pointing at an HDF5 file.
_Avoid_: "dataset" (overloaded — see below).

**Dataset**:
The top-level container grouping all entities of one collection (e.g. VDP), keyed by
`dataset_key`. Collides with HDF5's "dataset" (an array in a file) — qualify when ambiguous.

**Shared artifact**:
A 1-D axis array (e.g. `energies`, `H`, `eloss`) common to every entity in a dataset,
registered once per dataset rather than per entity.

**Layout**:
How entities map onto HDF5 files. This is the primary driver of the server-side
**asset-dedup** path (`_put_asset`, keyed on `data_uri`):
- **per_entity** — one file per entity → distinct asset per entity → asset **INSERT** heavy (worst case).
- **batched** — entities stacked on axis-0 of shared datasets, sliced by `index` → one shared asset → INSERT once, SELECT-hit after (dedup heavy).
- **grouped** — one HDF5 group per entity inside one file → one shared asset, distinct dataset-path parameters per entity.

**Write-path**:
The artifact mimetype/adapter variant — **stock** (`application/x-hdf5`) vs **broker**
(`application/x-hdf5-broker`, lazy slicing). Determines the *read* adapter; expected **not**
to affect registration cost.

**Parameter location**:
Where the generator reads entity params from in HDF5 (`root_scalars`, `root_attributes`,
`group`, `group_scalars`, `manifest`). A *generation* detail only — by registration time all
collapse into the entity metadata dict, so **not a benchmark axis**.

**Layer decomposition**:
The five cost layers the benchmark attributes time to: (1) HDF5 probe, (2) client prep,
(3) Tiled internals (`create_node`), (4) network RTT, (5) Postgres backend.

**Local vs remote**:
**local** = SQLite `tiled serve` subprocess on the dev box (RTT≈0, py-spy-able, profiling home);
**remote** = Postgres test host (real RTT + real backend, headline numbers).

**White-box**:
Attributing where server-side time goes — primarily **py-spy** on the local server PID plus
**black-box differentials** (containers-only vs +artifacts; closure-trigger on/off via
`DROP TRIGGER` on the SQLite file). In-process direct `create_node` + cProfile is an optional
deepening, used only if py-spy proportions are too coarse.

## Relationships

- A **Dataset** contains many **Entities**; an **Entity** contains many **Artifacts** and references shared **Shared artifacts**.
- **Layout** determines the asset-dedup path (and node count); **Write-path** determines the read adapter, not the write cost.
- Each artifact `create_node` = two DB commits + structure hash + structure/asset dedup + a refresh; each entity container = one commit. The **closure-table trigger** fires on every node insert.

## Example dialogue

> **Q:** "If a dataset is `batched`, does each artifact still cost a fresh asset insert?"
> **A:** "No — every entity's artifact points at the same file `data_uri`, so the asset is
> inserted once and every later entity is a SELECT-hit. That's why `batched` registers cheaper
> per node than `per_entity`, where each entity has its own file and forces a new asset INSERT."

## Flagged ambiguities

- "dataset" means both the **Dataset** container (collection) and an HDF5 array — always qualify.
- "size" was used loosely: array **element count** is irrelevant to registration cost (external
  management stores only shape/dtype); the real size levers are **node count** (driven by layout)
  and **metadata width** (JSON/GIN cost on Postgres).
