"""Benchmark axes (``RunConfig``) and the per-run metrics row (``RunResult``).

One ``RunConfig`` is a point in the sweep space defined in BENCHMARK-PLAN.md; one
``RunResult`` is a single CSV row (one config × one rep). The percentile fields are
declared now but left ``None`` — they are filled once the per-call / ``app;dur``
instruments land (see runner.py ``_INSTRUMENT SEAM``). Keeping them in the schema from
the start means the CSV columns never move.
"""

from dataclasses import dataclass, field, fields

LAYOUTS = ("per_entity", "batched", "grouped")
LOCATIONS = ("local", "remote")
WRITE_PATHS = ("stock", "broker")

# The write_path axis IS the artifact mimetype. create_data_source (http_register.py:108)
# reads art_row["mimetype"]; absent it, TCB defaults to the broker adapter, which a vanilla
# `tiled serve catalog` cannot read (415). The runner stamps this column per run.
WRITE_PATH_MIMETYPES = {
    "stock": "application/x-hdf5",
    "broker": "application/x-hdf5-broker",
}


@dataclass(frozen=True)
class RunConfig:
    """One point in the benchmark sweep. Frozen so it can key a manifest cache."""

    layout: str
    n_entities: int
    location: str = "local"
    write_path: str = "stock"
    max_workers: int = 8
    artifacts_per_entity: int = 1
    shape_diverse: bool = False

    def __post_init__(self):
        if self.layout not in LAYOUTS:
            raise ValueError(f"layout {self.layout!r} not in {LAYOUTS}")
        if self.location not in LOCATIONS:
            raise ValueError(f"location {self.location!r} not in {LOCATIONS}")
        if self.write_path not in WRITE_PATHS:
            raise ValueError(f"write_path {self.write_path!r} not in {WRITE_PATHS}")

    def label(self):
        """Short human/file-safe tag, e.g. ``per_entity_n1000_local_w8``."""
        return f"{self.layout}_n{self.n_entities}_{self.location}_w{self.max_workers}"


@dataclass
class RunResult:
    """One CSV row: the config axes flattened, plus measured metrics.

    ``field order here == CSV column order`` (see ``csv_fields``). Add new columns at the
    end so existing CSVs stay readable.
    """

    # --- axes (flattened from RunConfig) ---
    layout: str
    n_entities: int
    location: str
    write_path: str
    max_workers: int
    rep: int

    # --- identity / provenance ---
    dataset_key: str = ""
    tiled_version: str = ""
    tcb_sha: str = ""

    # --- throughput (wall-clock) ---
    wall_s: float = 0.0
    entities: int = 0
    artifacts: int = 0
    skipped: int = 0
    art_failed: int = 0
    entities_per_s: float = 0.0
    artifacts_per_s: float = 0.0
    nodes_per_s: float = 0.0

    # --- per-call latency (filled by the per-call instrument; None until then) ---
    call_p50_ms: float | None = None
    call_p95_ms: float | None = None
    call_p99_ms: float | None = None

    # --- server self-reported work, from Server-Timing: app;dur (None until then) ---
    app_dur_p50_ms: float | None = None
    app_dur_p95_ms: float | None = None

    @classmethod
    def from_config(cls, cfg: RunConfig, rep: int) -> "RunResult":
        return cls(
            layout=cfg.layout,
            n_entities=cfg.n_entities,
            location=cfg.location,
            write_path=cfg.write_path,
            max_workers=cfg.max_workers,
            rep=rep,
        )

    def finalize_rates(self):
        """Derive the per-second rates from counts + wall_s. Call after timing."""
        if self.wall_s > 0:
            self.entities_per_s = self.entities / self.wall_s
            self.artifacts_per_s = self.artifacts / self.wall_s
            self.nodes_per_s = (self.entities + self.artifacts) / self.wall_s
        return self


def csv_fields():
    """Canonical CSV column order (all RunResult fields, declaration order)."""
    return [f.name for f in fields(RunResult)]
