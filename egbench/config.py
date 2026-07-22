"""Egress benchmark axes (``FetchConfig``) and the per-run CSV row (``FetchResult``).

One ``FetchConfig`` is a point in the sweep space; one ``FetchResult`` is a single CSV
row (one config × one rep). Layer decomposition mirrors the notebook + regbench:

    app    = server-side work (Server-Timing ``app;dur``, summed over data requests)
    net    = wire time (per-request wall − app, summed over data requests)
    nav    = metadata/search requests (client navigation — the raw-read path pays these)
    decode = client-side h5py parse of exported containers (export path only)

``app_frac`` = app_sum / (wall × concurrency): the share of total worker-time spent in
server work. Near 1.0 → the server is the ceiling; near 0 → the wire or client is.
"""

from dataclasses import dataclass, fields

METHODS = ("export_hdf5", "raw_read", "h5py_direct")
LOCATIONS = ("local", "remote")


@dataclass(frozen=True)
class FetchConfig:
    """One point in the egress sweep. Frozen/hashable so sweeps can dedupe configs."""

    dataset_key: str
    method: str
    location: str = "local"
    concurrency: int = 1
    batch_size: int = 20   # entity keys per export work-unit; ignored by raw_read/h5py_direct
    n_entities: int = 0    # entities to fetch this run; 0 = all registered

    def __post_init__(self):
        if self.method not in METHODS:
            raise ValueError(f"method {self.method!r} not in {METHODS}")
        if self.location not in LOCATIONS:
            raise ValueError(f"location {self.location!r} not in {LOCATIONS}")

    def label(self):
        return (f"{self.dataset_key} {self.method} {self.location} "
                f"c={self.concurrency} b={self.batch_size} n={self.n_entities or 'all'}")


@dataclass
class FetchResult:
    """One CSV row: config axes flattened, plus measured metrics.

    Field order here == CSV column order; add new columns at the end.
    """

    # --- axes ---
    dataset_key: str
    method: str
    location: str
    concurrency: int
    batch_size: int
    n_entities: int
    rep: int

    # --- dataset identity / provenance ---
    layout: str = ""
    mb_per_entity: float = 0.0     # decoded payload per entity, measured this run
    tiled_version: str = ""
    url: str = ""

    # --- headline ---
    wall_s: float = 0.0
    entities: int = 0              # entities actually fetched
    errors: int = 0                # failed work units (after retries)
    payload_mb: float = 0.0        # decoded array bytes
    wire_mb: float = 0.0           # HTTP Content-Length sum (container/raw bytes fallback)
    payload_mbps: float = 0.0      # payload_mb / wall_s — the useful-data rate
    wire_mbps: float = 0.0
    ent_per_s: float = 0.0

    # --- per-request latency (data requests: /full/ endpoints) ---
    req_p50_ms: float | None = None
    req_p95_ms: float | None = None
    req_p99_ms: float | None = None

    # --- layer decomposition ---
    app_p50_ms: float | None = None
    app_sum_s: float = 0.0
    net_sum_s: float = 0.0
    nav_sum_s: float = 0.0
    decode_s: float = 0.0
    app_frac: float | None = None

    @classmethod
    def from_config(cls, cfg: FetchConfig, rep: int) -> "FetchResult":
        return cls(
            dataset_key=cfg.dataset_key,
            method=cfg.method,
            location=cfg.location,
            concurrency=cfg.concurrency,
            batch_size=cfg.batch_size,
            n_entities=cfg.n_entities,
            rep=rep,
        )

    def finalize_rates(self):
        """Derive rates from counts + wall_s. Call after timing."""
        if self.wall_s > 0:
            self.payload_mbps = self.payload_mb / self.wall_s
            self.wire_mbps = self.wire_mb / self.wall_s
            self.ent_per_s = self.entities / self.wall_s
            if self.app_sum_s:
                self.app_frac = self.app_sum_s / (self.wall_s * self.concurrency)
        if self.entities:
            self.mb_per_entity = self.payload_mb / self.entities
        return self


def csv_fields():
    """Canonical CSV column order (all FetchResult fields, declaration order)."""
    return [f.name for f in fields(FetchResult)]
