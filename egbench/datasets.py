"""Registry of egress benchmark datasets — the size × layout points.

Data creation/registration is deliberately NOT part of this package: datasets are
generated once (recipe in docs/agent-runbook-egress-benchmark.md) and sit under
``data-source/``. The registry records what the runner needs at fetch time: where the
bytes live (for ``h5py_direct``), which artifact to fetch, and the layout for slicing.
``mb_per_entity`` is the nominal decoded size — the correctness guardrail reference.
"""

from dataclasses import dataclass
from pathlib import Path

DATA_ROOT = Path("/sdf/data/lcls/ds/prj/prjmaiqmag01/results/data-source")
EGRESS_ROOT = DATA_ROOT / "egress_bench"
SERVER_PREFIX = "/prjmaiqmag01/data-source"  # shared-server mount prefix (registration recipe)
DEFAULT_REMOTE_URL = "https://lcls-data-portal.slac.stanford.edu/tiled-test"


@dataclass(frozen=True)
class DatasetSpec:
    key: str            # Tiled dataset key (top-level container)
    layout: str         # per_entity | batched | grouped
    data_dir: Path      # host dir containing the .h5 files
    artifact: str       # artifact node name to fetch
    h5_path: str        # HDF5 dataset path (grouped: relative to the entity group)
    n_entities: int
    mb_per_entity: float
    entity_group_fmt: str = "samples/sample_{i:06d}"  # grouped only

    def files(self):
        return sorted(self.data_dir.glob("*.h5"))


def _egress(name, layout, n, mb):
    return DatasetSpec(
        key=name.upper(), layout=layout, data_dir=EGRESS_ROOT / name / "data",
        artifact="signal", h5_path="signal", n_entities=n, mb_per_entity=mb,
    )


REGISTRY = {s.key: s for s in [
    # Synthetic egress family: single float64 'signal' artifact, random values
    # (incompressible so gzip can't fake the wire numbers). MB = 1e6 bytes.
    _egress("egress_pe_64k",  "per_entity", 1000, 0.065536),   # (8192,)
    _egress("egress_pe_1m",   "per_entity", 1000, 1.048576),   # (256, 512)
    _egress("egress_pe_16m",  "per_entity",  256, 16.777216),  # (1024, 2048)
    _egress("egress_bat_1m",  "batched",    1000, 1.048576),
    _egress("egress_bat_16m", "batched",     256, 16.777216),
    _egress("egress_grp_1m",  "grouped",    1000, 1.048576),
    # Broker-mimetype variant of egress_bat_1m (first 50 entities, remote only):
    # same file, application/x-hdf5-broker adapter — lazy slicing vs stock's
    # read-the-whole-stacked-dataset. Measured 33x faster per entity.
    DatasetSpec(key="EGRESS_BAT_1M_BROKER", layout="batched",
                data_dir=EGRESS_ROOT / "egress_bat_1m" / "data", artifact="signal",
                h5_path="signal", n_entities=50, mb_per_entity=1.048576),
    # Pre-existing benchmark sets (never regenerate these).
    DatasetSpec(key="BENCH_LARGE", layout="per_entity",
                data_dir=DATA_ROOT / "bench_large", artifact="powder",
                h5_path="powder", n_entities=1000, mb_per_entity=1.048576),
    DatasetSpec(key="VERY_LARGE_DATASET_BENCHMARK", layout="per_entity",
                data_dir=DATA_ROOT / "bench_super_large", artifact="spectral_cube",
                h5_path="spectral_cube", n_entities=200, mb_per_entity=5.04),
]}
