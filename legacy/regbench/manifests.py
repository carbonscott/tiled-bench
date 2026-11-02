"""Turn a :class:`~regbench.config.RunConfig` into the inputs ``register_dataset_http``
wants: ``(ent_df, art_df, base_dir, dataset_metadata)``.

Pipeline (all in a temp/cache dir keyed by the config):
    synth.write_synthetic_dataset  ->  <dir>/<label>.yml + <dir>/data/*.h5
    synth.run_generate (tcb generate) ->  <dir>/manifests/<label>/{entities,artifacts}.parquet
    pd.read_parquet                 ->  ent_df, art_df

The manifests only encode external pointers (file path + dataset path + shape/dtype), so a
10k-entity dataset is still KB on disk — see synth.py's module docstring for why. We cache
by config so a concurrency sweep (which reuses one dataset across worker counts) regenerates
the HDF5 + manifests exactly once.
"""

from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from ruamel.yaml import YAML

from .config import RunConfig
from .synth import run_generate, write_synthetic_dataset


@dataclass
class Manifest:
    """Everything ``register_dataset_http`` needs, plus provenance for the CSV."""

    ent_df: pd.DataFrame
    art_df: pd.DataFrame
    base_dir: str          # dir the HDF5 files live in (also the server --read root)
    dataset_metadata: dict
    yaml_path: Path
    manifest_dir: Path


# Cache keyed on the fields that change the on-disk data. write_path/location/max_workers
# do NOT affect the manifests, so a run varying only those reuses the same Manifest.
_CACHE: dict[tuple, Manifest] = {}


def _cache_key(cfg: RunConfig) -> tuple:
    return (cfg.layout, cfg.n_entities, cfg.artifacts_per_entity, cfg.shape_diverse)


def build_manifest(cfg: RunConfig, root: Path) -> Manifest:
    """Build (or fetch from cache) the manifest for ``cfg`` under ``root``.

    ``root`` is a workspace dir the caller owns (e.g. a scratch/temp dir). Each distinct
    dataset gets its own ``root/<label>/`` subtree.
    """
    key = _cache_key(cfg)
    if key in _CACHE:
        return _CACHE[key]

    label = f"synth_{cfg.layout}_n{cfg.n_entities}"
    out_dir = Path(root) / label
    out_dir.mkdir(parents=True, exist_ok=True)

    yaml_path = write_synthetic_dataset(
        out_dir=out_dir,
        layout=cfg.layout,
        n_entities=cfg.n_entities,
        artifacts_per_entity=cfg.artifacts_per_entity,
        shape_diverse=cfg.shape_diverse,
        label=label,
    )
    manifest_dir = run_generate(yaml_path)

    ent_df = pd.read_parquet(manifest_dir / "entities.parquet")
    art_df = pd.read_parquet(manifest_dir / "artifacts.parquet")

    # The dataset container's metadata = the YAML's metadata block. register_dataset_http
    # reads INHERITED_KEYS out of this; any missing key is simply skipped there.
    yaml = YAML()
    with open(yaml_path) as f:
        cfg_yaml = yaml.load(f)
    dataset_metadata = dict(cfg_yaml.get("metadata", {}))

    # base_dir: where the .h5 files are. The synth writer puts them in <out_dir>/data.
    base_dir = str(out_dir / "data")

    manifest = Manifest(
        ent_df=ent_df,
        art_df=art_df,
        base_dir=base_dir,
        dataset_metadata=dataset_metadata,
        yaml_path=yaml_path,
        manifest_dir=Path(manifest_dir),
    )
    _CACHE[key] = manifest
    return manifest
