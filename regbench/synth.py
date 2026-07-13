"""Synthetic HDF5 + YAML generator for the three Tiled registration layouts.

Writes tiny HDF5 files shaped like the canonical TCB layout examples
(``per_entity`` / ``batched`` / ``grouped``) plus a dataset YAML, so ``tcb generate``
can produce manifests with no real multi-GB data.

Why 1-element arrays are enough: registration stores only an external pointer
(file path + dataset path + shape/dtype), never the array bytes, so registration cost
is independent of element count. A 10k-entity synthetic dataset is therefore KB on disk
yet exercises the full per-node create path. See CONTEXT.md ("size") and BENCHMARK-PLAN.md.

The writer matches what each generator in ``tiled_catalog_broker.tools.generate`` reads:
  - per_entity: one file per entity; scalar params at root; artifact datasets at root.
  - batched:    one file; artifacts stacked on axis-0 (row i = entity i); params in /params.
  - grouped:    one file; /samples/sample_NNNN groups; per-group artifacts + params/ scalars.
"""

from pathlib import Path

import h5py
import numpy as np
from ruamel.yaml import YAML

from tiled_catalog_broker.utils import slugify_key

# Distinct shapes cycled when ``shape_diverse`` is set, so the server-side structure
# dedup path does real inserts (distinct structure ids) instead of the all-identical
# best case where every structure after the first is an ON CONFLICT no-op.
_DIVERSE_SHAPES = [(1,), (2,), (3,), (4,), (2, 2)]

# Valid controlled-vocabulary metadata so the YAML passes schema validation with no
# soft-warnings: method/material/data_type/producer are all canonical ids from the
# catalog model (data_type 'simulation' also requires a producer to be set).
_BASE_METADATA = {
    "method": ["RIXS"],
    "data_type": "simulation",
    "material": "NiPS3",
    "producer": "edrixs",
}

# Two physics-like params, varied per entity so the content-addressed UID is unique
# per entity (identical params would collapse to one UID and drop entities).
_PARAM_NAMES = ("p0", "p1")


def _artifact_shape(idx, base_shape, diverse):
    return _DIVERSE_SHAPES[idx % len(_DIVERSE_SHAPES)] if diverse else base_shape


def _params_for(i):
    """Per-entity parameter values; distinct across entities for unique UIDs."""
    return {"p0": float(i), "p1": float(i) * 0.5 + 1.0}


def _array(shape):
    return np.zeros(shape, dtype=np.float64)


def _write_yaml(path, layout, data_dir, file_pattern, artifact_types, params_section):
    cfg = {
        "key": slugify_key(path.stem),
        "label": path.stem,
        "metadata": dict(_BASE_METADATA),
        "data": {
            "directory": str(data_dir),
            "layout": layout,
            "file_pattern": file_pattern,
        },
        "artifacts": [{"type": t, "dataset": d} for t, d in artifact_types],
        "parameters": params_section,
    }
    yaml = YAML()
    yaml.default_flow_style = False
    with open(path, "w") as f:
        yaml.dump(cfg, f)
    return path


def _write_per_entity(data_dir, n_entities, n_artifacts, base_shape, diverse):
    for i in range(n_entities):
        with h5py.File(data_dir / f"entity_{i:06d}.h5", "w") as f:
            for name, val in _params_for(i).items():
                f[name] = val  # 0-dim scalar param at root
            for a in range(n_artifacts):
                f[f"art_{a}"] = _array(_artifact_shape(a, base_shape, diverse))
    artifact_types = [(f"art_{a}", f"/art_{a}") for a in range(n_artifacts)]
    return artifact_types, {"location": "root_scalars"}, "*.h5"


def _write_batched(data_dir, n_entities, n_artifacts, base_shape, diverse):
    # All entities stacked on axis-0 of each artifact dataset in a single file.
    with h5py.File(data_dir / "batch.h5", "w") as f:
        grp = f.create_group("params")
        for name in _PARAM_NAMES:
            grp[name] = np.array(
                [_params_for(i)[name] for i in range(n_entities)], dtype=np.float64
            )
        for a in range(n_artifacts):
            shape = _artifact_shape(a, base_shape, diverse)
            f[f"art_{a}"] = _array((n_entities, *shape))
    artifact_types = [(f"art_{a}", f"/art_{a}") for a in range(n_artifacts)]
    return artifact_types, {"location": "group", "group": "/params"}, "*.h5"


def _write_grouped(data_dir, n_entities, n_artifacts, base_shape, diverse):
    # One file, one /samples/sample_NNNN group per entity, each self-contained.
    with h5py.File(data_dir / "grouped.h5", "w") as f:
        samples = f.create_group("samples")
        for i in range(n_entities):
            g = samples.create_group(f"sample_{i:06d}")
            pg = g.create_group("params")
            for name, val in _params_for(i).items():
                pg[name] = val  # 0-dim scalar param inside the entity's params group
            for a in range(n_artifacts):
                g[f"art_{a}"] = _array(_artifact_shape(a, base_shape, diverse))
    artifact_types = [(f"art_{a}", f"art_{a}") for a in range(n_artifacts)]  # group-relative
    params = {"location": "group_scalars", "entity_group": "samples", "group": "params"}
    return artifact_types, params, "*.h5"


_WRITERS = {
    "per_entity": _write_per_entity,
    "batched": _write_batched,
    "grouped": _write_grouped,
}


def write_synthetic_dataset(
    out_dir,
    layout,
    n_entities,
    artifacts_per_entity=1,
    base_shape=(1,),
    shape_diverse=False,
    label=None,
):
    """Write a synthetic dataset (HDF5 + YAML) for one layout.

    Args:
        out_dir: Directory to create; HDF5 goes in ``out_dir/data``, YAML at ``out_dir/<label>.yml``.
        layout: One of ``per_entity`` / ``batched`` / ``grouped``.
        n_entities: Number of entities to synthesize.
        artifacts_per_entity: Array artifacts per entity.
        base_shape: Shape of each (tiny) artifact array.
        shape_diverse: Cycle distinct shapes across artifacts to exercise structure dedup.
        label: Dataset label (defaults to ``synth_<layout>_<n>``).

    Returns:
        Path to the written YAML config (feed it to ``tcb generate`` / ``run_generate``).
    """
    if layout not in _WRITERS:
        raise ValueError(f"unknown layout {layout!r}; expected one of {sorted(_WRITERS)}")

    out_dir = Path(out_dir)
    data_dir = out_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    label = label or f"synth_{layout}_{n_entities}"

    artifact_types, params_section, file_pattern = _WRITERS[layout](
        data_dir, n_entities, artifacts_per_entity, base_shape, shape_diverse
    )
    return _write_yaml(
        out_dir / f"{label}.yml", layout, data_dir, file_pattern,
        artifact_types, params_section,
    )


def run_generate(yaml_path, output_dir=None):
    """Run ``tcb generate`` programmatically on a synthetic config.

    Returns the directory holding ``entities.parquet`` / ``artifacts.parquet``.
    """
    from tiled_catalog_broker.tools.generate import generate_manifests

    yaml_path = Path(yaml_path)
    generate_manifests(str(yaml_path), output_dir=output_dir)
    if output_dir is not None:
        return Path(output_dir)
    # Mirror generate_manifests' default: <yaml_dir>/manifests/<label>/
    return yaml_path.parent / "manifests" / yaml_path.stem
