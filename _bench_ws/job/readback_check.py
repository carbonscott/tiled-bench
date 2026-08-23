"""Post-'after' guardrail (runbook §1.2): Fix B changes where shape/dtype come from,
so register a small dataset with the installed TCB and read one artifact back,
asserting the array's shape/dtype and the artifact metadata are sane.

Usage: readback_check.py <workspace-dir>   (run with PYTHONPATH pointing at the
TCB code state under test; cwd must be the tiled-bench repo so regbench imports)
"""
import sys
from pathlib import Path

from regbench.config import RunConfig
from regbench.manifests import build_manifest
from regbench.runner import make_client, run_registration
from regbench.server import local_server

ws = Path(sys.argv[1])
cfg = RunConfig(layout="per_entity", n_entities=5, max_workers=4)
manifest = build_manifest(cfg, root=ws / "datasets")

# The manifest produced by the after-branch generator must carry the new columns.
assert "shape" in manifest.art_df.columns, "after-branch manifest lacks 'shape' column"
assert "dtype" in manifest.art_df.columns, "after-branch manifest lacks 'dtype' column"

with local_server(workspace=ws) as server:
    client = make_client(server.uri, server.api_key)
    res = run_registration(cfg, manifest, client, rep=0)
    assert res.entities == 5, f"expected 5 entities, got {res.entities}"

    ds = client[res.dataset_key]
    ent = ds[sorted(ds.keys())[0]]
    art = ent[sorted(ent.keys())[0]]
    arr = art.read()
    assert arr.shape == (1,), f"shape {arr.shape} != (1,)"
    assert str(arr.dtype) == "float64", f"dtype {arr.dtype} != float64"
    md = dict(art.metadata)
    assert md.get("shape") == [1], f"metadata shape {md.get('shape')!r}"
    assert md.get("dtype") == "float64", f"metadata dtype {md.get('dtype')!r}"
    print(f"[readback] OK: array {arr.shape} {arr.dtype}, "
          f"metadata shape={md['shape']} dtype={md['dtype']}")
