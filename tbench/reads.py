"""The read paths under test: one function per way of getting bytes out of Tiled.

Each takes a Tiled node (a dataset container) and does one unit of work, returning a dict
of counts that ``tbench.load`` sums across units:

    entities   how many entities this unit covered
    payload    decoded bytes the caller actually got (0 when nothing is decoded)
    wire       bytes that crossed the wire, when the method can see them (else 0)
    decode_s   client-side parse time, when there is one (else 0)

Keep these honest and dumb: no retries, no timing, no aggregation. Retrying hides server
errors (the 2026-08-04 raw-wire probes showed the Tiled client's silent retries masking a
35% 500-rate); timing and aggregation belong to ``tbench.load`` and ``tbench.timing``.

container_export  ``container.export(buf, fields=keys, format="application/x-hdf5")``.
    One request per <=MAX_FIELDS_PER_REQUEST keys (the shared proxy 414s past ~8 KB of
    URI). The server re-encodes every artifact of every requested entity into one HDF5;
    the client pays an h5py parse. The wire carries all artifacts whether wanted or not.
artifact_read     ``node[key][artifact].read()``. Octet-stream numpy of one artifact:
    two metadata GETs of navigation plus one data GET per entity.
raw_export        ``node[key][artifact].raw_export(dest)``. Public download API: streams
    the backing asset file into RAM or onto disk.
asset_bytes       ``GET /asset/bytes`` streamed and discarded. Same bytes as raw_export
    without materialisation, so the gap between the two is the client library.
metadata_get      one raw ``GET`` of a node's metadata, no client library. The probe for
    the catalog path on its own (what step 7c did by hand).
h5py_direct       open the HDF5 straight off the filesystem: the no-HTTP reference line.
"""

import time
from io import BytesIO
from pathlib import Path

import h5py
import numpy as np

MAX_FIELDS_PER_REQUEST = 25  # proxy URI limit: one ~35-char field param per key
GROUP_FMT = "samples/sample_{i:06d}"


def container_export(node, keys, artifact=None):
    """Export ``keys`` in URL-safe chunks; decode all artifacts or just ``artifact``."""
    out = {"entities": 0, "payload": 0, "wire": 0, "decode_s": 0.0}
    for i in range(0, len(keys), MAX_FIELDS_PER_REQUEST):
        chunk = keys[i : i + MAX_FIELDS_PER_REQUEST]
        buf = BytesIO()
        node.export(buf, fields=chunk, format="application/x-hdf5")
        out["wire"] += buf.tell()
        buf.seek(0)
        t0 = time.perf_counter()
        with h5py.File(buf, "r") as f:
            for k in chunk:
                if k in f:
                    out["payload"] += _decode_group(f[k], artifact)
                    out["entities"] += 1
        out["decode_s"] += time.perf_counter() - t0
    return out


def artifact_read(node, key, artifact=None):
    """Read one entity's artifacts (all, or just ``artifact``) as numpy."""
    entity = node[key]
    names = [artifact] if artifact else list(entity)
    nbytes = sum(entity[name].read().nbytes for name in names)
    return {"entities": 1, "payload": nbytes, "wire": 0, "decode_s": 0.0}


def raw_export(node, keys, artifact, download_dir=None):
    """Download the backing asset through ``raw_export``; RAM unless ``download_dir``.

    raw_export is per *asset*, so layout sets the unit: batched/grouped share one file and
    one call serves every key in ``keys``; per_entity is one call per key.
    """
    art = node[keys[0]][artifact]
    if download_dir is None:
        buffers = {}
        art.raw_export(buffers)
        moved = sum(b.getbuffer().nbytes for b in buffers.values())
    else:
        moved = sum(Path(p).stat().st_size for p in art.raw_export(download_dir))
    return {"entities": len(keys), "payload": 0, "wire": moved, "decode_s": 0.0}


def asset_bytes(node, keys, artifact):
    """Stream the backing asset and count it; retain nothing."""
    art = node[keys[0]][artifact].include_data_sources()
    asset = art.data_sources()[0].assets[0]
    url = art.item["links"]["self"].replace("/metadata", "/asset/bytes", 1)
    moved = 0
    with art.context.http_client.stream("GET", url, params={"id": asset.id}) as r:
        r.raise_for_status()
        for chunk in r.iter_bytes():
            moved += len(chunk)
    return {"entities": len(keys), "payload": 0, "wire": moved, "decode_s": 0.0}


def metadata_get(node, key):
    """One raw metadata GET for ``node[key]`` through the node's own httpx client.

    No client-library retries, no navigation, no parsing beyond status. The number this
    produces is the catalog path and nothing else.
    """
    url = node.item["links"]["self"].rstrip("/") + "/" + key
    r = node.context.http_client.get(url)
    r.raise_for_status()
    return {"entities": 1, "payload": 0, "wire": len(r.content), "decode_s": 0.0}


def h5py_direct(files, layout, h5_path, i, group_fmt=GROUP_FMT):
    """Read entity ``i`` straight from HDF5 on disk."""
    path = files[i] if layout == "per_entity" else files[0]
    with h5py.File(path, "r") as f:
        nbytes = np.asarray(_entity(f, layout, h5_path, i, group_fmt)).nbytes
    return {"entities": 1, "payload": nbytes, "wire": 0, "decode_s": 0.0}


def _decode_group(grp, artifact=None):
    """Decoded bytes of one exported entity group; recurses into subgroups."""
    if artifact is not None:
        return np.asarray(grp[artifact]).nbytes if artifact in grp else 0
    total = 0

    def visit(_name, obj):
        nonlocal total
        if isinstance(obj, h5py.Dataset):
            total += np.asarray(obj).nbytes

    grp.visititems(visit)
    return total


def _entity(f, layout, h5_path, i, group_fmt):
    """Entity ``i`` inside an open HDF5 file: the one place layout is decoded."""
    if layout == "per_entity":
        return f[h5_path]
    if layout == "batched":
        return f[h5_path][i]
    return f[group_fmt.format(i=i)][h5_path]
