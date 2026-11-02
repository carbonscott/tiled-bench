"""What the benchmark tests: one function per retrieval path, named for the Tiled API
it exercises.

container_export — ``container.export(buf, fields=[...], format="application/x-hdf5")``.
    One request per <=MAX_FIELDS_PER_REQUEST keys (the shared proxy 414s past ~8 KB of
    URI). The server reads every artifact of every requested entity and re-encodes them
    into one HDF5 container; the client pays an h5py parse. Note the asymmetry: the wire
    carries *all* artifacts whether you wanted them or not.
artifact_read — ``node[key][artifact].read()``. Octet-stream numpy buffer of exactly one
    artifact, no h5py on either end, but per-entity metadata navigation plus one data
    request per entity.
raw_export — ``node[key][artifact].raw_export(dest)``. The public download API: streams
    the backing asset file and materializes it in RAM or on disk.
asset_bytes — ``GET /asset/bytes``, the endpoint underneath raw_export, streamed and
    discarded. Same requests and same wire bytes as raw_export, without the client-side
    buffering — so the gap between the two *is* the client library's overhead.
h5py_direct — open the HDF5 straight off the filesystem: the no-HTTP reference line.

Each returns ``(found, payload_bytes, wire_bytes, decode_s)``; runner.py times and
aggregates them.
"""

import time
from io import BytesIO
from pathlib import Path

import h5py
import httpx
import numpy as np

MAX_FIELDS_PER_REQUEST = 25  # proxy URI limit: one ~35-char field param per key
CHUNK_ATTEMPTS = 3
RETRY_BACKOFF_S = 10.0       # long enough for a transient shared-server stall to clear
GROUP_FMT = "samples/sample_{i:06d}"


def fetch_container_export(node, keys, artifact=None):
    """Export ``keys`` in URL-safe chunks.

    ``artifact=None`` counts and decodes every artifact in each entity — the "I want the
    whole entity" workload. Naming one artifact counts only that one, which is the "the
    server sent me four and I needed one" workload: the wire cost is identical, the
    decode cost is not.
    """
    found = 0
    payload = wire = 0
    decode_s = 0.0
    for i in range(0, len(keys), MAX_FIELDS_PER_REQUEST):
        chunk = keys[i : i + MAX_FIELDS_PER_REQUEST]
        for attempt in range(CHUNK_ATTEMPTS):
            buf = BytesIO()
            try:
                node.export(buf, fields=chunk, format="application/x-hdf5")
            except (httpx.HTTPStatusError, httpx.TransportError) as e:
                # 5xx/transport stalls are routine on the shared proxy (504 when the
                # server-side encode stalls) — back off and retry. 4xx is a real request
                # bug and retrying won't heal it: raise.
                code = getattr(getattr(e, "response", None), "status_code", None)
                if (code is not None and code < 500) or attempt == CHUNK_ATTEMPTS - 1:
                    raise
                time.sleep(RETRY_BACKOFF_S * (attempt + 1))
                continue
            break
        wire += buf.tell()
        buf.seek(0)
        t0 = time.perf_counter()
        with h5py.File(buf, "r") as f:
            for k in chunk:
                if k not in f:
                    continue
                payload += _decode_group(f[k], artifact)
                found += 1
        decode_s += time.perf_counter() - t0
    return found, payload, wire, decode_s


def fetch_artifact_read(node, key, artifact=None):
    """Read an entity's artifacts as raw numpy buffers.

    ``artifact=None`` reads every artifact of the entity — the same "whole entity"
    workload container_export serves, but paid as one request per artifact instead of one
    per 25 entities. That contrast is the point of comparing the two.
    """
    entity = node[key]
    names = [artifact] if artifact else list(entity)
    nbytes = sum(entity[name].read().nbytes for name in names)
    return 1, nbytes, 0, 0.0


def fetch_raw_export(node, keys, artifact, download_dir=None):
    """Download the backing asset through the public ``raw_export`` API.

    ``download_dir=None`` keeps the file in RAM; otherwise it lands on disk. Either way
    the whole file is materialized before this returns — that is the cost being measured,
    and it is what ``asset_bytes`` skips.

    raw_export is per-*asset*, so layout sets the unit of work: batched/grouped share one
    file, so one call serves every key in ``keys``; per_entity gets one call per key.
    """
    art = node[keys[0]][artifact]
    if download_dir is None:
        buffers = {}
        art.raw_export(buffers)
        moved = sum(b.getbuffer().nbytes for b in buffers.values())
    else:
        moved = sum(Path(p).stat().st_size for p in art.raw_export(download_dir))
    return len(keys), 0, moved, 0.0


def fetch_asset_bytes(node, keys, artifact):
    """Stream the backing asset off the server and count it. Nothing is retained.

    Same endpoint and same bytes as raw_export, minus the materialization, so this is the
    ceiling that path could reach. Nothing is decoded, so payload is 0 by construction —
    the honest number here is bytes off the wire.
    """
    art = node[keys[0]][artifact].include_data_sources()
    asset = art.data_sources()[0].assets[0]
    url = art.item["links"]["self"].replace("/metadata", "/asset/bytes", 1)
    moved = 0
    with art.context.http_client.stream("GET", url, params={"id": asset.id}) as r:
        r.raise_for_status()
        for chunk in r.iter_bytes():
            moved += len(chunk)
    return len(keys), 0, moved, 0.0


def read_direct(files, layout, h5_path, i, group_fmt=GROUP_FMT):
    """Read entity ``i`` straight from HDF5 on disk -> payload bytes."""
    path = files[i] if layout == "per_entity" else files[0]
    with h5py.File(path, "r") as f:
        return np.asarray(_entity(f, layout, h5_path, i, group_fmt)).nbytes


def _decode_group(grp, artifact=None):
    """Read arrays out of one exported entity group -> decoded bytes.

    Recurses, so an entity whose artifacts sit in subgroups is counted correctly rather
    than silently skipped. Reads for real (not ``Dataset.nbytes``) because the point is
    to pay the client-side parse cost that decode_s reports.
    """
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
    """Entity ``i`` inside an open HDF5 file — the one place layout is decoded.

    per_entity: the file *is* the entity, so ``i`` selects the file, not anything within
    it. batched: row ``i`` of a stacked dataset. grouped: entity ``i``'s own group.
    """
    if layout == "per_entity":
        return f[h5_path]
    if layout == "batched":
        return f[h5_path][i]
    return f[group_fmt.format(i=i)][h5_path]
