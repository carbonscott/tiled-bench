"""The three retrieval methods. Each returns raw counters for the runner to aggregate.

export_hdf5 — container-level bulk export (``node.export(..., format="application/x-hdf5")``):
    one request per ≤MAX_FIELDS_PER_REQUEST keys (the shared proxy 414s past ~8 KB of URI),
    server re-encodes into an HDF5 container, client pays an h5py parse (decode).
raw_read — array-level read (``node[key][artifact].read()``): octet-stream numpy buffer,
    no h5py on either end, but per-entity metadata navigation + one data request per entity.
h5py_direct — open the HDF5 straight from the filesystem (Mode A): the no-HTTP floor.
"""

import time
from io import BytesIO

import h5py
import httpx
import numpy as np

MAX_FIELDS_PER_REQUEST = 25  # proxy URI limit: one ~35-char field param per key → 414 past ~220
CHUNK_ATTEMPTS = 3
RETRY_BACKOFF_S = 10.0       # long enough for a transient shared-server stall to clear


def fetch_export(node, keys, artifact):
    """Export ``keys`` in URL-safe chunks → (found, payload_bytes, container_bytes, decode_s)."""
    found = 0
    payload = container = 0
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
        container += buf.tell()
        buf.seek(0)
        t0 = time.perf_counter()
        with h5py.File(buf, "r") as f:
            for k in chunk:
                if k in f and artifact in f[k]:
                    payload += np.asarray(f[k][artifact]).nbytes
                    found += 1
        decode_s += time.perf_counter() - t0
    return found, payload, container, decode_s


def fetch_raw(node, key, artifact):
    """Read one entity's artifact as a raw numpy buffer → payload bytes."""
    return node[key][artifact].read().nbytes


def read_direct(spec, files, i):
    """Read entity ``i`` straight from HDF5 on disk → payload bytes."""
    if spec.layout == "per_entity":
        with h5py.File(files[i], "r") as f:
            return np.asarray(f[spec.h5_path]).nbytes
    if spec.layout == "batched":
        with h5py.File(files[0], "r") as f:
            return np.asarray(f[spec.h5_path][i]).nbytes
    with h5py.File(files[0], "r") as f:  # grouped
        return np.asarray(f[spec.entity_group_fmt.format(i=i)][spec.h5_path]).nbytes
