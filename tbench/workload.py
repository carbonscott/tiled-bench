"""Request shapes: the primitive things a client asks a Tiled server to do.

Each shape is ``async (client, target) -> record`` over a bare ``httpx.AsyncClient``. No
Tiled client library in the load path: its retries, dask graphs and GIL time are a
property of the client, measured separately if wanted. The server model is built from
these shapes, and any real workload is some mix of them.

    healthz   no catalog, no auth: the fabric floor
    metadata  one catalog lookup: the request the ~20-slot question is about
    array     metadata + the array bytes: what ``node[key][artifact].read()`` costs the server
    asset     the backing file streamed: bulk egress

A record is one HTTP call: ``t0``/``t1`` (seconds on the driver's clock), ``status``,
``bytes``, ``wall_ms``, and one ``<seg>_ms`` per Server-Timing segment the server sent
(``tok``, ``acl``, ``read``, ``pack``, ``app``). A shape that makes two calls returns two
records.
"""

import time

import httpx


def parse_server_timing(header):
    out = {}
    if not header:
        return out
    for seg in header.split(","):
        parts = seg.strip().split(";")
        for kv in parts[1:]:
            kv = kv.strip()
            if kv.startswith("dur="):
                try:
                    out[parts[0].strip()] = float(kv[4:])
                except ValueError:
                    pass
    return out


async def call(client, path, shape, clock0, params=None, stream=False):
    """One GET, fully consumed, as a record. Errors become records too (status 0 + error)."""
    t0 = time.perf_counter() - clock0
    rec = {"shape": shape, "path": path, "t0": t0}
    try:
        if stream:
            n = 0
            async with client.stream("GET", path, params=params) as r:
                async for chunk in r.aiter_bytes():
                    n += len(chunk)
        else:
            r = await client.get(path, params=params)
            n = len(r.content)
        rec.update(status=r.status_code, bytes=n)
        for k, v in parse_server_timing(r.headers.get("server-timing")).items():
            rec[f"{k}_ms"] = v
    except httpx.HTTPError as e:
        rec.update(status=0, bytes=0, error=f"{type(e).__name__}: {str(e)[:120]}")
    rec["t1"] = time.perf_counter() - clock0
    rec["wall_ms"] = (rec["t1"] - t0) * 1000.0
    return rec


class Shapes:
    """Shapes bound to one dataset. ``target`` is an entity key."""

    def __init__(self, dataset, artifact=None):
        self.dataset, self.artifact = dataset, artifact

    async def healthz(self, c, target, clock0):
        return [await call(c, "/healthz", "healthz", clock0)]

    async def metadata(self, c, target, clock0):
        return [await call(c, f"/api/v1/metadata/{self.dataset}/{target}", "metadata", clock0)]

    async def array(self, c, target, clock0):
        base = f"/api/v1/metadata/{self.dataset}/{target}/{self.artifact}"
        return [await call(c, base, "array.nav", clock0),
                await call(c, f"/api/v1/array/full/{self.dataset}/{target}/{self.artifact}",
                           "array.data", clock0)]

    async def asset(self, c, target, clock0):
        base = f"/api/v1/metadata/{self.dataset}/{target}/{self.artifact}"
        nav = await call(c, base, "asset.nav", clock0, params={"include_data_sources": "true"})
        recs = [nav]
        if nav["status"] == 200:
            r = await c.get(base, params={"include_data_sources": "true"})
            asset_id = r.json()["data"]["attributes"]["data_sources"][0]["assets"][0]["id"]
            recs.append(await call(c, f"/api/v1/asset/bytes/{self.dataset}/{target}/{self.artifact}",
                                   "asset.data", clock0, params={"id": asset_id}, stream=True))
        return recs

    def __getitem__(self, name):
        return getattr(self, name)


def entity_keys(url, api_key, dataset, n=1000):
    """Up to ``n`` entity keys of ``dataset``, by raw search (no client library)."""
    keys = []
    with httpx.Client(base_url=url, headers=auth(api_key), timeout=60) as c:
        offset = 0
        while len(keys) < n:
            r = c.get(f"/api/v1/search/{dataset}",
                      params={"fields": "", "page[offset]": offset, "page[limit]": min(1000, n - len(keys))})
            r.raise_for_status()
            page = [d["id"] for d in r.json()["data"]]
            if not page:
                break
            keys += page
            offset += len(page)
    return keys


def auth(api_key):
    return {"Authorization": f"Apikey {api_key}"} if api_key else {}
