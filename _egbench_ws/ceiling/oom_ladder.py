"""Memory-pressure ladder: concurrent whole-dataset HDF5 exports until OOMKill.

Container export builds the ENTIRE output file in RAM server-side
(tiled/serialization/container.py: io.BytesIO) — ~4.3 GB+ per request for
EGRESS_BAT_16M_BROKER. Rungs: 1, 2, 4, 8 concurrent exports. Raw httpx
(no client retries). Failure-mode probe, not a measurement — no CSV rows.
Prints UTC timestamps so the admin can correlate pod OOMKills/restarts.
Stops at the first rung showing failures, then watches recovery.
"""
import concurrent.futures as cf
import datetime
import os
import time

import httpx

URL = os.environ["TILED_URL"].rstrip("/")
HDRS = {"Authorization": "Apikey " + os.environ["TILED_API_KEY"]}
DATASET = "EGRESS_BAT_16M_BROKER"
EXPORT = f"{URL}/api/v1/container/full/{DATASET}?format=application/x-hdf5"
HEALTH = f"{URL}/api/v1/metadata/{DATASET}"


def ts():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%H:%M:%S")


def health(tag):
    try:
        r = httpx.get(HEALTH, headers=HDRS, timeout=30)
        print(f"[{ts()} UTC] health {tag}: {r.status_code} in {r.elapsed.total_seconds()*1000:.0f} ms")
        return r.status_code == 200
    except Exception as e:
        print(f"[{ts()} UTC] health {tag}: EXC {type(e).__name__}: {str(e)[:100]}")
        return False


def one_export(i):
    t0 = time.perf_counter()
    n = 0
    try:
        with httpx.Client() as c, c.stream("GET", EXPORT, headers=HDRS,
                                           timeout=httpx.Timeout(900, connect=30)) as r:
            if r.status_code != 200:
                r.read()
                return f"  [exp-{i}] HTTP {r.status_code} after {time.perf_counter()-t0:.1f}s: {r.text[:150]}"
            for chunk in r.iter_bytes(1 << 20):
                n += len(chunk)
        return f"  [exp-{i}] OK {n/1e6:.0f} MB in {time.perf_counter()-t0:.1f}s"
    except Exception as e:
        return (f"  [exp-{i}] DIED after {time.perf_counter()-t0:.1f}s at {n/1e6:.0f} MB "
                f"— {type(e).__name__}: {str(e)[:120]}")


health("baseline")
for width in (1, 2, 4, 8):
    print(f"[{ts()} UTC] === rung: {width} concurrent whole-dataset export(s) (~4.3 GB each in server RAM)")
    with cf.ThreadPoolExecutor(width) as ex:
        results = list(ex.map(one_export, range(width)))
    for line in results:
        print(f"[{ts()} UTC]{line}")
    died = sum("DIED" in r or "HTTP 5" in r for r in results)
    ok = health(f"post-rung-{width}")
    if died or not ok:
        print(f"[{ts()} UTC] *** failures at width={width} — stopping ladder, watching recovery")
        for i in range(20):
            time.sleep(15)
            if health(f"recovery+{(i+1)*15}s"):
                break
        break
print(f"[{ts()} UTC] done")
