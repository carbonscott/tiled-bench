"""Offline tests: a fake Tiled behind httpx.MockTransport with a 4-slot semaphore and 20 ms
service time, so capacity is known (~200 req/s, in-flight 4) and the methodology itself is
under test. Run: .venv/bin/python tests/test_tbench.py"""

import asyncio
import json
import os
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ["TBENCH_LEDGER"] = tempfile.mkdtemp()

import httpx  # noqa: E402

from tbench import drive, experiments, fingerprint, ledger, model, observe, workload  # noqa: E402

SLOTS, SERVICE = 4, 0.020
_sems = {}   # one semaphore per event loop: every asyncio.run() is a new loop


async def _handler(request):
    loop = asyncio.get_running_loop()
    _sem = _sems.get(loop)
    if _sem is None:
        _sem = _sems[loop] = asyncio.Semaphore(SLOTS)
    if request.url.path == "/healthz":
        return httpx.Response(200, content=b"ok")
    if request.url.path == "/api/v1/":
        return httpx.Response(200, json={"library_version": "fake-1"})
    t0 = time.perf_counter()
    async with _sem:
        await asyncio.sleep(SERVICE)
    app = (time.perf_counter() - t0) * 1000
    return httpx.Response(200, content=b"x" * 500,
                          headers={"server-timing": f"tok;dur=0.1, read;dur={SERVICE*1000:.1f}, app;dur={app:.1f}"})


def _patch_client():
    """Route tbench's AsyncClient through the fake server."""
    real = drive._client

    def fake(url, api_key, max_conn, timeout):
        return httpx.AsyncClient(base_url="http://fake", transport=httpx.MockTransport(_handler))

    drive._client = fake
    return real


def test_server_timing_parse():
    assert workload.parse_server_timing("tok;dur=0.4, app;dur=17.4") == {"tok": 0.4, "app": 17.4}
    assert workload.parse_server_timing(None) == {}


def test_little_and_usl():
    assert model.little(100, 0.05) == 5
    N = [1, 2, 4, 8, 16, 32]
    X = model.usl(N, 50, 0.2, 0.001)
    fit = model.usl_fit(N, X)
    assert abs(fit["sigma"] - 0.2) < 0.03 and abs(fit["lambda"] - 50) < 3 and fit["r2"] > 0.99


def test_open_loop_finds_the_semaphore():
    _patch_client()
    shapes = workload.Shapes("DS", "signal")
    rungs = []
    for rate in (50, 100, 150, 180, 250, 400):
        tr = drive.open_loop("http://fake", None, shapes.metadata, ["k1", "k2"], rate, 1.5)
        rungs.append(tr.summary())
    k = model.knee(rungs)
    assert k["knee_offered"] in (250, 400), k
    cap = k["capacity_rung"]
    assert 140 <= cap["achieved"] <= 200, cap
    assert 3.0 <= cap["in_flight_app"] <= 4.5, cap["in_flight_app"]   # Little recovers the slot count
    assert "read_p50" in cap and "tok_p50" in cap


def test_closed_loop_usl_sees_contention():
    _patch_client()
    shapes = workload.Shapes("DS", "signal")
    N, X = [], []
    for n in (1, 2, 4, 8, 16):
        s = drive.closed_loop("http://fake", None, shapes.metadata, ["k"], n, 1.0).summary()
        N.append(n)
        X.append(s["achieved"])
    fit = model.usl_fit(N, X)
    assert X[-1] >= 150 and fit["slots_if_semaphore"] < 12, (X, fit)


def test_ledger_round_trip_and_staleness():
    obs = {"url": "http://fake", "server_version": "fake-1"}
    fp1 = fingerprint.merge(obs, ledger.reported())
    c = ledger.claim("metadata.capacity_rate", 190, fp1, ["run-a"], unit="req/s")
    assert ledger.known(fp1)["metadata.capacity_rate"]["id"] == c["id"]
    ledger.intervene("workers", 16, by="operator", note="scaled in conversation")
    fp2 = fingerprint.merge(obs, ledger.reported())
    assert fp2["key"] != fp1["key"] and fp2["reported"]["workers"] == 16
    assert "metadata.capacity_rate" not in ledger.known(fp2)
    assert fp1["key"] in ledger.stale(fp2)
    assert ledger.interventions()[-1]["after"] == 16


def test_experiment_writes_a_run():
    _patch_client()
    fp = fingerprint.merge({"url": "http://fake", "server_version": "fake-1"}, {})
    run_id, rungs, k = experiments.rate_ladder("http://fake", None, "DS", "signal", "metadata", ["k1"],
                                               [50, 150, 400], duration=1.0, fp=fp,
                                               observers=[observe.Reported(cpu_limit=2)])
    path = ledger.ROOT / "runs" / run_id
    assert (path / "params.json").exists() and (path / "knee.json").exists()
    assert len((path / "rungs.jsonl").read_text().splitlines()) == len(rungs)
    assert json.loads((path / "coverage.json").read_text())["reported"] == 1.0
    assert any(p.name.startswith("calls-metadata-rate50") for p in path.iterdir())


def test_capacity_predicts():
    cap = model.Capacity({"metadata": 50, "array.nav": 50, "array.data": 60}, slots=12)
    p = cap.predict({"array.nav": 2, "array.data": 1})
    assert abs(p["units_per_s"] - 12 / 0.16) < 1e-6 and p["req_per_s"] == p["units_per_s"] * 3


if __name__ == "__main__":
    tests = [(k, v) for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    failed = 0
    for name, fn in tests:
        try:
            fn()
            print(f"ok   {name}")
        except Exception as e:  # noqa: BLE001
            failed += 1
            import traceback
            print(f"FAIL {name}: {e!r}")
            traceback.print_exc()
    print(f"{len(tests) - failed}/{len(tests)} passed")
    sys.exit(1 if failed else 0)
