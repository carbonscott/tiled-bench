"""Offline tests for tbench: no server, no network. Run: .venv/bin/python tests/test_tbench.py"""

import json
import sys
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tbench import load, probes, timing  # noqa: E402


# ---- timing -------------------------------------------------------------------------

def test_parse_server_timing():
    got = timing.parse_server_timing("tok;dur=0.4, acl;dur=0.2, read;dur=12.1, pack;dur=0.9, app;dur=17.4")
    assert got == {"tok": 0.4, "acl": 0.2, "read": 12.1, "pack": 0.9, "app": 17.4}
    assert timing.parse_server_timing(None) == {}
    assert timing.parse_server_timing("app;dur=x") == {}


class _FakeHttp:
    """Enough of httpx.Client for Collector: event hooks and a get() that fires them."""

    def __init__(self, server_timing="tok;dur=1, app;dur=5", status=200, body=b"x" * 10):
        self.event_hooks = {"request": [], "response": []}
        self._st, self._status, self._body = server_timing, status, body

    def get(self, path):
        req = SimpleNamespace(method="GET", url=SimpleNamespace(path=path))
        for h in self.event_hooks["request"]:
            h(req)
        time.sleep(0.002)
        resp = SimpleNamespace(request=req, status_code=self._status, content=self._body,
                               headers={"server-timing": self._st, "content-length": str(len(self._body))})
        for h in self.event_hooks["response"]:
            h(resp)
        if self._status >= 400:
            raise RuntimeError(f"HTTP {self._status}")
        return resp


def fake_node(**kw):
    return SimpleNamespace(context=SimpleNamespace(http_client=_FakeHttp(**kw)))


def test_collector_keeps_every_segment_and_unhooks():
    node = fake_node()
    c = timing.Collector()
    with c.attached([node]):
        node.context.http_client.get("/api/v1/metadata/x")
    assert len(c.records) == 1
    r = c.records[0]
    assert r["tok_ms"] == 1 and r["app_ms"] == 5 and r["status"] == 200 and r["bytes"] == 10
    assert r["wall_ms"] > 0 and r["t1"] >= r["t0"] >= 0
    assert node.context.http_client.event_hooks == {"request": [], "response": []}


# ---- load ---------------------------------------------------------------------------

def _work(node, item):
    node.context.http_client.get(f"/api/v1/metadata/{item}")
    node.context.http_client.get(f"/api/v1/array/full/{item}")
    return {"entities": 1, "payload": 64, "note": "non-numeric stays per unit"}


def test_run_single_process():
    run = load.run(_work, range(10), fake_node, threads=2, procs=1, label="t2")
    run.assert_clean(expected_units=10)
    s = run.summary()
    assert s["units_ok"] == 10 and s["calls"] == 20 and s["offered"] == 2
    assert s["total_entities"] == 10 and s["total_payload"] == 640
    assert "total_note" not in s and run.units[0]["note"].startswith("non-numeric")
    assert "tok_p50_ms" in s and "app_sum_s" in s and s["in_flight"] > 0
    assert run.wall_s > 0 and all(0 <= u["t0"] <= u["t1"] <= run.wall_s + 1e-3 for u in run.units)


def test_run_multi_process_shards_every_item_once():
    run = load.run(_work, range(12), fake_node, threads=2, procs=3, label="p3t2")
    run.assert_clean(expected_units=12)
    assert sorted(int(u["item"]) for u in run.units) == list(range(12))
    assert {u["proc"] for u in run.units} == {0, 1, 2}
    assert len(run.calls) == 24
    assert all(c["t1"] <= run.wall_s + 1e-3 for c in run.calls)


def test_failed_unit_is_data_and_assert_clean_catches_it():
    def flaky(node, item):
        if item == 3:
            raise ValueError("boom?secret=1")
        return _work(node, item)

    run = load.run(flaky, range(5), fake_node, threads=1, procs=1)
    bad = [u for u in run.units if not u["ok"]]
    assert len(bad) == 1 and bad[0]["error"] == "ValueError: boom"
    try:
        run.assert_clean()
    except AssertionError as e:
        assert "1 failed units" in str(e)
    else:
        raise AssertionError("assert_clean should have raised")


def test_non_2xx_call_fails_assert_clean():
    run = load.run(_work, range(2), lambda: fake_node(status=500), threads=1, procs=1)
    assert run.summary()["calls_err"] == 2
    try:
        run.assert_clean()
    except AssertionError as e:
        assert "failed units" in str(e)  # the unit raised first
    else:
        raise AssertionError("expected failure")


def test_save_and_round(tmp=None):
    with tempfile.TemporaryDirectory() as d:
        with load.Round("smoke", root=d, day="2026-01-01", extra={"k": "v"}) as rd:
            assert rd.dir.name == "2026-01-01-smoke" and json.loads((rd.dir / "meta.json").read_text())["k"] == "v"
            run = load.run(_work, range(4), fake_node, threads=2, procs=1, label="t2")
            path = rd.save(run)
        assert path.name == "t2.summary.json"
        summ = json.loads(path.read_text())
        assert summ["meta"]["k"] == "v" and summ["units_ok"] == 4
        assert len((rd.dir / "t2.calls.jsonl").read_text().splitlines()) == 8
        assert len((rd.dir / "t2.units.jsonl").read_text().splitlines()) == 4


# ---- probes -------------------------------------------------------------------------

def test_prometheus_parse_and_delta():
    text = ("# HELP db_pool_checkedout x\n"
            'db_pool_checkedout{name="catalog"} 3\n'
            'db_pool_at_max_total{name="catalog"} 0\n'
            'http_request_duration_seconds_count{code="200"} 10\n'
            "process_cpu_seconds_total 1.5\n")
    import re
    out = {}
    for line in text.splitlines():
        if line.startswith("#"):
            continue
        m = probes._PROM.match(line)
        if m and m.group(1).startswith(("db_pool", "tiled", "http")):
            out[m.group(1) + (m.group(2) or "")] = float(m.group(3))
    assert out == {'db_pool_checkedout{name="catalog"}': 3.0, 'db_pool_at_max_total{name="catalog"}': 0.0,
                   'http_request_duration_seconds_count{code="200"}': 10.0}
    after = dict(out, **{'db_pool_at_max_total{name="catalog"}': 2.0})
    assert probes.metrics_delta(out, after) == {'db_pool_at_max_total{name="catalog"}': 2.0}


def test_pg_sampler_writes_jsonl_with_fake_psql():
    with tempfile.TemporaryDirectory() as d:
        fake = Path(d) / "psql"
        fake.write_text("#!/bin/sh\nprintf 'now\\ttiled-test\\tactive\\t\\t\\t3\\n'\n")
        fake.chmod(0o755)
        out = Path(d) / "pg.jsonl"
        with probes.PgSampler([str(fake)], out, hz=20):
            time.sleep(0.3)
        lines = [json.loads(l) for l in out.read_text().splitlines()]
        assert len(lines) >= 2 and lines[0]["activity"] == [["now", "tiled-test", "active", "", "", "3"]]


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
