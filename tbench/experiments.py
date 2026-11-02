"""Procedures that turn a hypothesis into evidence. Each writes a run directory under the
ledger and returns what it found, so the agent can claim it.

rate_ladder          open-loop arrival rates, ascending, until the knee. The capacity
                     measurement. Yields the capacity rate, the latency floor, and the
                     in-flight at capacity (the slot count), plus the per-segment split.
concurrency_ladder   closed-loop client counts, ascending; USL fit on the result says
                     whether the limit is contention or coherence, and how many slots a
                     pure semaphore would need to produce it.
compare              the same ladder for two shapes (or the same shape before and after an
                     intervention) under the same observers: the isolation experiment.

Every run saves: params.json, fingerprint.json, rungs.jsonl (one summary per rung),
calls-<rung>.jsonl (every HTTP call), observe.jsonl (observer samples, if any), and
coverage.json (which observers answered). ``replay(run_dir)`` reruns the same params.
"""

import json
from pathlib import Path

from . import drive, ledger, model, observe, workload


def _save_calls(path, calls):
    with open(path, "w") as f:
        for c in calls:
            f.write(json.dumps(c) + "\n")


def _ladder(kind, url, api_key, dataset, artifact, shape, targets, axis, values, duration, procs,
            observers, fp, stop, label):
    shapes = workload.Shapes(dataset, artifact)
    run_id, path = ledger.new_run(kind, {"url": url, "dataset": dataset, "artifact": artifact, "shape": shape,
                                         "n_targets": len(targets), axis: values, "duration": duration,
                                         "procs": procs, "label": label}, fp)
    rungs = []
    sampler = observe.Sampler(observers, path / "observe.jsonl") if observers else None
    if sampler:
        sampler.__enter__()
    try:
        for v in values:
            if axis == "rate":
                tr = drive.open_loop(url, api_key, shapes[shape], targets, v, duration, procs=procs)
            else:
                tr = drive.closed_loop(url, api_key, shapes[shape], targets, v, duration, procs=procs)
            s = tr.summary()
            s["label"] = f"{shape}-{axis}{v:g}"
            rungs.append(s)
            _save_calls(path / f"calls-{s['label']}.jsonl", tr.calls)
            with open(path / "rungs.jsonl", "a") as f:
                f.write(json.dumps(s) + "\n")
            print(f"[{run_id}] {s['label']:>22} achieved={s['achieved']:7.1f}/s  wall_p50={s.get('wall_p50', 0):6.0f} ms  "
                  f"app_p50={s.get('app_p50', float('nan')):6.0f} ms  in_flight={s.get('in_flight_app', float('nan')):5.1f}  "
                  f"err={s['err']} drop={s['dropped']}", flush=True)
            if stop and stop(s):
                print(f"[{run_id}] stop rule hit at {axis}={v}", flush=True)
                break
    finally:
        if sampler:
            sampler.__exit__(None, None, None)
            (path / "coverage.json").write_text(json.dumps(sampler.coverage(), indent=1))
    return run_id, path, rungs


def rate_ladder(url, api_key, dataset, artifact, shape, targets, rates, duration=20, procs=1,
                observers=(), fp=None, label="", max_err=0.05):
    """Open-loop ladder; stops past the knee. Returns (run_id, rungs, knee)."""
    fp = fp or ledger.current(url, api_key)

    def stop(s):
        return (s["err_rate"] or 0) > max_err or s["dropped"] > 0 or \
            ((s["achieved"] or 0) < 0.6 * s["offered"])

    run_id, path, rungs = _ladder("rate", url, api_key, dataset, artifact, shape, targets, "rate",
                                  list(rates), duration, procs, observers, fp, stop, label)
    k = model.knee(rungs)
    (path / "knee.json").write_text(json.dumps(k, indent=1, default=str))
    return run_id, rungs, k


def concurrency_ladder(url, api_key, dataset, artifact, shape, targets, concurrencies, duration=20, procs=1,
                       observers=(), fp=None, label="", max_err=0.05):
    """Closed-loop ladder with a USL fit. Returns (run_id, rungs, fit)."""
    fp = fp or ledger.current(url, api_key)

    def stop(s):
        return (s["err_rate"] or 0) > max_err

    run_id, path, rungs = _ladder("conc", url, api_key, dataset, artifact, shape, targets, "concurrency",
                                  list(concurrencies), duration, procs, observers, fp, stop, label)
    fit = model.usl_fit([r["offered"] for r in rungs], [r["achieved"] for r in rungs]) if len(rungs) >= 3 else None
    (path / "usl.json").write_text(json.dumps(fit, indent=1, default=str))
    return run_id, rungs, fit


def compare(url, api_key, dataset, artifact, shapes, targets, rates, **kw):
    """The same rate ladder per shape, back to back. Returns {shape: (run_id, rungs, knee)}."""
    return {s: rate_ladder(url, api_key, dataset, artifact, s, targets, rates, **kw) for s in shapes}


def replay(run_dir, api_key, observers=()):
    """Re-run a saved experiment's params; a new run directory, same fingerprint check."""
    p = json.loads((Path(run_dir) / "params.json").read_text())
    targets = workload.entity_keys(p["url"], api_key, p["dataset"], p["n_targets"])
    if p["kind"] == "rate":
        return rate_ladder(p["url"], api_key, p["dataset"], p["artifact"], p["shape"], targets, p["rate"],
                           p["duration"], p["procs"], observers, label=f"replay:{Path(run_dir).name}")
    return concurrency_ladder(p["url"], api_key, p["dataset"], p["artifact"], p["shape"], targets,
                              p["concurrency"], p["duration"], p["procs"], observers,
                              label=f"replay:{Path(run_dir).name}")
