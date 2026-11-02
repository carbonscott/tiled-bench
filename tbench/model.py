"""The server as a few numbers, and the arithmetic that connects them to measurements.

little(rate, latency)        in-flight = throughput x time in system. Holds for any stable
                             system; the one identity that turns a rate and a latency into
                             a concurrency, and so lets "how many at once" be measured.

usl_fit(N, X)                Universal Scalability Law: X(N) = lam*N / (1 + sig*(N-1) + kap*N*(N-1)).
                             Fit to a closed-loop ladder (N clients -> throughput X). ``sig``
                             is contention (a serialised resource: queueing for it), ``kap``
                             is coherence (cost that grows with N^2: locks, cache lines,
                             connection churn). Peak at N* = sqrt((1-sig)/kap). A pure
                             semaphore of S slots fits as sig ~ 1/S with kap ~ 0; a database
                             that thrashes fits with kap > 0 and throughput that *falls*.

knee(rungs)                  From an open-loop rate ladder: the first offered rate where
                             latency leaves its floor (p50 > 2x the lowest rung's) or the
                             server stops keeping up (achieved < 90% offered or errors > 1%).
                             Capacity is the rung before. Reported with the in-flight at
                             the knee: that number is the "slot count", measured.

Capacity(params).predict(workload)  Throughput and latency a mix of shapes should get,
                             from per-shape service times and the measured concurrency
                             limit. Compare with a measurement; a miss is a finding.
"""

import math

import numpy as np


def little(rate, latency_s):
    return rate * latency_s


def usl(N, lam, sig, kap):
    N = np.asarray(N, dtype=float)
    return lam * N / (1 + sig * (N - 1) + kap * N * (N - 1))


def usl_fit(N, X):
    """Least squares over a (sig, kap) grid with lam solved in closed form. No scipy."""
    N = np.asarray(N, dtype=float)
    X = np.asarray(X, dtype=float)
    best = None
    for sig in np.linspace(0.0, 0.999, 400):
        for kap in np.concatenate([[0.0], np.logspace(-6, -0.5, 120)]):
            f = N / (1 + sig * (N - 1) + kap * N * (N - 1))
            lam = float((X * f).sum() / (f * f).sum())
            sse = float(((X - lam * f) ** 2).sum())
            if best is None or sse < best[0]:
                best = (sse, lam, float(sig), float(kap))
    sse, lam, sig, kap = best
    nstar = math.sqrt((1 - sig) / kap) if kap > 0 else math.inf
    r2 = 1 - sse / float(((X - X.mean()) ** 2).sum()) if len(X) > 1 else float("nan")
    return {"lambda": lam, "sigma": sig, "kappa": kap, "n_star": nstar,
            "x_max": float(usl(nstar, lam, sig, kap)) if nstar != math.inf else float(lam / sig if sig else math.inf),
            "slots_if_semaphore": (1 / sig) if sig > 0 else math.inf, "r2": r2, "sse": sse}


def knee(rungs, latency_key="wall_p50", floor_factor=2.0, keepup=0.9, max_err=0.01):
    """``rungs``: list of Trace summaries from an open-loop ladder, ascending ``offered``."""
    rungs = sorted(rungs, key=lambda r: r["offered"])
    if not rungs:
        return None
    floor = rungs[0][latency_key]
    for i, r in enumerate(rungs):
        inflected = r[latency_key] > floor_factor * floor
        behind = (r["achieved"] or 0) < keepup * r["offered"]
        failing = (r["err_rate"] or 0) > max_err
        if inflected or behind or failing:
            cap = rungs[i - 1] if i else r
            return {"knee_offered": r["offered"], "why": ("latency" if inflected else "throughput" if behind else "errors"),
                    "capacity_rate": cap["achieved"], "latency_floor_ms": floor,
                    "in_flight_at_capacity": cap.get("in_flight_app"),
                    "in_flight_wall_at_capacity": cap.get("in_flight_wall"), "rung": r, "capacity_rung": cap}
    top = rungs[-1]
    return {"knee_offered": None, "why": "not reached", "capacity_rate": top["achieved"], "latency_floor_ms": floor,
            "in_flight_at_capacity": top.get("in_flight_app"), "in_flight_wall_at_capacity": top.get("in_flight_wall"),
            "rung": top, "capacity_rung": top}


class Capacity:
    """``service_ms``: per shape, server time per request at low load. ``slots``: measured
    concurrency limit (in-flight at capacity). ``mb_per_s``: wire ceiling."""

    def __init__(self, service_ms, slots, mb_per_s=None):
        self.service_ms, self.slots, self.mb_per_s = dict(service_ms), slots, mb_per_s

    def predict(self, mix, bytes_per_call=None):
        """``mix``: {shape: calls per unit of work}. Returns units/s, req/s and the binding limit."""
        slot_s = sum(n * self.service_ms[s] / 1000.0 for s, n in mix.items())
        units = self.slots / slot_s if slot_s else math.inf
        out = {"units_per_s": units, "req_per_s": units * sum(mix.values()), "slot_s_per_unit": slot_s, "limit": "slots"}
        if self.mb_per_s and bytes_per_call:
            mb_unit = sum(n * bytes_per_call.get(s, 0) for s, n in mix.items()) / 1e6
            if mb_unit and self.mb_per_s / mb_unit < units:
                out.update(units_per_s=self.mb_per_s / mb_unit, limit="wire")
        return out
