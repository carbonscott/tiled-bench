"""What we know, under which conditions, on what evidence. Append-only JSONL in ``ledger/``.

    claims.jsonl         one line per claim: quantity, value, interval, fingerprint key, evidence
    interventions.jsonl  one line per operator change: what, before, after, when, who said so
    reported.json        the operator-reported half of the fingerprint, as of now
    runs/<id>/           evidence: params, per-rung summaries, every call, observer samples

The loop an agent runs: ``current()`` to learn the fingerprint; ``known()`` to see which
claims still hold under it; run an experiment; ``claim()`` the result. When the operator
changes something in conversation, ``intervene()`` records it, the fingerprint key
changes, and every earlier claim is stale by construction, not by someone remembering.
"""

import json
import os
import time
import uuid
from pathlib import Path

from . import fingerprint

ROOT = Path(os.environ.get("TBENCH_LEDGER", "ledger"))


def _append(name, rec):
    ROOT.mkdir(parents=True, exist_ok=True)
    with open(ROOT / name, "a") as f:
        f.write(json.dumps(rec) + "\n")
    return rec


def _read(name):
    p = ROOT / name
    if not p.exists():
        return []
    return [json.loads(line) for line in p.read_text().splitlines() if line.strip()]


def reported():
    p = ROOT / "reported.json"
    return json.loads(p.read_text()) if p.exists() else {}


def current(url, api_key=None):
    """The fingerprint right now: observed from the server, reported from the ledger."""
    return fingerprint.merge(fingerprint.observe(url, api_key), reported())


def intervene(what, after, before=None, by="operator", note=""):
    """Record a back-end change the operator made or reported.

    ``what`` is a REPORTED_FIELDS name (or a free-text tag for one-offs); ``after`` is the
    new value as the operator stated it. Updates ``reported.json``, so the next
    ``current()`` has a new key and old claims show as stale.
    """
    rep = reported()
    rec = {"t": time.time(), "what": what, "before": rep.get(what, before), "after": after,
           "by": by, "note": note}
    if what in fingerprint.REPORTED_FIELDS or what not in rep:
        rep[what] = after
    ROOT.mkdir(parents=True, exist_ok=True)
    (ROOT / "reported.json").write_text(json.dumps(rep, indent=1))
    return _append("interventions.jsonl", rec)


def claim(quantity, value, fp, evidence, unit="", interval=None, method="", note="", status="supported"):
    """Write down a measured quantity under a fingerprint, with the runs that back it.

    ``status`` is 'supported' or 'refuted' (a prediction that did not hold is worth as much
    as one that did). Staleness is not a status; it is derived from the fingerprint.
    """
    rec = {"id": uuid.uuid4().hex[:8], "t": time.time(), "quantity": quantity, "value": value,
           "unit": unit, "interval": interval, "fingerprint": fp["key"], "evidence": list(evidence),
           "method": method, "status": status, "note": note}
    return _append("claims.jsonl", rec)


def claims(quantity=None):
    out = _read("claims.jsonl")
    return [c for c in out if quantity is None or c["quantity"] == quantity]


def known(fp, quantity=None):
    """Claims that hold under ``fp``: same key, latest per quantity."""
    latest = {}
    for c in claims(quantity):
        if c["fingerprint"] == fp["key"]:
            latest[c["quantity"]] = c
    return latest


def stale(fp, quantity=None):
    """Claims made under some other fingerprint, with what changed since, when known."""
    fps = {}
    for c in claims(quantity):
        if c["fingerprint"] != fp["key"]:
            fps.setdefault(c["fingerprint"], []).append(c)
    return fps


def interventions():
    return _read("interventions.jsonl")


def new_run(kind, params, fp):
    """A directory for one experiment's evidence; returns (run_id, path)."""
    run_id = time.strftime("%Y%m%d-%H%M%S") + "-" + kind
    path = ROOT / "runs" / run_id
    path.mkdir(parents=True, exist_ok=False)
    (path / "params.json").write_text(json.dumps({"kind": kind, **params}, indent=1, default=str))
    (path / "fingerprint.json").write_text(json.dumps(fp, indent=1))
    return run_id, path
