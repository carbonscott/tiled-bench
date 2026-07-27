"""Egress (read-path) benchmark harness for a Tiled catalog server.

Three files, one job each:

    methods.py  what is tested   — one function per retrieval path, named for its Tiled API
    runner.py   what is measured — the timed region and the app/net/nav/decode split
    cli.py      args in, measured JSON out

The harness measures; it does not describe. Dataset facts (artifact, layout, stack size,
mimetype, entity count) are read off the Tiled server by whoever drives the CLI and
recorded by them alongside the JSON. Data creation and registration are one-off steps —
see docs/agent-runbook-egress-benchmark.md.
"""
