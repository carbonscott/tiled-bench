"""Egress (read-path) benchmark harness for a Tiled catalog server.

Minimal CLI-driven sweep space — dataset (size × layout) × retrieval method ×
concurrency × location — one self-describing CSV row per run, so an agent loop can
iterate: run points → read CSV → pick next points.

Data creation and registration are one-off agentic steps, not part of this package.
See docs/agent-runbook-egress-benchmark.md.
"""
