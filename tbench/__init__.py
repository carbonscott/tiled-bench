"""tbench: small functions for measuring a Tiled server, meant to be driven by a script an
agent writes for one question. See README.md for the shape and .claude/skills/ for the
questions."""

from . import load, probes, reads, timing  # noqa: F401
from .load import Round, Run, run  # noqa: F401
