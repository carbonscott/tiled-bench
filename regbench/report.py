"""Append :class:`RunResult` rows to a CSV — one row per (config × rep).

Column order is fixed by ``config.csv_fields()`` so appending across sessions stays aligned.
Header is written only when the file is new/empty.
"""

import csv
from dataclasses import asdict
from pathlib import Path

from .config import RunResult, csv_fields


class CsvReporter:
    """Streaming CSV writer. Open once per benchmark session; ``append`` each result."""

    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fields = csv_fields()

    def _needs_header(self) -> bool:
        return not self.path.exists() or self.path.stat().st_size == 0

    def append(self, result: RunResult):
        write_header = self._needs_header()
        with open(self.path, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=self._fields)
            if write_header:
                writer.writeheader()
            writer.writerow(asdict(result))

    def append_all(self, results):
        for r in results:
            self.append(r)
