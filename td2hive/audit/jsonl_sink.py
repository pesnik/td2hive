#!/usr/bin/env python3
"""Default audit sink: one JSON line per run, appended to a local file.
Zero config, zero external dependency, always available - every install
gets a working audit trail with no setup. This is the baseline every other
sink sits alongside, not a fallback bolted onto a database-only design.
"""

import json
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

from . import AuditRecord


def _default(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Not JSON serializable: {obj!r}")


class JSONLFileAuditSink:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def record(self, run: AuditRecord) -> None:
        with open(self.path, "a") as f:
            f.write(json.dumps(asdict(run), default=_default) + "\n")

    def find_success(self, job_name: str, processing_date: str) -> bool:
        if not self.path.exists():
            return False
        with open(self.path) as f:
            for line in f:
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if (
                    rec.get("job_name") == job_name
                    and rec.get("processing_date") == processing_date
                    and rec.get("status") == "success"
                ):
                    return True
        return False
