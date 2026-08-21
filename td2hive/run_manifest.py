#!/usr/bin/env python3
"""Persists which units of a `run()` invocation already finished, so a
retry after a transient failure (disk full, network blip, OOM) resumes
from where it left off instead of redoing already-completed work -
confirmed a real, not hypothetical, cost 2026-08-21: two concurrent
production jobs failed on "No space left on device" mid-write, and a
bare re-run would have redone every unit's TPT export from scratch,
including units that had already exported (or even fully written and
verified) successfully before the failure.

Scoped deliberately to `run()`'s sequential single-process path only.
`run-unit` used via a real orchestrator (k8s/Airflow/Argo) already gets
equivalent retry-without-redoing-everything behavior for free from the
orchestrator's own per-task retry, since each `run-unit` call is already
atomic to one partition value - this manifest exists specifically to
give the sequential path that same property without an orchestrator.

Storage is pluggable via ManifestStore, mirroring audit/'s AuditSink
design - a package can't assume where an adopter wants this state to
live any more than it can assume that for audit records.
JSONLManifestStore (below) is the zero-config default - one small
append-only file per (job, processing_date) run, not one shared file
for every table and every day this pipeline has ever run. That's a
deliberate difference from JSONLFileAuditSink's single-file design:
audit history is meant to accumulate forever for reporting; a manifest
entry is permanently useless the moment its run reaches success (there's
nothing left to resume), so it belongs in its own small, easily-found,
easily-pruned file - same directory convention already used everywhere
else in this package (tpt_output/<table>/<date>/, datax run logs, etc.),
not a growing log a human would have to grep through every table's every
day to find one run's state. A SQL-backed store (matching
audit.SQLAuditSink's pattern, for centralized visibility across many
tables' in-flight runs) is a natural future addition once there's real
demand for it - not built until then, same posture SQL support for audit
itself had.
"""

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Protocol

from .job_runner import Unit


@dataclass
class UnitState:
    status: str  # "exported" | "written"
    csv_paths: List[str] = field(default_factory=list)
    records_read: int = 0
    records_failed: int = 0


class ManifestStore(Protocol):
    def record_event(
        self, job_name: str, processing_date: str, unit_key: str, state: UnitState
    ) -> None: ...

    def load_state(self, job_name: str, processing_date: str) -> Dict[str, UnitState]: ...

    def clear(self, job_name: str, processing_date: str) -> None: ...


class JSONLManifestStore:
    """Default: one small append-only JSONL file per (job, processing_date)
    run, at <base_dir>/<job_name>/<processing_date>/manifest.jsonl - find
    "what happened for table X on date Y" by opening that one file, not
    grepping a single ever-growing shared log. Append-only *within* a
    run's file for the same crash-safety reason JSONLFileAuditSink is
    append-only (a partial last line at worst, never a half-written
    file); the file itself is deleted once the run no longer needs it
    (see clear(), called by run() on success)."""

    def __init__(self, base_dir: Path):
        self.base_dir = Path(base_dir)

    def _path(self, job_name: str, processing_date: str) -> Path:
        return self.base_dir / job_name / processing_date / "manifest.jsonl"

    def record_event(
        self, job_name: str, processing_date: str, unit_key: str, state: UnitState
    ) -> None:
        path = self._path(job_name, processing_date)
        path.parent.mkdir(parents=True, exist_ok=True)
        event = {"unit_key": unit_key, **asdict(state)}
        with open(path, "a") as f:
            f.write(json.dumps(event) + "\n")

    def load_state(self, job_name: str, processing_date: str) -> Dict[str, UnitState]:
        path = self._path(job_name, processing_date)
        result: Dict[str, UnitState] = {}
        if not path.exists():
            return result
        with open(path) as f:
            for line in f:
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                unit_key = event.get("unit_key")
                if not unit_key:
                    continue
                result[unit_key] = UnitState(
                    status=event["status"],
                    csv_paths=event.get("csv_paths", []),
                    records_read=event.get("records_read", 0),
                    records_failed=event.get("records_failed", 0),
                )
        return result

    def clear(self, job_name: str, processing_date: str) -> None:
        self._path(job_name, processing_date).unlink(missing_ok=True)


class RunManifest:
    """Per-(job, processing_date) view over a ManifestStore - what
    job_runner.py actually calls. Doesn't load anything on construction;
    call load() explicitly right before use, so a `--force` run can skip
    it entirely and start from a clean slate without the store needing
    its own separate "clear" operation."""

    def __init__(self, store: ManifestStore, job_name: str, processing_date: str):
        self.store = store
        self.job_name = job_name
        self.processing_date = processing_date
        self.units: Dict[str, UnitState] = {}

    def load(self) -> "RunManifest":
        self.units = self.store.load_state(self.job_name, self.processing_date)
        return self

    def clear(self) -> None:
        """Call once this run reaches success - the manifest is
        permanently useless past that point (nothing left to resume),
        and JSONLManifestStore's per-run file layout makes deleting it a
        cheap, safe, local operation, unlike a shared log where "clear"
        would mean surgically removing lines."""
        self.units = {}
        self.store.clear(self.job_name, self.processing_date)

    @staticmethod
    def key_for(unit: Unit) -> str:
        # Must include load_table, not just file_label - two load tables
        # can share a partition value (see job_runner.py's _run_batch
        # run_dir comment for the real bug this exact ambiguity caused
        # elsewhere).
        return f"{unit.load_table}:{unit.file_label or 'static'}"

    def get(self, unit: Unit) -> Optional[UnitState]:
        return self.units.get(self.key_for(unit))

    def valid_exported_csv_paths(self, unit: Unit) -> Optional[List[Path]]:
        """Returns the unit's previously-exported CSV paths if it's
        marked `exported` AND every file is still present and non-empty
        - a cheap sanity check (existence + size), not a full re-parse,
        before trusting a prior run's claim that the export succeeded.
        None if the unit was never exported, is already `written`, or
        its files are gone/truncated (a full re-export is then needed)."""
        state = self.get(unit)
        if state is None or state.status != "exported":
            return None
        paths = [Path(p) for p in state.csv_paths]
        if not paths or not all(p.exists() and p.stat().st_size > 0 for p in paths):
            return None
        return paths

    def is_written(self, unit: Unit) -> bool:
        state = self.get(unit)
        return state is not None and state.status == "written"

    def mark_exported(self, unit: Unit, csv_paths: List[Path]) -> None:
        key = self.key_for(unit)
        state = UnitState(status="exported", csv_paths=[str(p) for p in csv_paths])
        self.units[key] = state
        self.store.record_event(self.job_name, self.processing_date, key, state)

    def mark_written(self, unit: Unit, records_read: int, records_failed: int) -> None:
        key = self.key_for(unit)
        existing = self.units.get(key, UnitState(status="exported"))
        state = UnitState(
            status="written",
            csv_paths=existing.csv_paths,
            records_read=records_read,
            records_failed=records_failed,
        )
        self.units[key] = state
        self.store.record_event(self.job_name, self.processing_date, key, state)
