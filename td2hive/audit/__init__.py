#!/usr/bin/env python3
"""Pluggable audit: AuditRecord is the one fixed schema, sink-independent.
Every sink implements the same tiny Protocol - a package can't assume what
audit/lineage destination an adopter already runs (MySQL, Postgres,
Marquez, DataHub, or just a local file), so the destination is
configuration, not a built-in assumption.
"""

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Protocol


@dataclass
class AuditRecord:
    job_name: str
    processing_date: str
    source_schema: str
    source_table: str
    hive_schema: str
    hive_table: str
    source_row_count: int
    target_row_count: int
    status: str  # success | dq_mismatch | failed
    start_time: datetime
    end_time: datetime
    loader: str = "datax"  # datax | legacy_write_nos | legacy_tpt_csv_stage
    # DataX's own self-reported count - stored for telemetry only, never
    # used to decide status (see verify.py for why).
    datax_reported_count: Optional[int] = None
    error_detail: Optional[str] = None
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    @property
    def duration_seconds(self) -> float:
        return (self.end_time - self.start_time).total_seconds()


class AuditSink(Protocol):
    def record(self, run: AuditRecord) -> None: ...

    def find_success(self, job_name: str, processing_date: str) -> bool:
        """Used for idempotency: has this (job, date) already succeeded?
        Sinks that can't look records back up (e.g. a write-only lineage
        emitter) should raise NotImplementedError - CompositeAuditSink
        requires at least one lookup-capable sink, enforced at config-load
        time, so a real answer is always available."""
        ...


class CompositeAuditSink:
    """Fans out to every configured sink. One sink failing logs and moves
    on - it must never block the others or the run itself."""

    def __init__(self, sinks: List[AuditSink]):
        if not sinks:
            raise ValueError("CompositeAuditSink requires at least one sink")
        self.sinks = sinks
        if not any(self._supports_lookup(s) for s in sinks):
            raise ValueError(
                "At least one configured sink must support find_success "
                "(JSONLFileAuditSink or SQLAuditSink) for idempotency checks"
            )

    @staticmethod
    def _supports_lookup(sink: AuditSink) -> bool:
        try:
            sink.find_success.__func__ is not None  # type: ignore
            return True
        except AttributeError:
            return False

    def record(self, run: AuditRecord) -> None:
        from loguru import logger

        for sink in self.sinks:
            try:
                sink.record(run)
            except Exception as e:
                logger.error(f"Audit sink {sink.__class__.__name__} failed: {e}")

    def find_success(self, job_name: str, processing_date: str) -> bool:
        for sink in self.sinks:
            try:
                return sink.find_success(job_name, processing_date)
            except NotImplementedError:
                continue
        return False
