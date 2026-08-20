#!/usr/bin/env python3
"""Independent Teradata-vs-Hive row count comparison. This is the ONLY
module allowed to decide status=success/dq_mismatch for a run.

Why this exists as its own module, not folded into datax/runner.py: the
original bug this whole rewrite traces back to was Hive's own INSERT
reporting success in seconds for 568M rows while writing zero real rows -
a component's self-report is exactly the thing that must never be trusted
as the verdict. DataX's own summary (datax/runner.py's DataxRunResult) is
a fast-fail signal only; it is never consulted here.
"""

from dataclasses import dataclass
from typing import List, Optional

from .partition_registrar import PartitionRegistrar


@dataclass
class VerifyResult:
    source_count: int
    target_count: int
    matched: bool

    @property
    def status(self) -> str:
        return "success" if self.matched else "dq_mismatch"


def get_source_count(cursor, schema: str, tables: List[str], where_clause: str = "") -> int:
    """Sum of row counts across every source (load) table - a load-table
    config can list more than one Teradata table backing a single Hive
    target, and every one of them counts toward the source total."""
    total = 0
    for table in tables:
        where_sql = f"WHERE {where_clause}" if where_clause else ""
        cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table} {where_sql}")
        (count,) = cursor.fetchone()
        total += count
    return total


def verify(
    td_cursor,
    source_schema: str,
    source_tables: List[str],
    hive_schema: str,
    hive_table: str,
    source_where: str = "",
    hive_where: str = "",
    registrar: Optional[PartitionRegistrar] = None,
) -> VerifyResult:
    """Independent count on both sides, own connections/queries - never
    trusts a prior step's self-reported count."""
    registrar = registrar or PartitionRegistrar()
    source_count = get_source_count(td_cursor, source_schema, source_tables, source_where)
    target_count = registrar.get_table_count(hive_schema, hive_table, hive_where)
    return VerifyResult(
        source_count=source_count,
        target_count=target_count,
        matched=source_count == target_count,
    )
