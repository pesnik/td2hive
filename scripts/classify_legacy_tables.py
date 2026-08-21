#!/usr/bin/env python3
"""Ranks the legacy-pipeline tables by migration risk, so onboarding via
scaffold_job.py/td2hive validate happens where it actually matters
first, not opportunistically or in alphabetical order.

Risk signal is grounded in two facts, not a guess:
  1. A WRITE_NOS + Hive-on-MR INSERT silently writing zero rows while
     reporting success is a real, previously-confirmed failure mode for
     at least one table's shape in this deployment: EXPORT_METHOD=WRITE_NOS
     combined with a dynamic (valueless) partition column, which routes
     the legacy pipeline's INSERT through
     `INSERT INTO TABLE ... PARTITION(dynamic_col)` + MSCK REPAIR instead
     of a plain ADD PARTITION. Any other table sharing that same
     EXPORT_METHOD=WRITE_NOS + dynamic partition shape is exposed to the
     same documented risk class - adjust the specific legacy table name
     in your own deployment's history/docs if this heuristic needs
     tightening.
  2. The legacy audit log's own data_mismatch column (adjust the table/
     column names below to match your own legacy schema) - a real,
     already-recorded history of source/target row-count disagreement,
     independent of any theory about *why*. A table with any recorded
     mismatch is empirically at-risk regardless of its EXPORT_METHOD/
     partition shape.

Tables already on EXPORT_METHOD=TPT are reported separately as already
migrated. Everything else with neither risk signal stays classified
low-risk and is NOT touched - no opportunistic migration: migrate the
at-risk bucket first, one table at a time, each through
scaffold_job.py -> td2hive validate -> promote, leaving tables with a
clean history on the legacy path.

This is read-only against the legacy config/audit tables - no jobs/
file is written, no Hive/OBS/Teradata call is made. Just a query and a
ranked report.
"""

from dataclasses import dataclass
from typing import List, Optional

import click
import pymysql


@dataclass
class TableRisk:
    hive_tab_name: str
    export_method: str
    skip: str
    partitions_raw: str
    has_dynamic_partition: bool
    total_runs: int = 0
    mismatch_count: int = 0
    last_backup_date: Optional[str] = None

    @property
    def already_migrated(self) -> bool:
        return self.export_method == "TPT"

    @property
    def at_risk(self) -> bool:
        if self.already_migrated:
            return False
        return self.mismatch_count > 0 or (self.has_dynamic_partition and self.export_method != "TPT")

    @property
    def risk_reasons(self) -> List[str]:
        reasons = []
        if self.mismatch_count > 0:
            reasons.append(f"{self.mismatch_count}/{self.total_runs} recorded run(s) had data_mismatch=1")
        if self.has_dynamic_partition and self.export_method != "TPT":
            reasons.append(
                f"dynamic partition ({self.partitions_raw}) still on EXPORT_METHOD={self.export_method} - "
                f"the same shape that has silently written zero rows under WRITE_NOS in this deployment before"
            )
        return reasons


def _has_dynamic_partition(hive_table_partitions: str) -> bool:
    return any(
        part.strip() and "=" not in part
        for part in (hive_table_partitions or "").split(",")
    )


def _fetch_configs(cur) -> List[TableRisk]:
    cur.execute(
        "SELECT HIVE_TAB_NAME, EXPORT_METHOD, SKIP, HIVE_TABLE_PARTITIONS "
        "FROM td_backcup_config ORDER BY HIVE_TAB_NAME"
    )
    rows = []
    for hive_tab_name, export_method, skip, partitions_raw in cur.fetchall():
        partitions_raw = partitions_raw or ""
        rows.append(TableRisk(
            hive_tab_name=hive_tab_name,
            export_method=(export_method or "WRITE_NOS").strip().upper(),
            skip=(skip or "N").strip().upper(),
            partitions_raw=partitions_raw,
            has_dynamic_partition=_has_dynamic_partition(partitions_raw),
        ))
    return rows


def _attach_audit_history(cur, tables: List[TableRisk]) -> None:
    """Joins on target_table (the audit log's own name for the Hive table
    a run wrote to) - the same HIVE_TAB_NAME this classifier keys on."""
    cur.execute(
        "SELECT target_table, COUNT(*), COALESCE(SUM(data_mismatch), 0), MAX(backup_date) "
        "FROM td_backup_audit_log GROUP BY target_table"
    )
    history = {row[0]: row for row in cur.fetchall()}
    for t in tables:
        row = history.get(t.hive_tab_name)
        if row:
            t.total_runs = row[1]
            t.mismatch_count = int(row[2])
            t.last_backup_date = str(row[3]) if row[3] else None


@click.command()
@click.option("--mysql-host", required=True, envvar="MYSQL_HOST")
@click.option("--mysql-port", default=3306, envvar="MYSQL_PORT")
@click.option("--mysql-user", required=True, envvar="MYSQL_USER")
@click.option("--mysql-password", required=True, envvar="MYSQL_PASSWORD")
@click.option("--mysql-database", default="monitoring", envvar="MYSQL_DATABASE")
@click.option("--include-skipped", is_flag=True, help="Also list tables with SKIP=Y (excluded from backup entirely)")
def main(
    mysql_host: str, mysql_port: int, mysql_user: str, mysql_password: str, mysql_database: str,
    include_skipped: bool,
):
    conn = pymysql.connect(
        host=mysql_host, user=mysql_user, password=mysql_password,
        port=mysql_port, database=mysql_database,
    )
    try:
        cur = conn.cursor()
        tables = _fetch_configs(cur)
        _attach_audit_history(cur, tables)
    finally:
        conn.close()

    if not include_skipped:
        tables = [t for t in tables if t.skip != "Y"]

    migrated = [t for t in tables if t.already_migrated]
    at_risk = [t for t in tables if t.at_risk]
    low_risk = [t for t in tables if not t.already_migrated and not t.at_risk]

    at_risk.sort(key=lambda t: (-t.mismatch_count, t.hive_tab_name))

    click.echo(f"=== ALREADY MIGRATED ({len(migrated)}) - EXPORT_METHOD=TPT, no action needed ===")
    for t in migrated:
        click.echo(f"  {t.hive_tab_name}")

    click.echo(f"\n=== AT RISK ({len(at_risk)}) - migrate these first, one at a time via scaffold_job.py -> td2hive validate ===")
    for t in at_risk:
        click.echo(f"  {t.hive_tab_name} (EXPORT_METHOD={t.export_method}, last_run={t.last_backup_date or 'never recorded'})")
        for reason in t.risk_reasons:
            click.echo(f"      - {reason}")

    click.echo(f"\n=== LOW RISK ({len(low_risk)}) - clean history, stays on the legacy path, no opportunistic migration ===")
    for t in low_risk:
        click.echo(f"  {t.hive_tab_name}")


if __name__ == "__main__":
    main()
