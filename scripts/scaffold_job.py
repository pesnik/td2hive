#!/usr/bin/env python3
"""Scaffolds a draft td2hive job YAML from a legacy MySQL config table
(schema `td_backcup_config`, adjust `_fetch_legacy_config`'s query for
your own legacy table name/shape), cross-checked against the REAL Hive
target table's column order via DESCRIBE - never the legacy config's own
COLUMNS field, which is only ever used here as a hint to warn about, not
a source of truth. hdfswriter writes Parquet columns positionally and
Hive reads them back positionally against its own DDL, so a legacy
config's column order silently disagreeing with the real Hive DDL order
is a real risk, not a hypothetical one - there is no way to catch that
class of bug except by actually comparing against the real DDL, every
time, for every table. (The Hive target's order is what matters here,
not the source table's own column order; the source side is still
checked, but only for column *existence* and a resolvable type, via
column_types.resolve_column_types - the same check td2hive's own real
run would do, just run here as a pre-flight instead of failing later.)

This only drafts a job spec - it does NOT validate the pipeline can
actually run it end to end. Run `td2hive validate` against the draft,
and get a clean result, before ever promoting it into jobs/ and pointing
production data at it. This script never writes into jobs/ itself -
promotion is a deliberate, reviewed step.

Usage:
  scripts/scaffold_job.py <table_name> [options] > jobs/drafts/<table_name>.yaml

<table_name> matches the legacy config's HIVE_TAB_NAME (case-insensitive)
- the same convention td2hive's own job YAMLs already use (lowercased
HIVE_TAB_NAME as both table_name and filename).
"""

import sys
from pathlib import Path
from typing import List, Optional, Tuple

import click
import pymysql
import teradatasql

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from td2hive.column_types import resolve_column_types  # noqa: E402
from td2hive.partition_registrar import PartitionRegistrar  # noqa: E402


def _fetch_legacy_config(
    mysql_host: str, mysql_port: int, mysql_user: str, mysql_password: str, mysql_database: str, table_name: str
) -> Optional[dict]:
    conn = pymysql.connect(
        host=mysql_host, user=mysql_user, password=mysql_password,
        port=mysql_port, database=mysql_database,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM td_backcup_config WHERE UPPER(HIVE_TAB_NAME) = UPPER(%s)",
                (table_name,),
            )
            row = cur.fetchone()
            if row is None:
                return None
            return dict(zip([col[0] for col in cur.description], row))
    finally:
        conn.close()


def _parse_partitions(hive_table_partitions: str) -> Tuple[List[str], List[str]]:
    """(static_column_names, dynamic_column_names) from the legacy
    config's comma-separated HIVE_TABLE_PARTITIONS field - `col=literal`
    is static, a bare `col` (no `=`) is dynamic. `processing_date` is
    never listed here in practice (it's implicit/hardcoded elsewhere in
    td2hive - see job_runner.py's _build_target_path) but excluded
    defensively here too if it does show up."""
    static, dynamic = [], []
    for part in (hive_table_partitions or "").split(","):
        part = part.strip()
        if not part:
            continue
        if "=" in part:
            name = part.split("=", 1)[0].strip()
            static.append(name)
        else:
            dynamic.append(part)
    return static, dynamic


def _real_source_columns(
    registrar: PartitionRegistrar, hive_owner: str, hive_table: str, static_partition_cols: List[str]
) -> List[str]:
    """The real Hive DDL's column order, minus processing_date and any
    static partition column - exactly what td2hive's own job_runner.py
    excludes via exclude_columns."""
    exclude = {"processing_date"} | {c.lower() for c in static_partition_cols}
    described = registrar.describe_table(hive_owner, hive_table)
    return [name for name, _ in described if name.lower() not in exclude]


def _teradata_source_columns(cursor, td_owner: str, load_table: str, static_partition_cols: List[str]) -> List[str]:
    """Teradata's own physical column order (DBC.ColumnsV.ColumnId) for
    one load table - the only fallback available for a genuinely new
    table, whose Hive target doesn't exist yet and so has no real DDL
    order to defer to instead. Once `td2hive create-target` creates the
    real table from a draft built this way, every future re-scaffold of
    the same table goes back to the real Hive DESCRIBE order via
    _real_source_columns - this fallback only ever fires once, for the
    table's first draft."""
    cursor.execute(
        "SELECT ColumnName FROM DBC.ColumnsV WHERE DatabaseName = ? AND TableName = ? ORDER BY ColumnId",
        [td_owner, load_table],
    )
    exclude = {c.lower() for c in static_partition_cols}
    return [name.strip() for (name,) in cursor.fetchall() if name.strip().lower() not in exclude]


def _render_yaml(
    table_name: str, td_owner: str, load_tables: List[str], columns: List[str],
    hive_owner: str, hive_table: str, obs_dir: str, dynamic_partitions: List[str],
    retention_days: Optional[int], speed_channel: int,
) -> str:
    load_tables_yaml = "\n".join(f"      - {t}" for t in load_tables)
    columns_yaml_lines = []
    for col in columns:
        if col.lower() in {p.lower() for p in dynamic_partitions}:
            columns_yaml_lines.append(f"    - {col}   # dynamic partition column - excluded from written data")
        else:
            columns_yaml_lines.append(f"    - {col}")
    columns_yaml = "\n".join(columns_yaml_lines)
    partitions_yaml = "\n".join(
        f"    - {{column: {p}, dynamic: true}}" for p in dynamic_partitions
    ) or "    []"
    retention_line = f"retention_days: {retention_days}\n" if retention_days else ""

    return f"""# DRAFT - scaffolded, not yet validated. Run `td2hive validate` against
# this file and confirm a clean row/field match before promoting it into
# jobs/ and pointing production data at it. speed_channel below is a
# conservative starting default, not a measured value - see README's
# "Scaling" section on choosing it from a real scratch-scale observation,
# never copying it from another table.
table_name: {table_name}
source:
  teradata:
    owner: {td_owner}
    load_tables:
{load_tables_yaml}
  columns:
{columns_yaml}
target:
  hive:
    owner: {hive_owner}
    table: {hive_table}
  obs_dir: {obs_dir}
  format: parquet   # not in the legacy config - confirm this is right for this table
  partitions:
{partitions_yaml}
loader: datax
{retention_line}setting:
  error_limit: {{record: 0, percentage: 0.0}}
  speed_channel: {speed_channel}
  write_mode: append
"""


@click.command()
@click.argument("table_name")
@click.option("--mysql-host", required=True, envvar="MYSQL_HOST")
@click.option("--mysql-port", default=3306, envvar="MYSQL_PORT")
@click.option("--mysql-user", required=True, envvar="MYSQL_USER")
@click.option("--mysql-password", required=True, envvar="MYSQL_PASSWORD")
@click.option("--mysql-database", default="monitoring", envvar="MYSQL_DATABASE")
@click.option("--td-host", required=True, envvar="TD_HOST")
@click.option("--td-user", required=True, envvar="TD_USER")
@click.option("--td-password", required=True, envvar="TD_PASSWORD")
@click.option("--speed-channel", default=4, help="Conservative starting default - tune via `td2hive validate`")
def main(
    table_name: str, mysql_host: str, mysql_port: int, mysql_user: str, mysql_password: str, mysql_database: str,
    td_host: str, td_user: str, td_password: str, speed_channel: int,
):
    config = _fetch_legacy_config(mysql_host, mysql_port, mysql_user, mysql_password, mysql_database, table_name)
    if config is None:
        raise click.ClickException(
            f"No row in {mysql_database}.td_backcup_config with HIVE_TAB_NAME = {table_name!r}"
        )

    td_owner = config.get("TD_LD_TAB_OWNER") or config["TD_TAB_OWNER"]
    load_tables_raw = config.get("TD_LD_TAB_NAME") or ""
    load_tables = [t.strip() for t in load_tables_raw.split(";") if t.strip()]
    if not load_tables:
        # No separate load table configured (the common case for most
        # tables - a dedicated load table is the exception, not the
        # rule). Falls back to the single source table directly, same
        # as the legacy pipeline's own standard-query path does when it
        # has no load table configured either.
        if not config.get("TD_TAB_NAME"):
            raise click.ClickException(
                f"{table_name}: neither TD_LD_TAB_NAME nor TD_TAB_NAME is set in the legacy "
                f"config - no source table to scaffold from."
            )
        load_tables = [config["TD_TAB_NAME"]]

    hive_owner = config["HIVE_TAB_OWNER"]
    hive_table = config["HIVE_TAB_NAME"]
    obs_dir = (config.get("TARGET_DIR") or "").lower()
    static_partition_cols, dynamic_partition_cols = _parse_partitions(config.get("HIVE_TABLE_PARTITIONS") or "")
    retention_days = int(config["RETENTION"]) if config.get("RETENTION") else None

    export_method = (config.get("EXPORT_METHOD") or "WRITE_NOS").strip().upper()
    if export_method != "TPT":
        click.echo(
            f"NOTE: legacy EXPORT_METHOD for {table_name} was {export_method!r}, not TPT - "
            f"scaffolding a new datax-loader job spec for it regardless.",
            err=True,
        )

    registrar = PartitionRegistrar()
    td_conn = teradatasql.connect(host=td_host, user=td_user, password=td_password)
    try:
        if registrar.table_exists(hive_owner, hive_table):
            real_columns = _real_source_columns(registrar, hive_owner, hive_table, static_partition_cols)
            if not real_columns:
                raise click.ClickException(
                    f"DESCRIBE {hive_owner}.{hive_table} returned no usable columns - "
                    f"is beeline reachable/authenticated here?"
                )
        else:
            click.echo(
                f"NOTE: {hive_owner}.{hive_table} doesn't exist in Hive yet - falling back to "
                f"{td_owner}.{load_tables[0]}'s own Teradata column order (DBC.ColumnsV) instead "
                f"of a real Hive DDL, since there isn't one yet. Run `td2hive create-target` "
                f"against the resulting draft to create the real table using exactly this order - "
                f"any future re-scaffold of this table will then use the real Hive DESCRIBE order "
                f"instead, same as every other already-onboarded table.",
                err=True,
            )
            real_columns = _teradata_source_columns(td_conn.cursor(), td_owner, load_tables[0], static_partition_cols)
            if not real_columns:
                raise click.ClickException(
                    f"DBC.ColumnsV returned no columns for {td_owner}.{load_tables[0]} - "
                    f"does this table exist?"
                )

        # Pre-flight: every column must actually exist (with a resolvable
        # type) in every Teradata source table - the same check td2hive's
        # own real run does via resolve_column_types(), just run here so a
        # typo'd/renamed column, or a mismatch across load tables, is
        # caught before ever writing a draft, not after a real run fails
        # partway through.
        try:
            resolve_column_types(td_conn.cursor(), td_owner, load_tables, real_columns)
        except ValueError as e:
            raise click.ClickException(
                f"Pre-flight check against Teradata failed: {e}\n"
                f"Every column must exist, with a resolvable type, in "
                f"{td_owner}.{{{', '.join(load_tables)}}} - fix the mismatch before this table "
                f"can be scaffolded."
            )
    finally:
        td_conn.close()

    legacy_columns = [c.strip() for c in (config.get("COLUMNS") or "").split(",") if c.strip()]
    legacy_upper = [c.upper() for c in legacy_columns]
    real_upper = [c.upper() for c in real_columns]
    if legacy_upper and legacy_upper != real_upper:
        if sorted(legacy_upper) == sorted(real_upper):
            click.echo(
                f"WARNING: legacy config's COLUMNS order disagrees with the real Hive DDL order "
                f"for {hive_owner}.{hive_table} (same columns, different order) - the legacy "
                f"order was NOT used. This is exactly the class of bug that silently corrupts "
                f"data with positional Parquet writing.\n"
                f"  legacy config order: {', '.join(legacy_columns)}\n"
                f"  real Hive DDL order: {', '.join(real_columns)}",
                err=True,
            )
        else:
            click.echo(
                f"WARNING: legacy config's COLUMNS list doesn't even contain the same columns "
                f"as the real Hive DDL for {hive_owner}.{hive_table} - double check this is the "
                f"right table before trusting this draft.\n"
                f"  legacy config columns: {', '.join(legacy_columns)}\n"
                f"  real Hive DDL columns: {', '.join(real_columns)}",
                err=True,
            )

    yaml_text = _render_yaml(
        table_name=table_name.lower(),
        td_owner=td_owner,
        load_tables=load_tables,
        columns=real_columns,
        hive_owner=hive_owner,
        hive_table=hive_table,
        obs_dir=obs_dir,
        dynamic_partitions=dynamic_partition_cols,
        retention_days=retention_days,
        speed_channel=speed_channel,
    )
    click.echo(yaml_text)


if __name__ == "__main__":
    main()
