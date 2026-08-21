#!/usr/bin/env python3
"""Hive partition registration and counting via beeline.

Runs `beeline` directly, so this must execute on a host with beeline
installed and (if your cluster requires it) already Kerberos-authenticated.
"""

import queue
import re
import subprocess
import tempfile
import threading
from dataclasses import dataclass
from typing import Callable, List, Optional, Tuple

from loguru import logger


@dataclass
class PartitionSpec:
    """One partition's column=value pairs, e.g. {"processing_date": "2026-08-20",
    "DATE_KEY": "7901"} -> `PARTITION (processing_date="2026-08-20", DATE_KEY="7901")`."""

    values: dict

    def to_sql(self) -> str:
        parts = ", ".join(f'{col}="{val}"' for col, val in self.values.items())
        return parts


class PartitionRegistrar:
    QUERY_TIMEOUT = 3600
    DESCRIBE_TIMEOUT = 300

    def register(
        self,
        schema: str,
        table: str,
        has_dynamic_column: bool,
        partition_spec: Optional[PartitionSpec] = None,
        location: Optional[str] = None,
    ) -> None:
        """MSCK REPAIR for any table with a dynamic partition column (scans
        the whole table's directory tree for all partition subdirs at once
        - needed because a single run can produce multiple partition-value
        subdirectories), otherwise a single ADD PARTITION for the static
        case. Note: the DataX loader in this package doesn't actually rely
        on this MSCK path - job_runner.py already knows every partition
        value it wrote and registers each one explicitly via add_partition
        instead (see job_runner.py). This dynamic-column/MSCK branch exists
        for callers that genuinely can't know partition values in advance
        (e.g. a Hive INSERT ... PARTITION(dynamic_col) that discovers
        values as a side effect)."""
        if has_dynamic_column:
            self.repair_table(schema, table)
            return
        if partition_spec is None or location is None:
            raise ValueError("Static partition registration requires partition_spec and location")
        self.add_partition(schema, table, partition_spec.to_sql(), location)

    def repair_table(self, schema: str, table: str) -> bool:
        try:
            self._execute_query(f"MSCK REPAIR TABLE {schema}.{table}", "table repair")
            return True
        except Exception:
            return False

    def add_partition(self, schema: str, table: str, partition_spec: str, location: str) -> bool:
        try:
            self._execute_query(
                f"ALTER TABLE {schema}.{table} ADD IF NOT EXISTS "
                f"PARTITION ({partition_spec}) LOCATION '{location}'",
                "add partition",
            )
            return True
        except Exception:
            return False

    def drop_partition(self, schema: str, table: str, partition_spec: str) -> bool:
        """Drop one partition's metastore entry - retention.py's caller is
        responsible for deleting the underlying OBS data separately
        (obs_client.delete_prefix); dropping the metastore entry does not
        delete data for an EXTERNAL table, by design (matches every other
        table operation in this package: Hive metadata and OBS storage are
        always handled as two explicit, independently-verifiable steps,
        never one operation assumed to imply the other)."""
        try:
            self._execute_query(
                f"ALTER TABLE {schema}.{table} DROP IF EXISTS PARTITION ({partition_spec})",
                "drop partition",
            )
            return True
        except Exception:
            return False

    def get_table_count(self, schema: str, table: str, where_clause: str = "") -> int:
        try:
            where_sql = f"WHERE {where_clause}" if where_clause else ""
            result = self._run_command(
                f"SELECT COUNT(*) FROM {schema}.{table} {where_sql}", self.QUERY_TIMEOUT
            )
            return self._extract_count_from_result(result)
        except Exception as e:
            logger.error(f"Failed to get Hive table count: {e}")
            return 0

    def describe_table(self, schema: str, table: str) -> List[Tuple[str, str]]:
        """Real Hive column order + type for schema.table, via DESCRIBE.
        This is the authoritative order for anything writing Parquet into
        this table - hdfswriter writes columns positionally, Hive reads
        Parquet back positionally against its own DDL, so this (never a
        legacy config's own column-list field, and never the source
        table's own column order) is the one order that actually matters.

        Stops at the first blank-name or `#`-prefixed row, which is where
        Hive's DESCRIBE output for a partitioned table repeats every
        partition column a second time under "# Partition Information" -
        only the first (inline) listing is real physical write order. A
        dynamic partition column still appears in that inline listing, in
        its real position - Hive's dynamic-partition INSERT expects it
        trailing the regular columns, not absent from them, and DataX
        writes need to match that same shape."""
        result = self._run_command(f"DESCRIBE {schema}.{table}", self.DESCRIBE_TIMEOUT)
        columns: List[Tuple[str, str]] = []
        for line in result.splitlines():
            line = line.strip()
            if not (line.startswith("|") and line.endswith("|")):
                continue
            parts = [p.strip() for p in line.strip("|").split("|")]
            name = parts[0] if parts else ""
            if name.lower() == "col_name":
                continue  # header row
            if not name or name.startswith("#"):
                break  # blank separator or "# Partition Information" - real columns end here
            dtype = parts[1] if len(parts) > 1 else ""
            columns.append((name, dtype))
        return columns

    def table_exists(self, schema: str, table: str) -> bool:
        """DESCRIBE on a nonexistent table fails (beeline exits nonzero,
        _run_command raises) - treat that as "doesn't exist" rather than
        propagating, since callers (create-target) need this as a plain
        existence check, not an error signal."""
        try:
            return bool(self.describe_table(schema, table))
        except Exception:
            return False

    def drop_table(self, schema: str, table: str) -> None:
        try:
            self._execute_query(
                f"DROP TABLE IF EXISTS {schema}.{table} PURGE", f"drop table {table}"
            )
        except Exception as e:
            logger.warning(f"Failed to drop table {table}: {e}")

    def create_external_table(
        self,
        schema: str,
        table: str,
        columns: List[Tuple[str, str]],
        location: str,
        file_format: str = "PARQUET",
        row_format_delimited_by: Optional[str] = None,
        partition_columns: List[Tuple[str, str]] = (),
    ) -> None:
        """columns is [(name, hive_type), ...]. Set row_format_delimited_by
        for TEXT format (e.g. '|'), leave None for PARQUET/ORC.
        `partition_columns` (also [(name, hive_type), ...], e.g.
        [("processing_date", "STRING"), ("date_key", "STRING")]) adds a
        PARTITIONED BY clause - needed for any table with a dynamic
        partition column; omit for a flat table."""
        columns_def = ",\n".join(f"`{name}` {hive_type}" for name, hive_type in columns)
        row_format = ""
        if row_format_delimited_by:
            row_format = (
                f"ROW FORMAT DELIMITED\nFIELDS TERMINATED BY '{row_format_delimited_by}'\n"
            )
        partitioned_by = ""
        if partition_columns:
            partition_def = ", ".join(f"`{name}` {hive_type}" for name, hive_type in partition_columns)
            partitioned_by = f"PARTITIONED BY ({partition_def})\n"
        create_query = f"""
            DROP TABLE IF EXISTS {schema}.{table} PURGE;
            CREATE EXTERNAL TABLE {schema}.{table} (
                {columns_def}
            )
            {partitioned_by}{row_format}STORED AS {file_format}
            LOCATION '{location}';
        """
        self._execute_query(create_query, f"create external table {table}")

    def _extract_count_from_result(self, result: str) -> int:
        lines = result.splitlines()
        for line in lines:
            line = line.strip()
            if line.startswith("|") and line.endswith("|") and line != "+------+":
                if "_c" in line or "count" in line.lower():
                    continue
                parts = [part.strip() for part in line.split("|") if part.strip()]
                if parts and parts[0].isdigit():
                    return int(parts[0])
        return 0

    def _execute_query(self, query: str, operation: str) -> None:
        try:
            self._run_command(query, self.QUERY_TIMEOUT)
            logger.info(f"Hive {operation} succeeded")
        except Exception as e:
            logger.error(f"Hive {operation} failed: {e}")
            raise

    def _run_command(
        self,
        query: str,
        timeout: int,
        output_callback: Optional[Callable[[str], None]] = None,
    ) -> str:
        with tempfile.NamedTemporaryFile("w", suffix=".hql") as tmp:
            tmp.write(query)
            tmp.flush()

            process = subprocess.Popen(
                ["beeline", "-f", tmp.name],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
            )

            stdout_queue: queue.Queue = queue.Queue()
            stderr_queue: queue.Queue = queue.Queue()

            def read_stdout():
                for line in iter(process.stdout.readline, ""):  # type: ignore
                    stdout_queue.put(("stdout", line.rstrip()))
                stdout_queue.put(("stdout", None))

            def read_stderr():
                for line in iter(process.stderr.readline, ""):  # type: ignore
                    stderr_queue.put(("stderr", line.rstrip()))
                stderr_queue.put(("stderr", None))

            stdout_thread = threading.Thread(target=read_stdout, daemon=True)
            stderr_thread = threading.Thread(target=read_stderr, daemon=True)
            stdout_thread.start()
            stderr_thread.start()

            output_lines: List[str] = []
            error_lines: List[str] = []
            stdout_done = stderr_done = False

            while not (stdout_done and stderr_done):
                try:
                    source, line = stdout_queue.get(timeout=1)
                    if line is None:
                        stdout_done = True
                    else:
                        output_lines.append(line)
                        if output_callback:
                            output_callback(line)
                except queue.Empty:
                    pass
                try:
                    source, line = stderr_queue.get(timeout=0.1)
                    if line is None:
                        stderr_done = True
                    else:
                        error_lines.append(line)
                except queue.Empty:
                    pass

            process.wait(timeout=timeout)
            output = "\n".join(output_lines)
            if process.returncode != 0:
                raise RuntimeError(
                    f"beeline exited {process.returncode}: {chr(10).join(error_lines[-20:])}"
                )
            return output
