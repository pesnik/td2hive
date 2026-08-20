#!/usr/bin/env python3
"""Thin coordinator: export (TPT, directly split by partition value and
directly chunked into parallel files) -> build job.json -> run DataX
(fast-fail check only) -> register partition -> verify (the real verdict)
-> audit. No business logic of its own - every step below is a call into
a module that owns that logic and can be tested/reused on its own. Kept
intentionally small so this file doesn't become the next 1700-line
orchestrator.
"""

import shutil
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Optional

from loguru import logger

from .audit import AuditRecord
from .column_types import resolve_column_types
from .datax.distribution import resolve_datax_home, validate_distribution
from .datax.job_spec import build_job_json
from .datax.runner import DataxRunner
from .jobspec import JobSpec
from .partition_registrar import PartitionRegistrar, PartitionSpec
from .obs_client import delete_prefix, ensure_prefix_exists
from .reader import ObsConfig, TPTExporter
from .verify import verify


@dataclass
class RunPaths:
    tpt_output_dir: Path
    datax_logs_dir: Path


class JobRunner:
    def __init__(
        self,
        td_cursor,
        td_host: str,
        td_user: str,
        td_password: str,
        obs_config: ObsConfig,
        obs_bucket: str,
        audit_sink,
        paths: RunPaths,
        datax_home: Optional[str] = None,
    ):
        self.td_cursor = td_cursor
        self.tpt_exporter = TPTExporter(td_host, td_user, td_password, str(paths.tpt_output_dir))
        self.obs_config = obs_config
        self.obs_bucket = obs_bucket
        self.audit_sink = audit_sink
        self.paths = paths
        self.registrar = PartitionRegistrar()
        self.datax_home = resolve_datax_home(datax_home or "")
        validate_distribution(self.datax_home)
        self.runner = DataxRunner(self.datax_home)

    def run(
        self, job: JobSpec, processing_date: date, row_limit: int = 0, force: bool = False
    ) -> Optional[AuditRecord]:
        start_time = datetime.now()
        date_str = processing_date.strftime("%Y-%m-%d")

        if not force and self.audit_sink.find_success(job.table_name, date_str):
            logger.info(f"{job.table_name}/{date_str} already succeeded, skipping (use force=True to re-run)")
            return None

        columns = resolve_column_types(
            self.td_cursor, job.source.owner, job.source.load_tables, job.source.columns
        )

        dynamic_cols = [p.column for p in job.target.partitions if p.dynamic]
        # Also doubles as TPT's export-instance count (reader.TPTExporter's
        # num_instances) - one knob for "how parallel should this table's
        # load be," not two. TPT's DataConnector writes directly into this
        # many files, round-robin distributed (tbuild -C), so DataX's
        # channel count naturally matches (see build_job_json) - no local
        # CSV read/rewrite pass needed for either partitioning or
        # chunking. Validated 2026-08-20 against 2M real rows: exact file
        # count, near-even distribution, 0 rows lost.
        num_instances = max(1, job.setting.speed_channel)
        datax_records_read = 0
        datax_records_failed = 0
        # Every distinct partition_values combination actually written -
        # registered explicitly via ADD PARTITION below, never via MSCK
        # REPAIR's directory-tree auto-discovery. We already know every
        # value up front (queried directly, see below), so there is
        # nothing for MSCK to discover that we don't already have - and
        # MSCK REPAIR was found 2026-08-20 to fail unreliably against this
        # OBS backend for DataX-written directories (generic DDLTask
        # error, no usable server-side detail from the client), while
        # explicit ADD PARTITION against the identical data succeeded and
        # read back an exact row-count match.
        registered_partitions: dict = {}
        # Cleared once per unique target_path per run, not once per
        # load_table: multiple source load tables can land rows in the
        # SAME partition directory (e.g. two load tables both contributing
        # DATE_KEY=X rows to one table). Deleting on every load_table's
        # write would wipe out an earlier load_table's just-written data
        # in the same run. This delete-then-write is the sole idempotency
        # mechanism for this loader - hdfswriter's own writeMode stays
        # 'append' (see jobspec.RunSetting) precisely so it never also
        # tries to manage conflicts in the same directory.
        cleared_paths: set = set()

        for load_table in job.source.load_tables:
            for partition_values, where_clause, file_label in self._partition_value_scopes(
                job, load_table, dynamic_cols, columns
            ):
                csv_paths = self.tpt_exporter.export(
                    job.source.owner,
                    load_table,
                    columns,
                    row_limit,
                    where_clause=where_clause,
                    num_instances=num_instances,
                    file_label=file_label,
                )

                target_path = self._build_target_path(job, date_str, partition_values)
                if target_path not in cleared_paths:
                    deleted = delete_prefix(
                        self.obs_config, self.obs_bucket, target_path.lstrip("/") + "/"
                    )
                    if deleted:
                        logger.info(f"Cleared {deleted} existing object(s) at {target_path} before writing")
                    cleared_paths.add(target_path)
                ensure_prefix_exists(self.obs_config, self.obs_bucket, target_path)
                job_json = build_job_json(
                    local_csv_paths=csv_paths,
                    columns=columns,
                    target_obs_path=f"obs://{self.obs_bucket}{target_path}",
                    file_name=job.target.hive_table.lower(),
                    file_type=job.target.format,
                    field_delimiter="|",
                    setting=job.setting,
                    obs_config=self.obs_config,
                    exclude_columns=dynamic_cols,
                )
                run_dir = (
                    self.paths.datax_logs_dir
                    / job.table_name
                    / date_str
                    / load_table
                    / (file_label or "_")
                )
                result = self.runner.run(job_json, run_dir)
                if not result.succeeded or not result.within_error_limit(
                    job.setting.error_limit.record
                ):
                    raise RuntimeError(
                        f"DataX run failed for {job.table_name}/{load_table}: "
                        f"see {result.log_path} (never display raw - may contain credentials)"
                    )
                datax_records_read += result.records_read or 0
                datax_records_failed += result.records_failed or 0
                registered_partitions[tuple(sorted(partition_values.items()))] = target_path

        for partition_values, target_path in registered_partitions.items():
            spec = PartitionSpec(values={"processing_date": date_str, **dict(partition_values)})
            self.registrar.add_partition(
                job.target.hive_owner,
                job.target.hive_table,
                spec.to_sql(),
                f"obs://{self.obs_bucket}{target_path}",
            )

        result = verify(
            self.td_cursor,
            job.source.owner,
            job.source.load_tables,
            job.target.hive_owner,
            job.target.hive_table,
            hive_where=f'processing_date="{date_str}"',
            registrar=self.registrar,
        )

        record = AuditRecord(
            job_name=job.table_name,
            processing_date=date_str,
            source_schema=job.source.owner,
            source_table=",".join(job.source.load_tables),
            hive_schema=job.target.hive_owner,
            hive_table=job.target.hive_table,
            source_row_count=result.source_count,
            target_row_count=result.target_count,
            status=result.status,
            loader="datax",
            datax_reported_count=datax_records_read,
            start_time=start_time,
            end_time=datetime.now(),
        )
        self.audit_sink.record(record)

        # Local TPT-exported CSVs serve no further purpose once verify()
        # independently confirms the data landed in OBS - leaving them on
        # disk is pure waste, and at real production scale it's not a
        # small amount: a single day's run for one large table left 169GB
        # of exported+split CSVs sitting untouched (confirmed 2026-08-20).
        # Kept on failure/mismatch deliberately, for debugging - only a
        # confirmed-successful run's local files are safe to delete.
        if result.status == "success":
            shutil.rmtree(self.tpt_exporter.output_dir, ignore_errors=True)

        return record

    def _partition_value_scopes(self, job: JobSpec, load_table: str, dynamic_cols: list, columns: list):
        """Yields (partition_values, where_clause, file_label) once per
        distinct combination of dynamic partition column values actually
        present in load_table - queried directly (SELECT DISTINCT), not
        discovered after an unscoped export. A table with no dynamic
        partition columns yields exactly one no-op scope (no WHERE
        filter, matching the whole table).

        WHERE values are never single-quoted, even for VARCHAR partition
        columns: the whole SelectStmt is itself wrapped in single quotes
        in jobvars.txt (see reader.TPTExporter.export), so an embedded
        quote terminates that value early and corrupts the file -
        confirmed 2026-08-20 (TPT04187: "lacks a value specification").
        In practice every dynamic partition column seen so far is numeric
        (DATE_KEY), where no quoting is correct SQL anyway. A VARCHAR
        dynamic partition column would need real escaping support this
        doesn't have yet - fail loudly rather than silently emit a
        corrupt jobvars file.
        """
        if not dynamic_cols:
            yield {}, "", ""
            return

        col_types = {name: tpt_type for name, tpt_type, _ in columns}
        for col in dynamic_cols:
            if col_types.get(col, "").startswith("VARCHAR"):
                raise NotImplementedError(
                    f"Dynamic partition column {col} is VARCHAR - WHERE-clause "
                    f"quoting for string partition values isn't supported yet "
                    f"(would corrupt TPT's jobvars.txt, see docstring). Only "
                    f"numeric dynamic partition columns are supported currently."
                )

        col_list = ", ".join(dynamic_cols)
        self.td_cursor.execute(f"SELECT DISTINCT {col_list} FROM {job.source.owner}.{load_table}")
        for row in self.td_cursor.fetchall():
            values = [str(v) for v in row]
            partition_values = dict(zip(dynamic_cols, values))
            where_clause = " AND ".join(
                f"{col} = {val}" for col, val in partition_values.items()
            )
            file_label = "_".join(values)
            yield partition_values, where_clause, file_label

    def _build_target_path(self, job: JobSpec, date_str: str, partition_values: dict) -> str:
        path = f"{job.target.obs_dir}/processing_date={date_str}"
        for column, value in partition_values.items():
            path += f"/{column}={value}"
        return path
