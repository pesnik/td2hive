#!/usr/bin/env python3
"""Thin coordinator: export -> split by partition -> build job.json -> run
DataX (fast-fail check only) -> register partition -> verify (the real
verdict) -> audit. No business logic of its own - every step below is a
call into a module that owns that logic and can be tested/reused on its
own. Kept intentionally small so this file doesn't become the next
1700-line orchestrator.
"""

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Optional

from loguru import logger

from .audit import AuditRecord
from .column_types import resolve_column_types
from .datax.distribution import resolve_datax_home, validate_distribution
from .datax.job_spec import build_job_json, split_csv_by_partition_value
from .datax.runner import DataxRunner
from .jobspec import JobSpec
from .partition_registrar import PartitionRegistrar, PartitionSpec
from .obs_client import ensure_prefix_exists
from .reader import ObsConfig, TPTExporter
from .verify import verify


@dataclass
class RunPaths:
    tpt_output_dir: Path
    partition_split_dir: Path
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
        datax_records_read = 0
        datax_records_failed = 0
        # Every distinct partition_values combination actually written -
        # registered explicitly via ADD PARTITION below, never via MSCK
        # REPAIR's directory-tree auto-discovery. We already know every
        # value combination from the CSV split (that's the whole point of
        # doing the split ourselves), so there is nothing for MSCK to
        # discover that we don't already have - and MSCK REPAIR was found
        # 2026-08-20 to fail unreliably against this OBS backend for
        # DataX-written directories (generic DDLTask error, no usable
        # server-side detail from the client), while explicit ADD
        # PARTITION against the identical data succeeded and read back an
        # exact row-count match. Precise per-partition registration is
        # also strictly cheaper than MSCK's full-table directory scan.
        registered_partitions: dict = {}

        for load_table in job.source.load_tables:
            csv_path = self.tpt_exporter.export(job.source.owner, load_table, columns, row_limit)
            groups = split_csv_by_partition_value(
                csv_path, columns, dynamic_cols, self.paths.partition_split_dir
            )

            for group in groups:
                target_path = self._build_target_path(job, date_str, group.partition_values)
                ensure_prefix_exists(self.obs_config, self.obs_bucket, target_path)
                job_json = build_job_json(
                    local_csv_path=group.local_csv_path,
                    columns=columns,
                    target_obs_path=f"obs://{self.obs_bucket}{target_path}",
                    file_name=job.target.hive_table.lower(),
                    file_type=job.target.format,
                    field_delimiter="|",
                    setting=job.setting,
                    obs_config=self.obs_config,
                    exclude_columns=dynamic_cols,
                )
                run_dir = self.paths.datax_logs_dir / job.table_name / date_str / load_table
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
                registered_partitions[tuple(sorted(group.partition_values.items()))] = target_path

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
        return record

    def _build_target_path(self, job: JobSpec, date_str: str, partition_values: dict) -> str:
        path = f"{job.target.obs_dir}/processing_date={date_str}"
        for column, value in partition_values.items():
            path += f"/{column}={value}"
        return path
