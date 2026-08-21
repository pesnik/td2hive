#!/usr/bin/env python3
"""Coordinator, split into independently-invokable phases so a job's units
of work can be distributed across processes/containers (k8s Jobs, Airflow
mapped tasks, Argo Workflow steps) instead of only ever running inside one
sequential Python process:

    plan()      -> list every distinct (load_table, partition_value) unit,
                   read-only, no side effects. Queries Teradata only.
    prepare()   -> clear every unit's target OBS path exactly once (must
                   run once, before any unit writes - see its docstring
                   for why this can't safely be pushed into run_unit()).
    run_unit()  -> do exactly ONE unit's TPT export + DataX write +
                   partition registration. The atomic, externally-
                   parallelizable primitive - this is what a k8s Job pod /
                   Airflow mapped task / Argo step calls.
    run_units() -> the same work as N run_unit() calls, but batches
                   multiple units' DataX writes into fewer job.json calls
                   (shared JVM/channel pool) - a real, measured cost
                   (DataX's JVM cold-start) that matters once a table has
                   many partition values and everything's running
                   sequentially in one process. NOT used by run_unit()
                   itself - batching across pods/containers would defeat
                   the point of distributing them.
    reconcile() -> after every unit's result is in, register every
                   partition, verify() the whole job once, write the one
                   AuditRecord.
    run()       -> the single-container default: plan -> prepare ->
                   run_units (batched) -> reconcile, all in-process. This
                   is `td2hive run`, unchanged in outward behavior from
                   before this split - every step it does was already
                   here, just no longer only reachable as one function.
"""

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, TYPE_CHECKING

from loguru import logger

from .audit import AuditRecord
from .column_types import ResolvedColumn, resolve_column_types
from .datax.distribution import resolve_datax_home, validate_distribution
from .datax.job_spec import ContentSpec, build_job_json
from .datax.runner import DataxRunner
from .jobspec import JobSpec
from .partition_registrar import PartitionRegistrar, PartitionSpec
from .obs_client import delete_prefix, ensure_prefix_exists
from .reader import FIELD_DELIMITER, ObsConfig, TPTExporter
from .verify import verify

if TYPE_CHECKING:
    # Deferred: run_manifest.py imports Unit from this module, so a
    # module-level import here would be circular. Only ever used for
    # type hints - the real import in run() is a local, call-time import
    # instead (see run()'s own docstring).
    from .run_manifest import ManifestStore, RunManifest


@dataclass
class RunPaths:
    tpt_output_dir: Path
    datax_logs_dir: Path
    # Overrides fs.obs.buffer.dir (see datax/job_spec.py's build_job_json
    # docstring) - the OBS Hadoop connector's own default is /tmp, a
    # small partition that two real concurrent jobs' writes exhausted in
    # production 2026-08-21. Empty string keeps the connector's default.
    obs_buffer_dir: str = ""


@dataclass
class Unit:
    """One distinct (load_table, partition_value) piece of work - the
    thing `plan()` enumerates and `run_unit()` executes. A table with no
    dynamic partition column has exactly one Unit per load_table (empty
    partition_values, no WHERE filter - matches the whole table)."""

    load_table: str
    partition_values: Dict[str, str]
    where_clause: str
    file_label: str
    target_path: str

    def to_dict(self) -> dict:
        return {
            "load_table": self.load_table,
            "partition_values": self.partition_values,
            "where_clause": self.where_clause,
            "file_label": self.file_label,
            "target_path": self.target_path,
        }

    @staticmethod
    def from_dict(d: dict) -> "Unit":
        return Unit(
            load_table=d["load_table"],
            partition_values=d["partition_values"],
            where_clause=d["where_clause"],
            file_label=d["file_label"],
            target_path=d["target_path"],
        )


@dataclass
class UnitResult:
    """What `run_unit()`/`run_units()` hands to `reconcile()` - either
    in-process (the default `run()` path) or via a small JSON result file
    a separate `run-unit` invocation writes to disk, for a caller (a k8s
    Job's exit step, an Airflow downstream task, an Argo exit handler) to
    collect before calling `reconcile()`."""

    unit: Unit
    records_read: int = 0
    records_failed: int = 0

    def to_dict(self) -> dict:
        return {
            "unit": self.unit.to_dict(),
            "records_read": self.records_read,
            "records_failed": self.records_failed,
        }

    @staticmethod
    def from_dict(d: dict) -> "UnitResult":
        return UnitResult(
            unit=Unit.from_dict(d["unit"]),
            records_read=d["records_read"],
            records_failed=d["records_failed"],
        )


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
        manifest_store: "Optional[ManifestStore]" = None,
    ):
        self.td_cursor = td_cursor
        self.tpt_exporter = TPTExporter(td_host, td_user, td_password, str(paths.tpt_output_dir))
        if paths.obs_buffer_dir:
            Path(paths.obs_buffer_dir).mkdir(parents=True, exist_ok=True)
        self.obs_config = obs_config
        self.obs_bucket = obs_bucket
        self.audit_sink = audit_sink
        self.paths = paths
        self.registrar = PartitionRegistrar()
        self._datax_home_arg = datax_home or ""
        self._runner: Optional[DataxRunner] = None
        self._manifest_store = manifest_store

    @property
    def manifest_store(self) -> "ManifestStore":
        """Lazy, same reasoning as the `runner` property: only run()
        actually needs this (plan()/prepare()/reconcile() don't), and
        constructing the zero-config default here keeps every other
        JobRunner method free of a dependency on it. Default is one
        small JSONL file per (job, processing_date) run under
        datax_logs_dir - see run_manifest.py's module docstring for why
        that's a per-run file, not one shared growing log. Pass your own
        ManifestStore to __init__ to use something else."""
        if self._manifest_store is None:
            from .run_manifest import JSONLManifestStore
            self._manifest_store = JSONLManifestStore(self.paths.datax_logs_dir)
        return self._manifest_store

    @property
    def runner(self) -> DataxRunner:
        """Lazy: resolving/validating DATAX_HOME is only meaningful for
        the methods that actually invoke DataX (_run_batch, via
        run_unit/run_units/run) - plan() and reconcile() never touch it,
        so a `td2hive plan` step (e.g. a k8s Job's init step, computing
        the fan-out before any worker pod exists) shouldn't need a DataX
        distribution mounted at all just to construct a JobRunner."""
        if self._runner is None:
            datax_home = resolve_datax_home(self._datax_home_arg)
            validate_distribution(datax_home)
            self._runner = DataxRunner(datax_home)
        return self._runner

    # ------------------------------------------------------------------
    # plan
    # ------------------------------------------------------------------

    def plan(self, job: JobSpec, processing_date: date) -> List[Unit]:
        """Enumerate every unit this job needs, for every load table.
        Read-only: queries Teradata (SELECT DISTINCT on dynamic partition
        columns) but touches nothing on OBS/Hive. Safe to call repeatedly
        for inspection (`td2hive plan`) without side effects."""
        date_str = processing_date.strftime("%Y-%m-%d")
        dynamic_cols = [p.column for p in job.target.partitions if p.dynamic]
        columns = resolve_column_types(
            self.td_cursor, job.source.owner, job.source.load_tables, job.source.columns
        )
        units: List[Unit] = []
        for load_table in job.source.load_tables:
            for partition_values, where_clause, file_label in self._partition_value_scopes(
                job, load_table, dynamic_cols, columns
            ):
                target_path = self._build_target_path(job, date_str, partition_values)
                units.append(Unit(
                    load_table=load_table,
                    partition_values=partition_values,
                    where_clause=where_clause,
                    file_label=file_label,
                    target_path=target_path,
                ))
        return units

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

    # ------------------------------------------------------------------
    # prepare
    # ------------------------------------------------------------------

    def prepare(self, units: List[Unit], manifest: "Optional[RunManifest]" = None) -> None:
        """Clear every unit's target OBS path exactly once before any
        unit writes. Must run as a single, one-time step ahead of the
        whole fan-out - NOT something each run_unit() can safely do for
        itself: multiple units (e.g. two load tables both contributing
        rows to the same DATE_KEY partition) can share one target_path,
        and if units run concurrently across pods/containers, whichever
        one "clears first" racing against another that already started
        writing would wipe out real data. Deleting every unique path up
        front, before fan-out begins, removes that race entirely. This
        delete-then-write is the sole idempotency mechanism for this
        loader - hdfswriter's own writeMode stays 'append' (see
        jobspec.RunSetting) precisely so it never also tries to manage
        conflicts in the same directory.

        `manifest` (resumed `run()` calls only - see run_manifest.py):
        never clear a target_path any already-`written` unit uses - it
        already holds good, independently-verified-eventually data from
        this same run; clearing it would destroy real work just to
        "resume" a job that doesn't need that unit redone at all."""
        paths_with_written_data: set = set()
        if manifest is not None:
            for unit in units:
                if manifest.is_written(unit):
                    paths_with_written_data.add(unit.target_path)

        cleared_paths: set = set()
        for unit in units:
            if unit.target_path in paths_with_written_data:
                continue
            if unit.target_path in cleared_paths:
                continue
            deleted = delete_prefix(
                self.obs_config, self.obs_bucket, unit.target_path.lstrip("/") + "/"
            )
            if deleted:
                logger.info(f"Cleared {deleted} existing object(s) at {unit.target_path} before writing")
            ensure_prefix_exists(self.obs_config, self.obs_bucket, unit.target_path)
            cleared_paths.add(unit.target_path)

    # ------------------------------------------------------------------
    # run_unit / run_units
    # ------------------------------------------------------------------

    def run_unit(self, job: JobSpec, unit: Unit, row_limit: int = 0) -> UnitResult:
        """Do exactly one unit's TPT export + DataX write + partition
        registration. Always one partition value, one job.json (one
        content block), one DataX JVM launch - this is the primitive a
        k8s Job pod / Airflow mapped task / Argo step calls, so it must
        stay independently runnable with nothing but the job spec and
        this one unit (columns are re-resolved here, not threaded through
        from plan() - a cheap DBC.ColumnsV metadata query, not worth
        coupling a distributed worker to plan()'s process). `row_limit`
        is for scratch/proof runs only (TOP N)."""
        columns = resolve_column_types(
            self.td_cursor, job.source.owner, job.source.load_tables, job.source.columns
        )
        results = self._run_batch(job, [unit], columns, row_limit=row_limit)
        return results[0]

    def run_units(
        self, job: JobSpec, units: List[Unit], row_limit: int = 0,
        manifest: "Optional[RunManifest]" = None,
    ) -> List[UnitResult]:
        """Same work as calling run_unit() once per unit, but groups
        multiple units' DataX writes into as few job.json calls as fit
        under job.setting.max_channels_per_job - each batch shares one
        JVM instead of paying a fresh cold-start per partition value.
        Used by the sequential single-container run() path; never used
        by run_unit() itself (batching across externally-scheduled
        pods/containers would defeat the point of distributing them).

        `manifest` (resumed `run()` calls only): a unit already marked
        `written` is skipped entirely - its persisted UnitResult is
        reused as-is, no TPT export, no DataX write, no JVM launch."""
        columns = resolve_column_types(
            self.td_cursor, job.source.owner, job.source.load_tables, job.source.columns
        )
        num_instances = max(1, job.setting.speed_channel)
        budget = max(1, job.setting.max_channels_per_job)

        results: List[UnitResult] = []
        pending: List[Unit] = []
        for unit in units:
            state = manifest.get(unit) if manifest is not None else None
            if state is not None and state.status == "written":
                logger.info(
                    f"{unit.load_table}/{unit.file_label or 'static'}: already written "
                    f"(resumed run) - skipping export and write entirely"
                )
                results.append(UnitResult(
                    unit=unit, records_read=state.records_read, records_failed=state.records_failed
                ))
            else:
                pending.append(unit)

        batch: List[Unit] = []
        batch_channels = 0
        for unit in pending:
            if batch and batch_channels + num_instances > budget:
                results.extend(self._run_batch(job, batch, columns, row_limit=row_limit, manifest=manifest))
                batch, batch_channels = [], 0
            batch.append(unit)
            batch_channels += num_instances
        if batch:
            results.extend(self._run_batch(job, batch, columns, row_limit=row_limit, manifest=manifest))
        return results

    def _run_batch(
        self, job: JobSpec, units: List[Unit], columns: List[ResolvedColumn], row_limit: int = 0,
        manifest: "Optional[RunManifest]" = None,
    ) -> List[UnitResult]:
        """Runs one or more units as ONE DataX job.json (one JVM, one
        content block per unit). TPT export stays one call per unit
        regardless - FILE_WRITER[n]/tbuild -C round-robins rows across n
        files, it doesn't route by column value into a named file, so a
        combined multi-value TPT export can't produce per-value output
        without a local re-split (the exact double-I/O this pipeline's
        redesign eliminated - see reader.py).

        `manifest` (resumed `run()` calls only): a unit already marked
        `exported`, with its CSVs still present and non-empty, skips a
        fresh TPT export and reuses those files - a real failure mode
        this exists for, not a hypothetical one: two concurrent
        production jobs failed mid-DataX-write 2026-08-21 after their
        TPT exports had already fully succeeded; without this, a retry
        would have re-exported everything from scratch."""
        num_instances = max(1, job.setting.speed_channel)

        content_specs = []
        unit_csv_paths: List[List[Path]] = []
        for unit in units:
            reused = manifest.valid_exported_csv_paths(unit) if manifest is not None else None
            if reused is not None:
                logger.info(
                    f"{unit.load_table}/{unit.file_label or 'static'}: reusing previously "
                    f"exported CSVs (resumed run) - skipping TPT export"
                )
                csv_paths = reused
            else:
                csv_paths = self.tpt_exporter.export(
                    job.source.owner,
                    unit.load_table,
                    columns,
                    row_limit=row_limit,
                    where_clause=unit.where_clause,
                    num_instances=num_instances,
                    file_label=unit.file_label,
                )
                if manifest is not None:
                    manifest.mark_exported(unit, csv_paths)
            unit_csv_paths.append(csv_paths)
            content_specs.append(ContentSpec(
                local_csv_paths=csv_paths,
                target_obs_path=f"obs://{self.obs_bucket}{unit.target_path}",
                file_name=job.target.hive_table.lower(),
                exclude_columns=list(unit.partition_values.keys()),
            ))

        job_json = build_job_json(
            content_specs=content_specs,
            columns=columns,
            file_type=job.target.format,
            field_delimiter=FIELD_DELIMITER,
            setting=job.setting,
            obs_config=self.obs_config,
            obs_buffer_dir=self.paths.obs_buffer_dir,
        )
        # Must be unique per load_table, not just file_label: two load
        # tables can (and do, for real jobs) share the same partition
        # value - a path keyed on file_label alone silently overwrote one
        # load table's job.json/log with another's, confirmed against
        # real production data 2026-08-21 (a second load table's write
        # for a given partition value landed in the same directory a
        # first load table's write for that same value had already used).
        run_dir = (
            self.paths.datax_logs_dir / job.table_name
            / "_".join(f"{u.load_table}_{u.file_label or 'static'}" for u in units)
        )
        result = self.runner.run(job_json, run_dir)
        if not result.succeeded or not result.within_error_limit(job.setting.error_limit.record):
            raise RuntimeError(
                f"DataX run failed for {job.table_name} ({len(units)} unit(s)): "
                f"see {result.log_path} (never display raw - may contain credentials)"
            )

        # DataX's own report is the fast-fail signal only (see runner.py) -
        # split evenly across this batch's units for telemetry, since
        # DataX itself doesn't report per-content-block counts. The real
        # verdict is reconcile()'s independent verify(), against the whole
        # job, not any individual unit or batch.
        per_unit_read = (result.records_read or 0) // len(units)
        per_unit_failed = (result.records_failed or 0) // len(units)

        # Local TPT-exported CSVs serve no further purpose once this
        # batch's DataX run reports success - deleted here (gated only on
        # DataX's fast-fail signal, not the whole job's later independent
        # verify()) because a unit run via run_unit() may execute in a
        # container with no access to disk any later reconcile() step
        # runs on. Real production scale: a single day's run for one
        # large table left 169GB of exported CSVs sitting untouched
        # before this cleanup existed at all (confirmed 2026-08-20).
        for csv_paths in unit_csv_paths:
            for p in csv_paths:
                p.unlink(missing_ok=True)

        results = [
            UnitResult(unit=u, records_read=per_unit_read, records_failed=per_unit_failed)
            for u in units
        ]
        if manifest is not None:
            for r in results:
                manifest.mark_written(r.unit, r.records_read, r.records_failed)
        return results

    # ------------------------------------------------------------------
    # reconcile
    # ------------------------------------------------------------------

    def reconcile(
        self,
        job: JobSpec,
        processing_date: date,
        unit_results: List[UnitResult],
        start_time: Optional[datetime] = None,
    ) -> AuditRecord:
        """After every unit's result is in: register every distinct
        partition explicitly (never MSCK REPAIR's directory-tree auto-
        discovery - every partition value is already known, queried up
        front in plan(), so there's nothing for MSCK to discover that
        isn't already known, and MSCK REPAIR was found 2026-08-20 to fail
        unreliably against this OBS backend for DataX-written
        directories), verify() the whole job once against independent
        Teradata/Hive counts, and write the one AuditRecord."""
        date_str = processing_date.strftime("%Y-%m-%d")
        datax_records_read = sum(r.records_read for r in unit_results)

        registered_partitions: dict = {}
        for result in unit_results:
            key = tuple(sorted(result.unit.partition_values.items()))
            registered_partitions[key] = result.unit.target_path

        for partition_values, target_path in registered_partitions.items():
            spec = PartitionSpec(values={"processing_date": date_str, **dict(partition_values)})
            self.registrar.add_partition(
                job.target.hive_owner,
                job.target.hive_table,
                spec.to_sql(),
                f"obs://{self.obs_bucket}{target_path}",
            )

        # Scope the source-side count to exactly the dynamic partition
        # values this run actually processed - never the source table's
        # entire history. For a table with a dynamic partition column
        # and no separate load table (the source IS the full historical
        # fact table, not a narrow daily staging table), an unscoped
        # COUNT(*) can be enormous or fail outright: confirmed live on a
        # real table with tens of thousands of PARTITION BY RANGE_N
        # partitions, which threw a Teradata numeric-overflow error on a
        # bare COUNT(*). Comparing against the whole table's history was
        # never the right check for an incremental dynamic-partition
        # load anyway - only what THIS run wrote should be verified
        # against what THIS run's source values actually contain.
        # Empty for a table with no dynamic partition column (every
        # `partition_values` tuple is then empty) - matches the existing,
        # correct behavior for snapshot/dimension tables that replace
        # their entire content under one processing_date partition.
        source_clauses = [
            " AND ".join(f"{col} = {val}" for col, val in partition_values)
            for partition_values in registered_partitions
            if partition_values
        ]
        source_where = " OR ".join(f"({clause})" for clause in source_clauses)

        result = verify(
            self.td_cursor,
            job.source.owner,
            job.source.load_tables,
            job.target.hive_owner,
            job.target.hive_table,
            source_where=source_where,
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
            start_time=start_time or datetime.now(),
            end_time=datetime.now(),
        )
        self.audit_sink.record(record)
        return record

    # ------------------------------------------------------------------
    # run - the single-container default, unchanged in outward behavior
    # ------------------------------------------------------------------

    def run(
        self, job: JobSpec, processing_date: date, row_limit: int = 0, force: bool = False
    ) -> Optional[AuditRecord]:
        """plan -> prepare -> run_units (batched) -> reconcile, all
        in-process. This is `td2hive run` - every external behavior
        (idempotency check, final AuditRecord, success/dq_mismatch
        semantics) is exactly what it was before this file was split
        into phases; only the internal shape changed. `row_limit` is for
        scratch/proof runs only (TOP N per unit).

        Resumable across failures via a persisted per-unit manifest (see
        run_manifest.py) - a real, not hypothetical, need: two concurrent
        production jobs both failed mid-DataX-write 2026-08-21 (local
        disk exhaustion), after their TPT exports had already fully
        succeeded. A bare re-run now skips any unit already `written`
        entirely and reuses any unit's already-`exported` CSVs instead of
        re-exporting them, rather than redoing the whole job from
        scratch. `force=True` skips loading any prior manifest state and
        starts from a clean slate, same semantics it already has for the
        audit "already succeeded" check. The manifest is deleted once
        this run reaches `success` - past that point it's permanently
        useless (nothing left to resume), matching this package's
        existing posture on local run artifacts: keep on failure for
        debugging, clean up once independently verified successful."""
        from .run_manifest import RunManifest  # local: avoids a circular import, see module docstring

        start_time = datetime.now()
        date_str = processing_date.strftime("%Y-%m-%d")

        if not force and self.audit_sink.find_success(job.table_name, date_str):
            logger.info(f"{job.table_name}/{date_str} already succeeded, skipping (use force=True to re-run)")
            return None

        manifest = RunManifest(self.manifest_store, job.table_name, date_str)
        if not force:
            manifest.load()

        units = self.plan(job, processing_date)
        self.prepare(units, manifest=manifest)
        unit_results = self.run_units(job, units, row_limit=row_limit, manifest=manifest)
        record = self.reconcile(job, processing_date, unit_results, start_time=start_time)
        if record.status == "success":
            manifest.clear()
        return record
