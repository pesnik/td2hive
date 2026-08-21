#!/usr/bin/env python3
"""td2hive CLI. Two main command groups: `run`/`run-all` for loading, and
`retention` for expiring old partitions - deliberately separate, since
retention runs on its own schedule (e.g. weekly) independent of loading
(e.g. daily), and should never be folded into a load run automatically.

`plan`/`prepare`/`run-unit`/`reconcile` are the lower-level primitives
`run` is built from - exposed on their own so an external scheduler
(a k8s Job with parallelism>1, an Airflow mapped task, an Argo Workflow
step) can fan a job's units out across processes/containers instead of
running them sequentially inside one `td2hive run` invocation. Most
users just want `run`/`run-all` - reach for the primitives only when
actually distributing a job's units across more than one worker.
"""

import json
from datetime import date, datetime
from pathlib import Path
from typing import List, Optional

import click
import teradatasql
from loguru import logger

from .audit import CompositeAuditSink
from .audit.jsonl_sink import JSONLFileAuditSink
from .column_types import resolve_column_types, to_hive_type
from .jobspec import JobSpec, TargetSpec, load_jobs_dir, load_jobspec
from .job_runner import JobRunner, RunPaths, Unit, UnitResult
from .obs_client import delete_prefix
from .partition_registrar import PartitionRegistrar
from .reader import ObsConfig
from .retention import process_retention

_TD_OPTS = [
    click.option("--td-host", required=True, envvar="TD_HOST"),
    click.option("--td-user", required=True, envvar="TD_USER"),
    click.option("--td-password", required=True, envvar="TD_PASSWORD"),
]
_OBS_OPTS = [
    click.option("--obs-access-key", required=True, envvar="OBS_ACCESS_KEY"),
    click.option("--obs-secret-key", required=True, envvar="OBS_SECRET_KEY"),
    click.option("--obs-endpoint", required=True, envvar="OBS_ENDPOINT"),
    click.option("--obs-bucket", required=True, envvar="OBS_BUCKET"),
]


def _apply_options(options):
    def decorator(f):
        for opt in reversed(options):
            f = opt(f)
        return f

    return decorator


@click.group()
def cli():
    """td2hive: Teradata -> Hive (MRS/HetuEngine) backup pipeline."""


@cli.command("run")
@click.option("--job", "job_path", required=True, type=click.Path(exists=True, path_type=Path))
@click.option("--processing-date", required=True, help="YYYY-MM-DD")
@click.option("--row-limit", default=0, help="Cap exported rows (scratch/proof runs only)")
@click.option("--force", is_flag=True, help="Re-run even if already succeeded for this date")
@_apply_options(_TD_OPTS)
@_apply_options(_OBS_OPTS)
@click.option("--datax-home", default="", envvar="DATAX_HOME")
@click.option("--tpt-output-dir", default="/data01/td2hive/tpt_output")
@click.option("--audit-jsonl", default="/data01/td2hive/logs/audit.jsonl")
@click.option("--audit-sql-url", default="", help="SQLAlchemy URL, e.g. mysql+pymysql://...")
def run_one(
    job_path: Path,
    processing_date: str,
    row_limit: int,
    force: bool,
    td_host: str,
    td_user: str,
    td_password: str,
    obs_access_key: str,
    obs_secret_key: str,
    obs_endpoint: str,
    obs_bucket: str,
    datax_home: str,
    tpt_output_dir: str,
    audit_jsonl: str,
    audit_sql_url: str,
):
    """Run one table's job spec for one processing date."""
    job = load_jobspec(job_path)
    record = _run_job(
        job, processing_date, row_limit, force, td_host, td_user, td_password,
        obs_access_key, obs_secret_key, obs_endpoint, obs_bucket,
        datax_home, tpt_output_dir, audit_jsonl, audit_sql_url,
    )
    if record is None:
        click.echo(f"{job.table_name}/{processing_date}: skipped (already succeeded)")
    else:
        click.echo(f"{job.table_name}/{processing_date}: {record.status} "
                    f"(source={record.source_row_count} target={record.target_row_count})")
        if record.status != "success":
            raise SystemExit(1)


@cli.command("run-all")
@click.option("--jobs-dir", required=True, type=click.Path(exists=True, path_type=Path))
@click.option("--processing-date", required=True, help="YYYY-MM-DD")
@click.option("--force", is_flag=True)
@_apply_options(_TD_OPTS)
@_apply_options(_OBS_OPTS)
@click.option("--datax-home", default="", envvar="DATAX_HOME")
@click.option("--tpt-output-dir", default="/data01/td2hive/tpt_output")
@click.option("--audit-jsonl", default="/data01/td2hive/logs/audit.jsonl")
@click.option("--audit-sql-url", default="")
def run_all(
    jobs_dir: Path,
    processing_date: str,
    force: bool,
    td_host: str,
    td_user: str,
    td_password: str,
    obs_access_key: str,
    obs_secret_key: str,
    obs_endpoint: str,
    obs_bucket: str,
    datax_home: str,
    tpt_output_dir: str,
    audit_jsonl: str,
    audit_sql_url: str,
):
    """Run every DataX-loader job spec under jobs-dir for one processing date."""
    jobs = [j for j in load_jobs_dir(jobs_dir) if j.uses_datax]
    failures = 0
    for job in jobs:
        try:
            record = _run_job(
                job, processing_date, 0, force, td_host, td_user, td_password,
                obs_access_key, obs_secret_key, obs_endpoint, obs_bucket,
                datax_home, tpt_output_dir, audit_jsonl, audit_sql_url,
            )
            if record is None:
                click.echo(f"{job.table_name}: skipped (already succeeded)")
            else:
                click.echo(f"{job.table_name}: {record.status}")
                if record.status != "success":
                    failures += 1
        except Exception as e:
            logger.error(f"{job.table_name} failed: {e}")
            click.echo(f"{job.table_name}: FAILED - {e}")
            failures += 1
    if failures:
        raise SystemExit(1)


def _build_runner(
    job: JobSpec, processing_date: str,
    td_host: str, td_user: str, td_password: str,
    obs_access_key: str, obs_secret_key: str, obs_endpoint: str, obs_bucket: str,
    datax_home: str, tpt_output_dir: str, audit_jsonl: str, audit_sql_url: str,
) -> JobRunner:
    """Shared connection/runner setup for every run/plan/run-unit/reconcile
    command below. Constructing a JobRunner never touches DataX itself
    (see JobRunner.runner's lazy property) - only actually running a unit
    does, so `plan`/`reconcile` work fine even without a DataX
    distribution available where they run."""
    conn = teradatasql.connect(host=td_host, user=td_user, password=td_password)
    cursor = conn.cursor()
    obs_config = ObsConfig(access_key=obs_access_key, secret_key=obs_secret_key, endpoint=obs_endpoint)

    sinks = [JSONLFileAuditSink(Path(audit_jsonl))]
    if audit_sql_url:
        from .audit.sql_sink import SQLAuditSink
        sinks.append(SQLAuditSink(audit_sql_url))
    audit_sink = CompositeAuditSink(sinks)

    run_id_dir = Path(tpt_output_dir) / job.table_name / processing_date
    return JobRunner(
        td_cursor=cursor, td_host=td_host, td_user=td_user, td_password=td_password,
        obs_config=obs_config, obs_bucket=obs_bucket, audit_sink=audit_sink,
        paths=RunPaths(
            tpt_output_dir=run_id_dir / "tpt",
            datax_logs_dir=Path("/data01/td2hive/logs/datax"),
            # Overrides fs.obs.buffer.dir - the OBS Hadoop connector's own
            # default (/tmp) is a small partition that two real
            # concurrent jobs' writes exhausted in production 2026-08-21
            # ("No space left on device", both jobs failed). /data01 has
            # far more headroom (1.5TB free at the time).
            obs_buffer_dir="/data01/td2hive/tmp/obs",
        ),
        datax_home=datax_home,
    )


def _run_job(
    job: JobSpec, processing_date: str, row_limit: int, force: bool,
    td_host: str, td_user: str, td_password: str,
    obs_access_key: str, obs_secret_key: str, obs_endpoint: str, obs_bucket: str,
    datax_home: str, tpt_output_dir: str, audit_jsonl: str, audit_sql_url: str,
):
    runner = _build_runner(
        job, processing_date, td_host, td_user, td_password,
        obs_access_key, obs_secret_key, obs_endpoint, obs_bucket,
        datax_home, tpt_output_dir, audit_jsonl, audit_sql_url,
    )
    return runner.run(
        job, date.fromisoformat(processing_date), row_limit=row_limit, force=force
    )


# ---------------------------------------------------------------------
# plan / prepare / run-unit / reconcile - the primitives `run` is built
# from, exposed for external fan-out (k8s Job parallelism, Airflow
# dynamic task mapping, Argo Workflow withParam steps). See module
# docstring; most users want `run`/`run-all` instead.
# ---------------------------------------------------------------------

@cli.command("plan")
@click.option("--job", "job_path", required=True, type=click.Path(exists=True, path_type=Path))
@click.option("--processing-date", required=True, help="YYYY-MM-DD")
@_apply_options(_TD_OPTS)
@_apply_options(_OBS_OPTS)
@click.option("--datax-home", default="", envvar="DATAX_HOME")
@click.option("--tpt-output-dir", default="/data01/td2hive/tpt_output")
@click.option("--audit-jsonl", default="/data01/td2hive/logs/audit.jsonl")
@click.option("--audit-sql-url", default="")
def plan_cmd(
    job_path: Path, processing_date: str,
    td_host: str, td_user: str, td_password: str,
    obs_access_key: str, obs_secret_key: str, obs_endpoint: str, obs_bucket: str,
    datax_home: str, tpt_output_dir: str, audit_jsonl: str, audit_sql_url: str,
):
    """List this job's units as JSON - one line per unit, to stdout. Feed
    into `run-unit --unit <line>` (once per line, anywhere) or an
    orchestrator's native fan-out (k8s Job parallelism, Airflow
    .expand(), Argo withParam)."""
    job = load_jobspec(job_path)
    runner = _build_runner(
        job, processing_date, td_host, td_user, td_password,
        obs_access_key, obs_secret_key, obs_endpoint, obs_bucket,
        datax_home, tpt_output_dir, audit_jsonl, audit_sql_url,
    )
    units = runner.plan(job, date.fromisoformat(processing_date))
    for unit in units:
        click.echo(json.dumps(unit.to_dict()))


@cli.command("prepare")
@click.option("--job", "job_path", required=True, type=click.Path(exists=True, path_type=Path))
@click.option("--processing-date", required=True, help="YYYY-MM-DD")
@_apply_options(_TD_OPTS)
@_apply_options(_OBS_OPTS)
@click.option("--datax-home", default="", envvar="DATAX_HOME")
@click.option("--tpt-output-dir", default="/data01/td2hive/tpt_output")
@click.option("--audit-jsonl", default="/data01/td2hive/logs/audit.jsonl")
@click.option("--audit-sql-url", default="")
def prepare_cmd(
    job_path: Path, processing_date: str,
    td_host: str, td_user: str, td_password: str,
    obs_access_key: str, obs_secret_key: str, obs_endpoint: str, obs_bucket: str,
    datax_home: str, tpt_output_dir: str, audit_jsonl: str, audit_sql_url: str,
):
    """Clear every unit's target OBS path once, before any run-unit call.
    Must run exactly once, before fan-out begins - see JobRunner.prepare's
    docstring for why this can't be pushed into each unit's own run."""
    job = load_jobspec(job_path)
    runner = _build_runner(
        job, processing_date, td_host, td_user, td_password,
        obs_access_key, obs_secret_key, obs_endpoint, obs_bucket,
        datax_home, tpt_output_dir, audit_jsonl, audit_sql_url,
    )
    units = runner.plan(job, date.fromisoformat(processing_date))
    runner.prepare(units)
    click.echo(f"{job.table_name}/{processing_date}: prepared {len(units)} unit(s)")


@cli.command("run-unit")
@click.option("--job", "job_path", required=True, type=click.Path(exists=True, path_type=Path))
@click.option("--processing-date", required=True, help="YYYY-MM-DD")
@click.option("--unit", "unit_json", required=True, help="One line of `td2hive plan`'s output")
@click.option("--row-limit", default=0, help="Cap exported rows (scratch/proof runs only)")
@click.option("--result-file", default="", type=click.Path(path_type=Path),
              help="Write this unit's result JSON here, for `reconcile --results-dir` to read back")
@_apply_options(_TD_OPTS)
@_apply_options(_OBS_OPTS)
@click.option("--datax-home", default="", envvar="DATAX_HOME")
@click.option("--tpt-output-dir", default="/data01/td2hive/tpt_output")
@click.option("--audit-jsonl", default="/data01/td2hive/logs/audit.jsonl")
@click.option("--audit-sql-url", default="")
def run_unit_cmd(
    job_path: Path, processing_date: str, unit_json: str, row_limit: int, result_file: Path,
    td_host: str, td_user: str, td_password: str,
    obs_access_key: str, obs_secret_key: str, obs_endpoint: str, obs_bucket: str,
    datax_home: str, tpt_output_dir: str, audit_jsonl: str, audit_sql_url: str,
):
    """Run exactly one unit (TPT export + DataX write + partition
    registration) - the thing a k8s Job pod / Airflow mapped task / Argo
    step actually runs. Requires `prepare` to have already run for this
    job/date. Does NOT verify or write an AuditRecord - call `reconcile`
    once every unit's run-unit has completed."""
    job = load_jobspec(job_path)
    runner = _build_runner(
        job, processing_date, td_host, td_user, td_password,
        obs_access_key, obs_secret_key, obs_endpoint, obs_bucket,
        datax_home, tpt_output_dir, audit_jsonl, audit_sql_url,
    )
    unit = Unit.from_dict(json.loads(unit_json))
    result = runner.run_unit(job, unit, row_limit=row_limit)
    click.echo(json.dumps(result.to_dict()))
    if result_file:
        result_file.parent.mkdir(parents=True, exist_ok=True)
        result_file.write_text(json.dumps(result.to_dict()))


@cli.command("reconcile")
@click.option("--job", "job_path", required=True, type=click.Path(exists=True, path_type=Path))
@click.option("--processing-date", required=True, help="YYYY-MM-DD")
@click.option("--results-dir", required=True, type=click.Path(exists=True, path_type=Path),
              help="Directory of result JSON files written by `run-unit --result-file`")
@_apply_options(_TD_OPTS)
@_apply_options(_OBS_OPTS)
@click.option("--datax-home", default="", envvar="DATAX_HOME")
@click.option("--tpt-output-dir", default="/data01/td2hive/tpt_output")
@click.option("--audit-jsonl", default="/data01/td2hive/logs/audit.jsonl")
@click.option("--audit-sql-url", default="")
def reconcile_cmd(
    job_path: Path, processing_date: str, results_dir: Path,
    td_host: str, td_user: str, td_password: str,
    obs_access_key: str, obs_secret_key: str, obs_endpoint: str, obs_bucket: str,
    datax_home: str, tpt_output_dir: str, audit_jsonl: str, audit_sql_url: str,
):
    """Once every unit's run-unit has completed: register every
    partition, verify() the whole job once, write the one AuditRecord.
    This is the ONLY step that decides success/dq_mismatch - never any
    individual run-unit call."""
    job = load_jobspec(job_path)
    runner = _build_runner(
        job, processing_date, td_host, td_user, td_password,
        obs_access_key, obs_secret_key, obs_endpoint, obs_bucket,
        datax_home, tpt_output_dir, audit_jsonl, audit_sql_url,
    )
    result_files = sorted(results_dir.glob("*.json"))
    if not result_files:
        raise click.ClickException(f"No unit result files found under {results_dir}")
    unit_results = [UnitResult.from_dict(json.loads(f.read_text())) for f in result_files]
    record = runner.reconcile(job, date.fromisoformat(processing_date), unit_results)
    click.echo(f"{job.table_name}/{processing_date}: {record.status} "
                f"(source={record.source_row_count} target={record.target_row_count})")
    if record.status != "success":
        raise SystemExit(1)


@cli.command("create-target")
@click.option("--job", "job_path", required=True, type=click.Path(exists=True, path_type=Path),
              help="Draft (or promoted) job YAML describing the real target table to create")
@click.option("--force", is_flag=True,
              help="Replace an existing table's Hive metadata (DROP + CREATE) instead of refusing")
@_apply_options(_TD_OPTS)
@_apply_options(_OBS_OPTS)
def create_target_cmd(
    job_path: Path, force: bool,
    td_host: str, td_user: str, td_password: str,
    obs_access_key: str, obs_secret_key: str, obs_endpoint: str, obs_bucket: str,
):
    """Create the REAL Hive table a job spec points at, if it doesn't
    already exist yet. Neither `td2hive run` nor a typical legacy
    pipeline does this automatically - job_runner.py only clears/writes
    an existing target path and registers partitions against an
    already-existing metastore entry; `td2hive validate` only ever
    creates/drops a throwaway scratch table. This closes that gap for
    genuinely new tables, whose real Hive table would otherwise need to
    be created out-of-band by hand before either pipeline could touch it.

    Refuses to touch an already-existing table unless --force is passed
    - this creates real, permanent Hive metadata, not a scratch proof.
    Passing --force drops and recreates the table DEFINITION only; the
    underlying OBS data is never deleted (external tables never lose
    data on DROP), but a column/partition mismatch against data already
    written under this path can break reads until reconciled - so only
    force this when intentionally correcting the DDL, not routinely.

    Column types are resolved from Teradata (the same
    resolve_column_types check a real run does - never guessed or
    copied from a legacy config). Partition columns are processing_date
    plus any dynamic partitions the job spec declares.

    This only creates the table - it loads no data. Run `td2hive
    validate` against the same draft next, or promote it and run a real
    `td2hive run`, to actually populate it."""
    job = load_jobspec(job_path)
    registrar = PartitionRegistrar()

    if registrar.table_exists(job.target.hive_owner, job.target.hive_table):
        if not force:
            raise click.ClickException(
                f"{job.target.hive_owner}.{job.target.hive_table} already exists - refusing to "
                f"touch it. Pass --force to replace its metadata (the underlying OBS data at "
                f"{job.target.obs_dir} is never deleted - external tables don't lose data on "
                f"DROP - but a column/partition mismatch against data already written there can "
                f"break reads until reconciled)."
            )
        click.echo(
            f"--force passed: {job.target.hive_owner}.{job.target.hive_table} already exists - "
            f"replacing its metadata (data at {job.target.obs_dir} is not touched)."
        )

    conn = teradatasql.connect(host=td_host, user=td_user, password=td_password)
    cursor = conn.cursor()
    dynamic_partition_cols = {p.column.lower() for p in job.target.partitions if p.dynamic}
    resolved = resolve_column_types(cursor, job.source.owner, job.source.load_tables, job.source.columns)
    data_columns = [
        (name, to_hive_type(tpt_type))
        for name, tpt_type, _ in resolved
        if name.lower() not in dynamic_partition_cols
    ]
    partition_columns = [("processing_date", "STRING")] + [
        (p.column, "STRING") for p in job.target.partitions if p.dynamic
    ]

    location = f"obs://{obs_bucket}{job.target.obs_dir}"
    registrar.create_external_table(
        schema=job.target.hive_owner,
        table=job.target.hive_table,
        columns=data_columns,
        location=location,
        file_format=job.target.format.upper(),
        partition_columns=partition_columns,
    )
    click.echo(f"Created {job.target.hive_owner}.{job.target.hive_table} at {location}")


@cli.command("validate")
@click.option("--job", "job_path", required=True, type=click.Path(exists=True, path_type=Path),
              help="Draft job YAML - a table only belongs in jobs/ after this passes")
@click.option("--processing-date", default=lambda: date.today().strftime("%Y-%m-%d"),
              help="YYYY-MM-DD, defaults to today")
@click.option("--row-limit", default=5000, help="Rows per unit for this scratch proof run")
@click.option("--keep-scratch", is_flag=True,
              help="Leave the scratch Hive table/OBS data in place for inspection instead of tearing it down")
@_apply_options(_TD_OPTS)
@_apply_options(_OBS_OPTS)
@click.option("--datax-home", default="", envvar="DATAX_HOME")
@click.option("--tpt-output-dir", default="/data01/td2hive/tpt_output")
def validate_cmd(
    job_path: Path, processing_date: str, row_limit: int, keep_scratch: bool,
    td_host: str, td_user: str, td_password: str,
    obs_access_key: str, obs_secret_key: str, obs_endpoint: str, obs_bucket: str,
    datax_home: str, tpt_output_dir: str,
):
    """Prove a draft job spec actually works against real Teradata data
    before it's trusted enough for jobs/: creates a scratch Hive table
    matching the draft's real schema (via the same Teradata type
    resolution a real run would do), loads --row-limit rows into it, and
    reports whether DataX's own reported write count and an independent
    Hive COUNT(*) agree.

    Deliberately does NOT judge success via record.status - `verify()`
    always compares target_row_count against the FULL unscoped source
    table's count, which --row-limit can never match by design (it's a
    scratch proof, not a full load); a dq_mismatch against
    source_row_count here is expected, not a failure signal. The real
    question this answers is narrower and more useful pre-promotion:
    did every row DataX claims to have written actually land in Hive,
    matching the schema this draft declares.

    Tears the scratch Hive table + OBS data down afterward unless
    --keep-scratch is passed."""
    job = load_jobspec(job_path)
    scratch_table = f"{job.target.hive_table}_TD2HIVE_VALIDATE"
    scratch_obs_dir = f"/td2hive_validate/{job.table_name}"

    conn = teradatasql.connect(host=td_host, user=td_user, password=td_password)
    cursor = conn.cursor()
    obs_config = ObsConfig(access_key=obs_access_key, secret_key=obs_secret_key, endpoint=obs_endpoint)
    registrar = PartitionRegistrar()

    dynamic_partition_cols = {p.column.lower() for p in job.target.partitions if p.dynamic}
    resolved = resolve_column_types(cursor, job.source.owner, job.source.load_tables, job.source.columns)
    data_columns = [
        (name, to_hive_type(tpt_type))
        for name, tpt_type, _ in resolved
        if name.lower() not in dynamic_partition_cols
    ]
    partition_columns = [("processing_date", "STRING")] + [
        (p.column, "STRING") for p in job.target.partitions if p.dynamic
    ]

    location = f"obs://{obs_bucket}{scratch_obs_dir}"
    click.echo(f"Creating scratch table {job.target.hive_owner}.{scratch_table} at {location}")
    registrar.create_external_table(
        schema=job.target.hive_owner,
        table=scratch_table,
        columns=data_columns,
        location=location,
        file_format=job.target.format.upper(),
        partition_columns=partition_columns,
    )

    scratch_job = JobSpec(
        table_name=f"{job.table_name}__validate",
        source=job.source,
        target=TargetSpec(
            hive_owner=job.target.hive_owner,
            hive_table=scratch_table,
            obs_dir=scratch_obs_dir,
            format=job.target.format,
            partitions=job.target.partitions,
        ),
        loader=job.loader,
        setting=job.setting,
    )

    try:
        audit_sink = CompositeAuditSink([JSONLFileAuditSink(Path(tpt_output_dir) / "_validate_audit.jsonl")])
        run_id_dir = Path(tpt_output_dir) / scratch_job.table_name / processing_date
        runner = JobRunner(
            td_cursor=cursor, td_host=td_host, td_user=td_user, td_password=td_password,
            obs_config=obs_config, obs_bucket=obs_bucket, audit_sink=audit_sink,
            paths=RunPaths(
                tpt_output_dir=run_id_dir / "tpt",
                datax_logs_dir=Path(tpt_output_dir) / "_validate_datax_logs",
                obs_buffer_dir="/data01/td2hive/tmp/obs",
            ),
            datax_home=datax_home,
        )
        record = runner.run(
            scratch_job, date.fromisoformat(processing_date), row_limit=row_limit, force=True,
        )
        if record is None:
            raise click.ClickException(
                "scratch run reported already-succeeded, which force=True should have prevented - "
                "this points at a bug in JobRunner.run(), not this draft"
            )

        click.echo(f"DataX reported: {record.datax_reported_count} row(s) written")
        click.echo(f"Hive independently counted: {record.target_row_count} row(s)")
        click.echo(
            f"(status={record.status} against source_row_count={record.source_row_count} is expected to "
            f"read dq_mismatch here - --row-limit={row_limit} deliberately doesn't export the whole "
            f"source table - that disagreement is not what this command is judging.)"
        )

        if record.target_row_count > 0 and record.target_row_count == record.datax_reported_count:
            click.echo(f"PASS: {job_path} is safe to promote into jobs/.")
        else:
            click.echo(
                f"FAIL: DataX's reported write count and Hive's independent recount disagree "
                f"({record.datax_reported_count} vs {record.target_row_count}) - do not promote this draft."
            )
            raise SystemExit(1)
    finally:
        if keep_scratch:
            click.echo(
                f"--keep-scratch passed: leaving {job.target.hive_owner}.{scratch_table} and "
                f"{scratch_obs_dir} in place for inspection."
            )
        else:
            registrar.drop_table(job.target.hive_owner, scratch_table)
            deleted = delete_prefix(obs_config, obs_bucket, scratch_obs_dir.lstrip("/") + "/")
            click.echo(f"Cleaned up scratch table and {deleted} OBS object(s) at {scratch_obs_dir}")


@cli.group("retention")
def retention_group():
    """Expire old processing_date partitions for tables that opt in via retention_days."""


@retention_group.command("run")
@click.option("--job", "job_path", required=True, type=click.Path(exists=True, path_type=Path))
@_apply_options(_OBS_OPTS)
@click.option("--dry-run/--no-dry-run", default=True, help="Default is dry-run - pass --no-dry-run to actually delete")
def retention_run_one(job_path: Path, obs_access_key: str, obs_secret_key: str,
                       obs_endpoint: str, obs_bucket: str, dry_run: bool):
    """Expire one table's old partitions (dry-run by default)."""
    job = load_jobspec(job_path)
    obs_config = ObsConfig(access_key=obs_access_key, secret_key=obs_secret_key, endpoint=obs_endpoint)
    registrar = PartitionRegistrar()
    result = process_retention(job, obs_config, obs_bucket, registrar, dry_run=dry_run)
    _print_retention_result(result)
    if result.had_errors:
        raise SystemExit(1)


@retention_group.command("run-all")
@click.option("--jobs-dir", required=True, type=click.Path(exists=True, path_type=Path))
@_apply_options(_OBS_OPTS)
@click.option("--dry-run/--no-dry-run", default=True)
def retention_run_all(jobs_dir: Path, obs_access_key: str, obs_secret_key: str,
                       obs_endpoint: str, obs_bucket: str, dry_run: bool):
    """Expire old partitions for every table with retention_days set (dry-run by default)."""
    obs_config = ObsConfig(access_key=obs_access_key, secret_key=obs_secret_key, endpoint=obs_endpoint)
    registrar = PartitionRegistrar()
    jobs = [j for j in load_jobs_dir(jobs_dir) if j.retention_days is not None]
    if not jobs:
        click.echo("No jobs have retention_days configured.")
        return
    had_errors = False
    for job in jobs:
        result = process_retention(job, obs_config, obs_bucket, registrar, dry_run=dry_run)
        _print_retention_result(result)
        had_errors = had_errors or result.had_errors
    if had_errors:
        raise SystemExit(1)


def _print_retention_result(result) -> None:
    mode = "DRY RUN" if result.dry_run else "APPLIED"
    click.echo(
        f"[{mode}] {result.job_name}: cutoff={result.cutoff_date} "
        f"kept={result.kept_count} expired={result.expired_count}"
    )
    for e in result.expired:
        status = "ERROR: " + e.error if e.error else ("would delete" if result.dry_run else "deleted")
        click.echo(f"  {e.partition_date} ({e.obs_prefix}): {status}")


if __name__ == "__main__":
    cli()
