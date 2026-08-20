#!/usr/bin/env python3
"""td2hive CLI. Two command groups: `run`/`run-all` for loading, and
`retention` for expiring old partitions - deliberately separate, since
retention runs on its own schedule (e.g. weekly) independent of loading
(e.g. daily), and should never be folded into a load run automatically.
"""

from datetime import date, datetime
from pathlib import Path
from typing import List, Optional

import click
import teradatasql
from loguru import logger

from .audit import CompositeAuditSink
from .audit.jsonl_sink import JSONLFileAuditSink
from .jobspec import JobSpec, load_jobs_dir, load_jobspec
from .job_runner import JobRunner, RunPaths
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


def _run_job(
    job: JobSpec, processing_date: str, row_limit: int, force: bool,
    td_host: str, td_user: str, td_password: str,
    obs_access_key: str, obs_secret_key: str, obs_endpoint: str, obs_bucket: str,
    datax_home: str, tpt_output_dir: str, audit_jsonl: str, audit_sql_url: str,
):
    conn = teradatasql.connect(host=td_host, user=td_user, password=td_password)
    cursor = conn.cursor()
    obs_config = ObsConfig(access_key=obs_access_key, secret_key=obs_secret_key, endpoint=obs_endpoint)

    sinks = [JSONLFileAuditSink(Path(audit_jsonl))]
    if audit_sql_url:
        from .audit.sql_sink import SQLAuditSink
        sinks.append(SQLAuditSink(audit_sql_url))
    audit_sink = CompositeAuditSink(sinks)

    run_id_dir = Path(tpt_output_dir) / job.table_name / processing_date
    runner = JobRunner(
        td_cursor=cursor, td_host=td_host, td_user=td_user, td_password=td_password,
        obs_config=obs_config, obs_bucket=obs_bucket, audit_sink=audit_sink,
        paths=RunPaths(
            tpt_output_dir=run_id_dir / "tpt",
            partition_split_dir=run_id_dir / "split",
            datax_logs_dir=Path("/data01/td2hive/logs/datax"),
        ),
        datax_home=datax_home,
    )
    return runner.run(
        job, date.fromisoformat(processing_date), row_limit=row_limit, force=force
    )


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
