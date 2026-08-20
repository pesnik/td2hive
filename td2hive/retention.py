#!/usr/bin/env python3
"""Retention: expire processing_date partitions older than a per-table
cutoff. Run standalone (`td2hive retention run[-all]`), never wired into
a backup run automatically - retention and loading are separate concerns
with separate schedules (e.g. retention on a weekly cron, loading daily).

Two things worth calling out, both deliberate:
- retention_days is a per-table JobSpec field (jobs/*.yaml) - any table
  can opt in, dynamically, the same way every other per-table setting in
  this package works. A table with retention_days=None (the default) is
  never touched - retention is never implicit.
- Every partition-expiry action is DRY-RUN BY DEFAULT. This is a
  destructive, irreversible operation (OBS object deletion) - the caller
  must pass dry_run=False explicitly to actually delete anything.

Hive metadata and OBS storage are dropped as two explicit steps, in a
fixed order (OBS data first, then the Hive partition entry) - matching
every other place in this package where storage and metadata are treated
as independently-verifiable, not one operation assumed to imply the
other. If OBS deletion fails, the Hive partition is deliberately left
pointing at now-partially-deleted data rather than silently orphaning
metadata that still claims complete data exists.
"""

from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import List

from loguru import logger

from . import obs_client
from .jobspec import JobSpec
from .partition_registrar import PartitionRegistrar
from .reader import ObsConfig


@dataclass
class PartitionExpiryResult:
    partition_date: date
    obs_prefix: str
    would_delete: bool  # True in dry-run, meaningless (delete already happened) otherwise
    obs_objects_deleted: int = 0
    hive_partition_dropped: bool = False
    error: str = ""


@dataclass
class RetentionResult:
    job_name: str
    retention_days: int
    cutoff_date: date
    dry_run: bool
    kept_count: int = 0
    expired: List[PartitionExpiryResult] = field(default_factory=list)

    @property
    def expired_count(self) -> int:
        return len(self.expired)

    @property
    def had_errors(self) -> bool:
        return any(e.error for e in self.expired)


def process_retention(
    job: JobSpec,
    obs_config: ObsConfig,
    obs_bucket: str,
    registrar: PartitionRegistrar,
    today: date = None,
    dry_run: bool = True,
) -> RetentionResult:
    """Expire job's processing_date partitions older than
    job.retention_days. Returns a no-op RetentionResult (0 expired) if
    job.retention_days is not set - callers should still check
    result.retention_days rather than assume every job is retention-
    enabled."""
    today = today or date.today()

    if job.retention_days is None:
        return RetentionResult(
            job_name=job.table_name, retention_days=0, cutoff_date=today, dry_run=dry_run
        )

    cutoff_date = today - timedelta(days=job.retention_days)
    result = RetentionResult(
        job_name=job.table_name,
        retention_days=job.retention_days,
        cutoff_date=cutoff_date,
        dry_run=dry_run,
    )

    partitions = obs_client.list_dated_partitions(obs_config, obs_bucket, job.target.obs_dir)
    logger.info(f"{job.table_name}: found {len(partitions)} partitions under {job.target.obs_dir}")

    for partition in partitions:
        if partition.partition_date > cutoff_date:
            result.kept_count += 1
            continue

        expiry = PartitionExpiryResult(
            partition_date=partition.partition_date,
            obs_prefix=partition.prefix,
            would_delete=dry_run,
        )

        if dry_run:
            logger.info(
                f"[DRY RUN] Would expire {job.table_name} partition "
                f"{partition.partition_date} at obs://{obs_bucket}/{partition.prefix}"
            )
            result.expired.append(expiry)
            continue

        try:
            logger.info(f"Expiring {job.table_name} partition {partition.partition_date}")
            expiry.obs_objects_deleted = obs_client.delete_prefix(
                obs_config, obs_bucket, partition.prefix
            )
            partition_spec = f'processing_date="{partition.partition_date.isoformat()}"'
            expiry.hive_partition_dropped = registrar.drop_partition(
                job.target.hive_owner, job.target.hive_table, partition_spec
            )
            if not expiry.hive_partition_dropped:
                expiry.error = "OBS data deleted but Hive DROP PARTITION failed - metastore now points at missing data, needs manual cleanup"
                logger.error(f"{job.table_name} {partition.partition_date}: {expiry.error}")
        except Exception as e:
            expiry.error = str(e)
            logger.error(f"Failed to expire {job.table_name} partition {partition.partition_date}: {e}")

        result.expired.append(expiry)

    return result
