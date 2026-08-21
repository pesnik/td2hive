#!/usr/bin/env python3
"""YAML job-spec model: one file per table, human-authored, committed.
This is the source of truth a run is driven from - DataX's own job.json is
a generated artifact built from this at run time (see datax/job_spec.py),
never hand-edited.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

import yaml


@dataclass
class PartitionColumn:
    column: str
    dynamic: bool = False


@dataclass
class SourceSpec:
    owner: str
    load_tables: List[str]
    columns: List[str]


@dataclass
class TargetSpec:
    hive_owner: str
    hive_table: str
    obs_dir: str
    format: str  # hdfswriter's fileType: text | orc | parquet
    partitions: List[PartitionColumn] = field(default_factory=list)

    def __post_init__(self):
        # Real OBS paths are always lowercase, even if obs_dir is authored
        # (or generated from an upstream config table) in mixed case -
        # confirmed against real production data: a config value like
        # "SOME_TABLE" pointed at an OBS directory that was actually
        # "some_table" on disk. Every path built from obs_dir must match
        # or every OBS listing silently finds nothing - normalize once,
        # here, rather than relying on every caller to remember.
        self.obs_dir = self.obs_dir.lower()


@dataclass
class ErrorLimit:
    record: int = 0
    percentage: float = 0.0


@dataclass
class RunSetting:
    error_limit: ErrorLimit = field(default_factory=ErrorLimit)
    speed_channel: int = 1
    # hdfswriter's own writeMode - kept at 'append' by convention. This
    # pipeline's delete-then-write of the target OBS partition path is the
    # sole idempotency mechanism; hdfswriter must never also try to manage
    # conflicts in the same directory (double-cleanup/race risk).
    write_mode: str = "append"
    # Caps how many partition values' DataX writes job_runner.run_units()
    # groups into one job.json (one shared JVM/channel pool), by summed
    # channel count. DataX's own JVM cold-start is a real fixed cost per
    # invocation - without batching, a table with hundreds of dynamic
    # partition values means hundreds of JVM launches for the sequential
    # single-process `run` path. Only applies there; `run-unit` (the
    # externally-parallelizable primitive for k8s/Airflow/Argo) always
    # does exactly one partition value per JVM by design, since batching
    # across pods/containers would defeat the point of distributing them.
    #
    # None (the default) resolves to speed_channel in __post_init__ -
    # i.e. NO batching benefit unless a table explicitly opts into a
    # higher value. A flat constant default here was a real mistake,
    # found live against production: memory scales with channel count
    # (a real 16-channel single-partition-value write already sat at
    # ~29GB/32GB, 91% of cap, on a ~36-column table), and a flat 64
    # silently let job_runner combine ALL of a 2-load-table job's units
    # (2 load tables x 2 partition values x 16 channels = 64) into ONE
    # JVM - untested territory that could plausibly have OOM'd. Batching's
    # real value is for tables with MANY SMALL partitions, not few
    # wide/heavy ones - opt in per table by setting this explicitly once
    # you've confirmed the memory headroom, don't rely on a shared
    # default to guess right for every table's row width.
    max_channels_per_job: Optional[int] = None

    def __post_init__(self):
        if self.max_channels_per_job is None:
            self.max_channels_per_job = self.speed_channel


@dataclass
class JobSpec:
    table_name: str
    source: SourceSpec
    target: TargetSpec
    loader: str  # datax | legacy_write_nos | legacy_tpt_csv_stage
    setting: RunSetting = field(default_factory=RunSetting)
    # Days to retain processing_date partitions before they're expired by
    # `td2hive retention run` (retention.py). None (the default, matching
    # most tables' blank RETENTION column today) means never expire - a
    # table must opt in explicitly, retention is never implicit.
    retention_days: Optional[int] = None

    @property
    def uses_datax(self) -> bool:
        return self.loader == "datax"


def load_jobspec(path: Path) -> JobSpec:
    with open(path) as f:
        raw = yaml.safe_load(f)

    source_raw = raw["source"]["teradata"]
    source = SourceSpec(
        owner=source_raw["owner"],
        load_tables=source_raw["load_tables"],
        columns=raw["source"]["columns"],
    )

    target_raw = raw["target"]
    hive_raw = target_raw["hive"]
    partitions = [
        PartitionColumn(column=p["column"], dynamic=p.get("dynamic", False))
        for p in target_raw.get("partitions", [])
    ]
    target = TargetSpec(
        hive_owner=hive_raw["owner"],
        hive_table=hive_raw["table"],
        obs_dir=target_raw["obs_dir"],
        format=target_raw.get("format", "parquet"),
        partitions=partitions,
    )

    setting_raw = raw.get("setting", {})
    error_limit_raw = setting_raw.get("error_limit", {})
    setting = RunSetting(
        error_limit=ErrorLimit(
            record=error_limit_raw.get("record", 0),
            percentage=error_limit_raw.get("percentage", 0.0),
        ),
        speed_channel=setting_raw.get("speed_channel", 1),
        write_mode=setting_raw.get("write_mode", "append"),
        # None -> RunSetting.__post_init__ resolves it to speed_channel
        # (no batching benefit unless a table opts in explicitly - see
        # the field's own docstring for why a flat constant was wrong).
        max_channels_per_job=setting_raw.get("max_channels_per_job"),
    )

    return JobSpec(
        table_name=raw.get("table_name", path.stem),
        source=source,
        target=target,
        loader=raw["loader"],
        setting=setting,
        retention_days=raw.get("retention_days"),
    )


def load_jobs_dir(jobs_dir: Path) -> List[JobSpec]:
    return [load_jobspec(p) for p in sorted(jobs_dir.glob("*.yaml"))]
