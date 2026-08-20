#!/usr/bin/env python3
"""Builds DataX's job.json - a generated artifact, never hand-edited. Every
value here traces back to the YAML JobSpec (jobspec.py) plus the dynamically
resolved Teradata column types (column_types.py). Reader is always
txtfilereader (local CSV, matches TPT's pipe-delimited output exactly);
writer is always hdfswriter (writes straight to the OBS partition path via
the Hadoop FileSystem API, bypassing Hive's query engine for the write).

Validated end-to-end against real production Teradata data: TPT export ->
txtfilereader -> hdfswriter -> a real OBS bucket, independently verified
via boto3 and beeline (not trusting DataX's own self-report - see
verify.py), exact row and field match.
"""

import csv
import re
from dataclasses import dataclass
from pathlib import Path
from typing import List

from ..column_types import ResolvedColumn, to_datax_reader_type, to_datax_writer_type
from ..jobspec import RunSetting
from ..reader import ObsConfig

_OBS_URL_RE = re.compile(r"^obs://([^/]+)(/.*)?$")


@dataclass
class PartitionGroup:
    """One dynamic-partition value's slice of a load, already split out of
    the TPT-exported CSV by split_csv_by_partition_value."""

    partition_values: dict  # {column_name: value} for every dynamic partition column
    local_csv_path: Path
    row_count: int


def split_csv_by_partition_value(
    csv_path: Path,
    columns: List[ResolvedColumn],
    dynamic_partition_columns: List[str],
    output_dir: Path,
    delimiter: str = "|",
) -> List[PartitionGroup]:
    """Split a TPT-exported CSV into one file per distinct combination of
    dynamic partition column values, mirroring what Hive's dynamic-
    partition INSERT does - done here in pure Python instead of trusted to
    that opaque engine, so the fan-out is inspectable (each group's row
    count is known up front, not discovered after the fact).

    No dynamic partition columns -> a single group covering the whole file
    (no-op split, matches a table with only static partitioning).
    """
    if not dynamic_partition_columns:
        with open(csv_path) as f:
            row_count = sum(1 for _ in f)
        return [PartitionGroup(partition_values={}, local_csv_path=csv_path, row_count=row_count)]

    col_names = [name for name, _, _ in columns]
    col_indices = [col_names.index(c) for c in dynamic_partition_columns]

    output_dir.mkdir(parents=True, exist_ok=True)
    writers: dict = {}
    counts: dict = {}

    with open(csv_path, newline="") as f:
        reader = csv.reader(f, delimiter=delimiter)
        for row in reader:
            key = tuple(row[i] for i in col_indices)
            if key not in writers:
                suffix = "_".join(key)
                out_path = output_dir / f"{csv_path.stem}__{suffix}.csv"
                writers[key] = open(out_path, "w", newline="")
                counts[key] = 0
            csv.writer(writers[key], delimiter=delimiter).writerow(row)
            counts[key] += 1

    groups = []
    for key, handle in writers.items():
        handle.close()
        partition_values = dict(zip(dynamic_partition_columns, key))
        suffix = "_".join(key)
        out_path = output_dir / f"{csv_path.stem}__{suffix}.csv"
        groups.append(
            PartitionGroup(
                partition_values=partition_values,
                local_csv_path=out_path,
                row_count=counts[key],
            )
        )
    return groups


def _split_obs_path(obs_full_path: str) -> tuple:
    """obs://bucket/some/dir -> ('obs://bucket', '/some/dir')"""
    m = _OBS_URL_RE.match(obs_full_path)
    if not m:
        raise ValueError(f"Not a valid obs:// path: {obs_full_path}")
    bucket, path = m.group(1), (m.group(2) or "/")
    return f"obs://{bucket}", path


def build_job_json(
    local_csv_path: Path,
    columns: List[ResolvedColumn],
    target_obs_path: str,
    file_name: str,
    file_type: str,
    field_delimiter: str,
    setting: RunSetting,
    obs_config: ObsConfig,
    exclude_columns: List[str] = (),
) -> dict:
    """Build one DataX job.json for one txtfilereader -> hdfswriter run,
    covering exactly one partition-value group's CSV slice.

    `exclude_columns` must list every dynamic partition column (e.g.
    DATE_KEY). Their values live in the target directory name
    (DATE_KEY=<value>/), not in the file - Hive derives them from the
    path. `local_csv_path` still has the full row layout (the split step
    doesn't rewrite rows), so this only changes which columns the reader
    *projects* by index - the reader still parses the full row.
    """
    default_fs, path = _split_obs_path(target_obs_path)
    exclude = set(exclude_columns)

    reader_columns = [
        {"index": i, "type": to_datax_reader_type(tpt_type)}
        for i, (name, tpt_type, _) in enumerate(columns)
        if name not in exclude
    ]
    writer_columns = [
        {"name": name, "type": to_datax_writer_type(tpt_type)}
        for name, tpt_type, _ in columns
        if name not in exclude
    ]

    writer_param = {
        "defaultFS": default_fs,
        "fileType": file_type,
        "path": path,
        "fileName": file_name,
        "writeMode": setting.write_mode,
        "column": writer_columns,
        "hadoopConfig": {
            "fs.obs.impl": "org.apache.hadoop.fs.obs.OBSFileSystem",
            "fs.obs.access.key": obs_config.access_key,
            "fs.obs.secret.key": obs_config.secret_key,
            "fs.obs.endpoint": obs_config.endpoint,
        },
    }
    # hdfswriter only accepts fieldDelimiter for text; other formats
    # (orc/parquet) reject the parameter outright.
    if file_type == "text":
        writer_param["fieldDelimiter"] = field_delimiter

    return {
        "job": {
            "setting": {
                "speed": {"channel": setting.speed_channel},
                "errorLimit": {
                    "record": setting.error_limit.record,
                    "percentage": setting.error_limit.percentage,
                },
            },
            "content": [
                {
                    "reader": {
                        "name": "txtfilereader",
                        "parameter": {
                            "path": [str(local_csv_path)],
                            "encoding": "UTF-8",
                            "column": reader_columns,
                            "fieldDelimiter": field_delimiter,
                        },
                    },
                    "writer": {
                        "name": "hdfswriter",
                        "parameter": writer_param,
                    },
                }
            ],
        }
    }
