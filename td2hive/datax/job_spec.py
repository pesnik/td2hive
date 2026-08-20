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
verify.py), exact row and field match, including a table with a genuine
dynamic partition column at real production scale (500M+ rows).
"""

import re
from pathlib import Path
from typing import List

from ..column_types import ResolvedColumn, to_datax_reader_type, to_datax_writer_type
from ..jobspec import RunSetting
from ..reader import ObsConfig

_OBS_URL_RE = re.compile(r"^obs://([^/]+)(/.*)?$")


def _split_obs_path(obs_full_path: str) -> tuple:
    """obs://bucket/some/dir -> ('obs://bucket', '/some/dir')"""
    m = _OBS_URL_RE.match(obs_full_path)
    if not m:
        raise ValueError(f"Not a valid obs:// path: {obs_full_path}")
    bucket, path = m.group(1), (m.group(2) or "/")
    return f"obs://{bucket}", path


def build_job_json(
    local_csv_paths: List[Path],
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
    covering exactly one partition value's data. `local_csv_paths` is
    normally more than one file - TPT's multi-instance DataConnector
    (reader.TPTExporter, num_instances>1) already exports directly into
    several files for one partition value, round-robin distributed, so
    DataX's channel count is set to match len(local_csv_paths): each file
    becomes its own reader/writer task pair, genuinely running in
    parallel (txtfilereader.split() sizes tasks off file count, not the
    configured channel number - a single file never runs as more than one
    task regardless of channel count).

    `exclude_columns` must list every dynamic partition column (e.g.
    DATE_KEY). Their values live in the target directory name
    (DATE_KEY=<value>/), not in the file - Hive derives them from the
    path. The CSV(s) still have the full row layout, so this only changes
    which columns the reader *projects* by index.
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
        # hdfswriter's validateParameter() requires fieldDelimiter
        # unconditionally, for every fileType - confirmed 2026-08-20 by
        # HdfsWriter-01 "[fieldDelimiter]是必填参数" against a real PARQUET
        # write. Not just a TEXT-format thing, despite what the actual
        # delimiting is used for (TEXT: real row separator; ORC/PARQUET:
        # required by validation but doesn't affect the encoded output).
        "fieldDelimiter": field_delimiter,
        "hadoopConfig": {
            "fs.obs.impl": "org.apache.hadoop.fs.obs.OBSFileSystem",
            "fs.obs.access.key": obs_config.access_key,
            "fs.obs.secret.key": obs_config.secret_key,
            "fs.obs.endpoint": obs_config.endpoint,
        },
    }

    return {
        "job": {
            "setting": {
                # Matches the number of input files, not job.setting -
                # more channels than files is wasted (idle channels),
                # fewer would leave files unread by any task.
                "speed": {"channel": len(local_csv_paths)},
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
                            "path": [str(p) for p in local_csv_paths],
                            "encoding": "UTF-8",
                            "column": reader_columns,
                            "fieldDelimiter": field_delimiter,
                            # Without this, txtfilereader has no default
                            # nullFormat (confirmed in its source - "注意:
                            # nullFormat 没有默认值") and never recognizes an
                            # empty field as NULL. TPT exports Teradata NULLs
                            # as empty strings (IndicatorMode='N' - no NULL
                            # indicator bytes), so every real NULL in an
                            # INTEGER-typed column would otherwise be rejected
                            # as dirty data ("无法将[] 转换为[LONG]"), confirmed
                            # against real production data 2026-08-20.
                            "nullFormat": "",
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
