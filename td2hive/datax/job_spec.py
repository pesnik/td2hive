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
from dataclasses import dataclass
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


@dataclass
class ContentSpec:
    """One partition value's txtfilereader -> hdfswriter pair within a
    job.json's `content` array. `local_csv_paths` is normally more than
    one file - TPT's multi-instance DataConnector (reader.TPTExporter,
    num_instances>1) already exports directly into several files for one
    partition value, round-robin distributed, so each content spec's own
    channel share is set to match len(local_csv_paths): each file becomes
    its own reader/writer task pair, genuinely running in parallel
    (txtfilereader.split() sizes tasks off file count, not the configured
    channel number - a single file never runs as more than one task
    regardless of channel count)."""

    local_csv_paths: List[Path]
    target_obs_path: str
    file_name: str
    exclude_columns: List[str] = ()


def build_job_json(
    content_specs: List[ContentSpec],
    columns: List[ResolvedColumn],
    file_type: str,
    field_delimiter: str,
    setting: RunSetting,
    obs_config: ObsConfig,
    obs_buffer_dir: str = "",
) -> dict:
    """Build one DataX job.json, with one `content` entry per content
    spec sharing a single JVM/channel pool. Multiple partition values'
    writes can share one job.json (and therefore one DataX JVM launch)
    this way - each still writes to its own target directory - which is
    the fix for DataX's real per-JVM cold-start cost mattering at
    real-world partition counts (a 200-partition-value table otherwise
    means 200 JVM launches for 200 single-partition job.json calls).
    `job_runner.run_units()` is what actually groups partition values
    into content_specs (bounded by a channel budget); a single-element
    list here reduces to exactly one partition value's job, unchanged
    from before this was generalized.

    `exclude_columns` on each spec must list every dynamic partition
    column (e.g. DATE_KEY). Their values live in the target directory
    name (DATE_KEY=<value>/), not in the file - Hive derives them from
    the path. The CSV(s) still have the full row layout, so this only
    changes which columns the reader *projects* by index.

    `obs_buffer_dir` overrides `fs.obs.buffer.dir` (the Huawei OBS Hadoop
    connector's local scratch dir for multipart-upload buffering before
    the real network transfer) - its own default is `${java.io.tmpdir}/obs`,
    i.e. /tmp, confirmed 2026-08-21 to be a real production risk: two
    real concurrent jobs' writes both buffering there exhausted /tmp's
    10GB partition ("No space left on device" mid-write, both jobs
    failed), while /data01 sat at 1.5TB free the whole time. Leave unset
    to keep the connector's own default (fine for a single job at a
    time, risky once more than one job's writes can overlap).
    """
    content = []
    for spec in content_specs:
        default_fs, path = _split_obs_path(spec.target_obs_path)
        exclude = set(spec.exclude_columns)

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
            "fileName": spec.file_name,
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
                **({"fs.obs.buffer.dir": obs_buffer_dir} if obs_buffer_dir else {}),
            },
        }

        content.append({
            "reader": {
                "name": "txtfilereader",
                "parameter": {
                    "path": [str(p) for p in spec.local_csv_paths],
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
        })

    total_channels = sum(len(spec.local_csv_paths) for spec in content_specs)
    return {
        "job": {
            "setting": {
                # Sum across every content spec sharing this JVM, not
                # job.setting - more channels than files is wasted (idle
                # channels), fewer would leave files unread by any task.
                "speed": {"channel": total_channels},
                "errorLimit": {
                    "record": setting.error_limit.record,
                    "percentage": setting.error_limit.percentage,
                },
            },
            "content": content,
        }
    }
