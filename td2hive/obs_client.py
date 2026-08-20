#!/usr/bin/env python3
"""All OBS (S3-compatible) object operations this package needs, in one
place - deliberately boto3-only, not a vendor-specific OBS SDK. boto3 is
already a real dependency of this package and is more portable than a
vendor SDK for a package meant to stay boto3/S3-compatible throughout.
"""

import re
from dataclasses import dataclass
from datetime import date
from typing import List

from .reader import ObsConfig


def _client(obs_config: ObsConfig):
    import boto3
    from botocore.config import Config

    endpoint = re.sub(r"^https?://", "", obs_config.endpoint)
    return boto3.client(
        "s3",
        endpoint_url=f"https://{endpoint}",
        aws_access_key_id=obs_config.access_key,
        aws_secret_access_key=obs_config.secret_key,
        verify=False,
        # Required, not optional: boto3/botocore >=1.36 defaults to
        # streaming PutObject with aws-chunked trailing checksums, which
        # this OBS endpoint's S3-compat gateway doesn't fully strip - the
        # raw HTTP chunk-framing bytes get stored as if they were the
        # object's actual content. Confirmed via direct inspection
        # 2026-08-20 - a general boto3 1.36+ regression against non-AWS
        # S3-compatible backends, not specific to this deployment.
        config=Config(request_checksum_calculation="when_required"),
    )


def ensure_prefix_exists(obs_config: ObsConfig, bucket: str, prefix: str) -> None:
    """hdfswriter refuses to write to a path that doesn't already exist
    (Job.prepare() validation - "请先在hive端创建对应的数据库和表", observed
    2026-08-20). Creates a zero-byte marker object (key ending in '/',
    content-length 0 - confirmed via the OBS Hadoop connector's own
    objectRepresentsDirectory() check in OBSCommonUtils.java, which
    requires exactly that)."""
    s3 = _client(obs_config)
    prefix = prefix.strip("/") + "/"
    s3.put_object(Bucket=bucket, Key=prefix, Body=b"")


_DATE_PARTITION_RE = re.compile(r"processing_date=(\d{4}-\d{2}-\d{2})")


@dataclass
class DatedPartition:
    prefix: str  # full key prefix, e.g. "warehouse.db/some_table/processing_date=2026-01-01/"
    partition_date: date


def list_dated_partitions(obs_config: ObsConfig, bucket: str, base_prefix: str) -> List[DatedPartition]:
    """List every processing_date=YYYY-MM-DD "directory" directly under
    base_prefix, via prefix+delimiter listing (no marker objects required
    for reads - only writes need the marker, per hdfswriter's own check)."""
    s3 = _client(obs_config)
    base_prefix = base_prefix.strip("/") + "/"
    partitions: List[DatedPartition] = []
    continuation_token = None

    while True:
        kwargs = {"Bucket": bucket, "Prefix": base_prefix, "Delimiter": "/"}
        if continuation_token:
            kwargs["ContinuationToken"] = continuation_token
        resp = s3.list_objects_v2(**kwargs)

        for common_prefix in resp.get("CommonPrefixes", []):
            key_prefix = common_prefix["Prefix"]
            match = _DATE_PARTITION_RE.search(key_prefix)
            if not match:
                continue
            partitions.append(
                DatedPartition(
                    prefix=key_prefix,
                    partition_date=date.fromisoformat(match.group(1)),
                )
            )

        if resp.get("IsTruncated"):
            continuation_token = resp.get("NextContinuationToken")
        else:
            break

    return partitions


def delete_prefix(obs_config: ObsConfig, bucket: str, prefix: str) -> int:
    """Delete every object under prefix (including the folder marker
    itself), batched at 1000 (OBS's per-request delete limit, matching
    AWS S3). Returns the number of objects deleted."""
    s3 = _client(obs_config)
    continuation_token = None
    total_deleted = 0

    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if continuation_token:
            kwargs["ContinuationToken"] = continuation_token
        resp = s3.list_objects_v2(**kwargs)

        keys = [{"Key": obj["Key"]} for obj in resp.get("Contents", [])]
        if keys:
            for i in range(0, len(keys), 1000):
                batch = keys[i : i + 1000]
                s3.delete_objects(Bucket=bucket, Delete={"Objects": batch})
                total_deleted += len(batch)

        if resp.get("IsTruncated"):
            continuation_token = resp.get("NextContinuationToken")
        else:
            break

    return total_deleted
