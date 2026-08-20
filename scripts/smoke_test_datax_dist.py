#!/usr/bin/env python3
"""Gate for promoting a DataX distribution to `current`: proves
streamreader -> hdfswriter can actually write to real OBS through this
exact distribution, independently verified (not trusting DataX's own
self-report - see td2hive/verify.py's docstring for why that's a hard
rule in this package, not a suggestion).

Run on the target host, after the candidate DataX version is extracted
but BEFORE it's symlinked to `current` - deploy_datax_dist.sh only
symlinks if this exits 0. Requires td2hive itself to already be deployed
(imports td2hive.obs_client), so deploy the app before the DataX dist.

Usage:
  DATAX_HOME=/data01/td2hive/datax/<candidate> \\
  OBS_ACCESS_KEY=... OBS_SECRET_KEY=... OBS_ENDPOINT=... OBS_BUCKET=... \\
  python3 smoke_test_datax_dist.py
"""

import json
import os
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from td2hive import obs_client
from td2hive.reader import ObsConfig


def main() -> int:
    datax_home = Path(os.environ["DATAX_HOME"])
    datax_py = datax_home / "bin" / "datax.py"
    if not datax_py.exists():
        print(f"FAIL: {datax_py} does not exist", file=sys.stderr)
        return 1

    obs_config = ObsConfig(
        access_key=os.environ["OBS_ACCESS_KEY"],
        secret_key=os.environ["OBS_SECRET_KEY"],
        endpoint=os.environ["OBS_ENDPOINT"],
    )
    bucket = os.environ["OBS_BUCKET"]
    run_id = f"smoke_{int(time.time())}"
    target_path = f"/td2hive_smoke_test/{run_id}"

    print(f"Preparing scratch OBS path: obs://{bucket}{target_path}")
    obs_client.ensure_prefix_exists(obs_config, bucket, target_path)

    job = {
        "job": {
            "setting": {"speed": {"channel": 1}},
            "content": [
                {
                    "reader": {
                        "name": "streamreader",
                        "parameter": {
                            "column": [
                                {"value": "smoke_test", "type": "string"},
                                {"value": 1, "type": "long"},
                            ],
                            "sliceRecordCount": 10,
                        },
                    },
                    "writer": {
                        "name": "hdfswriter",
                        "parameter": {
                            "defaultFS": f"obs://{bucket}",
                            "fileType": "text",
                            "path": target_path,
                            "fileName": "smoke",
                            "writeMode": "append",
                            "fieldDelimiter": "|",
                            "column": [
                                {"name": "col1", "type": "STRING"},
                                {"name": "col2", "type": "BIGINT"},
                            ],
                            "hadoopConfig": {
                                "fs.obs.impl": "org.apache.hadoop.fs.obs.OBSFileSystem",
                                "fs.obs.access.key": obs_config.access_key,
                                "fs.obs.secret.key": obs_config.secret_key,
                                "fs.obs.endpoint": obs_config.endpoint,
                            },
                        },
                    },
                }
            ],
        }
    }

    # Logs go to a persistent location, not a self-destructing TemporaryDirectory
    # - a failed run is exactly when you need the log, and it embeds live OBS
    # credentials (DataX logs its full job config at INFO level), so it's
    # deliberately kept out-of-band rather than printed - only ever read
    # directly on disk with credential lines filtered.
    log_dir = Path("/data01/td2hive/logs/smoke_test") / run_id
    log_dir.mkdir(parents=True, exist_ok=True)
    job_path = log_dir / "smoke_job.json"
    job_path.write_text(json.dumps(job))
    job_path.chmod(0o600)
    log_path = log_dir / "smoke.log"

    print("Running DataX...")
    with open(log_path, "w") as log_file:
        proc = subprocess.run(
            ["python3", str(datax_py), str(job_path)],
            stdout=log_file, stderr=subprocess.STDOUT, timeout=300,
        )
    log_text = log_path.read_text()

    if proc.returncode != 0 or "completed successfully" not in log_text:
        print("FAIL: DataX run did not complete successfully.", file=sys.stderr)
        print(f"(log not shown - may contain OBS credentials; inspect directly: "
              f"grep -v 'access.key|secret.key|fs.obs' {log_path})",
              file=sys.stderr)
        return 1

    print("DataX reported success. Verifying independently (not trusting the self-report)...")
    import boto3
    from botocore.config import Config
    import re

    endpoint = re.sub(r"^https?://", "", obs_config.endpoint)
    s3 = boto3.client(
        "s3", endpoint_url=f"https://{endpoint}",
        aws_access_key_id=obs_config.access_key,
        aws_secret_access_key=obs_config.secret_key,
        verify=False, config=Config(request_checksum_calculation="when_required"),
    )
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=target_path.lstrip("/") + "/smoke")
    data_objects = [o for o in resp.get("Contents", []) if o["Size"] > 0]
    if len(data_objects) != 1:
        print(f"FAIL: expected exactly 1 data object, found {len(data_objects)}", file=sys.stderr)
        return 1

    body = s3.get_object(Bucket=bucket, Key=data_objects[0]["Key"])["Body"].read()
    row_count = len(body.decode().strip().splitlines())
    if row_count != 10:
        print(f"FAIL: expected 10 rows, independently counted {row_count}", file=sys.stderr)
        return 1

    print(f"PASS: independently verified {row_count}/10 rows written through {datax_home}")
    obs_client.delete_prefix(obs_config, bucket, target_path.lstrip("/") + "/")
    print("Cleaned up scratch test data.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
