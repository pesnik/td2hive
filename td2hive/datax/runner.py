#!/usr/bin/env python3
"""Runs a generated job.json through `datax.py` as a subprocess and parses
DataX's own completion summary.

Security note (learned the hard way, 2026-08-20): DataX logs its full job
config - including the writer's hadoopConfig OBS credentials - at INFO
level on every run. This module NEVER returns raw log content to a caller;
only a parsed, credential-free DataxRunResult. If a human needs the raw
log, they must read log_path directly on disk and filter it themselves
(grep -v 'access.key|secret.key|fs.obs') - never pipe it through anything
that might display it in a transcript/terminal by accident.

Fast-fail signal only: this module's counts are DataX's own self-report,
which is exactly the kind of thing that masked the original silent-write
bug (Hive's INSERT also reported success). The only signal that ever sets
success/dq_mismatch is verify.py's independent count - see that module.
"""

import json
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

_READ_COUNT_RE = re.compile(r"读出记录总数\s*:\s*(\d+)")
_FAIL_COUNT_RE = re.compile(r"读写失败总数\s*:\s*(\d+)")
_COMPLETED_RE = re.compile(r"DataX jobId \[\d+\] completed successfully")


@dataclass
class DataxRunResult:
    exit_code: int
    succeeded: bool
    records_read: Optional[int]
    records_failed: Optional[int]
    log_path: Path

    def within_error_limit(self, error_limit_record: int) -> bool:
        if self.records_failed is None:
            return False
        return self.records_failed <= error_limit_record


class DataxRunner:
    # datax.py hardcodes -Xms1g -Xmx1g as its DEFAULT_JVM. Fine for a
    # laptop; on a real ingest host it just means smaller Parquet
    # row-group buffers and more GC churn than necessary. datax.py's -j
    # flag appends extra JVM args after the default, and the JVM honors
    # the LAST -Xmx/-Xms seen, so passing a bigger pair here overrides the
    # hardcoded default rather than conflicting with it.
    #
    # Raised from 16g/32g to 24g/64g after real production experience: a
    # real 16-channel write against a ~565M-row, ~36-column table sat at
    # ~29GB/32GB (91% of cap) mid-write on a 96-core/503GB host that was
    # otherwise ~7% utilized - too close to the ceiling for comfort given
    # an OOM mid-write means redoing the TPT export too (no partial-
    # resume). 64g is still under 13% of total host memory. Tune down for
    # a smaller host via jvm_opts.
    DEFAULT_JVM_OPTS = "-Xms24g -Xmx64g"

    def __init__(self, datax_home: Path, jvm_opts: str = DEFAULT_JVM_OPTS):
        self.datax_home = Path(datax_home)
        self.datax_py = self.datax_home / "bin" / "datax.py"
        self.jvm_opts = jvm_opts
        if not self.datax_py.exists():
            raise FileNotFoundError(f"datax.py not found under {datax_home}")

    def run(self, job_json: dict, job_dir: Path) -> DataxRunResult:
        """Write job_json to job_dir/job.json, run it, parse the summary.
        job_dir should be a run-scoped, git-ignored logs directory - the
        written job.json is a debugging artifact, never committed (it
        embeds live OBS credentials)."""
        job_dir.mkdir(parents=True, exist_ok=True)
        job_path = job_dir / "job.json"
        job_path.write_text(json.dumps(job_json, indent=4))
        job_path.chmod(0o600)  # contains live OBS credentials

        log_path = job_dir / "datax_run.log"
        with open(log_path, "w") as log_file:
            proc = subprocess.run(
                ["python3", str(self.datax_py), "-j", self.jvm_opts, str(job_path)],
                stdout=log_file,
                stderr=subprocess.STDOUT,
                timeout=3600,
            )

        log_text = log_path.read_text()
        succeeded = proc.returncode == 0 and bool(_COMPLETED_RE.search(log_text))
        read_match = _READ_COUNT_RE.search(log_text)
        fail_match = _FAIL_COUNT_RE.search(log_text)

        return DataxRunResult(
            exit_code=proc.returncode,
            succeeded=succeeded,
            records_read=int(read_match.group(1)) if read_match else None,
            records_failed=int(fail_match.group(1)) if fail_match else None,
            log_path=log_path,
        )
