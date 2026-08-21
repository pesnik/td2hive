#!/usr/bin/env python3
"""TPT-based extraction: Teradata table -> local CSV via a dockerized
`teradata/tpt` (wraps FastExport). This is the extraction engine for every
loader (legacy CSV-staging+INSERT and the DataX hdfswriter path alike) -
TDCH/Sqoop precedent and this pipeline's own validated experience both
confirm TPT/FastExport, not generic JDBC, is the right idiom for
Teradata at volume. Relocated unchanged from tpt_export.py.
"""

import re
import shutil
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List

from loguru import logger

from .column_types import ResolvedColumn

# Field delimiter shared by the TPT export CSV and DataX's read of it
# (job_runner.py passes this same value into build_job_json's
# field_delimiter). Not '|' - confirmed live: a real VARCHAR column
# value containing a literal '|' (or an embedded newline) silently
# misaligned every column after it, since DELIMITED text has no
# quoting/escaping mechanism here. ASCII Unit Separator (0x1F) is the
# character the ASCII standard itself designates for exactly this
# purpose (FS/GS/RS/US at 0x1C-0x1F are literally "hierarchical data
# separators") - real business text essentially never contains a raw
# control character.
#
# TPT's DataConnector operator does NOT accept a control character
# through the plain TextDelimiter attribute at all (confirmed via
# Teradata's own docs: "cannot be a control character other than a
# tab") - a first attempt using TextDelimiter = X'01' (a Teradata SQL
# hex literal) failed with TPT02954, since TPT's own job-script grammar
# doesn't accept a hex literal as an attribute's Initial Value either.
# The real, documented (if little-known) mechanism is the separate
# TextDelimiterHex attribute, which takes plain hex digits as a quoted
# string with no prefix (confirmed against a real working TPT example:
# `TextDelimiterHex = '1F'`) - used INSTEAD OF TextDelimiter, not
# alongside it.
FIELD_DELIMITER = "\x1f"
_TPT_TEXT_DELIMITER_HEX = "1F"


@dataclass
class ObsConfig:
    """OBS-native credentials, separate from Teradata's NOS AUTHORIZATION
    object - nothing in this package touches Teradata's NOS auth. See
    obs_client.py for the boto3 (S3-compatible) operations that use this."""

    access_key: str
    secret_key: str
    endpoint: str


def _build_tpt_job_script(columns: List[ResolvedColumn], num_instances: int = 1) -> str:
    # Column names are double-quoted here - TPT's own job-script grammar
    # has a broader reserved-word set than Teradata SQL itself (found
    # live: a real column named NAME - not reserved in SQL - failed
    # TPT02954 "missing { REGULAR_IDENTIFIER_ ... }" in DEFINE SCHEMA
    # unquoted). Quoting is harmless for every other column too.
    schema_lines = ",\n    ".join(f'"{name}" {tpt_type}' for name, tpt_type, _ in columns)
    # Multiple FILE_WRITER instances writing to `num_instances` output files
    # in parallel, round-robin-distributed by `tbuild -C` (see export()).
    # Validated 2026-08-20 against 2M real rows: exact instance count of
    # files, near-even row distribution, 0 rows lost - this is what makes
    # a single export produce several files DataX can read in parallel,
    # with no local re-read/re-split pass needed afterward. Confirmed this
    # genuinely requires volume to kick in - a small (~2K row) test landed
    # everything in instance 1, since round-robin operates at the transport
    # block level, not per-row; real loads are always far larger than one
    # block, so this isn't a concern in practice.
    apply_target = f"FILE_WRITER[{num_instances}]" if num_instances > 1 else "FILE_WRITER"
    return f"""DEFINE JOB EXPORT_TABLE_JOB
DESCRIPTION 'Export one Teradata table to a delimited flat file via FastExport'
(
  DEFINE SCHEMA SOURCE_SCHEMA
  (
    {schema_lines}
  );

  DEFINE OPERATOR EXPORT_OP
  TYPE EXPORT
  SCHEMA SOURCE_SCHEMA
  ATTRIBUTES
  (
    VARCHAR TdpId = @TdpId,
    VARCHAR UserName = @UserName,
    VARCHAR UserPassword = @UserPassword,
    VARCHAR SelectStmt = @SelectStmt,
    VARCHAR PrivateLogName = 'export_op_log'
  );

  DEFINE OPERATOR FILE_WRITER
  TYPE DATACONNECTOR CONSUMER
  SCHEMA SOURCE_SCHEMA
  ATTRIBUTES
  (
    VARCHAR DirectoryPath = @OutputDir,
    VARCHAR FileName = @OutputFile,
    VARCHAR Format = 'DELIMITED',
    VARCHAR TextDelimiterHex = '{_TPT_TEXT_DELIMITER_HEX}',
    -- Every field quoted (not just ones that need it) - simpler,
    -- unambiguous pairing with DataX's useTextQualifier reader config
    -- (see job_spec.py) than leaving TPT to decide per-field which
    -- ones need quoting. Preserves an embedded delimiter or CR/LF
    -- inside a VARCHAR value unchanged, instead of the data itself
    -- needing to change to survive export - confirmed live 2026-08-22:
    -- a real column's embedded \r (not even \n) split one logical row
    -- into two fragments downstream when unquoted.
    VARCHAR QuotedData = 'Yes',
    VARCHAR OpenQuoteMark = '"',
    VARCHAR CloseQuoteMark = '"',
    VARCHAR OpenMode = 'Write',
    VARCHAR IndicatorMode = 'N'
  );

  APPLY TO OPERATOR ({apply_target})
  SELECT * FROM OPERATOR (EXPORT_OP);
);
"""


def build_select_stmt(
    schema: str,
    table: str,
    columns: List[ResolvedColumn],
    row_limit: int = 0,
    where_clause: str = "",
) -> str:
    """Build the TPT EXPORT operator's SelectStmt. `row_limit` is for
    scratch/proof runs only (TOP N). `where_clause` scopes the export to
    one dynamic-partition value (e.g. "DATE_KEY = 7901") - TPT does the
    partitioning itself via one export per distinct value, rather than a
    single full export split apart afterward in Python.

    Column names are double-quoted, matching _build_tpt_job_script's own
    DEFINE SCHEMA quoting - not required by Teradata SQL itself (NAME
    isn't SQL-reserved), but consistent quoting everywhere a column name
    is embedded avoids depending on which specific words happen to be
    reserved in which of TPT's job-script grammar vs. Teradata SQL.

    When needs_cast, casts to tpt_type itself - resolve_column_types()
    already resolved tpt_type to whatever the CAST's target must be
    (FLOAT for DECIMAL-like columns, VARCHAR(n) for DATE/TIMESTAMP), so
    the cast target is never hardcoded here.

    No sanitization of column values happens here - an embedded CR/LF
    or delimiter character inside a VARCHAR value must survive export
    unchanged. That's handled by quoting (_build_tpt_job_script's
    QuotedData/OpenQuoteMark on the TPT side, csvReaderConfig's
    useTextQualifier on the DataX read side - see job_spec.py), not by
    altering the data itself."""
    exprs = [
        f'CAST("{name}" AS {tpt_type}) AS "{name}"' if needs_cast else f'"{name}"'
        for name, tpt_type, needs_cast in columns
    ]
    top_clause = f"TOP {row_limit} " if row_limit else ""
    where_sql = f" WHERE {where_clause}" if where_clause else ""
    return f"SELECT {top_clause}{', '.join(exprs)} FROM {schema}.{table}{where_sql};"


class TPTExporter:
    """Runs a TPT export via `tbuild`, either directly as a local
    subprocess (if `tbuild` is already on PATH - true when this process's
    own container image is built `FROM teradata/tpt` rather than the
    default python-only image, see docker/Dockerfile.td2hive-tpt) or by
    spawning a sibling `teradata/tpt` Docker container (the original,
    still-default mode - requires access to the Docker daemon, either a
    real host or a mounted docker.sock).

    The local-subprocess mode exists specifically to remove the Docker-
    in-Docker dependency in a Kubernetes context: `docker run` from
    inside a k8s pod means either a privileged DinD sidecar or mounting
    the host node's docker.sock (usually disallowed on managed clusters
    for good reason), neither of which is needed if `tbuild` itself is
    just part of the pod's own image. Auto-detected via `shutil.which`,
    not configured - whichever the image actually provides is the one
    used, with no separate flag to keep in sync with how the image was
    built.

    Docker mode requires Docker and the teradata/tpt image to already be
    present on the execution host - this does not pull the image
    (production hosts are typically network-restricted; pull it once via
    `docker save teradata/tpt | ssh host docker load` from a host with
    internet access, see tpt/README or the deployment notes).
    """

    DOCKER_IMAGE = "teradata/tpt:latest"
    TIMEOUT_SECONDS = 3600

    def __init__(self, td_host: str, td_user: str, td_password: str, output_dir: str):
        self.td_host = td_host
        self.td_user = td_user
        self.td_password = td_password
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        # Docker mode's container's internal user (ttuuser, uid 1001) isn't
        # the host user this runs as - needs write access to land the
        # exported CSV here regardless of which mode ends up used.
        self.output_dir.chmod(0o777)
        self._local_tbuild = shutil.which("tbuild")

    def export(
        self,
        schema: str,
        table: str,
        columns: List[ResolvedColumn],
        row_limit: int = 0,
        where_clause: str = "",
        num_instances: int = 1,
        file_label: str = "",
    ) -> List[Path]:
        """Export `schema.table` to num_instances local CSV file(s) under
        output_dir, return their paths. `where_clause` scopes to one
        partition value; `file_label` keeps output filenames distinct
        across multiple export() calls for the same table (e.g. one call
        per distinct partition value) - required, since without it every
        call would write to the same filename and overwrite the last."""
        base_name = f"{table.lower()}{'_' + file_label if file_label else ''}.csv"
        job_script = _build_tpt_job_script(columns, num_instances)
        select_stmt = build_select_stmt(schema, table, columns, row_limit, where_clause)
        job_name = f"export_{table.lower()}_{int(time.time())}"
        cyclic_flag = ["-C"] if num_instances > 1 else []

        if self._local_tbuild:
            self._export_via_local_tbuild(
                schema, table, base_name, job_script, select_stmt, job_name, cyclic_flag
            )
        else:
            self._export_via_docker(
                schema, table, base_name, job_script, select_stmt, job_name, cyclic_flag
            )

        if num_instances <= 1:
            return [self.output_dir / base_name]
        return [self.output_dir / f"{base_name}-{i}" for i in range(1, num_instances + 1)]

    def _export_via_local_tbuild(
        self, schema, table, base_name, job_script, select_stmt, job_name, cyclic_flag
    ) -> None:
        """Runs tbuild directly - no container boundary, so OutputDir is
        this process's own real output_dir path, not a mounted /tpt_output.
        Blocks until tbuild exits (no polling needed, unlike Docker mode:
        a plain subprocess naturally blocks for its own lifetime, it
        doesn't detach the way `docker run -d` does)."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            script_file = tmp_path / "export.tpt"
            script_file.write_text(job_script)
            jobvars_file = tmp_path / "jobvars.txt"
            jobvars_file.write_text(
                f"TdpId='{self.td_host}'\n"
                f"UserName='{self.td_user}'\n"
                f"UserPassword='{self.td_password}'\n"
                f"SelectStmt='{select_stmt}'\n"
                f"OutputDir='{self.output_dir}'\n"
                f"OutputFile='{base_name}'\n"
            )
            cmd = [
                self._local_tbuild, "-f", str(script_file), "-j", job_name,
                *cyclic_flag, "-v", str(jobvars_file),
            ]
            logger.info(f"Starting local tbuild for {schema}.{table}")
            try:
                result = subprocess.run(
                    cmd, capture_output=True, text=True, timeout=self.TIMEOUT_SECONDS,
                )
            except subprocess.TimeoutExpired:
                raise RuntimeError(
                    f"TPT export timed out after {self.TIMEOUT_SECONDS}s for {schema}.{table}"
                )
            logs = result.stdout + result.stderr
            self._check_tbuild_output(logs, schema, table)

    def _export_via_docker(
        self, schema, table, base_name, job_script, select_stmt, job_name, cyclic_flag
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            # tempfile.TemporaryDirectory() defaults to 0o700 (owner-only) -
            # the container's internal user needs to traverse into it too.
            tmp_path.chmod(0o755)
            script_file = tmp_path / "export.tpt"
            script_file.write_text(job_script)
            # Container's internal user (ttuuser, uid 1001) isn't the host
            # user that wrote this file - needs to be world-readable.
            script_file.chmod(0o644)

            jobvars_file = tmp_path / "jobvars.txt"
            jobvars_file.write_text(
                f"TdpId='{self.td_host}'\n"
                f"UserName='{self.td_user}'\n"
                f"UserPassword='{self.td_password}'\n"
                f"SelectStmt='{select_stmt}'\n"
                f"OutputDir='/tpt_output'\n"
                f"OutputFile='{base_name}'\n"
            )
            # Container's internal user (ttuuser, uid 1001) isn't the host
            # user that wrote this file - needs to be world-readable.
            jobvars_file.chmod(0o644)

            cmd = [
                "docker", "run", "-d",
                "-e", "accept_license=Y",
                "-v", f"{tmp_path}:/tpt_scripts:ro",
                "-v", f"{self.output_dir}:/tpt_output",
                "--entrypoint", "bash",
                self.DOCKER_IMAGE,
                "-c",
                "sudo /opt/teradata/client/20.00/bin/tdwallet installSoftware "
                ">/dev/null 2>&1 || true; "
                f"tbuild -f /tpt_scripts/export.tpt -j '{job_name}' "
                f"{' '.join(cyclic_flag)} -v /tpt_scripts/jobvars.txt",
            ]
            logger.info(f"Starting TPT export container for {schema}.{table}")
            start_result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            if start_result.returncode != 0:
                raise RuntimeError(
                    f"Failed to start TPT container for {schema}.{table}: "
                    f"{start_result.stderr[-2000:]}"
                )
            container_id = start_result.stdout.strip()

            try:
                self._wait_for_completion(container_id, schema, table)
            finally:
                # tbuild's process doesn't exit on its own once the job is
                # done (observed repeatedly) - --rm never fires, so this has
                # to be forced regardless of success/failure.
                subprocess.run(
                    ["docker", "rm", "-f", container_id],
                    capture_output=True, timeout=60,
                )

    @staticmethod
    def _is_tbuild_failure(logs: str) -> bool:
        """"Job terminated with status N." (N != 0) is tbuild's own
        generic failure marker - broader than the two specific substrings
        this used to check for, which missed at least one real failure
        (TPT04187 malformed jobvars.txt, confirmed 2026-08-20) and would
        have spun for the full timeout instead of failing fast. Shared by
        both the local-subprocess and Docker paths so they can't drift."""
        return bool(
            "terminated (status" in logs
            or "compilation failed" in logs
            or re.search(r"Job terminated with status [1-9]", logs)
        )

    def _check_tbuild_output(self, logs: str, schema: str, table: str) -> None:
        """Local mode has no "still running" ambiguity the way Docker
        mode's polling does - subprocess.run already blocked until tbuild
        fully exited, so anything other than the success marker is a
        failure, known-pattern or not."""
        if "completed successfully" in logs:
            logger.info(f"TPT export completed for {schema}.{table}")
            return
        raise RuntimeError(f"TPT export failed for {schema}.{table}: {logs[-2000:]}")

    def _wait_for_completion(
        self, container_id: str, schema: str, table: str, poll_interval: int = 15
    ) -> None:
        """Poll `docker logs` for tbuild's own completion marker, since the
        container's process never exits on its own once the job is done."""
        deadline = time.time() + self.TIMEOUT_SECONDS
        last_log_at = 0.0
        while time.time() < deadline:
            result = subprocess.run(
                ["docker", "logs", container_id],
                capture_output=True, text=True, timeout=30,
            )
            logs = result.stdout + result.stderr
            if "completed successfully" in logs:
                logger.info(f"TPT export completed for {schema}.{table}")
                return
            if self._is_tbuild_failure(logs):
                raise RuntimeError(
                    f"TPT export failed for {schema}.{table}: {logs[-2000:]}"
                )
            if time.time() - last_log_at > 120:
                logger.info(f"TPT export for {schema}.{table} still running...")
                last_log_at = time.time()
            time.sleep(poll_interval)
        raise RuntimeError(
            f"TPT export timed out after {self.TIMEOUT_SECONDS}s for {schema}.{table}"
        )


class ObsUploader:
    """Uploads a local file to OBS via obsutil, using per-invocation
    credentials only - never touches obsutil's persistent/global config.

    Only needed by the legacy CSV-staging+INSERT loader - the DataX
    hdfswriter loader writes straight to OBS itself and doesn't use this."""

    OBSUTIL_BIN = "/data01/obsutil/obsutil"
    TIMEOUT_SECONDS = 3600

    def __init__(self, config: ObsConfig):
        self.config = config

    def upload(self, local_path: Path, obs_target: str) -> None:
        cmd = [
            self.OBSUTIL_BIN, "cp", str(local_path), obs_target,
            f"-i={self.config.access_key}",
            f"-k={self.config.secret_key}",
            f"-e={self.config.endpoint}",
            "-f", "-vlength",
        ]
        logger.info(f"Uploading {local_path} -> {obs_target}")
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=self.TIMEOUT_SECONDS
        )
        if result.returncode != 0 or "Upload successfully" not in result.stdout:
            raise RuntimeError(
                f"OBS upload failed for {local_path}: "
                f"{result.stdout[-2000:]}\n{result.stderr[-2000:]}"
            )
        logger.info(f"Upload complete: {obs_target}")
