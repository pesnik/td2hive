# td2hive

A Teradata → Hive (MRS / HetuEngine) backup pipeline: Teradata Parallel
Transporter (TPT) extraction, [DataX](https://github.com/alibaba/DataX)
(stock plugins only, no custom Java) loading, independent verification,
and pluggable audit.

## Why this exists

A common, easy-to-miss failure mode: writing into a dynamically-partitioned
Hive table via `INSERT ... PARTITION(dynamic_col)` can report success in
seconds for hundreds of millions of rows — physically impossible — while
writing zero real rows, with no error surfaced. The write path silently
depends on HDFS/object-store permission and partition-discovery behavior
that a component's own self-report doesn't reveal.

td2hive avoids that failure mode structurally, not by adding more error
handling to the same mechanism:

- **Extraction**: [Teradata Parallel Transporter](https://github.com/Teradata/PT)
  (wraps FastExport), the same idiom TDCH and Sqoop use for Teradata at
  volume — not generic JDBC.
- **Loading**: DataX's stock `hdfswriter` writes straight to your
  target's OBS/HDFS partition path via the Hadoop `FileSystem` API,
  **bypassing Hive's query engine for the write entirely**. No custom
  DataX plugin — `txtfilereader` (reads TPT's local CSV output) paired
  with `hdfswriter` is the whole loading mechanism.
- **Partition registration**: td2hive queries every distinct dynamic
  partition value up front (`SELECT DISTINCT`) and exports each value's
  data separately — TPT does the partitioning itself, scoped by a `WHERE`
  clause per value, rather than exporting the whole table once and
  splitting it apart locally afterward. Since every value is known in
  advance, it registers each partition explicitly via `ALTER TABLE ...
  ADD PARTITION` — never `MSCK REPAIR`'s directory-tree auto-discovery,
  which in practice we found unreliable against at least one real OBS
  backend.
- **Parallelism**: TPT's own `DataConnector` operator (`FILE_WRITER[n]`
  + `tbuild -C`) writes each export directly into `n` files, round-robin
  distributed, in one pass — no local re-read/re-split pass over data
  that's already on disk just to fan it out across DataX channels.
- **Verification**: an independent Teradata-source-count vs.
  Hive-target-count comparison is the *only* thing that ever sets
  success/failure. A loader's own self-reported row count is stored for
  telemetry but never trusted as the verdict — that's the direct lesson
  of the bug above.
- **Audit**: pluggable sinks (JSONL file, any SQLAlchemy-supported
  database, OpenLineage) fan out via a `CompositeAuditSink` — a package
  can't assume what audit/lineage destination you already run.

## Architecture

```
per distinct dynamic       (no-op, one pass: tables with no dynamic
partition value:            partition column)
        │
        ▼
TPTExporter.export()        Teradata table, WHERE-scoped to one partition
        │                    value -> n local CSVs in parallel
        │                    (FILE_WRITER[n] + tbuild -C, round-robin)
        ▼
datax.job_spec.build_job_json()  generates DataX's job.json:
        │                        txtfilereader -> hdfswriter
        │                        (channel count = file count = real
        │                         parallelism, no local re-split pass)
        ▼
DataxRunner.run()            runs `datax.py`, parses its summary
        │                    (fast-fail signal only, not the verdict)
        ▼
PartitionRegistrar.add_partition()   explicit per-partition registration
        │
        ▼
verify()                     independent Teradata vs. Hive count compare
        │                    (the only thing that sets success/failure)
        ▼
AuditSink.record()           JSONL / SQL / OpenLineage, pluggable
```

## Installation

```bash
pip install td2hive
# or, for a specific audit backend:
pip install "td2hive[mysql]"       # SQLAuditSink against MySQL
pip install "td2hive[postgres]"    # SQLAuditSink against Postgres
pip install "td2hive[openlineage]" # OpenLineageAuditSink
```

You'll also need, on whatever host runs `td2hive run`:

- **Docker**, with a `teradata/tpt` image available (Teradata's licensing
  means you build/obtain this image yourself).
- **A DataX distribution** containing at minimum the `writer/hdfswriter`
  and `reader/txtfilereader` plugins. Build one:

  ```bash
  # Requires Java 8 (DataX's own build doesn't work on newer JDKs) and Maven.
  scripts/build_datax_dist.sh 3.3.6   # <- your cluster's Hadoop version
  ```

  This builds DataX from source at the commit pinned in
  `vendor/DATAX_PINNED_COMMIT` (with the one patch DataX's own assembly
  needs, `vendor/datax-patches/`), then resolves your Hadoop version's
  client jars via Maven Central — replacing DataX's bundled Hadoop 2.7.1
  jars, which throw `NoSuchMethodError`/`NoClassDefFoundError` against
  most modern (3.x) clusters — and removes every jar the new set
  supersedes. It finishes with a credential-free smoke test (writes
  locally via `file://`, no cloud credentials needed) before declaring
  success. Output lands at `build/datax-dist/datax`, ready for
  `scripts/package_datax_dist.sh` / `scripts/deploy_datax_dist.sh`.

  **Not resolved automatically:** your cloud/vendor object-store
  connector jar (Huawei's `hadoop-huaweicloud`, AWS's `hadoop-aws`,
  GCS's `gcs-connector`, etc.) — some vendors don't publish to public
  Maven Central at all, so this can't be one-size-fits-all. If yours is
  on Maven Central, pass its coordinates to `build_datax_dist.sh` /
  `scripts/resolve_hadoop_jars.sh` as extra arguments (see that script's
  header for the exact syntax); otherwise place the jar in
  `build/datax-dist/datax/plugin/writer/hdfswriter/libs` yourself after
  building. Then run `scripts/smoke_test_datax_dist.py` against your
  *real* object storage before trusting the distribution with real data
  — the build script's own smoke test only proves the Hadoop-jar swap
  didn't break the classpath, not that your connector jar works.
- **`beeline`**, for partition registration and verification queries.

## Quick start

1. Write a job spec (see `td2hive/jobs/example_dim_table.yaml` and
   `example_fact_table.yaml` for statically- and dynamically-partitioned
   examples):

   ```yaml
   table_name: my_table
   source:
     teradata:
       owner: STAGING_DB
       load_tables: [MY_TABLE_STG]
       columns: [ID, NAME, UPDATED_AT]
   target:
     hive:
       owner: WAREHOUSE_DB
       table: MY_TABLE
     obs_dir: /warehouse.db/my_table
     format: parquet
     partitions: []
   loader: datax
   retention_days: 30   # omit to never expire this table's data
   ```

2. Run it:

   ```bash
   td2hive run \
     --job jobs/my_table.yaml \
     --processing-date 2026-01-15 \
     --td-host ... --td-user ... --td-password ... \
     --obs-access-key ... --obs-secret-key ... --obs-endpoint ... --obs-bucket ... \
     --datax-home /path/to/your/datax/distribution
   ```

   Or run every DataX-loader job under a directory:

   ```bash
   td2hive run-all --jobs-dir jobs/ --processing-date 2026-01-15 ...
   ```

   Every option above can also come from an environment variable of the
   same name (`TD_HOST`, `OBS_ACCESS_KEY`, `DATAX_HOME`, ...) — see
   `td2hive/cli.py`.

3. Check the audit trail (`--audit-jsonl`, default
   `/data01/td2hive/logs/audit.jsonl`) or your configured SQL sink for
   the result. `status` is `success` or `dq_mismatch`, decided entirely
   by the independent count comparison in `verify.py`.

The example above is hand-written for a table you already understand.
Onboarding a table from an existing legacy config table - deriving the
real column order, proving it against real data, and picking a safe
`speed_channel` before it ever touches `jobs/` - is its own repeatable
sequence: see [`docs/onboarding.md`](docs/onboarding.md).

## Scaling: one job across many workers

`td2hive run` does everything sequentially in one process/container -
fine for most tables, but a table with hundreds of dynamic partition
values benefits from spreading the work out. Two separate mechanisms,
solving two separate costs:

- **DataX JVM cold-start** (real, measured cost per invocation) can be
  reduced by `run()` itself: `job.setting.max_channels_per_job` groups
  multiple partition values' writes into fewer `job.json` calls (shared
  JVM/channel pool) instead of one JVM launch per partition value.
  **Opt-in, not automatic**: it defaults to `speed_channel` itself, which
  means no batching at all unless a table's YAML sets it explicitly
  higher. This was a real production near-miss, not caution for its own
  sake — a flat default (64) doesn't account for memory scaling with
  channel count. A wide/heavy table already running close to its memory
  ceiling at a single partition value's channel count (confirmed: 16
  channels at ~29GB/32GB, 91% of cap) would have silently had its *entire
  job* - every load table, every partition value - combined into one
  untested, oversized JVM under a flat default. Batching's real value is
  for tables with **many small partitions**, not few wide ones - set
  `max_channels_per_job` explicitly per table only once you've confirmed
  the memory headroom for that table's row width, never rely on a shared
  default to guess right for every table.
- **Distributing units across separate processes/containers** (k8s Job
  parallelism, Airflow mapped tasks, Argo Workflow steps) uses the lower-
  level primitives `run()` is itself built from:

  ```bash
  td2hive plan --job jobs/my_table.yaml --processing-date 2026-01-15 ...
  #   -> one JSON unit per line to stdout

  td2hive prepare --job jobs/my_table.yaml --processing-date 2026-01-15 ...
  #   -> clears every unit's target path ONCE, before any unit writes -
  #      must run exactly once, ahead of fan-out (see JobRunner.prepare's
  #      docstring for why this can't be pushed into each unit's own run)

  # then, once per unit (in parallel, across as many workers as you like):
  td2hive run-unit --job jobs/my_table.yaml --processing-date 2026-01-15 \
    --unit '<one line from plan>' --result-file /shared/results/unit-N.json

  # once every run-unit has completed:
  td2hive reconcile --job jobs/my_table.yaml --processing-date 2026-01-15 \
    --results-dir /shared/results
  #   -> the ONLY step that decides success/dq_mismatch, same as run()
  ```

  `plan`/`reconcile` never touch Docker or DataX - only `run-unit` does,
  so only that step needs the full runtime (TPT client + DataX
  distribution). See `docker-compose.yml` (single host, no orchestrator -
  the easy win: multiple *different* jobs running concurrently),
  `k8s/` (indexed Job fan-out for *one* job's units, with a glue script
  since plain k8s Jobs have no native sequencing between separate Job
  objects), and `integrations/` (Airflow dynamic task mapping, Argo
  Workflows `withParam` - Argo's model maps almost exactly onto
  plan/run-unit/reconcile).

  For Kubernetes specifically: the default image's `TPTExporter` spawns a
  sibling `teradata/tpt` container via the Docker CLI, which needs a
  Docker daemon most managed clusters don't expose to pods for good
  security reasons. `docker/Dockerfile.td2hive-tpt` builds `FROM
  teradata/tpt` instead, so `tbuild` runs as a plain subprocess (auto-
  detected via `shutil.which` in `reader.py`) with no Docker-in-Docker
  dependency at all - use that image for k8s, the default one for
  Compose/bare-host use where a Docker daemon is already right there.

## Resumability

`td2hive run` self-heals across transient failures instead of restarting
a job from zero. A per-`(job, processing_date)` manifest tracks each
unit's real progress - `exported` (TPT done, local CSV paths recorded)
or `written` (DataX write + partition registration done) - so a bare
re-run after a failure:

- skips any unit already `written` entirely (no re-export, no re-write,
  and its target path is never re-cleared - it already holds good,
  verified data);
- for a unit that exported successfully but whose DataX write failed,
  validates its CSVs are still present and non-empty, then retries only
  the write, not the export.

This is a real, not hypothetical, need: two concurrent production jobs
once failed mid-DataX-write from local disk exhaustion, after their TPT
exports had already fully succeeded - without this, a retry would have
re-exported everything from scratch. `--force` skips loading prior
manifest state and starts from a clean slate.

Storage is pluggable via `ManifestStore` (`td2hive/run_manifest.py`),
mirroring the audit sink design below - `JSONLManifestStore` is the
zero-config default, one small append-only file per `(job,
processing_date)` run (`<datax-logs-dir>/<job>/<date>/manifest.jsonl`),
not one shared growing log for every table and every day this pipeline
has ever run - find what happened for one run by opening its one file,
and the file is deleted once that run reaches `success` (past that point
it's permanently useless, matching how local TPT-exported CSVs are
already treated: kept on failure for debugging, cleaned up once
independently verified). A SQL-backed store for centralized visibility
across many tables' in-flight runs is a natural future addition, not
built until there's real demand for it.

Deliberately scoped to `run()`'s sequential path only - `run-unit` used
via a real orchestrator (k8s/Airflow/Argo) already gets equivalent
retry-without-redoing-everything behavior for free from the
orchestrator's own per-task retry, since each `run-unit` call is already
atomic to one partition value.

## Concurrency on a single host

Running many tables off cron on one host has no built-in limit on how
many DataX JVMs run at once - fine for a handful of tables, a real risk
once dozens of tables' schedules start overlapping (each JVM can request
a large heap; on a host with real memory headroom this is still worth
bounding deliberately rather than trusting it never happens).
`scripts/run_guarded.sh` wraps any command in a fixed pool of
`flock`-based slots:

```bash
scripts/run_guarded.sh /path/to/python -m td2hive.cli run \
  --job jobs/my_table.yaml --processing-date 2026-01-15 ...
```

An invocation past the slot pool **blocks** (polling every
`TD2HIVE_LOCK_POLL_INTERVAL` seconds) until a slot frees, rather than
failing or running unbounded - a cron job that fires while every slot is
busy queues up and runs later. Configure via `TD2HIVE_CONCURRENCY_SLOTS`
(default 6), `TD2HIVE_LOCK_DIR` (default `/var/lock/td2hive`). Point
your cron entries at this wrapper instead of calling `td2hive`/`python
-m td2hive.cli` directly.

If you're on Kubernetes/Compose instead of cron, this isn't needed -
your orchestrator's own resource requests/limits (or Compose's own
container scheduling) already bound concurrency; see **Scaling** above.

## Retention

```bash
td2hive retention run --job jobs/my_table.yaml --obs-bucket ...       # dry-run (default)
td2hive retention run --job jobs/my_table.yaml --obs-bucket ... --no-dry-run   # actually delete
td2hive retention run-all --jobs-dir jobs/ --obs-bucket ...
```

Only tables with `retention_days` set in their job spec are touched.
Every expiry action is dry-run by default — `--no-dry-run` must be passed
explicitly to delete anything. OBS data is deleted first, then the Hive
partition entry is dropped; if the OBS deletion fails, the Hive partition
is deliberately left pointing at the now-partially-deleted data rather
than silently orphaning metadata that still claims complete data exists.

## Deployment

`scripts/` has setup, promotion-gated deployment, rollback, and teardown
for both the Python package and the DataX distribution:

```bash
scripts/deploy_app.sh <version> <ssh-host>          # ships td2hive itself
scripts/package_datax_dist.sh <version> <datax-dist-dir>
scripts/deploy_datax_dist.sh <version> <ssh-host>    # ships + smoke-tests DataX
scripts/rollback.sh <app|datax> <version> <ssh-host> # instant symlink repoint
scripts/teardown.sh <app|datax> <version> <ssh-host> # remove an old version
```

`deploy_datax_dist.sh` gates promotion on `scripts/smoke_test_datax_dist.py`
actually writing to real object storage and independently verifying the
result — a distribution that fails the smoke test is never symlinked to
`current`.

Both deploy scripts assume `python3` on the target host's `PATH` is
already set up with td2hive's dependencies; set `TD2HIVE_PYTHON` if it's
somewhere else (e.g. a venv). Set `TD2HIVE_HOST` instead of passing
`ssh-host` on every command if you're deploying to one host repeatedly.

## Design principles

- **No hardcoded schema mapping.** Column types are always resolved
  dynamically against `DBC.ColumnsV` at run time (`column_types.py`) -
  never a maintained name→type table. This is what makes the package
  usable against any Teradata schema.
- **Never trust a component's own self-report as the verdict.** DataX's
  summary, a Hive `INSERT`'s reported row count, an S3-compatible
  client's "success" - none of these are trusted as ground truth
  anywhere in this package. Every claim of "this data landed" is checked
  independently before anything is marked successful.
- **Storage and metadata are always two explicit, separately-verifiable
  steps.** Writing data and registering a Hive partition are never one
  operation that's assumed to imply the other.
- **Re-running a job for the same date is safe by construction.** Each
  target partition path is deleted before it's written to (once per
  unique path per run, not per source table, so multiple load tables can
  still share a partition directory within one run) — `hdfswriter`'s own
  `writeMode` stays `append` precisely so nothing else also tries to
  manage conflicts in the same directory. Local TPT-exported CSVs are
  only ever deleted after `verify()` independently confirms a successful
  write — kept on failure/mismatch for debugging.
- **No assumptions about your audit/lineage destination.** Pluggable
  sinks (JSONL, any SQLAlchemy database, OpenLineage), composable, not
  exclusive.

## License

MIT - see [LICENSE](LICENSE).
