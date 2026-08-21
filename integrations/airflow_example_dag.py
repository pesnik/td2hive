"""Example Airflow DAG fanning out one job's units across dynamically
mapped tasks, using Airflow 2.3+'s dynamic task mapping (`.expand()`)
over `plan()`'s output - each mapped task instance is one `run-unit`
call, run in its own pod via KubernetesPodOperator.

Real insight this DAG relies on: `plan()` and `reconcile()` are pure
Python/Teradata/Hive calls - they never touch Docker or DataX (see
JobRunner.runner's lazy property in td2hive/job_runner.py) - so they run
as plain Airflow tasks on the worker itself (assuming the worker has
`pip install td2hive` and network access to Teradata/beeline), no pod
needed. Only `run_unit()` needs the td2hive-tpt image and a DataX
distribution, so only that step is a KubernetesPodOperator.

If your Airflow deployment isn't on Kubernetes, swap KubernetesPodOperator
for DockerOperator (same idea - one container per mapped task instance,
running `td2hive run-unit`) or BashOperator if td2hive's already
installed directly on the worker/a remote host over SSH (the same
subprocess pattern this pipeline's own production deployment already
uses for beeline/TPT).

Install: pip install "apache-airflow-providers-cncf-kubernetes" and
`pip install td2hive` on whatever runs plan()/reconcile() (the Airflow
worker, or swap those tasks for KubernetesPodOperator too if you'd
rather keep the worker itself dependency-free).
"""
from __future__ import annotations

import json
from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

JOB_YAML = "/app/jobs/example_fact_table.yaml"
DATAX_HOME = "/data01/td2hive/datax/current"
TD2HIVE_TPT_IMAGE = "td2hive-tpt:latest"
NAMESPACE = "default"


@dag(schedule="@daily", start_date=datetime(2026, 1, 1), catchup=False)
def td2hive_example_fact_table():

    @task
    def plan(logical_date=None) -> list[str]:
        """Runs in-process on the Airflow worker - plan() only queries
        Teradata, no container needed. Returns one JSON string per unit,
        which .expand() below fans out into one mapped task instance
        each."""
        import teradatasql
        from datetime import date

        from td2hive.job_runner import JobRunner, RunPaths
        from td2hive.jobspec import load_jobspec
        from td2hive.reader import ObsConfig
        from airflow.hooks.base import BaseHook

        td_conn = BaseHook.get_connection("td2hive_teradata")
        obs_conn = BaseHook.get_connection("td2hive_obs")

        job = load_jobspec(JOB_YAML)
        cursor = teradatasql.connect(
            host=td_conn.host, user=td_conn.login, password=td_conn.password
        ).cursor()
        runner = JobRunner(
            td_cursor=cursor,
            td_host=td_conn.host, td_user=td_conn.login, td_password=td_conn.password,
            obs_config=ObsConfig(
                access_key=obs_conn.login, secret_key=obs_conn.password,
                endpoint=obs_conn.host,
            ),
            obs_bucket=obs_conn.schema,
            audit_sink=None,  # unused - plan() never touches the audit sink
            paths=RunPaths(tpt_output_dir="/tmp/unused", datax_logs_dir="/tmp/unused"),
        )
        processing_date = logical_date.date() if logical_date else date.today()
        units = runner.plan(job, processing_date)
        runner.prepare(units)  # must run once, before any run-unit - see its docstring
        return [json.dumps(u.to_dict()) for u in units]

    run_unit = KubernetesPodOperator.partial(
        task_id="run_unit",
        namespace=NAMESPACE,
        image=TD2HIVE_TPT_IMAGE,
        cmds=["td2hive"],
        # {{ ti.xcom_pull(task_ids='plan')[map_index] }} isn't needed -
        # .expand()'s mapped arguments are injected as Jinja-free plain
        # values into `arguments` per mapped instance automatically.
        env_vars={"DATAX_HOME": DATAX_HOME},
        secrets=[],  # wire up TD_*/OBS_* via a real Secret/Connection in your own deployment
        is_delete_operator_pod=True,
        get_logs=True,
    ).expand(
        arguments=plan().map(
            lambda unit_json: [
                "run-unit",
                f"--job={JOB_YAML}",
                "--processing-date={{ ds }}",
                f"--unit={unit_json}",
                "--result-file=/plan/results/unit.json",  # mount a shared volume per your cluster's storage
            ]
        )
    )

    @task(trigger_rule="all_done")  # run even if some mapped run_unit tasks failed - reconcile()'s
                                     # verify() is the real, honest signal either way
    def reconcile(logical_date=None):
        """Also runs in-process on the worker - reconcile() never
        touches Docker/DataX either, same as plan(). Reads back every
        run-unit's result file from wherever run_unit wrote it (a shared
        volume, or push each result to XCom instead and read those back
        here, depending on your cluster's storage options)."""
        import teradatasql
        from datetime import date
        from pathlib import Path

        from td2hive.audit import CompositeAuditSink
        from td2hive.audit.jsonl_sink import JSONLFileAuditSink
        from td2hive.job_runner import JobRunner, RunPaths, UnitResult
        from td2hive.jobspec import load_jobspec
        from td2hive.reader import ObsConfig
        from airflow.hooks.base import BaseHook

        td_conn = BaseHook.get_connection("td2hive_teradata")
        obs_conn = BaseHook.get_connection("td2hive_obs")

        job = load_jobspec(JOB_YAML)
        cursor = teradatasql.connect(
            host=td_conn.host, user=td_conn.login, password=td_conn.password
        ).cursor()
        runner = JobRunner(
            td_cursor=cursor,
            td_host=td_conn.host, td_user=td_conn.login, td_password=td_conn.password,
            obs_config=ObsConfig(
                access_key=obs_conn.login, secret_key=obs_conn.password,
                endpoint=obs_conn.host,
            ),
            obs_bucket=obs_conn.schema,
            audit_sink=CompositeAuditSink([JSONLFileAuditSink(Path("/data01/td2hive/logs/audit.jsonl"))]),
            paths=RunPaths(tpt_output_dir="/tmp/unused", datax_logs_dir="/tmp/unused"),
        )
        results_dir = Path("/plan/results")
        unit_results = [UnitResult.from_dict(json.loads(f.read_text())) for f in results_dir.glob("*.json")]
        processing_date = logical_date.date() if logical_date else date.today()
        record = runner.reconcile(job, processing_date, unit_results)
        if record.status != "success":
            raise RuntimeError(f"dq_mismatch: source={record.source_row_count} target={record.target_row_count}")

    reconcile.set_upstream(run_unit)


td2hive_example_fact_table()
