#!/usr/bin/env python3
"""Emits standard OpenLineage RunEvents - the idiomatic OSS answer for
audit/lineage. Plugs into whatever governance tooling an adopter already
runs (Marquez, DataHub, Atlan, anything OpenLineage-compatible) instead of
inventing a bespoke schema they'd have to integrate against.

Write-only by nature (OpenLineage has no query API of its own) - idempotency
checks always route through JSONLFileAuditSink or SQLAuditSink instead; see
CompositeAuditSink's find_success. The openlineage-client dependency is
imported lazily so it's only required when this sink is actually configured.
"""

from datetime import datetime, timezone

from . import AuditRecord


class OpenLineageAuditSink:
    def __init__(self, transport_url: str, namespace: str = "td2hive"):
        from openlineage.client import OpenLineageClient
        from openlineage.client.transport.http import HttpConfig, HttpTransport

        self.namespace = namespace
        self.client = OpenLineageClient(transport=HttpTransport(HttpConfig(url=transport_url)))

    def record(self, run: AuditRecord) -> None:
        from openlineage.client.event_v2 import Dataset, Job, Run, RunEvent, RunState
        from openlineage.client.facet_v2 import error_message_run

        state = RunState.COMPLETE if run.status == "success" else RunState.FAIL
        facets = {}
        if run.error_detail:
            facets["errorMessage"] = error_message_run.ErrorMessageRunFacet(
                message=run.error_detail, programmingLanguage="python"
            )

        event = RunEvent(
            eventType=state,
            eventTime=run.end_time.replace(tzinfo=timezone.utc).isoformat(),
            run=Run(runId=run.run_id, facets=facets),
            job=Job(namespace=self.namespace, name=run.job_name),
            inputs=[
                Dataset(namespace="teradata", name=f"{run.source_schema}.{run.source_table}")
            ],
            outputs=[
                Dataset(namespace="hive", name=f"{run.hive_schema}.{run.hive_table}")
            ],
            producer="https://github.com/td2hive",
        )
        self.client.emit(event)

    def find_success(self, job_name: str, processing_date: str) -> bool:
        raise NotImplementedError("OpenLineageAuditSink is write-only")
