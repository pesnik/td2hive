#!/usr/bin/env python3
"""Fires an alert for any run that didn't succeed - closes a real gap:
before this, a failed *load* for any table was only ever visible via
manual audit.jsonl/SQL inspection, while a failed *retention* run
already alerted via data_retention.py's own bolted-on SMSNotifier,
entirely outside this package's pluggable-sink design. Generalizes that
same send mechanism into a proper AuditSink instead of reinventing it.

Gateway URL/credentials are passed in via AlertingConfig, never
hardcoded - this package has no built-in gateway, same posture as
ObsConfig/every other credential-bearing config in this package.
"""

import time
from dataclasses import dataclass
from typing import List

from loguru import logger

from . import AuditRecord


@dataclass
class AlertingConfig:
    gateway_url: str
    username: str
    password: str
    from_number: str
    path: str = "cgi-bin/sendsms"
    timeout: int = 30
    max_retries: int = 1
    retry_delay: int = 1
    verify_ssl: bool = False


class AlertingAuditSink:
    """Write-only: find_success always raises NotImplementedError, so
    CompositeAuditSink's own init-time check refuses to accept this as
    the *only* configured sink - it must always sit alongside a real
    lookup-capable sink (JSONLFileAuditSink/SQLAuditSink)."""

    supports_lookup = False

    def __init__(self, config: AlertingConfig, recipients: List[str]):
        if not recipients:
            raise ValueError("AlertingAuditSink requires at least one recipient")
        self.config = config
        self.recipients = recipients
        self._base_url = f"{config.gateway_url.rstrip('/')}/{config.path.lstrip('/')}"

    def record(self, run: AuditRecord) -> None:
        if run.status == "success":
            return
        message = (
            f"{run.status.upper()}: {run.job_name}/{run.processing_date} "
            f"(source={run.source_row_count} target={run.target_row_count})"
        )
        if run.error_detail:
            message += f" - {run.error_detail[:100]}"
        for recipient in self.recipients:
            self._send(recipient, message)

    def find_success(self, job_name: str, processing_date: str) -> bool:
        raise NotImplementedError(
            "AlertingAuditSink is write-only - never used for idempotency lookups"
        )

    def _send(self, recipient: str, message: str) -> bool:
        import requests

        params = {
            "username": self.config.username,
            "password": self.config.password,
            "from": self.config.from_number,
            "to": recipient,
            "text": message,
        }
        for attempt in range(self.config.max_retries):
            try:
                response = requests.get(
                    self._base_url,
                    params=params,
                    timeout=self.config.timeout,
                    verify=self.config.verify_ssl,
                )
                if response.status_code < 300:
                    logger.info(f"Alert sent to {recipient}")
                    return True
                logger.warning(
                    f"Alert to {recipient} failed with status {response.status_code}: {response.text}"
                )
            except Exception as e:
                logger.error(f"Alert attempt {attempt + 1} to {recipient} failed: {e}")
                if attempt < self.config.max_retries - 1:
                    time.sleep(self.config.retry_delay)

        logger.error(f"Failed to send alert to {recipient} after {self.config.max_retries} attempts")
        return False
