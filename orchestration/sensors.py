"""Alerting: POST to a webhook whenever any run fails.

Set ALERT_WEBHOOK_URL to a Slack/Discord/Telegram-bridge incoming webhook.
The payload uses Slack's `text` convention, which most webhook receivers
accept. When the variable is unset the sensor logs and does nothing, so local
dev needs no configuration.
"""

import os

import requests
from dagster import DefaultSensorStatus, RunFailureSensorContext, run_failure_sensor


@run_failure_sensor(default_status=DefaultSensorStatus.RUNNING, minimum_interval_seconds=60)
def pipeline_failure_webhook(context: RunFailureSensorContext) -> None:
    url = os.getenv("ALERT_WEBHOOK_URL")
    if not url:
        context.log.info("ALERT_WEBHOOK_URL not set; skipping failure alert")
        return

    message = (
        f":rotating_light: Saudi Data Pulse run failed\n"
        f"*Job:* {context.dagster_run.job_name}\n"
        f"*Run ID:* {context.dagster_run.run_id}\n"
        f"*Error:* {context.failure_event.message}"
    )
    response = requests.post(url, json={"text": message}, timeout=10)
    response.raise_for_status()
    context.log.info("Failure alert sent for run %s", context.dagster_run.run_id)
