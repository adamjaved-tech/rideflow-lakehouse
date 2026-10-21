"""Airflow DAG: hourly RideFlow pipeline, one task per restartable stage.

Each task calls the same stage functions the CLI uses. Retries use exponential backoff;
a failed data-quality gate (DataQualityError) fails the task and blocks downstream loads.

Status: syntax/structure checked by tests/test_dag_definition.py only. The scheduler was
NOT started in the build environment (Airflow not installable offline) - see README.
Requires `RIDEFLOW_CONFIG` to point at config/pipeline.yaml (absolute path recommended).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from rideflow.config import load_config
from rideflow.pipeline import STAGES, run_stage
from rideflow.state import sanitize_run_id

log = logging.getLogger(__name__)


def _alert_on_failure(context):
    """Hook for Slack/e-mail/PagerDuty. Logs only; never include secrets here."""
    ti = context["task_instance"]
    log.error("RideFlow task failed: dag=%s task=%s run=%s", ti.dag_id, ti.task_id, context.get("run_id"))


def _run(stage: str, run_id: str, **_):
    run_stage(load_config(), stage, sanitize_run_id(run_id))


default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(minutes=20),
    "on_failure_callback": _alert_on_failure,
}

with DAG(
    dag_id="rideflow_lakehouse",
    description="Bronze -> silver -> warehouse with quality gates",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1),
    default_args=default_args,
    tags=["rideflow", "lakehouse"],
) as dag:
    previous = None
    for stage in STAGES:
        task = PythonOperator(
            task_id=stage,
            python_callable=_run,
            op_kwargs={"stage": stage, "run_id": "{{ run_id }}"},
        )
        if previous is not None:
            previous >> task
        previous = task
