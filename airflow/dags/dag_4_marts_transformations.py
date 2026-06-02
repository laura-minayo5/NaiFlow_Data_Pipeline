"""
Runs daily at 08:00 EAT (06:00 UTC) — 1 hour after DAG 3 Gold starts.
Waits for DAG 3 to finish (ExternalTaskSensor), then runs the
aggregated summary mart models via dbt DockerOperator.
"""

from __future__ import annotations
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sensors.external_task import ExternalTaskSensor

from pipeline_config import (
    DEFAULT_ARGS,
    DOCKER_OPERATOR_KWARGS,
)


with DAG(
    dag_id="dag_4_marts",
    description="Mart layer: High-level aggregated summary tables and metrics for reporting visuals",
    default_args=DEFAULT_ARGS,
    schedule="0 6 * * *",   # 06:00 UTC = 09:00 EAT daily (1 hour after DAG 3 Gold)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["nairobi", "marts", "dbt", "pipeline-4"],
) as dag:

    start = EmptyOperator(task_id="start")

    # Task 1: Sensor Gateway
    # Blocks execution until the Gold table builds and assertions pass successfully.
    wait_for_gold = ExternalTaskSensor(
        task_id="wait_for_gold_layer",
        external_dag_id="dag_3_gold",     # Points to your Gold layer DAG ID
        external_task_id="dbt_test_gold", # Explicitly waits for the tests to pass before aggregating
        allowed_states=['success'],
        execution_delta=timedelta(hours=1), # Looks back 1 hour to match DAG 3's 05:00 UTC execution timestamp
        timeout=3600,           
        poke_interval=60,             
        mode="reschedule",         # Frees up worker capacity during the polling loop
        doc_md="Waits for dag_3_gold's test suite to pass before generating analytical summaries.",
    )

    # Task 2: dbt Run Marts
    # Compiles your 4 specialized analytical summary models under the mart directory.
    dbt_run_marts = DockerOperator(
        task_id="dbt_run_marts",
        command=[
            "dbt", "run",
            "--project-dir",  "/dbt",
            "--profiles-dir", "/dbt",
            "--target",       "prod",
            "--select",       "marts",
            "--fail-fast",
        ],
        execution_timeout=timedelta(minutes=15),
        doc_md=(
            "Runs all dbt models inside the marts directory. "
            "Computes macro metrics like neighborhood counts, historical rent trend deltas, "
            "and affordability index rankings for direct Power BI exposure."
        ),
        **DOCKER_OPERATOR_KWARGS,
    )

    # Task 3: dbt Test Marts
    # Executes your custom singular tests (like assert_mart_neighbourhood_stats__rank_1_matches_global_benchmarks.sql).
    dbt_test_marts = DockerOperator(
        task_id="dbt_test_marts",
        command=[
            "dbt", "test",
            "--project-dir",  "/dbt",
            "--profiles-dir", "/dbt",
            "--target",       "prod",
            "--select",       "marts",
        ],
        execution_timeout=timedelta(minutes=10),
        doc_md="Runs unique constraints and complex unified ranking/benchmark validation scripts.",
        **DOCKER_OPERATOR_KWARGS,
    )

    end = EmptyOperator(task_id="end")

    # Executable Chain Flow
    start >> wait_for_gold >> dbt_run_marts >> dbt_test_marts >> end
