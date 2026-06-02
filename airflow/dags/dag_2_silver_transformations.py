"""
Runs daily at 07:00 EAT (04:00 UTC) — 1 hour after DAG 1 starts.
Waits for DAG 1 to finish (ExternalTaskSensor), then runs the
silver dbt models: stg_rentals and int_rentals_enriched.
"""

from __future__ import annotations
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty  import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sensors.external_task import ExternalTaskSensor

from pipeline_config import (
    DEFAULT_ARGS,
    DOCKER_OPERATOR_KWARGS,
)

with DAG(
    dag_id="dag_2_silver",
    description="Silver layer: stg_rentals + int_rentals_enriched via dbt DockerOperator",
    default_args=DEFAULT_ARGS,
    schedule="0 4 * * *",   # 04:00 UTC = 07:00 EAT daily
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["nairobi", "silver", "dbt", "pipeline-2"],
) as dag:

    start = EmptyOperator(task_id="start")

    # Task 1: Wait for DAG 1 to complete 
    wait_for_dag1 = ExternalTaskSensor(
        task_id="wait_for_dag1_ingestion",
        external_dag_id="dag_1_ingestion",
        external_task_id="end",          
        execution_delta=timedelta(hours=1),  # Look back 1 hour to align with DAG 1's logical date
        timeout=7200,           
        poke_interval=60,             
        mode="reschedule",      # Optimization: frees up worker slots between pokes
        doc_md=(
            "Waits for dag_1_ingestion to complete. Checks every 60s, times out after 2h. "
            "Ensures silver transformations only run after new data is ingested and available in Redshift."
        ),
    )
   
    # Task 2: dbt run — silver models only 
    dbt_run_silver = DockerOperator(
        task_id="dbt_run_silver",
        command=[
            "dbt", "run",
            "--project-dir",  "/dbt",
            "--profiles-dir", "/dbt",
            "--target",       "prod",
            "--select", "staging", "intermediate",
        ],
        execution_timeout=timedelta(minutes=15),
        doc_md=(
            "Runs silver dbt models: stg_rentals (deduplication, type casting, "
            "price cleaning) and int_rentals_enriched (commute_tier, location_segment, "
            "is_sweet_spot, bedroom_label, price_vs_neighbourhood_median)."
        ),
        **DOCKER_OPERATOR_KWARGS,
    )

    # Task 3: dbt test — silver models
    dbt_test_silver = DockerOperator(
        task_id="dbt_test_silver",
        command=[
            "dbt", "test",
            "--project-dir",  "/dbt",
            "--profiles-dir", "/dbt",
            "--target",       "prod",
            "--select", "staging", "intermediate",
        ],
        execution_timeout=timedelta(minutes=10),
        doc_md=(
            "Runs schema tests on silver models: unique property_id, "
            "not_null constraints, accepted_values for value_zone, "
            "price range checks, bedroom range checks."
        ),
        **DOCKER_OPERATOR_KWARGS,
    )

    end = EmptyOperator(task_id="end")

    # Task chain
    start >> wait_for_dag1 >> dbt_run_silver >> dbt_test_silver >> end
