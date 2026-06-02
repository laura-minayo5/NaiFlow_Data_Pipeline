from __future__ import annotations
from datetime import datetime, timedelta

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator 
from pipeline_config import (
    DEFAULT_ARGS,
    DOCKER_OPERATOR_KWARGS, 
)


with DAG(
    dag_id='dag_3_gold',
    default_args=DEFAULT_ARGS,
    schedule='0 5 * * *',  # FIX: Runs daily at 05:00 UTC (08:00 EAT) — exactly 1 hour after Silver DAG to ensure data freshness while allowing Silver transformations to complete
    start_date=datetime(2026, 1, 1),
    catchup=False
) as dag:

    # Start
    start = EmptyOperator(task_id="start")


    # Task 1: Sensor Gateway: Waiting for the Silver DAG to finish
    wait_for_silver = ExternalTaskSensor(
        task_id='wait_for_silver_layer',
        external_dag_id='dag_2_silver',                  # The ID of your previous DAG
        external_task_id='dbt_run_silver',               # The specific task to wait for
        allowed_states=['success'],
        execution_delta=timedelta(hours=1),             # Gold runs at 05:00, looks back 1 hour to Silver's 04:00 run
        timeout=3600,                                   # Give up after 1 hour
        mode='reschedule'                               # Optimization: 'reschedule' drops the worker lock between pokes to preserve system resources
    )

    
    # Task 2: Freshness Monitor
    # Data freshness is a business Service Level Agreement (SLA)—it tells you if your dashboards are lagging behind real life.
    # This task runs in parallel with the dbt transformations to monitor source freshness.
    # It checks the freshness of the bronze.raw_rentals source table and will warn or error if the data is stale, alerting you to upstream issues before the gold transformations run.
    # This ensures your business users still have a functional, running dashboard with yesterday's data instead of a completely blank report.
    # Defined in models/staging/sources.yml with freshness criteria:
    #   warn_after: {count: 25, period: hour}
    #   error_after: {count: 49, period: hour}
    dbt_source_freshness = DockerOperator(
        task_id  = "dbt_source_freshness",
        command  = [
            "dbt", "source", "freshness",
            "--project-dir",  "/dbt",
            "--profiles-dir", "/dbt",
            "--target",       "prod",
        ],
        execution_timeout = timedelta(minutes=5),
        doc_md = (
            "Runs dbt source freshness against bronze.raw_rentals. "
            "Warns if no data in 25h, errors at 49h. "
            "Alerts you if the Firehose or scraper has stopped delivering data."
        ),
        **DOCKER_OPERATOR_KWARGS,
    )

    # Task 3: Build Gold Materializations
    # dbt run — gold models
    # --select gold runs all models in the gold/ folder:
    # dim_neighbourhood
    # dim_date
    # fact_rental_listings
    # dbt resolves the dependency order automatically based on the ref() calls in the SQL files.

    dbt_run_gold = DockerOperator(
        task_id  = "dbt_run_gold",
        command  = [
            "dbt", "run",
            "--project-dir",  "/dbt",
            "--profiles-dir", "/dbt",
            "--target",       "prod",
            "--select",       "gold",
            "--fail-fast",
        ],
        execution_timeout = timedelta(minutes=20),
        doc_md = (
            "Runs all Gold models. "
            "dim_neighbourhood: one row per neighbourhood. "
            "dim_date: one row per date. "
            "fact_rental_listings: one row per rental listing, per day, with foreign keys to the dimension tables. "
        ),
        **DOCKER_OPERATOR_KWARGS,

    )

    # Task 4: dbt test — gold models
    # Runs schema tests from models/gold/schema.yml:
    # unique + not_null on neighbourhood, property_id
    # accepted_range on prices, distances, percentages
    # accepted_values on value_zone, verdict
    # Also runs singular tests from tests/gold/:
    # assert_gold_layer_no_join_leakage.sql
    dbt_test_gold = DockerOperator(
        task_id  = "dbt_test_gold",
        command  = [
            "dbt", "test",
            "--project-dir",  "/dbt",
            "--profiles-dir", "/dbt",
            "--target",       "prod",
            "--select",       "gold",
        ],
        execution_timeout = timedelta(minutes=10),
        doc_md = (
            "Tests all Gold models. Fails the DAG if any data quality "
            "check fails — e.g. duplicate neighbourhood rows, negative prices, "
            "empty gold tables, or invalid value_zone values."
        ),
        **DOCKER_OPERATOR_KWARGS,
    )

    # End
    end = EmptyOperator(task_id="end")

    # Task chain flow
    # 1. Pipeline initializes and triggers the sensor gateway
    start >> wait_for_silver
    
    # 2. Once the sensor passes, run the source freshness check and the gold builds in parallel paths
    wait_for_silver >> dbt_source_freshness >> end
    wait_for_silver >> dbt_run_gold >> dbt_test_gold >> end

  

