"""
airflow/dags/dag_1_ingestion.py
================================
DAG 1 — Ingestion Pipeline: Property24 -> Kinesis -> S3 -> Redshift
================================
"""

from __future__ import annotations
import json
import logging
from datetime import datetime, timedelta, timezone
import psycopg2
import boto3

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowFailException
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

from pipeline_config import (
    DEFAULT_ARGS,
    CLIENT_SCRIPT_PATH,
    FIREHOSE_WAIT_SECS,
    S3_STAGING_BUCKET,
    S3_STAGING_PREFIX,
    S3_TO_REDSHIFT_LAMBDA,
    GLUE_ETL_JOB_NAME,
    REDSHIFT_HOST,
    REDSHIFT_PORT,
    REDSHIFT_DB,
    REDSHIFT_USER,
    REDSHIFT_PASSWORD,
    AWS_REGION,
)

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Python Task Callbacks (Clean, Readable, Traceable)
# ---------------------------------------------------------------------------

def trigger_copy_lambda_callable():
    """Builds the current hour's S3 path and triggers the Redshift COPY Lambda."""
    now = datetime.now(timezone.utc)
    folder = f"{S3_STAGING_PREFIX}/{now.strftime('%Y/%m/%d/%H')}/"
    s3_folder_path = f"s3://{S3_STAGING_BUCKET}/{folder}"
    
    log.info(f"Triggering COPY Lambda for S3 folder path: {s3_folder_path}")
    
    payload = json.dumps({"s3_folder_path": s3_folder_path})
    client = boto3.client('lambda', region_name=AWS_REGION)
    
    response = client.invoke(
        FunctionName=S3_TO_REDSHIFT_LAMBDA,
        InvocationType='RequestResponse',
        Payload=payload.encode()
    )
    
    result = json.loads(response['Payload'].read().decode('utf-8'))
    log.info(f"Lambda response received: {result}")
    
    if response.get('FunctionError') or result.get('status') == 'error':
        raise AirflowFailException(f"AWS Lambda COPY command failed: {result}")

def check_bronze_data_callable():
    """
    Queries Redshift to verify data has safely processed in the last 2 hours.
    OPTION 1: Attempts native Airflow DbApiHook.
    OPTION 2: Falls back to raw psycopg2 if Hook fails or connection is missing.
    """
    log.info("Initializing Redshift connectivity gate...")
    
    stream_count = 0 # Initialize to zero for safety in case of connection failure
    batch_count = 0 # Initialize to zero for safety in case of connection failure
    connected_via_hook = False # Flag to track if we successfully connected via Airflow Hook or had to fallback to psycopg2

    # ── OPTION 1: TRY THE NATIVE AIRFLOW HOOK FIRST ──
    try:
        log.info("Attempting Option 1: Native Airflow Connection ('redshift_analytics')...")
        from airflow.providers.common.sql.hooks.sql import DbApiHook
        
        hook = DbApiHook.get_hook(conn_id="redshift_analytics")
        
        # Test the hook connection by running the queries
        stream_res = hook.get_first("""
            SELECT COUNT(*) 
            FROM bronze.raw_rentals 
            WHERE firehose_arrival_at >= GETDATE() - INTERVAL '2 hours';
        """)
        batch_res = hook.get_first("""
            SELECT COUNT(*) 
            FROM bronze.cleaned_batch_rentals 
            WHERE batch_cleaned_at >= GETDATE() - INTERVAL '2 hours';
        """)
        
        # Unpack indices safely (Airflow hook returns a row tuple containing our count at index 0!) 
        stream_count = stream_res[0] if stream_res else 0
        batch_count = batch_res[0] if batch_res else 0
        
        connected_via_hook = True
        log.info("Option 1 Successful! Connected and queried via Airflow Hook.")

    # ── OPTION 2: FALLBACK TO RAW PSYCOPG2 IF HOOK CRASHES OR IS MISSING ──
    except Exception as e:
        log.warning(f"Option 1 Failed (Airflow Hook error: {str(e)}). Engaging Option 2 Fallback...")
        
        if REDSHIFT_HOST and REDSHIFT_USER and REDSHIFT_PASSWORD:
            log.info("Opening raw fallback connection via psycopg2 using local .env keys...")
            conn = psycopg2.connect(
                host=REDSHIFT_HOST,
                port=int(REDSHIFT_PORT),
                dbname=REDSHIFT_DB,
                user=REDSHIFT_USER,
                password=REDSHIFT_PASSWORD,
                sslmode='require',
                connect_timeout=15
            )
            cur = conn.cursor()
        
            # DUAL-TRACK ARCHITECTURE VERIFICATION RULE:
            # 1. Check Track 1: Real-Time Stream Destination (bronze.raw_rentals)
            cur.execute("""
                SELECT COUNT(*) 
                FROM bronze.raw_rentals 
                WHERE firehose_arrival_at >= GETDATE() - INTERVAL '2 hours';
            """)
            stream_count = cur.fetchone()[0]
            
            # 2. Check Track 2: Spark Batch Destination (bronze.cleaned_batch_rentals)
            cur.execute("""
                SELECT COUNT(*) 
                FROM bronze.cleaned_batch_rentals 
                WHERE batch_cleaned_at >= GETDATE() - INTERVAL '2 hours';
            """)
            batch_count = cur.fetchone()[0]
            cur.close()
            conn.close()
            log.info("Option 2 Successful! Connected and queried via raw psycopg2.")
        else:
            raise AirflowFailException(
                "Both connectivity options failed! Airflow hook errored out, "
                "and local .env variables are incomplete or missing."
            )

    log.info(f"Validation Check Complete. Stream Row Count: {stream_count}, Batch Row Count: {batch_count}")
    
    # Dynamic circuit breaker protects downstream dbt constraints
    if stream_count == 0 or batch_count == 0:
        raise AirflowFailException(
            f"Data Quality Gate Failure! (Stream Row Count: {stream_count}, Batch Row Count: {batch_count}). "
            "Aborting pipeline execution to protect downstream dbt dependencies."
        )

# ---------------------------------------------------------------------------
# DAG Orchestration Layer
# ---------------------------------------------------------------------------
with DAG(
    dag_id="dag_1_ingestion",
    description="Scrape Property24 -> API Gateway -> Kinesis -> S3 -> Lambda COPY -> Redshift",
    default_args=DEFAULT_ARGS,
    schedule="0 3 * * *",   # 03:00 UTC / 06:00 EAT
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["nairobi", "ingestion", "pipeline-1"],
) as dag:

    start = EmptyOperator(task_id="start")

    # Task 1: Scrape (Uses BashOperator to isolate geopy/rapidfuzz version conflicts)
    scrape_and_ingest = BashOperator(
        task_id="scrape_and_ingest",
        bash_command=f"python3 {CLIENT_SCRIPT_PATH}",
        execution_timeout=timedelta(minutes=30),
        retries=1,
    )

    # Task 1.5: Trigger serverless AWS Glue Spark Job to clean raw history logs from cold storage
    # Utilizing aws_conn_id='aws_default' which implicitly falls back to your project .env keys!
    trigger_cold_storage_glue_job = GlueJobOperator(
        task_id="trigger_cold_storage_glue_job",
        job_name=GLUE_ETL_JOB_NAME,
        aws_conn_id="aws_default",
        region_name=AWS_REGION,
    )


    # Task 2: Buffer Wait (Uses simple bash sleep command)
    wait_for_firehose = BashOperator(
        task_id="wait_for_firehose_flush",
        bash_command=f"echo 'Waiting {FIREHOSE_WAIT_SECS}s for S3 flush...' && sleep {FIREHOSE_WAIT_SECS}",
    )

    # Task 3: Trigger the S3 -> Redshift COPY command via Lambda
    trigger_copy_lambda = PythonOperator(
        task_id="trigger_copy_lambda",
        python_callable=trigger_copy_lambda_callable,
    )

    # Task 4: Verify rows landed in the database
    check_bronze_data = PythonOperator(
        task_id="check_bronze_data",
        python_callable=check_bronze_data_callable,
    )

    end = EmptyOperator(task_id="end")

    # Executable Sequence Flow
    start >> scrape_and_ingest >> wait_for_firehose >> trigger_copy_lambda >> check_bronze_data >> end
