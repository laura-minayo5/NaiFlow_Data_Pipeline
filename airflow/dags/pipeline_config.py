# airflow/dags/pipeline_config.py
# =================================
# Shared config imported by dag_1_ingestion, dag_2_silver, dag_3_gold.
# One place for all constants — change here, applies everywhere.

import os
from datetime import timedelta
from docker.types import Mount

# ---------------------------------------------------------------------------
# Airflow default_args — same across all three DAGs
# ---------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner":            "nairobi-pipeline",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
}

# ---------------------------------------------------------------------------
# DAG 1 — Ingestion
# ---------------------------------------------------------------------------
CLIENT_SCRIPT_PATH = "/opt/airflow/client/client.py"

APIFY_TOKEN        = os.getenv("APIFY_TOKEN",     "")
API_GATEWAY_URL    = os.getenv("API_GATEWAY_URL", "")
API_GATEWAY_KEY    = os.getenv("API_GATEWAY_KEY", "")
MAX_ITEMS          = int(os.getenv("MAX_ITEMS",   "10000"))
FIREHOSE_WAIT_SECS = int(os.getenv("FIREHOSE_WAIT_MINUTES", "10")) * 60


# ── Double-Bucket S3 Architecture & Specific Lambda Hooks ──
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# The Immutable Data Lake Storage Target
S3_RAW_DATA_BUCKET = os.getenv("S3_RAW_DATA_BUCKET", "nairobi-rentals-raw")

# The Active Kinesis Firehose Staging Target
S3_STAGING_BUCKET = os.getenv("S3_STAGING_BUCKET", "nairobi-firehose-staging-2026")
S3_STAGING_PREFIX = os.getenv("S3_STAGING_PREFIX", "bronze/rentals/")
DYNAMODB_TABLE = os.getenv("DYNAMODB_TABLE", "nairobi_rentals")


# ── Cloud Ingestion, Storage, and Real-Time Lambda Handles ──
# Airflow automatically maps these to your 'aws_default' connection profile!
INGESTION_LAMBDA = os.getenv("INGESTION_LAMBDA", "ingestion-lambda")
STREAM_S3_LAMBDA = os.getenv("STREAM_S3_LAMBDA", "stream-to-s3-lambda")
STREAM_DYNAMODB_LAMBDA = os.getenv("STREAM_DYNAMODB_LAMBDA", "stream-to-dynamodb-lambda")
VISUALIZATION_LAMBDA = os.getenv("VISUALIZATION_LAMBDA", "visualization-lambda")
S3_TO_REDSHIFT_LAMBDA = os.getenv("S3_TO_REDSHIFT_LAMBDA", "s3-to-redshift-copy-trigger-lambda")
GLUE_ETL_JOB_NAME = os.getenv("GLUE_ETL_JOB_NAME", "s3_to_bronze_rentals_batch")
# ---------------------------------------------------------------------------
# Redshift — used by the bronze check task in DAG 1
# and injected into the dbt container in DAGs 2 & 3
# ---------------------------------------------------------------------------
REDSHIFT_HOST = os.getenv("DBT_REDSHIFT_HOST", "")
REDSHIFT_PORT = int(os.getenv("DBT_REDSHIFT_PORT", "5439"))
REDSHIFT_DB = os.getenv("DBT_REDSHIFT_DB", "dev")
REDSHIFT_USER = os.getenv("DBT_REDSHIFT_USER", "")
REDSHIFT_PASSWORD = os.getenv("DBT_REDSHIFT_PASSWORD", "")

# ---------------------------------------------------------------------------
# dbt DockerOperator — DAGs 2, 3 & 4 all use the same image, mounts, and environment variable setup
# ---------------------------------------------------------------------------
# Image name must EXACTLY match the `image:` field in docker-compose.yml
# so DockerOperator finds the locally-built image without pulling from a registry.
DBT_IMAGE = os.getenv("DBT_IMAGE", "dbt-redshift:1.9.0")

# Absolute path to nairobi_dbt/ on the HOST machine.
# Set this in .env — DockerOperator mounts host paths into the container.
DBT_PROJECT_HOST_PATH = os.getenv("DBT_PROJECT_HOST_PATH", "")

# Credentials injected into every dbt container at runtime.
# entrypoint.sh uses these to write profiles.yml inside the container.
DBT_ENV_VARS = {
    "DBT_REDSHIFT_HOST":     REDSHIFT_HOST,
    "DBT_REDSHIFT_PORT":     str(REDSHIFT_PORT),
    "DBT_REDSHIFT_DB":       REDSHIFT_DB,
    "DBT_REDSHIFT_USER":     REDSHIFT_USER,
    "DBT_REDSHIFT_PASSWORD": REDSHIFT_PASSWORD,
}

# Bind mount: dbt_project/ on host → /dbt inside the container
DBT_MOUNT = Mount(
    source = DBT_PROJECT_HOST_PATH,
    target = "/dbt",
    type   = "bind",
)

# Shared kwargs for every DockerOperator in DAGs 2 and 3
DOCKER_OPERATOR_KWARGS = dict(
    image          = DBT_IMAGE,
    mounts         = [DBT_MOUNT],
    environment    = DBT_ENV_VARS,
    docker_url     = "unix://var/run/docker.sock",
    auto_remove    = "success",
    force_pull     = False,  # use the locally-built image, never pull from registry
    mount_tmp_dir  = False,
    network_mode   = "bridge",
)