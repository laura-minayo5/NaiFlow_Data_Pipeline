"""
lambdas/s3-to-redshift-copy-trigger-lambda/lambda_function.py
==========================================
PURPOSE:
--------
This Lambda function forms Pipeline 3 of the Nairobi Rental Data Pipeline.
It is the bridge between the S3 staging bucket and Redshift Serverless.

WHY THIS LAMBDA EXISTS (instead of relying on Firehose direct delivery):
----------------------------------------------------------------------------
Firehose's native Redshift delivery requires enhanced VPC routing to be OFF
and has limited control over COPY options. This Lambda gives us full control:
  - We choose exactly which IAM role Redshift uses to read from S3
  - We specify our own JSON paths file for column mapping
  - We can add retry logic, monitoring, and alerting
  - We can update config (table names, workgroup) via SSM without redeploying

CONFIG STRATEGY:
----------------
All runtime config (workgroup name, table name, IAM role ARN, JSON paths path)
is stored in AWS SSM Parameter Store under the /nairobi/ path hierarchy.
Fetching at runtime (not at import time) means we can update config without
redeploying the Lambda — the next invocation picks up the new values automatically.

"""


import boto3
import logging
import urllib.parse # handles URL-encoded S3 keys
import time

# Logging Setup
# Use the root logger so CloudWatch captures all log levels.
# In Lambda, logs automatically go to CloudWatch Logs under
# /aws/lambda/<function-name> — no handler setup needed.

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# AWS Client
# ---------------------------------------------------------------------------
# Initialize the SSM clienta at module level (outside the handler).
# Lambda reuses the execution environment between warm invocations,
# so this avoids creating a new client on every request.
# region_name is hardcoded here because SSM itself needs a region
# before we can fetch the region from SSM config.
ssm = boto3.client('ssm', region_name='us-east-1')

# Config Loader Function
#  Returns:
#     dict: A flat dictionary mapping the last segment of each parameter path to its value.
#       SSM path → dict key mapping:

def get_nairobi_config():
    # Create a paginator for the SSM GetParametersByPath operation
    paginator = ssm.get_paginator('get_parameters_by_path')
    
    config = {}
    
    # This loop automatically handles 'NextToken' for you 
    # and fetches every single parameter under /nairobi/
    page_iterator = paginator.paginate(
        Path='/nairobi/',
        Recursive=True,
        WithDecryption=True
    )

    for page in page_iterator:
        for p in page['Parameters']:
            key = p['Name'].split('/')[-1]
            config[key] = p['Value']
            
    print(f"DEBUG: Successfully loaded {len(config)} parameters.")
    return config
# ---------------------------------------------------------------------------
# Lambda Handler
# lambda_handler is the entry point — the function AWS calls when Lambda is triggered
# event  = The S3 event payload. Contains a 'Records' list where each record describes one S3 object that was created.

# context = metadata about the Lambda execution (memory, timeout, etc.) — we don't use it here
def lambda_handler(event, context):
    # 1. Get bucket and file name from the S3 event
    if 'Records' not in event or not event['Records']:
        logger.info("No records found in this event trigger. Skipping.")
        return
    
    # 2. Fetch fresh config at runtime to ensure Lambda invocation gets the latest SSM values.
    # If we change the table name or IAM role in SSM, the next invocation picks it up automatically — no redeploy needed.
    config = get_nairobi_config()
    logger.info("SSM config dump: %s", config)  # ADD THIS LINE TEMPORARILY TO DEBUG CONFIG ISSUES
    WORKGROUP_NAME = config.get('workgroup_name')
    DATABASE_NAME = config.get('database_name')
    REDSHIFT_IAM_ROLE = config.get('redshift_iam_role') # ARN of the IAM role Redshift uses to read from S3
    TABLE_NAME = config.get('table_name')
    REGION_NAME = config.get('region_name', 'us-east-1') # default to us-east-1 if not set
    JSON_PATHS = config.get('json_paths_s3_path') # S3 URI of the JSON paths mapping file
    SECRET_ARN = config.get('redshift_secret_arn')
    
    logger.info(f"Config loaded | workgroup={WORKGROUP_NAME} | database={DATABASE_NAME} | table={TABLE_NAME}")

    if not SECRET_ARN:
        logger.error("Missing critical configuration parameter: redshift_secret_arn")
        raise ValueError("redshift_secret_arn must be present in SSM config.")

    
    logger.info(f"Config loaded | workgroup={WORKGROUP_NAME} | database={DATABASE_NAME} | table={TABLE_NAME}")

    # 3. Initialize Redshift Data API client
    # The Redshift Data API lets Lambda execute SQL against Redshift Serverless
    # without needing a VPC connection or JDBC driver.
    # We initialise inside the handler (not at module level) because the region
    # comes from SSM config fetched in Step 3.
    client = boto3.client('redshift-data', region_name=REGION_NAME)


    # 4. Extract bucket and object key from the S3 event
    # Process every file inside the pipeline invocation payload
    # Add unquote_plus here to handle special characters/spaces safely
    # S3 keys in event notifications are URL-encoded.
    # Extract the file key and decode it (e.g., convert %20 back to spaces)
    # This ensures the COPY command doesn't fail on special characters in intermediate s3 prefix
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        raw_key = record['s3']['object']['key']
        key = urllib.parse.unquote_plus(raw_key)

        # Skip folder creation triggers sent by automated tools
        if key.endswith('/'):
            logger.info(f"Skipping folder metadata directory event: {key}")
            continue

        # Build the full S3 URI that the Redshift COPY command will read from.
        s3_path = f's3://{bucket}/{key}'
        logger.info(f"Processing target file | bucket={bucket} | key={key}")

        # 5. Build the COPY command 
        # COPY is Redshift's bulk-load command. It reads the NDJSON file from S3 and inserts the rows into bronze redshift table.
        # Note: Using actual S3 path of our JSON mapping file that maps JSON keys → table columns
        sql_copy = f"""
        COPY {TABLE_NAME} 
        FROM '{s3_path}' 
        IAM_ROLE '{REDSHIFT_IAM_ROLE}'
        FORMAT AS JSON '{JSON_PATHS}';
        """
    
        # 6. Execute the COPY command via Redshift Data API
        # execute_statement is asynchronous — it submits the COPY job and returns a query ID immediately without waiting for completion.
        # The COPY runs inside Redshift and typically completes within 5–30 seconds depending on file size.
        try:
            logger.info(f"Triggering COPY for {s3_path}")
            response = client.execute_statement(
                WorkgroupName=WORKGROUP_NAME,
                Database=DATABASE_NAME, 
                SecretArn=SECRET_ARN,
                Sql=sql_copy
            )
            query_id = response['Id']
            logger.info(f"Triggered. Query ID: {query_id}")
            # Add a quick polling mechanism to capture immediate serverless initialization failures
            for _ in range(5):
                time.sleep(1)
                status_resp = client.describe_statement(Id=query_id)
                status = status_resp['Status']
                if status in ['FAILED', 'FINISHED', 'ABORTED']:
                    logger.info(f"Immediate Serverless Execution Status: {status}")
                    if status == 'FAILED':
                        logger.error(f"Redshift Serverless execution error: {status_resp.get('Error')}")
                    break
        except Exception as e:
            # Log the full error before re-raising so CloudWatch has the details.
            # Re-raising causes Lambda to mark this invocation as failed,
            # which triggers any CloudWatch Alarm you have on Lambda errors.
            logger.error(f"Error triggering Redshift COPY: {str(e)}")
            raise e
