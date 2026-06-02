"""
lambdas/pipeline_1_ingestion/lambda_function.py
================================================
PIPELINE 1  (matches your diagram exactly):
  Python client  →  POST  →  API Gateway  →  trigger  →  THIS Lambda
                                                               │
                                                               ▼  write
                                                         Kinesis Data Stream

What this Lambda does:
  1. Receives the HTTP POST body from API Gateway
  2. Validates that "location_raw" and "price_raw" exist
  3. Calls kinesis.put_record() to push the JSON onto the stream
     (Kinesis then fans out to Pipeline 2 consumers)
"""
# --- Standard Library (Built-in) ---
import json # built-in Python library — converts between Python objects and JSON strings
import logging  # built-in Python library — lets us write logs to CloudWatch
import base64 # built-in Python library — for encoding/decoding binary data (not used in this code but often needed for Kinesis)
import os # built-in Python library — for reading environment variables
from datetime import datetime, timezone

# --- Third-Party Libraries (must be in requirements.txt) ---
import boto3 # AWS SDK for Python — this is how Python talks to AWS services

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
# Get the root logger (AWS already configured this for CloudWatch)
logger = logging.getLogger()
logger.setLevel(logging.INFO)


# ---------------------------------------------------------------------------
# Config (set these in SSM Parameter Store and fetch at runtime)
# ---------------------------------------------------------------------------
# Initialize the SSM client
ssm = boto3.client('ssm', region_name='us-east-1')

# def get_nairobi_config():
#     # This fetches everything under the /nairobi/ path at once
#     response = ssm.get_parameters_by_path(
#         Path='/nairobi/',
#         WithDecryption=True
#     )
    
#     # This turns the list into a dictionary: {'dynamodb_table': 'nairobi-rentals', ...}
#     config = {p['Name'].split('/')[-1]: p['Value'] for p in response['Parameters']}
#     return config

def get_nairobi_config():
    try:
        response = ssm.get_parameters_by_path(
            Path='/nairobi/',
            WithDecryption=True
        )
        # Add this print line to see what's actually coming back in your logs
        print(f"SSM Response: {response['Parameters']}") 
        
        if not response['Parameters']:
             logger.error("No parameters found under /nairobi/ path!")
             
        config = {p['Name'].split('/')[-1]: p['Value'] for p in response['Parameters']}
        return config
    except Exception as e:
        print(f"Internal SSM Error: {str(e)}")
        raise e
# --- Usage ---
config = get_nairobi_config()

REGION_NAME   = config.get('region_name')
KINESIS_STREAM_NAME = config.get('kinesis_stream')

# Create a Kinesis client — this is the object we use to call Kinesis APIs
# Think of it as opening a connection to Kinesis so we can send data to it
# region_name tells boto3 which AWS region our Kinesis stream lives in

kinesis = boto3.client('kinesis', region_name=REGION_NAME)



# ---------------------------------------------------------------------------
# Helper Functions to keep the main lambda_handler clean and focused on the core logic
# ---------------------------------------------------------------------------
def _resp(status: int, body: dict) -> dict:
    """Helper function to format the HTTP response for API Gateway."""
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*", # CORS header to allow API calls from frontend dashboards
        },
        "body": json.dumps(body),
    }


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
# Define the fields every valid record must have
# If any are missing we reject the record before it pollutes Kinesis
REQUIRED_FIELDS = ["location_raw", "price_raw", "property_id"] # we also require property_id to ensure we can use it as a partition key in Kinesis and DynamoDB
# Custom exception for validation errors — this makes it easier to handle validation issues separately from other exceptions 
class ValidationError(Exception):
    pass

def validate(body: dict) -> None:
    missing = [field for field in REQUIRED_FIELDS if not body.get(field)]
    if missing:
        raise ValidationError(f"Missing required fields: {', '.join(missing)}")


# ---------------------------------------------------------------------------
# Lambda Handler
# lambda_handler is the entry point — the function AWS calls when Lambda is triggered
# event  = the data that triggered Lambda (contains the HTTP request from API Gateway)
# context = metadata about the Lambda execution (memory, timeout, etc.) — we don't use it here
# ---------------------------------------------------------------------------
def lambda_handler(event: dict, context) -> dict:
    """
    Triggered by API Gateway.
    event["body"] is the raw JSON string POSTed by client.py
    """
    # ── Parse body ──────────────────────────────────────────────────────────
    try:
        raw_body = event.get("body") or "{}"
        if event.get("isBase64Encoded"):
            raw_body = base64.b64decode(raw_body).decode("utf-8")
        
        # Guard check — if no body was sent, return a 400 error immediately
        # No point going further if there's no data to process
        if not raw_body:
            return _resp(400, {"error": "Empty request body"})

        # Handle cases where body might already be a dict or a JSON string
        body: dict = json.loads(raw_body) if isinstance(raw_body, str) else raw_body
    except Exception as e:
        logger.error(f"Failed to parse body: {e}")
        return _resp(400, {"error": "Invalid JSON or Base64 encoding"})


    # ── Validate ─────────────────────────────────────────────────────────────
    try:
        validate(body) # if validation fails, this will raise a ValidationError which we catch and return a 400 response to the client with the error message. This way we prevent bad data from entering Kinesis and give feedback to the client about what they did wrong.
    except ValidationError as e:
        logger.warning(f"Validation failed: {e}")
        return _resp(400, {"error": str(e)})
 

    # ── Stamp with ingestion time ─────────────────────────────────────────────
    body["ingested_at"] = datetime.now(timezone.utc).isoformat()

    # ── put_record → Kinesis ─────────────────────────────────────────────────
    # Partition key = property_id and has to exist in the body for us to write to Kinesis.
    # This ensures all updates for the SAME house stay in the same order
    partition_key = body["property_id"]

    try:
        resp = kinesis.put_record(
            StreamName   = KINESIS_STREAM_NAME,
            Data         = json.dumps(body).encode("utf-8"),
            PartitionKey = partition_key,
        )
    
        # Log the successful put_record with details about the record and where it was stored in Kinesis. This is useful for debugging and monitoring.
        # 1. Prepare the detailed log payload
        log_payload = {
            "status": "success",
            "property_id": partition_key,
            "shard_id": resp["ShardId"],
            "sequence_number": resp["SequenceNumber"],
            "neighbourhood": body.get("neighbourhood"),
            "price": body.get("price_raw") # Optional: handy to see in logs
        }

        # 2. Log as a JSON string for CloudWatch Logs Insights
        logger.info(json.dumps(log_payload))

        return _resp(201, {
            "message":      "Record ingested into Kinesis",
            "property_id": partition_key,
            "shardId": resp["ShardId"]
        })
    except Exception as e:
        # Catch any unexpected errors — log them and return a 500 error
        # 500 = Internal Server Error in HTTP
        # This means something broke inside Lambda itself
        logger.error(f"Kinesis put_record failed: {e}")
        return _resp(500, {"error": "Internal Server Error: Failed to write to Kinesis"})



