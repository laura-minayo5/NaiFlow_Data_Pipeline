"""
lambdas/pipeline_2_stream_to_s3/lambda_function.py
====================================================
PIPELINE 2  —  stream-to-s3-lambda  (matches your diagram exactly):


  Kinesis          →  Kinesis event              →  THIS Lambda           →  S3 bucket
  triggers Lambda     event['Records'] = list       reads event['Records']   file written ✅

  Ingestion: Scraper sends JSON string data to Kinesis.
  Kinesis: Encodes it to Binary and wraps it in a Dictionary Envelope.
  Kinesis Event: Triggers this Lambda with event['Records'] = list of Kinesis records.
  Lambda: * Unwraps the envelope (Records) by reading event['Records'].
          * Decodes the gift (base64 → json), i.e each record in event['Records'].
          * Stamps the history (_kinesis_shard) for traceability.
          * Stretches the list into a long text block (ndjson_body).
          * Writes the batch as a NDJSON file to S3
                (S3 key: raw/YYYY/MM/DD/HH/<shard>-<timestamp>.ndjson)

  S3: Stores it in a perfectly organized folder (year=2026/...).
  S3 Path convention:
    s3://<BUCKET>/raw/2025/01/15/14/shardId-000000000000-20250115T141523Z.ndjson
"""

import os
import json
import base64
import logging
import boto3
from datetime import datetime, timezone

logger = logging.getLogger()
logger.setLevel(logging.DEBUG) # Set the "Master" level to the lowest (DEBUG) so all logs are captured

# ---------------------------------------------------------------------------
# Config (set these in SSM Parameter Store and fetch at runtime)
# ---------------------------------------------------------------------------
# Initialize the SSM client
ssm = boto3.client('ssm', region_name='us-east-1')

def get_nairobi_config():
    # This fetches everything under the /nairobi/ path at once
    response = ssm.get_parameters_by_path(
        Path='/nairobi/',
        WithDecryption=True
    )
    
    # This turns the list into a dictionary: {'dynamodb_table': 'nairobi-rentals', ...}
    config = {p['Name'].split('/')[-1]: p['Value'] for p in response['Parameters']}
    return config

# --- Usage ---
config = get_nairobi_config()

S3_BUCKET     = config.get('s3_bucket')                         # S3 bucket name from environment variable
S3_PREFIX     = config.get('s3_prefix', 'raw')                  # folder prefix
REGION_NAME    = config.get('region_name', 'us-east-1') # default to us-east-1 if not set

s3 = boto3.client("s3", region_name=REGION_NAME)


# ---------------------------------------------------------------------------
# Helpers
# Decoding Kinesis records(Actual Package) and converting to NDJSON format for S3 storage

# ---------------------------------------------------------------------------
def decode_kinesis_record(record: dict) -> dict:
    """
    Kinesis wraps the payload in base64.
    event['Records'][i]['kinesis']['data'] is base64-encoded.
    """


    # record['kinesis']['data'] is where Kinesis puts our actual payload
    # data: This is the Base64 encoded JSON string scraper sent to Kinesis.
    # Kinesis always bae64 encodes data when storing it in the stream, regardless of the original format. This is because Kinesis is designed to handle binary data, and base64 encoding ensures that the data can be safely transmitted and stored as text.
    # base64 is an encoding that converts any data into plain text characters
    # we must decode it back before we can read it
        
    
    payload = record["kinesis"]["data"] #"data": "SGVsbG8gTmFpcm9iaSE=" encoded
    # Convert Base64 to String
    decoded = base64.b64decode(payload).decode("utf-8")
    # Convert String to Dictionary (JSON Parsing)
    return json.loads(decoded)


def build_s3_key(shard_id: str, now: datetime) -> str:
    """
    Partitioned path for easy Athena/Glue crawling:
    raw/year=2025/month=01/day=15/hour=14/<shard>-<ts>.ndjson
    """
    y  = now.strftime("%Y")
    mo = now.strftime("%m")
    d  = now.strftime("%d")
    h  = now.strftime("%H")
    ts = now.strftime("%Y%m%dT%H%M%SZ")
    safe_shard = shard_id.replace(":", "-") # Replace colons with hyphens for S3 key compatibility just in case, since shard IDs can contain colons and S3 keys can't have colons. This ensures our S3 keys are valid and won't cause issues when we try to read them later with Athena or Glue.
    return f"{S3_PREFIX}/year={y}/month={mo}/day={d}/hour={h}/{safe_shard}-{ts}.ndjson" #NDJSON ({}\n{}) format is a convenient way to store and process large datasets in S3, especially when we want to use Athena or Glue later for querying. Each line in the NDJSON file is a separate JSON object, which makes it easy to read and process without having to load the entire file into memory at once.

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
# Lambda Handler
# ---------------------------------------------------------------------------
def lambda_handler(event: dict, context) -> dict:
    """
    Triggered by Kinesis.
    event['Records'] is a list of Kinesis records, each containing a base64-encoded payload.
    """
    records    = event.get("Records", []) # return empty list if 'Records' key is missing
    now        = datetime.now(timezone.utc)
    batch_size = len(records)

    logger.info(f"Received {batch_size} Kinesis record(s)")

    if batch_size == 0:
        return _resp(200, {"message": "No records"})

    # ── Decode all records ────────────────────────────────────────────────────
    decoded_rows = []
    errors       = []

    for i, record in enumerate(records):
        try:
            data = decode_kinesis_record(record) # decode the base64-encoded payload and parse it as JSON and convert it to a dictionary
            # Attach Kinesis metadata for traceability
            # Metadata: Envelope information about the record, such as shard ID, sequence number, and timestamp
            # This metadata is crucial for debugging and traceability. If something goes wrong downstream (like in Athena queries), we can look back at the S3 file and see exactly which Kinesis shard and sequence number the data came from, and when it arrived. This helps us understand the flow of data through our pipeline and identify any issues that may arise.
            # eventID format: shardId-000000000000:sequenceNumber
            # where shardId-000000000000 is the shard ID and sequenceNumber is the unique identifier for the record within that shard. We split on ":" to extract the shard ID for our metadata.
            data["_kinesis_shard"]     = record["eventID"].split(":")[0]
            data["_kinesis_seq"]       = record["kinesis"]["sequenceNumber"]
            data["_kinesis_timestamp"] = record["kinesis"]["approximateArrivalTimestamp"]
            decoded_rows.append(data)
        except Exception as e:
            logger.error(f"Failed to decode record {i}: {e}")
            errors.append({"index": i, "error": str(e)})

    if not decoded_rows:
        logger.error(f"All {batch_size} records failed to decode")
        raise RuntimeError("All Kinesis records failed decoding — will retry")

    # ── Write NDJSON to S3 ───────────────────────────────────────────────────
    # Kinesis usually sends records from the same shard in a single Lambda trigger
    # eventID (Shard ID): Tells which pipe the data traveled through
    # Use the shard ID from the first record to name the file

    shard_id = records[0]["eventID"].split(":")[0]
    s3_key   = build_s3_key(shard_id, now)

    ndjson_body = "\n".join(json.dumps(row) for row in decoded_rows)


    try:
        s3.put_object(
            Bucket      = S3_BUCKET,
            Key         = s3_key,
            Body        = ndjson_body.encode("utf-8"),
            ContentType = "application/x-ndjson", 
            Metadata    = {
                "record-count": str(len(decoded_rows)),
                "pipeline":     "nairobi-rentals",
            },
        )
        logger.info(
            f"✔ S3 write OK | s3://{S3_BUCKET}/{s3_key} | {len(decoded_rows)} rows"
        )
    except Exception as e:
        logger.error(f"S3 put_object failed: {e}")
        # Raise so Kinesis retries the batch — prevents data loss.
        # If write to S3 fails, Kinesis retries the entire batch until it succeeds, according to the retry policy configured for the Lambda function.
        raise

    response_body = {
    "message": "Batch processed and stored in S3",
    "records_ok": len(decoded_rows),
    "records_error": len(errors),
    "s3_key": s3_key
    }
    
    return _resp(200, response_body)


    #KINESIS RECORD STRUCTURE:
    # {
    #   "eventID": "shardId-000000000000:sequenceNumber",
    #   "kinesis": {
    #       "data": "base64-encoded string",
    #       "sequenceNumber": "string",
    #       "approximateArrivalTimestamp": 1234567890.123
    #   },
    #   ...
    # }