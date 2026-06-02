"""
lambdas/pipeline_2_stream_to_dynamodb/lambda_function.py
=========================================================
PIPELINE 2  —  stream-to-dynamodb-lambda

  Kinesis Data Stream
       │
       ├──► stream-to-s3-lambda       (separate Lambda, same stream)
       │
       └──► THIS Lambda  ──► DynamoDB NairobiRentals

Why DynamoDB in parallel?
  • Low-latency lookups by property_id or neighbourhood
  • Power BI can query "latest price for Westlands" in <10 ms
  • Acts as a hot cache — no need to query Redshift for single-record lookups

Partition key: property_id  (unique per listing)
GSI:           neighbourhood (query all listings in a neighbourhood)
"""

import os
import json
import base64
import logging
import boto3
from decimal import Decimal
from datetime import datetime, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)

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

DYNAMODB_TABLE = config.get('dynamodb_table')   # NairobiRentals
TTL_DAYS       = int(config.get('ttl_days', '90'))
REGION_NAME = config.get('region_name', 'us-east-1') # default to us-east-1 if not set

# Create DynamoDB resource — note we use resource not client
# resource gives us a higher level, easier to use interface
dynamodb = boto3.resource("dynamodb", region_name=REGION_NAME)


# ---------------------------------------------------------------------------
# Helpers
# 1. decode_kinesis_record: Unwraps the Kinesis record to get the original JSON data.
# ---------------------------------------------------------------------------
def decode_kinesis_record(record: dict) -> dict:
    raw_data = record["kinesis"]["data"]
    # If it's already bytes, we don't need to decode b64
    if isinstance(raw_data, str):
        decoded = base64.b64decode(raw_data).decode("utf-8")
    else:
        decoded = raw_data.decode("utf-8")
    return json.loads(decoded)

# 2. to_decimal: Recursively converts floats to Decimal for DynamoDB compatibility.
def to_decimal(obj):
    """
    DynamoDB hates Python float types because they can lead to precision issues. Instead, it wants Decimal.
     - If we try to put a float directly into DynamoDB, we'll get a TypeError
     - This function recursively converts all floats in a dict (or list) to Decimal, which DynamoDB can handle safely.
     - We convert the float to a string first before Decimal to avoid any precision issues that can arise from directly converting a float to Decimal. This ensures that the exact value is preserved when we store it in DynamoDB.
    """
    # Base case: if it's a float, convert to Decimal
    if isinstance(obj, float):
        return Decimal(str(obj))
    # Recursive case: finds all floats hidden—in dict, list, or at the top level—find them all and fix them
    # if it's a dict, apply to_decimal to each value; if it's a list, apply to_decimal to each element
    if isinstance(obj, dict):
        return {k: to_decimal(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [to_decimal(v) for v in obj]
    return obj


# 3. build_dynamo_item: Prepares the item for DynamoDB, ensuring it has a partition key and TTL.
def build_dynamo_item(data: dict) -> dict:
    """Convert an enriched rental record into a DynamoDB-safe item. Returns None if critical data is missing."""
    # 1. Check for the MUST-HAVE field first
    # Validation: If no property_id, we can't save it as a unique record
    pid = data.get("property_id")
    if not pid:
        # Returning None tells the caller: "This record is junk, skip it"
        log.warning("Skipping record: missing property_id")
        return None
    # 2. Convert all floats to Decimal for DynamoDB compatibility
    # We also filter out any fields where the value is None, since DynamoDB does not allow null values. This ensures that we only include fields with valid data in our DynamoDB item.
    # we use a dict comprehension to create a new dictionary that includes only the key-value pairs from the original data where the value is not None.
    # The original data dictionary contains the fields of the rental record so no need to explicitly list them here. We just filter out any fields that have a value of None.
    item = to_decimal({k: v for k, v in data.items() if v is not None})


    # 3. Add Metadata 
    # TTL: (auto-destruct timer)Tells DynamoDB when to auto-delete this record (Current time + X days)
    # DynamoDB expects TTL to be a Unix timestamp (seconds since epoch)
    # so we calculate it by taking the current time in UTC, converting it to a timestamp, and adding the number of seconds in TTL_DAYS.
    # Since you've set TTL_DAYS to 90, this means that each record will automatically expire and be deleted from DynamoDB 90 days after it was added. This is a common practice to manage storage costs and ensure that your database doesn't get cluttered with old, irrelevant data over time.
    # This prevents our db from becoming a "Digital Graveyard" of old, useless rental ads.

    item["ttl"] = int(datetime.now(timezone.utc).timestamp()) + TTL_DAYS * 86400

 
    # Add the timestamp as the Sort Key
    # This ensures every time you scrape, you create a NEW entry 
    # instead of overwriting the old price. This allows you to track price changes over time for the same property_id.
    if data.get("scraped_at"):
        item["scraped_date"] = data["scraped_at"]
    else:
        # Use ISO format with 'T' (e.g., 2026-04-14T12:30:45) for better readability and sorting in DynamoDB. 
        # using full timestamp so every single scrape is a unique entry in the timeline. 
        item["scraped_date"] = datetime.now(timezone.utc).isoformat()

    return item


# ---------------------------------------------------------------------------
# Lambda Handler
    # Loop through each record Kinesis sent in this batch
    # kinesis returns an array of records in event['Records']
    # Each record has the structure:
    # {
    #   "kinesis": {
    #       "data": "base64-encoded string"
    #   },
    #   ...
    # }
# ---------------------------------------------------------------------------
def lambda_handler(event: dict, context) -> dict:
    records    = event.get("Records", [])
    batch_size = len(records)
    logger.info(f"Received {batch_size} Kinesis record(s) → writing to DynamoDB")

    table = dynamodb.Table(DYNAMODB_TABLE)
    success_count   = 0
    fail_count = 0

    # Use batch_writer for efficiency (auto-batches in groups of 25)
    with table.batch_writer() as batch:
        for i, record in enumerate(records):
            try:
                # 1. Decode the Kinesis record to get the original JSON data
                data = decode_kinesis_record(record)

                # 2. Build the DynamoDB item, ensuring it has a partition key and TTL
                item = build_dynamo_item(data)

                # 3. If build_dynamo_item returns None, it means the record is missing critical data (like property_id) and should be skipped
                if item is None:
                    fail_count += 1
                    logger.warning(f"  [{i}/{batch_size}] SKIPPED: missing property_id")
                    continue

                # 4. Write the item to DynamoDB using batch_writer, which handles batching and retries for us
                batch.put_item(Item=item)
                success_count += 1
                logger.info(
                    f"  [{i}/{batch_size}] ✔ DynamoDB item queued | property_id={item.get('property_id')} | neighbourhood={item.get('neighbourhood')}"
                )
            except Exception as e:
                fail_count += 1
                logger.error(f"  [{i}/{batch_size}] ✗ CRASHED (System Error): {e}")

    logger.info(f"DynamoDB batch write complete: {success_count} saved, {fail_count} failed/skipped")

    # Emergency Brake: If the whole batch crashed, tell Kinesis to retry
    if fail_count == batch_size and batch_size > 0:
        raise RuntimeError("All DynamoDB writes failed, Check Table/Permissions — Kinesis will retry")

    return {
        "statusCode": 200,
        "results": {"processed": success_count, "failed": fail_count}
    }

    # KINESIS RECORD STRUCTURE:
    # {
    #   "eventID": "shardId-000000000000:sequenceNumber",
    #   "kinesis": {
    #       "data": "base64-encoded string",
    #       "sequenceNumber": "string",
    #       "approximateArrivalTimestamp": 1234567890.123
    #   },
    #   ...
    # }