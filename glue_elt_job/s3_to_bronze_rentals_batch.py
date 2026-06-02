import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsgluedq.transforms import EvaluateDataQuality 
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType, StringType, IntegerType

# Remember to add 'CATALOG_DB', 'CATALOG_TABLE', and 'REDSHIFT_CONNECTION' as job parameters
# =============================================================================
# AWS GLUE SPARK INITIALIZATION & BOOTSTRAPPING BLOCK
# =============================================================================

# 1. PARSE EXTERNAL RUNTIME ARGUMENTS
# Extracts environmental variables passed from the AWS Console or an Orchestrator.
# It acts like an input directory lookup, packing values into a native Python dictionary.
# Add 'TempDir' to the end of your argument list so the script can fetch it from the console
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'REDSHIFT_CONNECTION', 'CATALOG_DB', 'CATALOG_TABLE', 'TempDir'])


# 2. START THE SPARKCONTEXT (The Communication Backbone)
# This instantiates the core engine inside the Driver Program.
# It communicates with the Cluster Manager to negotiate cloud resources, provision 
# your Worker Nodes (DPUs), and register active Executors in the cluster.
sc = SparkContext()

# 3. UPGRADE TO GLUBCONTEXT (The AWS Proprietary Integration Layer)
# Wraps the open-source SparkContext (sc) with AWS-specific features.
# It injects specialized tools allowing Spark to understand AWS infrastructure natively, 
# such as reading metadata from the Glue Catalog and handling network streaming into Redshift.
glueContext = GlueContext(sc)

# 4. SPIN UP THE SPARKSESSION (The SQL Query Engine Counter)
# Extracted from the Glue Context to act as the modern interface for relational queries.
# This initializes the Catalyst Optimizer, which parses your text strings (like your 'WHERE' 
# filters and 'PARTITION BY' statements), translates them into logical DAG blueprints, 
# and compiles them into parallel Tasks for the Executors.
spark = glueContext.spark_session

# 5. INITIALIZE THE AWS GLUE TRACKING SYSTEM
# Notifies the AWS management plane that your Driver has successfully booted up.
# It binds your script to Glue's infrastructure management, enabling operational 
# tracking tools like CloudWatch memory profiling metrics and Job Bookmarks.
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# =============================================================================
# KITCHEN ANALOGY: The commercial kitchen space is now open. 
# 'args' is your recipe instructions, 'sc' fired up the main utilities, 'glueContext' 
# set up the special appliances, 'spark' wiped down the prep counter, and 'job.init' 
# punched your timecard to start logging the shift.
# =============================================================================


# =============================================================================
# STEP 1A: READ RAW DATA FROM S3 VIA GLUE CATALOG
# =============================================================================
# create a DynamicFrame directly from the Glue Catalog, which abstracts the S3 data source
# DynamicFrames are AWS Glue's extension of Spark DataFrames, designed to handle semi-structured data and schema inconsistencies more gracefully. 
# They also integrate with AWS Glue's metadata management and transformation capabilities.
s3_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database=args['CATALOG_DB'],       # Tells Spark to look inside the database that "Holds metadata for raw scraped nairobi rentals data"
    table_name=args['CATALOG_TABLE'],   # Tells Spark to read from the specific table that "Points to the S3 location of the raw batch rentals data"
    transformation_ctx="s3_dynamic_frame"
)

# =============================================================================
# STEP 1B: SOURCE DATA QUALITY CHECK
# Validates raw scraped data before any transformation begins.
# Catches scraper failures, empty files, and missing core fields early
# so we don't waste compute transforming garbage data.
# =============================================================================
# 1.Create the ruleset using Data Quality Definition Language (DQDL).
source_rental_dq_ruleset="""
    Rules = [
        RowCount > 100,
        IsComplete "property_id",
        IsComplete "scraped_at",
        IsComplete "ingested_at",
        IsComplete "source_url"
    ]
"""

# 2.Validate the dataset against the ruleset.
source_dq_results = EvaluateDataQuality().process_rows(
    frame=s3_dynamic_frame,
    ruleset= source_rental_dq_ruleset,
    publishing_options={
        "dataQualityEvaluationContext": "source_dq",
        "enableDataQualityCloudWatchMetrics": True,
        "enableDataQualityResultsPublishing": True
    }
)

# 3.Review results
# Extract ruleOutcomes from collection and halt if any rule failed
source_dq_df = source_dq_results.select("ruleOutcomes").toDF()
failed_source_rules = source_dq_df.filter(source_dq_df.Outcome == "Failed").count()
if failed_source_rules > 0:
    source_dq_df.show(truncate=False)
    raise Exception(f"Pipeline halted at source: {failed_source_rules} source DQ rule(s) failed.")

# Extract rowLevelOutcomes(data) from collection
s3_dynamic_frame = source_dq_results.select("rowLevelOutcomes")

# Convert to standard Spark DataFrame for column casting
# This allows us to leverage Spark's powerful Catalyst Optimizer for query planning and execution, 
# which can significantly improve performance for complex transformations and aggregations.
raw_s3_df = s3_dynamic_frame.toDF()

# Cast all columns to their correct types immediately after reading from catalog
raw_s3_df = raw_s3_df \
    .withColumn("latitude",            col("latitude").cast(DoubleType())) \
    .withColumn("longitude",           col("longitude").cast(DoubleType())) \
    .withColumn("dist_to_archives_km", col("dist_to_archives_km").cast(DoubleType())) \
    .withColumn("price_ksh",           col("price_ksh").cast(IntegerType())) \
    .withColumn("bedrooms",            col("bedrooms").cast(IntegerType())) \
    .withColumn("bathrooms",           col("bathrooms").cast(IntegerType()))
# =============================================================================
# STEP 2: WRITE CLEANED AUDIT COPY TO BRONZE.RAW_BATCH_RENTALS
# =============================================================================
# OPTIMIZATION FIX: Select ONLY the base columns that match your Redshift table schema.
# rowLevelOutcomes adds DQ annotation columns which must be stripped before writing to Redshift
# Strip DQ annotation columns — keep only the original data columns
# This also explicitly strips away the extra 'kinesis...' and 'year/month' columns in raw data.
raw_redshift_aligned_df = raw_s3_df.select(
    "property_id", "scraped_at", "source_url", "location_raw", "neighbourhood",
    "latitude", "longitude", "dist_to_archives_km", "price_raw", "price_ksh",
    "bedrooms", "bathrooms", "property_type", "title", "ingested_at"
)
# Convert back to DynamicFrame for optimized bulk loading
raw_batch_dyf = DynamicFrame.fromDF(raw_redshift_aligned_df, glueContext, "raw_batch_dyf")

# =============================================================================
# STEP 3: WRITE UNTOUCHED RAW DATA TO BRONZE.BATCH_RENTALS
# =============================================================================
glueContext.write_dynamic_frame.from_options(
    frame=raw_batch_dyf,
    connection_type="redshift",
    connection_options={
        "useConnectionProperties": "true",
        "dbtable": "bronze.raw_batch_rentals_staging",  # writes to staging first
        "connectionName": args['REDSHIFT_CONNECTION'],
        "redshiftTmpDir": args['TempDir'], # A temporary S3 staging directory for the Redshift bulk upload process.
        "preactions": """
            DROP TABLE IF EXISTS bronze.raw_batch_rentals_staging;
            CREATE TABLE bronze.raw_batch_rentals_staging (
                property_id           VARCHAR(255),
                scraped_at            VARCHAR(50),
                source_url            VARCHAR(max),
                location_raw          VARCHAR(2000),
                neighbourhood         VARCHAR(500),
                latitude              FLOAT8,
                longitude             FLOAT8,
                dist_to_archives_km   FLOAT8,
                price_raw             VARCHAR(1000),
                price_ksh             INTEGER,
                bedrooms              SMALLINT,
                bathrooms             SMALLINT,
                property_type         VARCHAR(255),
                title                 VARCHAR(max),
                ingested_at           VARCHAR(50)
            );
        """,
        "postactions": """
            DELETE FROM bronze.raw_batch_rentals
            USING bronze.raw_batch_rentals_staging
            WHERE bronze.raw_batch_rentals.property_id 
                = bronze.raw_batch_rentals_staging.property_id;

            INSERT INTO bronze.raw_batch_rentals (
                property_id, scraped_at, source_url, location_raw, neighbourhood,
                latitude, longitude, dist_to_archives_km, price_raw, price_ksh,
                bedrooms, bathrooms, property_type, title, ingested_at
                )
            SELECT
                property_id, scraped_at, source_url, location_raw, neighbourhood,
                latitude, longitude, dist_to_archives_km, price_raw, price_ksh,
                bedrooms, bathrooms, property_type, title, ingested_at
            FROM bronze.raw_batch_rentals_staging;

            DROP TABLE bronze.raw_batch_rentals_staging;
        """
    }
)
# =============================================================================
# STEP 4: PERFORM CLEANING, DEDUPLICATION AND SOME BUSINESS LOGIC IN MEMORY
# =============================================================================
# Use spark.sql to run your dbt business logic directly on the raw Spark DataFrame, leveraging the Catalyst Optimizer for efficient execution.
# Register the raw Spark dataframe i.e raw_s3_df as an in-memory SQL view for Spark SQL execution
raw_s3_df.createOrReplaceTempView("raw_rentals_view")

# Merged dbt business logic running directly on Spark Catalyst Optimizer
cleaned_df = spark.sql("""
    WITH raw_data AS (
        SELECT * FROM raw_rentals_view
    ),
    
   -- STEP 1: Apply the quality gate immediately to discard useless payloads
   filtered_raw AS (
        SELECT *
        FROM raw_data
        WHERE property_id IS NOT NULL
          AND TRIM(property_id) <> ''
          -- Quality gate: The record MUST have at least a location or a price to be valuable
          AND location_raw IS NOT NULL      -- Reject records with no location data (unusable for geospatial analysis)
          AND price_ksh IS NOT NULL      -- Reject records with no price data (unusable for pricing analysis)
          AND price_ksh BETWEEN 3000 AND 2000000    -- Reject implausible prices (below cheapest Nairobi bedsit, above penthouse)
    ),

    -- STEP 2: Deduplicate only among the valid records
    -- Deduplicate
    -- Firehose guarantees at-least-once delivery — duplicates are expected.
    -- Keep only the most recently arrived copy per property_id.
    -- the "deduplicated" CTE removes duplicate records based on property_id, keeping only the first occurrence based on firehose_arrival_at.
    -- the ROW_NUMBER() function assigns a unique sequential integer to rows within a partition of a result set,
    -- in this case, partitioned by property_id and ordered by firehose_arrival_at in descending order.
    deduplicated AS (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY property_id
                ORDER BY ingested_at DESC
            ) AS _row_num
        FROM filtered_raw
    ),

    cleaned AS (
        SELECT
            property_id,

            -- Convert UTC ISO string explicitly to Nairobi Local Time (EAT = UTC+3)
            from_utc_timestamp(CAST(scraped_at AS TIMESTAMP), 'Africa/Nairobi') AS scraped_at_eat,
            CAST(CAST(scraped_at AS TIMESTAMP) AS DATE) AS scraped_date,

            source_url,

            -- Text standards casing and trimming
            TRIM(location_raw) AS location_raw,
            INITCAP(TRIM(neighbourhood)) AS neighbourhood,
            latitude,
            longitude,
            ROUND(CAST(dist_to_archives_km AS NUMERIC(10,2)), 2) AS dist_to_archives_km,

            -- Value Segment Classifications
            CASE 
                WHEN dist_to_archives_km IS NULL THEN 'Unknown'
                WHEN price_ksh >= 80000 AND dist_to_archives_km <= 5.0 THEN 'Premium'
                WHEN price_ksh < 50000 AND dist_to_archives_km <= 8.0 THEN 'Sweet Spot'
                WHEN dist_to_archives_km > 15.0 THEN 'Commuter'
                ELSE 'Mid-range'
            END AS value_zone,

            price_raw,
            price_ksh,

            -- Graphical bucketing columns
            CASE
                WHEN price_ksh < 15000                  THEN 'Under 15k'
                WHEN price_ksh BETWEEN 15000 AND 29999  THEN '15k - 30k'
                WHEN price_ksh BETWEEN 30000 AND 49999  THEN '30k - 50k'
                WHEN price_ksh BETWEEN 50000 AND 79999  THEN '50k - 80k'
                WHEN price_ksh >= 80000                 THEN '80k+'
                ELSE 'Unknown'
            END AS price_bucket,

            -- Attribute bounds processing
            CASE WHEN bathrooms BETWEEN 0 AND 10 THEN bathrooms ELSE NULL END AS bathrooms,
            CASE WHEN bedrooms  BETWEEN 0 AND 20 THEN bedrooms  ELSE NULL END AS bedrooms,
            COALESCE(NULLIF(TRIM(property_type), ''), 'Unknown') AS property_type,
            COALESCE(NULLIF(TRIM(title), ''), 'No title') AS title,

            ingested_at,
            current_timestamp() AS batch_cleaned_at

        FROM deduplicated
        WHERE _row_num = 1   -- Keep only the most recent record per property_id, deduplication step
    )
    SELECT * FROM cleaned
""")

# CONVERT BACK TO DYNAMIC FRAME BEFORE DATA QUALITY SESSIONS
# SHIFT DATA FORMAT FROM SPARK DATAFRAME BACK TO AWS DYNAMICFRAME
# This line transforms your computed 'cleaned_df' (a Spark DataFrame) back into 
# an AWS Glue DynamicFrame pointer named 'cleaned_output_dyf'. 
# This format shift is required because standard Spark uses a slow, row-by-row JDBC 
# database driver, whereas Glue's DynamicFrame features a highly optimized native 
# Redshift bulk-upload connector.
    
    
cleaned_output_dyf = DynamicFrame.fromDF(cleaned_df, glueContext, "cleaned_output_dyf")

# =============================================================================
# STEP 5: COMPREHENSIVE DATA QUALITY EVALUATION
# =============================================================================
# 1.Create the ruleset using Data Quality Definition Language (DQDL).
cleaned_rentals_dq_ruleset = """
    Rules = [
        RowCount > 0,
        IsComplete "property_id",
        ColumnValues "price_ksh" between 3000 and 2000000,
        ColumnValues "value_zone" in [ "Premium", "Sweet Spot", "Commuter", "Mid-range", "Unknown" ],
        ColumnValues "price_bucket" in [ "Under 15k", "15k - 30k", "30k - 50k", "50k - 80k", "80k+", "Unknown" ]
    ]
"""
# 2.Validate the dataset against the ruleset.
dq_results_collection = EvaluateDataQuality().process_rows(
    frame=cleaned_output_dyf,
    ruleset=cleaned_rentals_dq_ruleset,
    publishing_options={
        "dataQualityEvaluationContext": "cleaned_output_dq",
        "enableDataQualityCloudWatchMetrics": True,
        "enableDataQualityResultsPublishing": True
    }
)
# Review the results.
# 1. Extract rowLevelOutcomes(data) and ruleOutcomes from collection
cleaned_output_dyf = dq_results_collection.select("rowLevelOutcomes")
dq_results_dyf  = dq_results_collection.select("ruleOutcomes")

# 2. Strip DQ annotation columns from data
# rowLevelOutcomes adds DQ annotation columns which must be stripped before writing to Redshift
# Strip DQ annotation columns — keep only the original data columns
cleaned_final_df = cleaned_output_dyf.toDF().select(
    "property_id", "scraped_at_eat", "scraped_date", "source_url", "location_raw",
    "neighbourhood", "latitude", "longitude", "dist_to_archives_km", "value_zone",
    "price_raw", "price_ksh", "price_bucket", "bedrooms", "bathrooms",
    "property_type", "title", "ingested_at", "batch_cleaned_at"
)

# 3. Convert back to DynamicFrame for Redshift write
cleaned_output_dyf = DynamicFrame.fromDF(cleaned_final_df, glueContext, "cleaned_final_dyf")


# 4. Convert results to DataFrame to check pass/fail
dq_results_df = dq_results_dyf.toDF()

# Fail safe — halt pipeline if any rule fails
failed_rules = dq_results_df.filter(dq_results_df.Outcome == "Failed").count()
if failed_rules > 0:
    dq_results_df.show(truncate=False)
    raise Exception(f"Pipeline halted: {failed_rules} data quality rule(s) failed.")

# =============================================================================
# STEP 6: SAVE SECURE CLEANED DATA TO BRONZE.CLEANED_RENTALS
# =============================================================================

# STREAM CLEANED BATCH DATA INTO REDSHIFT (THE ACTION TRIGGER)
# ***CRITICAL POINT: This is the formal Action that wakes up the entire cluster!
# Until this line executes, not a single raw data byte has been read from S3.
# Once called, the Driver generates the physical DAG plan, slices it into parallel 
# Tasks, and tells the Executors to read S3, apply the SQL logic, and push data.
glueContext.write_dynamic_frame.from_options(
    frame=cleaned_output_dyf,
    connection_type="redshift",
    connection_options={
        "useConnectionProperties": "true",
        "dbtable": "bronze.cleaned_batch_rentals_staging",  # writes to staging first
        "connectionName": args['REDSHIFT_CONNECTION'], # Network routing tunnel via Glue Data Catalog
        "redshiftTmpDir": args['TempDir'],  # A temporary S3 staging directory for the Redshift bulk upload process.
        "preactions": """
            DROP TABLE IF EXISTS bronze.cleaned_batch_rentals_staging;
            CREATE TABLE bronze.cleaned_batch_rentals_staging (
                property_id           VARCHAR(255),
                scraped_at_eat        TIMESTAMP,
                scraped_date          DATE,
                source_url            VARCHAR(max),
                location_raw          VARCHAR(2000),
                neighbourhood         VARCHAR(500),
                latitude              FLOAT8,
                longitude             FLOAT8,
                dist_to_archives_km   FLOAT8,
                value_zone            VARCHAR(50),
                price_raw             VARCHAR(1000),
                price_ksh             INTEGER,
                price_bucket          VARCHAR(50),
                bedrooms              SMALLINT,
                bathrooms             SMALLINT,
                property_type         VARCHAR(255),
                title                 VARCHAR(max),
                ingested_at           VARCHAR(50),
                batch_cleaned_at    TIMESTAMP
            );
        """,
        "postactions": """
            DELETE FROM bronze.cleaned_batch_rentals
            USING bronze.cleaned_batch_rentals_staging
            WHERE bronze.cleaned_batch_rentals.property_id 
                = bronze.cleaned_batch_rentals_staging.property_id;

            INSERT INTO bronze.cleaned_batch_rentals (
                property_id, scraped_at_eat, scraped_date, source_url, location_raw,
                neighbourhood, latitude, longitude, dist_to_archives_km, value_zone,
                price_raw, price_ksh, price_bucket, bedrooms, bathrooms,
                property_type, title, ingested_at, batch_cleaned_at
            )
            SELECT
                property_id, scraped_at_eat, scraped_date, source_url, location_raw,
                neighbourhood, latitude, longitude, dist_to_archives_km, value_zone,
                price_raw, price_ksh, price_bucket, bedrooms, bathrooms,
                property_type, title, ingested_at, batch_cleaned_at
            FROM bronze.cleaned_batch_rentals_staging;

            DROP TABLE bronze.cleaned_batch_rentals_staging;
        """
    }
)

# =============================================================================
# STEP 7: PIPELINE FINALIZATION & RESOURCE TEARDOWN
# =============================================================================

# 3. COMMIT THE GLUE JOB RUN
# The Driver confirms that all Executors successfully completed their writing Tasks.
# If AWS Glue Job Bookmarks are turned on, this line saves the processed S3 state 
# metadata so identical file records are never re-evaluated during future cron runs.
# Finally, it flags the job as a 'Success' and gracefully shuts down the cluster's 
# DPUs (Worker Nodes) to stop your active cloud billing immediately.
job.commit()
