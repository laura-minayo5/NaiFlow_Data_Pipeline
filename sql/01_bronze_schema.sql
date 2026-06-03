-- create table for Kinesis Firehose Delivery Stream
-- create a custom schema to keep things organized
-- =============================================================================
-- FILE: sql/01_bronze_schema.sql
-- PURPOSE: Bronze layer — raw table that Kinesis Firehose writes into via COPY
-- RUN THIS ONCE in Redshift Serverless Query Editor before anything else
--
-- HOW FIREHOSE WRITES HERE:
--   Firehose buffers Kinesis records → dumps NDJSON files to S3 →
--   triggers a Redshift COPY command automatically into this table

-- ---------------------------------------------------------------------------
-- 1. Create schemas
-- ---------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;


-- ---------------------------------------------------------------------------
-- 2. Bronze raw_rentals table
--    One row per Kinesis record delivered by Firehose.
--    Columns match exactly what client.py sends in the JSON payload.
--    All columns are VARCHAR or nullable — we never reject data at this layer.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.raw_rentals (

    -- Surrogate key generated at insert time
    -- This creates a simple auto-incrementing number (1, 2, 3...)
    id                    INT IDENTITY(1,1),

    -- ── Identity fields (from client.py transform_listing) ─────────────────
    property_id           VARCHAR(255),
    scraped_at            VARCHAR(50),      -- ISO string e.g. "2025-01-15T14:23:01+00:00"
    source_url            VARCHAR(max),

    -- ── Location ────────────────────────────────────────────────────────────
    location_raw          VARCHAR(2000),     -- raw text from Property24, never null in valid records
    neighbourhood         VARCHAR(500),     -- fuzzy-matched name e.g. "Westlands"
    latitude              FLOAT8,
    longitude             FLOAT8,
    dist_to_archives_km   FLOAT8,           -- Haversine distance calculated by client.py

    -- ── Price ───────────────────────────────────────────────────────────────
    price_raw             VARCHAR(1000),     -- original string e.g. "KSh 55,000 per month"
    price_ksh             INTEGER,          -- cleaned integer e.g. 55000

    -- ── Property attributes ─────────────────────────────────────────────────
    bedrooms              SMALLINT,
    bathrooms             SMALLINT,
    property_type         VARCHAR(255),     -- Apartment | House | Studio | Unknown
    title                 VARCHAR(max),

    -- ── Pipeline metadata ───────────────────────────────────────────────────
    ingested_at           VARCHAR(50),      -- stamped by Lambda pipeline_1_ingestion
    firehose_arrival_at   TIMESTAMP DEFAULT GETDATE()

)
DISTSTYLE AUTO      --Tells Redshift where to put the data
SORTKEY (firehose_arrival_at, neighbourhood);



-- ================================================================================
-- TABLE NAME: bronze.raw_batch_rentals
-- ================================================================================
-- PURPOSE:
-- --------
-- This table acts as the dedicated landing area for Pipeline 4 (Batch Processing) 
-- of the Nairobi Rental Data Pipeline. It ingests large-scale historical uploads, 
-- bulk daily snapshots, or retrospective scraper dumps extracted from S3 via 
-- AWS Glue ETL jobs.

-- WHY THIS SEPARATE BATCH TABLE EXISTS (Instead of writing to streaming tables):
-- ----------------------------------------------------------------------------
-- 1. Isolation of Concerns: Keeps high-velocity streaming entries (from Firehose) 
--    isolated from heavy bulk data inserts (from Glue). This protects the streaming 
--    pipeline from lock contention during massive batch writes.
-- 2. Data Auditing: Allows us to verify, troubleshoot, and benchmark batch-loaded 
--    data independently before it undergoes reconciliation.
-- 3. Deduplication Prep: Serves as a raw staging layer. In the subsequent Silver 
--    Tier transformations, a deduplication script will merge this table with the 
--    streaming data table, resolving overlapping records based on `property_id`.

-- DATA ARCHITECTURE STRATEGY:
-- ---------------------------
-- - Schema Layer: Bronze (Raw, unstructured text and foundational primitive casts).
-- - Target Database: dev
-- - Storage Model: Managed Columnar (Optimized for deep analytical queries).
-- ================================================================================
CREATE TABLE IF NOT EXISTS bronze.raw_batch_rentals (

    -- Surrogate key generated at insert time (auto-incrementing unique identifier)
    id                    INT IDENTITY(1,1),

    -- ── Identity fields (from scraper payload) ─────────────────────────────
    property_id           VARCHAR(255),
    scraped_at            VARCHAR(50),      -- ISO string format e.g. "2026-05-14T12:00:00+00:00"
    source_url            VARCHAR(max),     -- Handles long, deeply-nested web links

    -- ── Location ────────────────────────────────────────────────────────────
    location_raw          VARCHAR(2000),     
    neighbourhood         VARCHAR(500),     -- Primary filter for rental data queries
    latitude              FLOAT8,
    longitude             FLOAT8,
    dist_to_archives_km   FLOAT8,           

    -- ── Price ───────────────────────────────────────────────────────────────
    price_raw             VARCHAR(1000),     
    price_ksh             INTEGER,          

    -- ── Property attributes ─────────────────────────────────────────────────
    bedrooms              SMALLINT,
    bathrooms             SMALLINT,
    property_type         VARCHAR(255),     -- Apartment | House | Studio | Unknown
    title                 VARCHAR(max),

    -- ── Pipeline Metadata (Tracking batch updates) ──────────────────────────
    ingested_at           VARCHAR(50),      -- Stamped during initial landing
    batch_loaded_at    TIMESTAMP DEFAULT GETDATE() -- Stamped automatically when Glue writes the row

)
-- ── Distribution and Sorting Optimization ──────────────────────────────────
DISTSTYLE KEY
DISTKEY (property_id)
COMPOUND SORTKEY (batch_loaded_at, neighbourhood);



CREATE TABLE IF NOT EXISTS bronze.cleaned_batch_rentals (

    -- ── Identity ─────────────────────────────────────────────────────────────
    property_id           VARCHAR(255),

    -- ── Timestamps ───────────────────────────────────────────────────────────
    scraped_at_eat        TIMESTAMP,        -- UTC scraped_at converted to Nairobi time (EAT UTC+3)
    scraped_date          DATE,             -- Date portion of scraped_at for easy filtering

    -- ── Source ───────────────────────────────────────────────────────────────
    source_url            VARCHAR(max),

    -- ── Location ─────────────────────────────────────────────────────────────
    location_raw          VARCHAR(2000),
    neighbourhood         VARCHAR(500),
    latitude              FLOAT8,
    longitude             FLOAT8,
    dist_to_archives_km   FLOAT8,

    -- ── Value Classification ──────────────────────────────────────────────────
    value_zone            VARCHAR(50),      -- Premium | Sweet Spot | Commuter | Mid-range | Unknown
    
    -- ── Price ────────────────────────────────────────────────────────────────
    price_raw             VARCHAR(1000),
    price_ksh             INTEGER,
    price_bucket          VARCHAR(50),      -- Under 15k | 15k - 30k | 30k - 50k | 50k - 80k | 80k+

    -- ── Property attributes ───────────────────────────────────────────────────
    bedrooms              SMALLINT,
    bathrooms             SMALLINT,
    property_type         VARCHAR(255),
    title                 VARCHAR(max),

    -- ── Pipeline Metadata ─────────────────────────────────────────────────────
    ingested_at           VARCHAR(50),      -- Stamped during initial S3 landing
    batch_cleaned_at      TIMESTAMP DEFAULT GETDATE()  -- Stamped automatically when Glue writes the row

)
DISTSTYLE KEY
DISTKEY (property_id)
COMPOUND SORTKEY (batch_cleaned_at, neighbourhood);


-- Quick sanity check after creation
-- Query the Metadata
SELECT
    table_schema,
    table_name,
    column_name,
    data_type
FROM information_schema.columns --Information Schema is automated table of contents views, for your entire warehouse.
WHERE table_schema = 'bronze'
  AND table_name   = 'raw_batch_rentals'
ORDER BY ordinal_position;