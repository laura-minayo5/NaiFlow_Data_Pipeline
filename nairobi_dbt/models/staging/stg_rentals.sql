-- =============================================================================
-- models/staging/stg_rentals.sql
-- LAYER:  Bronze (view)
-- SOURCE: bronze.raw_rentals
-- PURPOSE:
--   Step 1 of 3 in the transformation chain.
--   Deduplicate → cast types → clean prices → add derived columns.
--   Every downstream model reads from this view, never from bronze directly.
-- =============================================================================

{{ config(materialized='view', schema='bronze') }}
-- selects all columns from the "raw_rentals" table in the "bronze" schema, which is defined as a source in dbt source.yml.
WITH raw_data AS (

    SELECT * FROM {{ source('bronze', 'raw_rentals') }}

),


-- STEP 1: Apply the quality gate immediately to discard useless payloads
filtered_raw AS (

    SELECT * 
    FROM raw_data
    WHERE property_id IS NOT NULL
      AND TRIM(property_id) <> ''
      -- Quality gate: The record MUST have at least a location or a price to be valuable
      AND location_raw IS NOT NULL -- Reject records with no location data (unusable for geospatial analysis)
      AND price_ksh IS NOT NULL -- Reject records with no price data (unusable for pricing analysis)
      AND price_ksh BETWEEN 3000 AND 2000000 -- Reject implausible prices (below cheapest Nairobi bedsit, above penthouse)
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
            ORDER BY firehose_arrival_at DESC
        ) AS _row_num
    FROM filtered_raw
 
),

-- STEP 3: Cast, clean, and derive final schemas
cleaned AS (

    SELECT
        property_id,

        -- scraped_at arrives as a VARCHAR ISO string — cast to proper timestamp
        -- Convert to Nairobi local time (EAT = UTC+3) for easier analysis and reporting
        CONVERT_TIMEZONE('UTC', 'Africa/Nairobi',
            TRY_CAST(scraped_at AS TIMESTAMP))  AS scraped_at_eat,

        -- 
        CAST(TRY_CAST(scraped_at AS TIMESTAMP) AS DATE) AS scraped_date,

        source_url,

        -- LOCATION: Clean text, enforce consistent casing, and reject implausible coordinates
        TRIM(location_raw)  AS location_raw,
        INITCAP(TRIM(neighbourhood)) AS neighbourhood,
        latitude,
        longitude,
        ROUND(CAST(dist_to_archives_km AS NUMERIC), 2) AS dist_to_archives_km,

        
        
        CASE 
            -- Catch missing data first
            WHEN price_ksh IS NULL OR dist_to_archives_km IS NULL THEN 'Unknown'
            WHEN price_ksh >= 80000 AND dist_to_archives_km <= 5.0 THEN 'Premium'
            WHEN price_ksh < 50000 AND dist_to_archives_km <= 8.0 THEN 'Sweet Spot'
            WHEN dist_to_archives_km > 15.0 THEN 'Commuter'
            ELSE 'Mid-range'
        END AS value_zone,

        -- Price 
        price_raw,

        price_ksh,

        -- Categorical price bucket for bar charts
        CASE
            WHEN price_ksh < 15000                  THEN 'Under 15k'
            WHEN price_ksh BETWEEN 15000 AND 29999  THEN '15k - 30k'
            WHEN price_ksh BETWEEN 30000 AND 49999  THEN '30k - 50k'
            WHEN price_ksh BETWEEN 50000 AND 79999  THEN '50k - 80k'
            WHEN price_ksh >= 80000                 THEN '80k+'
            ELSE 'Unknown'
        END AS price_bucket,

        -- Property attributes 
        CASE WHEN bedrooms  BETWEEN 0 AND 20 THEN bedrooms  ELSE NULL END AS bedrooms,
        CASE WHEN bathrooms BETWEEN 0 AND 10 THEN bathrooms ELSE NULL END AS bathrooms,
        COALESCE(NULLIF(TRIM(property_type), ''), 'Unknown') AS property_type,
        -- CASE WHEN size_sqm BETWEEN 10 AND 5000 THEN size_sqm ELSE NULL END AS size_sqm,

        -- -- Price per sqm — true value metric
        -- CASE
        --     WHEN price_ksh BETWEEN 3000 AND 2000000
        --      AND size_sqm  BETWEEN 10   AND 5000
        --     THEN ROUND(CAST(price_ksh AS NUMERIC) / size_sqm, 0)
        --     ELSE NULL
        -- END AS price_per_sqm,

        COALESCE(NULLIF(TRIM(title), ''), 'No title') AS title,

        -- Pipeline metadata
        firehose_arrival_at,
        ingested_at

    FROM deduplicated
    WHERE _row_num = 1 -- Keep only the most recent fresh record per property_id

)

SELECT * FROM cleaned