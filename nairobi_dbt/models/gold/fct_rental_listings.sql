-- The "grain" of this table is one row per rental listing, per day.
{{ config(materialized='table', schema='gold', dist='AUTO', sort=['scraped_date', 'neighbourhood']) }}

WITH enriched_data AS (
    SELECT * FROM {{ ref('int_rentals_enriched') }}
)

SELECT
    -- Surrogate / Natural Keys
    property_id,

    -- Foreign Key → dim_date (Join on date_day)
    CAST(scraped_date AS DATE) AS scraped_date,

    -- Foreign Key → dim_neighbourhood (Join on neighbourhood)
    CAST(neighbourhood AS VARCHAR(50)) AS neighbourhood,

    -- ── Measures (The "Facts" - what we are analyzing)
    CAST(price_ksh AS DECIMAL(12,2)) AS price_ksh,
    CAST(price_vs_neighbourhood_median AS DECIMAL(10,4)) AS price_vs_neighbourhood_median,

    -- Descriptive attributes specific to the HOUSE 
    bedrooms,
    bathrooms,
    bedroom_label,
    property_type,
    price_bucket,
    is_sweet_spot,

    -- Degenerate Dimensions (Identifiers)
    title,
    source_url,
    location_raw,

    -- ── Audit Metadata
    scraped_at_eat,
    firehose_arrival_at,
    GETDATE() AS dbt_loaded_at

FROM enriched_data
WHERE neighbourhood IS NOT NULL 
  AND scraped_date IS NOT NULL