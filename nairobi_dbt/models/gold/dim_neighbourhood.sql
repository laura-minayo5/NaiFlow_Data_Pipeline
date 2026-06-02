-- models/gold/dim_neighbourhood.sql
-- LAYER:  Gold (table)
-- SOURCE: silver.int_rentals_enriched
--
-- PURPOSE:
--   One row per neighbourhood with all static descriptive attributes.
--   Fact tables JOIN to this for neighbourhood labels, coordinates,
--   commute_tier, value_zone, and affordability_index.
{{ config(materialized='table', schema='gold', dist='ALL', sort=['neighbourhood']) }}


WITH base AS (
    SELECT * FROM {{ ref('int_rentals_enriched') }}
    WHERE neighbourhood IS NOT NULL
)


SELECT
    -- The Grain: One row per neighbourhood pk: neighbourhood
    neighbourhood,

    -- Geography (The "Max" Trick)
    -- Since we assume that every listing in neighbourhood has the same coordinates and distance to archives,
    -- MAX() just grabs that one value for our dimension table.
    -- future improvement could be using GPS lookup to get actual neighbourhood centroids instead of relying on property listing data for coordinates
    -- as many property listing sites often don't provide a public latitude/longitude for every listing to prevent "lead poaching" (where people bypass the agent).
    MAX(latitude) AS latitude,
    MAX(longitude) AS longitude,
    MAX(dist_to_archives_km) AS dist_to_archives_km,

    -- Classification
    MAX(commute_tier) AS commute_tier,
    MAX(value_zone)  AS value_zone,

    -- Affordability Index
    -- This helps you rank neighbourhoods by "bang for buck" relative to the CBD.
    -- (avg rent / 1000) / distance — lower = better value relative to CBD
    -- We divide by 1000 just to keep the number in a more human-friendly range (e.g. 1.5 instead of 1500).
    CASE
        WHEN MAX(dist_to_archives_km) > 0
        THEN ROUND(
                (AVG(price_ksh) / 1000.0) / MAX(dist_to_archives_km),
                2
             )
        ELSE NULL
    END AS affordability_index,

    -- Metadata
    COUNT(*) AS total_listings_count,
    MIN(scraped_date) AS first_listing_date,
    MAX(scraped_date) AS last_update_date,
    GETDATE() AS dbt_updated_at

FROM base
GROUP BY neighbourhood
ORDER BY neighbourhood 

