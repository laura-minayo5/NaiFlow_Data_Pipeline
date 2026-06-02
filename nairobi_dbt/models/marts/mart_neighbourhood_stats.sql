/*
    This model creates a comprehensive neighborhood-level summary of the Nairobi rental market.
    It uses the Medallion Architecture by pulling from the 'Silver' layer (int_rentals_enriched)
    and materializing the final result into the 'Gold' layer for reporting.
*/

{{ config(
    materialized='table', 
    schema='gold', 
    dist='all', 
    sort=['affordability_index']
) }}

-- Initial cleanup: Ensure we only analyze records with a valid neighborhood
WITH base AS (
    SELECT * FROM {{ ref('int_rentals_enriched') }}
    WHERE neighbourhood IS NOT NULL
),

/* 
    REDSHIFT OPTIMIZATION:
    We use separate CTEs for Median and Percentile calculations.
    Redshift requires all 'WITHIN GROUP' functions in a single block to have the 
    EXACT SAME 'ORDER BY' clause. Splitting them avoids "mismatched order" errors.
*/

-- CTE 1: Calculates General Price Distribution (IQR and Median)
general_stats AS (
    SELECT
        neighbourhood,
        -- The middle price point of the market
        ROUND(MEDIAN(price_ksh)) AS median_rent_ksh,
        -- p25 and p75 define the "typical" range, helping identify outliers
        ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY price_ksh)) AS p25_rent_ksh,
        ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY price_ksh)) AS p75_rent_ksh
    FROM base
    GROUP BY 1
),

-- CTE 2: Calculates Segment-Specific Medians (1-Bedroom only)
-- This is in its own CTE because the 'CASE' statement inside the MEDIAN 
-- creates a different data subset than the general stats above.
bedroom_stats AS (
    SELECT
        neighbourhood,
        ROUND(MEDIAN(CASE WHEN bedrooms = 1 THEN price_ksh END)) AS median_1br_rent_ksh
    FROM base
    GROUP BY 1
),

/*
    Main Aggregation Block:
    Joins the specialized statistical CTEs back to the base descriptive data.
*/
aggregated AS (
    SELECT
        b.neighbourhood,

        -- Geography & Context (MAX used as these are constant for the neighborhood)
        MAX(b.latitude) AS latitude,
        MAX(b.longitude) AS longitude,
        MAX(b.dist_to_archives_km) AS dist_to_archives_km,
        MAX(b.commute_tier) AS commute_tier,
        MAX(b.value_zone) AS value_zone,

        -- Price Spread: Combines Averages with Medians/Percentiles from our CTEs
        ROUND(AVG(b.price_ksh)) AS avg_rent_ksh,
        g.median_rent_ksh,
        MIN(b.price_ksh) AS min_rent_ksh,
        MAX(b.price_ksh) AS max_rent_ksh,
        g.p25_rent_ksh,
        g.p75_rent_ksh,
        -- Interquartile Range (IQR): A statistical measure of price volatility in an area
        (g.p75_rent_ksh - g.p25_rent_ksh) AS iqr_rent_ksh,

        -- Standardized Comparison: Average and Median specifically for 1-Bedroom units
        ROUND(AVG(CASE WHEN b.bedrooms = 1 THEN b.price_ksh END)) AS avg_1br_rent_ksh,
        s.median_1br_rent_ksh,

        -- Supply Metrics: Understanding the volume and types of available housing
        COUNT(*) AS total_listings,
        SUM(CASE WHEN b.bedrooms = 0   THEN 1 ELSE 0 END) AS studio_count,
        SUM(CASE WHEN b.bedrooms = 1   THEN 1 ELSE 0 END) AS listings_1br,
        SUM(CASE WHEN b.bedrooms = 2   THEN 1 ELSE 0 END) AS listings_2br,
        SUM(CASE WHEN b.bedrooms = 3   THEN 1 ELSE 0 END) AS listings_3br,
        -- 'Sweet Spots' are high-value/low-price listings identified in the Silver layer
        SUM(CASE WHEN b.is_sweet_spot  THEN 1 ELSE 0 END) AS sweet_spot_count,

        -- THE AFFORDABILITY INDEX:
        -- Normalizes rent (in 1000s) against distance from the CBD (Archives).
        -- A lower index indicates a neighborhood offers better central value.
        CASE
            WHEN MAX(b.dist_to_archives_km) > 0
            THEN ROUND((AVG(b.price_ksh) / 1000.0) / MAX(b.dist_to_archives_km), 2)
            ELSE NULL
        END AS affordability_index,

        -- Market Quality: Percent of total listings that meet the 'Sweet Spot' criteria
        ROUND(100.0 * SUM(CASE WHEN b.is_sweet_spot THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) AS sweet_spot_pct,

        -- Metadata for tracking data freshness
        MIN(b.scraped_date) AS first_seen,
        MAX(b.scraped_date) AS last_seen,
        COUNT(DISTINCT b.scraped_date) AS days_observed,
        GETDATE() AS dbt_updated_at

    FROM base b
    JOIN general_stats g ON b.neighbourhood = g.neighbourhood
    JOIN bedroom_stats s ON b.neighbourhood = s.neighbourhood
    GROUP BY b.neighbourhood, g.median_rent_ksh, g.p25_rent_ksh, g.p75_rent_ksh, s.median_1br_rent_ksh
)

-- Final Selection with Analytical Rankings
SELECT
    *,
    -- RANK 1 = Best value for money based on index
    RANK() OVER (ORDER BY affordability_index ASC NULLS LAST) AS value_rank,
    -- RANK 1 = Most expensive neighborhood in Nairobi
    RANK() OVER (ORDER BY avg_rent_ksh DESC NULLS LAST) AS price_rank,
    -- RANK 1 = Area with the highest number of available rentals
    RANK() OVER (ORDER BY total_listings DESC NULLS LAST) AS supply_rank
FROM aggregated
ORDER BY affordability_index ASC NULLS LAST