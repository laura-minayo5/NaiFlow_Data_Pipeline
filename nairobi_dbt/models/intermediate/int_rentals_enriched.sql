-- =============================================================================
-- models/intermediate/int_rentals_enriched.sql
-- LAYER:  Silver (view)
-- SOURCE: silver.stg_rentals
--
-- PURPOSE:
--   Adds three categories of business logic on top of stg_rentals.
--   Every gold model reads from here instead of stg_rentals directly.
--
--  Granularity: One row per individual rental listing
--
-- WHAT IT ADDS (and why):
--
--   1. commute_tier
--      Converts raw dist_to_archives_km (a number like 4.3)
--      into a human label ("2 - Short (2–5 km)").
--      The numeric prefix makes Power BI sort the labels correctly.
--
--   2. location_segment
--      Defines a "neighborhood type" based on distance from the CBD, which is a common way Nairobi residents think about location.
--
--   3. is_sweet_spot
--      The "Sweet Spot" logic is a high-value listing that balances cost, space, and location:
--      - Cost and Location: Must be in the "Sweet Spot" value_zone, which already factors in price and distance to CBD
--      - Space: Must be a "livable" unit, not a single room/bedsitter (size_sqm >= 30 OR bedrooms >= 1)
--      - Value: Must be at or below the typical price for that specific area
--
--   4. bedroom_label
--      Converts raw bedrooms (0, 1, 2 ...) into display labels
--      ("Studio", "1 BR", "2 BR" ...) for chart legends.
--       Studios are a common type of listing in Nairobi, so we give them their own label instead of lumping them in with 1 BRs.
--
--   5. pct_vs_neighbourhood_median
--      Shows whether a listing is cheap or expensive relative to
--      its own neighbourhood. Needs a sub-query to get the median first.
--      Positive = above median (expensive for the area).
--      Negative = below median (bargain for the area).

{{ config(materialized='view', schema='silver') }}

WITH base AS (

    SELECT * FROM {{ ref('stg_rentals') }}
    WHERE price_ksh IS NOT NULL -- We need price_ksh to calculate the neighbourhood median, so exclude rows where it's missing.

),

-- Calculate the median price per neighbourhood once.
-- We need this before we can compute each listing's % vs median.
neighbourhood_medians AS (

    SELECT
        neighbourhood,
        MEDIAN(price_ksh) AS neighbourhood_median_ksh
    FROM base
    WHERE neighbourhood IS NOT NULL
    GROUP BY neighbourhood

)

SELECT
    -- All columns from stg_rentals
    b.*,
    -- Keep the raw median value too — useful for tooltips in Power BI
    nm.neighbourhood_median_ksh,

    -- 1. Commute Tier (Travel Focus)
    -- Defines how the commute feels for a Nairobi resident
    -- Numeric prefix (1–5) makes Power BI sort the labels in the right order
    -- without needing a custom sort column.
    CASE
        WHEN b.dist_to_archives_km <= 2.0  THEN '1 - Walking (0-2 km)'
        WHEN b.dist_to_archives_km <= 5.0  THEN '2 - Short (2-5 km)'
        WHEN b.dist_to_archives_km <= 8.0  THEN '3 - Medium (5-8 km)'
        WHEN b.dist_to_archives_km <= 15.0 THEN '4 - Long (8-15 km)'
        WHEN b.dist_to_archives_km >  15.0 THEN '5 - Far (15+ km)'
        ELSE 'Unknown'
    END AS commute_tier,

    -- 2. Location Segment (Ring Focus)
    -- Defines the "Neighborhood Type" based on distance from the CBD
    CASE
        WHEN b.dist_to_archives_km <= 5.0  THEN 'Core CBD / Inner Ring'
        WHEN b.dist_to_archives_km <= 10.0 THEN 'Mid-Ring Suburb'
        WHEN b.dist_to_archives_km <= 15.0 THEN 'Commuter Zone'
        ELSE 'Satellite Town'
    END AS location_segment,

    -- 3. The "Sweet Spot" Logic (Value Focus)
    CASE
    WHEN 
        -- 1. -- Cost and Location: Must be in the "Sweet Spot" value_zone, which already factors in price and distance to CBD
        b.value_zone = 'Sweet Spot'
        
        -- 2. Space/Value Hybrid: 
        -- Accepts any 1+ bedroom unit OR affordable studios (<35k)
        AND (b.bedrooms >= 1 OR (b.bedrooms = 0 AND b.price_ksh < 35000))
        
        -- 3. Competitive Pricing: Must be at or below the neighborhood median
        AND b.price_ksh <= nm.neighbourhood_median_ksh
    THEN TRUE 
    ELSE FALSE 
    END AS is_sweet_spot,

    -- 4. Bedroom labels
    CASE
        WHEN b.bedrooms = 0  THEN 'Studio'
        WHEN b.bedrooms = 1  THEN '1 BR'
        WHEN b.bedrooms = 2  THEN '2 BR'
        WHEN b.bedrooms = 3  THEN '3 BR'
        WHEN b.bedrooms >= 4 THEN '4+ BR'
        ELSE 'Unknown'
    END AS bedroom_label,

    -- 5. Price vs neighbourhood median
    -- How does this listing compare to the typical price in its area?
    -- Example: The median price in Kilimani is 85,000 KSh. You find an apartment in Kilimani for 70,000 KSh.
    -- The Calculation: $(70,000 - 85,000) = -15,000$.The Result: $-15,000 / 85,000 = -17.6\%$.
    -- NULL when we have no median to compare against (unknown neighbourhood).
    -- A negative percentage means the listing is cheaper than the median for its area (a potential bargain).
    -- A positive percentage means the listing is more expensive than the median for its area (a potential rip-off).
    CASE
        WHEN nm.neighbourhood_median_ksh > 0
        THEN ROUND(
                (b.price_ksh - nm.neighbourhood_median_ksh) * 100.0
                / nm.neighbourhood_median_ksh,
                1
             )
        ELSE NULL
    END AS price_vs_neighbourhood_median

FROM base b
LEFT JOIN neighbourhood_medians nm
    ON b.neighbourhood = nm.neighbourhood