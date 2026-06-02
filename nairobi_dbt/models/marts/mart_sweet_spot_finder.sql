/*
    mart_sweet_spot_finder.sql
    ==========================
    PURPOSE:
    This is the "Project Star" model — the headline deliverable of the entire pipeline.
    It answers the core question: "Where is the best place to rent in Nairobi right now?"

    It ranks every neighbourhood using a composite weighted score that balances
    three dimensions of value:
      - Price (50% weight)    → Is it cheap?
      - Distance (30% weight) → Is it close to CBD?
      - Consistency (20% weight) → Are prices predictable (low IQR)?

    GRANULARITY:
    One row per neighbourhood. Only neighbourhoods with at least 3 listings
    are included — fewer than 3 makes the statistics unreliable.

    SOURCE:
    Reads from mart_neighbourhood_stats (Gold layer) — we reuse the aggregations
    already calculated there rather than recalculating from scratch.

    COMPOSITE SCORE FORMULA:
    composite_score = (0.5 × price_rank) + (0.3 × distance_rank) + (0.2 × consistency_rank)
    Lower score = better value. Rank 1 = best value neighbourhood in Nairobi.

    POWER BI USAGE:
    - Table visual showing overall_value_rank, neighbourhood, verdict
    - Bar chart showing affordability_index by neighbourhood
    - KPI card showing the #1 ranked neighbourhood
*/

{{ config(
    materialized='table',
    schema='gold',
    dist='all'
) }}

-- Step 1: Filter to neighbourhoods with enough data to be statistically meaningful
WITH qualified AS (

    SELECT *
    FROM {{ ref('mart_neighbourhood_stats') }}
    -- Need at least 3 listings — fewer makes median and percentile unreliable
    WHERE total_listings >= 3

),

-- Step 2: Rank each neighbourhood on three individual dimensions
-- Each rank is independent: price_rank=1 means cheapest, distance_rank=1 means closest to CBD
ranked AS (

    SELECT
        -- Identity
        neighbourhood,
        value_zone,
        commute_tier,

        -- Geography
        dist_to_archives_km,
        latitude,
        longitude,

        -- Core price metrics
        avg_rent_ksh,
        median_rent_ksh,
        avg_1br_rent_ksh,
        median_1br_rent_ksh,

        -- Supply metrics
        total_listings,
        listings_1br,
        sweet_spot_count,
        sweet_spot_pct,

        -- Value metric
        affordability_index,
        iqr_rent_ksh,

        -- Temporal
        first_seen,
        last_seen,
        dbt_updated_at,

        -- Component ranks — each dimension ranked independently
        -- NULLS LAST ensures missing data sinks to the bottom, not the top

        -- Price rank: 1 = cheapest neighbourhood (best for renters on a budget)
        RANK() OVER (
            ORDER BY median_rent_ksh ASC NULLS LAST
        ) AS price_rank,

        -- Distance rank: 1 = closest to CBD (best for commuters)
        RANK() OVER (
            ORDER BY dist_to_archives_km ASC NULLS LAST
        ) AS distance_rank,

        -- Consistency rank: 1 = lowest IQR = most predictable pricing
        -- A low IQR means you know what to expect when searching in this area
        RANK() OVER (
            ORDER BY iqr_rent_ksh ASC NULLS LAST
        ) AS consistency_rank,

        -- Supply rank: 1 = most listings (most options available)
        RANK() OVER (
            ORDER BY total_listings DESC NULLS LAST
        ) AS supply_rank

    FROM qualified

),

-- Step 3: Calculate composite score and overall ranking
scored AS (

    SELECT
        *,

        -- COMPOSITE SCORE
        -- Weighted average of the three component ranks
        -- Weights reflect what matters most to a Nairobi renter:
        --   Price (50%)       — most renters are budget-driven
        --   Distance (30%)    — commute matters but can be tolerated
        --   Consistency (20%) — predictability is a nice-to-have
        ROUND(
            (0.5 * price_rank) +
            (0.3 * distance_rank) +
            (0.2 * consistency_rank),
            2
        ) AS composite_score

    FROM ranked

)

-- Step 4: Final output with overall ranking and human-readable verdict
SELECT
    *,

    -- OVERALL VALUE RANK
    -- 1 = best value neighbourhood in all of Nairobi
    RANK() OVER (
        ORDER BY composite_score ASC NULLS LAST
    ) AS overall_value_rank,

    -- VERDICT
    -- Human-readable label for Power BI card visuals and colour coding
    -- Priority order matters — more specific conditions checked first
    CASE
        -- Must be Sweet Spot zone, majority of listings qualify, AND close to CBD
        WHEN sweet_spot_pct >= 60
         AND dist_to_archives_km <= 8
            THEN 'Project Star'

        -- Close to CBD but expensive — good location, high cost
        WHEN dist_to_archives_km <= 5
         AND median_rent_ksh >= 50000
            THEN 'Premium Location'

        -- Close AND affordable — great value, just not dominant sweet spot %
        WHEN dist_to_archives_km <= 8
         AND median_rent_ksh < 40000
            THEN 'Good Value'

        -- Far from CBD but very cheap — trade commute for savings
        WHEN dist_to_archives_km > 12
         AND median_rent_ksh < 20000
            THEN 'Budget Commuter'

        -- Everything else
        ELSE 'Average'
    END AS verdict

FROM scored
ORDER BY overall_value_rank ASC