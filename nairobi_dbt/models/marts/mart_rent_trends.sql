{# 'all' is best for smaller summary tables so they are available on every node for instant joining #}
{# Sorting by neighbourhood then date optimizes the LAG() window function performance #}
{{ config(
    materialized='table', 
    schema='gold', 
    dist='all', 
    sort=['neighbourhood', 'month_start']
) }}

-- STEP 1: Aggregate daily listings into Monthly buckets per neighbourhood
WITH monthly_metrics AS (
    SELECT
        d.month_start,
        d.month_year,
        d.month_name,
        d.year,
        f.neighbourhood,
        n.value_zone,
        n.commute_tier,
        COUNT(*) AS listing_count,
        ROUND(AVG(f.price_ksh)) AS avg_rent_ksh,
        ROUND(MEDIAN(f.price_ksh)) AS median_rent_ksh,
        MIN(f.price_ksh) AS min_rent_ksh,
        MAX(f.price_ksh) AS max_rent_ksh,
        ROUND(AVG(CASE WHEN f.bedrooms = 1 THEN f.price_ksh END)) AS avg_1br_rent_ksh,
        SUM(CASE WHEN f.is_sweet_spot THEN 1 ELSE 0 END) AS sweet_spot_count

    FROM {{ ref('fct_rental_listings') }} f
    JOIN {{ ref('dim_date') }}  d ON f.scraped_date = d.date_day
    JOIN {{ ref('dim_neighbourhood') }} n ON f.neighbourhood = n.neighbourhood
    GROUP BY
        d.month_start, d.month_year, d.month_name, d.year,
        f.neighbourhood, n.value_zone, n.commute_tier
),

-- STEP 2: Use Window Functions to "look back" at the previous month
with_lag AS (
    SELECT
        *,
        LAG(median_rent_ksh) OVER (
            PARTITION BY neighbourhood 
            ORDER BY month_start
        ) AS prev_month_median,

        LAG(listing_count) OVER (
            PARTITION BY neighbourhood 
            ORDER BY month_start
        ) AS prev_month_count
    FROM monthly_metrics
)

-- STEP 3: Final calculations for Month-over-Month (MoM) growth
SELECT
    *,
    median_rent_ksh - prev_month_median AS mom_rent_change_ksh,
    CASE
        WHEN prev_month_median > 0
        THEN ROUND(
                (median_rent_ksh - prev_month_median) * 100.0
                / prev_month_median, 1
             )
        ELSE NULL 
    END AS mom_rent_change_pct,
    CASE 
        WHEN listing_count >= 15 THEN 'High (Robust)'
        WHEN listing_count >= 5  THEN 'Medium (Stable)'
        ELSE 'Low (Volatile/Small Sample)'
    END AS data_confidence_score

FROM with_lag
ORDER BY neighbourhood, month_start