-- This test finds cases where a larger house is significantly cheaper
-- than a smaller house in the SAME neighbourhood on average.
WITH aggregated_room_prices AS (
    SELECT
        neighbourhood,
        bedroom_label,
        AVG(price_ksh) as avg_rent_ksh
    FROM {{ ref('int_rentals_enriched') }}
    WHERE price_ksh IS NOT NULL
      AND bedroom_label IS NOT NULL
    GROUP BY neighbourhood, bedroom_label
),

local_benchmarks AS (
    SELECT
        neighbourhood,
        bedroom_label,
        avg_rent_ksh,
        -- Correctly find the average 1-BR price for this specific neighbourhood
        MAX(CASE WHEN bedroom_label = '1 BR' THEN avg_rent_ksh END) OVER (PARTITION BY neighbourhood) as local_1br_avg_price
    FROM aggregated_room_prices
)

SELECT * FROM local_benchmarks
WHERE bedroom_label IN ('3 BR', '4+ BR')
  AND local_1br_avg_price IS NOT NULL
  AND avg_rent_ksh < (local_1br_avg_price * 0.8) -- Fails if 3/4 BR average is >20% cheaper than 1 BR average