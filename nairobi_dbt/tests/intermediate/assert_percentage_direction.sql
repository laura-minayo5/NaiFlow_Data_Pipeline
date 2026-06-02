-- Validates direction and basic math boundaries of the price variance calculation.
-- Returns rows where the percentage sign contradicts the actual price vs median relationship.

SELECT
    property_id,
    price_ksh,
    neighbourhood_median_ksh,
    price_vs_neighbourhood_median
FROM {{ ref('int_rentals_enriched') }}
WHERE neighbourhood_median_ksh IS NOT NULL 
  AND price_ksh IS NOT NULL
  AND (
    (price_ksh > neighbourhood_median_ksh AND price_vs_neighbourhood_median <= 0)
    OR 
    (price_ksh < neighbourhood_median_ksh AND price_vs_neighbourhood_median >= 0)
    OR 
    (price_ksh = neighbourhood_median_ksh AND price_vs_neighbourhood_median != 0)
  )
