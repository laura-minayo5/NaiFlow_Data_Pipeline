-- INTERMEDIATE LAYER REASONABILITY TEST
-- =============================================================================
-- If a row has a neighborhood, it MUST successfully join to calculate a median.
-- If this test returns rows, it means the LEFT JOIN failed to match, likely due 
-- to unresolved trailing spaces or character encoding bugs in staging.

SELECT 
    property_id,
    neighbourhood,
    price_ksh,
    neighbourhood_median_ksh
FROM {{ ref('int_rentals_enriched') }}
WHERE neighbourhood IS NOT NULL 
  AND neighbourhood_median_ksh IS NULL
