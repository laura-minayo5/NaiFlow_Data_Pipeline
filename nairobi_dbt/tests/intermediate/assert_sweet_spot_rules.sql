-- This test checks for 'Sweet Spot' imposters.
-- It should return ZERO rows. If it returns rows, your logic is broken.

SELECT
    property_id,
    price_ksh,
    dist_to_archives_km,
    neighbourhood_median_ksh,
    is_sweet_spot
FROM {{ ref('int_rentals_enriched') }}
WHERE is_sweet_spot = TRUE
  AND (
    price_ksh >= 50000              -- Rule 1: Must be under 50k
    OR dist_to_archives_km > 8.0    -- Rule 2: Must be within 8km
    OR price_ksh > neighbourhood_median_ksh -- Rule 3: Cannot be above area median
  )