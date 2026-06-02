-- Validates that commute_tier and location_segment metrics do not contradict each other.
-- Returns rows where mapping logic is mismatched.

SELECT
    property_id,
    dist_to_archives_km,
    commute_tier,
    location_segment
FROM {{ ref('int_rentals_enriched') }}
WHERE (commute_tier IN ('1 - Walking (0-2 km)', '2 - Short (2–5 km)') AND location_segment != 'Core CBD / Inner Ring')
   OR (commute_tier = '3 - Medium (5-8 km)' AND location_segment NOT IN ('Core CBD / Inner Ring', 'Mid-Ring Suburb'))
   OR (commute_tier = '5 - Far (15+ km)' AND location_segment = 'Core CBD / Inner Ring')
