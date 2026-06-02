-- tests/marts/assert_sweet_spot_verdict_logic_correct.sql
-- ===================================================
-- PURPOSE:
--   Verifies that the verdict column follows the CASE logic exactly.
--   Tests two of the most important verdicts:
--
--   1. Any row with verdict = 'Project Star' must have
--      sweet_spot_pct >= 60 AND dist_to_archives_km <= 8
--
--   2. Any row with verdict = 'Premium Location' must have
--      dist_to_archives_km <= 5 AND median_rent_ksh >= 50000
--
-- WHY THIS MATTERS:
--   'Project Star' is the headline label shown in Power BI.
--   If the CASE logic in the model changes, a neighbourhood could be
--   incorrectly labelled as Project Star without meeting the criteria.
--   This test acts as a business rule contract.
--
-- Returns violating rows — test FAILS if any come back.

-- Check 1: Project Star rows must meet the Project Star criteria
SELECT
    neighbourhood,
    verdict,
    sweet_spot_pct,
    dist_to_archives_km,
    'Project Star criteria violated' AS failure_reason
FROM {{ ref('mart_sweet_spot_finder') }}
WHERE verdict = 'Project Star'
  AND NOT (
      sweet_spot_pct >= 60
      AND dist_to_archives_km <= 8
  )

UNION ALL

-- Check 2: Premium Location rows must meet the Premium criteria
SELECT
    neighbourhood,
    verdict,
    sweet_spot_pct,
    dist_to_archives_km,
    'Premium Location criteria violated' AS failure_reason
FROM {{ ref('mart_sweet_spot_finder') }}
WHERE verdict = 'Premium Location'
  AND NOT (
      dist_to_archives_km <= 5
      AND median_rent_ksh >= 50000
  )