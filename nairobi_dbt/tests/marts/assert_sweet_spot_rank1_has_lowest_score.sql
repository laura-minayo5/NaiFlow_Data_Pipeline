-- tests/marts/assert_sweet_spot_rank1_has_lowest_score.sql
-- =====================================================
-- PURPOSE:
--   The neighbourhood ranked #1 must have the lowest composite_score.
--   If rank 1 has a higher score than rank 2, the RANK() window function
--   was applied incorrectly (wrong sort direction).
--
-- HOW IT WORKS:
--   Gets the composite_score for rank 1 and rank 2.
--   If rank 1 score > rank 2 score — something is wrong.
--   Returns a row if the invariant is violated — test FAILS if rows come back.

WITH rank1 AS (
    SELECT composite_score AS rank1_score
    FROM {{ ref('mart_sweet_spot_finder') }}
    WHERE overall_value_rank = 1
    LIMIT 1
),
rank2 AS (
    SELECT composite_score AS rank2_score
    FROM {{ ref('mart_sweet_spot_finder') }}
    WHERE overall_value_rank = 2
    LIMIT 1
)
SELECT
    r1.rank1_score,
    r2.rank2_score
FROM rank1 r1
CROSS JOIN rank2 r2
-- Fail if rank 1 has a HIGHER score than rank 2
-- (lower score should always mean better rank)
WHERE r1.rank1_score > r2.rank2_score