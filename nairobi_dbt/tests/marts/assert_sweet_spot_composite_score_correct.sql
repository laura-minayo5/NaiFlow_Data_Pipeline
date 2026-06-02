-- tests/marts/assert_sweet_spot_composite_score_correct.sql
-- ======================================================
-- PURPOSE:
--   Verifies the composite_score stored in the model exactly matches
--   the weighted formula: (0.5 × price_rank) + (0.3 × distance_rank) + (0.2 × consistency_rank)
--
-- WHY THIS MATTERS:
--   The composite_score drives overall_value_rank which is the headline
--   number on the Project Star Power BI page. If the formula drifted
--   (e.g. someone changed a weight), this test catches it immediately.
--
-- HOW IT WORKS:
--   Recalculates the expected score from the stored component ranks,
--   then compares it to the stored composite_score.
--   A tolerance of 0.02 accounts for floating point rounding differences.
--   Any row exceeding this tolerance is returned — test FAILS if rows come back.

SELECT
    neighbourhood,
    composite_score AS stored_score,
    ROUND(
        (0.5 * price_rank) + (0.3 * distance_rank) + (0.2 * consistency_rank),
        2
    ) AS expected_score,
    ABS(
        composite_score -
        ROUND((0.5 * price_rank) + (0.3 * distance_rank) + (0.2 * consistency_rank), 2)
    ) AS deviation

FROM {{ ref('mart_sweet_spot_finder') }}

-- Fail if deviation exceeds floating point tolerance
WHERE ABS(
    composite_score -
    ROUND((0.5 * price_rank) + (0.3 * distance_rank) + (0.2 * consistency_rank), 2)
) > 0.02