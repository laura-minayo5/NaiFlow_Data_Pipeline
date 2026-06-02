-- GOLD LAYER BUSINESS INTEGRITY TEST
-- ==========================================
-- This test checks for data leakage between the Silver and Gold layers.
-- It fails if rows are dropped because a neighborhood or date doesn't exist
-- inside the dimension tables.

WITH silver_layer AS (
    SELECT COUNT(*) AS total_expected_rows
    FROM {{ ref('int_rentals_enriched') }}
    -- Replicate the non-null business filter applied in the fact table
    WHERE neighbourhood IS NOT NULL 
      AND scraped_date IS NOT NULL
),

gold_layer AS (
    SELECT COUNT(*) AS total_actual_rows
    FROM {{ ref('fct_rental_listings') }}
)

SELECT 
    s.total_expected_rows,
    g.total_actual_rows,
    (s.total_expected_rows - g.total_actual_rows) AS dropped_rows_count
FROM silver_layer s
CROSS JOIN gold_layer g
WHERE g.total_actual_rows < s.total_expected_rows
