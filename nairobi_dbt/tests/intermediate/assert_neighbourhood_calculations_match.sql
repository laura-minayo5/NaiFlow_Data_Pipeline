-- This test identifies listings where the neighbourhood-level math is broken.
-- It fails if:
-- 1. A listing has a neighbourhood but the median calculation returned NULL.
-- 2. The 'price vs median' percentage is missing even though we have all the inputs.

SELECT
    property_id,
    neighbourhood,
    price_ksh,
    neighbourhood_median_ksh,
    price_vs_neighbourhood_median
FROM {{ ref('int_rentals_enriched') }}
WHERE 
    -- Case 1: The "Invisible Median"
    -- We have a neighbourhood and a price, but the median calculation failed to join.
    (
        neighbourhood IS NOT NULL 
        AND price_ksh IS NOT NULL 
        AND neighbourhood_median_ksh IS NULL
    )
    OR
    -- Case 2: The "Broken Percent"
    -- We have the price and the median, but the percentage math didn't execute.
    (
        price_ksh IS NOT NULL 
        AND neighbourhood_median_ksh IS NOT NULL 
        AND price_vs_neighbourhood_median IS NULL
    )
    OR
    -- Case 3: The "Zero Guard"
    -- If the median is somehow 0, our percentage math would divide by zero.
    (neighbourhood_median_ksh = 0)