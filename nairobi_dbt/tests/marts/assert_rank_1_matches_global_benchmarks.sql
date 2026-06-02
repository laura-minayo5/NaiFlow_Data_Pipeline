/* 
    DATA QUALITY TEST: UNIFIED RANKING VALIDATION
    ============================================
    PURPOSE: 
    This test verifies that the analytical rankings (Value, Price, and Supply) 
    align perfectly with their underlying numerical benchmarks. 
    
    METHODOLOGY: 
    We use a 'Centralized Benchmark' approach via a CTE and CROSS JOIN to avoid 
    nested subqueries. This ensures we are testing against the 'Global Truth' 
    of the entire dataset.

    FAILURE CRITERIA: 
    The test fails if any neighborhood assigned a 'Rank 1' does not actually 
    possess the absolute minimum/maximum value for that specific metric.
*/

WITH global_benchmarks AS (
    -- Calculate the absolute best/worst values across the whole market
    SELECT 
        -- The lowest index score represents the best value for money
        MIN(affordability_index) AS true_min_index,
        -- The highest average rent identifies the most expensive area
        MAX(avg_rent_ksh) AS true_max_rent,
        -- The highest count identifies the area with peak market supply
        MAX(total_listings) AS true_max_supply
    FROM {{ ref('mart_neighbourhood_stats') }}
)

SELECT
    m.neighbourhood,
    m.value_rank,
    m.price_rank,
    m.supply_rank,
    m.affordability_index,
    m.avg_rent_ksh,
    m.total_listings,
    -- Pulling in benchmarks for a side-by-side comparison in the error report
    b.true_min_index,
    b.true_max_rent,
    b.true_max_supply
FROM {{ ref('mart_neighbourhood_stats') }} m
CROSS JOIN global_benchmarks b
WHERE 
    /* 
       TEST 1: AFFORDABILITY RANK 
       Ensures Rank 1 is the actual Minimum Index (Lowest = Best)
    */
    (m.value_rank = 1 AND m.affordability_index != b.true_min_index)
    
    OR 
    
    /* 
       TEST 2: PRICE RANK 
       Ensures Rank 1 is the actual Maximum Rent (Highest = Most Expensive)
    */
    (m.price_rank = 1 AND m.avg_rent_ksh != b.true_max_rent)
    
    OR
    
    /* 
       TEST 3: SUPPLY RANK 
       Ensures Rank 1 is the actual Maximum Count (Highest = Most Listings)
    */
    (m.supply_rank = 1 AND m.total_listings != b.true_max_supply)