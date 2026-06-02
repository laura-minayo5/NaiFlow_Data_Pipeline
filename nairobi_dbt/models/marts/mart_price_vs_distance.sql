-- Since we use this for high-res scatter plots, we want it spread across the cluster
-- Sorting by distance and price optimizes Redshift's ability to serve "Range" queries
{{ config(
    materialized='table', 
    schema='gold', 
    dist='even', 
    sort=['dist_to_archives_km', 'price_ksh'] 
) }}

SELECT
    -- Identifiers
    f.property_id,
    f.title,
    f.source_url,

    -- Categorical Dimensions (For Color & Slicing) 
    f.neighbourhood,
    f.value_zone,
    f.commute_tier,
    f.bedroom_label,
    f.property_type,
    f.price_bucket,

    -- The Scatter Axes (The "Meat" of the chart)
    -- Explicitly casting to DECIMAL ensures Power BI handles the axes as continuous 
    -- numbers rather than floating point approximations.
    CAST(f.dist_to_archives_km AS DECIMAL(10,2)) AS dist_to_archives_km,
    CAST(f.price_ksh AS DECIMAL(12,2)) AS price_ksh,

    -- Size & Tooltip Metrics
    f.bedrooms,
    f.bathrooms,
    f.is_sweet_spot,

    -- Statistical Context (For Trend Lines & Benchmarking)
    -- Useful for showing how a specific point deviates from its local peers
    f.neighbourhood_median_ksh,
    f.price_vs_neighbourhood_median,

    -- Time & Space (For Filtering)
    f.scraped_date,
    
    /* 
       DATA ARCHITECTURE NOTE: 
       month_year is a descriptive attribute of a date. In a Star Schema, 
       we keep these in the dimension table (dim_date) to keep the fact table 
       slimmer and ensure consistent date labeling across all models.
    */
    d.month_year, 
    
    f.latitude,
    f.longitude,

    -- Custom Labeling for Power BI tooltips
    -- This creates a nice label for the "hover" tooltip in your scatter plot
    f.neighbourhood || ' - ' || f.bedroom_label || ' (' || f.price_ksh || ' KSh)' AS tooltip_label

/* 
   We alias 'int_rentals_enriched' as 'f' (for Fact) and 'dim_date' as 'd' (for Dimension).
   The LEFT JOIN ensures that even if a date is missing in our dimension, 
   we don't lose the rental listing from our scatter plot.
*/
FROM {{ ref('int_rentals_enriched') }} f
LEFT JOIN {{ ref('dim_date') }} d 
    ON CAST(f.scraped_date AS DATE) = d.date_day

-- Data Quality Gate: Scatter plots fail if axes are NULL
WHERE f.dist_to_archives_km IS NOT NULL
  AND f.price_ksh IS NOT NULL
  -- Filtering out extreme outliers (e.g. 1 KSh listings) to keep the chart scaled correctly
  AND f.price_ksh > 1000