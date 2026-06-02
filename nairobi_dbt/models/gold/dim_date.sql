{{ config(materialized='table', schema='gold' , dist='all', sort='date_day') }}

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2026-01-01' as date)",
        end_date="cast('2027-01-01' as date)"
    ) }}
)

SELECT
    -- Primary Key 
    CAST(date_day AS DATE) AS date_day,
    
    -- Optimized Join Key
    CAST(TO_CHAR(date_day, 'YYYYMMDD') AS INT)  AS date_id,
    
    -- Time Parts
    CAST(EXTRACT(year FROM date_day) AS INT) AS year,
    CAST(EXTRACT(quarter FROM date_day) AS INT) AS quarter,
    CAST(EXTRACT(month FROM date_day) AS INT)  AS month,

    CAST(DATE_TRUNC('month', date_day) AS DATE) AS month_start,
    
    -- Descriptive Labels
    CAST(TO_CHAR(date_day, 'fmMonth') AS VARCHAR(10)) AS month_name,
    CAST(EXTRACT(dow FROM date_day) AS INT) AS day_of_week_num,
    CAST(TO_CHAR(date_day, 'DY') AS VARCHAR(3)) AS day_name_short,
    CAST(TO_CHAR(date_day, 'fmDay') AS VARCHAR(10)) AS day_of_week_name,
    CAST(TO_CHAR(date_day, 'fmMonth YYYY') AS VARCHAR(20)) AS month_year,
    
    -- Boolean Logic
    CASE 
        WHEN EXTRACT(dow FROM date_day) IN (0, 6) THEN TRUE 
        ELSE FALSE 
    END AS is_weekend

FROM date_spine