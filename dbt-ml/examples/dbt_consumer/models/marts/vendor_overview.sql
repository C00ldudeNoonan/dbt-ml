{{ config(materialized='table') }}

WITH facts AS (
    SELECT * FROM {{ ref('invoice_facts') }}
),
dbt_ml_summary AS (
    SELECT * FROM {{ source('dbt_ml_invoice_pipeline', 'invoice_summary') }}
)
SELECT
    f.vendor,
    s.invoice_count       AS dbt_ml_invoice_count,
    s.total_spend         AS dbt_ml_total_spend,
    SUM(f.total)          AS dbt_total_spend,
    COUNT(*)              AS dbt_row_count,
    SUM(CASE WHEN f.size_bucket = 'large'  THEN 1 ELSE 0 END) AS large_count,
    SUM(CASE WHEN f.size_bucket = 'medium' THEN 1 ELSE 0 END) AS medium_count,
    SUM(CASE WHEN f.size_bucket = 'small'  THEN 1 ELSE 0 END) AS small_count
FROM facts f
LEFT JOIN dbt_ml_summary s USING (vendor)
GROUP BY f.vendor, s.invoice_count, s.total_spend
ORDER BY dbt_total_spend DESC
