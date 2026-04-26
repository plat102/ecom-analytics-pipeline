-- Test: Each customer has exactly 1 current version
-- Purpose: Ensure SCD Type 2 integrity - prevent multiple current versions
-- Expected: 0 rows (PASS if no violations)

SELECT
  customer_natural_key,
  SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS current_count
FROM {{ ref('dim_customer') }}
GROUP BY customer_natural_key
HAVING SUM(CASE WHEN is_current THEN 1 ELSE 0 END) != 1
