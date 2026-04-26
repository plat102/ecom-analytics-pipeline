-- Test: valid_to >= valid_from for all customer versions
-- Purpose: Ensure temporal integrity - no invalid date ranges
-- Expected: 0 rows (PASS if no violations)

SELECT
  customer_key,
  customer_natural_key,
  valid_from,
  valid_to,
  is_current
FROM {{ ref('dim_customer') }}
WHERE valid_to < valid_from
