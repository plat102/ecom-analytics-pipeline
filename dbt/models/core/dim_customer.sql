-- SCD Type 2 implementation with temporal backfill capability
-- Tracks email_address changes over time
-- Initial load: Reconstructs 826 historical versions from Oct-Nov 2019 events
-- Incremental runs: Detects changes via row_hash and maintains version history
{{
  config(
    materialized='incremental',
    unique_key='customer_key',
    schema='core',
    cluster_by=['customer_natural_key', 'is_current'],
    on_schema_change='append_new_columns'
  )
}}

WITH users_raw AS (
  SELECT
    COALESCE(user_id_db, device_id) AS customer_natural_key,
    user_id_db,
    device_id,
    email_address,
    event_timestamp
  FROM {{ ref('stg_events') }}
  WHERE COALESCE(user_id_db, device_id) IS NOT NULL
),

-- Deduplicate and track versions:
-- GROUP BY email, 1 ver created for each changes occur
-- Row hash used for detect changes in incremental runs
email_changes AS (
  SELECT
    customer_natural_key,
    MAX(user_id_db) AS user_id_db,
    MAX(device_id) AS device_id,
    email_address,
    MIN(event_timestamp) AS first_seen_with_this_email,
    MAX(event_timestamp) AS last_seen_with_this_email,
    ROW_NUMBER() OVER (
      PARTITION BY customer_natural_key
      ORDER BY MIN(event_timestamp)
    ) AS version_number,
    {{ scd2_row_hash(['email_address']) }} AS row_hash
  FROM users_raw
  GROUP BY
    customer_natural_key,
    email_address
),

customers_staging AS (
  SELECT
    customer_natural_key,
    user_id_db,
    device_id,
    email_address,
    first_seen_with_this_email,
    version_number,
    row_hash,
    CURRENT_DATE() AS snapshot_date
  FROM email_changes
),

{% if is_incremental() %}
-- INCREMENTAL LOGIC:
-- 1. Expire old versions (set valid_to = today, is_current = FALSE)
-- 2. Insert new versions for changed customers
-- 3. Insert brand new customers

existing_customers AS (
  SELECT *
  FROM {{ this }}
  WHERE is_current = TRUE
),

-- Detect changes: Compare row_hash between staging and existing current versions
changed_customers AS (
  SELECT
    s.customer_natural_key,
    e.customer_key AS old_customer_key,
    s.user_id_db,
    s.device_id,
    s.email_address,
    s.row_hash,
    s.snapshot_date
  FROM customers_staging s
  INNER JOIN existing_customers e
    ON s.customer_natural_key = e.customer_natural_key
  WHERE s.row_hash != e.row_hash
),

expire_old_versions AS (
  SELECT
    customer_key,
    customer_natural_key,
    user_id_db,
    device_id,
    email_address,
    valid_from,
    CURRENT_DATE() AS valid_to,
    FALSE AS is_current,
    row_hash
  FROM {{ this }}
  WHERE customer_natural_key IN (
    SELECT customer_natural_key FROM changed_customers
  )
  AND is_current = TRUE
),

new_versions AS (
  SELECT
    {{ generate_incremental_surrogate_key('customer_natural_key',
      key_column='customer_key') }} AS customer_key,
    customer_natural_key,
    user_id_db,
    device_id,
    email_address,
    CURRENT_DATE() AS valid_from,
    {{ scd_end_date() }} AS valid_to,
    TRUE AS is_current,
    row_hash
  FROM changed_customers
),

brand_new_customers AS (
  SELECT
    s.customer_natural_key,
    s.user_id_db,
    s.device_id,
    s.email_address,
    s.row_hash
  FROM customers_staging s
  LEFT JOIN existing_customers e
    ON s.customer_natural_key = e.customer_natural_key
  WHERE e.customer_natural_key IS NULL
),

insert_new_customers AS (
  SELECT
    {{ generate_incremental_surrogate_key('customer_natural_key', key_column='customer_key') }} AS customer_key,
    customer_natural_key,
    user_id_db,
    device_id,
    email_address,
    CURRENT_DATE() AS valid_from,
    {{ scd_end_date() }} AS valid_to,
    TRUE AS is_current,
    row_hash
  FROM brand_new_customers
),

{% else %}
-- INITIAL LOAD: Temporal backfill from historical events
-- Reconstruct email versions using window functions to calculate valid_to/is_current

initial_load AS (
  SELECT
    ROW_NUMBER() OVER (ORDER BY customer_natural_key, version_number) AS customer_key,
    customer_natural_key,
    user_id_db,
    device_id,
    email_address,
    DATE(first_seen_with_this_email) AS valid_from,
    {{ scd2_calculate_valid_to('first_seen_with_this_email', 'customer_natural_key') }} AS valid_to,
    {{ scd2_calculate_is_current('first_seen_with_this_email', 'customer_natural_key') }} AS is_current,
    row_hash
  FROM customers_staging
),

{% endif %}

final AS (
  {% if is_incremental() %}
    SELECT * FROM expire_old_versions
    UNION ALL
    SELECT * FROM new_versions
    UNION ALL
    SELECT * FROM insert_new_customers
  {% else %}
    SELECT * FROM initial_load
  {% endif %}
)

SELECT * FROM final
