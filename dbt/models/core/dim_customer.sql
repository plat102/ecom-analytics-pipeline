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
    MD5(CONCAT(COALESCE(email_address, ''))) AS row_hash
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

existing_customers AS (
  SELECT *
  FROM {{ this }}
  WHERE is_current = TRUE
),

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
    ROW_NUMBER() OVER (ORDER BY customer_natural_key) +
      (SELECT COALESCE(MAX(customer_key), 0) FROM {{ this }})
      AS customer_key,
    customer_natural_key,
    user_id_db,
    device_id,
    email_address,
    CURRENT_DATE() AS valid_from,
    DATE('9999-12-31') AS valid_to,
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
    ROW_NUMBER() OVER (ORDER BY customer_natural_key) +
      (SELECT COALESCE(MAX(customer_key), 0) FROM {{ this }})
      AS customer_key,
    customer_natural_key,
    user_id_db,
    device_id,
    email_address,
    CURRENT_DATE() AS valid_from,
    DATE('9999-12-31') AS valid_to,
    TRUE AS is_current,
    row_hash
  FROM brand_new_customers
),

{% else %}

initial_load AS (
  SELECT
    ROW_NUMBER() OVER (ORDER BY customer_natural_key, version_number) AS customer_key,
    customer_natural_key,
    user_id_db,
    device_id,
    email_address,

    -- valid_from = when this email first appeared
    DATE(first_seen_with_this_email) AS valid_from,

    -- valid_to = when next email appeared (or 9999-12-31 if latest)
    COALESCE(
      LEAD(DATE(first_seen_with_this_email)) OVER (
        PARTITION BY customer_natural_key
        ORDER BY first_seen_with_this_email
      ),
      DATE('9999-12-31')
    ) AS valid_to,

    -- is_current = TRUE for latest version only
    CASE
      WHEN LEAD(first_seen_with_this_email) OVER (
        PARTITION BY customer_natural_key
        ORDER BY first_seen_with_this_email
      ) IS NULL
      THEN TRUE
      ELSE FALSE
    END AS is_current,

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
