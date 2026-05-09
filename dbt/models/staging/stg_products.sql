{{
  config(
    materialized='view',
    schema='staging'
  )
}}

WITH raw_product__filter AS (
  SELECT *
  FROM {{ source('glamira_raw', 'products') }}
  WHERE status = 'success'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY ingested_at DESC) = 1
),

raw_product__rename AS (
  SELECT
    -- Primary key
    product_id,

    -- Product metadata
    url,
    product_name,
    sku,

    -- Classification
    product_type,
    collection AS collection_name,
    gender,
    category AS category_id,
    category_name,

    -- Price fields
    currency_code,
    price,
    min_price,
    max_price,

    -- Physical attributes
    gold_weight,

    -- Metadata
    status,
    ingested_at

  FROM raw_product__filter
),

raw_product__cast_type AS (
  SELECT
    -- Primary key (already STRING)
    product_id,

    -- Product metadata (keep as STRING)
    url,
    product_name,
    sku,
    product_type,
    collection_name,
    gender,
    category_id,
    category_name,
    currency_code,

    -- Price fields: cast to NUMERIC
    SAFE_CAST(price AS NUMERIC) AS price,
    SAFE_CAST(min_price AS NUMERIC) AS min_price,
    SAFE_CAST(max_price AS NUMERIC) AS max_price,
    SAFE_CAST(gold_weight AS NUMERIC) AS gold_weight,

    -- Metadata
    status,
    ingested_at

  FROM raw_product__rename
),

raw_product__handle_null AS (
  SELECT
    -- Primary key
    product_id,

    -- Product metadata: convert empty strings to NULL
    NULLIF(url, '') AS url,
    NULLIF(product_name, '') AS product_name,
    NULLIF(sku, '') AS sku,
    NULLIF(product_type, '') AS product_type,
    NULLIF(collection_name, '') AS collection_name,
    NULLIF(gender, '') AS gender,
    NULLIF(currency_code, '') AS currency_code,

    -- Category: convert '0' to NULL, handle empty category_name
    CASE
      WHEN category_id = '0' THEN NULL
      ELSE NULLIF(category_id, '')
    END AS category_id,
    NULLIF(category_name, '') AS category_name,

    -- Price fields (already NUMERIC)
    price,
    min_price,
    max_price,
    gold_weight,

    -- Metadata
    NULLIF(status, '') AS status,
    ingested_at

  FROM raw_product__cast_type
),

raw_product__fix_invalid_value AS (
  SELECT
    -- Primary key
    product_id,

    -- Product metadata (clean gender, category, and product_type fields)
    url,
    product_name,
    sku,
    CASE
      WHEN NULLIF(product_type, '') IN ('-1', '--_select_--') THEN 'Unknown'
      WHEN NULLIF(product_type, '') IS NULL THEN 'Unknown'
      ELSE NULLIF(product_type, '')
    END AS product_type,
    collection_name,
    CASE
      WHEN NULLIF(gender, '') IN ('false', 'FALSE') THEN 'Unknown'
      WHEN NULLIF(gender, '') IS NULL THEN 'Unknown'
      ELSE NULLIF(gender, '')
    END AS gender,
    category_id,
    CASE
      WHEN NULLIF(category_name, '') IS NULL THEN 'Unknown'
      ELSE NULLIF(category_name, '')
    END AS category_name,
    currency_code,

    -- Price fields: set invalid prices (<=0) to NULL
    CASE
      WHEN price <= 0 THEN NULL
      ELSE price
    END AS price,

    CASE
      WHEN min_price <= 0 THEN NULL
      ELSE min_price
    END AS min_price,

    CASE
      WHEN max_price <= 0 THEN NULL
      ELSE max_price
    END AS max_price,

    -- Physical attributes
    gold_weight,

    -- Metadata
    status,
    ingested_at

  FROM raw_product__handle_null
),

final AS (
  SELECT * FROM raw_product__fix_invalid_value
)


SELECT * FROM final
