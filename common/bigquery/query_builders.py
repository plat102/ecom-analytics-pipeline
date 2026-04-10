"""
BigQuery typed schema query builders.

Shared across CLI and Cloud Functions for consistent data loading.
Each query builder generates SQL INSERT statements that parse JSONL data
from external tables into typed BigQuery tables.
"""


def build_ip_locations_query(external_table_id: str, final_table_id: str) -> str:
    """
    Build INSERT query for ip_locations table with typed schema (5 fields).

    Args:
        external_table_id: Temporary external table ID
        final_table_id: Target ip_locations table ID

    Returns:
        SQL INSERT query string
    """
    return f"""
    INSERT INTO `{final_table_id}` (ip, country, region, city, ingested_at)
    SELECT
        JSON_VALUE(doc, '$.ip') AS ip,
        JSON_VALUE(doc, '$.country') AS country,
        JSON_VALUE(doc, '$.region') AS region,
        JSON_VALUE(doc, '$.city') AS city,
        CURRENT_TIMESTAMP() AS ingested_at
    FROM (
        SELECT PARSE_JSON(line) AS doc
        FROM `{external_table_id}`
        WHERE line IS NOT NULL AND TRIM(line) != ''
    )
    """


def build_products_query(external_table_id: str, final_table_id: str) -> str:
    """
    Build INSERT query for products table with typed schema (42 fields).

    Extracts flat fields from react_data structure (new crawler output).
    Handles REPEATED field (visible_contents) and mixed types.

    Args:
        external_table_id: Temporary external table ID
        final_table_id: Target products table ID

    Returns:
        SQL INSERT query string
    """
    return f"""
    INSERT INTO `{final_table_id}` (
        product_id, url, status, http_status, error_message, fallback_used,
        product_name, sku, attribute_set_id, attribute_set, type_id,
        product_type, product_type_value,
        price, min_price, max_price, min_price_format, max_price_format,
        gold_weight, none_metal_weight, fixed_silver_weight, material_design,
        qty, collection, collection_id, category, category_name,
        store_code, gender,
        platinum_palladium_info_in_alloy, bracelet_without_chain,
        show_popup_quantity_eternity, visible_contents, configure_mode,
        included_chain_weight, currency_code, tax_rate,
        primary_image_url, primary_video_url,
        gender_label, is_solid,
        ingested_at
    )
    SELECT
        JSON_VALUE(doc, '$.product_id') AS product_id,
        JSON_VALUE(doc, '$.url') AS url,
        JSON_VALUE(doc, '$.status') AS status,
        CAST(JSON_VALUE(doc, '$.http_status') AS INT64) AS http_status,
        JSON_VALUE(doc, '$.error_message') AS error_message,
        CAST(JSON_VALUE(doc, '$.fallback_used') AS BOOL) AS fallback_used,
        JSON_VALUE(doc, '$.product_name') AS product_name,
        JSON_VALUE(doc, '$.sku') AS sku,
        CAST(JSON_VALUE(doc, '$.attribute_set_id') AS INT64) AS attribute_set_id,
        JSON_VALUE(doc, '$.attribute_set') AS attribute_set,
        JSON_VALUE(doc, '$.type_id') AS type_id,
        JSON_VALUE(doc, '$.product_type') AS product_type,
        JSON_VALUE(doc, '$.product_type_value') AS product_type_value,
        JSON_VALUE(doc, '$.price') AS price,
        JSON_VALUE(doc, '$.min_price') AS min_price,
        JSON_VALUE(doc, '$.max_price') AS max_price,
        JSON_VALUE(doc, '$.min_price_format') AS min_price_format,
        JSON_VALUE(doc, '$.max_price_format') AS max_price_format,
        JSON_VALUE(doc, '$.gold_weight') AS gold_weight,
        CAST(JSON_VALUE(doc, '$.none_metal_weight') AS FLOAT64) AS none_metal_weight,
        CAST(JSON_VALUE(doc, '$.fixed_silver_weight') AS FLOAT64) AS fixed_silver_weight,
        JSON_VALUE(doc, '$.material_design') AS material_design,
        CAST(JSON_VALUE(doc, '$.qty') AS INT64) AS qty,
        JSON_VALUE(doc, '$.collection') AS collection,
        JSON_VALUE(doc, '$.collection_id') AS collection_id,
        JSON_VALUE(doc, '$.category') AS category,
        JSON_VALUE(doc, '$.category_name') AS category_name,
        JSON_VALUE(doc, '$.store_code') AS store_code,
        JSON_VALUE(doc, '$.gender') AS gender,
        CAST(JSON_VALUE(doc, '$.platinum_palladium_info_in_alloy') AS INT64) AS platinum_palladium_info_in_alloy,
        CAST(JSON_VALUE(doc, '$.bracelet_without_chain') AS INT64) AS bracelet_without_chain,
        CAST(JSON_VALUE(doc, '$.show_popup_quantity_eternity') AS INT64) AS show_popup_quantity_eternity,
        ARRAY(
            SELECT JSON_VALUE(value)
            FROM UNNEST(JSON_QUERY_ARRAY(doc, '$.visible_contents')) AS value
        ) AS visible_contents,
        CAST(JSON_VALUE(doc, '$.configure_mode') AS INT64) AS configure_mode,
        CAST(JSON_VALUE(doc, '$.included_chain_weight') AS FLOAT64) AS included_chain_weight,
        JSON_VALUE(doc, '$.currency_code') AS currency_code,
        CAST(JSON_VALUE(doc, '$.tax_rate') AS FLOAT64) AS tax_rate,
        JSON_VALUE(doc, '$.primary_image_url') AS primary_image_url,
        JSON_VALUE(doc, '$.primary_video_url') AS primary_video_url,
        JSON_VALUE(doc, '$.gender_label') AS gender_label,
        JSON_VALUE(doc, '$.is_solid') AS is_solid,
        CURRENT_TIMESTAMP() AS ingested_at
    FROM (
        SELECT PARSE_JSON(line) AS doc
        FROM `{external_table_id}`
        WHERE line IS NOT NULL AND TRIM(line) != ''
    )
    """


def build_events_query(external_table_id: str, final_table_id: str) -> str:
    """
    Build INSERT query for events table with typed schema (33 columns, 47 field paths).

    Handles:
    - Converting option (both object and array) to REPEATED RECORD
    - Converting cart_products with nested option to RECORD REPEATED structure
    - Null handling: empty arrays for REPEATED fields, NULL for scalar fields

    Args:
        external_table_id: Temporary external table ID
        final_table_id: Target events table ID

    Returns:
        SQL INSERT query string
    """
    return f"""
    INSERT INTO `{final_table_id}` (
        _id, device_id, user_id_db, email_address, ip, store_id, user_agent, resolution,
        utm_source, utm_medium, api_version, collection, time_stamp, local_time,
        collect_id, current_url, referrer_url, show_recommendation,
        product_id, viewing_product_id,
        option,
        price, currency, cat_id, key_search,
        cart_products,
        order_id, is_paypal, recommendation, recommendation_product_id,
        recommendation_product_position, recommendation_clicked_position,
        ingested_at
    )
    SELECT
        JSON_VALUE(doc, '$._id') AS _id,
        JSON_VALUE(doc, '$.device_id') AS device_id,
        IFNULL(JSON_VALUE(doc, '$.user_id_db'), '') AS user_id_db,
        IFNULL(JSON_VALUE(doc, '$.email_address'), '') AS email_address,
        JSON_VALUE(doc, '$.ip') AS ip,
        CAST(JSON_VALUE(doc, '$.store_id') AS STRING) AS store_id,
        JSON_VALUE(doc, '$.user_agent') AS user_agent,
        JSON_VALUE(doc, '$.resolution') AS resolution,
        CAST(JSON_VALUE(doc, '$.utm_source') AS STRING) AS utm_source,
        CAST(JSON_VALUE(doc, '$.utm_medium') AS STRING) AS utm_medium,
        JSON_VALUE(doc, '$.api_version') AS api_version,
        JSON_VALUE(doc, '$.collection') AS collection,
        CAST(JSON_VALUE(doc, '$.time_stamp') AS INT64) AS time_stamp,
        JSON_VALUE(doc, '$.local_time') AS local_time,
        JSON_VALUE(doc, '$.collect_id') AS collect_id,
        JSON_VALUE(doc, '$.current_url') AS current_url,
        JSON_VALUE(doc, '$.referrer_url') AS referrer_url,
        JSON_VALUE(doc, '$.show_recommendation') AS show_recommendation,
        JSON_VALUE(doc, '$.product_id') AS product_id,
        JSON_VALUE(doc, '$.viewing_product_id') AS viewing_product_id,

        ARRAY(
            SELECT AS STRUCT
                JSON_VALUE(opt, '$.option_id') AS option_id,
                JSON_VALUE(opt, '$.option_label') AS option_label,
                JSON_VALUE(opt, '$.value_id') AS value_id,
                JSON_VALUE(opt, '$.value_label') AS value_label,
                JSON_VALUE(opt, '$.quality') AS quality,
                JSON_VALUE(opt, '$.quality_label') AS quality_label
            FROM UNNEST(
                CASE
                    WHEN JSON_TYPE(JSON_QUERY(doc, '$.option')) = 'array'
                    THEN JSON_QUERY_ARRAY(doc, '$.option')
                    ELSE []
                END
            ) AS opt
        ) AS option,

        JSON_VALUE(doc, '$.price') AS price,
        JSON_VALUE(doc, '$.currency') AS currency,
        JSON_VALUE(doc, '$.cat_id') AS cat_id,
        JSON_VALUE(doc, '$.key_search') AS key_search,

        ARRAY(
            SELECT AS STRUCT
                CAST(JSON_VALUE(cp, '$.product_id') AS INT64) AS product_id,
                CAST(JSON_VALUE(cp, '$.amount') AS INT64) AS amount,
                JSON_VALUE(cp, '$.price') AS price,
                JSON_VALUE(cp, '$.currency') AS currency,
                ARRAY(
                    SELECT AS STRUCT
                        CAST(JSON_VALUE(opt, '$.option_id') AS INT64) AS option_id,
                        JSON_VALUE(opt, '$.option_label') AS option_label,
                        CAST(JSON_VALUE(opt, '$.value_id') AS INT64) AS value_id,
                        JSON_VALUE(opt, '$.value_label') AS value_label
                    FROM UNNEST(
                        CASE
                            WHEN JSON_TYPE(JSON_QUERY(cp, '$.option')) = 'array'
                            THEN JSON_QUERY_ARRAY(cp, '$.option')
                            ELSE []
                        END
                    ) AS opt
                ) AS option
            FROM UNNEST(JSON_QUERY_ARRAY(doc, '$.cart_products')) AS cp
        ) AS cart_products,

        CAST(JSON_VALUE(doc, '$.order_id') AS STRING) AS order_id,
        CAST(JSON_VALUE(doc, '$.is_paypal') AS BOOL) AS is_paypal,
        CAST(JSON_VALUE(doc, '$.recommendation') AS BOOL) AS recommendation,
        JSON_VALUE(doc, '$.recommendation_product_id') AS recommendation_product_id,
        CAST(JSON_VALUE(doc, '$.recommendation_product_position') AS STRING) AS recommendation_product_position,
        CAST(JSON_VALUE(doc, '$.recommendation_clicked_position') AS INT64) AS recommendation_clicked_position,

        CURRENT_TIMESTAMP() AS ingested_at
    FROM (
        SELECT PARSE_JSON(line) AS doc
        FROM `{external_table_id}`
        WHERE line IS NOT NULL AND TRIM(line) != ''
    )
    """
