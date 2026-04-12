-- INFORMATION_SCHEMA Metadata Queries
-- Purpose: Extract table and column metadata from BigQuery system tables

-- =============================================================================
-- SECTION 1: Table-Level Metadata
-- =============================================================================

SELECT
    'Table Metadata' AS metadata_category,
    table_name,
    table_type,
    creation_time,

    -- Size metrics
    ROUND(size_bytes / POW(1024, 3), 2) AS size_gb,
    row_count,

    -- Partitioning info
    CASE
        WHEN table_name = 'events' THEN 'PARTITIONED BY DATE(ingested_at)'
        ELSE 'NOT PARTITIONED'
    END AS partitioning_info,

    -- Time travel window
    ROUND(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), snapshot_time_ms / 1000, DAY), 0) AS days_since_snapshot

FROM `ecom-analytics-tp.glamira_raw.INFORMATION_SCHEMA.TABLES`
WHERE table_name IN ('events', 'ip_locations', 'products')
ORDER BY table_name;


-- =============================================================================
-- SECTION 2: Column-Level Metadata for Events Table
-- =============================================================================

SELECT
    'Events Table Columns' AS metadata_category,
    column_name,
    data_type,
    is_nullable,
    is_partitioning_column,
    clustering_ordinal_position

FROM `ecom-analytics-tp.glamira_raw.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'events'
ORDER BY ordinal_position;


-- =============================================================================
-- SECTION 3: Column-Level Metadata for IP Locations Table
-- =============================================================================

SELECT
    'IP Locations Table Columns' AS metadata_category,
    column_name,
    data_type,
    is_nullable,
    is_partitioning_column

FROM `ecom-analytics-tp.glamira_raw.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'ip_locations'
ORDER BY ordinal_position;


-- =============================================================================
-- SECTION 4: Column-Level Metadata for Products Table
-- =============================================================================

SELECT
    'Products Table Columns' AS metadata_category,
    column_name,
    data_type,
    is_nullable,
    is_partitioning_column

FROM `ecom-analytics-tp.glamira_raw.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'products'
ORDER BY ordinal_position;


-- =============================================================================
-- SECTION 5: Partition Metadata for Events Table
-- =============================================================================

SELECT
    'Events Table Partitions' AS metadata_category,
    partition_id,
    total_rows,
    ROUND(total_logical_bytes / POW(1024, 2), 2) AS size_mb,
    last_modified_time

FROM `ecom-analytics-tp.glamira_raw.INFORMATION_SCHEMA.PARTITIONS$__UNPARTITIONED__`
WHERE table_name = 'events'
ORDER BY partition_id DESC
LIMIT 10;


-- =============================================================================
-- SECTION 6: Dataset-Level Metadata
-- =============================================================================

SELECT
    'Dataset Metadata' AS metadata_category,
    schema_name AS dataset_name,
    location,
    creation_time,
    default_table_expiration_ms,
    default_partition_expiration_ms

FROM `ecom-analytics-tp.glamira_raw.INFORMATION_SCHEMA.SCHEMATA`
WHERE schema_name = 'glamira_raw';


-- =============================================================================
-- SECTION 7: Nested Column Details for Events Table
-- =============================================================================

SELECT
    'Events Nested Columns' AS metadata_category,
    column_name,
    data_type,
    CASE
        WHEN data_type LIKE '%ARRAY%' THEN 'REPEATED'
        WHEN data_type LIKE '%STRUCT%' THEN 'RECORD'
        ELSE 'SCALAR'
    END AS field_mode,
    is_nullable

FROM `ecom-analytics-tp.glamira_raw.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'events'
  AND (data_type LIKE '%ARRAY%' OR data_type LIKE '%STRUCT%')
ORDER BY ordinal_position;


-- =============================================================================
-- SECTION 8: Column Count Summary
-- =============================================================================

SELECT
    'Column Count Summary' AS metadata_category,
    table_name,
    COUNT(*) AS total_columns,
    COUNTIF(is_nullable = 'YES') AS nullable_columns,
    COUNTIF(is_nullable = 'NO') AS non_nullable_columns,
    COUNTIF(is_partitioning_column = 'YES') AS partitioning_columns

FROM `ecom-analytics-tp.glamira_raw.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name IN ('events', 'ip_locations', 'products')
GROUP BY table_name
ORDER BY table_name;


-- =============================================================================
-- SECTION 9: Storage and Cost Estimates
-- =============================================================================

SELECT
    'Storage and Cost Estimates' AS metadata_category,
    table_name,

    -- Storage metrics
    ROUND(size_bytes / POW(1024, 3), 2) AS storage_gb,
    row_count,
    ROUND(size_bytes / row_count, 0) AS avg_bytes_per_row,

    -- Cost estimates (placeholder values - adjust based on pricing)
    ROUND(size_bytes / POW(1024, 3) * 0.02, 2) AS estimated_monthly_storage_cost_usd,

    -- Compression ratio estimate
    CASE
        WHEN table_name = 'events' THEN
            ROUND(row_count * 2000 / size_bytes, 2)  -- Assume avg 2KB per raw JSON event
        WHEN table_name = 'ip_locations' THEN
            ROUND(row_count * 500 / size_bytes, 2)   -- Assume avg 500B per location record
        WHEN table_name = 'products' THEN
            ROUND(row_count * 1500 / size_bytes, 2)  -- Assume avg 1.5KB per product
        ELSE NULL
    END AS estimated_compression_ratio

FROM `ecom-analytics-tp.glamira_raw.INFORMATION_SCHEMA.TABLES`
WHERE table_name IN ('events', 'ip_locations', 'products')
ORDER BY table_name;
