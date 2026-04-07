# BigQuery Raw Layer Tables

## Tables

- **`glamira_raw.events`** - 41M+ MongoDB events, partitioned by `ingested_at` (DAY)
- **`glamira_raw.ip_locations`** - IP geolocation reference data
- **`glamira_raw.products`** - Product catalog data

## Schema Design

All raw tables use the same schema:
- `raw_doc` (JSON) - Full document from source
- `ingested_at` (TIMESTAMP) - Ingestion timestamp

**Why JSON column?**
MongoDB has mixed types (e.g., `store_id` can be `6` or `"6"`). Auto-detect schemas fail unpredictably when loading multiple files. JSON column eliminates this risk.
Schema enforcement happens in dbt silver layer.

**Why partition events table?**
41M rows require partitioning to avoid expensive full scans in dbt queries.

## Recreate Tables

```bash
# Events (partitioned)
bq mk --table \
  --time_partitioning_field=ingested_at \
  --time_partitioning_type=DAY \
  --description="Raw MongoDB events data from Glamira e-commerce platform" \
  glamira_raw.events \
  schemas/events.json

# IP Locations (no partition)
bq mk --table \
  --description="IP geolocation reference data processed from IP2Location binary" \
  glamira_raw.ip_locations \
  schemas/ip_locations.json

# Products (no partition)
bq mk --table \
  --description="Glamira product catalog data crawled from website" \
  glamira_raw.products \
  schemas/products.json
```

## Verify tables

List tables:
```bash
bq ls glamira_raw
```

Output:
```
    tableId      Type    Labels      Time Partitioning       Clustered Fields  
 -------------- ------- -------- -------------------------- ------------------ 
  events         TABLE            DAY (field: ingested_at)                     
  ip_locations   TABLE                                                         
  products       TABLE                                                         
```

Source tables metadata and current state can be checked with:

```bash
bq show --schema glamira_raw.events
bq show glamira_raw.events
```
