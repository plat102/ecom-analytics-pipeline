# dbt Data Models

Star Schema dimensional model for Glamira e-commerce analytics.

---

## Quick Start

```bash
cd dbt
poetry install
poetry run dbt run     # Build all models
poetry run dbt test    # Run all tests (38 tests, 100% pass rate)
poetry run dbt docs generate && dbt docs serve  # View lineage graph
```

---

## Model Architecture

**Total:** 18 models + 1 seed file

| Layer | Model Name | Type | Rows | Notes |
|-------|------------|------|------|-------|
| **Staging** | `stg_events` | view | 40.7M | Raw data cleanup |
| | `stg_ip_locations` | view | 3.24M | Unique IPs |
| | `stg_products` | view | 18.9K | Product catalog |
| **Intermediate** | `int_sales_with_customer` | table | 27.6K | Customer joins |
| | `int_events_checkout_success` | view | 25K | Checkout events |
| | `int_events_cart_items` | view | 34K | Line items |
| | `int_dq_sales_outliers` | view | - | Outlier detection |
| | `int_dq_outlier_summary` | view | - | Monitoring metrics |
| **Core - Dimensions** | `dim_date` | table | 5.8K | Calendar (2015-2030) |
| | `dim_customer` | table | 7.49M | SCD Type 2, email tracking |
| | `dim_location` | table | 54.6K | Country × region × city |
| | `dim_product` | table | 18.9K | Type 1 SCD |
| | `dim_device` | table | 17 | Event grain |
| | `dim_exchange_rate` | table | 42 | Seed file |
| **Core - Fact** | `fact_sales_order_line` | table | 27.6K | Partitioned by order_date |
| **Mart** | `mart_sales_daily` | table | 4.6K | Store × date × device |
| | `mart_sales_by_geography` | table | 18.7K | Country × city × date |
| | `mart_sales_by_product` | table | 21.2K | Product × date |
| | `mart_customer_summary` | table | 22.9K | Customer aggregates |

---

## Key Features

### SCD Type 2 Implementation
- Customer dimension tracks email changes over time
- 826 historical versions across 7.49M customer records
- Incremental materialization for performance
- Temporal fields: valid_from, valid_to, is_current

### Data Quality Layer
- Statistical outlier detection (P99 thresholds + business rules)
- 538 orders (2.07%) flagged as outliers
- Flag-based approach (not hard filters) for transparency
- Monitoring view: `int_dq_outlier_summary`

### Incremental Materialization
- All mart models use `insert_overwrite` strategy
- Partitioned by order_date for efficient updates
- Clustered by relevant dimensions (store_id, country_name, product_id, etc.)

---

## Common Commands

```bash
# Build all models
dbt run

# Build specific model
dbt run --select dim_customer

# Build model and downstream
dbt run --select dim_customer+

# Test all models
dbt test

# Test specific model
dbt test --select dim_customer

# Full refresh (recreate incremental models)
dbt run --full-refresh

# Compile SQL (no execution)
dbt compile

# Clean cache
dbt clean
```

---

## Project Structure

```
dbt/
├── models/
│   ├── staging/           # Raw data cleanup
│   ├── intermediate/      # Business logic
│   │   └── data_quality/  # Outlier detection
│   ├── core/              # Dimensions + fact
│   └── mart/              # Pre-aggregated for dashboards
├── seeds/
│   └── exchange_rates.csv
├── tests/                 # Custom tests
├── macros/
│   ├── scd/               # SCD Type 2 macros
│   └── schema.yml         # Macro documentation
├── dbt_project.yml
└── profiles.yml           # BigQuery connection
```
