# Data Dictionary

**Database:** `countly`
**Collection:** `summary`

---

## Full Collection Overview

E-commerce analytics events from Glamira online stores.

- **Total Documents:** 41,432,473
- **Data Coverage:** March 31, 2020 - June 4, 2020 (65 days)
- **Event Types:** 27 distinct collection types
- **Stores:** 86 unique store IDs

---

*Metrics calculated on entire collection (41M+ documents)*

### Total Document Count

```
Estimated Count: 41,432,473
Query Time: ~1 second
```

**Method:** `db.summary.estimated_document_count()`
- **Script:** [01_total_count.txt](../scripts/explore_raw_glamira/01_total_count.txt)

---

### Distinct Event Types

**Total:** 27 event types

| Event Type                              |      Count |      % | Lifecycle      | Description                              |
|-----------------------------------------|-----------:|-------:|----------------|------------------------------------------|
| `view_listing_page`                     | 11,259,694 | 27.18% | Browsing       | Visit listing page                       |
| `view_product_detail`                   | 10,944,427 | 26.42% | Product        | View product detail page                 |
| `select_product_option`                 |  8,844,342 | 21.35% | Product        | User selects product option              |
| `select_product_option_quality`         |  2,231,825 |  5.39% | Product        | User selects product quality option      |
| `view_static_page`                      |  1,451,565 |  3.50% | Browsing       | Visit static page                        |
| `view_landing_page`                     |  1,434,230 |  3.46% | Browsing       | Visit landing page                       |
| `product_detail_recommendation_visible` |  1,302,362 |  3.14% | Recommendation | Recommendation visible on product detail |
| `view_home_page`                        |  1,053,420 |  2.54% | Browsing       | Visit home page                          |
| `listing_page_recommendation_visible`   |    718,048 |  1.73% | Recommendation | Recommendation visible on listing page   |
| `product_detail_recommendation_noticed` |    490,780 |  1.18% | Recommendation | Recommendation noticed on product detail |
| `view_shopping_cart`                    |    343,077 |  0.83% | Product        | View shopping cart                       |
| `landing_page_recommendation_visible`   |    314,999 |  0.76% | Recommendation | Recommendation visible on landing page   |
| `search_box_action`                     |    238,308 |  0.58% | Search         | User performs search                     |
| `add_to_cart_action`                    |    187,901 |  0.45% | Product        | User adds product to cart                |
| `product_detail_recommendation_clicked` |    179,228 |  0.43% | Product        | Click recommendation on product detail   |
| `view_my_account`                       |    112,066 |  0.27% | Browsing       | Visit account page                       |
| `checkout`                              |     88,540 |  0.21% | Transaction    | User proceeds to checkout                |
| `landing_page_recommendation_noticed`   |     58,186 |  0.14% | Recommendation | Recommendation noticed on landing page   |
| `listing_page_recommendation_noticed`   |     39,819 |  0.10% | Recommendation | Recommendation noticed on listing page   |
| `view_all_recommend`                    |     33,664 |  0.08% | Recommendation | View all recommendations                 |
| `checkout_success`                      |     26,079 |  0.06% | Transaction    | Order successfully completed             |
| `listing_page_recommendation_clicked`   |     25,545 |  0.06% | Product        | Click recommendation on listing page     |
| `landing_page_recommendation_clicked`   |     20,128 |  0.05% | Product        | Click recommendation on landing page     |
| `product_view_all_recommend_clicked`    |     16,682 |  0.04% | Product        | Click "view all" recommendations         |
| `view_sorting_relevance`                |     15,284 |  0.04% | Browsing       | Sort by relevance                        |
| `sorting_relevance_click_action`        |      1,713 |  0.00% | Search         | User clicks on sorting relevance         |
| `back_to_product_action`                |        561 |  0.00% | Search         | User navigates back to product           |


- **Top 3 events** account for 75% of all events:
  - `view_listing_page` (27%),
  - `view_product_detail` (26%),
  - `select_product_option` (21%)
- **Conversion events** are rare: `checkout` (0.21%), `checkout_success` (0.06%)
- **Recommendation events** show engagement: visible (5.63%), noticed (1.42%), clicked (0.52%)


**Method:** `db.summary.aggregate([{$group: {_id: "$collection", count: {$sum: 1}}}, {$sort: {count: -1}}])`
- **Script:** [03_distinct_collections.txt](../scripts/explore_raw_glamira/03_distinct_collections.txt)

#### Event Lifecycle Summary

| Stage          | Events     | % of Total | Description                                 |
|----------------|------------|------------|---------------------------------------------|
| Browsing       | 14,327,162 | 34.58%     | Navigation & page views                     |
| Product        | 22,450,822 | 54.18%     | Product interaction, configuration & cart   |
| Recommendation | 3,554,008  | 8.58%      | Recommendation visibility & user engagement |
| Search         | 240,582    | 0.58%      | Search & navigation actions                 |
| Transaction    | 114,619    | 0.28%      | Checkout & purchase completion              |

**Journey Insights:**
- **Product-focused**: 54% product interaction, 35% browsing, 9% recommendation engagement
- **Recommendation system**: 8.6% of events are recommendation-related (visible, noticed, clicked)
- **Low search dependency**: Only 0.58% search actions - users prefer browsing
- **Conversion**: 0.28% checkout events, 0.06% successful purchases

---

### Timestamp Range

```
Min Timestamp: 1585699201 (2020-03-31 07:58:21 UTC)
Max Timestamp: 1591266092 (2020-06-04 12:21:32 UTC)
Data Coverage: 65 days
```

**Key Details:**
- **Earliest event:** `select_product_option` on March 31, 2020
- **Latest event:** `view_product_detail` on June 4, 2020
- **Format:** Unix timestamp (seconds since epoch)

**Method:** Sort by `time_stamp` field (ascending/descending) and get first document
- **Script:** [06_timestamp_range.js](../scripts/explore_raw_glamira/06_timestamp_range.js)

---

### Product

**Total Distinct Products:** 19,418

**Method:** Count distinct `product_id` values
- **Script:** [09_distinct_product_count.js](../scripts/explore_raw_glamira/09_distinct_product_count.js)

---

### Store Information

**Total Stores:** 86 distinct store IDs

**What is `store_id`?**
Country/region localized websites (Top-Level domains)
- Each store ID maps to a different Glamira domain (glamira.de, glamira.fr, glamira.com,...)
- Glamira operates 86 localized e-commerce sites across different countries/regions

**Top 10 Stores by Event Volume:**

| Store ID | Event Count | % of Total | Domain         | Country/Region     |
|----------|-------------|------------|----------------|--------------------|
| 6        | 5,162,382   | 12.46%     | glamira.de     | Germany            |
| 7        | 3,359,850   | 8.11%      | glamira.co.uk  | United Kingdom     |
| 12       | 2,509,355   | 6.06%      | glamira.fr     | France             |
| 41       | 2,360,349   | 5.70%      | glamira.com    | International (US) |
| 14       | 1,866,741   | 4.51%      | glamira.it     | Italy              |
| 8        | 1,680,873   | 4.06%      | glamira.es     | Spain              |
| 19       | 1,537,060   | 3.71%      | glamira.se     | Sweden             |
| 29       | 1,412,478   | 3.41%      | glamira.com.au | Australia          |
| 51       | 1,378,809   | 3.33%      | glamira.ro     | Romania            |
| 11       | 1,246,968   | 3.01%      | glamira.nl     | Netherlands        |

**Distribution Insights:**
- **Top 3 markets:** Germany (12.46%), UK (8.11%), France (6.06%)
- **Top 10 stores** account for 54.36% of all events
- **Long tail:** 76 stores have <1M events each
- **Store range:** ID 1 to 101 (non-continuous)
- **Smallest stores:** ID 78 and 24 with only 1 event each

**Complete Store List:** 1, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16, 17, 19, 24, 25, 26, 27, 29, 30, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101

**Method:** `db.summary.aggregate([{$group: {_id: "$store_id", count: {$sum: 1}}}, {$sort: {count: -1}}])`
- **Script:** [07_distinct_stores.js](../scripts/explore_raw_glamira/07_distinct_stores.js)

---

*Metrics based on random sampling for performance*

**Why Sampling?**

With 41M+ documents, analyzing the full collection would be slow. We use statistical sampling:

| Analysis          | Sample Size                   |
|-------------------|-------------------------------|
| Field List        | 10 per event type (270 total) |
| Null Rates        | 50,000 (0.12%)                |
| Nested Structures | 10 docs per structure         |



---

## Fields List

**Total Fields:** 57 (Updated 2026-04-08)
- **Top-level fields:** 32
- **Nested fields:** 25 (option.*, option[], cart_products[])

**Sampling Method:** Stratified sampling (100 docs per event type, 2700 total)

**Scripts:**
- [discover_all_fields.py](../scripts/explore_raw_glamira/discover_all_fields.py)
- [export_sample_events.py](../scripts/explore_raw_glamira/export_sample_events.py)



### Field Presence Summary

Quick reference showing which fields appear in which event types.

| Field                                                          | Coverage | Key Event Types                          |
|----------------------------------------------------------------|----------|------------------------------------------|
| **Universal fields (100%)**                                    | 27/27    | All events                               |
| `_id`, `device_id`, `user_id_db`, `email_address`, `ip`        |          |                                          |
| `store_id`, `user_agent`, `resolution`                         |          |                                          |
| `api_version`, `collection`, `time_stamp`, `local_time`        |          |                                          |
| `current_url`, `referrer_url`, `show_recommendation`           |          |                                          |
| **Product fields (22-37%)**                                    | 6-10/27  | Product interaction events               |
| `product_id`, `option`                                         |          | select_option, view_product, add_to_cart |
| **Cart fields (7-11%)**                                        | 2-3/27   | Checkout flow                            |
| `cart_products`, `order_id`                                    |          | checkout, checkout_success, view_cart    |
| **Recommendation fields (4-19%)**                              | 1-5/27   | Recommendation tracking                  |
| `recommendation_product_id`, `recommendation_clicked_position` |          | *_recommendation_* events                |
| **Rare fields (<15%)**                                         | 1-4/27   | Specific contexts                        |
| `utm_source`, `utm_medium`, `key_search`, `currency`, `price`  |          | Varies by event                          |

**Full matrix:** 27 event types × 32 top-level fields (see end of section)

---

### Category 1: User Data (10 fields)

Information about the user, device, session context, and marketing attribution.

#### 1.1 Identity & Session (5 fields)

User identification across devices and sessions.

| Field Name      | MongoDB Type     | BigQuery Type | Coverage | Notes                                        |
|-----------------|------------------|---------------|----------|----------------------------------------------|
| `_id`           | ObjectId         | STRING        | 100%     | MongoDB document ID (convert to string)      |
| `device_id`     | string           | STRING        | 100%     | User device UUID                             |
| `user_id_db`    | string           | STRING        | 100%     | User ID in database (empty if guest)         |
| `email_address` | string           | STRING        | 100%     | User email (empty if not logged in)          |
| `ip`            | string           | STRING        | 100%     | User IP address (always present)             |

#### 1.2 Device & Browser Context (3 fields)

Device and browser information.

| Field Name      | MongoDB Type     | BigQuery Type | Coverage | Notes                                        |
|-----------------|------------------|---------------|----------|----------------------------------------------|
| `user_agent`    | string           | STRING        | 100%     | Browser user agent string                    |
| `resolution`    | string           | STRING        | 100%     | Screen resolution (e.g., "375x667")          |
| `store_id`      | string           | STRING        | 100%     | Store/locale ID (86 distinct values)         |

#### 1.3 Marketing Attribution (2 fields)

UTM parameters for traffic source tracking.

| Field Name      | MongoDB Type     | BigQuery Type | Coverage | Notes                                        |
|-----------------|------------------|---------------|----------|----------------------------------------------|
| `utm_source`    | bool, string     | STRING        | 4%       | UTM source (mixed: bool False or string)     |
| `utm_medium`    | bool, string     | STRING        | 4%       | UTM medium (mixed: bool False or string)     |

### Category 2: Interaction Data (47 fields)

Business events, navigation, products, cart, recommendations, and system metadata.

#### 2.1 System & Metadata (5 fields)

Event tracking and system information.

| Field Name      | MongoDB Type     | BigQuery Type | Coverage | Notes                                        |
|-----------------|------------------|---------------|----------|----------------------------------------------|
| `api_version`   | string           | STRING        | 100%     | Always "1.0"                                 |
| `collection`    | string           | STRING        | 100%     | Event type (27 distinct values)              |
| `time_stamp`    | int              | INT64         | 100%     | Unix timestamp (seconds since epoch)         |
| `local_time`    | string           | STRING        | 100%     | Local timestamp "YYYY-MM-DD HH:MM:SS"        |
| `collect_id`    | string           | STRING        | 15%      | Collection ID (listing/landing pages)        |

#### 2.2 Navigation (3 fields)

Page URLs and navigation flow.

| Field Name            | MongoDB Type | BigQuery Type | Coverage | Notes                                   |
|-----------------------|--------------|---------------|----------|-----------------------------------------|
| `current_url`         | string       | STRING        | 100%     | Current page URL                        |
| `referrer_url`        | string       | STRING        | 100%     | Referrer URL (empty for direct traffic) |
| `show_recommendation` | string       | STRING        | 100%     | "true", "false", or null                |

#### 2.3 Product Interaction (6 fields + nested)

Product viewing, configuration, and pricing.

| Field Name           | MongoDB Type  | BigQuery Type | Coverage | Notes                                   |
|----------------------|---------------|---------------|----------|-----------------------------------------|
| `product_id`         | string        | STRING        | 22%      | Product ID (product interaction events) |
| `viewing_product_id` | string        | STRING        | 15%      | Currently viewed product (rare)         |
| `option`             | array, object | JSON STRING   | 37%      | **Mixed structure** - see nested fields |
| `price`              | string        | STRING        | 4%       | Product price (locale-formatted)        |
| `currency`           | string        | STRING        | 4%       | Currency symbol (€, $, £, kr)           |
| `cat_id`             | null          | STRING        | 15%      | Category ID (mostly null)               |
| `key_search`         | string, null  | STRING        | 4%       | Search keyword                          |

**Note:** `option` has two structures depending on event type:
- **Array structure** (product configuration): Used in `select_product_option`, `add_to_cart_action`
- **Object structure** (filters): Used in `view_listing_page`

##### 2.3.1 Product Options - Array Structure (6 nested fields)

When `option` is an array (product configuration events).

| Field Path                 | MongoDB Type | BigQuery Type | Notes                                        |
|----------------------------|--------------|---------------|----------------------------------------------|
| `option[].option_id`       | string       | STRING        | Option ID (e.g., "105683")                   |
| `option[].option_label`    | string       | STRING        | Option name (alloy, diamond, etc.)           |
| `option[].value_id`        | string       | STRING        | Selected value ID                            |
| `option[].value_label`     | string       | STRING        | Selected value label                         |
| `option[].quality`         | string       | STRING        | Gemstone quality (A, AAA, AAAA, VVS, I)      |
| `option[].quality_label`   | string       | STRING        | Quality label display                        |

##### 2.3.2 Product Options - Object Structure (10 nested fields)

When `option` is an object (browsing/filtering events).

| Field Path                 | MongoDB Type | BigQuery Type | Notes                                        |
|----------------------------|--------------|---------------|----------------------------------------------|
| `option.alloy`             | string       | STRING        | Metal type filter (e.g., "white-silber")     |
| `option.diamond`           | string       | STRING        | Gemstone filter                              |
| `option.shapediamond`      | string       | STRING        | Diamond shape filter                         |
| `option.finish`            | string       | STRING        | Finish type (polished, polished_new)         |
| `option.stone`             | string       | STRING        | Stone count filter                           |
| `option.pearlcolor`        | string       | STRING        | Pearl color filter                           |
| `option.price`             | string       | STRING        | Price filter value                           |
| `option.Kollektion`        | string       | STRING        | Collection filter (URL-encoded)              |
| `option.kollektion_id`     | string       | STRING        | Collection ID                                |
| `option.category id`       | string       | STRING        | Category ID filter                           |

#### 2.4 Shopping Cart & Checkout (3 fields + nested)

Cart contents and checkout flow.

| Field Name      | MongoDB Type         | BigQuery Type      | Coverage | Notes                                        |
|-----------------|----------------------|--------------------|----------|----------------------------------------------|
| `cart_products` | array                | ARRAY<STRUCT>      | 11%      | Array of cart items (see nested fields)      |
| `order_id`      | string, int, float   | STRING             | 7%       | **Mixed types** - use STRING                 |
| `is_paypal`     | null                 | BOOL               | 4%       | PayPal flag (always null in sample)          |

##### 2.4.1 Cart Products - Array Elements (9 nested fields)

Structure of items in `cart_products` array.

| Field Path                              | MongoDB Type  | BigQuery Type | Notes                             |
|-----------------------------------------|---------------|---------------|-----------------------------------|
| `cart_products[].product_id`            | int           | INT64         | Product ID in cart                |
| `cart_products[].amount`                | int           | INT64         | Quantity (typically 1)            |
| `cart_products[].price`                 | string        | STRING        | Item price (locale-formatted)     |
| `cart_products[].currency`              | string        | STRING        | Currency symbol                   |
| `cart_products[].option`                | array, string | JSON STRING   | **Mixed** - array or empty string |
| `cart_products[].option[].option_id`    | int           | INT64         | Option ID                         |
| `cart_products[].option[].option_label` | string        | STRING        | Option label                      |
| `cart_products[].option[].value_id`     | int           | INT64         | Value ID                          |
| `cart_products[].option[].value_label`  | string        | STRING        | Value label                       |

#### 2.5 Recommendation Tracking (5 fields)

Recommendation engine visibility and engagement.

| Field Name                        | MongoDB Type    | BigQuery Type | Coverage | Notes                                        |
|-----------------------------------|-----------------|---------------|----------|----------------------------------------------|
| `recommendation`                  | bool            | BOOL          | 4%       | Recommendation flag                          |
| `recommendation_product_id`       | string, null    | STRING        | 19%      | Recommended product ID                       |
| `recommendation_product_position` | int, string     | STRING        | 11%      | **Mixed types** - position in list           |
| `recommendation_clicked_position` | int, null       | INT64         | 7%       | Position of clicked recommendation           |

---

### Full Field Presence Matrix

Complete matrix showing all 32 top-level fields across 27 event types.


| Event Type         | _id | device_id | user_id_db | email_address | ip | store_id | user_agent | resolution | utm_source | utm_medium | api_version | cart_products | cat_id | collect_id | collection | currency | current_url | is_paypal | key_search | local_time | option | order_id | price | product_id | recommendation | recommendation_clicked_position | recommendation_product_id | recommendation_product_position | referrer_url | show_recommendation | time_stamp | viewing_product_id |
|--------------------|-----|-----------|------------|---------------|----|----------|------------|------------|------------|------------|-------------|---------------|--------|------------|------------|----------|-------------|-----------|------------|------------|--------|----------|-------|------------|----------------|---------------------------------|---------------------------|---------------------------------|--------------|---------------------|------------|--------------------|
| add_to_cart        | ✓   | ✓         | ✓          | ✓             | ✓  | ✓        | ✓          | ✓          |            |            | ✓           |               |        |            | ✓          | ✓        | ✓           | ✓         |            | ✓          | ✓      |          | ✓     | ✓          |                |                                 |                           |                                 | ✓            | ✓                   | ✓          |                    |
| back_to_prod       | ✓   | ✓         | ✓          | ✓             | ✓  | ✓        | ✓          | ✓          |            |            | ✓           |               |        |            | ✓          |          | ✓           |           |            | ✓          |        |          |       | ✓          |                |                                 |                           |                                 | ✓            | ✓                   | ✓          |                    |
| checkout           | ✓   | ✓         | ✓          | ✓             | ✓  | ✓        | ✓          | ✓          |            |            | ✓           | ✓             |        |            | ✓          |          | ✓           |           |            | ✓          |        | ✓        |       |            |                |                                 |                           |                                 | ✓            | ✓                   | ✓          |                    |
| checkout_ok        | ✓   | ✓         | ✓          | ✓             | ✓  | ✓        | ✓          | ✓          |            |            | ✓           | ✓             |        |            | ✓          |          | ✓           |           |            | ✓          |        | ✓        |       |            |                |                                 |                           |                                 | ✓            | ✓                   | ✓          |                    |
| land_rec_click     | ✓   | ✓         | ✓          | ✓             | ✓  | ✓        | ✓          | ✓          |            |            | ✓           |               |        |            | ✓          |          | ✓           |           |            | ✓          |        |          |       |            |                |                                 | ✓                         | ✓                               | ✓            | ✓                   | ✓          |                    |
| land_rec_notice    | ✓   | ✓         | ✓          | ✓             | ✓  | ✓        | ✓          | ✓          |            |            | ✓           |               |        |            | ✓          |          | ✓           |           |            | ✓          |        |          |       |            |                |                                 |                           |                                 | ✓            | ✓                   | ✓          |                    |
| land_rec_vis       | ✓   | ✓         | ✓          | ✓             | ✓  | ✓        | ✓          | ✓          |            |            | ✓           |               |        |            | ✓          |          | ✓           |           |            | ✓          |        |          |       |            |                |                                 |                           |                                 | ✓            | ✓                   | ✓          |                    |
| list_rec_click     | ✓   | ✓         | ✓          | ✓             | ✓  | ✓        | ✓          | ✓          |            |            | ✓           |               | ✓      | ✓          | ✓          |          | ✓           |           |            | ✓          | ✓      |          |       |            |                | ✓                               | ✓                         |                                 | ✓            | ✓                   | ✓          |                    |
| list_rec_notice    | ✓   | ✓         | ✓          | ✓             | ✓  | ✓        | ✓          | ✓          |            |            | ✓           |               | ✓      | ✓          | ✓          |          | ✓           |           |            | ✓          | ✓      |          |       |            |                |                                 |                           |                                 | ✓            | ✓                   | ✓          |                    |
| list_rec_vis       | ✓   | ✓         | ✓          | ✓             | ✓  | ✓        | ✓          | ✓          |            |            | ✓           |               | ✓      | ✓          | ✓          |          | ✓           |           |            | ✓          | ✓      |          |       |            |                |                                 |                           |                                 | ✓            | ✓                   | ✓          |                    |
| prod_rec_click     | ✓   | ✓         | ✓          | ✓             | ✓  | ✓        | ✓          | ✓          |            |            | ✓           |               |        |            | ✓          |          | ✓           |           |            | ✓          |        |          |       |            |                | ✓                               | ✓                         |                                 | ✓            | ✓                   | ✓          | ✓                  |
| prod_rec_notice    | ✓   | ✓         | ✓          | ✓             | ✓  | ✓        | ✓          | ✓          |            |            | ✓           |               |        |            | ✓          |          | ✓           |           |            | ✓          |        |          |       |            |                |                                 |                           |                                 | ✓            | ✓                   | ✓          | ✓                  |
| prod_rec_vis       | ✓   | ✓         | ✓          | ✓             | ✓  | ✓        | ✓          | ✓          |            |            | ✓           |               |        |            | ✓          |          | ✓           |           |            | ✓          |        |          |       |            |                |                                 |                           |                                 | ✓            | ✓                   | ✓          | ✓                  |
| view_all_rec_click | ✓   | ✓         | ✓          | ✓             | ✓  | ✓        | ✓          | ✓          |            |            | ✓           |               |        |            | ✓          |          | ✓           |           |            | ✓          |        |          |       |            |                |                                 | ✓                         | ✓                               | ✓            | ✓                   | ✓          | ✓                  |
| search_box         | ✓   | ✓         | ✓          | ✓             | ✓  | ✓        | ✓          | ✓          |            |            | ✓           |               |        |            | ✓          |          | ✓           |           | ✓          | ✓          |        |          |       |            |                |                                 |                           |                                 | ✓            | ✓                   | ✓          |                    |
| select_option      | ✓   | ✓         | ✓          | ✓             | ✓  | ✓        | ✓          | ✓          |            |            | ✓           |               |        |            | ✓          |          | ✓           |           |            | ✓          | ✓      |          |       | ✓          |                |                                 |                           |                                 | ✓            | ✓                   | ✓          |                    |
| select_quality     | ✓   | ✓         | ✓          | ✓             | ✓  | ✓        | ✓          | ✓          |            |            | ✓           |               |        |            | ✓          |          | ✓           |           |            | ✓          | ✓      |          |       | ✓          |                |                                 |                           |                                 | ✓            | ✓                   | ✓          |                    |
| sort_click         | ✓   | ✓         | ✓          | ✓             | ✓  | ✓        | ✓          | ✓          |            |            | ✓           |               |        |            | ✓          |          | ✓           |           |            | ✓          |        |          |       |            |                |                                 | ✓                         | ✓                               | ✓            | ✓                   | ✓          |                    |
| view_all_rec       | ✓   | ✓         | ✓          | ✓             | ✓  | ✓        | ✓          | ✓          |            |            | ✓           |               |        |            | ✓          |          | ✓           |           |            | ✓          | ✓      |          |       | ✓          |                |                                 |                           |                                 | ✓            | ✓                   | ✓          |                    |
| view_home          | ✓   | ✓         | ✓          | ✓             | ✓  | ✓        | ✓          | ✓          |            |            | ✓           |               |        |            | ✓          |          | ✓           |           |            | ✓          |        |          |       |            |                |                                 |                           |                                 | ✓            | ✓                   | ✓          |                    |
| view_landing       | ✓   | ✓         | ✓          | ✓             | ✓  | ✓        | ✓          | ✓          |            |            | ✓           |               |        |            | ✓          |          | ✓           |           |            | ✓          |        |          |       |            |                |                                 |                           |                                 | ✓            | ✓                   | ✓          |                    |
| view_listing       | ✓   | ✓         | ✓          | ✓             | ✓  | ✓        | ✓          | ✓          |            |            | ✓           |               | ✓      | ✓          | ✓          |          | ✓           |           |            | ✓          | ✓      |          |       |            |                |                                 |                           |                                 | ✓            | ✓                   | ✓          |                    |
| view_account       | ✓   | ✓         | ✓          | ✓             | ✓  | ✓        | ✓          | ✓          |            |            | ✓           |               |        |            | ✓          |          | ✓           |           |            | ✓          |        |          |       |            |                |                                 |                           |                                 | ✓            | ✓                   | ✓          |                    |
| view_product       | ✓   | ✓         | ✓          | ✓             | ✓  | ✓        | ✓          | ✓          | ✓          | ✓          | ✓           |               |        |            | ✓          |          | ✓           |           |            | ✓          | ✓      |          |       | ✓          | ✓              |                                 |                           |                                 | ✓            | ✓                   | ✓          |                    |
| view_cart          | ✓   | ✓         | ✓          | ✓             | ✓  | ✓        | ✓          | ✓          |            |            | ✓           | ✓             |        |            | ✓          |          | ✓           |           |            | ✓          |        |          |       |            |                |                                 |                           |                                 | ✓            | ✓                   | ✓          |                    |
| view_sort          | ✓   | ✓         | ✓          | ✓             | ✓  | ✓        | ✓          | ✓          |            |            | ✓           |               |        |            | ✓          |          | ✓           |           |            | ✓          | ✓      |          |       |            |                |                                 |                           |                                 | ✓            | ✓                   | ✓          |                    |
| view_static        | ✓   | ✓         | ✓          | ✓             | ✓  | ✓        | ✓          | ✓          |            |            | ✓           |               |        |            | ✓          |          | ✓           |           |            | ✓          |        |          |       |            |                |                                 |                           |                                 | ✓            | ✓                   | ✓          |                    |

**Key insights from matrix:**
- **Universal fields (100% coverage):** 14 fields present in all event types
- **Product-specific fields:** `option`, `product_id` present in 10 and 6 event types respectively
- **Cart-specific fields:** `cart_products`, `order_id` only in checkout/cart events
- **Recommendation fields:** Scattered across recommendation-related events (5-19% coverage)
- **Rare fields:** `utm_source`, `utm_medium`, `currency`, `price` appear in only 1 event type each

---

## Sample-based Analysis

### Null/Missing Rates for Key Fields

**Sample Size:** 50,000 documents (0.12% of 41M total)

| Field                | Null Count | Null Rate (%) | Notes                       |
|----------------------|------------|---------------|-----------------------------|
| `ip`                 | 0          | 0.00%         | Always present              |
| `collection`         | 0          | 0.00%         | Always present (event type) |
| `current_url`        | 0          | 0.00%         | Always present              |
| `store_id`           | 0          | 0.00%         | Always present              |
| `time_stamp`         | 0          | 0.00%         | Always present              |
| `referrer_url`       | 4,599      | 9.20%         | Missing for direct traffic  |
| `product_id`         | 23,051     | 46.10%        | Only for product events     |
| `viewing_product_id` | 47,565     | 95.13%        | Rare field                  |

**Key Observations:**
- **Mandatory fields** (0% null): `ip`, `collection`, `current_url`, `store_id`, `time_stamp`
- **Contextual field**: `product_id` (~46% null)
  - may only present for product-related events like `view_product_detail`, `add_to_cart_action`
- **Rarely used**: `viewing_product_id` (~95% null) - special tracking field
- **Direct traffic**: `referrer_url` (~9% null) - missing when users arrive directly

**Method:** Random sampling with null rate calculation
- **Script:** [05_null_rates.py](../scripts/explore_raw_glamira/05_null_rates.py)

---

### Nested Field Structures

- **Method:** Sample-based structure analysis
- **Script:** [08_nested_structures.js](../scripts/explore_raw_glamira/08_nested_structures.js)


#### Field: `option`

**Type:** Array or Object (varies by context)

**Array Structure** - Used for product configuration events:
```json
[
  {
    "option_label": "alloy",
    "option_id": "187492",
    "value_label": "red-750",
    "value_id": "1593626"
  },
  {
    "option_label": "stone/diamonds",
    "option_id": "57695",
    "value_label": "diamond-Brillant",
    "value_id": "308213",
    "quality": "A",              // Optional - for gemstones
    "quality_label": "I"         // Optional - quality grade
  }
]
```

**Object Structure** - Used for filtering/browsing:
```json
{
  "alloy": "",
  "diamond": "diamond-Brillant",
  "shapediamond": ""
}
```

**Key Differences:**
- **Array format:** Detailed product configuration with IDs - used in `select_product_option`, `add_to_cart_action`, `checkout`
- **Object format:** Simple key-value filters - used in `view_listing_page` for category browsing
- **Quality fields:** Only present for some options like diamond (quality grades like AAAA, VVS, I, etc.)
- **Empty values:** Can be empty string `""` when option not selected

**Common Options:**
- `alloy`: Metal type (e.g., "white-585", "red-750", "yellow-375")
- `diamond`: Gemstone type (e.g., "diamond-Brillant", "ruby", "sapphire")
- `shapediamond`: Diamond shape (e.g., "round", "princess")
- `stone/diamonds`: Combined gemstone option

---

#### Field: `cart_products`

**Type:** Array of objects

**Present in:** `checkout`, `checkout_success`, `view_shopping_cart`

**Structure:**
```json
[
  {
    "product_id": 103324,
    "amount": 1,              // Optional (quantity)
    "price": "880.00",        // Optional (locale-formatted)
    "currency": "£",          // Optional (symbol)
    "option": [
      {
        "option_label": "diamond",
        "option_id": 261151,
        "value_label": "Swarovsky Cristall",
        "value_id": 2166253
      },
      {
        "option_label": "alloy",
        "option_id": 261154,
        "value_label": "Weißgold 585",
        "value_id": 2166328
      }
    ]
  }
]
```

**Key Characteristics:**
- **Required field:** Only `product_id` is always present
- **Optional fields:** `amount`, `price`, `currency` may be missing
- **Option structure:** Same array format as product configuration events
- **Multiple items:** Can contain multiple products (including duplicates)
- **Empty cart:** Can be empty array `[]` when no items in cart

**Examples:**

*Single item cart:*
```json
[{"product_id": 91041, "option": [...]}]
```

*Multi-item cart:*
```json
[
  {"product_id": 97471, "option": [...]},
  {"product_id": 98502, "option": [...]},
  {"product_id": 98497, "option": [...]}
]
```

*Cart with duplicates (same product added multiple times):*
```json
[
  {"product_id": 90475, "option": [...]},
  {"product_id": 90475, "option": [...]},
  {"product_id": 90476, "option": [...]}
]
```

**Fields:**
- `product_id`: int (product identifier)
- `amount`: int (quantity, typically 1 if present)
- `price`: string (decimal with locale formatting)
- `currency`: string (symbol: €, £, $, etc.)
- `option`: array of option objects (same structure as Field: `option`)


---

### Sample Documents by Event Type

To get sample documents for each event type:

```bash
# Python (with logging)
python scripts/explore_raw_glamira/02_sample_documents.py

# MongoDB shell
db.summary.findOne({ collection: "add_to_cart_action" })
db.summary.findOne({ collection: "checkout_success" })
```

**Script:** [08_nested_structures.js](../scripts/explore_raw_glamira/08_nested_structures.js)

---

## Notes

### Data Quality
- **Timestamps:** Unix epoch format (seconds)
- **URL encoding:** Special characters properly encoded
- **Currency symbols:** UTF-8 strings (€, £, $, kr)
- **Empty values:** Both `[]` and `null` used

### Performance Tips
- Full collection queries (41M docs) can be slow
- Use sampling for exploration
- Use `estimated_document_count()` for total count
- Indexes exist on: `time_stamp`, `collection`, `store_id`

### How to Reproduce

All queries can be reproduced using:
```
scripts/explore_raw_glamira/
  ├── *.py  - Python version (with logging)
  └── *.js  - MongoDB shell version
```
