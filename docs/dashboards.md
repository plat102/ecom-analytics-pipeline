# Looker Studio Dashboards

Interactive dashboards for e-commerce analytics.

---

## Dashboard 1: Revenue Overview

**Purpose:** Executive summary of sales performance

**Key Metrics:** Total revenue, orders, AOV, date range

**Visualizations:**
- Scorecards (revenue, orders, AOV)
- Revenue trend with average line
- Top stores by revenue
- Guest vs Registered orders
- Day of week analysis
- Mobile vs Desktop revenue
- Weekly breakdown table
- Revenue by country (geo map + table)

**Screenshot:** [`dashboard/snapshots/1_revenue-overview.pdf`](../dashboard/snapshots/1_revenue-overview.pdf)

---

## Dashboard 2: Product Performance

**Purpose:** Product catalog analysis and pricing insights

**Key Metrics:** Revenue, products, collections, gender split

**Visualizations:**
- Scorecards (revenue, products, collections)
- Top products by revenue
- Top collections by revenue
- Units sold by price bucket
- Category breakdown table
- Sales by product gender
- Product type trend (stacked area chart)
- Price vs Revenue scatter plot
- Gold weight vs Revenue scatter plot

**Screenshot:** [`dashboard/snapshots/2_product.pdf`](../dashboard/snapshots/2_product.pdf)

---

## Dashboard 3: Customer Behavior

**Purpose:** Customer segmentation and retention analysis

**Key Metrics:** Customers, LTV, registered/guest split, repeat purchase rate

**Visualizations:**
- Scorecards (customers, LTV, days since last order)
- Guest vs Registered customers
- Customer device preference
- Top email domains
- Top countries by customer count (geo map + table)
- Customer lifecycle distribution
- Repeat purchase rate breakdown
- Top customers by LTV table
- Customer acquisition trend

**Screenshot:** [`dashboard/snapshots/3_customer.pdf`](../dashboard/snapshots/3_customer.pdf)

---

## Technical Details

**Data Sources:**
- Dashboard 1: `mart.mart_sales_daily` + `mart.mart_sales_by_geography`
- Dashboard 2: `mart.mart_sales_by_product`
- Dashboard 3: `mart.mart_customer_summary`

**Data Quality:**
- All dashboards filter outliers using `is_outlier = FALSE`
- Date range: March 30, 2020 - June 5, 2020 (65 days)
