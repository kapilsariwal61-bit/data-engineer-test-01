# Airbnb Data Engineering Solution

## 1. Overview

This project builds a small analytical data warehouse for Airbnb listings and reviews.

**Tech stack**

- Data warehouse: **Postgres**
- Orchestration & ETL: **Python + Pandas**
- Source data: `listings.csv`, `reviews.csv`
- Warehouse model: **star schema**
- Pipeline: **Extract → Validate → Transform → Load → Orchestrator**
- Analytics: 3 SQL queries for pricing, host performance, and market opportunities

The goal is to support pricing intelligence, host ranking, and market opportunity analysis on top of the Airbnb dataset.

---

## 2. Data Model (Star Schema)

### 2.1 Grain & Fact Table

**Fact table: `fact_reviews`**

- **Grain**: one row = **one review for a listing on a specific date**.
- Columns:
  - `review_sk` – surrogate key
  - `listing_sk` – FK → `dim_listing`
  - `host_sk` – FK → `dim_host`
  - `date_sk` – FK → `dim_date`
  - `review_count` – always `1` per row (used for aggregations)

Rationale:

- Reviews are a good proxy for demand.
- This grain lets us easily aggregate demand by host, listing, neighbourhood, time, etc.

### 2.2 Dimensions

#### `dim_date`

- One row per calendar date.
- Columns: `date_sk`, `full_date`, `year`, `month`, `day`, `month_name`, `day_of_week`, `day_name`.
- Used for time-based aggregations (month/year trends, “last 12 months”, etc.).

#### `dim_neighborhood`

- One row per unique `(neighbourhood_group, neighbourhood)` pair.
- Columns: `neighborhood_sk`, `neighbourhood_group`, `neighbourhood`.
- Normalizes repeated text values out of the listing dimension.

#### `dim_host` (SCD Type 2 ready)

- Natural key from source: `host_id`.
- Columns:
  - `host_sk` (surrogate key)
  - `host_id`
  - `host_name`
  - `calculated_host_listings_count`
  - `valid_from`, `valid_to`, `is_current`
- In the current dataset, there is a single “snapshot” for each host, but the table is modeled as **SCD Type 2** so it can track historical changes in host attributes over time (e.g., host name changes, portfolio size changes).

#### `dim_listing`

- One row per listing.
- Columns:
  - Keys: `listing_sk`, `listing_id`, `host_id`, `neighborhood_sk`
  - Attributes: `name`, `room_type`, `price`, `minimum_nights`,
    `availability_365`, `number_of_reviews`, `reviews_per_month`,
    `number_of_reviews_ltm`, `license`, `latitude`, `longitude`
- Listing-level attributes used for pricing and inventory analysis.

---

## 3. ETL / Pipeline Design

The pipeline is implemented under `src/pipeline`:

- `extract.py`
- `validate.py`
- `transform.py`
- `load.py`
- `orchestrator.py`

### 3.1 Extract

**File:** `src/pipeline/extract.py`

- Reads raw CSV files using paths from `config.yaml`:
  - `data/listings.csv`
  - `data/reviews.csv`
- Returns two Pandas DataFrames: `listings`, `reviews`.
- Minimal logic: no transformation, just ingestion.

### 3.2 Validate

**File:** `src/pipeline/validate.py`

Data quality checks applied:

- **Schema checks**
  - Verify expected columns exist in both listings and reviews.
  - Drop any unexpected extra columns.

- **Null checks**
  - `listings.id` and `reviews.listing_id` must be non-null.
  - Rows with null IDs are dropped.

- **Uniqueness**
  - `listings.id` must be unique.
  - Duplicates are dropped (first occurrence retained).

- **Range checks**
  - `latitude` ∈ [-90, 90]
  - `longitude` ∈ [-180, 180]
  - `availability_365` ∈ [0, 365]
  - Rows failing these checks are dropped.

- **Foreign key check**
  - Every `reviews.listing_id` must exist in `listings.id`.
  - Orphan reviews are dropped.

A summary of all checks is written to:

- `output/data_quality_report.json`

This file includes row counts after validation and details per check (status, counts, etc.).

### 3.3 Transform

**File:** `src/pipeline/transform.py`

Key transformations:

- **Cleaning & type conversions**
  - `price`: remove currency symbols/commas and cast to numeric.
  - Numeric fields (`minimum_nights`, `number_of_reviews`, `reviews_per_month`, `number_of_reviews_ltm`) converted to numeric with safe handling of missing values.

- **Feature engineering**
  - A simple proxy for occupancy can be derived from `reviews_per_month` (e.g., scaled to [0, 1]); this can be used for rough revenue estimates.
  - (Optional) simple price banding into tiers (budget/mid/premium) if included.

- **Building dimensions**
  - `dim_date`: from unique review dates.
  - `dim_neighborhood`: from unique `(neighbourhood_group, neighbourhood)` pairs.
  - `dim_host`: distinct hosts with SCD2 columns (`valid_from`, `valid_to`, `is_current`).
  - `dim_listing`: listings joined with `dim_neighborhood` and enriched with clean numeric fields.

- **Building fact table**
  - `fact_reviews`:
    - Join `reviews` with `dim_listing` to get `listing_sk` and `host_id`.
    - Join with `dim_host` to get `host_sk`.
    - Join with `dim_date` to get `date_sk`.
    - Set `review_count = 1` for each row.

### 3.4 Load

**File:** `src/pipeline/load.py`

- Connects to Postgres using SQLAlchemy (`src/utils/db_connector.py`).
- Implements a **full-refresh** strategy:
  - In a transaction:
    - `TRUNCATE` fact and dimension tables.
    - `RESTART IDENTITY` to keep surrogate keys consistent.
  - Inserts dimension tables first, then `fact_reviews`.
- This makes the pipeline **idempotent**:
  - Re-running the pipeline with the same input leads to the same final warehouse state, no duplicate facts.

### 3.5 Orchestration & Logging

**File:** `src/pipeline/orchestrator.py`  
**Logging:** `src/utils/logger.py`

- `run_pipeline()` orchestrates all steps:

  1. `run_extract`
  2. `run_validate`
  3. `run_transform`
  4. `run_load`

- Logs are written to:

  - `logs/pipeline_execution.log`

  including start/end messages and record counts.

- Configuration (`data paths`, `table names`, `validation thresholds`, `log path`) is centralized in `src/config/config.yaml`.

---

## 4. Data Quality Strategy

- Invalid or inconsistent data is **cleaned or dropped**, not allowed to break the pipeline.
- All critical decisions (dropped rows, invalid ranges, orphans) are:
  - Logged via the logger.
  - Summarized in `data_quality_report.json`.
- This makes the pipeline robust to real-world messy CSV files.

---

## 5. Analytics / Business Queries

All queries live under `sql/queries/`.

### 5.1 Pricing Intelligence – `01_pricing_intelligence.sql`

**Objective:**  
Identify over- and under-priced listings relative to similar listings.

**Logic:**

- Peer group defined by:
  - `neighbourhood_group`
  - `neighbourhood`
  - `room_type`
- For each listing:
  - Compute peer group average price using window functions.
  - Compute `price_difference_pct` relative to the group average.
- Classification:
  - `price_difference_pct <= -20%` → `underpriced`
  - `price_difference_pct >= 20%` → `overpriced`
  - Otherwise → `fair`

**Output:**

- `listing_id`, `name`, `neighbourhood_group`, `neighbourhood`, `room_type`,
  `current_price`, `market_average`, `price_difference_pct`, `recommendation`.

### 5.2 Host Performance Ranking – `02_host_performance.sql`

**Objective:**  
Rank hosts using a composite performance score.

**Components:**

- **Portfolio size** – number of listings per host.
- **Estimated revenue** – based on listing price and review metrics.
- **Review activity** – number of reviews in the recent period (based on available data).

**Scoring:**

- Normalize each component across hosts to [0,1].
- Weighted composite score:

  - 0.5 × revenue score  
  - 0.3 × review activity score  
  - 0.2 × portfolio size score

- Use `RANK()` window function to assign ranking.

**Output:**

- `host_id`, `host_name`, `performance_score`, `ranking`,
- `key_metrics_breakdown` (JSON with components and raw metrics).

### 5.3 Market Opportunity Analysis – `03_market_opportunities.sql`

**Objective:**  
Identify neighbourhoods where demand is high relative to supply.

**Definitions:**

- **Demand score**: total number of reviews (`fact_reviews`) per neighbourhood.
- **Supply score**: number of active listings (`availability_365 > 0`) per neighbourhood.
- **Opportunity score**:
  - `demand_per_listing = demand / supply`
  - scaled by a factor (×10) for readability.

**Classification:**

- `opportunity_score >= 50` & `active_listings < 50` → `increase_supply`
- `opportunity_score >= 50` & `active_listings >= 50` → `high_demand_competitive`
- `20 ≤ opportunity_score < 50` → `moderate_opportunity`
- else → `low_demand`

**Output:**

- `neighbourhood_group`, `neighbourhood`,
- `demand_score`, `supply_score`, `opportunity_score`, `recommended_action`.

---