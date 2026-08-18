# Databricks Fintech Pipeline — Medallion Architecture

A complete end-to-end data engineering pipeline built on **Databricks Free Edition**, simulating a fintech platform's transaction processing system. Covers the full medallion architecture (bronze → silver → gold) with Unity Catalog governance, Delta Live Tables for declarative ETL, automated Workflows, and fraud detection.

**Purpose:** Learn Databricks data engineering by building a realistic pipeline — not toy examples, but production-shaped infrastructure with intentional data quality issues, proper governance, and orchestrated jobs.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                        Databricks Free Edition                           │
│                                                                          │
│  ┌────────────┐    ┌────────────┐    ┌────────────────────────────────┐  │
│  │  01: Data  │───▶│ 02: Bronze │───▶│       DLT Pipeline (03+04)    │  │
│  │ Generation │    │ Ingestion  │    │                                │  │
│  │  (Python)  │    │ (JSON→Δ)   │    │  ┌─────────┐    ┌──────────┐  │  │
│  └────────────┘    └────────────┘    │  │ 03:     │───▶│ 04:      │  │  │
│        │                              │  │ Silver  │    │ Gold     │  │  │
│        ▼                              │  │ (clean) │    │ (agg)    │  │  │
│  ┌────────────┐                      │  └─────────┘    └──────────┘  │  │
│  │  Volumes   │                      └───────────┬───────────────────┘  │
│  │ (landing   │                                  │                      │
│  │  zone)     │                    ┌─────────────┼─────────────┐        │
│  └────────────┘                    ▼             ▼             ▼        │
│                             ┌───────────┐ ┌───────────┐ ┌───────────┐  │
│                             │ 05: Fraud │ │ 06: SQL   │ │ Dashboards│  │
│                             │ Detection │ │ Analytics │ │ & Genie   │  │
│                             └───────────┘ └───────────┘ └───────────┘  │
│                                                                          │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │ 07: Workflow Orchestration (daily schedule, task dependencies)     │  │
│  └───────────────────────────────────────────────────────────────────┘  │
│                                                                          │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │ Unity Catalog: fintech_lab.{bronze, silver, gold}                 │  │
│  └───────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Data Model

| Entity | Count | Description |
|--------|------:|-------------|
| **Customers** | 5,000 | Demographics, KYC status, risk tier |
| **Accounts** | 12,000 | Checking, savings, credit cards, investments (multi-account per customer) |
| **Transactions** | 250,000 | 6 months of purchases, transfers, deposits, ATM withdrawals, fees, interest |
| **Merchants** | 2,000 | Merchant profiles with MCC (Merchant Category Code) classifications |
| **Exchange Rates** | 180 days | Daily USD/EUR/GBP cross rates |

### Entity Relationships

```
Customers 1──────┐
    │             │
    │ 1:N         │ 1:N (via account)
    ▼             ▼
Accounts ──── Transactions ────── Merchants
    │              │
    │              │ N:1
    │              ▼
    │         Exchange Rates
    │         (currency conversion)
    │
    └──── credit_limit, interest_rate, status
```

---

## Medallion Architecture Layers

### Bronze — Raw Ingestion

> Philosophy: **Append-only, no transforms, preserve the mess.**

Raw JSON files land in a Unity Catalog Volume, then get batch-loaded into Delta tables with ingestion metadata. No data cleaning happens here — bronze is the audit trail.

| Table | Source |
|-------|--------|
| `fintech_lab.bronze.raw_customers` | `customers/customers.json` |
| `fintech_lab.bronze.raw_accounts` | `accounts/accounts.json` |
| `fintech_lab.bronze.raw_transactions` | `transactions/transactions_*.json` |
| `fintech_lab.bronze.raw_merchants` | `merchants/merchants.json` |
| `fintech_lab.bronze.raw_exchange_rates` | `exchange_rates/rates.json` |

Every row gets two metadata columns:
- `_ingested_at` — ingestion timestamp
- `_source_file` — originating file path

### Silver — Cleaned & Validated

> Philosophy: **Deduplicated, typed, validated, standardized. One trusted version of each entity.**

Handled by Delta Live Tables with declarative expectations:

| Table | Key Transforms |
|-------|---------------|
| `silver.customers` | Dedup on `customer_id`, parse mixed date formats, standardize casing, validate emails |
| `silver.accounts` | Dedup on `account_id`, validate FK to customers, cast types |
| `silver.transactions` | Dedup on `transaction_id`, validate amounts, parse timestamps, filter future dates |
| `silver.merchants` | Dedup on `merchant_id`, standardize casing |
| `silver.exchange_rates` | Dedup on composite key, cast rate to decimal |
| `silver.transactions_enriched` | Join transactions + merchants + accounts for downstream consumption |

### Gold — Business-Ready Aggregates

> Philosophy: **Pre-computed, denormalized, optimized for analysts and dashboards.**

| Table | Description |
|-------|-------------|
| `gold.customer_360` | Wide customer view: all accounts, lifetime value, activity metrics, tenure, preferred channel |
| `gold.daily_transaction_summary` | Daily aggregates by type, channel, currency |
| `gold.merchant_analytics` | Per-merchant volume, transaction count, unique customers, avg ticket, refund rate |
| `gold.fraud_risk_scores` | Rule-based scoring: velocity, amount anomaly, geo-impossibility, dormant spikes |
| `gold.customer_segments` | RFM segmentation (Recency, Frequency, Monetary) with labeled segments |
| `gold.monthly_revenue` | Fee + interest revenue by account type, month over month |

---

## Data Quality Issues (Intentional)

The synthetic data generator injects 10 realistic quality problems. Each one teaches a specific cleaning technique in the silver layer:

| # | Issue | % | Silver Fix |
|---|-------|--:|------------|
| 1 | Duplicate transactions (same `transaction_id`) | ~5% | `ROW_NUMBER()` window dedup |
| 2 | Null `merchant_id` on purchase transactions | ~3% | DLT `@dlt.expect` flagging |
| 3 | Negative amounts on non-refund transactions | ~2% | `@dlt.expect_or_drop` + filter |
| 4 | Future-dated transactions | ~1% | Date validation against `current_date()` |
| 5 | Mixed date formats (`YYYY-MM-DD`, `MM/DD/YYYY`, `DD-MM-YYYY`) | ~100% of DOB | `coalesce(to_date(..., fmt1), to_date(..., fmt2), ...)` |
| 6 | Inconsistent casing (`"new york"`, `"New York"`, `"NEW YORK"`) | ~30% | `initcap()` standardization |
| 7 | Invalid email addresses | ~2% | Regex validation with `rlike` |
| 8 | Phone numbers in mixed formats | ~100% | Digit extraction / standardization |
| 9 | Orphan accounts (FK to nonexistent customer) | ~1% | Left anti-join FK validation |
| 10 | Floating-point artifacts (`29.990000000001`) | ~10% | `round(amount, 2)` |

---

## Notebooks

| # | Notebook | Purpose | Layer | Runtime |
|---|----------|---------|-------|---------|
| 00 | [`00_setup_catalog.py`](notebooks/00_setup_catalog.py) | Create Unity Catalog: catalog, schemas, volume | Setup | Serverless |
| 01 | [`01_generate_data.py`](notebooks/01_generate_data.py) | Generate 250K+ synthetic records with quality issues | Source | Serverless |
| 02 | [`02_bronze_ingestion.py`](notebooks/02_bronze_ingestion.py) | Batch ingest JSON → Delta tables | Bronze | Serverless |
| 03 | [`03_dlt_bronze_to_silver.py`](notebooks/03_dlt_bronze_to_silver.py) | DLT: dedup, clean, validate, enrich | Silver | DLT Pipeline |
| 04 | [`04_dlt_silver_to_gold.py`](notebooks/04_dlt_silver_to_gold.py) | DLT: aggregate, score, segment | Gold | DLT Pipeline |
| 05 | [`05_fraud_detection.py`](notebooks/05_fraud_detection.py) | Advanced rule-based fraud engine (6 detection patterns) | Gold+ | Serverless |
| 06 | [`06_analytics_queries.sql`](notebooks/06_analytics_queries.sql) | 10 BI queries for dashboards | Analytics | SQL Warehouse |
| 07 | [`07_workflow_orchestration.py`](notebooks/07_workflow_orchestration.py) | Workflow DAG config + monitoring setup | Ops | Serverless |

---

## Getting Started

### Prerequisites

- A **Databricks Free Edition** account — sign up at [databricks.com/try-databricks](https://www.databricks.com/try-databricks)
- No local tools needed — everything runs inside Databricks

### Step-by-Step Setup

#### 1. Import Notebooks

```
Workspace → Home → ⋮ menu → Import
→ Upload all .py and .sql files from notebooks/
```

Or drag-and-drop the files into your Databricks workspace. They're in standard Databricks notebook source format and will be recognized automatically.

#### 2. Create the Catalog (Notebook 00)

Run `00_setup_catalog`. This creates:

```sql
CREATE CATALOG IF NOT EXISTS fintech_lab;
CREATE SCHEMA IF NOT EXISTS fintech_lab.bronze;
CREATE SCHEMA IF NOT EXISTS fintech_lab.silver;
CREATE SCHEMA IF NOT EXISTS fintech_lab.gold;
CREATE VOLUME IF NOT EXISTS fintech_lab.bronze.landing_zone;
```

#### 3. Generate Synthetic Data (Notebook 01)

Run `01_generate_data`. Takes ~2–3 minutes. Writes JSON Lines files to the volume:

```
/Volumes/fintech_lab/bronze/landing_zone/
├── customers/customers.json
├── accounts/accounts.json
├── transactions/transactions_20260218.json  (one per day, 180 files)
├── merchants/merchants.json
└── exchange_rates/rates.json
```

#### 4. Ingest to Bronze (Notebook 02)

Run `02_bronze_ingestion`. Reads from the volume and writes Delta tables to `fintech_lab.bronze.*`.

#### 5. Create the DLT Pipeline (Notebooks 03 + 04)

This is the core of the project — a **Delta Live Tables** pipeline:

1. Go to **Workflows → Delta Live Tables → Create Pipeline**
2. Configure:

| Setting | Value |
|---------|-------|
| Pipeline name | `fintech-medallion-pipeline` |
| Source code | Select notebooks `03_dlt_bronze_to_silver` and `04_dlt_silver_to_gold` |
| Target catalog | `fintech_lab` |
| Pipeline mode | **Triggered** (manual runs for learning; switch to Continuous for streaming) |
| Compute | Serverless |

3. Click **Start** — the pipeline builds the DAG from your `@dlt.table` definitions and runs bronze → silver → gold.
4. Explore the **lineage graph** in the pipeline UI to see table dependencies.

#### 6. Run Fraud Detection (Notebook 05)

After the DLT pipeline completes, run `05_fraud_detection`. This applies 6 advanced fraud rules and writes to `fintech_lab.gold.fraud_alerts`.

#### 7. Explore Analytics (Notebook 06)

Open `06_analytics_queries` in a **SQL Warehouse** to run the 10 business queries. These are ready to wire into a Databricks SQL Dashboard.

#### 8. Set Up Automated Workflow (Notebook 07)

Run `07_workflow_orchestration` to create a multi-task Databricks Workflow that chains everything with proper dependencies and scheduling.

---

## Key Databricks Features Demonstrated

| Feature | Where Used | What You Learn |
|---------|-----------|----------------|
| **Unity Catalog** | All notebooks | Three-level namespace (`catalog.schema.table`), governance, volumes |
| **Delta Lake** | Bronze ingestion, all layers | ACID transactions, time travel, schema evolution, `OPTIMIZE` / `VACUUM` |
| **Delta Live Tables** | Notebooks 03 + 04 | Declarative ETL, `@dlt.table`, expectations, lineage graphs |
| **DLT Expectations** | Silver layer | `@dlt.expect`, `@dlt.expect_or_drop`, `@dlt.expect_or_fail` |
| **Workflows / Jobs** | Notebook 07 | Multi-task DAGs, dependencies, scheduling, retry policies |
| **SQL Warehouse** | Notebook 06 | BI-optimized queries, dashboard-ready analytics |
| **Volumes** | Notebooks 01 + 02 | Managed file storage for raw data landing zone |
| **Serverless Compute** | All Python notebooks | On-demand compute with no cluster management |

---

## Fraud Detection Rules

The fraud engine (notebook 05) implements 6 detection patterns:

| Rule | Pattern | Severity |
|------|---------|----------|
| **Velocity** | > 10 transactions in 24h rolling window | High |
| **Amount Anomaly** | > 3σ above account's 90-day mean | High |
| **Geo-Impossibility** | Different countries within 2 hours | Critical |
| **Dormant Spike** | No activity for 60+ days, then > $1,000 | Medium |
| **Round Amounts** | > 3 consecutive round-number transactions | Medium |
| **Merchant Concentration** | > 80% of 7-day spend at single merchant | Low |

Each flagged transaction gets a composite score and risk label (`low` / `medium` / `high` / `critical`).

---

## Extending This Pipeline

Once you've run through the base pipeline, here are progressions to deepen your Databricks skills:

| Extension | Skill Area | Difficulty |
|-----------|-----------|------------|
| Add **Auto Loader** for streaming bronze ingestion | Structured Streaming | ⭐⭐ |
| Build a **Databricks SQL Dashboard** from notebook 06 queries | BI / Visualization | ⭐ |
| Train a **fraud detection ML model** with MLflow | ML Engineering | ⭐⭐⭐ |
| Implement **SCD Type 2** for customer dimension tracking | Advanced ETL | ⭐⭐⭐ |
| Add **column masking** for PII (email, phone) in Unity Catalog | Data Governance | ⭐⭐ |
| Set up **Databricks Asset Bundles (DABs)** for CI/CD | DevOps | ⭐⭐⭐ |
| Connect **Genie** for natural language querying of gold tables | AI/BI | ⭐ |
| Add a **data contract** layer with expectations + alerting | Data Mesh | ⭐⭐⭐ |
| Backfill with **Delta time travel** and replay bronze ingestion | Delta Lake | ⭐⭐ |

---

## Free Edition — Limits & Tips

| Topic | Guidance |
|-------|---------|
| **Compute caps** | Serverless has monthly usage limits — batch your development sessions, don't leave notebooks idling |
| **Unity Catalog** | Fully available — use it from day one, don't skip it |
| **DLT pipelines** | May queue if compute pool is saturated — schedule off-peak or use Triggered mode |
| **Delta time travel** | 30-day retention by default — use `DESCRIBE HISTORY` to explore, `VACUUM` to reclaim storage |
| **Storage** | Monitor with `SHOW TABLES IN fintech_lab.bronze EXTENDED` — Delta logs grow; run `OPTIMIZE` periodically |
| **Workflows** | Available in Free Edition — leverage scheduling instead of manual notebook runs |
| **SQL Warehouse** | Serverless SQL available — use for analytics queries (notebook 06) for best performance |

### Cost-Saving Habits

1. **Don't leave clusters running** — serverless auto-terminates, but watch for idle notebook connections
2. **Use `OPTIMIZE` + `VACUUM`** after large writes to compact small files and reclaim storage
3. **Schedule off-peak** — workflow runs are cheaper when compute pool is less contended
4. **Delta caching** — repeated reads of the same table are faster; structure queries to reuse cached data

---

## Project Structure

```
experiments/databricks-fintech/
├── README.md                 ← you are here
└── notebooks/
    ├── 00_setup_catalog.py
    ├── 01_generate_data.py
    ├── 02_bronze_ingestion.py
    ├── 03_dlt_bronze_to_silver.py
    ├── 04_dlt_silver_to_gold.py
    ├── 05_fraud_detection.py
    ├── 06_analytics_queries.sql
    └── 07_workflow_orchestration.py
```

All notebooks use the Databricks notebook source format (`.py` with `# COMMAND ----------` cell separators) and can be imported directly into any Databricks workspace.
