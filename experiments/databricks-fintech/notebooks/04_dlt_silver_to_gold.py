# Databricks notebook source
# ruff: noqa: E402, F821

# MAGIC %md
# MAGIC # 04 — Delta Live Tables: Silver → Gold
# MAGIC
# MAGIC This notebook builds the **gold layer** — pre-aggregated, business-ready tables
# MAGIC optimized for analysts and BI dashboards.
# MAGIC
# MAGIC ## Gold-layer philosophy
# MAGIC
# MAGIC | Principle | What it means here |
# MAGIC |---|---|
# MAGIC | **Business language** | Column names match what a product manager or analyst would say |
# MAGIC | **Pre-aggregated** | Aggregations are done once at write time, not at every query |
# MAGIC | **Denormalized** | Wide tables — no joins needed at query time |
# MAGIC | **Idempotent** | Re-running produces the same result (no double-counting) |
# MAGIC | **SLA-ready** | Dashboards read from gold, so these tables must be reliable |
# MAGIC
# MAGIC ## Tables in this layer
# MAGIC
# MAGIC | Table | Purpose |
# MAGIC |---|---|
# MAGIC | `customer_360` | One row per customer — everything you need to know |
# MAGIC | `daily_transaction_summary` | Daily aggregates by type and channel |
# MAGIC | `merchant_analytics` | Per-merchant volume, ticket size, refund rate |
# MAGIC | `fraud_risk_scores` | Rule-based risk score per transaction |
# MAGIC | `customer_segments` | RFM segmentation with human-readable labels |
# MAGIC | `monthly_revenue` | Monthly fee + interest revenue by account type |
# MAGIC
# MAGIC ---

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, lit, when, count, sum as spark_sum, avg, min as spark_min, max as spark_max,
    countDistinct, round as spark_round, datediff, current_date, first, concat_ws, collect_set, date_format, dense_rank, ntile,
    coalesce, expr,
)
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: Customer 360°
# MAGIC
# MAGIC A **wide denormalized view** with one row per customer.  Joins customer
# MAGIC demographics, account summary, and transaction aggregates into a single table
# MAGIC that answers questions like:
# MAGIC
# MAGIC - *How many accounts does this customer have?*
# MAGIC - *What is their total lifetime spend?*
# MAGIC - *When were they last active?*
# MAGIC - *What channel do they prefer?*

# COMMAND ----------

@dlt.table(
    name="customer_360",
    comment="One-row-per-customer denormalized view with accounts and transaction metrics",
    table_properties={"quality": "gold"},
)
def customer_360():
    customers = dlt.read("customers")
    accounts = dlt.read("accounts")
    txn_enriched = dlt.read("transactions_enriched")

    # --- Account summary per customer -----------------------------------------
    acct_summary = (
        accounts
        .groupBy("customer_id")
        .agg(
            count("account_id").alias("total_accounts"),
            countDistinct("account_type").alias("distinct_account_types"),
            collect_set("account_type").alias("account_types_held"),
            spark_sum(
                when(col("account_type") == "credit_card", col("credit_limit"))
                .otherwise(lit(0))
            ).cast(DecimalType(14, 2)).alias("total_credit_limit"),
        )
    )

    # --- Transaction summary per customer -------------------------------------
    txn_summary = (
        txn_enriched
        .filter(col("status") == "completed")
        .groupBy("customer_id")
        .agg(
            count("transaction_id").alias("lifetime_transaction_count"),
            spark_round(spark_sum("amount"), 2).alias("lifetime_transaction_value"),
            spark_round(avg("amount"), 2).alias("avg_transaction_amount"),
            spark_min("transaction_date").alias("first_transaction_date"),
            spark_max("transaction_date").alias("last_transaction_date"),
        )
    )

    # --- Preferred channel (mode) per customer --------------------------------
    channel_counts = (
        txn_enriched
        .filter(col("status") == "completed")
        .groupBy("customer_id", "channel")
        .agg(count("*").alias("ch_cnt"))
    )
    ch_window = Window.partitionBy("customer_id").orderBy(col("ch_cnt").desc())
    preferred_channel = (
        channel_counts
        .withColumn("_rn", dense_rank().over(ch_window))
        .filter(col("_rn") == 1)
        .groupBy("customer_id")
        .agg(first("channel").alias("preferred_channel"))
    )

    # --- Top merchant category per customer -----------------------------------
    cat_counts = (
        txn_enriched
        .filter(col("status") == "completed")
        .filter(col("mcc_description").isNotNull())
        .groupBy("customer_id", "mcc_description")
        .agg(count("*").alias("cat_cnt"))
    )
    cat_window = Window.partitionBy("customer_id").orderBy(col("cat_cnt").desc())
    top_category = (
        cat_counts
        .withColumn("_rn", dense_rank().over(cat_window))
        .filter(col("_rn") == 1)
        .groupBy("customer_id")
        .agg(first("mcc_description").alias("top_merchant_category"))
    )

    # --- Assemble the 360 view ------------------------------------------------
    c360 = (
        customers
        .join(acct_summary, on="customer_id", how="left")
        .join(txn_summary, on="customer_id", how="left")
        .join(preferred_channel, on="customer_id", how="left")
        .join(top_category, on="customer_id", how="left")
        .withColumn(
            "full_name",
            concat_ws(" ", col("first_name"), col("last_name")),
        )
        .withColumn(
            "tenure_days",
            datediff(current_date(), col("created_at").cast("date")),
        )
        .select(
            "customer_id",
            "full_name",
            "email",
            "city",
            "state",
            "country",
            "kyc_status",
            "risk_tier",
            "created_at",
            "tenure_days",
            "total_accounts",
            "distinct_account_types",
            "account_types_held",
            "total_credit_limit",
            "lifetime_transaction_count",
            "lifetime_transaction_value",
            "avg_transaction_amount",
            "first_transaction_date",
            "last_transaction_date",
            "preferred_channel",
            "top_merchant_category",
        )
    )

    return c360

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: Daily Transaction Summary
# MAGIC
# MAGIC One row per `(date, transaction_type, channel, currency)`.  Dashboards
# MAGIC filter on these dimensions to show trends, heatmaps, and volume charts.

# COMMAND ----------

@dlt.table(
    name="daily_transaction_summary",
    comment="Daily transaction aggregates by type, channel, and currency",
    table_properties={"quality": "gold"},
)
def daily_transaction_summary():
    txn = dlt.read("transactions_enriched").filter(col("status") == "completed")

    summary = (
        txn
        .groupBy(
            col("transaction_date"),
            col("transaction_type"),
            col("channel"),
            col("currency"),
        )
        .agg(
            count("transaction_id").alias("transaction_count"),
            spark_round(spark_sum("amount"), 2).alias("total_amount"),
            spark_round(avg("amount"), 2).alias("avg_amount"),
            spark_round(spark_min("amount"), 2).alias("min_amount"),
            spark_round(spark_max("amount"), 2).alias("max_amount"),
        )
        .orderBy("transaction_date", "transaction_type")
    )

    return summary

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: Merchant Analytics
# MAGIC
# MAGIC Per-merchant performance metrics.  Includes a **refund rate** — a simple but
# MAGIC powerful fraud/quality signal at the merchant level.

# COMMAND ----------

@dlt.table(
    name="merchant_analytics",
    comment="Per-merchant performance: volume, ticket size, refund rate",
    table_properties={"quality": "gold"},
)
def merchant_analytics():
    txn = dlt.read("transactions_enriched").filter(col("merchant_id").isNotNull())

    completed = txn.filter(col("status") == "completed")

    merch_agg = (
        completed
        .groupBy("merchant_id", "merchant_name", "mcc_code", "mcc_description")
        .agg(
            spark_round(spark_sum("amount"), 2).alias("total_volume"),
            count("transaction_id").alias("transaction_count"),
            countDistinct("customer_id").alias("unique_customers"),
            spark_round(avg("amount"), 2).alias("avg_ticket_size"),
            spark_round(spark_min("amount"), 2).alias("min_ticket"),
            spark_round(spark_max("amount"), 2).alias("max_ticket"),
        )
    )

    # Refund rate per merchant
    refund_counts = (
        txn
        .groupBy("merchant_id")
        .agg(
            spark_sum(when(col("transaction_type") == "refund", 1).otherwise(0)).alias("refund_count"),
            count("transaction_id").alias("all_txn_count"),
        )
        .withColumn(
            "refund_rate",
            spark_round(col("refund_count") / col("all_txn_count") * 100, 2),
        )
        .select("merchant_id", "refund_count", "refund_rate")
    )

    # Top channel per merchant
    ch_counts = (
        completed
        .groupBy("merchant_id", "channel")
        .agg(count("*").alias("ch_cnt"))
    )
    ch_window = Window.partitionBy("merchant_id").orderBy(col("ch_cnt").desc())
    top_ch = (
        ch_counts
        .withColumn("_rn", dense_rank().over(ch_window))
        .filter(col("_rn") == 1)
        .groupBy("merchant_id")
        .agg(first("channel").alias("top_channel"))
    )

    result = (
        merch_agg
        .join(refund_counts, on="merchant_id", how="left")
        .join(top_ch, on="merchant_id", how="left")
    )

    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: Fraud Risk Scores
# MAGIC
# MAGIC A **rule-based** fraud-scoring model.  Each completed transaction gets points
# MAGIC from five independent rules, summed into a total score and mapped to a risk
# MAGIC label.
# MAGIC
# MAGIC | Rule | Description | Points |
# MAGIC |------|-------------|--------|
# MAGIC | 1 | Amount > 3× customer average | +30 |
# MAGIC | 2 | Country ≠ customer home country | +20 |
# MAGIC | 3 | > 5 transactions in 1 hour (velocity) | +25 |
# MAGIC | 4 | Amount > $5 000 | +15 |
# MAGIC | 5 | Online + uncommon device type | +10 |
# MAGIC
# MAGIC | Total Score | Risk Label |
# MAGIC |-------------|------------|
# MAGIC | 0–20 | low |
# MAGIC | 21–50 | medium |
# MAGIC | 51–75 | high |
# MAGIC | 76+ | critical |

# COMMAND ----------

@dlt.table(
    name="fraud_risk_scores",
    comment="Rule-based fraud risk score per transaction",
    table_properties={"quality": "gold"},
)
def fraud_risk_scores():
    txn = dlt.read("transactions_enriched").filter(col("status") == "completed")
    customers = dlt.read("customers").select("customer_id", col("country").alias("home_country"))

    # --- Customer average amount (for Rule 1) ---------------------------------
    cust_avg = (
        txn
        .groupBy("customer_id")
        .agg(avg("amount").alias("cust_avg_amount"))
    )

    # --- Velocity: count of txns in the 1-hour window around each txn (Rule 3) -
    velocity_window = (
        Window
        .partitionBy("account_id")
        .orderBy(col("transaction_timestamp").cast("long"))
        .rangeBetween(-3600, 0)  # 3600 seconds = 1 hour look-back
    )

    # --- Customer's most-used device (for Rule 5) -----------------------------
    device_counts = (
        txn
        .filter(col("device_type").isNotNull())
        .groupBy("customer_id", "device_type")
        .agg(count("*").alias("dev_cnt"))
    )
    dev_window = Window.partitionBy("customer_id").orderBy(col("dev_cnt").desc())
    common_device = (
        device_counts
        .withColumn("_rn", dense_rank().over(dev_window))
        .filter(col("_rn") == 1)
        .groupBy("customer_id")
        .agg(first("device_type").alias("common_device"))
    )

    # --- Assemble and score ---------------------------------------------------
    scored = (
        txn
        .join(customers, on="customer_id", how="left")
        .join(cust_avg, on="customer_id", how="left")
        .join(common_device, on="customer_id", how="left")
        # Velocity count within 1-hour window
        .withColumn("txn_velocity_1h", count("transaction_id").over(velocity_window))
        # Rule 1: amount > 3x customer average
        .withColumn(
            "rule1_high_amount",
            when(col("amount") > col("cust_avg_amount") * 3, lit(30)).otherwise(lit(0)),
        )
        # Rule 2: transaction country != customer home country
        .withColumn(
            "rule2_foreign_country",
            when(
                (col("location_country").isNotNull())
                & (col("home_country").isNotNull())
                & (col("location_country") != col("home_country")),
                lit(20),
            ).otherwise(lit(0)),
        )
        # Rule 3: > 5 transactions in 1 hour
        .withColumn(
            "rule3_velocity",
            when(col("txn_velocity_1h") > 5, lit(25)).otherwise(lit(0)),
        )
        # Rule 4: amount > $5,000
        .withColumn(
            "rule4_large_amount",
            when(col("amount") > 5000, lit(15)).otherwise(lit(0)),
        )
        # Rule 5: online + uncommon device
        .withColumn(
            "rule5_unusual_device",
            when(
                (col("channel") == "online")
                & (col("device_type").isNotNull())
                & (col("device_type") != col("common_device")),
                lit(10),
            ).otherwise(lit(0)),
        )
        # Total score
        .withColumn(
            "fraud_score",
            col("rule1_high_amount")
            + col("rule2_foreign_country")
            + col("rule3_velocity")
            + col("rule4_large_amount")
            + col("rule5_unusual_device"),
        )
        # Risk label
        .withColumn(
            "risk_label",
            when(col("fraud_score") >= 76, lit("critical"))
            .when(col("fraud_score") >= 51, lit("high"))
            .when(col("fraud_score") >= 21, lit("medium"))
            .otherwise(lit("low")),
        )
        .select(
            "transaction_id",
            "account_id",
            "customer_id",
            "merchant_id",
            "amount",
            "transaction_timestamp",
            "channel",
            "location_country",
            "home_country",
            "device_type",
            "common_device",
            "cust_avg_amount",
            "txn_velocity_1h",
            "rule1_high_amount",
            "rule2_foreign_country",
            "rule3_velocity",
            "rule4_large_amount",
            "rule5_unusual_device",
            "fraud_score",
            "risk_label",
        )
    )

    return scored

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: Customer Segments (RFM)
# MAGIC
# MAGIC **RFM segmentation** is a classic marketing/risk model:
# MAGIC
# MAGIC | Dimension | Meaning | How we score |
# MAGIC |-----------|---------|--------------|
# MAGIC | **R**ecency | Days since last transaction | Lower is better → higher quintile |
# MAGIC | **F**requency | Transaction count (last 90 days) | Higher is better → higher quintile |
# MAGIC | **M**onetary | Total spend (last 90 days) | Higher is better → higher quintile |
# MAGIC
# MAGIC Each dimension is split into quintiles (1–5).  The combination determines the
# MAGIC segment label.

# COMMAND ----------

@dlt.table(
    name="customer_segments",
    comment="RFM segmentation — recency, frequency, monetary scores per customer",
    table_properties={"quality": "gold"},
)
def customer_segments():
    txn = (
        dlt.read("transactions_enriched")
        .filter(col("status") == "completed")
        .filter(col("customer_id").isNotNull())
    )

    # --- Recency, Frequency, Monetary ----------------------------------------
    rfm_raw = (
        txn
        .groupBy("customer_id")
        .agg(
            datediff(current_date(), spark_max("transaction_date")).alias("recency_days"),
            count(
                when(
                    col("transaction_date") >= expr("date_sub(current_date(), 90)"),
                    col("transaction_id"),
                )
            ).alias("frequency_90d"),
            spark_round(
                coalesce(
                    spark_sum(
                        when(
                            col("transaction_date") >= expr("date_sub(current_date(), 90)"),
                            col("amount"),
                        )
                    ),
                    lit(0),
                ),
                2,
            ).alias("monetary_90d"),
            # Also keep lifetime metrics for context
            count("transaction_id").alias("lifetime_frequency"),
            spark_round(spark_sum("amount"), 2).alias("lifetime_monetary"),
        )
    )

    # --- Score each dimension 1-5 using ntile ---------------------------------
    # Recency: LOWER is better, so we REVERSE the quintile (5 = most recent)
    r_window = Window.orderBy(col("recency_days").asc())   # ascending = best first
    f_window = Window.orderBy(col("frequency_90d").desc())  # descending = best first
    m_window = Window.orderBy(col("monetary_90d").desc())   # descending = best first

    scored = (
        rfm_raw
        .withColumn("r_score", ntile(5).over(r_window))
        .withColumn("f_score", ntile(5).over(f_window))
        .withColumn("m_score", ntile(5).over(m_window))
        .withColumn("rfm_score", concat_ws("", col("r_score"), col("f_score"), col("m_score")))
    )

    # --- Map to human-readable segments ----------------------------------------
    segmented = (
        scored
        .withColumn(
            "segment",
            when(
                (col("r_score") >= 4) & (col("f_score") >= 4) & (col("m_score") >= 4),
                lit("Champion"),
            )
            .when(
                (col("r_score") >= 3) & (col("f_score") >= 3) & (col("m_score") >= 3),
                lit("Loyal"),
            )
            .when(
                (col("r_score") >= 4) & (col("f_score") <= 2),
                lit("New Customer"),
            )
            .when(
                (col("r_score") <= 2) & (col("f_score") >= 3) & (col("m_score") >= 3),
                lit("At Risk"),
            )
            .when(
                (col("r_score") <= 2) & (col("f_score") <= 2),
                lit("Hibernating"),
            )
            .when(
                (col("r_score") >= 3) & (col("f_score") >= 3) & (col("m_score") <= 2),
                lit("Promising"),
            )
            .when(
                (col("r_score") <= 2) & (col("m_score") >= 4),
                lit("Can't Lose Them"),
            )
            .otherwise(lit("Needs Attention")),
        )
    )

    return segmented

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: Monthly Revenue
# MAGIC
# MAGIC A simple revenue summary — fees and interest income grouped by month and
# MAGIC account type.  This is the starting point for a finance dashboard.

# COMMAND ----------

@dlt.table(
    name="monthly_revenue",
    comment="Monthly fee and interest revenue by account type",
    table_properties={"quality": "gold"},
)
def monthly_revenue():
    txn = (
        dlt.read("transactions_enriched")
        .filter(col("status") == "completed")
        .filter(col("transaction_type").isin("fee", "interest"))
    )

    revenue = (
        txn
        .withColumn("year_month", date_format(col("transaction_date"), "yyyy-MM"))
        .groupBy("year_month", "account_type")
        .agg(
            spark_round(
                spark_sum(when(col("transaction_type") == "fee", col("amount")).otherwise(lit(0))), 2
            ).alias("fee_revenue"),
            spark_round(
                spark_sum(when(col("transaction_type") == "interest", col("amount")).otherwise(lit(0))), 2
            ).alias("interest_revenue"),
            count("transaction_id").alias("revenue_transaction_count"),
        )
        .withColumn(
            "total_revenue",
            spark_round(col("fee_revenue") + col("interest_revenue"), 2),
        )
        .orderBy("year_month", "account_type")
    )

    return revenue

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Pipeline configuration
# MAGIC
# MAGIC When creating the DLT pipeline in the Databricks UI:
# MAGIC
# MAGIC 1. **Pipeline name**: `fintech_medallion`
# MAGIC 2. **Product edition**: Advanced (needed for expectations)
# MAGIC 3. **Source code**: Add both notebooks:
# MAGIC    - `03_dlt_bronze_to_silver`
# MAGIC    - `04_dlt_silver_to_gold`
# MAGIC 4. **Target catalog**: `fintech_lab`
# MAGIC 5. **Target schema**: _leave empty_ — each `@dlt.table` writes to its own
# MAGIC    schema based on the `name` parameter and the pipeline's catalog.
# MAGIC
# MAGIC > **Important:** In the pipeline settings, set the **Target schema** for silver
# MAGIC > tables to `silver` and gold tables to `gold`.  Alternatively, you can create
# MAGIC > two separate pipelines (one per layer) for cleaner separation.
# MAGIC >
# MAGIC > The simplest approach on Free Edition: create a single pipeline and let DLT
# MAGIC > manage the schema names — they default to the pipeline name.  Then create
# MAGIC > views in `fintech_lab.silver` / `fintech_lab.gold` that point to the DLT-
# MAGIC > managed tables.
# MAGIC
# MAGIC ### Pipeline settings JSON (for API / Terraform)
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "name": "fintech_medallion",
# MAGIC   "catalog": "fintech_lab",
# MAGIC   "target": "silver",
# MAGIC   "libraries": [
# MAGIC     {"notebook": {"path": "/Workspace/Users/<you>/notebooks/03_dlt_bronze_to_silver"}},
# MAGIC     {"notebook": {"path": "/Workspace/Users/<you>/notebooks/04_dlt_silver_to_gold"}}
# MAGIC   ],
# MAGIC   "configuration": {
# MAGIC     "pipelines.enableTrackHistory": "true"
# MAGIC   },
# MAGIC   "continuous": false,
# MAGIC   "development": true
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC Set `"development": true` while iterating — it uses cheaper compute and skips
# MAGIC retries.  Flip to `false` for production-mode runs.
