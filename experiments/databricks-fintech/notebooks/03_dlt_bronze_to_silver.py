# Databricks notebook source
# ruff: noqa: E402, F821

# MAGIC %md
# MAGIC # 03 — Delta Live Tables: Bronze → Silver
# MAGIC
# MAGIC This notebook defines the **silver layer** of our medallion architecture using
# MAGIC [Delta Live Tables (DLT)](https://docs.databricks.com/en/delta-live-tables/index.html).
# MAGIC
# MAGIC ## Why DLT instead of manual Spark ETL?
# MAGIC
# MAGIC | Manual Spark | Delta Live Tables |
# MAGIC |---|---|
# MAGIC | You manage schema, dedup, retries | Declarative — define *what*, not *how* |
# MAGIC | Error handling is ad-hoc | Built-in **expectations** track & enforce quality |
# MAGIC | Incremental logic is DIY | Automatic incremental processing |
# MAGIC | No lineage out of the box | Full lineage graph in the DLT UI |
# MAGIC | Orchestration = separate Jobs | Pipeline manages execution order from the DAG |
# MAGIC
# MAGIC ## How expectations work
# MAGIC
# MAGIC | Decorator | Behavior |
# MAGIC |---|---|
# MAGIC | `@dlt.expect(name, condition)` | **Warn** — bad rows pass through, metric is logged |
# MAGIC | `@dlt.expect_or_drop(name, condition)` | **Drop** — bad rows are silently removed |
# MAGIC | `@dlt.expect_or_fail(name, condition)` | **Fail** — pipeline stops on the first violation |
# MAGIC
# MAGIC We use a mix below: hard constraints (`expect_or_drop`) for primary-key nulls,
# MAGIC soft constraints (`expect`) for data-quality monitoring where we still want the
# MAGIC record but want to track the issue.
# MAGIC
# MAGIC ## Quarantine pattern
# MAGIC
# MAGIC For records that fail `expect_or_drop`, DLT removes them from the target table.
# MAGIC In production you would route them to a **quarantine table** for manual review.
# MAGIC We demonstrate this with the `transactions_quarantine` table below.
# MAGIC
# MAGIC ---

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, when, lit, coalesce, to_date, to_timestamp,
    initcap, lower, upper, trim, round as spark_round,
    regexp_replace, current_timestamp, row_number, abs as spark_abs,
    date_format
)
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType, DateType, BooleanType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: Customers
# MAGIC
# MAGIC **Cleaning steps:**
# MAGIC 1. Deduplicate on `customer_id` (keep latest ingestion)
# MAGIC 2. Parse `date_of_birth` — handle three date formats (`YYYY-MM-DD`, `MM/DD/YYYY`, `DD-MM-YYYY`)
# MAGIC 3. Standardize city / state to title-case
# MAGIC 4. Strip non-digit characters from phone numbers
# MAGIC 5. Validate email format (soft expectation — warn, don't drop)
# MAGIC 6. Cast `created_at` to proper TIMESTAMP

# COMMAND ----------

@dlt.table(
    name="customers",
    comment="Clean, deduplicated customer profiles",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "customer_id",
    },
)
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect("valid_email", "email RLIKE '^[a-zA-Z0-9._%+\\\\-]+@[a-zA-Z0-9.\\\\-]+\\\\.[a-zA-Z]{2,}$'")
@dlt.expect("has_first_name", "first_name IS NOT NULL")
@dlt.expect("has_last_name", "last_name IS NOT NULL")
def customers():
    """Bronze customers → silver: dedup, parse dates, standardize text."""
    raw = spark.read.table("fintech_lab.bronze.raw_customers")

    # Deduplicate: keep the most recently ingested record per customer_id
    dedup_window = Window.partitionBy("customer_id").orderBy(col("_ingested_at").desc())
    deduped = (
        raw
        .withColumn("_row_num", row_number().over(dedup_window))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )

    # Parse date_of_birth — try three formats, take the first that succeeds
    cleaned = (
        deduped
        .withColumn(
            "date_of_birth",
            coalesce(
                to_date(col("date_of_birth"), "yyyy-MM-dd"),   # ISO
                to_date(col("date_of_birth"), "MM/dd/yyyy"),   # US
                to_date(col("date_of_birth"), "dd-MM-yyyy"),   # European
            ),
        )
        # Standardize text fields
        .withColumn("first_name", initcap(trim(col("first_name"))))
        .withColumn("last_name", initcap(trim(col("last_name"))))
        .withColumn("email", lower(trim(col("email"))))
        .withColumn("city", initcap(trim(col("city"))))
        .withColumn("state", upper(trim(col("state"))))
        .withColumn("country", upper(trim(col("country"))))
        # Normalize phone to digits only
        .withColumn("phone", regexp_replace(col("phone"), "[^0-9]", ""))
        # Cast created_at to timestamp
        .withColumn("created_at", to_timestamp(col("created_at")))
        # Standardize enum values
        .withColumn("kyc_status", lower(trim(col("kyc_status"))))
        .withColumn("risk_tier", lower(trim(col("risk_tier"))))
        # Audit column
        .withColumn("_cleaned_at", current_timestamp())
        .drop("_ingested_at", "_source_file")
    )

    return cleaned

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: Accounts
# MAGIC
# MAGIC **Cleaning steps:**
# MAGIC 1. Deduplicate on `account_id`
# MAGIC 2. Cast `opened_at` to TIMESTAMP, `credit_limit` and `interest_rate` to DECIMAL
# MAGIC 3. Validate foreign-key integrity with a soft expectation (orphan accounts are kept
# MAGIC    but flagged — a downstream join will naturally exclude them)

# COMMAND ----------

@dlt.table(
    name="accounts",
    comment="Clean, deduplicated account records",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_account_id", "account_id IS NOT NULL")
@dlt.expect("valid_customer_fk", "customer_id IS NOT NULL")
@dlt.expect("valid_account_type", "account_type IN ('checking', 'savings', 'credit_card', 'investment')")
@dlt.expect("valid_status", "status IN ('active', 'frozen', 'closed')")
def accounts():
    """Bronze accounts → silver: dedup, type-cast, validate."""
    raw = spark.read.table("fintech_lab.bronze.raw_accounts")

    dedup_window = Window.partitionBy("account_id").orderBy(col("_ingested_at").desc())
    deduped = (
        raw
        .withColumn("_row_num", row_number().over(dedup_window))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )

    cleaned = (
        deduped
        .withColumn("opened_at", to_timestamp(col("opened_at")))
        .withColumn("credit_limit", col("credit_limit").cast(DecimalType(12, 2)))
        .withColumn("interest_rate", col("interest_rate").cast(DecimalType(5, 4)))
        .withColumn("account_type", lower(trim(col("account_type"))))
        .withColumn("status", lower(trim(col("status"))))
        .withColumn("currency", upper(trim(col("currency"))))
        .withColumn("_cleaned_at", current_timestamp())
        .drop("_ingested_at", "_source_file")
    )

    return cleaned

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: Merchants
# MAGIC
# MAGIC **Cleaning steps:**
# MAGIC 1. Deduplicate on `merchant_id`
# MAGIC 2. Title-case city/merchant names
# MAGIC 3. Cast `is_online` to proper BOOLEAN

# COMMAND ----------

@dlt.table(
    name="merchants",
    comment="Clean merchant reference data",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_merchant_id", "merchant_id IS NOT NULL")
@dlt.expect("has_merchant_name", "merchant_name IS NOT NULL")
@dlt.expect("valid_mcc", "mcc_code IS NOT NULL AND length(mcc_code) = 4")
def merchants():
    """Bronze merchants → silver: dedup, standardize text, cast types."""
    raw = spark.read.table("fintech_lab.bronze.raw_merchants")

    dedup_window = Window.partitionBy("merchant_id").orderBy(col("_ingested_at").desc())
    deduped = (
        raw
        .withColumn("_row_num", row_number().over(dedup_window))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )

    cleaned = (
        deduped
        .withColumn("merchant_name", initcap(trim(col("merchant_name"))))
        .withColumn("city", initcap(trim(col("city"))))
        .withColumn("state", upper(trim(col("state"))))
        .withColumn("country", upper(trim(col("country"))))
        .withColumn("is_online", col("is_online").cast(BooleanType()))
        .withColumn("_cleaned_at", current_timestamp())
        .drop("_ingested_at", "_source_file")
    )

    return cleaned

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: Exchange Rates
# MAGIC
# MAGIC Simple reference table — deduplicate on the composite key
# MAGIC `(rate_date, base_currency, target_currency)` and cast types.

# COMMAND ----------

@dlt.table(
    name="exchange_rates",
    comment="Clean exchange-rate reference data",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_rate", "rate IS NOT NULL AND rate > 0")
def exchange_rates():
    """Bronze exchange rates → silver: dedup, type-cast."""
    raw = spark.read.table("fintech_lab.bronze.raw_exchange_rates")

    dedup_window = (
        Window
        .partitionBy("rate_date", "base_currency", "target_currency")
        .orderBy(col("_ingested_at").desc())
    )
    deduped = (
        raw
        .withColumn("_row_num", row_number().over(dedup_window))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )

    cleaned = (
        deduped
        .withColumn("rate_date", to_date(col("rate_date"), "yyyy-MM-dd"))
        .withColumn("rate", spark_round(col("rate").cast("double"), 6))
        .withColumn("base_currency", upper(trim(col("base_currency"))))
        .withColumn("target_currency", upper(trim(col("target_currency"))))
        .withColumn("_cleaned_at", current_timestamp())
        .drop("_ingested_at", "_source_file")
    )

    return cleaned

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: Transactions
# MAGIC
# MAGIC The richest cleaning job in the pipeline:
# MAGIC
# MAGIC | Issue | Fix |
# MAGIC |---|---|
# MAGIC | ~5% duplicates | `ROW_NUMBER` dedup on `transaction_id` |
# MAGIC | Negative amounts on non-refund types | Absolute-value + soft expectation |
# MAGIC | Future-dated timestamps | Quarantine (routed to separate table) |
# MAGIC | Floating-point artifacts | `ROUND(amount, 2)` |
# MAGIC | Null merchant on purchases | Soft expectation (tracked, not dropped) |

# COMMAND ----------

@dlt.table(
    name="transactions",
    comment="Clean, deduplicated transactions",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "transaction_id",
    },
)
@dlt.expect_or_drop("valid_transaction_id", "transaction_id IS NOT NULL")
@dlt.expect_or_drop("valid_account_id", "account_id IS NOT NULL")
@dlt.expect_or_drop("not_future_dated", "transaction_timestamp <= current_timestamp()")
@dlt.expect("positive_amount", "amount >= 0 OR transaction_type IN ('refund', 'fee')")
@dlt.expect(
    "has_merchant_for_purchase",
    "merchant_id IS NOT NULL OR transaction_type NOT IN ('purchase')",
)
@dlt.expect("valid_status", "status IN ('completed', 'pending', 'failed', 'reversed')")
def transactions():
    """Bronze transactions → silver: dedup, fix amounts, filter future dates."""
    raw = spark.read.table("fintech_lab.bronze.raw_transactions")

    # Deduplicate on transaction_id
    dedup_window = Window.partitionBy("transaction_id").orderBy(col("_ingested_at").desc())
    deduped = (
        raw
        .withColumn("_row_num", row_number().over(dedup_window))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )

    cleaned = (
        deduped
        .withColumn("transaction_timestamp", to_timestamp(col("transaction_timestamp")))
        # Fix floating-point artifacts: round to 2 decimal places
        .withColumn("amount", spark_round(col("amount").cast("double"), 2))
        # For non-refund/non-fee types, take the absolute value of negative amounts
        .withColumn(
            "amount",
            when(
                (col("amount") < 0) & (~col("transaction_type").isin("refund", "fee")),
                spark_abs(col("amount")),
            ).otherwise(col("amount")),
        )
        .withColumn("amount", col("amount").cast(DecimalType(12, 2)))
        .withColumn("transaction_type", lower(trim(col("transaction_type"))))
        .withColumn("status", lower(trim(col("status"))))
        .withColumn("channel", lower(trim(col("channel"))))
        .withColumn("location_city", initcap(trim(col("location_city"))))
        .withColumn("location_country", upper(trim(col("location_country"))))
        .withColumn("currency", upper(trim(col("currency"))))
        .withColumn("_cleaned_at", current_timestamp())
        .drop("_ingested_at", "_source_file")
    )

    return cleaned

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quarantine: Future-Dated Transactions
# MAGIC
# MAGIC Records that violate the `not_future_dated` expectation above are **dropped**
# MAGIC from `silver.transactions`.  In a production pipeline you would route them here
# MAGIC for investigation (perhaps a timezone bug in the source system).
# MAGIC
# MAGIC This table captures the *inverse* of the filter — everything the main table
# MAGIC rejected.

# COMMAND ----------

@dlt.table(
    name="transactions_quarantine",
    comment="Transactions rejected by silver quality gates — for manual review",
    table_properties={"quality": "quarantine"},
)
def transactions_quarantine():
    """Capture future-dated and otherwise invalid transactions for review."""
    raw = spark.read.table("fintech_lab.bronze.raw_transactions")

    dedup_window = Window.partitionBy("transaction_id").orderBy(col("_ingested_at").desc())
    deduped = (
        raw
        .withColumn("_row_num", row_number().over(dedup_window))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )

    quarantined = (
        deduped
        .withColumn("transaction_timestamp", to_timestamp(col("transaction_timestamp")))
        # Keep only the records that the main table drops
        .filter(col("transaction_timestamp") > current_timestamp())
        .withColumn("amount", spark_round(col("amount").cast("double"), 2))
        .withColumn("amount", col("amount").cast(DecimalType(12, 2)))
        .withColumn("quarantine_reason", lit("future_dated_transaction"))
        .withColumn("_quarantined_at", current_timestamp())
        .drop("_ingested_at", "_source_file")
    )

    return quarantined

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: Transactions Enriched
# MAGIC
# MAGIC A wide **fact table** that joins transactions with merchant and account
# MAGIC dimensions.  Downstream gold aggregations read from this table so they don't
# MAGIC have to repeat the joins.
# MAGIC
# MAGIC We use `dlt.read()` to reference the other tables **within the same DLT
# MAGIC pipeline** — DLT resolves the dependency graph automatically.

# COMMAND ----------

@dlt.table(
    name="transactions_enriched",
    comment="Transactions enriched with merchant and account context",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "transaction_id",
    },
)
def transactions_enriched():
    """Join silver transactions with merchants and accounts for a wide fact table."""
    txn = dlt.read("transactions")
    merch = dlt.read("merchants").select(
        col("merchant_id"),
        col("merchant_name"),
        col("mcc_code"),
        col("mcc_description"),
    )
    acct = dlt.read("accounts").select(
        col("account_id"),
        col("customer_id"),
        col("account_type"),
        col("currency").alias("account_currency"),
    )

    enriched = (
        txn
        .join(acct, on="account_id", how="left")
        .join(merch, on="merchant_id", how="left")
        .withColumn("transaction_date", col("transaction_timestamp").cast(DateType()))
        .withColumn("transaction_hour", date_format(col("transaction_timestamp"), "HH").cast("int"))
        .withColumn("day_of_week", date_format(col("transaction_timestamp"), "EEEE"))
    )

    return enriched

# COMMAND ----------

# MAGIC %md
# MAGIC ## How DLT handles incremental processing
# MAGIC
# MAGIC When you run this pipeline a **second** time:
# MAGIC
# MAGIC 1. DLT checks which source data is new (Delta change data feed or file
# MAGIC    modification tracking).
# MAGIC 2. Only the new / changed rows flow through the transformation graph.
# MAGIC 3. Tables defined with `@dlt.table` are **materialized views** — DLT rewrites
# MAGIC    them efficiently rather than re-scanning everything.
# MAGIC 4. For truly append-only streaming, you'd switch to
# MAGIC    `@dlt.table(... , streaming=True)` with `dlt.read_stream()`.
# MAGIC
# MAGIC This means you can schedule the pipeline to run every hour and it processes
# MAGIC only the delta — no custom watermarks or checkpoints needed.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Next steps
# MAGIC
# MAGIC This notebook is **Notebook 1 of 2** in the DLT pipeline.
# MAGIC Add both this notebook and `04_dlt_silver_to_gold` as source notebooks in a
# MAGIC single DLT pipeline configuration.  DLT will build the combined DAG
# MAGIC automatically.
