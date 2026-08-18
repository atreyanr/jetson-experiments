# Databricks notebook source
# ruff: noqa: E402, F821

# MAGIC %md
# MAGIC # 🥉 Notebook 02 — Bronze Layer Ingestion
# MAGIC
# MAGIC Reads raw JSON Lines files from the landing zone and writes them as
# MAGIC **Delta tables** in `fintech_lab.bronze` — unchanged, append-only, with
# MAGIC ingestion metadata.
# MAGIC
# MAGIC ### Bronze layer philosophy
# MAGIC
# MAGIC | Principle | What it means |
# MAGIC |-----------|---------------|
# MAGIC | **No transforms** | Data arrives exactly as the source produced it |
# MAGIC | **Append-only** | Every run adds rows; nothing is updated or deleted |
# MAGIC | **Metadata stamped** | `_ingested_at` and `_source_file` track provenance |
# MAGIC | **Schema-on-read** | All columns stay as strings; the silver layer casts |
# MAGIC
# MAGIC This is the "just get it in" layer.  Data quality issues (nulls, dupes,
# MAGIC bad formats) are **expected** here and cleaned downstream.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0 · Configuration

# COMMAND ----------

CATALOG = "fintech_lab"
SCHEMA  = "bronze"
VOLUME  = f"/Volumes/{CATALOG}/{SCHEMA}/landing_zone"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print(f"Target: {CATALOG}.{SCHEMA}")
print(f"Source:  {VOLUME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Helper function
# MAGIC
# MAGIC A reusable function that:
# MAGIC 1. Reads JSON Lines from a path
# MAGIC 2. Adds `_ingested_at` and `_source_file` audit columns
# MAGIC 3. Writes to a Delta table (overwrite mode for this demo; production
# MAGIC    would use append + merge)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, input_file_name

def ingest_to_bronze(source_path, table_name, file_format="json"):
    """
    Read raw files from the landing zone and write to a bronze Delta table.

    Args:
        source_path: Path to JSON Lines files (can use wildcards)
        table_name:  Unqualified table name (written to current schema)
        file_format: Source format (default: json)
    """
    df = (
        spark.read
        .format(file_format)
        .option("multiline", "false")          # JSON Lines = one object per line
        .option("mode", "PERMISSIVE")          # don't fail on bad records
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .load(source_path)
    )

    # Add ingestion metadata
    df_with_meta = (
        df
        .withColumn("_ingested_at",  current_timestamp())
        .withColumn("_source_file",  input_file_name())
    )

    # Write as Delta
    (
        df_with_meta.write
        .format("delta")
        .mode("overwrite")                     # idempotent for demo
        .option("mergeSchema", "true")         # tolerate schema drift
        .saveAsTable(table_name)
    )

    count = spark.table(table_name).count()
    print(f"  ✅ {table_name}: {count:,} rows ingested")
    return count

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Ingest each entity
# MAGIC
# MAGIC ### 2a · Customers

# COMMAND ----------

ingest_to_bronze(f"{VOLUME}/customers/*.jsonl", "raw_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2b · Accounts

# COMMAND ----------

ingest_to_bronze(f"{VOLUME}/accounts/*.jsonl", "raw_accounts")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2c · Merchants

# COMMAND ----------

ingest_to_bronze(f"{VOLUME}/merchants/*.jsonl", "raw_merchants")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2d · Transactions

# COMMAND ----------

ingest_to_bronze(f"{VOLUME}/transactions/*.jsonl", "raw_transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2e · Exchange Rates

# COMMAND ----------

ingest_to_bronze(f"{VOLUME}/exchange_rates/*.jsonl", "raw_exchange_rates")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Verify: row counts

# COMMAND ----------

tables = [
    "raw_customers",
    "raw_accounts",
    "raw_merchants",
    "raw_transactions",
    "raw_exchange_rates",
]

print("=" * 50)
print("  BRONZE LAYER — ROW COUNTS")
print("=" * 50)
total = 0
for t in tables:
    n = spark.table(t).count()
    total += n
    print(f"  {t:30s} {n:>10,}")
print("-" * 50)
print(f"  {'TOTAL':30s} {total:>10,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Spot-check data quality issues
# MAGIC
# MAGIC Peek at the problems that the silver layer will need to handle.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4a · Duplicate transactions

# COMMAND ----------

from pyspark.sql.functions import col, count as spark_count

dupes = (
    spark.table("raw_transactions")
    .groupBy("transaction_id")
    .agg(spark_count("*").alias("cnt"))
    .filter(col("cnt") > 1)
)
n_dupe_ids = dupes.count()
n_dupe_rows = dupes.agg({"cnt": "sum"}).collect()[0][0] or 0
print(f"Duplicate transaction_ids: {n_dupe_ids:,} ids "
      f"({int(n_dupe_rows):,} total rows)")
dupes.orderBy(col("cnt").desc()).show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4b · Null merchant on purchases

# COMMAND ----------

null_merchant_purchases = (
    spark.table("raw_transactions")
    .filter(
        (col("transaction_type") == "purchase")
        & col("merchant_id").isNull()
    )
    .count()
)
total_purchases = (
    spark.table("raw_transactions")
    .filter(col("transaction_type") == "purchase")
    .count()
)
pct = 100 * null_merchant_purchases / max(total_purchases, 1)
print(f"Purchases with null merchant_id: "
      f"{null_merchant_purchases:,} / {total_purchases:,} ({pct:.1f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4c · Negative amounts (non-refund / non-fee)

# COMMAND ----------

neg_amounts = (
    spark.table("raw_transactions")
    .filter(
        (col("amount") < 0)
        & (~col("transaction_type").isin("refund", "fee"))
    )
    .count()
)
print(f"Negative amounts on non-refund/non-fee: {neg_amounts:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4d · Sample of mixed date formats (customers.date_of_birth)

# COMMAND ----------

(
    spark.table("raw_customers")
    .select("customer_id", "date_of_birth")
    .show(10, truncate=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4e · Mixed city casing

# COMMAND ----------

(
    spark.table("raw_customers")
    .select("city")
    .distinct()
    .orderBy("city")
    .show(30, truncate=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Auto Loader alternative (production pattern)
# MAGIC
# MAGIC In production you would use **Auto Loader** (`cloudFiles`) instead of
# MAGIC batch reads.  Auto Loader incrementally processes new files as they
# MAGIC arrive:
# MAGIC
# MAGIC ```python
# MAGIC # Production pattern — DO NOT RUN in this demo
# MAGIC (
# MAGIC     spark.readStream
# MAGIC     .format("cloudFiles")
# MAGIC     .option("cloudFiles.format", "json")
# MAGIC     .option("cloudFiles.schemaLocation",
# MAGIC             f"{VOLUME}/_schemas/transactions")
# MAGIC     .load(f"{VOLUME}/transactions/")
# MAGIC     .withColumn("_ingested_at", current_timestamp())
# MAGIC     .withColumn("_source_file", input_file_name())
# MAGIC     .writeStream
# MAGIC     .format("delta")
# MAGIC     .option("checkpointLocation",
# MAGIC             f"{VOLUME}/_checkpoints/transactions")
# MAGIC     .trigger(availableNow=True)
# MAGIC     .toTable("raw_transactions")
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC Auto Loader advantages:
# MAGIC - **Exactly-once** — tracks which files have been processed
# MAGIC - **Schema evolution** — detects new columns automatically
# MAGIC - **Scalable** — handles millions of files efficiently
# MAGIC - **Incremental** — only reads new files each run

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC All raw data is now in Delta tables under `fintech_lab.bronze`:
# MAGIC
# MAGIC | Table | Status |
# MAGIC |-------|--------|
# MAGIC | `raw_customers` | Done |
# MAGIC | `raw_accounts` | Done |
# MAGIC | `raw_merchants` | Done |
# MAGIC | `raw_transactions` | Done |
# MAGIC | `raw_exchange_rates` | Done |
# MAGIC
# MAGIC Known quality issues confirmed — the silver layer will fix them.
# MAGIC
# MAGIC ---
# MAGIC **Next** Run `03_dlt_bronze_to_silver` to deploy the Delta Live Tables
# MAGIC pipeline that cleans and validates the data.
