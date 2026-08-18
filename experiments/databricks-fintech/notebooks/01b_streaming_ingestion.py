# Databricks notebook source
# ruff: noqa: E402, F821

# MAGIC %md
# MAGIC # Streaming Bronze Ingestion from Cloudflare R2
# MAGIC
# MAGIC This notebook uses **Databricks Auto Loader** (`cloudFiles`) to stream new JSONL files
# MAGIC from a Cloudflare R2 bucket into bronze Delta tables. It replaces the batch approach
# MAGIC (notebooks 01 + 02) with an incremental, production-grade ingestion pipeline.
# MAGIC
# MAGIC ### Why Auto Loader?
# MAGIC
# MAGIC | Feature | Batch (`spark.read`) | Auto Loader (`cloudFiles`) |
# MAGIC |---------|---------------------|---------------------------|
# MAGIC | File tracking | None — re-reads everything | Checkpoint — processes each file exactly once |
# MAGIC | New file discovery | Manual re-run | Automatic on each trigger |
# MAGIC | Schema evolution | Fails on new columns | `addNewColumns` mode handles it |
# MAGIC | Scalability | Full scan every time | Incremental — only new files |
# MAGIC | Exactly-once | No guarantee | Yes, via checkpoint |
# MAGIC
# MAGIC ### Prerequisites
# MAGIC
# MAGIC Run `01a_configure_r2_connection` first to set up the R2 credentials on this cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Architecture
# MAGIC
# MAGIC ```
# MAGIC ┌────────────────────────────────┐
# MAGIC │  Cloudflare Worker (cron 5min) │
# MAGIC │  + Stripe test API             │
# MAGIC └──────────┬─────────────────────┘
# MAGIC            │ writes JSONL
# MAGIC            ▼
# MAGIC ┌────────────────────────────────┐
# MAGIC │  Cloudflare R2 Bucket          │
# MAGIC │  fintech-events/               │
# MAGIC │  ├── customers/year=.../...    │
# MAGIC │  ├── transactions/year=.../... │
# MAGIC │  ├── refunds/year=.../...      │
# MAGIC │  └── activity/year=.../...     │
# MAGIC └──────────┬─────────────────────┘
# MAGIC            │ Auto Loader (cloudFiles)
# MAGIC            ▼
# MAGIC ┌────────────────────────────────┐
# MAGIC │  Databricks Bronze Layer       │
# MAGIC │  fintech_lab.bronze.*_stream   │
# MAGIC └──────────┬─────────────────────┘
# MAGIC            │ DLT Pipeline (03+04)
# MAGIC            ▼
# MAGIC ┌────────────────────────────────┐
# MAGIC │  Silver → Gold                 │
# MAGIC └────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# R2 bucket — must match what the Cloudflare Worker writes to
R2_BUCKET = "fintech-events"
BASE_PATH = f"s3a://{R2_BUCKET}"

# Auto Loader checkpoint location — tracks which files have been processed.
# Use a DBFS or Volume path so checkpoints survive cluster restarts.
CHECKPOINT_BASE = "/tmp/fintech_checkpoints"

# Unity Catalog target
CATALOG = "fintech_lab"
SCHEMA = "bronze"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print(f"Source:      {BASE_PATH}")
print(f"Checkpoints: {CHECKPOINT_BASE}")
print(f"Target:      {CATALOG}.{SCHEMA}.*")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto Loader: How It Works
# MAGIC
# MAGIC Auto Loader uses the `cloudFiles` format to read streaming data from cloud storage:
# MAGIC
# MAGIC ```python
# MAGIC spark.readStream.format("cloudFiles")
# MAGIC     .option("cloudFiles.format", "json")        # File format (json, csv, parquet, etc.)
# MAGIC     .option("cloudFiles.schemaLocation", "...")  # Where to store inferred schema
# MAGIC     .option("cloudFiles.inferColumnTypes", True)  # Infer types (not just StringType)
# MAGIC     .load("s3a://bucket/path")
# MAGIC ```
# MAGIC
# MAGIC Under the hood, Auto Loader:
# MAGIC 1. Lists the source directory for new files (directory listing mode)
# MAGIC 2. Reads only files not yet processed (tracked via the checkpoint)
# MAGIC 3. Infers or evolves the schema as new columns appear
# MAGIC 4. Writes to the target Delta table with exactly-once semantics
# MAGIC
# MAGIC **Two trigger modes:**
# MAGIC - `trigger(availableNow=True)` — batch: process all pending files, then stop
# MAGIC - `trigger(processingTime="30 seconds")` — continuous: poll every 30s for new files

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, input_file_name, lit

def stream_from_r2(event_type, target_table, schema_hints=None):
    """
    Start an Auto Loader stream from R2 for a specific event type.

    Args:
        event_type: Subfolder in R2 (customers, transactions, refunds, activity).
        target_table: Bronze Delta table name (e.g. "raw_customers_stream").
        schema_hints: Optional schema hints string, e.g. "amount DOUBLE, id STRING".

    Returns:
        The streaming query handle (already started).
    """
    source_path = f"{BASE_PATH}/{event_type}"
    checkpoint_path = f"{CHECKPOINT_BASE}/{event_type}"

    reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/_schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        # Directory listing mode — works with R2 (no S3 event notifications needed)
        .option("cloudFiles.useNotifications", "false")
        # Only pick up .jsonl files (ignore _state/ and other metadata)
        .option("pathGlobFilter", "*.jsonl")
    )

    # Schema hints help Auto Loader assign correct types on first inference
    if schema_hints:
        reader = reader.option("cloudFiles.schemaHints", schema_hints)

    df = (
        reader
        .load(source_path)
        # Add ingestion metadata — same pattern as the batch bronze notebook
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", input_file_name())
        .withColumn("_event_type", lit(event_type))
    )

    full_table = f"{CATALOG}.{SCHEMA}.{target_table}"

    query = (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        # availableNow: process all new files then stop (good for scheduled jobs).
        # Switch to processingTime for continuous near-real-time ingestion:
        #   .trigger(processingTime="30 seconds")
        .trigger(availableNow=True)
        .toTable(full_table)
    )

    return query

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trigger Modes
# MAGIC
# MAGIC | Mode | Code | Use Case |
# MAGIC |------|------|----------|
# MAGIC | **Batch (used here)** | `.trigger(availableNow=True)` | Scheduled job — process all pending files, then stop. Cluster can shut down between runs. |
# MAGIC | **Continuous** | `.trigger(processingTime="30 seconds")` | Always-on — poll for new files every 30 seconds. Sub-minute latency. |
# MAGIC | **Once (legacy)** | `.trigger(once=True)` | Older API — same as `availableNow` but less efficient. Prefer `availableNow`. |
# MAGIC
# MAGIC We use `availableNow=True` below so the notebook can run as a scheduled job.
# MAGIC The checkpoint ensures each file is processed exactly once across runs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest: Customers

# COMMAND ----------

print("🔄 Streaming customers from R2...")
q_customers = stream_from_r2(
    event_type="customers",
    target_table="raw_customers_stream",
    schema_hints="id STRING, email STRING, created LONG",
)
q_customers.awaitTermination()

count = spark.table(f"{CATALOG}.{SCHEMA}.raw_customers_stream").count()
print(f"✅ raw_customers_stream: {count:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest: Transactions (Stripe PaymentIntents)

# COMMAND ----------

print("🔄 Streaming transactions from R2...")
q_transactions = stream_from_r2(
    event_type="transactions",
    target_table="raw_transactions_stream",
    schema_hints="id STRING, amount LONG, currency STRING, status STRING, created LONG",
)
q_transactions.awaitTermination()

count = spark.table(f"{CATALOG}.{SCHEMA}.raw_transactions_stream").count()
print(f"✅ raw_transactions_stream: {count:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest: Refunds

# COMMAND ----------

print("🔄 Streaming refunds from R2...")
q_refunds = stream_from_r2(
    event_type="refunds",
    target_table="raw_refunds_stream",
    schema_hints="id STRING, amount LONG, currency STRING, status STRING, created LONG",
)
q_refunds.awaitTermination()

count = spark.table(f"{CATALOG}.{SCHEMA}.raw_refunds_stream").count()
print(f"✅ raw_refunds_stream: {count:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest: User Activity Events

# COMMAND ----------

print("🔄 Streaming activity events from R2...")
q_activity = stream_from_r2(
    event_type="activity",
    target_table="raw_activity_stream",
    schema_hints="event_id STRING, event_type STRING, customer_id STRING, timestamp STRING",
)
q_activity.awaitTermination()

count = spark.table(f"{CATALOG}.{SCHEMA}.raw_activity_stream").count()
print(f"✅ raw_activity_stream: {count:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion Summary

# COMMAND ----------

STREAM_TABLES = [
    "raw_customers_stream",
    "raw_transactions_stream",
    "raw_refunds_stream",
    "raw_activity_stream",
]

print("=" * 60)
print("STREAMING INGESTION SUMMARY")
print("=" * 60)
total = 0
for t in STREAM_TABLES:
    fqn = f"{CATALOG}.{SCHEMA}.{t}"
    try:
        n = spark.table(fqn).count()
    except Exception:
        n = 0
    total += n
    print(f"  {t:35s} {n:>10,}")
print("-" * 60)
print(f"  {'TOTAL':35s} {total:>10,}")

if total == 0:
    print()
    print("⚠️  No data ingested. Possible causes:")
    print("   1. The Cloudflare Worker hasn't run yet (no files in R2)")
    print("   2. R2 connection not configured (run 01a first)")
    print("   3. Bucket name mismatch (check R2_BUCKET variable)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Data

# COMMAND ----------

for t in STREAM_TABLES:
    fqn = f"{CATALOG}.{SCHEMA}.{t}"
    try:
        count = spark.table(fqn).count()
        if count > 0:
            print(f"\n{'─' * 60}")
            print(f"  {t} — {count:,} rows (showing first 5)")
            print(f"{'─' * 60}")
            display(spark.table(fqn).limit(5))  # noqa: F405
    except Exception:
        pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Switching to Continuous Mode
# MAGIC
# MAGIC To run Auto Loader as a **continuously streaming** pipeline (sub-minute latency):
# MAGIC
# MAGIC 1. Change the trigger in `stream_from_r2`:
# MAGIC    ```python
# MAGIC    .trigger(processingTime="30 seconds")  # Poll every 30 seconds
# MAGIC    ```
# MAGIC
# MAGIC 2. Start all 4 streams without awaiting each one:
# MAGIC    ```python
# MAGIC    q1 = stream_from_r2("customers", "raw_customers_stream")
# MAGIC    q2 = stream_from_r2("transactions", "raw_transactions_stream")
# MAGIC    q3 = stream_from_r2("refunds", "raw_refunds_stream")
# MAGIC    q4 = stream_from_r2("activity", "raw_activity_stream")
# MAGIC
# MAGIC    # Monitor active streams
# MAGIC    for s in spark.streams.active:
# MAGIC        print(f"  {s.name}: {s.status}")
# MAGIC    ```
# MAGIC
# MAGIC 3. Set the downstream DLT pipeline to **Continuous** mode (instead of Triggered)
# MAGIC    so silver/gold tables update as new bronze data arrives.
# MAGIC
# MAGIC 4. To stop all streams:
# MAGIC    ```python
# MAGIC    for s in spark.streams.active:
# MAGIC        s.stop()
# MAGIC    ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC - The **original notebooks 01 + 02** (local data gen + batch ingestion) are still
# MAGIC   available as a fallback if you want to work without the R2 connection.
# MAGIC - The **DLT pipeline** (notebooks 03 + 04) works with either ingestion path.
# MAGIC   To use the streaming tables, update the source table names in notebook 03:
# MAGIC   ```python
# MAGIC   # Change from:
# MAGIC   spark.read.table("fintech_lab.bronze.raw_customers")
# MAGIC   # To:
# MAGIC   spark.read.table("fintech_lab.bronze.raw_customers_stream")
# MAGIC   ```
# MAGIC - Run `05_fraud_detection` and `06_analytics_queries` against the gold layer as before.
# MAGIC - Use `07_workflow_orchestration` to schedule this notebook on a recurring basis.
