# Databricks notebook source
# ruff: noqa: E402, F821

# MAGIC %md
# MAGIC # Streaming Bronze Ingestion from Cloudflare R2
# MAGIC
# MAGIC This notebook syncs JSONL files from a Cloudflare R2 bucket into bronze Delta tables.
# MAGIC It replaces the batch approach (notebooks 01 + 02) with an incremental pipeline fed
# MAGIC by the Cloudflare Worker + Stripe test data.
# MAGIC
# MAGIC ### Approach: boto3 → Volume → Spark
# MAGIC
# MAGIC Databricks Free Edition uses **serverless compute (Spark Connect)** which blocks
# MAGIC Hadoop `fs.s3a.*` configs and Auto Loader's `cloudFiles` format for custom S3
# MAGIC endpoints. Instead, we:
# MAGIC
# MAGIC 1. **boto3** downloads new JSONL files from R2 into a Unity Catalog Volume
# MAGIC 2. **Spark** reads from the Volume and writes to bronze Delta tables
# MAGIC 3. A **checkpoint file** tracks which R2 objects have been synced (incremental)
# MAGIC
# MAGIC ### Prerequisites
# MAGIC
# MAGIC Run `01a_configure_r2_connection` first to set up boto3 credentials and test the connection.

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
# MAGIC └──────────┬─────────────────────┘
# MAGIC            │ boto3 sync (incremental)
# MAGIC            ▼
# MAGIC ┌────────────────────────────────┐
# MAGIC │  Unity Catalog Volume          │
# MAGIC │  /Volumes/.../landing_zone/    │
# MAGIC └──────────┬─────────────────────┘
# MAGIC            │ spark.read.json()
# MAGIC            ▼
# MAGIC ┌────────────────────────────────┐
# MAGIC │  Bronze Delta Tables           │
# MAGIC │  fintech_lab.bronze.*_stream   │
# MAGIC └──────────┬─────────────────────┘
# MAGIC            │ DLT Pipeline (03+04)
# MAGIC            ▼
# MAGIC ┌────────────────────────────────┐
# MAGIC │  Silver → Gold                 │
# MAGIC └────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %pip install boto3 -q

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import boto3
import os
import json

# R2 connection — reuse credentials from 01a widgets
r2_access_key = dbutils.widgets.get("r2_access_key")  # noqa: F405
r2_secret_key = dbutils.widgets.get("r2_secret_key")  # noqa: F405
r2_endpoint = dbutils.widgets.get("r2_endpoint")  # noqa: F405
r2_bucket = dbutils.widgets.get("r2_bucket")  # noqa: F405

s3 = boto3.client(
    "s3",
    endpoint_url=r2_endpoint,
    aws_access_key_id=r2_access_key,
    aws_secret_access_key=r2_secret_key,
    region_name="auto",
)

# Paths
VOLUME_PATH = "/Volumes/fintech_lab/bronze/landing_zone"
CHECKPOINT_FILE = f"{VOLUME_PATH}/_sync_checkpoint.json"
CATALOG = "fintech_lab"
SCHEMA = "bronze"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print(f"R2 bucket:   {r2_bucket}")
print(f"Volume:      {VOLUME_PATH}")
print(f"Target:      {CATALOG}.{SCHEMA}.*_stream")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incremental Sync: R2 → Volume
# MAGIC
# MAGIC Downloads only **new** JSONL files from R2. A checkpoint file
# MAGIC in the Volume tracks which R2 keys have been synced.

# COMMAND ----------

def load_checkpoint():
    """Load the set of already-synced R2 object keys."""
    try:
        with open(CHECKPOINT_FILE) as f:
            return set(json.load(f))
    except (FileNotFoundError, json.JSONDecodeError):
        return set()


def save_checkpoint(synced_keys):
    """Persist the set of synced R2 object keys."""
    os.makedirs(os.path.dirname(CHECKPOINT_FILE), exist_ok=True)
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(sorted(synced_keys), f)


def sync_r2_to_volume(event_type, synced_keys):
    """
    Download new JSONL files from R2 into the Volume.

    Args:
        event_type: R2 prefix (customers, transactions, refunds, activity).
        synced_keys: Set of already-synced R2 keys (mutated in place).

    Returns:
        Number of new files downloaded.
    """
    prefix = f"{event_type}/"
    paginator = s3.get_paginator("list_objects_v2")
    new_count = 0

    for page in paginator.paginate(Bucket=r2_bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            if not key.endswith(".jsonl"):
                continue
            if key in synced_keys:
                continue

            local_path = f"{VOLUME_PATH}/{key}"
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            s3.download_file(r2_bucket, key, local_path)

            synced_keys.add(key)
            new_count += 1

    return new_count

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sync All Event Types from R2

# COMMAND ----------

EVENT_TYPES = ["customers", "transactions", "refunds", "activity"]

synced_keys = load_checkpoint()
print(f"Checkpoint: {len(synced_keys)} files previously synced")
print()

total_new = 0
for event_type in EVENT_TYPES:
    new_count = sync_r2_to_volume(event_type, synced_keys)
    total_new += new_count
    status = f"{new_count} new files" if new_count > 0 else "up to date"
    print(f"  {event_type:15s} → {status}")

save_checkpoint(synced_keys)
print(f"\n✅ R2 → Volume sync complete ({total_new} new files, {len(synced_keys)} total)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Volume → Bronze Delta Tables
# MAGIC
# MAGIC Spark reads JSONL files natively from the Volume and writes to
# MAGIC bronze Delta tables with ingestion metadata.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, input_file_name, lit

def ingest_to_bronze(event_type, target_table):
    """
    Read JSONL files from the Volume and write to a bronze Delta table.
    """
    source_path = f"{VOLUME_PATH}/{event_type}"
    full_table = f"{CATALOG}.{SCHEMA}.{target_table}"

    df = (
        spark.read
        .option("multiline", "false")
        .option("recursiveFileLookup", "true")
        .json(source_path)
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", input_file_name())
        .withColumn("_event_type", lit(event_type))
    )

    row_count = df.count()
    if row_count == 0:
        print(f"  {target_table:35s} — no data found")
        return 0

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(full_table)
    )

    print(f"  {target_table:35s} → {row_count:>8,} rows")
    return row_count

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest All Event Types

# COMMAND ----------

TABLE_MAP = {
    "customers": "raw_customers_stream",
    "transactions": "raw_transactions_stream",
    "refunds": "raw_refunds_stream",
    "activity": "raw_activity_stream",
}

print("Loading Volume → Bronze Delta tables...")
print()

total_rows = 0
for event_type, table_name in TABLE_MAP.items():
    rows = ingest_to_bronze(event_type, table_name)
    total_rows += rows

print(f"\n✅ Bronze ingestion complete: {total_rows:,} total rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion Summary

# COMMAND ----------

print("=" * 60)
print("STREAMING INGESTION SUMMARY")
print("=" * 60)
total = 0
for table_name in TABLE_MAP.values():
    fqn = f"{CATALOG}.{SCHEMA}.{table_name}"
    try:
        n = spark.table(fqn).count()
    except Exception:
        n = 0
    total += n
    print(f"  {table_name:35s} {n:>10,}")
print("-" * 60)
print(f"  {'TOTAL':35s} {total:>10,}")

if total == 0:
    print()
    print("⚠️  No data ingested. Possible causes:")
    print("   1. The Cloudflare Worker hasn't run yet (no files in R2)")
    print("   2. R2 credentials not set (run 01a first)")
    print("   3. Bucket name mismatch (check r2_bucket widget)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Data

# COMMAND ----------

for table_name in TABLE_MAP.values():
    fqn = f"{CATALOG}.{SCHEMA}.{table_name}"
    try:
        count = spark.table(fqn).count()
        if count > 0:
            print(f"\n{'─' * 60}")
            print(f"  {table_name} — {count:,} rows (showing first 5)")
            print(f"{'─' * 60}")
            display(spark.table(fqn).limit(5))  # noqa: F405
    except Exception:
        pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scheduling
# MAGIC
# MAGIC This notebook is designed for **scheduled execution**:
# MAGIC
# MAGIC 1. Each run syncs only **new** files from R2 (checkpoint-tracked)
# MAGIC 2. Bronze tables are rebuilt from all synced files in the Volume
# MAGIC 3. Schedule every 15–30 minutes to keep bronze fresh
# MAGIC
# MAGIC **Setup:** Workflows → Create Job → Add this notebook → Schedule `*/15 * * * *`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC - The **DLT pipeline** (notebooks 03 + 04) works with these streaming tables.
# MAGIC   Update source table names in notebook 03:
# MAGIC   ```python
# MAGIC   spark.read.table("fintech_lab.bronze.raw_customers_stream")
# MAGIC   ```
# MAGIC - Run `05_fraud_detection` and `06_analytics_queries` against the gold layer.
# MAGIC - The **original notebooks 01 + 02** remain as an offline fallback.
