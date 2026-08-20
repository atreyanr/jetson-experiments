# Databricks notebook source
# ruff: noqa: E402, F821

# MAGIC %md
# MAGIC # Configure Cloudflare R2 Connection
# MAGIC
# MAGIC This notebook establishes a connection between Databricks and a **Cloudflare R2** bucket
# MAGIC that serves as the landing zone for our fintech event data.
# MAGIC
# MAGIC **Databricks Free Edition uses serverless compute (Spark Connect)**, which does not
# MAGIC support Hadoop filesystem configs (`fs.s3a.*`). Instead, we use **`boto3`** (the standard
# MAGIC Python S3 client) to pull data from R2 into a Unity Catalog Volume, then Spark reads
# MAGIC from the Volume natively.
# MAGIC
# MAGIC **Run this notebook once** per session before using `01b_streaming_ingestion`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prerequisites
# MAGIC
# MAGIC Before running this notebook, ensure you have:
# MAGIC
# MAGIC 1. ✅ A **Cloudflare R2 bucket** named `fintech-events`
# MAGIC 2. ✅ An **R2 API token** with Object Read permission
# MAGIC    - Found in: Cloudflare Dashboard → R2 → Manage R2 API Tokens
# MAGIC 3. ✅ Your **R2 Access Key ID** and **Secret Access Key** (from the API token)
# MAGIC 4. ✅ Your **R2 S3-compatible endpoint URL**
# MAGIC    - Format: `https://<account_id>.r2.cloudflarestorage.com`
# MAGIC 5. ✅ Notebook `00_setup_catalog` has been run (Unity Catalog + Volume exist)

# COMMAND ----------

# MAGIC %pip install boto3 -q

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Provide R2 Credentials
# MAGIC
# MAGIC We use Databricks widgets so credentials are never hardcoded in the notebook.
# MAGIC Fill in the widget values at the top of the notebook after running this cell.

# COMMAND ----------

dbutils.widgets.text("r2_access_key", "", "R2 Access Key ID")  # noqa: F405
dbutils.widgets.text("r2_secret_key", "", "R2 Secret Access Key")  # noqa: F405
dbutils.widgets.text("r2_endpoint", "", "R2 S3 Endpoint URL")  # noqa: F405
dbutils.widgets.text("r2_bucket", "fintech-events", "R2 Bucket Name")  # noqa: F405

print("⬆️  Fill in the widget values at the top of this notebook, then run the next cell.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Credential Storage Options
# MAGIC
# MAGIC ### Option A — Databricks Secrets (recommended for production)
# MAGIC
# MAGIC ```bash
# MAGIC databricks secrets create-scope fintech-r2
# MAGIC databricks secrets put-secret fintech-r2 access-key --string-value "<your-access-key>"
# MAGIC databricks secrets put-secret fintech-r2 secret-key --string-value "<your-secret-key>"
# MAGIC databricks secrets put-secret fintech-r2 endpoint   --string-value "https://<account_id>.r2.cloudflarestorage.com"
# MAGIC ```
# MAGIC
# MAGIC Then replace widget reads with:
# MAGIC ```python
# MAGIC r2_access_key = dbutils.secrets.get("fintech-r2", "access-key")
# MAGIC ```
# MAGIC
# MAGIC ### Option B — Widgets (used here for quick setup)

# COMMAND ----------

# Read credentials from widgets
r2_access_key = dbutils.widgets.get("r2_access_key")  # noqa: F405
r2_secret_key = dbutils.widgets.get("r2_secret_key")  # noqa: F405
r2_endpoint = dbutils.widgets.get("r2_endpoint")  # noqa: F405
r2_bucket = dbutils.widgets.get("r2_bucket")  # noqa: F405

missing = []
if not r2_access_key:
    missing.append("R2 Access Key ID")
if not r2_secret_key:
    missing.append("R2 Secret Access Key")
if not r2_endpoint:
    missing.append("R2 S3 Endpoint URL")

if missing:
    raise ValueError(
        f"Missing required credentials: {', '.join(missing)}. "
        "Fill in the widgets at the top of this notebook."
    )

print(f"✅ Credentials loaded for bucket: {r2_bucket}")
print(f"   Endpoint: {r2_endpoint}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Connect to R2 via boto3
# MAGIC
# MAGIC Databricks Free Edition (serverless / Spark Connect) blocks Hadoop `fs.s3a.*` configs.
# MAGIC We use `boto3` instead — the standard Python S3 client that works with any
# MAGIC S3-compatible storage including Cloudflare R2.

# COMMAND ----------

import boto3

s3 = boto3.client(
    "s3",
    endpoint_url=r2_endpoint,
    aws_access_key_id=r2_access_key,
    aws_secret_access_key=r2_secret_key,
    region_name="auto",
)

print("✅ boto3 S3 client configured for Cloudflare R2")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Test the Connection

# COMMAND ----------

try:
    response = s3.list_objects_v2(Bucket=r2_bucket, MaxKeys=20)
    objects = response.get("Contents", [])
    print(f"✅ Connected to R2 bucket: {r2_bucket}")
    print(f"   Endpoint: {r2_endpoint}")
    print(f"   Found {len(objects)} objects (showing up to 20):")
    for obj in objects:
        size_str = f"{obj['Size']:,} bytes"
        print(f"   📄 {obj['Key']:55s} {size_str}")
    if response.get("IsTruncated"):
        print("   ... (more objects exist)")
except Exception as e:
    error_msg = str(e)
    print(f"❌ Connection failed: {error_msg[:200]}")
    print()
    if "403" in error_msg or "Forbidden" in error_msg:
        print("  → Check R2 API token has Object Read permission.")
    elif "404" in error_msg or "NoSuchBucket" in error_msg:
        print(f"  → Bucket '{r2_bucket}' not found.")
    elif "resolve" in error_msg.lower():
        print(f"  → Cannot resolve endpoint: {r2_endpoint}")
        print("    Expected: https://<account_id>.r2.cloudflarestorage.com")
    else:
        print("  → Check Access Key, Secret Key, and endpoint URL.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Verify Volume Exists

# COMMAND ----------

VOLUME_PATH = "/Volumes/fintech_lab/bronze/landing_zone"

try:
    files = dbutils.fs.ls(VOLUME_PATH)  # noqa: F405
    print(f"✅ Volume exists: {VOLUME_PATH}")
    print(f"   Contains {len(files)} items")
except Exception:
    print(f"⚠️  Volume not found at {VOLUME_PATH}")
    print("   Run notebook 00_setup_catalog first to create it.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Configuration Complete
# MAGIC
# MAGIC The boto3 S3 client can reach Cloudflare R2. The ingestion flow:
# MAGIC
# MAGIC 1. `boto3` downloads JSONL files from R2 → Unity Catalog Volume
# MAGIC 2. Spark reads from the Volume natively (no S3A config needed)
# MAGIC 3. Works on **all Databricks compute types** including serverless
# MAGIC
# MAGIC **Next:** Run `01b_streaming_ingestion` to sync R2 data into bronze Delta tables.
