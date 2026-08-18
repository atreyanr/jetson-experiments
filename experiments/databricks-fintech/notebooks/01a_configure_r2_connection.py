# Databricks notebook source
# ruff: noqa: E402, F821

# MAGIC %md
# MAGIC # Configure Cloudflare R2 Connection
# MAGIC
# MAGIC This notebook establishes a connection between Databricks and a **Cloudflare R2** bucket
# MAGIC that serves as the landing zone for our fintech event data. R2 is S3-compatible, so we
# MAGIC use Spark's S3A filesystem connector with R2's endpoint URL.
# MAGIC
# MAGIC **Run this notebook once** per cluster session before using `01b_streaming_ingestion`.
# MAGIC The configuration persists for the lifetime of the cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prerequisites
# MAGIC
# MAGIC Before running this notebook, ensure you have:
# MAGIC
# MAGIC 1. ✅ A **Cloudflare R2 bucket** named `fintech-events` (or your chosen name)
# MAGIC 2. ✅ An **R2 API token** with Object Read & Write permissions
# MAGIC 3. ✅ Your **R2 Access Key ID** and **Secret Access Key**
# MAGIC    - Found in: Cloudflare Dashboard → R2 → Manage R2 API Tokens
# MAGIC 4. ✅ Your **R2 S3-compatible endpoint URL**
# MAGIC    - Format: `https://<account_id>.r2.cloudflarestorage.com`
# MAGIC    - Found in: Cloudflare Dashboard → R2 → your bucket → Settings → S3 API
# MAGIC
# MAGIC > **Tip:** The account ID is the hex string in your Cloudflare dashboard URL.

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
# MAGIC Store credentials in a Databricks secret scope so they are encrypted at rest
# MAGIC and never visible in notebook output:
# MAGIC
# MAGIC ```bash
# MAGIC # Run these via the Databricks CLI (one-time setup):
# MAGIC databricks secrets create-scope fintech-r2
# MAGIC databricks secrets put-secret fintech-r2 access-key --string-value "<your-access-key>"
# MAGIC databricks secrets put-secret fintech-r2 secret-key --string-value "<your-secret-key>"
# MAGIC databricks secrets put-secret fintech-r2 endpoint   --string-value "https://<account_id>.r2.cloudflarestorage.com"
# MAGIC ```
# MAGIC
# MAGIC Then replace the widget reads below with:
# MAGIC ```python
# MAGIC r2_access_key = dbutils.secrets.get("fintech-r2", "access-key")
# MAGIC r2_secret_key = dbutils.secrets.get("fintech-r2", "secret-key")
# MAGIC r2_endpoint   = dbutils.secrets.get("fintech-r2", "endpoint")
# MAGIC ```
# MAGIC
# MAGIC ### Option B — Widgets (used here for learning / quick setup)
# MAGIC
# MAGIC The widgets above are fine for experimentation. The values are session-scoped
# MAGIC and not persisted to the notebook source.

# COMMAND ----------

# Read credentials from widgets
r2_access_key = dbutils.widgets.get("r2_access_key")  # noqa: F405
r2_secret_key = dbutils.widgets.get("r2_secret_key")  # noqa: F405
r2_endpoint = dbutils.widgets.get("r2_endpoint")  # noqa: F405
r2_bucket = dbutils.widgets.get("r2_bucket")  # noqa: F405

# Validate that all fields are filled
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
# MAGIC ## Step 3: Configure Spark S3A Connector for R2
# MAGIC
# MAGIC Cloudflare R2 is S3-compatible but requires **path-style access** (not virtual-hosted).
# MAGIC We configure Spark's `S3AFileSystem` to point at the R2 endpoint.

# COMMAND ----------

# R2 is S3-compatible — configure the S3A filesystem connector
spark.conf.set("fs.s3a.endpoint", r2_endpoint)
spark.conf.set("fs.s3a.access.key", r2_access_key)
spark.conf.set("fs.s3a.secret.key", r2_secret_key)

# R2 requires path-style access (not virtual-hosted-style)
spark.conf.set("fs.s3a.path.style.access", "true")

# Use the S3A filesystem implementation
spark.conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# Enable SSL for secure transport
spark.conf.set("fs.s3a.connection.ssl.enabled", "true")

# Disable S3 bucket existence checks (R2 doesn't support GetBucketLocation)
spark.conf.set("fs.s3a.bucket.probe", "0")

print("✅ Spark S3A connector configured for Cloudflare R2")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Test the Connection
# MAGIC
# MAGIC Verify we can reach the R2 bucket and list its contents.

# COMMAND ----------

base_path = f"s3a://{r2_bucket}"

try:
    files = dbutils.fs.ls(base_path)  # noqa: F405
    print(f"✅ Connected to R2 bucket: {r2_bucket}")
    print(f"   Endpoint: {r2_endpoint}")
    print(f"   Found {len(files)} top-level paths:")
    for f in files[:20]:  # Show first 20 to avoid flooding output
        size_str = f"{f.size:,} bytes" if f.size > 0 else "directory"
        print(f"   📁 {f.name:40s} {size_str}")
    if len(files) > 20:
        print(f"   ... and {len(files) - 20} more")
except Exception as e:
    error_msg = str(e)
    print(f"❌ Connection failed: {error_msg[:200]}")
    print()
    print("Troubleshooting:")
    if "403" in error_msg or "Forbidden" in error_msg:
        print("  → Access denied. Check that your R2 API token has Object Read permission.")
    elif "404" in error_msg or "NoSuchBucket" in error_msg:
        print(f"  → Bucket '{r2_bucket}' not found. Verify the bucket name in Cloudflare R2.")
    elif "UnknownHost" in error_msg or "resolve" in error_msg.lower():
        print(f"  → Cannot resolve endpoint. Verify: {r2_endpoint}")
        print("    Expected format: https://<account_id>.r2.cloudflarestorage.com")
    else:
        print("  → Check your R2 Access Key, Secret Key, and endpoint URL.")
        print("  → Ensure the R2 API token has not expired.")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- (Optional) Unity Catalog External Location
# MAGIC --
# MAGIC -- For full governance over external data, register the R2 bucket as an
# MAGIC -- external location. This requires a storage credential and admin privileges.
# MAGIC --
# MAGIC -- Step 1: Create a storage credential (admin only):
# MAGIC -- CREATE STORAGE CREDENTIAL IF NOT EXISTS fintech_r2_cred
# MAGIC -- WITH (AWS_KEY_ID = '<r2_access_key>', AWS_SECRET_KEY = '<r2_secret_key>');
# MAGIC --
# MAGIC -- Step 2: Create the external location:
# MAGIC -- CREATE EXTERNAL LOCATION IF NOT EXISTS fintech_r2
# MAGIC -- URL 's3a://fintech-events'
# MAGIC -- WITH (STORAGE CREDENTIAL fintech_r2_cred);
# MAGIC --
# MAGIC -- Note: External locations with custom S3 endpoints may have limited support
# MAGIC -- in Databricks Free Edition. The direct S3A configuration above works universally.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Configuration Complete
# MAGIC
# MAGIC The Spark session is now configured to read from Cloudflare R2. This configuration
# MAGIC persists for the lifetime of the current cluster.
# MAGIC
# MAGIC **Next steps:**
# MAGIC 1. Run `01b_streaming_ingestion` to start streaming data from R2 into bronze Delta tables
# MAGIC 2. Or run `02_bronze_ingestion` if you prefer the batch approach with locally generated data
# MAGIC
# MAGIC **To re-run later:** If the cluster restarts, run this notebook again to restore the R2 connection.
