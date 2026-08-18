# Databricks notebook source
# ruff: noqa: E402, F821

# MAGIC %md
# MAGIC # 🏗️ Notebook 00 — Unity Catalog Setup
# MAGIC
# MAGIC This notebook bootstraps the **Unity Catalog** infrastructure for the
# MAGIC fintech-transactions pipeline.
# MAGIC
# MAGIC ### What is Unity Catalog?
# MAGIC Unity Catalog is Databricks' unified governance layer.  It organises data
# MAGIC in a three-level namespace:
# MAGIC
# MAGIC ```
# MAGIC catalog
# MAGIC └── schema (a.k.a. database)
# MAGIC     ├── table / view
# MAGIC     └── volume  (managed file storage)
# MAGIC ```
# MAGIC
# MAGIC We'll create:
# MAGIC
# MAGIC | Object | Name | Purpose |
# MAGIC |--------|------|---------|
# MAGIC | **Catalog** | `fintech_lab` | Top-level container for the whole project |
# MAGIC | **Schema** | `bronze` | Raw, untransformed data |
# MAGIC | **Schema** | `silver` | Cleaned, validated, deduplicated data |
# MAGIC | **Schema** | `gold` | Business-level aggregates & metrics |
# MAGIC | **Volume** | `bronze.landing_zone` | File landing area for raw JSON ingestion |
# MAGIC
# MAGIC > **Idempotent**: every statement uses `IF NOT EXISTS` so you can re-run
# MAGIC > this notebook safely.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Create the catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS fintech_lab;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG fintech_lab;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Create the medallion schemas
# MAGIC
# MAGIC The **medallion architecture** (bronze → silver → gold) is the standard
# MAGIC Databricks pattern for progressively refining data quality.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze
# MAGIC COMMENT 'Raw ingestion layer – append-only, no transforms';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS silver
# MAGIC COMMENT 'Cleaned & validated layer – deduped, typed, enriched';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS gold
# MAGIC COMMENT 'Business aggregates – customer 360, fraud scores, revenue';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Create the raw-file landing volume
# MAGIC
# MAGIC A **managed volume** gives us a cloud-storage-backed directory we can
# MAGIC write files into using `/Volumes/fintech_lab/bronze/landing_zone/...`.
# MAGIC
# MAGIC The data-generation notebook will drop JSON files here; the bronze
# MAGIC ingestion notebook reads from here.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS bronze.landing_zone
# MAGIC COMMENT 'Raw JSON landing zone for batch ingestion';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Verify the setup

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN fintech_lab;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VOLUMES IN fintech_lab.bronze;

# COMMAND ----------

print("✅  Unity Catalog setup complete.")
print()
print("Catalog:  fintech_lab")
print("Schemas:  bronze · silver · gold")
print("Volume:   fintech_lab.bronze.landing_zone")
print("Path:     /Volumes/fintech_lab/bronze/landing_zone/")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Next →** Run `01_generate_data` to populate the landing zone with
# MAGIC synthetic fintech data.
