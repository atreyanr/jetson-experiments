# Databricks notebook source
# ruff: noqa: E402, F821

# MAGIC %md
# MAGIC # 🔍 Advanced Fraud Detection Engine
# MAGIC
# MAGIC This notebook implements a **rule-based fraud detection engine** that runs as a scheduled
# MAGIC Databricks Job (outside the DLT pipeline). It demonstrates sophisticated fraud detection
# MAGIC patterns commonly used in fintech:
# MAGIC
# MAGIC | Rule | Pattern | Why It Matters |
# MAGIC |------|---------|----------------|
# MAGIC | 1 | Velocity check | Card-present fraud often involves rapid-fire purchases |
# MAGIC | 2 | Amount anomaly | Stolen credentials lead to unusual spending |
# MAGIC | 3 | Geographic impossibility | Can't physically be in two countries within hours |
# MAGIC | 4 | Dormant account spike | Compromised dormant accounts are high-value targets |
# MAGIC | 5 | Round amount pattern | Money laundering uses round numbers to move funds |
# MAGIC | 6 | Merchant concentration | Bust-out fraud funnels spend to a single merchant |
# MAGIC
# MAGIC **Architecture**: Reads from `silver.transactions_enriched`, applies each rule independently,
# MAGIC unions all alerts, and writes to `gold.fraud_alerts` via Delta merge (upsert).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup & Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from functools import reduce

# -- Configuration ----------------------------------------------------------
CATALOG = "fintech_lab"
SILVER_SCHEMA = f"{CATALOG}.silver"
GOLD_SCHEMA = f"{CATALOG}.gold"

SOURCE_TABLE = f"{SILVER_SCHEMA}.transactions_enriched"
ALERTS_TABLE = f"{GOLD_SCHEMA}.fraud_alerts"

# Thresholds — tune these per your risk appetite
VELOCITY_WINDOW_HOURS = 24
VELOCITY_THRESHOLD = 10
ANOMALY_STDDEV_MULTIPLIER = 3.0
ANOMALY_LOOKBACK_DAYS = 90
GEO_WINDOW_HOURS = 2
DORMANT_DAYS = 60
DORMANT_AMOUNT_THRESHOLD = 1000.0
ROUND_AMOUNT_CONSECUTIVE = 3
MERCHANT_CONCENTRATION_PCT = 0.80
MERCHANT_CONCENTRATION_DAYS = 7

print(f"Source: {SOURCE_TABLE}")
print(f"Target: {ALERTS_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Source Data
# MAGIC
# MAGIC We read from the silver enriched transactions table, which already has merchant
# MAGIC and account information joined in. We also bring in customer home country
# MAGIC for geographic comparisons.

# COMMAND ----------

txn = (
    spark.read.table(SOURCE_TABLE)
    .withColumn("txn_ts", F.col("transaction_timestamp").cast("timestamp"))
    .withColumn("txn_date", F.to_date("txn_ts"))
)

customers = spark.read.table(f"{SILVER_SCHEMA}.customers").select(
    "customer_id", F.col("country").alias("home_country")
)

# Join home country onto transactions for geo rules
txn = txn.join(
    customers,
    on="customer_id",
    how="left"
)

txn.cache()
total_txn_count = txn.count()
print(f"Loaded {total_txn_count:,} enriched transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rule 1 — Velocity Check
# MAGIC
# MAGIC **Pattern**: More than N transactions from the same account within a rolling 24-hour window.
# MAGIC
# MAGIC **Why**: Stolen card details are often tested with many small purchases in quick succession
# MAGIC before the cardholder notices. Payment processors flag rapid bursts as a primary signal.
# MAGIC
# MAGIC **Implementation**: Use a range-based window (seconds) partitioned by `account_id`,
# MAGIC ordered by timestamp, counting rows within the preceding 24 hours.

# COMMAND ----------

velocity_window = (
    Window.partitionBy("account_id")
    .orderBy(F.col("txn_ts").cast("long"))
    .rangeBetween(-VELOCITY_WINDOW_HOURS * 3600, 0)
)

velocity_alerts = (
    txn
    .withColumn("txn_count_24h", F.count("transaction_id").over(velocity_window))
    .filter(F.col("txn_count_24h") > VELOCITY_THRESHOLD)
    .select(
        "transaction_id", "account_id", "customer_id", "amount", "txn_ts",
        F.lit("velocity_check").alias("alert_type"),
        F.lit("high").alias("alert_severity"),
        F.concat(
            F.lit("Account had "),
            F.col("txn_count_24h").cast("string"),
            F.lit(f" transactions in {VELOCITY_WINDOW_HOURS}h (threshold: {VELOCITY_THRESHOLD})")
        ).alias("alert_description"),
        F.col("txn_count_24h").alias("rule_detail_value")
    )
)

velocity_count = velocity_alerts.count()
print(f"Rule 1 — Velocity: {velocity_count:,} alerts")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rule 2 — Amount Anomaly
# MAGIC
# MAGIC **Pattern**: Transaction amount exceeds 3 standard deviations above the account's
# MAGIC 90-day rolling mean.
# MAGIC
# MAGIC **Why**: When credentials are stolen, fraudsters often make purchases much larger
# MAGIC than the legitimate cardholder's typical spending. Statistical outlier detection
# MAGIC catches these without hard-coded thresholds.
# MAGIC
# MAGIC **Implementation**: Window function over last 90 days per account to compute
# MAGIC rolling mean and stddev, then flag outliers.

# COMMAND ----------

anomaly_window = (
    Window.partitionBy("account_id")
    .orderBy(F.col("txn_ts").cast("long"))
    .rangeBetween(-ANOMALY_LOOKBACK_DAYS * 86400, -1)  # exclude current row
)

amount_anomaly_alerts = (
    txn
    .withColumn("rolling_mean", F.avg("amount").over(anomaly_window))
    .withColumn("rolling_stddev", F.stddev("amount").over(anomaly_window))
    # Need at least 10 prior transactions for meaningful statistics
    .withColumn("prior_count", F.count("transaction_id").over(anomaly_window))
    .filter(F.col("prior_count") >= 10)
    .filter(F.col("rolling_stddev") > 0)
    .withColumn(
        "z_score",
        (F.col("amount") - F.col("rolling_mean")) / F.col("rolling_stddev")
    )
    .filter(F.col("z_score") > ANOMALY_STDDEV_MULTIPLIER)
    .select(
        "transaction_id", "account_id", "customer_id", "amount", "txn_ts",
        F.lit("amount_anomaly").alias("alert_type"),
        F.when(F.col("z_score") > 5, "critical")
         .when(F.col("z_score") > 4, "high")
         .otherwise("medium").alias("alert_severity"),
        F.concat(
            F.lit("Amount $"),
            F.round("amount", 2).cast("string"),
            F.lit(" is "),
            F.round("z_score", 1).cast("string"),
            F.lit("σ above rolling mean of $"),
            F.round("rolling_mean", 2).cast("string")
        ).alias("alert_description"),
        F.round("z_score", 2).alias("rule_detail_value")
    )
)

anomaly_count = amount_anomaly_alerts.count()
print(f"Rule 2 — Amount Anomaly: {anomaly_count:,} alerts")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rule 3 — Geographic Impossibility
# MAGIC
# MAGIC **Pattern**: Two consecutive transactions from the same account occur in different
# MAGIC countries within 2 hours — physically impossible without air travel between most pairs.
# MAGIC
# MAGIC **Why**: Cloned cards are often used simultaneously in different locations. This rule
# MAGIC detects scenarios where the cardholder couldn't have physically traveled between
# MAGIC transaction locations in the elapsed time.
# MAGIC
# MAGIC **Implementation**: LAG window to compare each transaction's country with the previous
# MAGIC one, checking time difference.

# COMMAND ----------

geo_window = Window.partitionBy("account_id").orderBy("txn_ts")

geo_alerts = (
    txn
    .filter(F.col("location_country").isNotNull())
    .withColumn("prev_country", F.lag("location_country").over(geo_window))
    .withColumn("prev_ts", F.lag("txn_ts").over(geo_window))
    .filter(F.col("prev_country").isNotNull())
    .withColumn(
        "hours_gap",
        (F.col("txn_ts").cast("long") - F.col("prev_ts").cast("long")) / 3600.0
    )
    # Different country AND within the time threshold
    .filter(
        (F.col("location_country") != F.col("prev_country"))
        & (F.col("hours_gap") <= GEO_WINDOW_HOURS)
        & (F.col("hours_gap") > 0)
    )
    .select(
        "transaction_id", "account_id", "customer_id", "amount", "txn_ts",
        F.lit("geo_impossibility").alias("alert_type"),
        F.lit("critical").alias("alert_severity"),
        F.concat(
            F.lit("Transaction in "),
            F.col("location_country"),
            F.lit(" only "),
            F.round("hours_gap", 1).cast("string"),
            F.lit("h after transaction in "),
            F.col("prev_country")
        ).alias("alert_description"),
        F.round("hours_gap", 2).alias("rule_detail_value")
    )
)

geo_count = geo_alerts.count()
print(f"Rule 3 — Geographic Impossibility: {geo_count:,} alerts")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rule 4 — Dormant Account Spike
# MAGIC
# MAGIC **Pattern**: An account with no transactions in 60+ days suddenly has a transaction
# MAGIC exceeding $1,000.
# MAGIC
# MAGIC **Why**: Compromised accounts that have been inactive are prime targets — the legitimate
# MAGIC owner is unlikely to notice unusual activity quickly. Fraudsters often "sit" on stolen
# MAGIC credentials and exploit them after a cooling-off period.
# MAGIC
# MAGIC **Implementation**: LAG window to find the gap between consecutive transactions per account.

# COMMAND ----------

dormant_window = Window.partitionBy("account_id").orderBy("txn_ts")

dormant_alerts = (
    txn
    .withColumn("prev_txn_ts", F.lag("txn_ts").over(dormant_window))
    .filter(F.col("prev_txn_ts").isNotNull())
    .withColumn(
        "days_since_last",
        F.datediff(F.col("txn_ts").cast("date"), F.col("prev_txn_ts").cast("date"))
    )
    .filter(
        (F.col("days_since_last") >= DORMANT_DAYS)
        & (F.col("amount") >= DORMANT_AMOUNT_THRESHOLD)
    )
    .select(
        "transaction_id", "account_id", "customer_id", "amount", "txn_ts",
        F.lit("dormant_spike").alias("alert_type"),
        F.when(F.col("amount") > 5000, "critical")
         .when(F.col("amount") > 2000, "high")
         .otherwise("medium").alias("alert_severity"),
        F.concat(
            F.lit("$"),
            F.round("amount", 2).cast("string"),
            F.lit(" transaction after "),
            F.col("days_since_last").cast("string"),
            F.lit(" days of inactivity")
        ).alias("alert_description"),
        F.col("days_since_last").cast("double").alias("rule_detail_value")
    )
)

dormant_count = dormant_alerts.count()
print(f"Rule 4 — Dormant Account Spike: {dormant_count:,} alerts")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rule 5 — Round Amount Pattern
# MAGIC
# MAGIC **Pattern**: 3+ consecutive transactions from the same account with perfectly round
# MAGIC amounts ($100, $200, $500, $1000, etc.).
# MAGIC
# MAGIC **Why**: Money laundering often involves structured transactions at round amounts
# MAGIC to move funds through the system. Legitimate spending rarely produces long sequences
# MAGIC of perfectly round numbers. This is related to "structuring" (a.k.a. "smurfing")
# MAGIC where criminals break large amounts into smaller round transfers.
# MAGIC
# MAGIC **Implementation**: Flag transactions where amount % 50 == 0 (round to nearest $50),
# MAGIC then use a running count of consecutive round transactions per account.

# COMMAND ----------

round_window = Window.partitionBy("account_id").orderBy("txn_ts")

round_alerts = (
    txn
    .withColumn(
        "is_round",
        F.when(
            (F.col("amount") % 50 == 0) & (F.col("amount") > 0),
            F.lit(1)
        ).otherwise(F.lit(0))
    )
    # Build groups: each non-round transaction resets the streak
    .withColumn("row_num", F.row_number().over(round_window))
    .withColumn(
        "round_group",
        F.col("row_num") - F.sum("is_round").over(
            round_window.rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
    )
    # Only keep round transactions
    .filter(F.col("is_round") == 1)
    # Count consecutive round transactions in each group
    .withColumn(
        "consecutive_round",
        F.count("*").over(Window.partitionBy("account_id", "round_group"))
    )
    .filter(F.col("consecutive_round") >= ROUND_AMOUNT_CONSECUTIVE)
    .select(
        "transaction_id", "account_id", "customer_id", "amount", "txn_ts",
        F.lit("round_amount_pattern").alias("alert_type"),
        F.when(F.col("consecutive_round") >= 5, "high")
         .otherwise("medium").alias("alert_severity"),
        F.concat(
            F.col("consecutive_round").cast("string"),
            F.lit(" consecutive round-amount transactions (current: $"),
            F.round("amount", 0).cast("string"),
            F.lit(")")
        ).alias("alert_description"),
        F.col("consecutive_round").cast("double").alias("rule_detail_value")
    )
)

round_count = round_alerts.count()
print(f"Rule 5 — Round Amount Pattern: {round_count:,} alerts")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rule 6 — Merchant Concentration
# MAGIC
# MAGIC **Pattern**: Over 80% of an account's transactions in the last 7 days go to a
# MAGIC single merchant.
# MAGIC
# MAGIC **Why**: "Bust-out" fraud involves maxing out credit on a single merchant
# MAGIC (often a colluding business). Legitimate customers diversify their spending
# MAGIC across multiple merchants. A sudden concentration is suspicious, especially
# MAGIC combined with high total volume.
# MAGIC
# MAGIC **Implementation**: Rolling 7-day window per account, count transactions per
# MAGIC merchant, compare to total.

# COMMAND ----------

merchant_conc_window = (
    Window.partitionBy("account_id")
    .orderBy(F.col("txn_ts").cast("long"))
    .rangeBetween(-MERCHANT_CONCENTRATION_DAYS * 86400, 0)
)

# Count total transactions and per-merchant transactions in 7-day window
txn_with_counts = (
    txn
    .filter(F.col("merchant_id").isNotNull())
    .withColumn("total_7d", F.count("transaction_id").over(merchant_conc_window))
    .withColumn(
        "merchant_7d",
        F.count("transaction_id").over(
            Window.partitionBy("account_id", "merchant_id")
            .orderBy(F.col("txn_ts").cast("long"))
            .rangeBetween(-MERCHANT_CONCENTRATION_DAYS * 86400, 0)
        )
    )
    .withColumn("concentration_pct", F.col("merchant_7d") / F.col("total_7d"))
)

merchant_conc_alerts = (
    txn_with_counts
    .filter(
        (F.col("concentration_pct") >= MERCHANT_CONCENTRATION_PCT)
        & (F.col("total_7d") >= 5)  # Need at least 5 transactions to be meaningful
    )
    .select(
        "transaction_id", "account_id", "customer_id", "amount", "txn_ts",
        F.lit("merchant_concentration").alias("alert_type"),
        F.when(F.col("concentration_pct") >= 0.95, "high")
         .otherwise("medium").alias("alert_severity"),
        F.concat(
            F.round(F.col("concentration_pct") * 100, 0).cast("string"),
            F.lit("% of last 7d transactions ("),
            F.col("merchant_7d").cast("string"),
            F.lit("/"),
            F.col("total_7d").cast("string"),
            F.lit(") at merchant "),
            F.coalesce(F.col("merchant_name"), F.col("merchant_id"))
        ).alias("alert_description"),
        F.round("concentration_pct", 4).alias("rule_detail_value")
    )
)

merchant_conc_count = merchant_conc_alerts.count()
print(f"Rule 6 — Merchant Concentration: {merchant_conc_count:,} alerts")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Combine All Alerts & Write to Gold
# MAGIC
# MAGIC Union all rule outputs into a single alerts DataFrame, add a detection timestamp,
# MAGIC deduplicate (a single transaction can trigger multiple rules), and merge into the
# MAGIC `gold.fraud_alerts` table.

# COMMAND ----------

# Standardize schema and union all alerts
all_alerts = reduce(
    lambda a, b: a.unionByName(b),
    [
        velocity_alerts,
        amount_anomaly_alerts,
        geo_alerts,
        dormant_alerts,
        round_alerts,
        merchant_conc_alerts,
    ]
).withColumn("detection_timestamp", F.current_timestamp())

# A transaction can trigger multiple rules — keep all, but add a composite key
all_alerts = all_alerts.withColumn(
    "alert_id",
    F.concat_ws("_", "transaction_id", "alert_type")
)

alert_count = all_alerts.count()
print(f"Total alerts before dedup: {alert_count:,}")

# Deduplicate: keep highest severity per (transaction_id, alert_type)
severity_order = F.when(F.col("alert_severity") == "critical", 1) \
                  .when(F.col("alert_severity") == "high", 2) \
                  .when(F.col("alert_severity") == "medium", 3) \
                  .otherwise(4)

dedup_window = Window.partitionBy("alert_id").orderBy(severity_order)

alerts_deduped = (
    all_alerts
    .withColumn("rn", F.row_number().over(dedup_window))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

final_count = alerts_deduped.count()
print(f"Total alerts after dedup: {final_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write alerts via Delta MERGE (upsert)
# MAGIC
# MAGIC Using `MERGE INTO` so re-running the notebook updates existing alerts rather than
# MAGIC duplicating them. The merge key is `alert_id` (composite of transaction + rule).

# COMMAND ----------

# Ensure target table exists (first run)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {ALERTS_TABLE} (
        alert_id STRING,
        transaction_id STRING,
        account_id STRING,
        customer_id STRING,
        amount DOUBLE,
        txn_ts TIMESTAMP,
        alert_type STRING,
        alert_severity STRING,
        alert_description STRING,
        rule_detail_value DOUBLE,
        detection_timestamp TIMESTAMP
    )
    USING DELTA
    COMMENT 'Fraud detection alerts from rule-based engine'
""")

# Register source as temp view for SQL merge
alerts_deduped.createOrReplaceTempView("new_alerts")

spark.sql(f"""
    MERGE INTO {ALERTS_TABLE} AS target
    USING new_alerts AS source
    ON target.alert_id = source.alert_id
    WHEN MATCHED THEN UPDATE SET
        alert_severity = source.alert_severity,
        alert_description = source.alert_description,
        rule_detail_value = source.rule_detail_value,
        detection_timestamp = source.detection_timestamp
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"✅ Merged {final_count:,} alerts into {ALERTS_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Dashboard
# MAGIC
# MAGIC Quick overview of fraud detection results for this run.

# COMMAND ----------

# Alerts by type
print("=" * 60)
print("FRAUD ALERTS BY TYPE")
print("=" * 60)
alerts_deduped.groupBy("alert_type").agg(
    F.count("*").alias("alert_count"),
    F.round(F.avg("amount"), 2).alias("avg_amount"),
    F.countDistinct("account_id").alias("unique_accounts")
).orderBy(F.desc("alert_count")).show(truncate=False)

# COMMAND ----------

# Alerts by severity
print("=" * 60)
print("FRAUD ALERTS BY SEVERITY")
print("=" * 60)
alerts_deduped.groupBy("alert_severity").agg(
    F.count("*").alias("count"),
    F.round(F.sum("amount"), 2).alias("total_flagged_amount")
).orderBy(
    F.when(F.col("alert_severity") == "critical", 1)
     .when(F.col("alert_severity") == "high", 2)
     .when(F.col("alert_severity") == "medium", 3)
     .otherwise(4)
).show(truncate=False)

# COMMAND ----------

# Top 10 most-flagged accounts
print("=" * 60)
print("TOP 10 MOST-FLAGGED ACCOUNTS")
print("=" * 60)
alerts_deduped.groupBy("account_id", "customer_id").agg(
    F.count("*").alias("total_alerts"),
    F.sum(F.when(F.col("alert_severity") == "critical", 1).otherwise(0)).alias("critical"),
    F.sum(F.when(F.col("alert_severity") == "high", 1).otherwise(0)).alias("high"),
    F.round(F.sum("amount"), 2).alias("total_flagged_amount"),
    F.collect_set("alert_type").alias("triggered_rules")
).orderBy(F.desc("total_alerts")).limit(10).show(truncate=False)

# COMMAND ----------

# Overall fraud rate
fraud_rate = (final_count / total_txn_count * 100) if total_txn_count > 0 else 0
print("\n📊 FRAUD DETECTION SUMMARY")
print(f"   Total transactions analyzed: {total_txn_count:,}")
print(f"   Total alerts generated:      {final_count:,}")
print(f"   Alert rate:                  {fraud_rate:.2f}%")
print(f"   Unique accounts flagged:     {alerts_deduped.select('account_id').distinct().count():,}")

# COMMAND ----------

# Clean up cache
txn.unpersist()
print("✅ Fraud detection complete")
