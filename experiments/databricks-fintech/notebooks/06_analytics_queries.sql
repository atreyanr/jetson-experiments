-- Databricks notebook source

-- MAGIC %md
-- MAGIC # 📊 Fintech Analytics — Business Intelligence Queries
-- MAGIC
-- MAGIC This notebook contains **10 production-ready SQL queries** against the gold layer,
-- MAGIC designed to power Databricks SQL dashboards. Each query uses CTEs for readability
-- MAGIC and is optimized for the Delta Lake query engine.
-- MAGIC
-- MAGIC **How to use**: Run these in a Databricks SQL Warehouse to create dashboard visualizations,
-- MAGIC or schedule them as part of a workflow for reporting refreshes.
-- MAGIC
-- MAGIC | Query | Business Question |
-- MAGIC |-------|-------------------|
-- MAGIC | 1 | Executive KPIs (MTD/QTD/YTD) |
-- MAGIC | 2 | Monthly transaction trends with MoM change |
-- MAGIC | 3 | Customer acquisition funnel |
-- MAGIC | 4 | Revenue analysis by account type |
-- MAGIC | 5 | Top 10 merchants by volume |
-- MAGIC | 6 | Channel mix over time |
-- MAGIC | 7 | Customer segmentation dashboard |
-- MAGIC | 8 | Fraud detection summary |
-- MAGIC | 9 | Currency exposure (USD-normalized) |
-- MAGIC | 10 | Customer churn risk indicators |

-- COMMAND ----------

-- Set the default catalog so all queries resolve correctly
USE CATALOG fintech_lab;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Executive Dashboard KPIs
-- MAGIC
-- MAGIC A single-row summary of the key metrics an executive wants at a glance:
-- MAGIC total customers, active accounts, transaction volume for month/quarter/year,
-- MAGIC average transaction size, and the fraud alert rate.

-- COMMAND ----------

WITH current_period AS (
    SELECT
        current_date()                                    AS today,
        date_trunc('month', current_date())               AS mtd_start,
        date_trunc('quarter', current_date())             AS qtd_start,
        date_trunc('year', current_date())                AS ytd_start
),
customer_kpis AS (
    SELECT
        COUNT(DISTINCT customer_id)                       AS total_customers,
        SUM(CASE WHEN kyc_status = 'verified' THEN 1 ELSE 0 END) AS verified_customers,
        ROUND(AVG(lifetime_transaction_value), 2)         AS avg_customer_ltv
    FROM gold.customer_360
),
account_kpis AS (
    SELECT COUNT(*) AS active_accounts
    FROM silver.accounts
    WHERE status = 'active'
),
txn_kpis AS (
    SELECT
        COUNT(*)                                          AS total_transactions,
        ROUND(SUM(amount), 2)                             AS total_volume,
        ROUND(AVG(amount), 2)                             AS avg_transaction_size,
        SUM(CASE WHEN t.transaction_timestamp >= p.mtd_start THEN amount ELSE 0 END) AS mtd_volume,
        SUM(CASE WHEN t.transaction_timestamp >= p.qtd_start THEN amount ELSE 0 END) AS qtd_volume,
        SUM(CASE WHEN t.transaction_timestamp >= p.ytd_start THEN amount ELSE 0 END) AS ytd_volume
    FROM silver.transactions t
    CROSS JOIN current_period p
    WHERE t.status = 'completed'
),
fraud_kpis AS (
    SELECT
        COUNT(DISTINCT transaction_id)                    AS flagged_transactions,
        SUM(CASE WHEN alert_severity = 'critical' THEN 1 ELSE 0 END) AS critical_alerts
    FROM gold.fraud_alerts
)
SELECT
    c.total_customers,
    c.verified_customers,
    a.active_accounts,
    t.total_transactions,
    t.total_volume,
    t.avg_transaction_size,
    ROUND(t.mtd_volume, 2)                               AS mtd_volume,
    ROUND(t.qtd_volume, 2)                               AS qtd_volume,
    ROUND(t.ytd_volume, 2)                               AS ytd_volume,
    f.flagged_transactions,
    f.critical_alerts,
    ROUND(f.flagged_transactions * 100.0 / NULLIF(t.total_transactions, 0), 2)
                                                          AS fraud_alert_rate_pct,
    c.avg_customer_ltv
FROM customer_kpis c
CROSS JOIN account_kpis a
CROSS JOIN txn_kpis t
CROSS JOIN fraud_kpis f

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Monthly Transaction Trends
-- MAGIC
-- MAGIC Month-over-month transaction volume and count with percentage change.
-- MAGIC Great for a line chart with dual axes (volume bars + count line).

-- COMMAND ----------

WITH monthly AS (
    SELECT
        date_trunc('month', transaction_timestamp)        AS txn_month,
        COUNT(*)                                          AS txn_count,
        ROUND(SUM(amount), 2)                             AS total_volume,
        ROUND(AVG(amount), 2)                             AS avg_amount,
        COUNT(DISTINCT account_id)                        AS unique_accounts
    FROM silver.transactions
    WHERE status = 'completed'
    GROUP BY date_trunc('month', transaction_timestamp)
),
with_lag AS (
    SELECT
        txn_month,
        txn_count,
        total_volume,
        avg_amount,
        unique_accounts,
        LAG(txn_count) OVER (ORDER BY txn_month)          AS prev_count,
        LAG(total_volume) OVER (ORDER BY txn_month)        AS prev_volume
    FROM monthly
)
SELECT
    txn_month,
    txn_count,
    total_volume,
    avg_amount,
    unique_accounts,
    ROUND((txn_count - prev_count) * 100.0 / NULLIF(prev_count, 0), 1)
                                                          AS count_mom_pct,
    ROUND((total_volume - prev_volume) * 100.0 / NULLIF(prev_volume, 0), 1)
                                                          AS volume_mom_pct
FROM with_lag
ORDER BY txn_month

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Customer Acquisition Funnel
-- MAGIC
-- MAGIC New customers by month, KYC conversion rates, and median time to first transaction.
-- MAGIC Useful for a funnel chart or stacked bar visualization.

-- COMMAND ----------

WITH new_customers AS (
    SELECT
        date_trunc('month', created_at)                   AS signup_month,
        COUNT(*)                                          AS signups,
        SUM(CASE WHEN kyc_status = 'verified' THEN 1 ELSE 0 END) AS kyc_verified,
        SUM(CASE WHEN kyc_status = 'pending'  THEN 1 ELSE 0 END) AS kyc_pending,
        SUM(CASE WHEN kyc_status = 'rejected' THEN 1 ELSE 0 END) AS kyc_rejected
    FROM silver.customers
    GROUP BY date_trunc('month', created_at)
),
first_txns AS (
    SELECT
        c.customer_id,
        date_trunc('month', c.created_at)                 AS signup_month,
        MIN(t.transaction_timestamp)                      AS first_txn_ts,
        DATEDIFF(MIN(t.transaction_timestamp), c.created_at) AS days_to_first_txn
    FROM silver.customers c
    INNER JOIN silver.accounts a ON a.customer_id = c.customer_id
    INNER JOIN silver.transactions t ON t.account_id = a.account_id
    GROUP BY c.customer_id, date_trunc('month', c.created_at), c.created_at
),
first_txn_stats AS (
    SELECT
        signup_month,
        COUNT(*)                                          AS customers_with_txn,
        ROUND(AVG(days_to_first_txn), 1)                  AS avg_days_to_first_txn,
        PERCENTILE_APPROX(days_to_first_txn, 0.5)        AS median_days_to_first_txn
    FROM first_txns
    GROUP BY signup_month
)
SELECT
    nc.signup_month,
    nc.signups,
    nc.kyc_verified,
    nc.kyc_pending,
    nc.kyc_rejected,
    ROUND(nc.kyc_verified * 100.0 / NULLIF(nc.signups, 0), 1)
                                                          AS kyc_conversion_rate_pct,
    COALESCE(ft.customers_with_txn, 0)                    AS activated_customers,
    ROUND(COALESCE(ft.customers_with_txn, 0) * 100.0 / NULLIF(nc.signups, 0), 1)
                                                          AS activation_rate_pct,
    ft.avg_days_to_first_txn,
    ft.median_days_to_first_txn
FROM new_customers nc
LEFT JOIN first_txn_stats ft ON ft.signup_month = nc.signup_month
ORDER BY nc.signup_month

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Revenue Analysis
-- MAGIC
-- MAGIC Monthly revenue breakdown by account type, separating fee revenue from interest
-- MAGIC revenue, with month-over-month growth rate. Good for a stacked area chart.

-- COMMAND ----------

WITH revenue AS (
    SELECT
        year_month,
        account_type,
        fee_revenue,
        interest_revenue,
        total_revenue
    FROM gold.monthly_revenue
),
with_growth AS (
    SELECT
        year_month,
        account_type,
        fee_revenue,
        interest_revenue,
        total_revenue,
        LAG(total_revenue) OVER (PARTITION BY account_type ORDER BY year_month)
                                                          AS prev_month_revenue
    FROM revenue
)
SELECT
    year_month,
    account_type,
    ROUND(fee_revenue, 2)                                 AS fee_revenue,
    ROUND(interest_revenue, 2)                            AS interest_revenue,
    ROUND(total_revenue, 2)                               AS total_revenue,
    ROUND(
        (total_revenue - prev_month_revenue) * 100.0
        / NULLIF(prev_month_revenue, 0), 1
    )                                                     AS mom_growth_pct
FROM with_growth
ORDER BY year_month, account_type

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Top 10 Merchants by Volume
-- MAGIC
-- MAGIC Highest-volume merchants with transaction count, unique customer reach,
-- MAGIC and average ticket size. Horizontal bar chart or table visualization.

-- COMMAND ----------

SELECT
    merchant_name,
    mcc_code,
    mcc_description,
    ROUND(total_volume, 2)                                AS total_volume,
    transaction_count,
    unique_customers,
    ROUND(avg_ticket_size, 2)                             AS avg_ticket_size,
    ROUND(refund_rate * 100, 2)                           AS refund_rate_pct
FROM gold.merchant_analytics
ORDER BY total_volume DESC
LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. Channel Mix Analysis
-- MAGIC
-- MAGIC How transaction volume distributes across channels (online, in-store, ATM,
-- MAGIC mobile app) over time. Shows the shift toward digital channels. Use a
-- MAGIC 100% stacked area chart for share-of-wallet view.

-- COMMAND ----------

WITH channel_monthly AS (
    SELECT
        date_trunc('month', summary_date)                 AS month,
        channel,
        SUM(transaction_count)                            AS txn_count,
        ROUND(SUM(total_amount), 2)                       AS volume
    FROM gold.daily_transaction_summary
    GROUP BY date_trunc('month', summary_date), channel
),
monthly_totals AS (
    SELECT
        month,
        SUM(txn_count)                                    AS total_count,
        SUM(volume)                                       AS total_volume
    FROM channel_monthly
    GROUP BY month
)
SELECT
    cm.month,
    cm.channel,
    cm.txn_count,
    cm.volume,
    ROUND(cm.txn_count * 100.0 / NULLIF(mt.total_count, 0), 1)
                                                          AS count_share_pct,
    ROUND(cm.volume * 100.0 / NULLIF(mt.total_volume, 0), 1)
                                                          AS volume_share_pct
FROM channel_monthly cm
INNER JOIN monthly_totals mt ON mt.month = cm.month
ORDER BY cm.month, cm.channel

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7. Customer Segmentation Dashboard
-- MAGIC
-- MAGIC RFM segment distribution with count, total value, and averages per segment.
-- MAGIC Use a treemap (area = count, color = avg monetary) or a grouped bar chart.

-- COMMAND ----------

WITH segment_stats AS (
    SELECT
        cs.segment_label,
        COUNT(*)                                          AS customer_count,
        ROUND(AVG(cs.recency_score), 2)                   AS avg_recency,
        ROUND(AVG(cs.frequency_score), 2)                 AS avg_frequency,
        ROUND(AVG(cs.monetary_score), 2)                  AS avg_monetary,
        ROUND(AVG(c360.lifetime_transaction_value), 2)    AS avg_ltv,
        ROUND(SUM(c360.lifetime_transaction_value), 2)    AS total_ltv,
        ROUND(AVG(c360.tenure_days), 0)                   AS avg_tenure_days,
        ROUND(AVG(c360.lifetime_transaction_count), 1)    AS avg_txn_count
    FROM gold.customer_segments cs
    INNER JOIN gold.customer_360 c360 ON c360.customer_id = cs.customer_id
    GROUP BY cs.segment_label
),
grand_total AS (
    SELECT SUM(customer_count) AS total_customers
    FROM segment_stats
)
SELECT
    s.segment_label,
    s.customer_count,
    ROUND(s.customer_count * 100.0 / NULLIF(gt.total_customers, 0), 1)
                                                          AS pct_of_customers,
    s.avg_recency,
    s.avg_frequency,
    s.avg_monetary,
    s.avg_ltv,
    s.total_ltv,
    s.avg_tenure_days,
    s.avg_txn_count
FROM segment_stats s
CROSS JOIN grand_total gt
ORDER BY s.total_ltv DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 8. Fraud Detection Summary
-- MAGIC
-- MAGIC Alerts by type and severity, percentage of transactions flagged, top flagged
-- MAGIC accounts, and a crude false-positive estimate (assuming all "medium" alerts on
-- MAGIC verified/low-risk customers are FPs). Use a heatmap (type × severity) and a bar chart.

-- COMMAND ----------

-- 8a: Alert distribution heatmap
SELECT
    alert_type,
    alert_severity,
    COUNT(*)                                              AS alert_count,
    ROUND(SUM(amount), 2)                                 AS total_flagged_amount,
    COUNT(DISTINCT account_id)                            AS unique_accounts
FROM gold.fraud_alerts
GROUP BY alert_type, alert_severity
ORDER BY
    CASE alert_severity
        WHEN 'critical' THEN 1
        WHEN 'high'     THEN 2
        WHEN 'medium'   THEN 3
        ELSE 4
    END,
    alert_count DESC

-- COMMAND ----------

-- 8b: Overall fraud rate + false positive estimate
WITH totals AS (
    SELECT COUNT(*) AS total_transactions FROM silver.transactions
),
alert_totals AS (
    SELECT
        COUNT(DISTINCT transaction_id) AS flagged_txns,
        SUM(CASE WHEN alert_severity IN ('critical', 'high') THEN 1 ELSE 0 END)
                                                          AS high_severity_count
    FROM gold.fraud_alerts
),
fp_estimate AS (
    -- Crude FP estimate: medium alerts on low-risk, KYC-verified customers
    SELECT COUNT(*) AS estimated_false_positives
    FROM gold.fraud_alerts fa
    INNER JOIN gold.customer_360 c ON c.customer_id = fa.customer_id
    WHERE fa.alert_severity = 'medium'
      AND c.risk_tier = 'low'
      AND c.kyc_status = 'verified'
)
SELECT
    t.total_transactions,
    a.flagged_txns,
    ROUND(a.flagged_txns * 100.0 / NULLIF(t.total_transactions, 0), 2)
                                                          AS fraud_alert_rate_pct,
    a.high_severity_count,
    fp.estimated_false_positives,
    ROUND(fp.estimated_false_positives * 100.0 / NULLIF(a.flagged_txns, 0), 1)
                                                          AS estimated_fp_rate_pct
FROM totals t
CROSS JOIN alert_totals a
CROSS JOIN fp_estimate fp

-- COMMAND ----------

-- 8c: Top 10 most-flagged accounts
SELECT
    fa.account_id,
    fa.customer_id,
    c.full_name,
    c.risk_tier,
    COUNT(*)                                              AS total_alerts,
    SUM(CASE WHEN fa.alert_severity = 'critical' THEN 1 ELSE 0 END) AS critical,
    SUM(CASE WHEN fa.alert_severity = 'high' THEN 1 ELSE 0 END)     AS high,
    ROUND(SUM(fa.amount), 2)                              AS total_flagged_amount,
    COLLECT_SET(fa.alert_type)                            AS triggered_rules
FROM gold.fraud_alerts fa
LEFT JOIN gold.customer_360 c ON c.customer_id = fa.customer_id
GROUP BY fa.account_id, fa.customer_id, c.full_name, c.risk_tier
ORDER BY total_alerts DESC
LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 9. Currency Exposure
-- MAGIC
-- MAGIC Transaction volume by currency, converted to USD using the latest exchange rate.
-- MAGIC Shows the bank's cross-currency exposure. Use a donut chart or horizontal bar.

-- COMMAND ----------

WITH latest_rates AS (
    SELECT
        base_currency,
        target_currency,
        rate,
        ROW_NUMBER() OVER (
            PARTITION BY base_currency, target_currency
            ORDER BY rate_date DESC
        ) AS rn
    FROM silver.exchange_rates
    WHERE target_currency = 'USD'
),
current_rates AS (
    SELECT base_currency, rate
    FROM latest_rates
    WHERE rn = 1

    UNION ALL

    -- USD to USD is always 1
    SELECT 'USD' AS base_currency, 1.0 AS rate
),
currency_volume AS (
    SELECT
        t.currency,
        COUNT(*)                                          AS txn_count,
        ROUND(SUM(t.amount), 2)                           AS native_volume
    FROM silver.transactions t
    WHERE t.status = 'completed'
    GROUP BY t.currency
)
SELECT
    cv.currency,
    cv.txn_count,
    cv.native_volume,
    COALESCE(cr.rate, 1.0)                                AS usd_rate,
    ROUND(cv.native_volume * COALESCE(cr.rate, 1.0), 2)  AS usd_equivalent_volume,
    ROUND(
        cv.native_volume * COALESCE(cr.rate, 1.0) * 100.0
        / SUM(cv.native_volume * COALESCE(cr.rate, 1.0)) OVER (), 1
    )                                                     AS pct_of_total_usd
FROM currency_volume cv
LEFT JOIN current_rates cr ON cr.base_currency = cv.currency
ORDER BY usd_equivalent_volume DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 10. Customer Churn Risk
-- MAGIC
-- MAGIC Identifies customers with declining transaction frequency by comparing the last 30 days
-- MAGIC to the prior 30 days. A drop of > 50% is flagged as "high risk", 25-50% as "medium".
-- MAGIC Pair with the RFM segments to prioritize retention campaigns.

-- COMMAND ----------

WITH period_bounds AS (
    SELECT
        current_date()                                    AS today,
        date_sub(current_date(), 30)                      AS recent_start,
        date_sub(current_date(), 60)                      AS prior_start,
        date_sub(current_date(), 30)                      AS prior_end
),
customer_activity AS (
    SELECT
        a.customer_id,
        SUM(CASE
            WHEN t.transaction_timestamp >= p.recent_start
            THEN 1 ELSE 0
        END)                                              AS txns_last_30d,
        SUM(CASE
            WHEN t.transaction_timestamp >= p.prior_start
             AND t.transaction_timestamp <  p.prior_end
            THEN 1 ELSE 0
        END)                                              AS txns_prior_30d,
        SUM(CASE
            WHEN t.transaction_timestamp >= p.recent_start
            THEN t.amount ELSE 0
        END)                                              AS volume_last_30d,
        SUM(CASE
            WHEN t.transaction_timestamp >= p.prior_start
             AND t.transaction_timestamp <  p.prior_end
            THEN t.amount ELSE 0
        END)                                              AS volume_prior_30d
    FROM silver.accounts a
    INNER JOIN silver.transactions t ON t.account_id = a.account_id
    CROSS JOIN period_bounds p
    WHERE t.status = 'completed'
    GROUP BY a.customer_id
),
with_delta AS (
    SELECT
        ca.*,
        CASE
            WHEN txns_prior_30d = 0 AND txns_last_30d > 0 THEN 'reactivated'
            WHEN txns_prior_30d = 0 AND txns_last_30d = 0 THEN 'dormant'
            ELSE ROUND(
                (txns_last_30d - txns_prior_30d) * 100.0
                / txns_prior_30d, 1
            )
        END                                               AS frequency_change_pct,
        CASE
            WHEN txns_prior_30d > 0
             AND txns_last_30d < txns_prior_30d * 0.5 THEN 'high_risk'
            WHEN txns_prior_30d > 0
             AND txns_last_30d < txns_prior_30d * 0.75 THEN 'medium_risk'
            WHEN txns_prior_30d = 0 AND txns_last_30d = 0 THEN 'dormant'
            WHEN txns_prior_30d = 0 AND txns_last_30d > 0 THEN 'reactivated'
            ELSE 'stable'
        END                                               AS churn_risk
    FROM customer_activity ca
)
SELECT
    wd.customer_id,
    c360.full_name,
    cs.segment_label,
    wd.txns_prior_30d,
    wd.txns_last_30d,
    wd.frequency_change_pct,
    ROUND(wd.volume_prior_30d, 2)                         AS volume_prior_30d,
    ROUND(wd.volume_last_30d, 2)                          AS volume_last_30d,
    wd.churn_risk,
    c360.risk_tier,
    c360.lifetime_transaction_value
FROM with_delta wd
LEFT JOIN gold.customer_360 c360 ON c360.customer_id = wd.customer_id
LEFT JOIN gold.customer_segments cs ON cs.customer_id = wd.customer_id
WHERE wd.churn_risk IN ('high_risk', 'medium_risk')
ORDER BY
    CASE wd.churn_risk WHEN 'high_risk' THEN 1 ELSE 2 END,
    wd.volume_prior_30d DESC
LIMIT 50
