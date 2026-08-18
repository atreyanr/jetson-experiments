# Streaming Pipeline Setup Guide

Connect Stripe (test mode) → Cloudflare Worker → R2 → Databricks Auto Loader for a realistic, continuously-flowing fintech data pipeline.

**Time to complete:** ~30 minutes

**Prerequisites:** A Cloudflare account (you have this) and a Databricks Free Edition workspace (you have this).

---

## 1. Stripe Setup (Test Mode)

Stripe's test mode creates real API objects (customers, charges, refunds) with fake payment data. It's free forever — no credit card required.

### Create an account

1. Go to [dashboard.stripe.com/register](https://dashboard.stripe.com/register)
2. Sign up with email + password
3. You land in the dashboard with an orange **"Test mode"** badge in the top bar — this is where you stay

### Get your test API key

1. Click **Developers** (left sidebar) → **API keys**
2. Under "Standard keys", reveal and copy the **Secret key**
   - It starts with `sk_test_...`
   - This is the only key the Worker needs

> ⚠️ **Never use live keys** (`sk_live_...`). Test mode is completely free and creates realistic but fake data. The Worker enforces test mode by checking the key prefix.

### Test credit cards reference

The Worker uses these programmatically — you don't need to enter them manually, but useful to know:

| Card Number | Brand | Result |
|-------------|-------|--------|
| `4242 4242 4242 4242` | Visa | Succeeds |
| `5555 5555 5555 4444` | Mastercard | Succeeds |
| `4000 0000 0000 3220` | Visa | Requires 3D Secure |
| `4000 0000 0000 0002` | Visa | Declined (generic) |
| `4000 0000 0000 9995` | Visa | Declined (insufficient funds) |

Full list: [stripe.com/docs/testing#cards](https://docs.stripe.com/testing#cards)

---

## 2. Cloudflare R2 Bucket Setup

R2 is S3-compatible object storage — Databricks connects to it using the same S3A connector it uses for AWS S3.

### Create the bucket

1. Go to [Cloudflare Dashboard](https://dash.cloudflare.com) → **R2 Object Storage** (left sidebar)
2. Click **Create bucket**
3. Name: `fintech-events`
4. Location hint: choose the region closest to your Databricks workspace
5. Click **Create bucket**

### Note your Account ID

1. In the R2 overview page, find your **Account ID** (32-character hex string)
2. Your S3-compatible endpoint is:
   ```
   https://<account_id>.r2.cloudflarestorage.com
   ```
   Example: `https://a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4.r2.cloudflarestorage.com`

### Create an R2 API Token (for Databricks)

1. In R2, click **Manage R2 API Tokens** (top right)
2. Click **Create API token**
3. Configure:
   - **Token name:** `databricks-reader`
   - **Permissions:** Object Read & Write
   - **Specify bucket:** select `fintech-events`
4. Click **Create API Token**
5. **Copy both values immediately** — the Secret Access Key is shown only once:
   - **Access Key ID** (looks like a short alphanumeric string)
   - **Secret Access Key** (longer string)

Save these somewhere safe — you'll enter them in Databricks notebook `01a_configure_r2_connection`.

---

## 3. Deploy the Cloudflare Worker

The Worker is a TypeScript application that runs on Cloudflare's edge network. It creates Stripe test data on a schedule and writes events to R2.

### Install dependencies

```bash
cd experiments/databricks-fintech/worker
npm install
```

### Authenticate with Cloudflare

```bash
npx wrangler login
```

This opens a browser window for OAuth. After authorizing, you're authenticated.

### Set the Stripe secret key

```bash
npx wrangler secret put STRIPE_SECRET_KEY
```

When prompted, paste your `sk_test_...` key from Step 1. It's stored encrypted and never visible in logs or source.

### Deploy

```bash
npx wrangler deploy
```

Output shows the Worker URL:
```
Published fintech-data-source (X.XX sec)
  https://fintech-data-source.<your-subdomain>.workers.dev
  Current Version ID: ...
```

### Verify it works

Trigger a manual run:

```bash
curl -s https://fintech-data-source.<your-subdomain>.workers.dev | jq .
```

Expected response:

```json
{
  "status": "ok",
  "customers_created": 3,
  "transactions_created": 22,
  "refunds_created": 2,
  "activity_events": 41,
  "duration_ms": 1847
}
```

Check that files landed in R2:

```bash
npx wrangler r2 object list fintech-events --prefix customers/
```

### What the Worker does each run

Every 5 minutes (cron trigger), the Worker:

| Action | Volume per run | Source |
|--------|---------------|--------|
| Create customers | 2–5 | Stripe test API |
| Create charges | 15–30 | Stripe test API (test cards) |
| Create refunds | 1–3 | Stripe test API |
| Generate activity events | 30–50 | Synthetic (login, browse, search, checkout) |

Events are written as JSONL files to R2:

```
fintech-events/
├── customers/year=2026/month=08/day=18/batch_1723968000000.jsonl
├── transactions/year=2026/month=08/day=18/batch_1723968000000.jsonl
├── refunds/year=2026/month=08/day=18/batch_1723968000000.jsonl
└── activity/year=2026/month=08/day=18/batch_1723968000000.jsonl
```

The Hive-style partitioning (`year=.../month=.../day=...`) enables Databricks Auto Loader partition discovery.

---

## 4. Connect Databricks to R2

### Quick setup (widgets — good for learning)

1. In Databricks, open notebook **`01a_configure_r2_connection`**
2. Fill in the widgets at the top:

   | Widget | Value |
   |--------|-------|
   | R2 Access Key ID | From Step 2 |
   | R2 Secret Access Key | From Step 2 |
   | R2 S3 Endpoint URL | `https://<account_id>.r2.cloudflarestorage.com` |
   | R2 Bucket Name | `fintech-events` |

3. Run all cells
4. The test cell should print:
   ```
   ✅ Connected to R2 bucket: fintech-events
      Found 4 top-level paths:
      customers/ (0 bytes)
      transactions/ (0 bytes)
      refunds/ (0 bytes)
      activity/ (0 bytes)
   ```

### Production setup (Databricks secrets — recommended)

Using the Databricks CLI, store credentials in an encrypted secret scope:

```bash
# Install the CLI if needed
pip install databricks-cli

# Configure CLI with your workspace URL + personal access token
databricks configure --token

# Create a secret scope
databricks secrets create-scope fintech-r2

# Store each credential
databricks secrets put-secret fintech-r2 access-key \
  --string-value "<your-r2-access-key>"

databricks secrets put-secret fintech-r2 secret-key \
  --string-value "<your-r2-secret-access-key>"

databricks secrets put-secret fintech-r2 endpoint \
  --string-value "https://<account_id>.r2.cloudflarestorage.com"
```

Then in notebook `01a`, switch to the secrets-based configuration (code is in the notebook, commented out by default).

---

## 5. Run the Streaming Pipeline

### Let data accumulate

After deploying the Worker, let it run for **15–30 minutes** so there's meaningful data in R2 (3–6 cron runs = ~100 customers, ~500 transactions, ~200 activity events).

You can watch data arrive:

```bash
# Count files in R2
npx wrangler r2 object list fintech-events --prefix transactions/ | wc -l
```

### Run Auto Loader ingestion

1. Open notebook **`01b_streaming_ingestion`** in Databricks
2. Run all cells — Auto Loader reads new JSONL files from R2 and writes to bronze Delta tables:
   - `fintech_lab.bronze.raw_customers_stream`
   - `fintech_lab.bronze.raw_transactions_stream`
   - `fintech_lab.bronze.raw_refunds_stream`
   - `fintech_lab.bronze.raw_activity_stream`
3. The summary cell prints row counts for each table

### Continue with the DLT pipeline

The DLT pipeline (notebooks 03 + 04) transforms bronze → silver → gold. To use the streaming tables:

1. In notebook `03_dlt_bronze_to_silver.py`, update the source table names:
   ```python
   # Change from:
   spark.read.table("fintech_lab.bronze.raw_customers")
   # To:
   spark.read.table("fintech_lab.bronze.raw_customers_stream")
   ```
2. Do the same for `raw_accounts` → `raw_transactions_stream`, etc.
3. Re-run the DLT pipeline

> **Note:** The streaming bronze tables have Stripe's native schema (e.g., `id`, `object`, `amount`, `currency` for charges) rather than the synthetic schema. The DLT silver layer may need schema adjustments to map Stripe fields to the silver model. This is a great exercise in handling real-world schema differences.

---

## 6. Architecture

```
┌─ Stripe Test Mode ──────────────────────────────────────────┐
│  Free, unlimited test API calls                              │
│  Real Customer, Charge, PaymentIntent, Refund objects        │
│  Test cards: 4242... (Visa), 5555... (Mastercard)            │
└──────────────────────────────┬───────────────────────────────┘
                               │ API calls
                               ▼
┌─ Cloudflare ─────────────────────────────────────────────────┐
│                                                               │
│  Worker: fintech-data-source (cron: */5 * * * *)             │
│  ├── Creates 2-5 Stripe customers per run                    │
│  ├── Creates 15-30 charges with test cards                   │
│  ├── Creates 1-3 refunds for recent charges                  │
│  ├── Generates 30-50 synthetic activity events               │
│  └── Writes JSONL files → R2                                 │
│                                                               │
│  R2 Bucket: fintech-events (S3-compatible)                   │
│  ├── customers/year=YYYY/month=MM/day=DD/batch_*.jsonl       │
│  ├── transactions/year=YYYY/month=MM/day=DD/batch_*.jsonl    │
│  ├── refunds/year=YYYY/month=MM/day=DD/batch_*.jsonl         │
│  └── activity/year=YYYY/month=MM/day=DD/batch_*.jsonl        │
│                                                               │
└──────────────────────────────┬───────────────────────────────┘
                               │ S3A connector (path-style)
                               ▼
┌─ Databricks Free Edition ────────────────────────────────────┐
│                                                               │
│  Auto Loader (cloudFiles format)                             │
│  ├── Incrementally discovers new JSONL files                 │
│  ├── Schema inference + evolution                            │
│  ├── Exactly-once processing via checkpoints                 │
│  └── trigger(availableNow=True) for batch-scheduled          │
│      trigger(processingTime="30s") for continuous             │
│                     │                                         │
│                     ▼                                         │
│  Bronze Layer (append-only Delta tables)                     │
│  ├── raw_customers_stream                                    │
│  ├── raw_transactions_stream                                 │
│  ├── raw_refunds_stream                                      │
│  └── raw_activity_stream                                     │
│                     │                                         │
│                     ▼                                         │
│  DLT Pipeline (notebooks 03 + 04)                            │
│  ├── Silver: dedup, type-cast, validate, enrich              │
│  └── Gold: customer_360, fraud_scores, segments, revenue     │
│                     │                                         │
│                     ▼                                         │
│  Fraud Detection (notebook 05) + Analytics (notebook 06)     │
│                                                               │
└───────────────────────────────────────────────────────────────┘
```

---

## 7. Monitoring & Troubleshooting

### Worker issues

| Symptom | Check | Fix |
|---------|-------|-----|
| Worker not running | Dashboard → Workers → fintech-data-source → Triggers | Verify cron trigger `*/5 * * * *` is listed |
| Stripe errors in logs | Dashboard → Workers → Logs (real-time) | Verify `STRIPE_SECRET_KEY` is set: `npx wrangler secret list` |
| No files in R2 | Trigger manually: `curl https://fintech-data-source.<subdomain>.workers.dev` | Check Worker logs for R2 write errors |

### Databricks connection issues

| Symptom | Check | Fix |
|---------|-------|-----|
| Can't list R2 files | Endpoint URL format | Must be `https://<32-char-id>.r2.cloudflarestorage.com` — no trailing slash, no bucket name in URL |
| 403 Forbidden | R2 API token permissions | Token must have **Object Read & Write** on `fintech-events` bucket |
| S3A errors | Spark config | Verify `fs.s3a.path.style.access` = `true` (R2 requires path-style, not virtual-hosted) |

### Auto Loader issues

| Symptom | Check | Fix |
|---------|-------|-----|
| No new data ingested | Checkpoint directory | Clear and retry: `dbutils.fs.rm(checkpoint_path, True)` |
| Schema mismatch | New fields from Stripe | `schemaEvolutionMode = "addNewColumns"` handles this automatically |
| Duplicate records | Checkpoint corruption | Clear checkpoint, re-ingest — dedup in silver layer catches duplicates |

---

## 8. Cost Implications

Everything in this pipeline fits within free tiers:

| Service | Free Tier | This Pipeline's Usage | Cost |
|---------|-----------|----------------------|------|
| **Stripe test mode** | Unlimited API calls | ~288 calls/day (every 5 min) | **$0** |
| **Cloudflare Workers** | 100,000 requests/day | ~288 cron triggers/day | **$0** |
| **Cloudflare R2 storage** | 10 GB/month | ~50 MB/month (JSONL text) | **$0** |
| **Cloudflare R2 operations** | 1M writes, 10M reads/month | ~8,640 writes + reads/month | **$0** |
| **Databricks Free Edition** | Limited serverless DBUs | Depends on run frequency | **$0** |

> **Tip:** If you stop using the pipeline, disable the Worker's cron trigger in `wrangler.toml` (comment out `[triggers]`) and redeploy to stop generating data.

---

## 9. Switching Between Batch and Streaming

Both ingestion paths are available — use whichever fits your workflow:

| Mode | Notebooks | Data Source | Best For |
|------|-----------|-------------|----------|
| **Batch** (original) | 01 → 02 | Local Python generation → Volume | Offline development, no external dependencies |
| **Streaming** (new) | 01a → 01b | Cloudflare Worker → R2 → Auto Loader | Realistic pipeline, learning streaming patterns |

The DLT pipeline (03 + 04) and downstream notebooks (05, 06, 07) work identically with either path — the only difference is the bronze table names:

| Batch Table | Streaming Table |
|-------------|-----------------|
| `bronze.raw_customers` | `bronze.raw_customers_stream` |
| `bronze.raw_accounts` | *(Stripe creates customers, not separate accounts)* |
| `bronze.raw_transactions` | `bronze.raw_transactions_stream` |
| `bronze.raw_merchants` | *(derived from Stripe charge metadata)* |
| `bronze.raw_exchange_rates` | `bronze.raw_refunds_stream` + `bronze.raw_activity_stream` |

> **Schema mapping exercise:** The Stripe-sourced data has a different schema than the synthetic data. Adapting the silver layer DLT to handle both schemas (or mapping Stripe fields to the existing model) is an excellent real-world data engineering exercise.
