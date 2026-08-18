# Databricks notebook source
# ruff: noqa: E402, F821

# MAGIC %md
# MAGIC # 🔧 Workflow Orchestration — Fintech Pipeline
# MAGIC
# MAGIC This notebook programmatically creates a **Databricks Workflow (Job)** that orchestrates
# MAGIC the entire fintech data pipeline end-to-end. It uses the Databricks Python SDK to define
# MAGIC a multi-task DAG with dependencies, scheduling, retry policies, and notifications.
# MAGIC
# MAGIC ## Pipeline DAG
# MAGIC
# MAGIC ```
# MAGIC ┌───────────────────┐
# MAGIC │  01_generate_data  │   (one-time or scheduled synthetic data refresh)
# MAGIC └────────┬──────────┘
# MAGIC          │
# MAGIC          ▼
# MAGIC ┌────────────────────┐
# MAGIC │ 02_bronze_ingestion │   (load raw files → bronze Delta tables)
# MAGIC └────────┬───────────┘
# MAGIC          │
# MAGIC          ▼
# MAGIC ┌────────────────────────────┐
# MAGIC │ DLT Pipeline (03 + 04)     │   (bronze → silver → gold via Delta Live Tables)
# MAGIC └────────┬──────────┬────────┘
# MAGIC          │          │
# MAGIC          ▼          ▼
# MAGIC ┌─────────────┐  ┌───────────────────┐
# MAGIC │ 05_fraud     │  │ 06_analytics      │   (parallel: fraud rules + SQL refresh)
# MAGIC │ _detection   │  │ _queries          │
# MAGIC └─────────────┘  └───────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC 1. All notebooks (00–06) are imported into your Databricks workspace
# MAGIC 2. The `00_setup_catalog` notebook has been run at least once
# MAGIC 3. A DLT pipeline has been created pointing to notebooks 03 + 04
# MAGIC 4. You have workspace-level permissions to create jobs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Configuration
# MAGIC
# MAGIC Adjust these paths to match where you imported the notebooks in your workspace.
# MAGIC The notebook paths should be **workspace paths** (starting with `/Workspace/` or
# MAGIC `/Users/your-email/...`).

# COMMAND ----------

# -- Adjust these to your workspace layout -----------------------------------

# Your Databricks username (email)
DATABRICKS_USER = spark.sql("SELECT current_user()").first()[0]

# Base path where notebooks are imported
NOTEBOOK_BASE = f"/Workspace/Users/{DATABRICKS_USER}/databricks-fintech/notebooks"

# The DLT pipeline ID (created separately — see Step 2b below)
# Set this after creating the DLT pipeline, or leave empty to skip that task
DLT_PIPELINE_ID = ""  # e.g., "a1b2c3d4-e5f6-7890-abcd-ef1234567890"

# Notification email for failures
NOTIFICATION_EMAIL = DATABRICKS_USER

# Job name
JOB_NAME = "fintech-pipeline-daily"

print(f"User:          {DATABRICKS_USER}")
print(f"Notebook base: {NOTEBOOK_BASE}")
print(f"DLT pipeline:  {DLT_PIPELINE_ID or '(not set — see Step 2b)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2a — Create the DLT Pipeline (Manual)
# MAGIC
# MAGIC Delta Live Tables pipelines are created separately from Jobs. You need to create
# MAGIC one first, then reference its ID in the workflow.
# MAGIC
# MAGIC ### How to create the DLT pipeline in the UI:
# MAGIC
# MAGIC 1. Go to **Delta Live Tables** in the left sidebar
# MAGIC 2. Click **Create pipeline**
# MAGIC 3. Configure:
# MAGIC    - **Pipeline name**: `fintech-bronze-to-gold`
# MAGIC    - **Product edition**: `Advanced` (for expectations/quality monitoring)
# MAGIC    - **Pipeline mode**: `Triggered` (we'll trigger it from the workflow)
# MAGIC    - **Source code**: Add both notebooks:
# MAGIC      - `03_dlt_bronze_to_silver`
# MAGIC      - `04_dlt_silver_to_gold`
# MAGIC    - **Destination**:
# MAGIC      - **Catalog**: `fintech_lab`
# MAGIC      - **Target schema**: Leave empty (tables go to silver/gold per the DLT definitions)
# MAGIC    - **Compute**: Serverless
# MAGIC    - **Advanced → Configuration**:
# MAGIC      ```
# MAGIC      spark.databricks.delta.preview.enabled = true
# MAGIC      ```
# MAGIC 4. Click **Create**
# MAGIC 5. Copy the **Pipeline ID** from the URL or pipeline details
# MAGIC 6. Paste it into `DLT_PIPELINE_ID` above
# MAGIC
# MAGIC ### Alternatively, create via SDK:

# COMMAND ----------

# Optional: Create DLT pipeline programmatically
# Uncomment and run this cell if you prefer SDK over UI

# from databricks.sdk import WorkspaceClient
# from databricks.sdk.service.pipelines import (
#     PipelineSpec, PipelineLibrary, NotebookLibrary
# )
#
# w = WorkspaceClient()
#
# dlt_pipeline = w.pipelines.create(
#     name="fintech-bronze-to-gold",
#     continuous=False,  # Triggered mode
#     development=True,  # Start in dev mode for testing
#     channel="CURRENT",
#     libraries=[
#         PipelineLibrary(
#             notebook=NotebookLibrary(
#                 path=f"{NOTEBOOK_BASE}/03_dlt_bronze_to_silver"
#             )
#         ),
#         PipelineLibrary(
#             notebook=NotebookLibrary(
#                 path=f"{NOTEBOOK_BASE}/04_dlt_silver_to_gold"
#             )
#         ),
#     ],
#     catalog="fintech_lab",
#     configuration={
#         "spark.databricks.delta.preview.enabled": "true"
#     },
#     serverless=True,
# )
#
# DLT_PIPELINE_ID = dlt_pipeline.pipeline_id
# print(f"Created DLT pipeline: {DLT_PIPELINE_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2b — Create the Workflow (Job)
# MAGIC
# MAGIC This cell creates a Databricks Workflow with 5 tasks in a DAG, using the
# MAGIC Databricks Python SDK. The SDK is pre-installed on Databricks clusters.
# MAGIC
# MAGIC ### Task dependency graph:
# MAGIC ```
# MAGIC generate_data → bronze_ingestion → dlt_pipeline → fraud_detection
# MAGIC                                                 → analytics_refresh
# MAGIC ```

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import (
    Task,
    NotebookTask,
    PipelineTask,
    TaskDependency,
    CronSchedule,
    JobEmailNotifications,
    PauseStatus,
)

w = WorkspaceClient()

# Build the task list --------------------------------------------------------

tasks = [
    # Task 1: Generate synthetic data
    Task(
        task_key="generate_data",
        description="Generate synthetic fintech data and write to landing zone volume",
        notebook_task=NotebookTask(
            notebook_path=f"{NOTEBOOK_BASE}/01_generate_data",
        ),
        # Serverless compute (Databricks Free Edition default)
        # No cluster config needed — serverless is the default on Free Edition
        max_retries=1,
        min_retry_interval_millis=300_000,  # 5 minutes between retries
        timeout_seconds=1800,  # 30 minute timeout
    ),

    # Task 2: Bronze ingestion (depends on data generation)
    Task(
        task_key="bronze_ingestion",
        description="Ingest raw files from volume into bronze Delta tables",
        depends_on=[TaskDependency(task_key="generate_data")],
        notebook_task=NotebookTask(
            notebook_path=f"{NOTEBOOK_BASE}/02_bronze_ingestion",
        ),
        max_retries=2,
        min_retry_interval_millis=300_000,
        timeout_seconds=1800,
    ),

    # Task 3: DLT pipeline (depends on bronze ingestion)
    # This triggers the separately-created DLT pipeline
    Task(
        task_key="dlt_pipeline",
        description="Run DLT pipeline: bronze → silver → gold transformations",
        depends_on=[TaskDependency(task_key="bronze_ingestion")],
        pipeline_task=PipelineTask(
            pipeline_id=DLT_PIPELINE_ID or "PLACEHOLDER_SET_DLT_PIPELINE_ID",
            full_refresh=False,  # Incremental by default
        ) if DLT_PIPELINE_ID else None,
        # Fallback to notebook task if DLT pipeline ID not set
        notebook_task=NotebookTask(
            notebook_path=f"{NOTEBOOK_BASE}/03_dlt_bronze_to_silver",
        ) if not DLT_PIPELINE_ID else None,
        max_retries=2,
        min_retry_interval_millis=300_000,
        timeout_seconds=3600,
    ),

    # Task 4: Fraud detection (depends on DLT pipeline — runs in parallel with analytics)
    Task(
        task_key="fraud_detection",
        description="Run advanced fraud detection rules engine",
        depends_on=[TaskDependency(task_key="dlt_pipeline")],
        notebook_task=NotebookTask(
            notebook_path=f"{NOTEBOOK_BASE}/05_fraud_detection",
        ),
        max_retries=2,
        min_retry_interval_millis=300_000,
        timeout_seconds=1800,
    ),

    # Task 5: Analytics refresh (depends on DLT pipeline — runs in parallel with fraud)
    Task(
        task_key="analytics_refresh",
        description="Refresh analytics queries and dashboard cache",
        depends_on=[TaskDependency(task_key="dlt_pipeline")],
        notebook_task=NotebookTask(
            notebook_path=f"{NOTEBOOK_BASE}/06_analytics_queries",
        ),
        max_retries=1,
        min_retry_interval_millis=300_000,
        timeout_seconds=1200,
    ),
]

print(f"Defined {len(tasks)} tasks")
for t in tasks:
    deps = [d.task_key for d in (t.depends_on or [])]
    print(f"  {t.task_key:25s} → depends on: {deps or '(none)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create (or update) the Job

# COMMAND ----------

# Check if job already exists
existing_jobs = [j for j in w.jobs.list(name=JOB_NAME)]

if existing_jobs:
    job_id = existing_jobs[0].job_id
    print(f"Job '{JOB_NAME}' already exists (ID: {job_id}). Updating...")
    w.jobs.reset(
        job_id=job_id,
        new_settings={
            "name": JOB_NAME,
            "tasks": tasks,
            "schedule": CronSchedule(
                quartz_cron_expression="0 0 2 * * ?",  # Daily at 2 AM UTC
                timezone_id="UTC",
                pause_status=PauseStatus.PAUSED,  # Start paused — enable when ready
            ),
            "email_notifications": JobEmailNotifications(
                on_failure=[NOTIFICATION_EMAIL],
            ),
            "tags": {
                "environment": "lab",
                "project": "fintech-pipeline",
                "owner": DATABRICKS_USER,
            },
            "max_concurrent_runs": 1,
            "timeout_seconds": 7200,  # 2 hour overall timeout
        },
    )
    print(f"✅ Job updated: {JOB_NAME} (ID: {job_id})")
else:
    created_job = w.jobs.create(
        name=JOB_NAME,
        tasks=tasks,
        schedule=CronSchedule(
            quartz_cron_expression="0 0 2 * * ?",  # Daily at 2 AM UTC
            timezone_id="UTC",
            pause_status=PauseStatus.PAUSED,  # Start paused — enable when ready
        ),
        email_notifications=JobEmailNotifications(
            on_failure=[NOTIFICATION_EMAIL],
        ),
        tags={
            "environment": "lab",
            "project": "fintech-pipeline",
            "owner": DATABRICKS_USER,
        },
        max_concurrent_runs=1,
        timeout_seconds=7200,
    )
    job_id = created_job.job_id
    print(f"✅ Job created: {JOB_NAME} (ID: {job_id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Manual Trigger
# MAGIC
# MAGIC ### Option A: Trigger from this notebook

# COMMAND ----------

# Trigger a one-time run now
run = w.jobs.run_now(job_id=job_id)
print(f"🚀 Triggered run: {run.run_id}")
print(f"   Monitor at: {w.config.host}#job/{job_id}/run/{run.run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option B: Trigger from the Databricks UI
# MAGIC
# MAGIC 1. Go to **Workflows** in the left sidebar
# MAGIC 2. Find `fintech-pipeline-daily`
# MAGIC 3. Click **Run now** (top right)
# MAGIC 4. Watch the DAG visualization — tasks light up green as they complete
# MAGIC
# MAGIC ### Option C: Trigger via REST API (from outside Databricks)
# MAGIC
# MAGIC ```bash
# MAGIC # Get your Databricks host and token
# MAGIC export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
# MAGIC export DATABRICKS_TOKEN="dapi..."
# MAGIC
# MAGIC curl -X POST "${DATABRICKS_HOST}/api/2.1/jobs/run-now" \
# MAGIC   -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
# MAGIC   -H "Content-Type: application/json" \
# MAGIC   -d '{"job_id": JOB_ID}'
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Monitoring & Alerting
# MAGIC
# MAGIC ### 4.1 Job Run History
# MAGIC
# MAGIC View recent runs programmatically:

# COMMAND ----------

# Show last 5 runs
runs = list(w.jobs.list_runs(job_id=job_id, limit=5))

if runs:
    print(f"Last {len(runs)} runs of '{JOB_NAME}':")
    print(f"{'Run ID':>12}  {'State':>12}  {'Result':>12}  {'Duration':>10}  Started")
    print("-" * 75)
    for r in runs:
        duration = ""
        if r.end_time and r.start_time:
            dur_sec = (r.end_time - r.start_time) / 1000
            duration = f"{dur_sec:.0f}s"
        state = str(r.state.life_cycle_state) if r.state else "unknown"
        result = str(r.state.result_state) if r.state and r.state.result_state else "-"
        started = r.start_time if r.start_time else "-"
        print(f"{r.run_id:>12}  {state:>12}  {result:>12}  {duration:>10}  {started}")
else:
    print("No runs yet.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 DLT Data Quality Monitoring
# MAGIC
# MAGIC Delta Live Tables tracks data quality expectations automatically. To monitor them:
# MAGIC
# MAGIC 1. **DLT Pipeline UI** → Click your pipeline → **Data Quality** tab
# MAGIC    - Shows pass/fail rates for every `@dlt.expect*` decorator
# MAGIC    - Red alerts when `expect_or_fail` constraints are violated
# MAGIC
# MAGIC 2. **System Tables** (Unity Catalog):
# MAGIC    ```sql
# MAGIC    -- Query DLT event log for quality metrics
# MAGIC    SELECT
# MAGIC        timestamp,
# MAGIC        details:flow_progress:data_quality:expectations
# MAGIC    FROM event_log(TABLE(fintech_lab.silver.customers))
# MAGIC    WHERE event_type = 'flow_progress'
# MAGIC    ORDER BY timestamp DESC
# MAGIC    LIMIT 20
# MAGIC    ```
# MAGIC
# MAGIC 3. **Set up SQL Alerts** on quality degradation:
# MAGIC    - Go to **SQL** → **Alerts** → **Create Alert**
# MAGIC    - Query: count of dropped rows from DLT expectations
# MAGIC    - Trigger: when dropped rows > threshold (e.g., 100)
# MAGIC    - Notification: email or Slack webhook
# MAGIC
# MAGIC ### 4.3 Slack Notifications
# MAGIC
# MAGIC To add Slack notifications to the job:
# MAGIC
# MAGIC 1. Create a Slack webhook URL in your Slack workspace
# MAGIC 2. In Databricks, go to **Settings** → **Notification destinations**
# MAGIC 3. Add a Slack destination with your webhook URL
# MAGIC 4. Edit the job → **Notifications** → Add the Slack destination for `on_failure`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Cost Optimization Tips
# MAGIC
# MAGIC ### 💰 Databricks Free Edition Specifics
# MAGIC
# MAGIC | Resource | Free Edition Limit | Tip |
# MAGIC |----------|-------------------|-----|
# MAGIC | Serverless compute | ~15 DBU/day | Schedule heavy jobs during off-peak hours |
# MAGIC | Storage | 10 GB Unity Catalog | Use `OPTIMIZE` + `VACUUM` to compact Delta files |
# MAGIC | DLT | Included | Use `Triggered` mode (not `Continuous`) to avoid idle compute |
# MAGIC | SQL Warehouse | Serverless included | Set auto-stop to 5 minutes |
# MAGIC | Workflows | 1 concurrent run | Chain jobs sequentially, avoid overlapping schedules |
# MAGIC
# MAGIC ### Best Practices
# MAGIC
# MAGIC 1. **Serverless compute**: Always use serverless (default on Free Edition) — zero cost
# MAGIC    when idle, no cluster management overhead.
# MAGIC
# MAGIC 2. **DLT in Triggered mode**: Only runs when triggered by the workflow. Continuous mode
# MAGIC    runs constantly and burns compute even when there's no new data.
# MAGIC
# MAGIC 3. **Delta table maintenance**: Run `OPTIMIZE` on frequently-read tables to compact
# MAGIC    small files and improve query performance:
# MAGIC    ```sql
# MAGIC    OPTIMIZE fintech_lab.silver.transactions;
# MAGIC    OPTIMIZE fintech_lab.gold.customer_360;
# MAGIC    ```
# MAGIC
# MAGIC 4. **VACUUM old files**: Remove deleted files older than the retention period:
# MAGIC    ```sql
# MAGIC    VACUUM fintech_lab.silver.transactions RETAIN 168 HOURS;
# MAGIC    ```
# MAGIC
# MAGIC 5. **Partition hot tables**: For large transaction tables, partition by month:
# MAGIC    ```sql
# MAGIC    -- In DLT, add this to the table definition:
# MAGIC    -- @dlt.table(partition_cols=["txn_month"])
# MAGIC    ```
# MAGIC
# MAGIC 6. **Cache wisely**: Use `spark.catalog.cacheTable()` only for tables read multiple
# MAGIC    times in the same job. Uncache after use to free memory.
# MAGIC
# MAGIC 7. **Right-size data generation**: Start with 50K transactions for development,
# MAGIC    scale to 250K for realistic testing. The fraud detection notebook benefits
# MAGIC    from larger datasets for pattern detection.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Teardown (Optional)
# MAGIC
# MAGIC Clean up everything when you're done experimenting. **Uncomment and run only
# MAGIC if you want to delete all resources.**

# COMMAND ----------

# # ⚠️ DANGER ZONE — Uncomment to delete everything
#
# # Delete the workflow job
# w.jobs.delete(job_id=job_id)
# print(f"Deleted job: {job_id}")
#
# # Delete the DLT pipeline
# if DLT_PIPELINE_ID:
#     w.pipelines.delete(pipeline_id=DLT_PIPELINE_ID)
#     print(f"Deleted DLT pipeline: {DLT_PIPELINE_ID}")
#
# # Drop all tables and the catalog
# spark.sql("DROP SCHEMA IF EXISTS fintech_lab.gold CASCADE")
# spark.sql("DROP SCHEMA IF EXISTS fintech_lab.silver CASCADE")
# spark.sql("DROP SCHEMA IF EXISTS fintech_lab.bronze CASCADE")
# spark.sql("DROP CATALOG IF EXISTS fintech_lab CASCADE")
# print("Dropped catalog: fintech_lab")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Summary
# MAGIC
# MAGIC You now have a fully orchestrated fintech data pipeline:
# MAGIC
# MAGIC | Component | Status |
# MAGIC |-----------|--------|
# MAGIC | Unity Catalog (`fintech_lab`) | Created by notebook 00 |
# MAGIC | Synthetic data generator | Notebook 01 |
# MAGIC | Bronze ingestion | Notebook 02 |
# MAGIC | DLT pipeline (silver + gold) | Notebooks 03 + 04 |
# MAGIC | Fraud detection engine | Notebook 05 |
# MAGIC | Analytics queries | Notebook 06 |
# MAGIC | Workflow orchestration | This notebook (07) |
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC 1. **Run the full pipeline** once manually to populate all tables
# MAGIC 2. **Explore the DLT quality dashboard** to see expectation metrics
# MAGIC 3. **Build SQL dashboards** using the queries from notebook 06
# MAGIC 4. **Enable the schedule** when you're happy with the pipeline
# MAGIC 5. **Experiment**: Try adding new fraud rules, new gold aggregates,
# MAGIC    or connecting a BI tool (e.g., Databricks built-in dashboards)
