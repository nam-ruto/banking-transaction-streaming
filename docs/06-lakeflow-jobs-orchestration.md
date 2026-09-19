# Phase 6: Lakeflow Jobs & Workflow Orchestration

This guide walks you through wrapping your end-to-end streaming solution into a multi-task **Databricks Lakeflow Job (Workflow)** with pre-flight network validation, pipeline execution, post-run data quality audits, and automated scheduling.

---

## 1. Orchestration Architecture

Instead of manually clicking "Start" on your pipeline, a multi-task Lakeflow Job coordinates the workflow as a DAG:

```
                      Lakeflow Multi-Task Job DAG
┌────────────────────────────────────────────────────────────────────────┐
│                                                                        │
│   ┌────────────────────────┐                                           │
│   │ Task 1: Pre-Flight     │                                           │
│   │ Network & Health Check │                                           │
│   │ (06_preflight_check.py)│                                           │
│   └───────────┬────────────┘                                           │
│               │                                                        │
│               ▼ (Run if Task 1 succeeds)                               │
│   ┌────────────────────────┐                                           │
│   │ Task 2: Declarative    │                                           │
│   │ Pipeline Ingestion     │                                           │
│   │ (Pipeline Task:        │                                           │
│   │  banking-bronze-...)   │                                           │
│   └───────────┬────────────┘                                           │
│               │                                                        │
│               ▼ (Run if Task 2 succeeds)                               │
│   ┌────────────────────────┐                                           │
│   │ Task 3: Post-Pipeline  │                                           │
│   │ SLA & KPI Validation   │                                           │
│   │ (06_post_validation.py)│                                           │
│   └────────────────────────┘                                           │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Step-by-Step UI Setup

### Step 1: Import the Helper Notebooks
1. In Databricks, navigate to **Workspace** → Your user folder.
2. Click **⋮** → **Import** and upload:
   - `notebooks/06_preflight_health_check.py`
   - `notebooks/06_post_pipeline_validation.py`

---

### Step 2: Create the Workflow (Job)
1. In the Databricks left sidebar, click **Workflows** (or **Jobs**).
2. Click **Create job**.
3. Name the job: `banking-transaction-e2e-workflow` (at the top left).

---

### Step 3: Configure Task 1 (Pre-Flight Check)
In the task creation panel:
- **Task name**: `preflight_health_check`
- **Type**: `Notebook`
- **Source**: `Workspace`
- **Path**: Browse and select `06_preflight_health_check.py`
- **Compute**: `Serverless` (or select your cluster)
- **Parameters**:
  - `kafka_bootstrap_server`: `15.135.41.136:9092`
  - `catalog`: `workspace`
  - `schema`: `banking_streaming`
- Click **Create task**.

---

### Step 4: Configure Task 2 (Declarative Pipeline)
1. Click the **+ Add task** button on the DAG canvas below Task 1.
2. Configure:
   - **Task name**: `run_declarative_pipeline`
   - **Type**: `Pipeline (Delta Live Tables)`
   - **Pipeline**: Select your existing `banking-bronze-ingestion` pipeline.
   - **Depends on**: `preflight_health_check`
   - **Full refresh**: Leave unchecked (incremental stream processing).
3. Click **Create task**.

---

### Step 5: Configure Task 3 (Post-Pipeline Validation)
1. Click **+ Add task** below Task 2.
2. Configure:
   - **Task name**: `post_pipeline_validation`
   - **Type**: `Notebook`
   - **Source**: `Workspace`
   - **Path**: Browse and select `06_post_pipeline_validation.py`
   - **Depends on**: `run_declarative_pipeline`
   - **Compute**: `Serverless`
   - **Parameters**:
     - `catalog`: `workspace`
     - `schema`: `banking_streaming`
3. Click **Create task**.

---

## 3. Job Scheduling & Notifications

In the right-hand panel of the Job details page:

### A. Set a Schedule
- Click **Add trigger** → **Scheduled**.
- Choose **Periodic** or **Cron**:
  - **Hourly trigger**: `0 0 * * * ?` (runs at the start of every hour).
  - **Every 15 minutes**: `0 */15 * * * ?`
- Set Timezone to your local timezone.
- Click **Save**.

### B. Configure Failure Notifications
- In the right sidebar, find **Job notifications**.
- Click **Add notification**.
- Set **Notify when**: `Failure`
- Enter your email address or Slack webhook destination.

---

## 4. Testing the Workflow

1. Click **Run now** in the top right.
2. Watch the DAG execution:
   - `preflight_health_check` connects to EC2 Kafka and verifies schema (`~15s`) ➔ ✅
   - `run_declarative_pipeline` updates Bronze, Silver, Gold, and emits alerts ➔ ✅
   - `post_pipeline_validation` audits row counts and asserts fraud rate bounds ➔ ✅
3. Click into `post_pipeline_validation` run logs to view the summary:
   ```
   ============================================================
   🏦 Post-Pipeline Validation Summary
   ============================================================
     🥉 Bronze Total Rows:         1,250
     🥈 Silver Total Rows:         1,250
     🥇 Gold Merchant Groups:      48
     🚨 Gold Flagged Fraud Rows:   102
   ------------------------------------------------------------
     📈 Processed Volume:          $521,430.50
     🛡️ Fraud Volume:              $188,320.10
     🎯 Calculated Fraud Rate:     8.16%
   ============================================================
   ✅ ALL POST-PIPELINE VALIDATION CHECKS PASSED
   ============================================================
   ```

---

## 5. Declarative Job Definition (DABs / YAML Reference)

For version-controlled deployment via **Databricks Asset Bundles (DABs)**, the equivalent `resources/job.yml` looks like:

```yaml
resources:
  jobs:
    banking_streaming_job:
      name: "Banking Streaming End-to-End Workflow"
      
      schedule:
        quartz_cron_expression: "0 0 * * * ?"
        timezone_id: "UTC"
        pause_status: UNPAUSED

      tasks:
        - task_key: preflight_health_check
          notebook_task:
            notebook_path: ../notebooks/06_preflight_health_check.py
            base_parameters:
              kafka_bootstrap_server: "15.135.41.136:9092"
              catalog: "workspace"
              schema: "banking_streaming"

        - task_key: run_declarative_pipeline
          depends_on:
            - task_key: preflight_health_check
          pipeline_task:
            pipeline_id: "${resources.pipelines.banking_bronze_ingestion.id}"

        - task_key: post_pipeline_validation
          depends_on:
            - task_key: run_declarative_pipeline
          notebook_task:
            notebook_path: ../notebooks/06_post_pipeline_validation.py
            base_parameters:
              catalog: "workspace"
              schema: "banking_streaming"
```

---

## Summary of Completed Phases

| Phase | Milestone | Deliverables |
|---|---|---|
| **Phase 1** | AWS EC2 & Kafka Setup | `kafka.service`, dual listeners, Elastic IP |
| **Phase 2** | Mock Banking Producer | `transaction_producer.py` (~8% anomalies) |
| **Phase 3** | Bronze Streaming Table | `03_bronze_ingestion.py` (`@dp.table`) |
| **Phase 4** | Silver Transformation & Fraud | `04_silver_transformation.py` (parsed & rules) |
| **Phase 5** | Gold Layer & Kafka Alerts | `05_gold_aggregations_and_alerts.py` (MV + Sink) |
| **Phase 6** | Lakeflow Workflow Orchestration | Multi-task DAG, preflight & post-run audits |
