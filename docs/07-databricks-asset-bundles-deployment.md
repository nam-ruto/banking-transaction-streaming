# Phase 7: Infrastructure as Code with Databricks Asset Bundles (DABs)

This guide walks you through deploying your entire real-time streaming platform (Declarative Pipeline + Multi-task Workflow Job) as code using **Databricks Asset Bundles (DABs)**.

---

## 1. Project Organization Best Practices

To cleanly separate transformation logic from workflow orchestration, the repository is structured as follows:

```
banking-transaction-streaming/
├── databricks.yml                      # Root bundle config (targets, variables, envs)
│
├── resources/                          # Declarative resource definitions (YAML)
│   ├── banking_pipeline.pipeline.yml  # Declarative Pipeline resource (Bronze->Silver->Gold)
│   └── banking_workflow.job.yml       # Lakeflow Multi-task Job resource (DAG orchestration)
│
├── src/
│   ├── pipeline/                      # 👈 Declarative Pipeline source code ONLY
│   │   ├── bronze_ingestion.py
│   │   ├── silver_transformation.py
│   │   └── gold_aggregations_and_alerts.py
│   │
│   └── jobs/                          # 👈 Workflow task helper notebooks ONLY
│       ├── preflight_health_check.py
│       └── post_pipeline_validation.py
│
├── producer/
│   └── transaction_producer.py        # EC2 Kafka Producer
│
└── docs/                              # Step-by-step markdown documentation
```

### Why this organization?
- **`src/pipeline/`**: Contains only notebooks with `@dp.table`, `@dp.materialized_view`, and `@dp.append_flow`. The pipeline engine discovers these without parsing non-pipeline scripts.
- **`src/jobs/`**: Contains helper tasks that use `dbutils.widgets` and `dbutils.notebook.exit()`.
- **`resources/`**: All infrastructure configuration is version-controlled in YAML, enabling CI/CD pull requests, code reviews, and automated deployments.

---

## 2. Step 1: Authenticate Databricks CLI

Ensure your Databricks CLI is authenticated with your workspace:

```bash
# In your local terminal:
databricks auth login --host https://<your-databricks-workspace-url>
```
*Follow the browser prompt to log in and authorize the CLI.*

To verify authentication:
```bash
databricks auth profiles
```

---

## 3. Step 2: Validate the Bundle Configuration

Navigate to the project directory:
```bash
cd banking-transaction-streaming
```

Run validation to ensure syntax, file paths, and resource references are valid:
```bash
databricks bundle validate
```

Expected output:
```
Name: banking-transaction-streaming
Target: dev
Workspace:
  Host: https://<workspace-url>
  User: your-email@example.com
  Path: /Users/your-email/.bundle/banking-transaction-streaming/dev

Validation OK!
```

---

## 4. Step 3: Deploy to Development (`dev`)

Deploy the pipeline and workflow to your personal development sandbox in Databricks:

```bash
databricks bundle deploy -t dev
```

### What happens during `bundle deploy`:
1. Uploads the notebooks in `src/` to your workspace under `/Users/your-email/.bundle/banking-transaction-streaming/dev/files/`.
2. Creates or updates the Declarative Pipeline: `[dev] Banking Transaction Streaming Pipeline`.
3. Creates or updates the Lakeflow Workflow: `[dev] Banking Streaming End-to-End Workflow`.
4. In `dev` mode, Databricks automatically isolates resources with `[dev your_username]` prefixes and pauses schedules to prevent unintended automated runs during active development.

---

## 5. Step 4: Run & Monitor the Pipeline/Job via CLI

### Run the Workflow Job:
```bash
databricks bundle run banking_streaming_job -t dev
```

### Run the Pipeline directly:
```bash
databricks bundle run banking_pipeline -t dev
```

### Check execution status in the terminal:
```bash
databricks bundle run banking_streaming_job -t dev --refresh-all
```

---

## 6. Development vs. Production Targets

The `databricks.yml` file is configured with multiple deployment targets:

```yaml
targets:
  dev:
    mode: development
    default: true
    variables:
      catalog: "workspace"
      schema: "banking_streaming"
      kafka_server: "15.135.41.136:9092"

  prod:
    mode: production
    variables:
      catalog: "prod_catalog"
      schema: "banking_streaming"
      kafka_server: "15.135.41.136:9092"
```

To deploy to Production:
```bash
databricks bundle deploy -t prod
```

In `prod` mode:
- Schedules and triggers are **unpaused** and active.
- Resource names are clean (no `[dev]` prefix).
- Deployments are locked to production-managed paths.

---

## 7. Cleaning Up Resources

To remove all deployed bundle resources (Pipelines, Jobs, uploaded code) from the workspace:

```bash
databricks bundle destroy -t dev
```
*(Confirms deletion and leaves your manual workspace clean).*

---

## Summary of All Project Artifacts

| Component | UI Way | Code / DABs Way |
|---|---|---|
| **Kafka Broker** | AWS EC2 Console | `server.properties` (KRaft) |
| **Data Generation** | Manual terminal run | `transaction_producer.py` |
| **Bronze ➔ Silver ➔ Gold** | Databricks Pipeline UI | `resources/banking_pipeline.pipeline.yml` + `src/pipeline/` |
| **Workflow DAG & Scheduling** | Workflows / Jobs UI | `resources/banking_workflow.job.yml` + `src/jobs/` |
| **Deployment & CI/CD** | Manual Import/Export | `databricks bundle deploy` |
