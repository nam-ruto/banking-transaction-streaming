# Phase 3: Databricks Streaming Ingestion (Bronze Layer)

This guide walks you through setting up a Databricks workspace on AWS, connecting to your EC2 Kafka broker, and ingesting the raw transaction stream into a Bronze Delta table.

---

## Part A: Databricks Workspace Setup

### Step 1: Create a Databricks Account

1. Go to [https://www.databricks.com/try-databricks](https://www.databricks.com/try-databricks).
2. Sign up with your email and select **AWS** as the cloud provider.
3. Choose the **Free Edition** (formerly Community Edition).
   - Includes Unity Catalog, serverless compute, and Kafka streaming support.
   - Usage is quota-limited but sufficient for this learning project.
4. Follow the on-screen setup to create your workspace.

### Step 2: Verify Unity Catalog

Once logged in:

1. Click the **Catalog** icon (📦) in the left sidebar.
2. You should see a default catalog (e.g., `workspace` or your workspace name).
3. If no catalog exists, create one:
   - Click **+ Add** → **Add a catalog** → Name it `banking_project` → **Create**.

### Step 3: Create the Schema (Database)

In the Catalog Explorer:

1. Select your catalog (e.g., `workspace`).
2. Click **+ Add** → **Add a schema** → Name it `banking_streaming`.
3. Click **Create**.

This gives you the namespace: `workspace.banking_streaming` (or `banking_project.banking_streaming` if you created a custom catalog).

### Step 4: Verify Kafka Connectivity

Before writing any streaming code, verify that Databricks can reach your EC2 Kafka broker:

1. Open a **Notebook** in Databricks.
2. Attach it to a running cluster (serverless is fine).
3. Run this cell:

```python
import socket
s = socket.socket()
s.settimeout(10)
try:
    s.connect(("15.135.41.136", 9092))
    print("✅ SUCCESS: Databricks can reach Kafka at 15.135.41.136:9092")
except Exception as e:
    print(f"❌ FAILED: {e}")
finally:
    s.close()
```

If this fails, double-check:
- EC2 Security Group has port `9092` open to `0.0.0.0/0`.
- Elastic IP is still associated with the instance.
- Kafka service is running: `sudo systemctl status kafka` on EC2.

---

## Part B: Bronze Layer Declarative Pipeline

The notebook uses the modern **Lakeflow Declarative Pipeline** API (`from pyspark import pipelines as dp`).
It is designed to be run as a **Pipeline**, not as an interactive notebook.

### Step 5: Import the Notebook

1. In Databricks, click **Workspace** in the left sidebar.
2. Navigate to your user folder (e.g., `/Users/your-email@example.com/`).
3. Click **⋮** → **Import**.
4. Upload the file: `notebooks/03_bronze_ingestion.py`.

### Step 6: Create a Declarative Pipeline

1. In Databricks, click **Pipelines** in the left sidebar (under "Data Engineering").
2. Click **Create pipeline**.
3. Configure the pipeline:

   | Setting | Value |
   |---------|-------|
   | **Pipeline name** | `banking-bronze-ingestion` |
   | **Source code** | Browse to the imported `03_bronze_ingestion.py` notebook |
   | **Target catalog** | `workspace` (or your catalog name) |
   | **Target schema** | `banking_streaming` |
   | **Compute** | **Serverless** (recommended — includes Kafka connector) |

4. Click **Create**.

### Step 7: Start the Pipeline

1. On the pipeline page, click **Start**.
2. The pipeline will:
   - Provision serverless compute (may take 1–2 minutes on first run).
   - Connect to Kafka at `15.135.41.136:9092`.
   - Create the `bronze_transactions` streaming table in Unity Catalog.
   - Ingest all available messages from the `banking.transactions` topic.
3. Watch the DAG visualization — you should see `bronze_transactions` turn green ✅.

### Step 8: Verify Data Quality

The pipeline automatically tracks data quality via **Expectations**:

| Expectation | Rule | Action |
|---|---|---|
| `valid_payload` | `raw_value IS NOT NULL` | Warn (log violation, keep row) |
| `valid_key` | `kafka_key IS NOT NULL` | Warn |
| `valid_timestamp` | `kafka_timestamp IS NOT NULL` | Warn |

View expectation metrics in the pipeline's **Data Quality** tab.

### Step 9: Query the Bronze Table

After the pipeline completes, query the table in a SQL notebook or SQL Editor:

```sql
-- Preview raw data
SELECT * FROM workspace.banking_streaming.bronze_transactions LIMIT 10;

-- Row count
SELECT COUNT(*) AS total_rows FROM workspace.banking_streaming.bronze_transactions;

-- Data quality summary
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT kafka_key) AS unique_accounts,
  MIN(kafka_timestamp) AS earliest_message,
  MAX(kafka_timestamp) AS latest_message,
  SUM(CASE WHEN raw_value IS NULL THEN 1 ELSE 0 END) AS null_payloads
FROM workspace.banking_streaming.bronze_transactions;
```

---

## Key Concepts: Why Declarative Pipelines?

| Feature | Interactive Notebook (`writeStream`) | Declarative Pipeline (`@dp.table`) |
|---|---|---|
| **Checkpointing** | Manual (`checkpointLocation` option) | Automatic (managed by the pipeline engine) |
| **Data quality** | Custom code required | Built-in `@dp.expect` / `@dp.expect_or_drop` |
| **Dependency management** | Manual ordering of cells | Automatic DAG resolution |
| **Compute** | Attached cluster | Serverless or dedicated pipeline cluster |
| **Monitoring** | Manual logging | Pipeline UI with DAG, metrics, lineage |
| **Error recovery** | Manual restart from checkpoint | Auto-retry with exactly-once guarantees |

---

## Architecture Recap

```
EC2 Kafka Producer                    Databricks Declarative Pipeline
(transaction_producer.py)             (03_bronze_ingestion.py)
        │                                     │
        ▼                                     ▼
┌──────────────────┐     port 9092    ┌──────────────────────────┐
│ banking.transactions │─────────────▶│  @dp.table               │
│   (Kafka topic)      │  (EXTERNAL)  │    spark.readStream       │
└──────────────────┘                  │      .format("kafka")     │
                                      └──────────────────────────┘
                                                │
                                                ▼
                                      ┌──────────────────────────┐
                                      │  bronze_transactions      │
                                      │  (Streaming Table)        │
                                      │  Unity Catalog managed    │
                                      │  + Data Quality Expects   │
                                      │  + Liquid Clustering      │
                                      └──────────────────────────┘
```

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Pipeline stuck on `INITIALIZING` | Cold start for serverless compute | Normal — wait 1–2 minutes, don't cancel |
| `Connection timed out to 15.135.41.136:9092` | Security Group blocks Databricks IPs | Open port 9092 to `0.0.0.0/0` in EC2 Security Group |
| No data in bronze table | Producer not running | Start producer on EC2: `python3 ~/transaction_producer.py` |
| `ModuleNotFoundError: pyspark.pipelines` | Running as interactive notebook, not as a Pipeline | This notebook must be run via **Pipelines** (not "Run All") |
| `AnalysisException: schema not found` | Target schema not configured | Set **Target catalog** and **Target schema** in pipeline settings |
| `Kafka connector not available` | Classic cluster missing Kafka library | Use serverless compute (recommended) or install `org.apache.spark:spark-sql-kafka-0-10` on cluster |

---

## What's Next?

**Phase 4: Stream Transformation & Fraud Detection (Silver Layer)**
- Parse the raw JSON payload from Bronze into strongly typed columns.
- Apply fraud detection rules (amount threshold, velocity check, geographic hop).
- Flag transactions with `is_fraud` and `fraud_reason` columns.
- Write enriched data to `silver_transactions` streaming table.
- All within the same Declarative Pipeline!

