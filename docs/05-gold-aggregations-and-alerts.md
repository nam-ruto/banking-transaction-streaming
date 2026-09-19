# Phase 5: Aggregations & Real-Time Alerts (Gold Layer)

This guide walks you through adding the **Gold Layer** to your Declarative Pipeline in Databricks.

---

## Overview

The Gold layer delivers business-level value from the enriched Silver transactions:

1. **`gold_merchant_analytics` (Materialized View)**:
   - Aggregated KPIs grouped by `merchant_category`, `transaction_date`, `transaction_hour`, and `country`.
   - Metrics: `total_transactions`, `total_volume_usd`, `avg_transaction_amount`, `fraud_transactions_count`, `fraud_volume_usd`, `fraud_rate_pct`.
   - Powering executive dashboards and BI reporting.

2. **`gold_fraud_alerts` (Streaming Table)**:
   - Dedicated table of all flagged fraud events for security operations and historical audit logs.

3. **`fraud_alerts_kafka_sink` (Kafka Sink)**:
   - Streams live fraud alert JSON payloads back to the EC2 Kafka topic `banking.fraud-alerts` via port `9092`.
   - Enables downstream systems (notification engines, card lock microservices) to act on fraud immediately.

```
                  Pipeline: banking-bronze-ingestion
┌────────────────────────────────────────────────────────────────────────┐
│                                                                        │
│  ┌────────────────────┐      ┌────────────────────┐                    │
│  │ 03_bronze_         │      │ 04_silver_         │                    │
│  │ ingestion.py       │      │ transformation.py  │                    │
│  │                    │      │                    │                    │
│  │ @dp.table          │─────▶│ @dp.table          │                    │
│  │ bronze_            │      │ silver_            │                    │
│  │ transactions       │      │ transactions       │                    │
│  └────────────────────┘      └─────────┬──────────┘                    │
│                                        │                               │
│                                        │                               │
│                      ┌─────────────────┴─────────────────┐             │
│                      ▼                                   ▼             │
│            ┌────────────────────┐              ┌───────────────────┐   │
│            │ 05_gold_           │              │ 05_gold_          │   │
│            │ aggregations...py  │              │ aggregations...py │   │
│            │                    │              │                   │   │
│            │ @dp.materialized_  │              │ @dp.table         │   │
│            │ view               │              │ gold_fraud_alerts │   │
│            │ gold_merchant_     │              └─────────┬─────────┘   │
│            │ analytics          │                        │             │
│            └────────────────────┘                        │             │
│                                                          ▼             │
│                                                ┌───────────────────┐   │
│                                                │ dp.create_sink    │   │
│                                                │ fraud_alerts_     │   │
│                                                │ kafka_sink        │   │
│                                                └─────────┬─────────┘   │
│                                                          │             │
└──────────────────────────────────────────────────────────┼─────────────┘
                                                           │ (Port 9092)
                                                           ▼
                                                EC2 Kafka Topic:
                                                banking.fraud-alerts
```

---

## Step 1: Import the Gold Notebook into Databricks

1. In Databricks, click **Workspace** in the left sidebar.
2. Navigate to your user folder (`/Users/your-email@example.com/`).
3. Click **⋮** → **Import**.
4. Upload: `notebooks/05_gold_aggregations_and_alerts.py`.

---

## Step 2: Add to the Existing Pipeline

Add the notebook to your `banking-bronze-ingestion` pipeline:

1. Navigate to **Pipelines** in Databricks.
2. Open `banking-bronze-ingestion`.
3. Click **Settings** (gear icon) or **Edit**.
4. Under **Source code**, click **Add source code** and select `05_gold_aggregations_and_alerts.py`.
5. Click **Save**.

The pipeline now contains all three medallion layers:
- `03_bronze_ingestion.py`
- `04_silver_transformation.py`
- `05_gold_aggregations_and_alerts.py`

---

## Step 3: Run the Pipeline

1. Click **Start** (or **Full refresh all** if applying new schema definitions).
2. Observe the pipeline DAG:
   - `bronze_transactions` (Streaming Table)
   - `silver_transactions` (Streaming Table)
   - `gold_merchant_analytics` (Materialized View)
   - `gold_fraud_alerts` (Streaming Table)
   - `fraud_alerts_kafka_sink` (External Sink Flow)
3. Wait until all nodes finish processing and show a green status ✅.

---

## Step 4: Verify the Gold Tables in SQL

Open a SQL query editor or notebook in Databricks and test the outputs:

### Query 1: Merchant & Category Analytics
```sql
SELECT
  merchant_category,
  transaction_date,
  total_transactions,
  total_volume_usd,
  avg_transaction_amount,
  fraud_transactions_count,
  fraud_volume_usd,
  fraud_rate_pct
FROM workspace.banking_streaming.gold_merchant_analytics
ORDER BY total_volume_usd DESC;
```

### Query 2: Real-Time Fraud Alerts Table
```sql
SELECT
  transaction_id,
  account_id,
  amount,
  currency,
  merchant_name,
  city,
  country,
  fraud_reasons,
  event_timestamp
FROM workspace.banking_streaming.gold_fraud_alerts
ORDER BY event_timestamp DESC
LIMIT 25;
```

---

## Step 5: Verify the Kafka Alerts Topic on EC2

To confirm that fraud alert messages are being streamed back to Kafka:

1. Open your terminal and SSH into the EC2 instance:
   ```bash
   ssh -i key/kafka-key-pair.pem ubuntu@15.135.41.136
   ```
2. Run the console consumer on the `banking.fraud-alerts` topic:
   ```bash
   ~/kafka/bin/kafka-console-consumer.sh \
     --topic banking.fraud-alerts \
     --from-beginning \
     --bootstrap-server localhost:29092
   ```

You should see real-time JSON alert messages produced by Databricks:
```json
{"transaction_id":"tx-26611763-1444","account_id":"acc-609231","card_number_masked":"6011-XXXX-XXXX-4502","timestamp":"2026-08-25 03:29:41.287","amount":37824.98,"currency":"USD","transaction_type":"ONLINE_TRANSFER","merchant_name":"Spotify","merchant_category":"ENTERTAINMENT","city":"Chicago","country":"US","fraud_reasons":"HIGH_AMOUNT"}
```

---

## What's Next?

**Phase 6: Lakeflow Jobs & Automation**
- Orchestrating and scheduling the end-to-end process: Kafka Producer trigger / continuous stream → Declarative Pipeline update → Automated monitoring.
