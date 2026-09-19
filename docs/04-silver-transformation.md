# Phase 4: Stream Transformation & Fraud Detection (Silver Layer)

This guide walks you through adding the Silver layer to your existing Declarative Pipeline. It parses raw Bronze JSON into typed columns and applies rule-based fraud detection.

---

## Overview

The Silver layer transforms raw data from Bronze into a clean, analytics-ready format:

```
bronze_transactions              silver_transactions
┌─────────────────────┐         ┌─────────────────────────────┐
│ kafka_key (string)  │         │ transaction_id (string)     │
│ raw_value (string)  │───────▶ │ account_id (string)         │
│ kafka_partition     │  parse  │ amount (double)             │
│ kafka_offset        │  +fraud │ currency (string)           │
│ kafka_timestamp     │  detect │ transaction_type (string)   │
│ ingestion_timestamp │         │ merchant_name (string)      │
│ ingestion_date      │         │ merchant_category (string)  │
└─────────────────────┘         │ city, country, lat, lon     │
                                │ is_fraud (boolean)          │
                                │ fraud_reasons (string)      │
                                │ transaction_date (date)     │
                                └─────────────────────────────┘
```

### Fraud Detection Rules

| Rule | Condition | Risk Level |
|------|-----------|------------|
| **HIGH_AMOUNT** | `amount > $10,000` | High — unusually large transaction |
| **INTERNATIONAL_ATM** | ATM withdrawal outside the US | Medium — potential stolen card abroad |
| **SUSPICIOUS_CURRENCY** | Currency doesn't match country (e.g., EUR in US) | Medium — potential card cloning |
| **HIGH_VALUE_ONLINE** | `amount > $5,000` AND `ONLINE_TRANSFER` | Medium — large online transfer |

Multiple flags can apply to a single transaction (e.g., a $15,000 online transfer gets both `HIGH_AMOUNT` and `HIGH_VALUE_ONLINE`).

### Data Quality Expectations

| Expectation | Rule | Action |
|---|---|---|
| `valid_transaction_id` | `transaction_id IS NOT NULL` | Warn |
| `valid_account_id` | `account_id IS NOT NULL` | Warn |
| `valid_amount` | `amount IS NOT NULL AND amount > 0` | Warn |
| `valid_timestamp` | `event_timestamp IS NOT NULL` | Warn |
| `parseable_json` | `_rescued_data IS NULL` | **Drop** (malformed JSON rows are removed) |

---

## Step 1: Import the Notebook

1. In Databricks, click **Workspace** in the left sidebar.
2. Navigate to your user folder.
3. Click **⋮** → **Import**.
4. Upload: `notebooks/04_silver_transformation.py`.

---

## Step 2: Add to the Existing Pipeline

You do **NOT** create a new pipeline. Add this notebook to the same `banking-bronze-ingestion` pipeline:

1. Go to **Pipelines** → Click `banking-bronze-ingestion`.
2. Click **Settings** (gear icon) or **Edit** at the top.
3. Under **Source code**, click **Add source code**.
4. Browse to the imported `04_silver_transformation.py` notebook.
5. Click **Save**.

The pipeline now has two source notebooks:
- `03_bronze_ingestion.py` → defines `bronze_transactions`
- `04_silver_transformation.py` → defines `silver_transactions`

The engine automatically resolves the dependency chain.

---

## Step 3: Run the Pipeline

1. Click **Start** (or **Full refresh all** if it's the first run with Silver).
2. The DAG should now show two tables: `bronze_transactions` → `silver_transactions`.
3. Both should turn green ✅ when complete.

---

## Step 4: Query the Silver Table

```sql
-- Preview enriched data
SELECT
  transaction_id,
  account_id,
  amount,
  currency,
  transaction_type,
  merchant_name,
  merchant_category,
  city,
  country,
  is_fraud,
  fraud_reasons
FROM workspace.banking_streaming.silver_transactions
LIMIT 20;
```

```sql
-- Fraud summary
SELECT
  COUNT(*) AS total_transactions,
  SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) AS fraud_count,
  ROUND(SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS fraud_pct,
  SUM(CASE WHEN is_high_amount THEN 1 ELSE 0 END) AS high_amount_count,
  SUM(CASE WHEN is_international_atm THEN 1 ELSE 0 END) AS intl_atm_count,
  SUM(CASE WHEN is_suspicious_currency THEN 1 ELSE 0 END) AS suspicious_currency_count,
  SUM(CASE WHEN is_high_value_online THEN 1 ELSE 0 END) AS high_value_online_count
FROM workspace.banking_streaming.silver_transactions;
```

```sql
-- Top flagged transactions
SELECT
  transaction_id,
  account_id,
  amount,
  currency,
  transaction_type,
  city,
  country,
  fraud_reasons
FROM workspace.banking_streaming.silver_transactions
WHERE is_fraud = TRUE
ORDER BY amount DESC
LIMIT 20;
```

---

## Pipeline Architecture (So Far)

```
Pipeline: banking-bronze-ingestion
┌──────────────────────────────────────────────────────────────┐
│                                                              │
│  ┌─────────────────┐         ┌──────────────────────┐       │
│  │ 03_bronze_       │         │ 04_silver_            │       │
│  │ ingestion.py     │         │ transformation.py     │       │
│  │                  │         │                       │       │
│  │ @dp.table        │         │ @dp.table             │       │
│  │ bronze_          │────────▶│ silver_               │       │
│  │ transactions     │ stream  │ transactions          │       │
│  │ (Kafka ingest)   │  read   │ (parse + fraud flags) │       │
│  └─────────────────┘         └──────────────────────┘       │
│                                                              │
└──────────────────────────────────────────────────────────────┘
         ▲
         │ port 9092
         │
  EC2 Kafka Broker
  (15.135.41.136)
```

---

## What's Next?

**Phase 5: Aggregations & Real-Time Alerts (Gold Layer)**
- Windowed aggregations (5-min sliding windows for transaction volume per merchant category)
- Write KPIs to `gold_merchant_analytics` materialized view
- Filter fraud records and write back to Kafka `banking.fraud-alerts` topic via a Sink
