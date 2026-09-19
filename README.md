# 🏦 Real-Time Banking Transaction Processing & Fraud Detection Platform

An end-to-end streaming data engineering project integrating **Apache Kafka (on AWS EC2)**, **Apache Spark (Structured Streaming)**, and **Databricks (Lakehouse / Delta Lake)**.

---

## 1. Executive Summary

This project builds a production-grade real-time streaming pipeline for a digital banking platform. It ingests simulated high-volume financial transactions through **Apache Kafka hosted on an AWS EC2 instance**, processes and cleans the stream with **Spark Structured Streaming on Databricks**, builds a **Medallion Architecture (Bronze ➔ Silver ➔ Gold) on Delta Lake**, and triggers real-time fraud alerts while serving operational banking dashboards.

---

## 2. Business Objectives & Use Cases

- **Real-Time Transaction Ingestion**: Ingest banking events (ATM withdrawals, POS purchases, online wire transfers) with sub-second latency.
- **Rule-Based Fraud Detection**: Detect suspicious activities in real time (e.g., transactions > $10,000, rapid geographic hops between transactions, velocity spikes).
- **Customer Spending Analytics**: Compute continuous rolling-window aggregations (e.g., hourly spending per merchant category and account activity).
- **Governed Data Lakehouse**: Store immutable audit logs and analytics-ready datasets governed by Databricks Unity Catalog.

---

## 3. End-to-End Architecture

```
 ┌─────────────────────────────────────────────────────────────┐
 │                     AWS CLOUD (VPC)                         │
 │                                                             │
 │   ┌──────────────────────┐        ┌──────────────────────┐  │
 │   │  EC2 Producer Script │───────▶│     AWS EC2 Node     │  │
 │   │ (Faker / Bank Events)│        │   Apache Kafka Broker│  │
 │   └──────────────────────┘        │ (KRaft Mode, Port 9092) │
 │                                   └──────────┬───────────┘  │
 └──────────────────────────────────────────────┼──────────────┘
                                                │
                               readStream over SSL / Public IP
                                                ▼
 ┌─────────────────────────────────────────────────────────────┐
 │                     DATABRICKS WORKSPACE                    │
 │                                                             │
 │  ┌───────────────────────────────────────────────────────┐  │
 │  │        Spark Structured Streaming (Python / PySpark)  │  │
 │  └───────────────────────────┬───────────────────────────┘  │
 │                              │                              │
 │                              ▼                              │
 │    ┌──────────────┐   ┌──────────────┐   ┌──────────────┐   │
 │    │ 🥉 Bronze    │──▶│ 🥈 Silver    │──▶│ 🥇 Gold      │   │
 │    │ Raw Payload  │   │ Enriched &   │   │ Hourly Fraud │   │
 │    │ & Kafka Meta │   │ Fraud Flagged│   │ Aggregates   │   │
 │    └──────────────┘   └──────┬───────┘   └──────┬───────┘   │
 │                              │                  │           │
 │                              ▼                  ▼           │
 └──────────────────────┼──────────────────────────┼───────────┘
                        │                          │
                        ▼                          ▼
          ┌──────────────────────────┐   ┌───────────────────────────┐
          │  Alerts Topic on Kafka   │   │ Databricks AI/BI Dashboard│
          │ (Real-time notifications)│   │ (Fraud & KPI Metrics)     │
          └──────────────────────────┘   └───────────────────────────┘
```

---

## 4. Technology Stack

| Layer | Technology | Purpose |
|---|---|---|
| **Data Generation** | Python (`Faker`, `confluent-kafka`) | Simulates realistic banking transactions & customer profiles |
| **Streaming Broker** | Apache Kafka (KRaft mode) on **AWS EC2** | Decoupled message queue, partition management, and buffer |
| **Compute Engine** | **Databricks** (Spark Structured Streaming) | Real-time stream processing, JSON parsing, watermarking, joins |
| **Storage Layer** | **Delta Lake** (Unity Catalog) | ACID transaction store, time travel, schema enforcement |
| **Serving / BI** | **Databricks SQL** / AI/BI Dashboards | Fraud monitoring dashboard, merchant breakdown, volume KPIs |

---

## 5. Sample Data Schema

### Kafka Raw Message (`banking.transactions` topic)
```json
{
  "transaction_id": "tx-98234-8841",
  "account_id": "acc-109283",
  "card_number_masked": "4532-XXXX-XXXX-1289",
  "timestamp": "2026-08-19T14:40:00Z",
  "amount": 1450.50,
  "currency": "USD",
  "transaction_type": "POS_PURCHASE",
  "merchant": {
    "merchant_id": "m-7721",
    "name": "Apple Store NYC",
    "category": "ELECTRONICS"
  },
  "location": {
    "city": "New York",
    "country": "US",
    "lat": 40.7128,
    "lon": -74.0060
  },
  "device_id": "dev-ios-99182"
}
```

---

## 6. Project Implementation Phases

```
Phase 1: AWS EC2 & Kafka Setup
  ├── Launch EC2 instance (Ubuntu/Amazon Linux on t3.xlarge or t3.large)
  ├── Install OpenJDK 17 & Apache Kafka (latest KRaft mode — no ZooKeeper needed)
  ├── Configure server.properties (listeners, advertised listeners, security group port 9092/9093)
  └── Create Kafka topics: `banking.transactions`, `banking.fraud-alerts`

Phase 2: Mock Transaction Producer
  ├── Develop Python script using `Faker` to generate synthetic bank transactions
  ├── Inject intentional anomalies (large transactions > $10K, rapid transactions in 2 cities)
  └── Stream events continuously at 10–50 records/sec

Phase 3: Databricks Streaming Ingestion (Bronze Layer)
  ├── Set up secure connectivity from Databricks to EC2 Kafka IP
  ├── Use `spark.readStream.format("kafka")` to ingest raw streams
  └── Persist raw stream into `bronze_transactions` Delta table with checkpointing

Phase 4: Stream Transformation & Fraud Detection (Silver Layer)
  ├── Parse JSON payload into strongly typed Spark schema
  ├── Apply fraud validation rules (velocity check, amount threshold, country mismatch)
  ├── Flag transactions: `is_fraud = true` / `fraud_reason = 'HIGH_AMOUNT'`
  └── Write cleaned data to `silver_transactions` Delta table

Phase 5: Aggregations & Real-Time Alerts (Gold Layer)
  ├── Windowed aggregations (category & hourly metrics)
  ├── Write aggregated KPIs to `gold_merchant_analytics` Materialized View
  ├── Filter high-risk records to `gold_fraud_alerts` Streaming Table
  └── Stream fraud alerts back to Kafka `banking.fraud-alerts` topic via `@dp.append_flow`

Phase 6: Lakeflow Jobs & Orchestration
  ├── Pre-flight TCP connectivity and schema readiness validation task
  ├── Trigger Declarative Pipeline update across Bronze -> Silver -> Gold + Kafka Sink
  ├── Post-pipeline SLA and data quality assertion task
  └── Cron scheduling and failure notification alerts

Phase 7: Infrastructure as Code with Databricks Asset Bundles (DABs)
  ├── Structured repository organization (`src/pipeline/`, `src/jobs/`, `resources/`)
  ├── Declarative YAML definitions for pipelines and multi-task jobs
  └── Automated multi-environment deployments (`dev` / `prod`) via Databricks CLI
```

---

## 7. Key Learning Outcomes

1. **EC2 Kafka Operations**: Configuring network listeners, security groups, retention policies, and topics on cloud VMs.
2. **Spark Structured Streaming**: Handling streaming triggers, checkpointing, watermarking for late-arriving data, and stateful processing.
3. **Delta Lake on Databricks**: Implementing the Medallion Architecture pattern with ACID guarantees and streaming tables.
4. **End-to-End Real-Time Architecture**: Connecting independent cloud services into a cohesive, secure data pipeline.
