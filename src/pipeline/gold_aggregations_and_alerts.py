# Databricks notebook source
# MAGIC %md
# MAGIC # 🥇 Gold Layer: Business Analytics & Real-Time Fraud Alerts
# MAGIC
# MAGIC This notebook defines the Gold layer of the Medallion Architecture using **Lakeflow Declarative Pipelines**.
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC 1. **`gold_merchant_analytics` (Materialized View)**:
# MAGIC    Aggregates transaction volume, spending patterns, and fraud rates by merchant category, date, and hour for business intelligence.
# MAGIC 2. **`gold_fraud_alerts` (Streaming Table)**:
# MAGIC    Filters high-risk fraudulent transactions in real time with operational metadata for rapid incident triage.
# MAGIC 3. **`fraud_alerts_kafka_sink` (Kafka Sink)**:
# MAGIC    Streams real-time fraud alert JSON events back to the EC2 Kafka topic `banking.fraud-alerts` for downstream consumers.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F

KAFKA_BOOTSTRAP_SERVER = "15.135.41.136:9092"
FRAUD_ALERTS_TOPIC = "banking.fraud-alerts"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Gold Materialized View: Merchant & Category Analytics
# MAGIC
# MAGIC Materialized views compute aggregations across the Silver dataset, providing high-performance query access for dashboards and BI tools.

# COMMAND ----------

@dp.materialized_view(
    comment="Hourly and category-level transaction aggregations, fraud volume, and average ticket sizes for executive dashboards.",
    table_properties={
        "quality": "gold",
    },
    cluster_by=["merchant_category", "transaction_date"],
)
def gold_merchant_analytics():
    """
    Computes business KPIs grouped by category, date, and hour:
      - Total transaction count & monetary volume
      - Total fraud count & fraud volume
      - Fraud rate percentage
      - Average transaction amount
    """
    return (
        spark.read.table("silver_transactions")
        .groupBy(
            "transaction_date",
            "transaction_hour",
            "merchant_category",
            "country"
        )
        .agg(
            F.count("*").alias("total_transactions"),
            F.round(F.sum("amount"), 2).alias("total_volume_usd"),
            F.round(F.avg("amount"), 2).alias("avg_transaction_amount"),
            F.sum(F.when(F.col("is_fraud"), 1).otherwise(0)).alias("fraud_transactions_count"),
            F.round(F.sum(F.when(F.col("is_fraud"), F.col("amount")).otherwise(0.0)), 2).alias("fraud_volume_usd"),
            F.round(
                (F.sum(F.when(F.col("is_fraud"), 1).otherwise(0)) * 100.0) / F.count("*"),
                2
            ).alias("fraud_rate_pct")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Gold Streaming Table: Real-Time Fraud Alerts Table
# MAGIC
# MAGIC Persists real-time alerts into a dedicated Gold table for compliance audits, security alerts, and fraud analyst monitoring.

# COMMAND ----------

@dp.table(
    comment="Curated real-time fraud alerts filtered from the Silver stream.",
    table_properties={
        "quality": "gold",
    },
    cluster_by=["fraud_reasons", "event_timestamp"],
)
@dp.expect("is_valid_fraud_flag", "is_fraud = TRUE")
def gold_fraud_alerts():
    """
    Continuous stream of flagged transactions.
    """
    return (
        spark.readStream.table("silver_transactions")
        .filter(F.col("is_fraud") == True)
        .select(
            "transaction_id",
            "account_id",
            "card_number_masked",
            "event_timestamp",
            "amount",
            "currency",
            "transaction_type",
            "merchant_name",
            "merchant_category",
            "city",
            "country",
            "is_fraud",
            "fraud_reasons",
            "is_high_amount",
            "is_international_atm",
            "is_suspicious_currency",
            "is_high_value_online",
            "ingestion_timestamp"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Kafka Sink: Stream Alerts back to `banking.fraud-alerts`
# MAGIC
# MAGIC Publishes real-time fraud alert JSON payloads back to the Kafka cluster on EC2 so downstream applications (SMS notifications, mobile push alerts, automated card lock services) can react instantly.

# COMMAND ----------

# Define the external Kafka sink
dp.create_sink(
    name="fraud_alerts_kafka_sink",
    format="kafka",
    options={
        "kafka.bootstrap.servers": KAFKA_BOOTSTRAP_SERVER,
        "topic": FRAUD_ALERTS_TOPIC,
    }
)

@dp.append_flow(target="fraud_alerts_kafka_sink")
def emit_fraud_alerts_to_kafka():
    """
    Reads flagged fraud transactions from Silver, serializes them as JSON,
    and appends them to the external Kafka sink.
    """
    return (
        spark.readStream.table("silver_transactions")
        .filter(F.col("is_fraud") == True)
        .select(
            # Key: account_id (for partition affinity)
            F.col("account_id").alias("key"),
            # Value: full fraud alert JSON payload
            F.to_json(
                F.struct(
                    F.col("transaction_id"),
                    F.col("account_id"),
                    F.col("card_number_masked"),
                    F.col("event_timestamp").cast("string").alias("timestamp"),
                    F.col("amount"),
                    F.col("currency"),
                    F.col("transaction_type"),
                    F.col("merchant_name"),
                    F.col("merchant_category"),
                    F.col("city"),
                    F.col("country"),
                    F.col("fraud_reasons")
                )
            ).alias("value")
        )
    )
