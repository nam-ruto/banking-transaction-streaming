# Databricks notebook source
# MAGIC %md
# MAGIC # 🥉 Bronze Layer: Kafka Streaming Ingestion (Declarative Pipeline)
# MAGIC
# MAGIC This notebook defines a **Lakeflow Declarative Pipeline** (formerly Delta Live Tables)
# MAGIC that ingests raw banking transactions from the `banking.transactions` Kafka topic
# MAGIC into a `bronze_transactions` **Streaming Table** in Unity Catalog.
# MAGIC
# MAGIC **Pipeline Features:**
# MAGIC - Uses the modern `pyspark.pipelines` (`dp`) API — NOT the legacy `dlt` module
# MAGIC - Streaming Table with `@dp.table` for exactly-once, incremental Kafka ingestion
# MAGIC - Data quality expectations (`@dp.expect`) to track and warn on null/invalid payloads
# MAGIC - Liquid clustering for optimized query performance
# MAGIC
# MAGIC **How to run:**
# MAGIC 1. In Databricks, go to **Pipelines** (left sidebar) → **Create pipeline**
# MAGIC 2. Set **Source code** to this notebook's path
# MAGIC 3. Set **Target catalog** and **Target schema** (e.g., `workspace.banking_streaming`)
# MAGIC 4. Set **Serverless** compute (recommended, includes Kafka connector)
# MAGIC 5. Click **Start** to run the pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# ── Pipeline Configuration ────────────────────────────────────────────────────
# These values are used by the streaming table definitions below.
# Update KAFKA_BOOTSTRAP_SERVER with your EC2 Elastic IP.

KAFKA_BOOTSTRAP_SERVER = "15.135.41.136:9092"   # EC2 Elastic IP (EXTERNAL listener)
KAFKA_TOPIC = "banking.transactions"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Streaming Table: Raw Kafka Ingestion
# MAGIC
# MAGIC This streaming table reads from the Kafka topic and lands the raw data with
# MAGIC Kafka metadata columns. JSON parsing is deferred to the Silver layer.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    comment="Raw banking transactions ingested from Kafka. Preserves the original JSON payload and Kafka metadata.",
    table_properties={
        "quality": "bronze",
    },
    cluster_by=["kafka_partition", "ingestion_date"],
)
@dp.expect("valid_payload", "raw_value IS NOT NULL")
@dp.expect("valid_key", "kafka_key IS NOT NULL")
@dp.expect("valid_timestamp", "kafka_timestamp IS NOT NULL")
def bronze_transactions():
    """
    Ingest raw banking transactions from the Kafka topic.

    Reads from the EXTERNAL listener (port 9092) on the EC2 Kafka broker.
    Each row preserves:
      - kafka_key:        The message key (account_id)
      - raw_value:        The full JSON payload as a string (parsed in Silver)
      - kafka_topic:      Source Kafka topic name
      - kafka_partition:  Kafka partition number
      - kafka_offset:     Message offset within the partition
      - kafka_timestamp:  Kafka-assigned message timestamp
      - ingestion_timestamp: When Databricks ingested the record
      - ingestion_date:   Date partition for efficient querying
    """
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 10000)
        .option("kafka.session.timeout.ms", "30000")
        .option("kafka.request.timeout.ms", "40000")
        .option("failOnDataLoss", "false")
        .load()
        # Decode binary key and value to strings
        .withColumn("kafka_key", F.col("key").cast("string"))
        .withColumn("raw_value", F.col("value").cast("string"))
        # Kafka metadata
        .withColumn("kafka_topic", F.col("topic"))
        .withColumn("kafka_partition", F.col("partition"))
        .withColumn("kafka_offset", F.col("offset"))
        .withColumn("kafka_timestamp", F.col("timestamp"))
        # Ingestion metadata
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("ingestion_date", F.current_date())
        # Select final columns (drop original binary columns)
        .select(
            "kafka_key",
            "raw_value",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "ingestion_timestamp",
            "ingestion_date",
        )
    )
