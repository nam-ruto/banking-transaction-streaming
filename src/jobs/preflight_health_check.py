# Databricks notebook source
# MAGIC %md
# MAGIC # 🩺 Task 1: Pre-Flight Health Check
# MAGIC
# MAGIC This notebook acts as the first task in the **Lakeflow Job DAG**.
# MAGIC
# MAGIC **Purpose:**
# MAGIC - Validates TCP connectivity to the AWS EC2 Kafka broker (`15.135.41.136:9092`).
# MAGIC - Validates Unity Catalog catalog and schema accessibility.
# MAGIC - Fails fast if the broker is unreachable before launching downstream pipeline tasks.

# COMMAND ----------

# ── Parameters ────────────────────────────────────────────────────────────────
dbutils.widgets.text("kafka_bootstrap_server", "15.135.41.136:9092", "Kafka Bootstrap Server")
dbutils.widgets.text("catalog", "workspace", "Target Catalog")
dbutils.widgets.text("schema", "banking_streaming", "Target Schema")

KAFKA_SERVER = dbutils.widgets.get("kafka_bootstrap_server")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

print(f"Checking Kafka Broker: {KAFKA_SERVER}")
print(f"Target Namespace:      {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Test Network Connectivity to Kafka

# COMMAND ----------

import socket

host, port_str = KAFKA_SERVER.split(":")
port = int(port_str)

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(10)

try:
    s.connect((host, port))
    print(f"✅ SUCCESS: Connected to Kafka broker at {KAFKA_SERVER}")
except Exception as e:
    error_msg = f"❌ FAILED: Cannot connect to Kafka broker at {KAFKA_SERVER}. Error: {e}"
    print(error_msg)
    s.close()
    raise ConnectionError(error_msg)
finally:
    s.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ensure Unity Catalog Schema Exists

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
print(f"✅ SUCCESS: Verified schema '{CATALOG}.{SCHEMA}' exists in Unity Catalog.")

# COMMAND ----------

dbutils.notebook.exit("PREFLIGHT_HEALTH_CHECK_PASSED")
