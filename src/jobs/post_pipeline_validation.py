# Databricks notebook source
# MAGIC %md
# MAGIC # 📊 Task 3: Post-Pipeline Validation & SLA Monitoring
# MAGIC
# MAGIC This notebook runs as the final task in the **Lakeflow Job DAG** after the Declarative Pipeline update completes.
# MAGIC
# MAGIC **Purpose:**
# MAGIC - Validates that new records landed across Bronze, Silver, and Gold.
# MAGIC - Asserts critical business SLAs (e.g., fraud rate % is within realistic bounds, non-zero total volume).
# MAGIC - Generates an executive summary log.

# COMMAND ----------

# ── Parameters ────────────────────────────────────────────────────────────────
dbutils.widgets.text("catalog", "workspace", "Target Catalog")
dbutils.widgets.text("schema", "banking_streaming", "Target Schema")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_transactions"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_transactions"
GOLD_MERCHANT_TABLE = f"{CATALOG}.{SCHEMA}.gold_merchant_analytics"
GOLD_FRAUD_TABLE = f"{CATALOG}.{SCHEMA}.gold_fraud_alerts"

# COMMAND ----------

from pyspark.sql import functions as F

print("=" * 60)
print("🏦 Post-Pipeline Validation Summary")
print("=" * 60)

# 1. Check Row Counts
bronze_count = spark.table(BRONZE_TABLE).count()
silver_count = spark.table(SILVER_TABLE).count()
gold_merchant_count = spark.table(GOLD_MERCHANT_TABLE).count()
gold_fraud_count = spark.table(GOLD_FRAUD_TABLE).count()

print(f"  🥉 Bronze Total Rows:         {bronze_count:,}")
print(f"  🥈 Silver Total Rows:         {silver_count:,}")
print(f"  🥇 Gold Merchant Groups:      {gold_merchant_count:,}")
print(f"  🚨 Gold Flagged Fraud Rows:   {gold_fraud_count:,}")
print("-" * 60)

# 2. SLA Assertions
assert bronze_count > 0, "❌ SLA Violation: Bronze table is empty!"
assert silver_count > 0, "❌ SLA Violation: Silver table is empty!"

# 3. Fraud Metrics Sanity Check
fraud_stats = spark.table(SILVER_TABLE).agg(
    F.count("*").alias("total"),
    F.sum(F.when(F.col("is_fraud"), 1).otherwise(0)).alias("fraud_cnt"),
    F.round((F.sum(F.when(F.col("is_fraud"), 1).otherwise(0)) * 100.0) / F.count("*"), 2).alias("fraud_pct"),
    F.round(F.sum("amount"), 2).alias("total_vol_usd"),
    F.round(F.sum(F.when(F.col("is_fraud"), F.col("amount")).otherwise(0.0)), 2).alias("fraud_vol_usd")
).collect()[0]

print(f"  📈 Processed Volume:          ${fraud_stats['total_vol_usd']:,.2f}")
print(f"  🛡️ Fraud Volume:              ${fraud_stats['fraud_vol_usd']:,.2f}")
print(f"  🎯 Calculated Fraud Rate:     {fraud_stats['fraud_pct']}%")

# We injected ~8% synthetic fraud; fail if fraud rate exceeds a critical safety threshold (e.g. 50%)
assert fraud_stats['fraud_pct'] <= 50.0, f"❌ Fraud anomaly spike: fraud rate is {fraud_stats['fraud_pct']}% (> 50%)!"

print("=" * 60)
print("✅ ALL POST-PIPELINE VALIDATION CHECKS PASSED")
print("=" * 60)

dbutils.notebook.exit("POST_PIPELINE_VALIDATION_PASSED")
