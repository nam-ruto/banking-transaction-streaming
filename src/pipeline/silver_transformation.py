# Databricks notebook source
# MAGIC %md
# MAGIC # 🥈 Silver Layer: Stream Transformation & Fraud Detection
# MAGIC
# MAGIC This notebook defines the Silver layer of the Medallion Architecture as a
# MAGIC **Lakeflow Declarative Pipeline** streaming table.
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC 1. Reads the raw JSON from `bronze_transactions` (streaming read)
# MAGIC 2. Parses into strongly typed columns (flattens nested `merchant` and `location`)
# MAGIC 3. Applies rule-based fraud detection flags
# MAGIC 4. Writes to `silver_transactions` streaming table with data quality expectations
# MAGIC
# MAGIC **How to use:**
# MAGIC Add this notebook as an additional source in the same pipeline as `bronze_ingestion.py`.
# MAGIC The pipeline engine automatically resolves the dependency: Silver reads from Bronze.


# COMMAND ----------

# MAGIC %md
# MAGIC ## Transaction JSON Schema
# MAGIC
# MAGIC The `raw_value` column in Bronze contains JSON like:
# MAGIC ```json
# MAGIC {
# MAGIC   "transaction_id": "tx-a1b2c3d4-1234",
# MAGIC   "account_id": "acc-482910",
# MAGIC   "card_number_masked": "4532-XXXX-XXXX-7821",
# MAGIC   "timestamp": "2026-08-25T10:00:01.234Z",
# MAGIC   "amount": 42.50,
# MAGIC   "currency": "USD",
# MAGIC   "transaction_type": "POS_PURCHASE",
# MAGIC   "merchant": {
# MAGIC     "merchant_id": "m-3391",
# MAGIC     "name": "Starbucks",
# MAGIC     "category": "RESTAURANT"
# MAGIC   },
# MAGIC   "location": {
# MAGIC     "city": "New York",
# MAGIC     "country": "US",
# MAGIC     "lat": 40.7152,
# MAGIC     "lon": -74.0034
# MAGIC   },
# MAGIC   "device_id": "dev-ios-44821"
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define the JSON Schema

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)

# Explicit schema for the JSON payload (avoids schema inference on streaming)
TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("card_number_masked", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("merchant", StructType([
        StructField("merchant_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("category", StringType(), True),
    ]), True),
    StructField("location", StructType([
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
    ]), True),
    StructField("device_id", StringType(), True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fraud Detection Rules
# MAGIC
# MAGIC | Rule | Condition | Description |
# MAGIC |------|-----------|-------------|
# MAGIC | `HIGH_AMOUNT` | `amount > 10,000` | Unusually large transaction |
# MAGIC | `INTERNATIONAL_ATM` | `transaction_type = ATM_WITHDRAWAL` AND `country ≠ US` | ATM withdrawal abroad |
# MAGIC | `SUSPICIOUS_CURRENCY` | Currency doesn't match country (e.g., EUR in US, USD in Japan) | Potential card cloning |
# MAGIC | `HIGH_VALUE_ONLINE` | `amount > 5,000` AND `transaction_type = ONLINE_TRANSFER` | Large online transfer |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Streaming Table

# COMMAND ----------

# Currency-to-country mapping for suspicious currency detection
EXPECTED_CURRENCY = {
    "US": "USD",
    "CA": "CAD",
    "GB": "GBP",
    "AU": "AUD",
    "DE": "EUR",
    "JP": "JPY",
}

# Build a MapType column for the lookup
currency_map_expr = F.create_map(*[
    item for pair in EXPECTED_CURRENCY.items() for item in (F.lit(pair[0]), F.lit(pair[1]))
])


@dp.table(
    comment="Enriched banking transactions with parsed JSON, flattened fields, and fraud detection flags.",
    table_properties={
        "quality": "silver",
    },
    cluster_by=["account_id", "transaction_date"],
)
@dp.expect("valid_transaction_id", "transaction_id IS NOT NULL")
@dp.expect("valid_account_id", "account_id IS NOT NULL")
@dp.expect("valid_amount", "amount IS NOT NULL AND amount > 0")
@dp.expect("valid_timestamp", "event_timestamp IS NOT NULL")
@dp.expect_or_drop("parseable_json", "_rescued_data IS NULL")
def silver_transactions():
    """
    Parse raw Bronze JSON into typed columns, flatten nested structs,
    and apply rule-based fraud detection.

    Reads from: bronze_transactions (streaming)
    Fraud flags: HIGH_AMOUNT, INTERNATIONAL_ATM, SUSPICIOUS_CURRENCY, HIGH_VALUE_ONLINE
    """
    # 1. Read from Bronze (streaming read from sibling table)
    bronze = spark.readStream.table("bronze_transactions")

    # 2. Parse JSON payload
    parsed = (
        bronze
        .withColumn("parsed", F.from_json(F.col("raw_value"), TRANSACTION_SCHEMA, {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_rescued_data"}))
        .withColumn("_rescued_data", F.get_json_object(F.col("raw_value"), "$._rescued_data"))
    )

    # 3. Flatten into typed columns + apply fraud rules
    return (
        parsed
        # Core transaction fields
        .withColumn("transaction_id", F.col("parsed.transaction_id"))
        .withColumn("account_id", F.col("parsed.account_id"))
        .withColumn("card_number_masked", F.col("parsed.card_number_masked"))
        .withColumn("event_timestamp", F.to_timestamp(F.col("parsed.timestamp")))
        .withColumn("amount", F.col("parsed.amount"))
        .withColumn("currency", F.col("parsed.currency"))
        .withColumn("transaction_type", F.col("parsed.transaction_type"))

        # Flatten merchant struct
        .withColumn("merchant_id", F.col("parsed.merchant.merchant_id"))
        .withColumn("merchant_name", F.col("parsed.merchant.name"))
        .withColumn("merchant_category", F.col("parsed.merchant.category"))

        # Flatten location struct
        .withColumn("city", F.col("parsed.location.city"))
        .withColumn("country", F.col("parsed.location.country"))
        .withColumn("latitude", F.col("parsed.location.lat"))
        .withColumn("longitude", F.col("parsed.location.lon"))

        # Device
        .withColumn("device_id", F.col("parsed.device_id"))

        # ── Fraud Detection Rules ─────────────────────────────────
        .withColumn("is_high_amount", F.col("amount") > 10_000)
        .withColumn("is_international_atm",
            (F.col("transaction_type") == "ATM_WITHDRAWAL") & (F.col("country") != "US")
        )
        .withColumn("is_suspicious_currency",
            currency_map_expr[F.col("country")].isNotNull() &
            (currency_map_expr[F.col("country")] != F.col("currency"))
        )
        .withColumn("is_high_value_online",
            (F.col("amount") > 5_000) & (F.col("transaction_type") == "ONLINE_TRANSFER")
        )

        # Aggregate fraud flags
        .withColumn("is_fraud",
            F.col("is_high_amount") |
            F.col("is_international_atm") |
            F.col("is_suspicious_currency") |
            F.col("is_high_value_online")
        )
        .withColumn("fraud_reasons", F.concat_ws(", ", *[
            F.when(F.col("is_high_amount"), F.lit("HIGH_AMOUNT")),
            F.when(F.col("is_international_atm"), F.lit("INTERNATIONAL_ATM")),
            F.when(F.col("is_suspicious_currency"), F.lit("SUSPICIOUS_CURRENCY")),
            F.when(F.col("is_high_value_online"), F.lit("HIGH_VALUE_ONLINE")),
        ]))

        # Derived columns for partitioning and analytics
        .withColumn("transaction_date", F.to_date(F.col("event_timestamp")))
        .withColumn("transaction_hour", F.hour(F.col("event_timestamp")))

        # Select final columns (drop intermediate)
        .select(
            # Transaction core
            "transaction_id",
            "account_id",
            "card_number_masked",
            "event_timestamp",
            "transaction_date",
            "transaction_hour",
            "amount",
            "currency",
            "transaction_type",
            # Merchant
            "merchant_id",
            "merchant_name",
            "merchant_category",
            # Location
            "city",
            "country",
            "latitude",
            "longitude",
            # Device
            "device_id",
            # Fraud flags
            "is_fraud",
            "fraud_reasons",
            "is_high_amount",
            "is_international_atm",
            "is_suspicious_currency",
            "is_high_value_online",
            # Kafka metadata (carry forward for lineage)
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "ingestion_timestamp",
            # Rescue column (for data quality)
            "_rescued_data",
        )
    )
