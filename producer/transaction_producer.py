"""
Banking Transaction Producer for Apache Kafka
==============================================
Generates realistic synthetic banking transactions using Faker and streams
them to the `banking.transactions` Kafka topic. Injects intentional fraud
anomalies (~8% of transactions) for downstream detection.

Usage:
    # Run on EC2 (uses INTERNAL listener)
    python3 transaction_producer.py

    # Run from laptop (uses EXTERNAL listener)
    python3 transaction_producer.py --bootstrap-server 15.135.41.136:9092

    # Custom rate (default: 10 records/sec)
    python3 transaction_producer.py --rate 25
"""

import argparse
import json
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer, KafkaError
from faker import Faker

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
TOPIC = "banking.transactions"

TRANSACTION_TYPES = [
    "ATM_WITHDRAWAL",
    "POS_PURCHASE",
    "ONLINE_TRANSFER",
    "WIRE_TRANSFER",
    "MOBILE_PAYMENT",
    "DIRECT_DEPOSIT",
]

MERCHANT_CATEGORIES = {
    "GROCERY":      ["Whole Foods", "Trader Joe's", "Kroger", "Safeway", "Costco"],
    "ELECTRONICS":  ["Apple Store", "Best Buy", "Micro Center", "B&H Photo"],
    "RESTAURANT":   ["McDonald's", "Chipotle", "Starbucks", "Olive Garden", "Shake Shack"],
    "GAS_STATION":  ["Shell", "Chevron", "BP", "ExxonMobil", "Sunoco"],
    "TRAVEL":       ["Delta Airlines", "Marriott Hotels", "Hilton", "United Airlines", "Airbnb"],
    "HEALTHCARE":   ["CVS Pharmacy", "Walgreens", "Kaiser Permanente", "Quest Diagnostics"],
    "ENTERTAINMENT": ["Netflix", "Spotify", "AMC Theatres", "Ticketmaster", "Steam"],
    "RETAIL":       ["Amazon", "Walmart", "Target", "Nordstrom", "Nike Store"],
}

CURRENCIES = ["USD", "EUR", "GBP", "AUD", "CAD"]

# City locations with lat/lon for geographic anomaly injection
LOCATIONS = [
    {"city": "New York",      "country": "US", "lat": 40.7128,  "lon": -74.0060},
    {"city": "Los Angeles",   "country": "US", "lat": 34.0522,  "lon": -118.2437},
    {"city": "Chicago",       "country": "US", "lat": 41.8781,  "lon": -87.6298},
    {"city": "Houston",       "country": "US", "lat": 29.7604,  "lon": -95.3698},
    {"city": "Miami",         "country": "US", "lat": 25.7617,  "lon": -80.1918},
    {"city": "San Francisco", "country": "US", "lat": 37.7749,  "lon": -122.4194},
    {"city": "Seattle",       "country": "US", "lat": 47.6062,  "lon": -122.3321},
    {"city": "London",        "country": "GB", "lat": 51.5074,  "lon": -0.1278},
    {"city": "Tokyo",         "country": "JP", "lat": 35.6762,  "lon": 139.6503},
    {"city": "Sydney",        "country": "AU", "lat": -33.8688, "lon": 151.2093},
    {"city": "Toronto",       "country": "CA", "lat": 43.6532,  "lon": -79.3832},
    {"city": "Berlin",        "country": "DE", "lat": 52.5200,  "lon": 13.4050},
]

DEVICE_PREFIXES = ["dev-ios", "dev-android", "dev-web", "dev-atm", "dev-pos"]

COUNTRY_CURRENCY = {
    "US": "USD",
    "CA": "CAD",
    "GB": "GBP",
    "AU": "AUD",
    "DE": "EUR",
    "JP": "JPY",
}

US_LOCATIONS = [loc for loc in LOCATIONS if loc["country"] == "US"]
INTL_LOCATIONS = [loc for loc in LOCATIONS if loc["country"] != "US"]

# ---------------------------------------------------------------------------
# Account Pool (pre-generated for realistic repeat behavior)
# ---------------------------------------------------------------------------
fake = Faker()
Faker.seed(42)
random.seed(42)

NUM_ACCOUNTS = 100
ACCOUNTS = []
for i in range(NUM_ACCOUNTS):
    acc_id = f"acc-{random.randint(100000, 999999)}"
    card_prefix = random.choice(["4532", "5412", "3782", "6011"])
    card_suffix = f"{random.randint(1000, 9999)}"
    card_masked = f"{card_prefix}-XXXX-XXXX-{card_suffix}"
    device_id = f"{random.choice(DEVICE_PREFIXES)}-{random.randint(10000, 99999)}"
    home_location = random.choice(US_LOCATIONS)  # US-based domestic accounts

    ACCOUNTS.append({
        "account_id": acc_id,
        "card_number_masked": card_masked,
        "device_id": device_id,
        "home_location": home_location,
    })

# Track recent transactions per account for velocity-based fraud injection
account_last_tx: dict[str, dict] = {}

# ---------------------------------------------------------------------------
# Transaction Generator
# ---------------------------------------------------------------------------
def generate_normal_transaction() -> dict:
    """Generate a single normal (non-fraudulent) banking transaction."""
    account = random.choice(ACCOUNTS)
    category = random.choice(list(MERCHANT_CATEGORIES.keys()))
    merchant_name = random.choice(MERCHANT_CATEGORIES[category])
    location = account["home_location"]  # Normal: transact near home

    # Normal amounts: $1 - $2,000 with realistic distribution
    amount = round(random.lognormvariate(3.5, 1.2), 2)
    amount = min(amount, 2000.0)  # Cap normal transactions
    amount = max(amount, 1.00)

    tx = {
        "transaction_id": f"tx-{uuid.uuid4().hex[:8]}-{random.randint(1000, 9999)}",
        "account_id": account["account_id"],
        "card_number_masked": account["card_number_masked"],
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "amount": amount,
        "currency": COUNTRY_CURRENCY.get(location["country"], "USD"),
        "transaction_type": random.choices(
            ["POS_PURCHASE", "ONLINE_TRANSFER", "ATM_WITHDRAWAL", "MOBILE_PAYMENT", "DIRECT_DEPOSIT"],
            weights=[45, 20, 15, 15, 5],
        )[0],
        "merchant": {
            "merchant_id": f"m-{random.randint(1000, 9999)}",
            "name": merchant_name,
            "category": category,
        },
        "location": {
            "city": location["city"],
            "country": location["country"],
            "lat": round(location["lat"] + random.uniform(-0.05, 0.05), 4),
            "lon": round(location["lon"] + random.uniform(-0.05, 0.05), 4),
        },
        "device_id": account["device_id"],
    }

    # Track for velocity checks
    account_last_tx[account["account_id"]] = {
        "timestamp": tx["timestamp"],
        "location": location,
    }

    return tx


def inject_high_amount_fraud() -> dict:
    """Inject a suspiciously large transaction (> $10,000)."""
    tx = generate_normal_transaction()
    tx["amount"] = round(random.uniform(10_000, 95_000), 2)
    tx["transaction_type"] = random.choice(["WIRE_TRANSFER", "ONLINE_TRANSFER", "ATM_WITHDRAWAL"])
    return tx


def inject_geo_hop_fraud() -> dict:
    """Inject a rapid geographic hop: same account, drastically different city."""
    # Pick an account that has recent activity
    active_accounts = [acc for acc in ACCOUNTS if acc["account_id"] in account_last_tx]

    if not active_accounts:
        return inject_high_amount_fraud()  # Fallback

    account = random.choice(active_accounts)
    last_loc = account_last_tx[account["account_id"]]["location"]

    # Pick a distant city (different from last location)
    distant_locations = [loc for loc in LOCATIONS if loc["city"] != last_loc["city"]
                         and loc["country"] != last_loc["country"]]
    if not distant_locations:
        distant_locations = [loc for loc in LOCATIONS if loc["city"] != last_loc["city"]]

    new_location = random.choice(distant_locations)
    category = random.choice(list(MERCHANT_CATEGORIES.keys()))
    merchant_name = random.choice(MERCHANT_CATEGORIES[category])

    tx = {
        "transaction_id": f"tx-{uuid.uuid4().hex[:8]}-{random.randint(1000, 9999)}",
        "account_id": account["account_id"],
        "card_number_masked": account["card_number_masked"],
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "amount": round(random.uniform(100, 5000), 2),
        "currency": "USD",
        "transaction_type": "POS_PURCHASE",
        "merchant": {
            "merchant_id": f"m-{random.randint(1000, 9999)}",
            "name": merchant_name,
            "category": category,
        },
        "location": {
            "city": new_location["city"],
            "country": new_location["country"],
            "lat": round(new_location["lat"] + random.uniform(-0.02, 0.02), 4),
            "lon": round(new_location["lon"] + random.uniform(-0.02, 0.02), 4),
        },
        "device_id": account["device_id"],
    }

    account_last_tx[account["account_id"]] = {
        "timestamp": tx["timestamp"],
        "location": new_location,
    }

    return tx


def inject_velocity_spike_fraud() -> list[dict]:
    """Inject a burst of 3-5 rapid transactions from the same account (velocity spike)."""
    account = random.choice(ACCOUNTS)
    burst_count = random.randint(3, 5)
    transactions = []

    for _ in range(burst_count):
        category = random.choice(list(MERCHANT_CATEGORIES.keys()))
        merchant_name = random.choice(MERCHANT_CATEGORIES[category])
        location = account["home_location"]

        tx = {
            "transaction_id": f"tx-{uuid.uuid4().hex[:8]}-{random.randint(1000, 9999)}",
            "account_id": account["account_id"],
            "card_number_masked": account["card_number_masked"],
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            "amount": round(random.uniform(200, 3000), 2),
            "currency": "USD",
            "transaction_type": random.choice(["POS_PURCHASE", "ONLINE_TRANSFER", "MOBILE_PAYMENT"]),
            "merchant": {
                "merchant_id": f"m-{random.randint(1000, 9999)}",
                "name": merchant_name,
                "category": category,
            },
            "location": {
                "city": location["city"],
                "country": location["country"],
                "lat": round(location["lat"] + random.uniform(-0.05, 0.05), 4),
                "lon": round(location["lon"] + random.uniform(-0.05, 0.05), 4),
            },
            "device_id": account["device_id"],
        }
        transactions.append(tx)

    return transactions


def generate_transaction() -> list[dict]:
    """Generate one or more transactions, with ~8% chance of fraud injection."""
    roll = random.random()

    if roll < 0.03:
        # 3% chance: high amount fraud
        return [inject_high_amount_fraud()]
    elif roll < 0.06:
        # 3% chance: geographic hop fraud
        return [inject_geo_hop_fraud()]
    elif roll < 0.08:
        # 2% chance: velocity spike (burst of 3-5 transactions)
        return inject_velocity_spike_fraud()
    else:
        # 92% chance: normal transaction
        return [generate_normal_transaction()]


# ---------------------------------------------------------------------------
# Kafka Producer
# ---------------------------------------------------------------------------
def delivery_report(err, msg):
    """Callback for Kafka delivery confirmation."""
    if err is not None:
        print(f"  ❌ Delivery failed: {err}")
    # Uncomment below for verbose per-message logging:
    # else:
    #     print(f"  ✅ Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")


def create_producer(bootstrap_server: str) -> Producer:
    """Create and return a Kafka producer instance."""
    config = {
        "bootstrap.servers": bootstrap_server,
        "client.id": "banking-tx-producer",
        "acks": "all",
        "retries": 3,
        "retry.backoff.ms": 500,
        "linger.ms": 10,          # Small batch window for throughput
        "batch.size": 32768,      # 32 KB batch size
        "compression.type": "lz4",
    }
    return Producer(config)


def run_producer(bootstrap_server: str, rate: int, max_messages: int | None):
    """Main producer loop."""
    producer = create_producer(bootstrap_server)
    interval = 1.0 / rate  # Seconds between each transaction
    total_sent = 0
    total_fraud = 0

    # Graceful shutdown on Ctrl+C
    shutdown = False

    def signal_handler(sig, frame):
        nonlocal shutdown
        shutdown = True
        print("\n🛑 Shutting down gracefully...")

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    print("=" * 60)
    print("🏦 Banking Transaction Producer")
    print("=" * 60)
    print(f"  Kafka Broker:  {bootstrap_server}")
    print(f"  Topic:         {TOPIC}")
    print(f"  Target Rate:   ~{rate} msg/sec")
    print(f"  Max Messages:  {'unlimited' if max_messages is None else max_messages}")
    print(f"  Fraud Rate:    ~8% (high amount / geo hop / velocity spike)")
    print(f"  Account Pool:  {NUM_ACCOUNTS} accounts")
    print("=" * 60)
    print("  Press Ctrl+C to stop.\n")

    start_time = time.time()

    try:
        while not shutdown:
            if max_messages is not None and total_sent >= max_messages:
                print(f"\n✅ Reached max messages ({max_messages}). Stopping.")
                break

            transactions = generate_transaction()

            for tx in transactions:
                if shutdown:
                    break

                # Use account_id as key for partition affinity
                key = tx["account_id"]
                value = json.dumps(tx)

                producer.produce(
                    topic=TOPIC,
                    key=key,
                    value=value,
                    callback=delivery_report,
                )

                total_sent += 1
                is_fraud = tx["amount"] > 10_000 or len(transactions) > 2
                if is_fraud:
                    total_fraud += 1

                # Progress log every 100 messages
                if total_sent % 100 == 0:
                    elapsed = time.time() - start_time
                    actual_rate = total_sent / elapsed if elapsed > 0 else 0
                    fraud_pct = (total_fraud / total_sent * 100) if total_sent > 0 else 0
                    print(
                        f"  📊 Sent: {total_sent:,} | "
                        f"Rate: {actual_rate:.1f} msg/s | "
                        f"Fraud: {total_fraud} ({fraud_pct:.1f}%) | "
                        f"Elapsed: {elapsed:.0f}s"
                    )

                # Flush periodically to avoid buffer overflow
                producer.poll(0)

            time.sleep(interval)

    finally:
        # Flush remaining messages
        remaining = producer.flush(timeout=10)
        elapsed = time.time() - start_time
        fraud_pct = (total_fraud / total_sent * 100) if total_sent > 0 else 0

        print("\n" + "=" * 60)
        print("📋 Producer Summary")
        print("=" * 60)
        print(f"  Total Sent:    {total_sent:,}")
        print(f"  Fraud Injected:{total_fraud:,} ({fraud_pct:.1f}%)")
        print(f"  Elapsed:       {elapsed:.1f}s")
        print(f"  Avg Rate:      {total_sent / elapsed:.1f} msg/s" if elapsed > 0 else "")
        if remaining > 0:
            print(f"  ⚠️  Unflushed:  {remaining} messages")
        print("=" * 60)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Banking Transaction Producer for Kafka",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run on EC2 (INTERNAL listener, default)
  python3 transaction_producer.py

  # Run from laptop (EXTERNAL listener)
  python3 transaction_producer.py --bootstrap-server 15.135.41.136:9092

  # Produce at 25 messages/sec, stop after 1000 messages
  python3 transaction_producer.py --rate 25 --max-messages 1000
        """,
    )
    parser.add_argument(
        "--bootstrap-server",
        default="localhost:29092",
        help="Kafka bootstrap server (default: localhost:29092 for EC2 INTERNAL listener)",
    )
    parser.add_argument(
        "--rate",
        type=int,
        default=10,
        help="Target messages per second (default: 10)",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=None,
        help="Stop after N messages (default: unlimited)",
    )

    args = parser.parse_args()
    run_producer(args.bootstrap_server, args.rate, args.max_messages)


if __name__ == "__main__":
    main()
