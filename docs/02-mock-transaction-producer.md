# Phase 2: Mock Transaction Producer

This guide walks you through setting up and running a Python-based Kafka producer that generates realistic synthetic banking transactions with intentional fraud anomalies.

---

## Overview

The producer script generates transactions matching the schema defined in `README.md` and streams them to the `banking.transactions` Kafka topic at a configurable rate.

**Fraud injection (~8% of all transactions):**

| Fraud Type | % | Description |
|---|---|---|
| **High Amount** | ~3% | Transactions > $10,000 (wire transfers, ATM withdrawals) |
| **Geographic Hop** | ~3% | Same account transacts in two distant cities (e.g., New York → Tokyo) within seconds |
| **Velocity Spike** | ~2% | Burst of 3–5 rapid transactions from the same account |

---

## Step 1: Install Python Dependencies

SSH into your EC2 instance and install the required packages:

```bash
ssh -i key/kafka-key-pair.pem ubuntu@15.135.41.136
```

```bash
# Install pip if not already available
sudo apt install -y python3-pip python3-venv

# Create a virtual environment for the producer
cd ~
python3 -m venv kafka-producer-env
source kafka-producer-env/bin/activate

# Install dependencies
pip install confluent-kafka faker
```

---

## Step 2: Upload the Producer Script to EC2

From your **local machine** (not EC2), run:

```bash
scp -i key/kafka-key-pair.pem \
  banking-transaction-streaming/producer/transaction_producer.py \
  ubuntu@15.135.41.136:~/transaction_producer.py
```

Or, you can copy-paste the script contents directly on EC2:

```bash
nano ~/transaction_producer.py
# Paste the contents of producer/transaction_producer.py
# Save with Ctrl + O -> Enter -> Ctrl + X
```

---

## Step 3: Run the Producer

Activate the virtual environment and start producing:

```bash
# Activate the virtual environment
source ~/kafka-producer-env/bin/activate

# Run the producer at 10 messages/sec (default)
python3 ~/transaction_producer.py
```

You should see output like:

```
============================================================
🏦 Banking Transaction Producer
============================================================
  Kafka Broker:  localhost:29092
  Topic:         banking.transactions
  Target Rate:   ~10 msg/sec
  Max Messages:  unlimited
  Fraud Rate:    ~8% (high amount / geo hop / velocity spike)
  Account Pool:  100 accounts
============================================================
  Press Ctrl+C to stop.

  📊 Sent: 100 | Rate: 10.1 msg/s | Fraud: 7 (7.0%) | Elapsed: 10s
  📊 Sent: 200 | Rate: 10.0 msg/s | Fraud: 15 (7.5%) | Elapsed: 20s
  📊 Sent: 300 | Rate: 10.0 msg/s | Fraud: 24 (8.0%) | Elapsed: 30s
```

Press **Ctrl+C** to stop gracefully.

---

## Step 4: Producer CLI Options

```bash
# Run at 25 messages/sec
python3 ~/transaction_producer.py --rate 25

# Produce exactly 500 messages then stop
python3 ~/transaction_producer.py --max-messages 500

# Run from your laptop (EXTERNAL listener via Elastic IP)
python3 ~/transaction_producer.py --bootstrap-server 15.135.41.136:9092

# Combine options
python3 ~/transaction_producer.py --rate 50 --max-messages 1000
```

---

## Step 5: Verify with a Consumer

Open a **second SSH terminal** and verify messages are flowing:

```bash
# Quick peek at the latest messages (show last 5 and exit)
~/kafka/bin/kafka-console-consumer.sh \
  --topic banking.transactions \
  --from-beginning \
  --max-messages 5 \
  --bootstrap-server localhost:29092
```

Sample output:

```json
{"transaction_id": "tx-a1b2c3d4-1234", "account_id": "acc-482910", "card_number_masked": "4532-XXXX-XXXX-7821", "timestamp": "2026-08-25T10:00:01.234Z", "amount": 42.50, "currency": "USD", "transaction_type": "POS_PURCHASE", "merchant": {"merchant_id": "m-3391", "name": "Starbucks", "category": "RESTAURANT"}, "location": {"city": "New York", "country": "US", "lat": 40.7152, "lon": -74.0034}, "device_id": "dev-ios-44821"}
```

---

## Step 6: Run as a Background Service (Optional)

To keep the producer running even after you close your SSH session:

```bash
# Run in background with nohup
source ~/kafka-producer-env/bin/activate
nohup python3 ~/transaction_producer.py --rate 10 > ~/producer.log 2>&1 &

# Check the log
tail -f ~/producer.log

# Stop it later
pkill -f transaction_producer.py
```

Or create a systemd service (similar to the Kafka service in Phase 1):

```bash
sudo tee /etc/systemd/system/kafka-producer.service > /dev/null << "EOF_SERVICE"
[Unit]
Description=Banking Transaction Kafka Producer
After=kafka.service

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu
Environment="PATH=/home/ubuntu/kafka-producer-env/bin:/usr/bin"
ExecStart=/home/ubuntu/kafka-producer-env/bin/python3 /home/ubuntu/transaction_producer.py --rate 10
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF_SERVICE

sudo systemctl daemon-reload
sudo systemctl enable --now kafka-producer

# Check status
sudo systemctl status kafka-producer

# View live logs
journalctl -u kafka-producer -f
```

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `ModuleNotFoundError: confluent_kafka` | Virtual env not activated | Run `source ~/kafka-producer-env/bin/activate` |
| `KafkaException: ...broker not available` | Kafka service not running | Run `sudo systemctl start kafka` |
| `Connection refused on localhost:29092` | Wrong port or Kafka crashed | Check `sudo systemctl status kafka` and `ss -tlnp \| grep 29092` |
| `BufferError: Local queue full` | Producing faster than Kafka can handle | Lower `--rate` or increase `linger.ms` in script |

---

## What's Next?

Phase 2 is complete! You now have a continuous stream of realistic banking transactions flowing into Kafka. Next up:

**Phase 3: Databricks Streaming Ingestion (Bronze Layer)**
- Connect Databricks to EC2 Kafka via the External listener (`15.135.41.136:9092`)
- Use `spark.readStream.format("kafka")` to ingest the raw stream
- Persist into a `bronze_transactions` Delta table with checkpointing
