# Phase 1: AWS EC2 & Apache Kafka Setup (KRaft Mode)

This guide walks you through provisioning an AWS EC2 instance, installing Apache Kafka 4.x (KRaft mode, no ZooKeeper), configuring dual network listeners for both local and external access (Databricks), and creating the banking topics.

---

## Step 1: Launch an AWS EC2 Instance

1. Go to the **AWS Console -> EC2 -> Launch Instance**.
2. **Name**: `kafka-broker-node`
3. **OS Image**: `Ubuntu 24.04 LTS` (or `Ubuntu 22.04 LTS`)
4. **Instance Type**: `t3.medium` (2 vCPU, 4 GB RAM — minimum recommended for Kafka).
   > ⚠️ `t3.micro` (1 GB RAM) is **not enough** — Kafka's default 1 GB heap + console tools will cause OOM crashes.
5. **Key Pair**: Select an existing `.pem` key or create a new one (e.g., `kafka-key.pem`).
6. **Network Settings (Security Group)**:
   Configure Inbound Rules:
   - **SSH (Port 22)**: `My IP` (for your personal SSH access)
   - **Custom TCP (Port 9092)**: `0.0.0.0/0` (or restricted to your IP + Databricks NAT Gateway/IPs)
7. **Storage**: 20-30 GB `gp3`.
8. Click **Launch Instance**.
9. **Allocate an Elastic IP** (EC2 -> Elastic IPs -> Allocate -> Associate to your instance).
   This gives you a **fixed public IP** that won't change when you stop/start the instance.
10. Note down your **Elastic IP Address** (e.g., `15.135.41.136`).

---

## Step 2: Connect via SSH

Open your terminal and run:

```bash
chmod 400 /path/to/kafka-key.pem
ssh -i key/kafka-key-pair.pem ubuntu@15.135.41.136
```

---

## Step 3: Install Java 17

Kafka requires a Java Virtual Machine (JVM). Run:

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y openjdk-17-jdk wget curl

# Verify installation
java -version
```

---

## Step 4: Download & Extract Apache Kafka

Download the latest Kafka binary release (Kafka 4.3.1, built for Scala 2.13):

```bash
cd ~
# Download Kafka 4.3.1
wget https://downloads.apache.org/kafka/4.3.1/kafka_2.13-4.3.1.tgz

# Extract archive
tar -xzf kafka_2.13-4.3.1.tgz
mv kafka_2.13-4.3.1 kafka
cd ~/kafka
```

---

## Step 5: Configure Kafka in KRaft Mode (Dual Listeners)

Open the server configuration file:

```bash
nano config/server.properties
```

Locate and update the **Socket Server Settings** section with dual listeners:

```properties
############################# Server Basics #############################

process.roles=broker,controller
node.id=1
controller.quorum.bootstrap.servers=localhost:9093

############################# Socket Server Settings #############################

# Dual listeners: INTERNAL for local CLI commands, EXTERNAL for outside clients
listeners=INTERNAL://0.0.0.0:29092,EXTERNAL://0.0.0.0:9092,CONTROLLER://localhost:9093

# Inter-broker communication uses the INTERNAL listener
inter.broker.listener.name=INTERNAL

# Advertised addresses sent back to clients:
#   - INTERNAL: localhost (for commands run on this EC2 machine)
#   - EXTERNAL: Elastic IP (for Databricks / external producers)
# Replace 15.135.41.136 with your Elastic IP
advertised.listeners=INTERNAL://localhost:29092,EXTERNAL://15.135.41.136:9092,CONTROLLER://localhost:9093

controller.listener.names=CONTROLLER

# Map custom listener names to security protocols
listener.security.protocol.map=CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT

############################# Log Basics #############################

# Use a persistent directory (NOT /tmp, which gets cleared on reboot)
log.dirs=/home/ubuntu/kafka/kraft-combined-logs

############################# Log Retention Policy #############################

log.retention.hours=168
```

> **Why dual listeners?** AWS EC2 instances cannot route to their own Public IP (NAT loopback).
> Without dual listeners, local CLI tools (e.g., `kafka-topics.sh`) would receive the Public IP
> from Kafka and fail to connect. The `INTERNAL` listener (port `29092`) advertises `localhost`,
> while `EXTERNAL` (port `9092`) advertises the Elastic IP for outside clients.

Save and exit (`Ctrl + O` -> `Enter` -> `Ctrl + X`).

---

## Step 6: Initialize KRaft Cluster Storage

Generate a unique cluster ID and format the storage directory:

```bash
cd ~/kafka

# Generate a random Cluster ID
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
echo "Cluster ID: $KAFKA_CLUSTER_ID"

# Format the storage directory with the Cluster ID
# --standalone is required for Kafka 4.x single-node KRaft (no static voter list)
bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/server.properties --standalone
```

---

## Step 7: Create a Systemd Service (Run in Background)

Create a systemd unit file so Kafka starts automatically and runs reliably:

```bash
sudo tee /etc/systemd/system/kafka.service > /dev/null << "EOF_SERVICE"
[Unit]
Description=Apache Kafka Server (KRaft)
Documentation=https://kafka.apache.org/documentation/
After=network.target

[Service]
Type=simple
User=ubuntu
Environment="JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64"
Environment="KAFKA_HEAP_OPTS=-Xmx512M -Xms512M"
ExecStart=/home/ubuntu/kafka/bin/kafka-server-start.sh /home/ubuntu/kafka/config/server.properties
ExecStop=/home/ubuntu/kafka/bin/kafka-server-stop.sh
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF_SERVICE
```

Reload systemd and start Kafka:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now kafka

# Check service status (should show "active (running)")
sudo systemctl status kafka
```

---

## Step 8: Create Banking Topics

Create the two topics required for our project.

> **Important:** Use `localhost:29092` (the INTERNAL listener) for all commands run directly on the EC2 instance.

```bash
cd ~/kafka

# 1. Main incoming transaction stream topic (3 partitions for parallelism)
bin/kafka-topics.sh --create \
  --topic banking.transactions \
  --bootstrap-server localhost:29092 \
  --partitions 3 \
  --replication-factor 1

# 2. Real-time fraud alerts topic
bin/kafka-topics.sh --create \
  --topic banking.fraud-alerts \
  --bootstrap-server localhost:29092 \
  --partitions 1 \
  --replication-factor 1

# Verify topics created
bin/kafka-topics.sh --list --bootstrap-server localhost:29092
```

---

## Step 9: Smoke Test (Producer & Consumer)

Test producing and consuming messages in **two separate SSH terminal tabs**:

**Terminal Tab 1 (Consumer):**
```bash
~/kafka/bin/kafka-console-consumer.sh \
  --topic banking.transactions \
  --from-beginning \
  --bootstrap-server localhost:29092
```

**Terminal Tab 2 (Producer):**
```bash
~/kafka/bin/kafka-console-producer.sh \
  --topic banking.transactions \
  --bootstrap-server localhost:29092
```

When the `>` prompt appears, type a sample JSON message and press Enter:
```json
{"transaction_id": "test-001", "account_id": "ACC-101", "amount": 100.0, "type": "ATM_WITHDRAWAL"}
```

If the message appears instantly in Tab 1, your Kafka broker is ready for Phase 2!

> **External connectivity test** (from your laptop, not EC2):
> ```bash
> python3 -c "
> import socket
> s = socket.socket(); s.settimeout(20)
> try:
>     s.connect(('15.135.41.136', 9092))
>     print('✅ SUCCESS: Kafka port 9092 is reachable!')
> except Exception as e: print('❌ FAILED:', e)
> finally: s.close()
> "
> ```
