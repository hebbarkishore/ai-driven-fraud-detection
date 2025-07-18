import argparse
import json
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_RAW_TRANSACTIONS = "raw_transactions"

MERCHANT_CATEGORIES = [
    "grocery", "electronics", "travel", "dining", "healthcare",
    "entertainment", "fuel", "clothing", "online_retail", "utilities",
]

DEVICES = ["mobile_ios", "mobile_android", "web_chrome", "web_safari", "pos_terminal"]

LOCATIONS = [
    "New York, US", "Los Angeles, US", "Chicago, US", "Houston, US",
    "Atlanta, US", "San Francisco, US", "London, UK", "Toronto, CA",
    "Mumbai, IN", "Singapore, SG",
]

FRAUD_INJECTION_RATE = 0.002


def build_producer(bootstrap_servers: str) -> Producer:
    return Producer({
        "bootstrap.servers": bootstrap_servers,
        "acks": "all",
        "retries": 3,
        "batch.size": 16384,
        "linger.ms": 5,
        "compression.type": "lz4",
        "enable.idempotence": True,
    })


def delivery_callback(err, msg):
    if err:
        print(f"Delivery failed [{msg.key()}]: {err}", file=sys.stderr)


def generate_synthetic_transaction(user_pool: list[str]) -> dict:
    is_fraud = random.random() < FRAUD_INJECTION_RATE

    if is_fraud:
        amount = round(random.uniform(800.0, 5000.0), 2)
        merchant_category = random.choice(["electronics", "online_retail", "travel"])
        location = random.choice(LOCATIONS[-3:])
    else:
        amount = round(min(random.expovariate(1 / 75.0), 2000.0), 2)
        merchant_category = random.choice(MERCHANT_CATEGORIES)
        location = random.choice(LOCATIONS)

    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": random.choice(user_pool),
        "amount": amount,
        "merchant_category": merchant_category,
        "location": location,
        "device": random.choice(DEVICES),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "currency": "USD",
        "is_fraud_label": int(is_fraud),
    }


def ensure_topic_exists(bootstrap_servers: str, topic: str, partitions: int = 6):
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    metadata = admin.list_topics(timeout=5)
    if topic not in metadata.topics:
        futures = admin.create_topics(
            [NewTopic(topic, num_partitions=partitions, replication_factor=1)]
        )
        for _, f in futures.items():
            try:
                f.result()
            except Exception:
                pass


def run_synthetic(bootstrap_servers: str, tps: int, duration_seconds: Optional[int]):
    producer = build_producer(bootstrap_servers)
    user_pool = [f"user_{i:05d}" for i in range(1, 5001)]
    ensure_topic_exists(bootstrap_servers, TOPIC_RAW_TRANSACTIONS)

    sleep_interval = 1.0 / tps
    total_sent = 0
    start_time = time.time()

    try:
        while True:
            if duration_seconds and (time.time() - start_time) >= duration_seconds:
                break

            txn = generate_synthetic_transaction(user_pool)
            producer.produce(
                topic=TOPIC_RAW_TRANSACTIONS,
                key=txn["user_id"].encode("utf-8"),
                value=json.dumps(txn).encode("utf-8"),
                callback=delivery_callback,
            )
            total_sent += 1
            producer.poll(0)
            time.sleep(sleep_interval)

    except KeyboardInterrupt:
        pass
    finally:
        producer.flush(timeout=30)


def run_replay(bootstrap_servers: str, csv_file: str, tps: int):
    try:
        import pandas as pd
    except ImportError:
        print("pandas is required for replay mode: pip install pandas", file=sys.stderr)
        sys.exit(1)

    df = pd.read_csv(csv_file)
    producer = build_producer(bootstrap_servers)
    ensure_topic_exists(bootstrap_servers, TOPIC_RAW_TRANSACTIONS)

    sleep_interval = 1.0 / tps
    user_pool = [f"user_{i:05d}" for i in range(1, 1001)]

    try:
        for _, row in df.iterrows():
            txn = {
                "transaction_id": str(uuid.uuid4()),
                "user_id": random.choice(user_pool),
                "amount": float(row["Amount"]),
                "merchant_category": random.choice(MERCHANT_CATEGORIES),
                "location": random.choice(LOCATIONS),
                "device": random.choice(DEVICES),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "currency": "USD",
                "is_fraud_label": int(row["Class"]),
                "pca_features": [float(row.get(f"V{i}", 0.0)) for i in range(1, 29)],
            }
            producer.produce(
                topic=TOPIC_RAW_TRANSACTIONS,
                key=txn["user_id"].encode("utf-8"),
                value=json.dumps(txn).encode("utf-8"),
                callback=delivery_callback,
            )
            producer.poll(0)
            time.sleep(sleep_interval)

    except KeyboardInterrupt:
        pass
    finally:
        producer.flush(timeout=30)


def parse_args():
    parser = argparse.ArgumentParser(description="Fraud Detection — Kafka Transaction Producer")
    parser.add_argument("--mode", choices=["synthetic", "replay"], default="synthetic")
    parser.add_argument("--tps", type=int, default=100)
    parser.add_argument("--duration", type=int, default=None)
    parser.add_argument("--file", type=str, default="../data/raw/creditcard.csv")
    parser.add_argument("--bootstrap-servers", type=str, default=BOOTSTRAP_SERVERS)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.mode == "synthetic":
        run_synthetic(args.bootstrap_servers, args.tps, args.duration)
    else:
        run_replay(args.bootstrap_servers, args.file, args.tps)
