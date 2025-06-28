import json
import os
import signal
import sys

import requests
from confluent_kafka import Consumer, KafkaError, Producer

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka:29092")
SCORING_URL = os.getenv("SCORING_URL", "http://scoring-service:5000/score")
API_KEY = os.getenv("API_KEY", "fraud-detection-api-key")

TOPIC_IN = "enriched_transactions"
TOPIC_APPROVED = "approved_transactions"
TOPIC_REVIEW = "review_queue"
TOPIC_ALERTS = "fraud_alerts"
GROUP_ID = "risk-router"

_running = True


def _handle_signal(signum, frame):
    global _running
    _running = False


def build_consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "max.poll.interval.ms": 300000,
        "session.timeout.ms": 30000,
    })


def build_producer() -> Producer:
    return Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "acks": "all",
        "retries": 3,
        "batch.size": 16384,
        "linger.ms": 2,
        "compression.type": "lz4",
        "enable.idempotence": True,
    })


def _target_topic(risk_tier: str) -> str:
    if risk_tier == "high":
        return TOPIC_ALERTS
    if risk_tier == "medium":
        return TOPIC_REVIEW
    return TOPIC_APPROVED


def run():
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    consumer = build_consumer()
    producer = build_producer()
    consumer.subscribe([TOPIC_IN])
    headers = {"Authorization": f"Bearer {API_KEY}"}

    try:
        while _running:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"Consumer error: {msg.error()}", file=sys.stderr)
                continue

            try:
                txn = json.loads(msg.value().decode("utf-8"))
                resp = requests.post(SCORING_URL, json=txn, headers=headers, timeout=2)
                resp.raise_for_status()
                result = resp.json()

                txn["fraud_score"] = result["fraud_score"]
                txn["risk_tier"] = result["risk_tier"]

                producer.produce(
                    topic=_target_topic(result["risk_tier"]),
                    key=msg.key(),
                    value=json.dumps(txn).encode("utf-8"),
                )
                producer.poll(0)
                consumer.commit(msg)

            except Exception as exc:
                print(f"Routing error: {exc}", file=sys.stderr)

    finally:
        producer.flush(timeout=30)
        consumer.close()


if __name__ == "__main__":
    run()
