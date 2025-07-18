import json
import os
import signal
import sys

from confluent_kafka import Consumer, KafkaError, Producer

from feature_engine import compute_features
from state_store import WindowedStateStore

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_IN = "raw_transactions"
TOPIC_OUT = "enriched_transactions"
GROUP_ID = "enrichment-processor"

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


def run():
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    consumer = build_consumer()
    producer = build_producer()
    store = WindowedStateStore()

    consumer.subscribe([TOPIC_IN])

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
                enriched = compute_features(txn, store)
                producer.produce(
                    topic=TOPIC_OUT,
                    key=msg.key(),
                    value=json.dumps(enriched).encode("utf-8"),
                )
                producer.poll(0)
                consumer.commit(msg)
            except Exception as exc:
                print(f"Processing error: {exc}", file=sys.stderr)

    finally:
        producer.flush(timeout=30)
        consumer.close()


if __name__ == "__main__":
    run()
