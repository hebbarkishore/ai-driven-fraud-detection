import json
import os
import signal
import sys

from confluent_kafka import Consumer, KafkaError

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka:29092")
BUFFER_PATH = os.getenv("FEEDBACK_BUFFER_PATH", "/data/feedback/feedback_buffer.jsonl")
GROUP_ID = "feedback-consumer"
TOPICS = ["fraud_alerts", "review_queue"]

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


def run():
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    os.makedirs(os.path.dirname(BUFFER_PATH), exist_ok=True)
    consumer = build_consumer()
    consumer.subscribe(TOPICS)

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
                with open(BUFFER_PATH, "a") as f:
                    f.write(json.dumps(txn) + "\n")
                consumer.commit(msg)
            except Exception as exc:
                print(f"Buffer write error: {exc}", file=sys.stderr)

    finally:
        consumer.close()


if __name__ == "__main__":
    run()
