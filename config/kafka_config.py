KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_BOOTSTRAP_SERVERS_INTERNAL = "kafka:29092"

TOPIC_RAW_TRANSACTIONS = "raw_transactions"
TOPIC_ENRICHED_TRANSACTIONS = "enriched_transactions"
TOPIC_FRAUD_ALERTS = "fraud_alerts"
TOPIC_APPROVED_TRANSACTIONS = "approved_transactions"
TOPIC_REVIEW_QUEUE = "review_queue"

PRODUCER_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "acks": "all",
    "retries": 3,
    "batch.size": 16384,
    "linger.ms": 5,
    "compression.type": "lz4",
    "enable.idempotence": True,
}

CONSUMER_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
    "max.poll.interval.ms": 300000,
    "session.timeout.ms": 30000,
}

RISK_THRESHOLD_LOW = 0.60
RISK_THRESHOLD_HIGH = 0.85
