#!/usr/bin/env bash

set -euo pipefail

BOOTSTRAP="localhost:9092"
REQUIRED_TOPICS=(
    "raw_transactions"
    "enriched_transactions"
    "fraud_alerts"
    "approved_transactions"
    "review_queue"
)

EXISTING=$(docker exec fraud-kafka kafka-topics \
    --bootstrap-server localhost:9092 \
    --list 2>/dev/null) || {
    echo "[ERROR] Could not connect to Kafka. Is docker-compose up?"
    exit 1
}

MISSING=()
for topic in "${REQUIRED_TOPICS[@]}"; do
    if echo "$EXISTING" | grep -q "^${topic}$"; then
        echo "[OK]      $topic"
    else
        echo "[MISSING] $topic"
        MISSING+=("$topic")
    fi
done

if [ ${#MISSING[@]} -ne 0 ]; then
    echo "Missing topics: ${MISSING[*]}"
    exit 1
fi
