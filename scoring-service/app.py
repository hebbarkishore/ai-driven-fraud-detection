import os
import time

from flask import Flask, jsonify, request
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Histogram,
    generate_latest,
)

from scorer import predict

RISK_THRESHOLD_LOW = float(os.getenv("RISK_THRESHOLD_LOW", "0.60"))
RISK_THRESHOLD_HIGH = float(os.getenv("RISK_THRESHOLD_HIGH", "0.85"))
API_KEY = os.getenv("API_KEY", "fraud-detection-api-key")

app = Flask(__name__)

scoring_requests = Counter(
    "fraud_scoring_requests_total",
    "Total scoring requests",
    ["risk_tier"],
)

scoring_latency = Histogram(
    "fraud_scoring_latency_seconds",
    "Scoring request latency in seconds",
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5],
)


def _authorized() -> bool:
    auth = request.headers.get("Authorization", "")
    return auth.startswith("Bearer ") and auth[len("Bearer "):] == API_KEY


def _risk_tier(score: float) -> str:
    if score >= RISK_THRESHOLD_HIGH:
        return "high"
    if score >= RISK_THRESHOLD_LOW:
        return "medium"
    return "low"


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


@app.route("/metrics", methods=["GET"])
def metrics():
    return generate_latest(), 200, {"Content-Type": CONTENT_TYPE_LATEST}


@app.route("/score", methods=["POST"])
def score():
    if not _authorized():
        return jsonify({"error": "unauthorized"}), 401

    txn = request.get_json(force=True, silent=True)
    if not txn:
        return jsonify({"error": "invalid payload"}), 400

    start = time.perf_counter()
    fraud_score = predict(txn)
    scoring_latency.observe(time.perf_counter() - start)

    tier = _risk_tier(fraud_score)
    scoring_requests.labels(risk_tier=tier).inc()

    return jsonify({
        "transaction_id": txn.get("transaction_id"),
        "fraud_score": round(fraud_score, 4),
        "risk_tier": tier,
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
