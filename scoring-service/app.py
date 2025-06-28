import os

from flask import Flask, jsonify, request

from scorer import predict

RISK_THRESHOLD_LOW = float(os.getenv("RISK_THRESHOLD_LOW", "0.60"))
RISK_THRESHOLD_HIGH = float(os.getenv("RISK_THRESHOLD_HIGH", "0.85"))
API_KEY = os.getenv("API_KEY", "fraud-detection-api-key")

app = Flask(__name__)


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


@app.route("/score", methods=["POST"])
def score():
    if not _authorized():
        return jsonify({"error": "unauthorized"}), 401

    txn = request.get_json(force=True, silent=True)
    if not txn:
        return jsonify({"error": "invalid payload"}), 400

    fraud_score = predict(txn)
    return jsonify({
        "transaction_id": txn.get("transaction_id"),
        "fraud_score": round(fraud_score, 4),
        "risk_tier": _risk_tier(fraud_score),
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
