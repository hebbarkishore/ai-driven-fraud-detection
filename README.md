# AI-Driven Real-Time Fraud Detection using Kafka Streams

Real-time fraud detection built on Apache Kafka Streams and XGBoost. Transactions are enriched with behavioral features in-flight, scored by an ML microservice, and routed to one of three risk tiers - all under 250ms end-to-end.

Based on: *"AI-Driven Real-Time Fraud Detection using Kafka Streams in FinTech"* - International Journal of Applied Mathematics, Vol. 38 No. 6s, 2025.

Latest Code release: https://codeberg.org/kishorehebbar/ai-driven-fraud-detection.git

## How it works

```
Transaction Sources
        │
        ▼
  raw_transactions  (Kafka)
        │
        ▼
  Enrichment Processor  ── behavioral + temporal features ──►  enriched_transactions
        │
        ▼
  Risk Router  ──► POST /score (Flask + XGBoost)
        │
        ├── score < 0.60   →  approved_transactions
        ├── score 0.60–0.85 →  review_queue
        └── score ≥ 0.85   →  fraud_alerts
```

## Stack

| Layer | Technology |
|-------|-----------|
| Streaming | Apache Kafka 3.7, confluent-kafka |
| ML | XGBoost 2.0, scikit-learn 1.5, Isolation Forest |
| Serving | Flask 3.0, gunicorn |
| Infra | Docker, Kubernetes |
| Monitoring | Prometheus, Grafana |

Throughput: >500 TPS sustained, peak 1000 TPS. Latency: <250ms ingestion to decision.

## Project layout

```
├── producer/          transaction event generator (synthetic or CSV replay)
├── streams/           Kafka consumer that enriches transactions with 10 features
├── scoring-service/   Flask REST API + Kafka router
├── training/          XGBoost + Isolation Forest training pipeline
├── feedback/          consumes fraud_alerts/review_queue, triggers retraining
├── monitoring/        Prometheus scrape config + Grafana dashboard
├── k8s/               Kubernetes manifests
└── docker-compose.yml full local stack
```

## Running locally

**Prerequisites:** Docker Desktop, Python 3.11+, ~2 GB disk

### 1. Start Kafka

```bash
docker-compose up -d zookeeper kafka kafka-init kafka-ui
```

Wait ~30s, then check topics are ready:

```bash
bash scripts/verify_kafka.sh
```

Kafka UI: http://localhost:8080

### 2. Train the models

Download [creditcard.csv from Kaggle](https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud) into `data/raw/`, then:

```bash
cd training
pip install -r requirements.txt
python train.py --dataset ../data/raw/creditcard.csv
```

Takes ~5 minutes. Saves `xgboost_model.joblib`, `scaler.joblib`, and `feature_columns.json` to `models/`.

To see evaluation metrics:

```bash
python evaluate.py
```

```
Model                  Precision     Recall         F1   Accuracy    ROC-AUC
------------------------------------------------------------------------
XGBoost                   0.9400     0.9200     0.9300     0.9780     0.9870
Isolation Forest          0.8700     0.8100     0.8400     0.9650     0.9260
Logistic Regression       0.8000     0.7800     0.7900     0.9500     0.9030
```

### 3. Start all services

```bash
cd ..
docker-compose up -d
```

### 4. Smoke test the scoring endpoint

```bash
curl -s -X POST http://localhost:5001/score \
  -H "Authorization: Bearer fraud-detection-api-key" \
  -H "Content-Type: application/json" \
  -d '{
    "transaction_id": "test-001",
    "user_id": "user_00001",
    "amount": 4800.00,
    "hour_of_day": 2,
    "pca_features": [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
  }'
```

```json
{"transaction_id": "test-001", "fraud_score": 0.87, "risk_tier": "high"}
```

### 5. Run the producer

```bash
cd producer
pip install -r requirements.txt
python transaction_producer.py --mode synthetic --tps 100
```

Watch messages flowing through topics in Kafka UI. To replay the actual Kaggle dataset instead:

```bash
python transaction_producer.py --mode replay --file ../data/raw/creditcard.csv --tps 500
```

## Monitoring

| Service | URL |
|---------|-----|
| Scoring API | http://localhost:5001 |
| Kafka UI | http://localhost:8080 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 (admin/admin) |

The Grafana *Fraud Detection* dashboard auto-provisions on startup. Useful Prometheus queries:

```
rate(fraud_scoring_requests_total[1m])
histogram_quantile(0.95, sum(rate(fraud_scoring_latency_seconds_bucket[5m])) by (le))
rate(fraud_scoring_requests_total{risk_tier="high"}[5m])
```

## Retraining

The feedback consumer accumulates records from `fraud_alerts` and `review_queue` in a volume. Once 500 records build up, a marker file `retrain_requested` is written. To retrain:

```bash
cd training && python train.py --dataset ../data/raw/creditcard.csv
docker-compose restart scoring-service
```
