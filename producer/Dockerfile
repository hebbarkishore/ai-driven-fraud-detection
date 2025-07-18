FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY transaction_producer.py .

ENV BOOTSTRAP_SERVERS=kafka:29092

CMD ["python", "transaction_producer.py", "--mode", "synthetic", "--tps", "500"]
