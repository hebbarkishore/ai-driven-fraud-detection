import os
import subprocess
import sys
import time

BUFFER_PATH = os.getenv("FEEDBACK_BUFFER_PATH", "/data/feedback/feedback_buffer.jsonl")
DATASET_PATH = os.getenv("DATASET_PATH", "/data/raw/creditcard.csv")
RETRAIN_THRESHOLD = int(os.getenv("RETRAIN_THRESHOLD", "500"))
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "300"))


def _buffer_size() -> int:
    if not os.path.exists(BUFFER_PATH):
        return 0
    with open(BUFFER_PATH) as f:
        return sum(1 for _ in f)


def _retrain() -> bool:
    result = subprocess.run(
        [sys.executable, "/training/train.py", "--dataset", DATASET_PATH],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(result.stderr, file=sys.stderr)
    return result.returncode == 0


def run():
    while True:
        time.sleep(CHECK_INTERVAL)
        if _buffer_size() >= RETRAIN_THRESHOLD:
            if _retrain():
                os.remove(BUFFER_PATH)


if __name__ == "__main__":
    run()
