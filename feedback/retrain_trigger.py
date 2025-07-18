import os
import time

BUFFER_PATH = os.getenv("FEEDBACK_BUFFER_PATH", "/data/feedback/feedback_buffer.jsonl")
MARKER_PATH = os.path.join(os.path.dirname(BUFFER_PATH), "retrain_requested")
RETRAIN_THRESHOLD = int(os.getenv("RETRAIN_THRESHOLD", "500"))
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "300"))


def _buffer_size() -> int:
    if not os.path.exists(BUFFER_PATH):
        return 0
    with open(BUFFER_PATH) as f:
        return sum(1 for _ in f)


def run():
    while True:
        time.sleep(CHECK_INTERVAL)
        size = _buffer_size()
        if size >= RETRAIN_THRESHOLD and not os.path.exists(MARKER_PATH):
            with open(MARKER_PATH, "w") as f:
                f.write(str(size))


if __name__ == "__main__":
    run()
