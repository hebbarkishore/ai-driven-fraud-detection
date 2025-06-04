from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Deque


@dataclass
class TransactionRecord:
    timestamp: float
    amount: float
    device: str
    location: str


class WindowedStateStore:
    MAX_WINDOW_SECONDS = 7 * 24 * 3600

    def __init__(self):
        self._store: dict[str, Deque[TransactionRecord]] = defaultdict(deque)

    def add(self, user_id: str, record: TransactionRecord):
        dq = self._store[user_id]
        dq.append(record)
        cutoff = record.timestamp - self.MAX_WINDOW_SECONDS
        while dq and dq[0].timestamp < cutoff:
            dq.popleft()

    def get_window(self, user_id: str, seconds: int) -> list[TransactionRecord]:
        cutoff = datetime.now(timezone.utc).timestamp() - seconds
        return [r for r in self._store[user_id] if r.timestamp >= cutoff]

    def get_last_device(self, user_id: str) -> str | None:
        dq = self._store[user_id]
        if len(dq) < 2:
            return None
        return dq[-2].device
