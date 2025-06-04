from datetime import datetime, timezone

from state_store import TransactionRecord, WindowedStateStore

WINDOW_1H = 3600
WINDOW_24H = 86400
WINDOW_7D = 7 * 86400


def compute_features(txn: dict, store: WindowedStateStore) -> dict:
    user_id = txn["user_id"]
    ts = datetime.fromisoformat(txn["timestamp"]).timestamp()

    w1h = store.get_window(user_id, WINDOW_1H)
    w24h = store.get_window(user_id, WINDOW_24H)
    w7d = store.get_window(user_id, WINDOW_7D)
    last_device = store.get_last_device(user_id)

    store.add(user_id, TransactionRecord(
        timestamp=ts,
        amount=txn["amount"],
        device=txn["device"],
        location=txn["location"],
    ))

    avg_amount_7d = sum(r.amount for r in w7d) / len(w7d) if w7d else txn["amount"]
    amount_deviation = (
        (txn["amount"] - avg_amount_7d) / avg_amount_7d
        if avg_amount_7d > 0 else 0.0
    )

    dt = datetime.fromtimestamp(ts, tz=timezone.utc)

    return {
        **txn,
        "txn_count_1h": len(w1h),
        "txn_count_24h": len(w24h),
        "avg_amount_1h": round(sum(r.amount for r in w1h) / len(w1h), 2) if w1h else txn["amount"],
        "avg_amount_7d": round(avg_amount_7d, 2),
        "amount_deviation": round(amount_deviation, 4),
        "device_switched": int(last_device is not None and last_device != txn["device"]),
        "unique_locations_24h": len({r.location for r in w24h}),
        "hour_of_day": dt.hour,
        "day_of_week": dt.weekday(),
        "is_weekend": int(dt.weekday() >= 5),
    }
