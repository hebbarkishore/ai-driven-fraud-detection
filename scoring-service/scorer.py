import json
import os

import joblib
import numpy as np

MODELS_DIR = os.getenv("MODELS_DIR", os.path.join(os.path.dirname(__file__), "../models"))

_xgb = None
_scaler = None
_feature_columns = None


def _load():
    global _xgb, _scaler, _feature_columns
    if _xgb is None:
        _xgb = joblib.load(os.path.join(MODELS_DIR, "xgboost_model.joblib"))
        _scaler = joblib.load(os.path.join(MODELS_DIR, "scaler.joblib"))
        with open(os.path.join(MODELS_DIR, "feature_columns.json")) as f:
            _feature_columns = json.load(f)


def _prepare(txn: dict) -> np.ndarray:
    pca = txn.get("pca_features", [0.0] * 28)
    normalized_amount = float(_scaler.transform([[txn["amount"]]])[0][0])
    hour_of_day = int(txn.get("hour_of_day", 0))

    feature_dict = {f"V{i}": float(pca[i - 1]) for i in range(1, 29)}
    feature_dict["normalized_amount"] = normalized_amount
    feature_dict["hour_of_day"] = hour_of_day

    return np.array([[feature_dict[col] for col in _feature_columns]])


def predict(txn: dict) -> float:
    _load()
    return float(_xgb.predict_proba(_prepare(txn))[0][1])
