import os

import joblib
import pandas as pd
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)

MODELS_DIR = os.path.join(os.path.dirname(__file__), "../models")
PROCESSED_DIR = os.path.join(os.path.dirname(__file__), "../data/processed")


def load_test_data() -> tuple[pd.DataFrame, pd.Series]:
    X_test = pd.read_csv(os.path.join(PROCESSED_DIR, "X_test.csv"))
    y_test = pd.read_csv(os.path.join(PROCESSED_DIR, "y_test.csv")).squeeze()
    return X_test, y_test


def score_model(name: str, y_true, y_pred, y_prob) -> dict:
    return {
        "model": name,
        "precision": round(precision_score(y_true, y_pred), 4),
        "recall": round(recall_score(y_true, y_pred), 4),
        "f1": round(f1_score(y_true, y_pred), 4),
        "accuracy": round(accuracy_score(y_true, y_pred), 4),
        "roc_auc": round(roc_auc_score(y_true, y_prob), 4),
    }


def main():
    X_test, y_test = load_test_data()

    xgb = joblib.load(os.path.join(MODELS_DIR, "xgboost_model.joblib"))
    iso = joblib.load(os.path.join(MODELS_DIR, "isolation_forest_model.joblib"))
    lr = joblib.load(os.path.join(MODELS_DIR, "logistic_regression_model.joblib"))

    results = [
        score_model(
            "XGBoost",
            y_test,
            xgb.predict(X_test),
            xgb.predict_proba(X_test)[:, 1],
        ),
        score_model(
            "Isolation Forest",
            y_test,
            (iso.predict(X_test) == -1).astype(int),
            -iso.score_samples(X_test),
        ),
        score_model(
            "Logistic Regression",
            y_test,
            lr.predict(X_test),
            lr.predict_proba(X_test)[:, 1],
        ),
    ]

    header = f"\n{'Model':<22} {'Precision':>10} {'Recall':>10} {'F1':>10} {'Accuracy':>10} {'ROC-AUC':>10}"
    print(header)
    print("-" * len(header.strip()))
    for r in results:
        print(
            f"{r['model']:<22} {r['precision']:>10} {r['recall']:>10} "
            f"{r['f1']:>10} {r['accuracy']:>10} {r['roc_auc']:>10}"
        )


if __name__ == "__main__":
    main()
