import argparse
import json
import os

import joblib
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import GridSearchCV
from xgboost import XGBClassifier

from preprocess import FEATURE_COLUMNS, build_features, load_and_clean, split_with_smote

MODELS_DIR = os.path.join(os.path.dirname(__file__), "../models")
PROCESSED_DIR = os.path.join(os.path.dirname(__file__), "../data/processed")


def train_xgboost(X_train: pd.DataFrame, y_train: pd.Series) -> XGBClassifier:
    param_grid = {
        "n_estimators": [100, 200],
        "max_depth": [4, 6],
        "learning_rate": [0.05, 0.1],
        "subsample": [0.8],
        "colsample_bytree": [0.8],
    }
    base = XGBClassifier(
        eval_metric="logloss",
        random_state=42,
        n_jobs=-1,
    )
    search = GridSearchCV(base, param_grid, scoring="f1", cv=3, n_jobs=-1, refit=True)
    search.fit(X_train, y_train)
    return search.best_estimator_


def train_isolation_forest(X_train: pd.DataFrame) -> IsolationForest:
    model = IsolationForest(
        n_estimators=200,
        contamination=0.002,
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_train)
    return model


def train_logistic_regression(X_train: pd.DataFrame, y_train: pd.Series) -> LogisticRegression:
    model = LogisticRegression(
        max_iter=1000,
        random_state=42,
        n_jobs=-1,
        class_weight="balanced",
    )
    model.fit(X_train, y_train)
    return model


def save_artifacts(xgb, iso, lr, scaler):
    os.makedirs(MODELS_DIR, exist_ok=True)
    joblib.dump(xgb, os.path.join(MODELS_DIR, "xgboost_model.joblib"))
    joblib.dump(iso, os.path.join(MODELS_DIR, "isolation_forest_model.joblib"))
    joblib.dump(lr, os.path.join(MODELS_DIR, "logistic_regression_model.joblib"))
    joblib.dump(scaler, os.path.join(MODELS_DIR, "scaler.joblib"))
    with open(os.path.join(MODELS_DIR, "feature_columns.json"), "w") as f:
        json.dump(FEATURE_COLUMNS, f)


def main(dataset_path: str):
    df = load_and_clean(dataset_path)
    df, scaler = build_features(df)
    X_train, X_test, y_train, y_test = split_with_smote(df)

    xgb_model = train_xgboost(X_train, y_train)
    iso_model = train_isolation_forest(X_train)
    lr_model = train_logistic_regression(X_train, y_train)

    save_artifacts(xgb_model, iso_model, lr_model, scaler)

    os.makedirs(PROCESSED_DIR, exist_ok=True)
    X_test.to_csv(os.path.join(PROCESSED_DIR, "X_test.csv"), index=False)
    y_test.to_csv(os.path.join(PROCESSED_DIR, "y_test.csv"), index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=str, default="../data/raw/creditcard.csv")
    args = parser.parse_args()
    main(args.dataset)
