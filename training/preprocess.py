import pandas as pd
from imblearn.over_sampling import SMOTE
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import RobustScaler

FEATURE_COLUMNS = [f"V{i}" for i in range(1, 29)] + ["normalized_amount", "hour_of_day"]
TARGET_COLUMN = "Class"


def load_and_clean(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    return df.drop_duplicates().reset_index(drop=True)


def build_features(df: pd.DataFrame) -> tuple[pd.DataFrame, RobustScaler]:
    df = df.copy()
    scaler = RobustScaler()
    df["normalized_amount"] = scaler.fit_transform(df[["Amount"]])
    df["hour_of_day"] = (df["Time"] % 86400 / 3600).astype(int)
    return df.drop(columns=["Time", "Amount"]), scaler


def split_with_smote(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
    X = df[FEATURE_COLUMNS]
    y = df[TARGET_COLUMN]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    X_train_bal, y_train_bal = SMOTE(random_state=42).fit_resample(X_train, y_train)
    return X_train_bal, X_test, y_train_bal, y_test
