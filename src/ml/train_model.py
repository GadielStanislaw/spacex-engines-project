"""Train a launch-success classifier from the curated batch data.

Target: `success` (bool). Features: engine specs (thrust, Isp, engine count,
propellant type) + launch year -- deliberately NOT `agency`, since agency
almost 1:1 determines rocket_family in this dataset (SpaceX -> Falcon 9,
NASA -> Space Shuttle, etc.), so including it would let the model cheat by
re-identifying the rocket family instead of learning from engine performance.

Note on what this can and can't show: engine specs are constant within a
rocket family (all Falcon 9 launches have identical thrust/Isp), so the model
is really learning "which family/era is more reliable," not a continuous
engine-performance relationship -- an honest limitation given only 8 distinct
engine families in the data, worth saying in the presentation rather than
overselling.

This is a plain script by design: trains in seconds on ~900 rows, no benefit
from SageMaker or Lambda packaging. Runs identically on Floci or real AWS via
the usual AWS_ENV toggle in .env -- the same file is what would run on the
EC2 instance on demo day.

Usage: python -m src.ml.train_model
"""
import io
import json
import pickle
import sys
from pathlib import Path

import boto3
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, average_precision_score, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.config import S3_BUCKET, boto3_kwargs

NUMERIC_FEATURES = ["thrust_sl_kN", "thrust_vac_kN", "isp_sl_s", "isp_vac_s", "engine_count", "year"]
CATEGORICAL_FEATURES = ["propellant_type"]


def read_parquet_from_s3(s3, key: str) -> pd.DataFrame:
    body = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
    return pd.read_parquet(io.BytesIO(body))


def build_pipeline(model) -> Pipeline:
    preprocessor = ColumnTransformer([
        ("num", StandardScaler(), NUMERIC_FEATURES),
        ("cat", OneHotEncoder(handle_unknown="ignore"), CATEGORICAL_FEATURES),
    ])
    return Pipeline([("preprocess", preprocessor), ("model", model)])


def evaluate(name: str, pipeline: Pipeline, X_test, y_test) -> dict:
    y_pred = pipeline.predict(X_test)
    proba_success = pipeline.predict_proba(X_test)[:, 1]

    report = classification_report(y_test, y_pred, output_dict=True, zero_division=0)
    roc_auc = roc_auc_score(y_test, proba_success)  # symmetric: same value regardless of which class is "positive"

    # PR-AUC against the SUCCESS class is inflated by base rate alone (97.4%
    # success) and is not the useful number here -- what matters is whether
    # the model can rank the rare FAILURE case, so compute PR-AUC treating
    # failure as the positive class explicitly.
    pr_auc_failure = average_precision_score((y_test == 0).astype(int), 1 - proba_success)

    print(f"\n=== {name} ===")
    print(classification_report(y_test, y_pred, zero_division=0))
    print(f"ROC-AUC: {roc_auc:.3f}  PR-AUC (failure class): {pr_auc_failure:.3f}")

    return {"classification_report": report, "roc_auc": roc_auc, "pr_auc_failure": pr_auc_failure}


def main():
    s3 = boto3.client("s3", **boto3_kwargs())

    engines_df = read_parquet_from_s3(s3, "curated/engines.parquet")
    launches_df = read_parquet_from_s3(s3, "curated/launches.parquet")
    df = launches_df.merge(engines_df, on="rocket_family", how="inner")
    print(f"Training data: {len(df)} rows, success rate = {df['success'].mean():.3f}")

    X = df[NUMERIC_FEATURES + CATEGORICAL_FEATURES]
    y = df["success"].astype(int)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.25, random_state=42, stratify=y
    )

    # No class_weight="balanced": with only ~23 failures in 927 launches, it
    # overcorrects hard (verified -- it collapsed logistic regression's
    # failure-class precision to 0.03). PR-AUC/ROC-AUC below are the
    # threshold-independent metrics that actually matter under this much
    # imbalance; the classification_report at the natural 0.5 threshold is
    # secondary context, not the headline number.
    models = {
        "logistic_regression": build_pipeline(LogisticRegression(max_iter=1000)),
        "random_forest": build_pipeline(RandomForestClassifier(n_estimators=200, random_state=42)),
    }

    metrics = {}
    best_name, best_pipeline, best_roc_auc = None, None, -1
    for name, pipeline in models.items():
        pipeline.fit(X_train, y_train)
        metrics[name] = evaluate(name, pipeline, X_test, y_test)
        if metrics[name]["roc_auc"] > best_roc_auc:
            best_name, best_pipeline, best_roc_auc = name, pipeline, metrics[name]["roc_auc"]

    # Feature importance from the Random Forest, for the dashboard/presentation
    rf_model = models["random_forest"].named_steps["model"]
    feature_names = (
        NUMERIC_FEATURES
        + list(models["random_forest"].named_steps["preprocess"]
               .named_transformers_["cat"].get_feature_names_out(CATEGORICAL_FEATURES))
    )
    importances = sorted(zip(feature_names, rf_model.feature_importances_), key=lambda x: -x[1])
    metrics["feature_importance"] = [{"feature": f, "importance": float(i)} for f, i in importances]
    metrics["best_model"] = best_name

    print(f"\nBest model by ROC-AUC: {best_name} ({best_roc_auc:.3f})")
    print("Feature importance (Random Forest):")
    for f, i in importances:
        print(f"  {f}: {i:.3f}")

    s3.put_object(Bucket=S3_BUCKET, Key="models/model.pkl", Body=pickle.dumps(best_pipeline))
    s3.put_object(Bucket=S3_BUCKET, Key="models/metrics.json", Body=json.dumps(metrics, indent=2).encode("utf-8"))
    print(f"\nSaved model + metrics to s3://{S3_BUCKET}/models/")


if __name__ == "__main__":
    main()
