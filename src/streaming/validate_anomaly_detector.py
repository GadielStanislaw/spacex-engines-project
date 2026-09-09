"""Validates the Lambda consumer's z-score anomaly detector against ground
truth: producer.py records exactly which (engine_id, timestamp) readings it
deliberately faked, and consumer.py now archives its own anomaly_flag
decision alongside each raw reading in S3 -- this joins the two and reports
precision/recall/F1.

Usage: python -m src.streaming.validate_anomaly_detector
"""
import json
import sys
from pathlib import Path

import boto3
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.config import S3_BUCKET, boto3_kwargs


def load_json_objects(s3, prefix: str) -> list:
    paginator = s3.get_paginator("list_objects_v2")
    objects = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            body = s3.get_object(Bucket=S3_BUCKET, Key=obj["Key"])["Body"].read()
            objects.append((obj["Key"], json.loads(body)))
    return objects


def main():
    s3 = boto3.client("s3", **boto3_kwargs())

    print("Loading ground truth (producer.py's injected-anomaly labels)...")
    ground_truth_rows = []
    for _, records in load_json_objects(s3, "raw/telemetry_ground_truth/"):
        ground_truth_rows.extend(records)
    truth_df = pd.DataFrame(ground_truth_rows)
    print(f"  {len(truth_df)} ground-truth labels, {truth_df['is_anomaly'].sum()} true anomalies")

    print("Loading detector decisions (consumer.py's archived anomaly_flag per reading)...")
    decision_rows = []
    for _, records in load_json_objects(s3, "raw/telemetry/"):
        decision_rows.extend(records)
    decisions_df = pd.DataFrame(decision_rows)
    # Older archived batches (from before anomaly_flag was added to the
    # archive, or any other malformed record) won't have this column set --
    # drop them rather than let a mixed-dtype column silently break the `~`
    # boolean logic below (verified: object dtype from a bool/NaN mix makes
    # `~` misbehave without raising, since NaN has no real __invert__).
    decisions_df = decisions_df.dropna(subset=["anomaly_flag"])
    decisions_df["anomaly_flag"] = decisions_df["anomaly_flag"].astype(bool)
    print(f"  {len(decisions_df)} processed readings with a valid anomaly_flag")

    merged = truth_df.merge(decisions_df[["engine_id", "timestamp", "anomaly_flag"]],
                             on=["engine_id", "timestamp"], how="inner")
    print(f"  {len(merged)} readings matched between ground truth and processed decisions")

    tp = ((merged["is_anomaly"]) & (merged["anomaly_flag"])).sum()
    fp = ((~merged["is_anomaly"]) & (merged["anomaly_flag"])).sum()
    fn = ((merged["is_anomaly"]) & (~merged["anomaly_flag"])).sum()
    tn = ((~merged["is_anomaly"]) & (~merged["anomaly_flag"])).sum()

    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0

    print("\n=== Anomaly detector validation ===")
    print(f"True positives:  {tp}")
    print(f"False positives: {fp}")
    print(f"False negatives: {fn}")
    print(f"True negatives:  {tn}")
    print(f"Precision: {precision:.3f}")
    print(f"Recall:    {recall:.3f}")
    print(f"F1:        {f1:.3f}")

    missed = merged[(merged["is_anomaly"]) & (~merged["anomaly_flag"])]
    if len(missed):
        print(f"\nMissed anomalies (false negatives), by injected field:")
        print(missed["anomaly_field"].value_counts().to_string())


if __name__ == "__main__":
    main()
