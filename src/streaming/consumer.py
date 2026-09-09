"""Lambda handler for the streaming leg.

Triggered by a Kinesis event source mapping. For each telemetry record:
  - maintains per-engine rolling mean/variance (Welford's online algorithm,
    stored in DynamoDB so state survives across separate invocations)
  - flags an anomaly when a field's z-score vs. its own rolling mean crosses
    a threshold
  - archives the raw batch of records for this invocation to S3

Self-contained on purpose (no imports from the rest of this repo, no
third-party deps beyond boto3, which every Lambda runtime already has) so the
deploy package is just this one file — standard Lambda practice. Config comes
from environment variables set on the function itself (see infra/deploy_lambda.py),
not from .env / dotenv.

No custom endpoint/credential handling here: on Floci, the Lambda execution
container already has `AWS_ENDPOINT_URL` and temporary credentials injected
automatically (botocore reads AWS_ENDPOINT_URL natively) -- and on real AWS,
that variable simply won't be set, so boto3 talks to real AWS by default. Same
code, no branching.
"""
import base64
import json
import os
import time
from decimal import Decimal

import boto3

FIELDS = ["chamber_pressure", "temperature", "vibration", "fuel_flow", "thrust"]
Z_SCORE_THRESHOLD = 3.0
MIN_SAMPLES_BEFORE_FLAGGING = 5  # don't flag anomalies until a baseline exists

S3_BUCKET = os.environ["S3_BUCKET"]
DYNAMODB_TABLE = os.environ["DYNAMODB_TABLE"]

dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")
table = dynamodb.Table(DYNAMODB_TABLE)


def _d(value) -> Decimal:
    return Decimal(str(value))


def update_rolling_stats(engine_id: str, reading: dict) -> dict:
    existing = table.get_item(Key={"engine_id": engine_id}).get("Item", {})
    count = int(existing.get("count", 0)) + 1

    updated = {"engine_id": engine_id, "count": count, "timestamp": _d(reading["timestamp"])}
    anomalous_fields = []

    for field in FIELDS:
        value = float(reading[field])
        prev_mean = float(existing.get(f"{field}_mean", value))
        prev_m2 = float(existing.get(f"{field}_m2", 0.0))

        delta = value - prev_mean
        new_mean = prev_mean + delta / count
        new_m2 = prev_m2 + delta * (value - new_mean)

        variance = new_m2 / count if count > 1 else 0.0
        std = variance ** 0.5
        z_score = (value - prev_mean) / std if std > 0 else 0.0

        updated[field] = _d(round(value, 4))
        updated[f"{field}_mean"] = _d(round(new_mean, 4))
        updated[f"{field}_m2"] = _d(round(new_m2, 4))
        updated[f"{field}_zscore"] = _d(round(z_score, 3))

        if count > MIN_SAMPLES_BEFORE_FLAGGING and abs(z_score) > Z_SCORE_THRESHOLD:
            anomalous_fields.append(field)

    updated["anomaly_flag"] = len(anomalous_fields) > 0
    updated["anomaly_reason"] = ", ".join(anomalous_fields) if anomalous_fields else ""

    table.put_item(Item=updated)
    return updated


def handler(event, context):
    raw_batch = []

    for record in event.get("Records", []):
        payload = base64.b64decode(record["kinesis"]["data"])
        try:
            reading = json.loads(payload)
            engine_id = reading["engine_id"]
            for field in FIELDS:
                float(reading[field])
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as e:
            print(f"[WARN] skipping malformed record: {e}")
            continue

        updated = update_rolling_stats(engine_id, reading)
        # Archive the detector's decision alongside the raw reading (not just
        # the raw input) -- this is what lets validate_anomaly_detector.py
        # later measure precision/recall against producer.py's ground truth.
        reading["anomaly_flag"] = updated["anomaly_flag"]
        reading["anomaly_reason"] = updated["anomaly_reason"]
        raw_batch.append(reading)

    if raw_batch:
        key = f"raw/telemetry/{int(time.time() * 1000)}.json"
        s3.put_object(Bucket=S3_BUCKET, Key=key, Body=json.dumps(raw_batch).encode("utf-8"))

    return {"processed": len(raw_batch)}
