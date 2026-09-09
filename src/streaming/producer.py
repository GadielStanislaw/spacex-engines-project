"""Streaming leg producer: simulates live engine telemetry and pushes it to
Kinesis, one record per engine per tick.

Baselines are derived from real specs in engine_specs.py (not arbitrary):
  - thrust: the engine's real sea-level thrust
  - fuel_flow: computed from the rocket equation (thrust / (isp * g0)) --
    real physics, not a guess
  - chamber_pressure / temperature / vibration: plausible values scaled off
    thrust and propellant type, since no free source publishes these at this
    granularity

Occasionally injects a deliberate anomaly (spike/drop in one field) so the
Lambda consumer's z-score detector has something real to catch. Every
injected anomaly's (engine_id, timestamp, field) is recorded and written to
S3 at the end of the run as ground truth, so validate_anomaly_detector.py can
later measure the detector's actual precision/recall against it.

Usage: python -m src.streaming.producer [--ticks N] [--interval SECONDS]
"""
import argparse
import json
import random
import sys
import time
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.config import KINESIS_STREAM, S3_BUCKET, boto3_kwargs
from src.batch.engine_specs import ENGINE_SPECS

G0 = 9.80665  # standard gravity, m/s^2
ANOMALY_PROBABILITY = 0.03


def baseline_for(spec: dict) -> dict:
    thrust_kN = spec["thrust_sl_kN"]
    isp_s = spec["isp_sl_s"]
    fuel_flow_kg_s = (thrust_kN * 1000) / (isp_s * G0)  # rocket equation: F = isp * g0 * mdot

    is_cryo = "LH2" in spec["propellant_type"]
    return {
        "thrust": thrust_kN,
        "fuel_flow": fuel_flow_kg_s,
        "chamber_pressure": thrust_kN / 8.0,  # plausible bar-scale figure, proportional to thrust
        "temperature": 3300 if is_cryo else 3650,  # K, typical combustion temps by propellant
        "vibration": 5.0,
    }


def make_reading(engine_id: str, baseline: dict, force_anomaly: bool) -> tuple:
    reading = {"engine_id": engine_id, "timestamp": time.time()}
    anomaly_field = random.choice(list(baseline.keys())) if force_anomaly else None

    for field, base_value in baseline.items():
        noise = random.gauss(0, 0.02 * base_value)  # ~2% normal sensor noise
        value = base_value + noise
        if field == anomaly_field:
            value *= random.choice([1.3, 1.4, 0.6, 0.7])  # simulated fault: spike or drop
        reading[field] = round(value, 4)

    return reading, anomaly_field


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ticks", type=int, default=60, help="number of readings per engine (default 60)")
    parser.add_argument("--interval", type=float, default=1.0, help="seconds between ticks (default 1.0)")
    args = parser.parse_args()

    kinesis = boto3.client("kinesis", **boto3_kwargs())
    s3 = boto3.client("s3", **boto3_kwargs())
    baselines = {spec["rocket_family"]: baseline_for(spec) for spec in ENGINE_SPECS}

    ground_truth = []  # every record's true label, not just the anomalous ones

    print(f"Producing {args.ticks} ticks for {len(baselines)} engines to stream '{KINESIS_STREAM}'...")
    for tick in range(args.ticks):
        for engine_id, baseline in baselines.items():
            force_anomaly = random.random() < ANOMALY_PROBABILITY
            reading, anomaly_field = make_reading(engine_id, baseline, force_anomaly)

            kinesis.put_record(
                StreamName=KINESIS_STREAM,
                Data=json.dumps(reading).encode("utf-8"),
                PartitionKey=engine_id,
            )
            ground_truth.append({
                "engine_id": engine_id,
                "timestamp": reading["timestamp"],
                "is_anomaly": anomaly_field is not None,
                "anomaly_field": anomaly_field,
            })
            if force_anomaly:
                print(f"  [tick {tick}] injected anomaly for {engine_id}: {reading}")

        time.sleep(args.interval)

    run_id = int(time.time() * 1000)
    key = f"raw/telemetry_ground_truth/{run_id}.json"
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=json.dumps(ground_truth).encode("utf-8"))
    print(f"Ground truth for this run -> s3://{S3_BUCKET}/{key}")
    print("Producer done.")


if __name__ == "__main__":
    main()
