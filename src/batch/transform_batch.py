"""Batch transform: reads raw JSON from S3 (Launch Library 2 launches +
engine_specs.json), cleans/joins/feature-engineers, writes curated Parquet.

Data-quality note: Launch Library 2's `search=` is fuzzy text matching, not
an exact rocket-family filter -- it returns unrelated variants (e.g. our
"Atlas V" search returned only 1960s Atlas D/Agena launches, which use
completely different engines than the real modern RD-180) and the same
launch can appear across multiple per-family raw files. So this script:
  1. merges all raw files and de-duplicates by launch id
  2. matches each launch to a known engine family by its *real*
     rocket.configuration.name, using exact match for most families and a
     "starts with" match only where that's actually correct (Ariane 5 and
     Soyuz sub-variants legitimately share the same base engine family)
  3. drops anything that doesn't cleanly match a known family (this is where
     Atlas V ends up excluded -- see ATLAS_V note below)

Usage: python -m src.batch.transform_batch
"""
import io
import json
import sys
from pathlib import Path

import boto3
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.config import S3_BUCKET, boto3_kwargs
from src.batch.engine_specs import ENGINE_SPECS

RAW_FAMILY_FILES = [
    "falcon-9", "falcon-heavy", "saturn-v", "space-shuttle", "soyuz",
    "atlas-v", "delta-iv-heavy", "ariane-5", "electron", "new-glenn",
]

# Exact match by default; "starts with" only for families where every
# sub-variant genuinely shares the same base engine (verified by inspection,
# see conversation/design doc). Atlas V is intentionally absent: its search
# results only ever matched 1960s Atlas D/Agena launches (Rocketdyne
# LR89/MA-3-era engines), not the real RD-180 Atlas V -- there is no
# legitimate match in this dataset, so it's dropped rather than mislabeled.
STARTSWITH_FAMILIES = {"Soyuz", "Ariane 5"}

STATUS_TO_SUCCESS = {"Success": True, "Failure": False, "Partial Failure": False}


def load_raw_launches(s3) -> list:
    seen_ids = set()
    merged = []
    for slug in RAW_FAMILY_FILES:
        body = s3.get_object(Bucket=S3_BUCKET, Key=f"raw/launchlibrary2/{slug}.json")["Body"].read()
        for item in json.loads(body):
            if item["id"] not in seen_ids:
                seen_ids.add(item["id"])
                merged.append(item)
    return merged


def match_family(config_name: str, known_families: list) -> str | None:
    for family in known_families:
        if family in STARTSWITH_FAMILIES:
            if config_name.startswith(family):
                return family
        elif config_name == family:
            return family
    return None


def build_launches_df(raw_launches: list, known_families: list) -> pd.DataFrame:
    rows = []
    for item in raw_launches:
        success = STATUS_TO_SUCCESS.get(item["status"]["abbrev"])
        if success is None:
            continue  # exclude upcoming/TBD launches -- no known outcome yet

        config_name = item["rocket"]["configuration"]["name"]
        family = match_family(config_name, known_families)
        if family is None:
            continue  # doesn't cleanly match a known engine family -- see module docstring

        mission = item.get("mission") or {}
        rows.append({
            "launch_id": item["id"],
            "date": item["net"],
            "rocket_family": family,
            "rocket_variant": config_name,
            "agency": item["launch_service_provider"]["name"],
            "orbit": (mission.get("orbit") or {}).get("name"),
            "success": success,
        })
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df["year"] = df["date"].dt.year
    return df


def write_parquet(s3, df: pd.DataFrame, key: str):
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=buf.getvalue())
    print(f"  -> s3://{S3_BUCKET}/{key} ({len(df)} rows)")


def main():
    s3 = boto3.client("s3", **boto3_kwargs())

    engines_df = pd.DataFrame(ENGINE_SPECS)
    known_families = engines_df["rocket_family"].tolist()

    print("Loading and de-duplicating raw launches...")
    raw_launches = load_raw_launches(s3)
    print(f"  {len(raw_launches)} unique launches across all raw files")

    launches_df = build_launches_df(raw_launches, known_families)
    print(f"  {len(launches_df)} launches matched to a known engine family")
    print(launches_df["rocket_family"].value_counts())

    matched_families = set(launches_df["rocket_family"].unique())
    unmatched = set(known_families) - matched_families
    if unmatched:
        print(f"  NOTE: no matching launches for: {sorted(unmatched)} (excluded from curated data)")

    print("\nWriting curated Parquet...")
    write_parquet(s3, engines_df, "curated/engines.parquet")
    write_parquet(s3, launches_df, "curated/launches.parquet")
    print("Done.")


if __name__ == "__main__":
    main()
