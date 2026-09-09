"""Batch ingestion: pull real launch history from Launch Library 2 (one search
per known rocket family) plus the static engine specs reference table, and
land both as raw JSON in S3.

Usage: python -m src.batch.ingest_batch
"""
import json
import sys
import time
from pathlib import Path

import boto3
import requests
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.config import S3_BUCKET, boto3_kwargs
from src.batch.engine_specs import ENGINE_SPECS

LL2_BASE = "https://ll.thespacedevs.com/2.2.0/launch/"
PAGE_LIMIT = 100
MAX_PAGES_PER_FAMILY = 1  # 1 page = up to 100 launches/family; plenty, and keeps us well under the free-tier rate limit


def slugify(name: str) -> str:
    return name.lower().replace(" ", "-")


def s3_key_exists(s3, key: str) -> bool:
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=key)
        return True
    except ClientError:
        return False


def fetch_launches_for_family(family: str) -> list:
    """Pull launches for one rocket family, following pagination up to a cap.
    Honors the API's Retry-After header on 429 instead of hammering it."""
    results = []
    url = LL2_BASE
    params = {"search": family, "limit": PAGE_LIMIT}
    for page in range(MAX_PAGES_PER_FAMILY):
        while True:
            resp = requests.get(url, params=params if page == 0 else None, timeout=30)
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 60))
                print(f"    rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            break
        data = resp.json()
        results.extend(data["results"])
        url = data.get("next")
        if not url:
            break
        params = None  # `next` is already a full URL with query params
        time.sleep(1)  # be polite to the free API
    return results


def main():
    s3 = boto3.client("s3", **boto3_kwargs())

    print(f"Uploading engine_specs reference table to s3://{S3_BUCKET}/raw/engine_specs.json")
    s3.put_object(
        Bucket=S3_BUCKET,
        Key="raw/engine_specs.json",
        Body=json.dumps(ENGINE_SPECS, indent=2).encode("utf-8"),
    )

    for spec in ENGINE_SPECS:
        family = spec["rocket_family"]
        key = f"raw/launchlibrary2/{slugify(family)}.json"

        if s3_key_exists(s3, key):
            print(f"Skipping '{family}' — already ingested at s3://{S3_BUCKET}/{key}")
            continue

        print(f"Fetching launches for '{family}' from Launch Library 2...")
        launches = fetch_launches_for_family(family)
        print(f"  got {len(launches)} launches")

        s3.put_object(Bucket=S3_BUCKET, Key=key, Body=json.dumps(launches).encode("utf-8"))
        print(f"  -> s3://{S3_BUCKET}/{key}")
        time.sleep(1)

    print("Batch ingestion done.")


if __name__ == "__main__":
    main()
