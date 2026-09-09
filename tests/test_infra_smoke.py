"""Smoke test: confirms S3, DynamoDB, and Kinesis are actually usable, not just
present. Run after `python infra/setup.py` and before any real ingestion.

Usage: python tests/test_infra_smoke.py
"""
import sys
import time
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.config import S3_BUCKET, DYNAMODB_TABLE, KINESIS_STREAM, boto3_kwargs


def check_s3():
    s3 = boto3.client("s3", **boto3_kwargs())
    key = "smoke_test/hello.txt"
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=b"hello from smoke test")
    body = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
    assert body == b"hello from smoke test", "S3 round-trip mismatch"
    s3.delete_object(Bucket=S3_BUCKET, Key=key)
    print("[PASS] S3: put/get/delete object OK")


def check_dynamodb():
    ddb = boto3.resource("dynamodb", **boto3_kwargs())
    table = ddb.Table(DYNAMODB_TABLE)
    status = table.table_status
    assert status == "ACTIVE", f"table not active: {status}"
    table.put_item(Item={"engine_id": "smoke-test-engine", "value": 42})
    item = table.get_item(Key={"engine_id": "smoke-test-engine"})["Item"]
    assert item["value"] == 42, "DynamoDB round-trip mismatch"
    table.delete_item(Key={"engine_id": "smoke-test-engine"})
    print("[PASS] DynamoDB: table ACTIVE, put/get/delete item OK")


def check_kinesis():
    kinesis = boto3.client("kinesis", **boto3_kwargs())
    desc = kinesis.describe_stream(StreamName=KINESIS_STREAM)["StreamDescription"]
    assert desc["StreamStatus"] == "ACTIVE", f"stream not active: {desc['StreamStatus']}"
    shard_id = desc["Shards"][0]["ShardId"]

    # Get the iterator *before* putting the record (LATEST = only records from
    # this point forward) so this test is correct regardless of how much
    # backlog already sits in the stream from prior runs/producer bursts.
    iterator = kinesis.get_shard_iterator(
        StreamName=KINESIS_STREAM, ShardId=shard_id, ShardIteratorType="LATEST"
    )["ShardIterator"]

    kinesis.put_record(StreamName=KINESIS_STREAM, Data=b'{"smoke":"test"}', PartitionKey="smoke")
    time.sleep(1)

    records = kinesis.get_records(ShardIterator=iterator, Limit=10)["Records"]
    assert any(b'"smoke":"test"' in r["Data"] for r in records), "Kinesis record not found"
    print("[PASS] Kinesis: stream ACTIVE, put/get record OK")


if __name__ == "__main__":
    check_s3()
    check_dynamodb()
    check_kinesis()
    print("\nAll infra smoke tests passed. Environment is ready for ingestion.")
