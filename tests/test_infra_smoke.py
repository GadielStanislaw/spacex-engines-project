"""Smoke test: confirms S3, DynamoDB, and SQS are actually usable, not just
present. Run after `python infra/setup.py` and before any real ingestion.

Usage: python tests/test_infra_smoke.py
"""
import sys
import time
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.config import S3_BUCKET, DYNAMODB_TABLE, SQS_QUEUE, boto3_kwargs


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


def check_sqs():
    sqs = boto3.client("sqs", **boto3_kwargs())
    queue_url = sqs.get_queue_url(QueueName=SQS_QUEUE)["QueueUrl"]

    sqs.send_message(QueueUrl=queue_url, MessageBody='{"smoke":"test"}')
    time.sleep(1)

    resp = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=2)
    messages = resp.get("Messages", [])
    assert any('"smoke":"test"' in m["Body"] for m in messages), "SQS message not found"
    for m in messages:
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=m["ReceiptHandle"])
    print("[PASS] SQS: queue reachable, send/receive/delete message OK")


if __name__ == "__main__":
    check_s3()
    check_dynamodb()
    check_sqs()
    print("\nAll infra smoke tests passed. Environment is ready for ingestion.")
