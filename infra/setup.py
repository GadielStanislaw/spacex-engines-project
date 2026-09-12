"""Create the S3 bucket, DynamoDB table, and SQS queue this project needs.

Run once against local (moto server) while building, and again against real
AWS on demo day by setting AWS_ENV=aws in .env (or as an env var override).
"""
import sys
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.config import AWS_ENV, S3_BUCKET, DYNAMODB_TABLE, SQS_QUEUE, AWS_REGION, boto3_kwargs


def create_bucket():
    s3 = boto3.client("s3", **boto3_kwargs())
    try:
        if AWS_REGION == "us-east-1":
            s3.create_bucket(Bucket=S3_BUCKET)
        else:
            s3.create_bucket(
                Bucket=S3_BUCKET,
                CreateBucketConfiguration={"LocationConstraint": AWS_REGION},
            )
        print(f"[s3] created bucket: {S3_BUCKET}")
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            print(f"[s3] bucket already exists: {S3_BUCKET}")
        else:
            raise


def create_dynamodb_table():
    ddb = boto3.client("dynamodb", **boto3_kwargs())
    try:
        ddb.create_table(
            TableName=DYNAMODB_TABLE,
            KeySchema=[{"AttributeName": "engine_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "engine_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        print(f"[dynamodb] created table: {DYNAMODB_TABLE}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceInUseException":
            print(f"[dynamodb] table already exists: {DYNAMODB_TABLE}")
        else:
            raise


def create_sqs_queue():
    sqs = boto3.client("sqs", **boto3_kwargs())
    try:
        sqs.create_queue(QueueName=SQS_QUEUE)
        print(f"[sqs] created queue: {SQS_QUEUE}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "QueueAlreadyExists":
            print(f"[sqs] queue already exists: {SQS_QUEUE}")
        else:
            raise


if __name__ == "__main__":
    print(f"Setting up resources against AWS_ENV={AWS_ENV}")
    create_bucket()
    create_dynamodb_table()
    create_sqs_queue()
    print("Done.")
