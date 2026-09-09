"""Package and deploy consumer.py as a real Lambda function, triggered by the
Kinesis stream. Same script works against Floci (AWS_ENV=local) and a real
AWS account (AWS_ENV=aws) -- only the .env values change.

Usage: python infra/deploy_lambda.py
"""
import io
import json
import sys
import time
import zipfile
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.config import (
    AWS_ENV, AWS_REGION, S3_BUCKET, DYNAMODB_TABLE,
    KINESIS_STREAM, boto3_kwargs,
)

FUNCTION_NAME = "engine-telemetry-consumer"
ROLE_NAME = "spacex-consumer-lambda-role"
CONSUMER_PATH = Path(__file__).resolve().parent.parent / "src" / "streaming" / "consumer.py"

TRUST_POLICY = {
    "Version": "2012-10-17",
    "Statement": [{"Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}],
}

PERMISSIONS_POLICY = {
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Action": ["logs:*", "dynamodb:*", "s3:*", "kinesis:*"],
        "Resource": "*",
    }],
}


def build_zip() -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(CONSUMER_PATH, arcname="consumer.py")
    return buf.getvalue()


def ensure_role(iam) -> str:
    try:
        resp = iam.create_role(
            RoleName=ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(TRUST_POLICY),
        )
        role_arn = resp["Role"]["Arn"]
        print(f"[iam] created role: {ROLE_NAME}")
    except ClientError as e:
        if e.response["Error"]["Code"] != "EntityAlreadyExists":
            raise
        role_arn = iam.get_role(RoleName=ROLE_NAME)["Role"]["Arn"]
        print(f"[iam] role already exists: {ROLE_NAME}")

    iam.put_role_policy(
        RoleName=ROLE_NAME,
        PolicyName="spacex-consumer-permissions",
        PolicyDocument=json.dumps(PERMISSIONS_POLICY),
    )
    return role_arn


def env_vars() -> dict:
    # No endpoint override here: Floci auto-injects AWS_ENDPOINT_URL + temp
    # credentials into the Lambda container itself; on real AWS that env var
    # is simply absent, so boto3 falls back to real AWS. See consumer.py.
    return {
        "S3_BUCKET": S3_BUCKET,
        "DYNAMODB_TABLE": DYNAMODB_TABLE,
        "AWS_REGION": AWS_REGION,
    }


def ensure_function(lam, role_arn: str) -> str:
    zip_bytes = build_zip()
    try:
        resp = lam.create_function(
            FunctionName=FUNCTION_NAME,
            Runtime="python3.11",
            Role=role_arn,
            Handler="consumer.handler",
            Code={"ZipFile": zip_bytes},
            Timeout=30,
            MemorySize=256,
            Environment={"Variables": env_vars()},
        )
        print(f"[lambda] created function: {FUNCTION_NAME}")
        return resp["FunctionArn"]
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceConflictException":
            raise
        print(f"[lambda] function already exists, updating code + config: {FUNCTION_NAME}")
        lam.update_function_code(FunctionName=FUNCTION_NAME, ZipFile=zip_bytes)
        time.sleep(1)
        resp = lam.update_function_configuration(
            FunctionName=FUNCTION_NAME, Environment={"Variables": env_vars()}
        )
        return resp["FunctionArn"]


def ensure_event_source_mapping(lam, kinesis):
    stream_arn = kinesis.describe_stream(StreamName=KINESIS_STREAM)["StreamDescription"]["StreamARN"]

    existing = lam.list_event_source_mappings(FunctionName=FUNCTION_NAME)["EventSourceMappings"]
    if any(m["EventSourceArn"] == stream_arn for m in existing):
        print(f"[lambda] event source mapping already exists for {KINESIS_STREAM}")
        return

    lam.create_event_source_mapping(
        EventSourceArn=stream_arn,
        FunctionName=FUNCTION_NAME,
        StartingPosition="LATEST",  # only new records going forward; this is a live feed, not a backlog to replay
        BatchSize=10,
    )
    print(f"[lambda] created event source mapping: {KINESIS_STREAM} -> {FUNCTION_NAME}")


if __name__ == "__main__":
    print(f"Deploying Lambda against AWS_ENV={AWS_ENV}")
    kwargs = boto3_kwargs()
    iam = boto3.client("iam", **kwargs)
    lam = boto3.client("lambda", **kwargs)
    kinesis = boto3.client("kinesis", **kwargs)

    role_arn = ensure_role(iam)
    time.sleep(2)  # role propagation
    ensure_function(lam, role_arn)
    time.sleep(2)  # function must be Active before mapping
    ensure_event_source_mapping(lam, kinesis)
    print("Done.")
