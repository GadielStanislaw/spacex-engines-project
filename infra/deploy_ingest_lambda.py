"""Package and deploy ingest_lambda.py as a real Lambda function (batch
ingestion). Same pattern as deploy_lambda.py (the streaming consumer), but
this one needs `requests` vendored into the zip since Lambda's runtime
doesn't include it by default (unlike boto3, which every runtime has).

Usage: python infra/deploy_ingest_lambda.py
Then invoke manually: ./awslocal.sh lambda invoke --function-name batch-ingest --payload '{}' /tmp/out.json
"""
import io
import json
import shutil
import subprocess
import sys
import tempfile
import time
import zipfile
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.config import AWS_REGION, S3_BUCKET, boto3_kwargs

FUNCTION_NAME = "batch-ingest"
ROLE_NAME = "spacex-consumer-lambda-role"  # reusing the role created by deploy_lambda.py
HANDLER_PATH = Path(__file__).resolve().parent.parent / "src" / "batch" / "ingest_lambda.py"

TRUST_POLICY = {
    "Version": "2012-10-17",
    "Statement": [{"Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}],
}
PERMISSIONS_POLICY = {
    "Version": "2012-10-17",
    "Statement": [{"Effect": "Allow", "Action": ["logs:*", "s3:*"], "Resource": "*"}],
}


def build_zip() -> bytes:
    build_dir = Path(tempfile.mkdtemp(prefix="ingest_lambda_build_"))
    try:
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "requests", "-t", str(build_dir), "-q"],
            check=True,
        )
        shutil.copy(HANDLER_PATH, build_dir / "ingest_lambda.py")

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for path in build_dir.rglob("*"):
                if path.is_file():
                    zf.write(path, arcname=path.relative_to(build_dir))
        return buf.getvalue()
    finally:
        shutil.rmtree(build_dir, ignore_errors=True)


def ensure_role(iam) -> str:
    try:
        role_arn = iam.create_role(
            RoleName=ROLE_NAME, AssumeRolePolicyDocument=json.dumps(TRUST_POLICY)
        )["Role"]["Arn"]
        print(f"[iam] created role: {ROLE_NAME}")
    except ClientError as e:
        if e.response["Error"]["Code"] != "EntityAlreadyExists":
            raise
        role_arn = iam.get_role(RoleName=ROLE_NAME)["Role"]["Arn"]
        print(f"[iam] role already exists: {ROLE_NAME}")

    iam.put_role_policy(
        RoleName=ROLE_NAME,
        PolicyName="spacex-ingest-permissions",
        PolicyDocument=json.dumps(PERMISSIONS_POLICY),
    )
    return role_arn


def ensure_function(lam, role_arn: str):
    print("Building deployment package (vendoring requests)...")
    zip_bytes = build_zip()
    print(f"  package size: {len(zip_bytes) / 1024:.0f} KB")

    env = {"Variables": {"S3_BUCKET": S3_BUCKET}}
    try:
        lam.create_function(
            FunctionName=FUNCTION_NAME,
            Runtime="python3.11",
            Role=role_arn,
            Handler="ingest_lambda.handler",
            Code={"ZipFile": zip_bytes},
            Timeout=120,
            MemorySize=256,
            Environment=env,
        )
        print(f"[lambda] created function: {FUNCTION_NAME}")
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceConflictException":
            raise
        print(f"[lambda] function already exists, updating code + config: {FUNCTION_NAME}")
        lam.update_function_code(FunctionName=FUNCTION_NAME, ZipFile=zip_bytes)
        time.sleep(1)
        lam.update_function_configuration(FunctionName=FUNCTION_NAME, Environment=env)


if __name__ == "__main__":
    kwargs = boto3_kwargs()
    iam = boto3.client("iam", **kwargs)
    lam = boto3.client("lambda", **kwargs)

    role_arn = ensure_role(iam)
    time.sleep(2)
    ensure_function(lam, role_arn)
    print("Done. Invoke with:")
    print(f"  ./awslocal.sh lambda invoke --function-name {FUNCTION_NAME} --payload '{{}}' /tmp/out.json && cat /tmp/out.json")
