import os

from dotenv import load_dotenv

load_dotenv()

AWS_ENV = os.getenv("AWS_ENV", "local")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
FLOCI_ENDPOINT = os.getenv("FLOCI_ENDPOINT", "http://localhost:4566")

S3_BUCKET = os.getenv("S3_BUCKET", "spacex-engines-project")
DYNAMODB_TABLE = os.getenv("DYNAMODB_TABLE", "latest_reading")
KINESIS_STREAM = os.getenv("KINESIS_STREAM", "engine-telemetry")


def boto3_kwargs() -> dict:
    """Common kwargs for every boto3 client/resource in this project.

    AWS_ENV=local -> points at the local Floci AWS emulator (fake creds, local endpoint).
    AWS_ENV=aws   -> real AWS; uses whatever credentials/region are in the
                     environment (aws configure / IAM role), no endpoint override.
    """
    if AWS_ENV == "local":
        return {
            "region_name": AWS_REGION,
            "endpoint_url": FLOCI_ENDPOINT,
            "aws_access_key_id": "test",
            "aws_secret_access_key": "test",
        }
    return {"region_name": AWS_REGION}
