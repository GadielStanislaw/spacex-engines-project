#!/usr/bin/env bash
# Wrapper to run AWS CLI against the local moto server instead of real AWS.
# Usage: ./awslocal.sh s3 ls
#        ./awslocal.sh dynamodb scan --table-name latest_reading
#        ./awslocal.sh kinesis list-streams
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
aws --endpoint-url=http://localhost:4566 "$@"
