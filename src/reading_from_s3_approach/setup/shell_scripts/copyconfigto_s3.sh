#!/bin/bash

REGION="us-east-1"
AWS_PROFILE="data-platform-dev"  # Your AWS CLI profile
CONFIG_JSON="/Users/O.Ogunfunminiyi2/Documents/lakehouse/src/reading_from_s3_approach/setup/config.json"

CONFIG_S3_PATH="s3://mp2appsrvdevshell/deploysetup/"


# Copy config.json to S3
aws s3 cp $CONFIG_JSON $CONFIG_S3_PATH --region $REGION --profile $AWS_PROFILE
if [ $? -ne 0 ]; then
    echo "Failed to upload config.json to S3"
    exit 1
fi
