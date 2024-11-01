#!/bin/bash

# Define variables
name: Glue Job Deploy - Test Script

on:
  workflow_dispatch:

concurrency:
  group: ${{ github.ref }}
  cancel-in-progress: true

permissions:
  id-token: write  
  contents: read
  
jobs:
  run-glue-job:
    runs-on: ubuntu-latest

    steps:
    - name: Checkout code
      uses: actions/checkout@v4

    - name: Configure AWS Credentials
      uses: aws-actions/configure-aws-credentials@v4
      with:
        role-to-assume: arn:aws:iam::802803944024:role/oidc-github-sede-res-lakehouse
        role-session-name: github-sede-res-lakehouse
        aws-region: us-east-1
     
    - name: Run Glue Job 
      run: |
        #!/bin/bash

        # Define variables
        JOB_NAME="process_tables_by_schema_v2"
        SCRIPT_LOCATION="s3://mp2appsrvdevshell/deploysetup/process_tables_by_schema_v2.py"
        LOCAL_SCRIPT_PATH="src/reading_from_s3_approach/process_tables_by_schema_v2.py"
        GLUE_ROLE="arn:aws:iam::802803944024:role/service-role/AWSGlueServiceRole"
        REGION="us-east-1"
        CONFIG_JSON="src/reading_from_s3_approach/setup/config.json"
        CONFIG_S3_PATH="s3://mp2appsrvdevshell/deploysetup/"
        CONFIG_S3_BUCKET="mp2appsrvdevshell"
        CONFIG_S3_KEY="deploysetup/config.json"
        CRON_EXPRESSION="cron(*/15 * * * ? *)"

        # Copy LOCAL_SCRIPT to S3
        aws s3 cp $LOCAL_SCRIPT_PATH $SCRIPT_LOCATION --region $REGION 
        if [ $? -ne 0 ]; then
            echo "Failed to upload script to S3"
            exit 1
        fi

        # Copy CONFIG_JSON to S3
        aws s3 cp $CONFIG_JSON $CONFIG_S3_PATH --region $REGION 
        if [ $? -ne 0 ]; then
            echo "Failed to upload config.json to S3"
            exit 1
        fi

        # Check if the Glue job already exists
        EXISTING_JOB=$(aws glue get-job --job-name $JOB_NAME --region $REGION  2>&1)
        if echo $EXISTING_JOB | grep -q 'ResourceNotFoundException'; then
            # Create the Glue job if it doesn't exist
            CREATE_RESPONSE=$(aws glue create-job \
                --name $JOB_NAME \
                --role $GLUE_ROLE \
                --command "Name=glueetl,ScriptLocation=$SCRIPT_LOCATION,PythonVersion=3" \
                --default-arguments "{\"--datalake-formats\": \"iceberg\", \"--CONFIG_S3_BUCKET\": \"$CONFIG_S3_BUCKET\", \"--CONFIG_S3_KEY\": \"$CONFIG_S3_KEY\", $(cat $CONFIG_JSON | jq -r 'to_entries|map("--"+.key+"=\""+.value+"\"")|.[]')" \
                --region $REGION 2>&1)
            if echo $CREATE_RESPONSE | grep -q 'error'; then
                echo "Failed to create Glue job: $CREATE_RESPONSE"
                exit 1
            fi
            echo "Glue job $JOB_NAME created successfully."
        else
            # Edit the Glue job if it already exists
            UPDATE_RESPONSE=$(aws glue update-job \
                --job-name $JOB_NAME \
                --job-update "{
                    \"Role\": \"$GLUE_ROLE\",
                    \"Command\": {
                        \"Name\": \"glueetl\",
                        \"ScriptLocation\": \"$SCRIPT_LOCATION\",
                        \"PythonVersion\": \"3\"
                    },
                    \"DefaultArguments\": {
                        \"--datalake-formats\": \"iceberg\",
                        \"--CONFIG_S3_BUCKET\": \"$CONFIG_S3_BUCKET\",
                        \"--CONFIG_S3_KEY\": \"$CONFIG_S3_KEY\",
                        $(cat $CONFIG_JSON | jq -r 'to_entries|map("--"+.key+"=\""+.value+"\"")|.[]')
                    }
                }" \
                --region $REGION  2>&1)
            if echo $UPDATE_RESPONSE | grep -q 'error'; then
                echo "Failed to update Glue job: $UPDATE_RESPONSE"
                exit 1
            fi
            echo "Glue job $JOB_NAME updated successfully."
        fi

        # Start the Glue job
        START_RESPONSE=$(aws glue start-job-run \
            --job-name $JOB_NAME \
            --region $REGION \
            --arguments "--JOB_NAME=$JOB_NAME" \
                        "--CONFIG_S3_BUCKET=$CONFIG_S3_BUCKET" \
                        "--CONFIG_S3_KEY=$CONFIG_S3_KEY" 2>&1)
        if echo $START_RESPONSE | grep -q 'error'; then
            echo "Failed to start Glue job: $START_RESPONSE"
            exit 1
        fi

        echo "Glue job $JOB_NAME started successfully."

        # Check if the trigger exists
        aws glue get-trigger --name "$JOB_NAME-trigger" > /dev/null 2>&1

        if [ $? -eq 0 ]; then
            echo "Trigger already exists."
        else
            echo "Creating a new trigger..."
            aws glue create-trigger --name "$JOB_NAME-trigger" \
                --type SCHEDULED \
                --schedule "$CRON_EXPRESSION" \
                --actions JobName=$JOB_NAME \
                --start-on-creation
        fi

echo "Glue job $JOB_NAME started successfully."