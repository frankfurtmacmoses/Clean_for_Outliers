
import sys
import boto3
import json
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, Row
from pyspark.sql.functions import col, expr

s3_client = boto3.client('s3')
def read_s3_file(bucket_name, key):
    response = s3_client.get_object(Bucket=bucket_name, Key=key)
    return response['Body'].read().decode('utf-8')

if __name__ == "__main__":
   res = read_s3_file('mp2appsrvdevshell', 'deploysetup/config.json')
          